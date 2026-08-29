package testkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type ProviderBehavior string

const (
	ProviderValid       ProviderBehavior = "valid"
	ProviderMalformed   ProviderBehavior = "malformed"
	ProviderOversized   ProviderBehavior = "oversized"
	ProviderExitBefore  ProviderBehavior = "exit_before"
	ProviderPartial     ProviderBehavior = "partial"
	ProviderHang        ProviderBehavior = "hang"
	ProviderProgress    ProviderBehavior = "progress"
	ProviderSecret      ProviderBehavior = "secret"
	ProviderForbidden   ProviderBehavior = "forbidden"
	ProviderHistoryEdit ProviderBehavior = "history_edit"
	ProviderVerifyEdit  ProviderBehavior = "verification_edit"
)

// ProviderStep is a deterministic script entry for one phase. A step may
// write partial files before returning an error, which lets recovery tests
// assert that inspectable changes do not advance state.
type ProviderStep struct {
	Behavior     ProviderBehavior
	Outcome      string
	Artifact     []byte
	Transcript   string
	ChangedFiles []string
	Delay        time.Duration
	WriteFiles   map[string][]byte
	UsageUnits   int64
}

// ScriptedProvider implements contracts.Provider without a process or
// network. It records calls and consumes one step per phase, making fallback
// and retry counts directly assertable.
type ScriptedProvider struct {
	mu       sync.Mutex
	Identity domain.ProviderIdentity
	Steps    map[domain.Phase][]ProviderStep
	Default  ProviderStep
	Calls    []domain.Phase
	Clock    *FakeClock
	Crash    *CrashController
}

func NewScriptedProvider(identity domain.ProviderIdentity) *ScriptedProvider {
	return &ScriptedProvider{Identity: identity, Steps: make(map[domain.Phase][]ProviderStep)}
}

func (p *ScriptedProvider) Name() string { return p.Identity.Provider }

func (p *ScriptedProvider) Probe(context.Context) (domain.ProviderIdentity, error) {
	if p.Identity.Provider == "" || p.Identity.Version == "" {
		return domain.ProviderIdentity{}, errors.New("testkit: incomplete provider identity")
	}
	return p.Identity, nil
}

func (p *ScriptedProvider) Add(phase domain.Phase, step ProviderStep) {
	p.mu.Lock()
	p.Steps[phase] = append(p.Steps[phase], step)
	p.mu.Unlock()
}

func (p *ScriptedProvider) CallsSnapshot() []domain.Phase {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Phase(nil), p.Calls...)
}

func (p *ScriptedProvider) next(phase domain.Phase) ProviderStep {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Calls = append(p.Calls, phase)
	steps := p.Steps[phase]
	if len(steps) == 0 {
		return p.Default
	}
	step := steps[0]
	p.Steps[phase] = steps[1:]
	return step
}

func (p *ScriptedProvider) Run(ctx context.Context, input contracts.PhaseInput) (contracts.PhaseResult, error) {
	if err := input.Ticket.Validate(); err != nil {
		return contracts.PhaseResult{}, err
	}
	step := p.next(input.Phase)
	if p.Crash != nil {
		if err := p.Crash.Hit(BeforeExternalCall); err != nil {
			return contracts.PhaseResult{}, err
		}
	}
	if step.Delay > 0 {
		if p.Clock != nil {
			if err := p.Clock.Sleep(ctx, step.Delay); err != nil {
				return contracts.PhaseResult{}, err
			}
		} else {
			timer := time.NewTimer(step.Delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return contracts.PhaseResult{}, ctx.Err()
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return contracts.PhaseResult{}, err
	}
	for path, content := range step.WriteFiles {
		if err := writeWithin(input.Worktree, path, content); err != nil {
			return contracts.PhaseResult{}, err
		}
	}
	if step.Behavior == ProviderPartial && len(step.WriteFiles) == 0 {
		if err := writeWithin(input.Worktree, "testkit-partial.txt", []byte("partial\n")); err != nil {
			return contracts.PhaseResult{}, err
		}
	}
	if step.Behavior == ProviderHang || step.Behavior == ProviderProgress {
		<-ctx.Done()
		return contracts.PhaseResult{}, ctx.Err()
	}
	if step.Behavior == ProviderExitBefore {
		return contracts.PhaseResult{}, errors.New("testkit: provider exited before submission")
	}
	if step.Behavior == ProviderMalformed {
		return contracts.PhaseResult{Outcome: "malformed", Artifact: []byte("not-json"), Provider: p.Identity}, errors.New("testkit: malformed structured output")
	}
	if step.Behavior == ProviderOversized {
		return contracts.PhaseResult{Outcome: "oversized", Artifact: []byte(strings.Repeat("x", 2<<20)), Provider: p.Identity}, errors.New("testkit: oversized structured output")
	}
	if step.Behavior == ProviderSecret {
		return contracts.PhaseResult{Outcome: "secret", Transcript: "token=fixture-secret-must-redact", Provider: p.Identity}, errors.New("testkit: secret output fixture")
	}
	if step.Behavior == ProviderForbidden || step.Behavior == ProviderHistoryEdit || step.Behavior == ProviderVerifyEdit {
		return contracts.PhaseResult{Outcome: string(step.Behavior), Provider: p.Identity}, errors.New("testkit: forbidden provider action")
	}
	outcome := step.Outcome
	if outcome == "" {
		outcome = "completed"
	}
	return contracts.PhaseResult{
		Outcome:      outcome,
		Artifact:     append([]byte(nil), step.Artifact...),
		Transcript:   step.Transcript,
		Provider:     p.Identity,
		ChangedFiles: append([]string(nil), step.ChangedFiles...),
		UsageTrusted: true,
		UsageUnits:   step.UsageUnits,
	}, nil
}

func writeWithin(root, name string, content []byte) error {
	if root == "" || filepath.IsAbs(name) {
		return errors.New("testkit: invalid fixture path")
	}
	clean := filepath.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return errors.New("testkit: fixture path escapes worktree")
	}
	path := filepath.Join(root, clean)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create fixture directory: %w", err)
	}
	return os.WriteFile(path, content, 0o644)
}
