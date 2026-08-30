package testkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

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

// Supervisor is a deterministic test process supervisor. Production runners
// must supply an OS process-group supervisor; adapters never receive Signer.
type Supervisor struct{ Signer *contracts.DrainSigner }

func NewSupervisor() *Supervisor        { s, _ := contracts.NewDrainSigner(); return &Supervisor{Signer: s} }
func (s *Supervisor) PublicKey() []byte { return s.Signer.PublicKey() }
func (s *Supervisor) Run(ctx context.Context, _ contracts.DrainRequest, p contracts.Provider, input contracts.PhaseInput) (contracts.PhaseResult, error) {
	return p.Run(ctx, input)
}
func (s *Supervisor) Drain(_ context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	return s.Signer.ProveDrained(request)
}

func NewScriptedProvider(identity domain.ProviderIdentity) *ScriptedProvider {
	return &ScriptedProvider{Identity: identity, Steps: make(map[domain.Phase][]ProviderStep)}
}

func (p *ScriptedProvider) Name() string { return p.Identity.Provider }

func (p *ScriptedProvider) Probe(context.Context) (domain.ProviderIdentity, error) {
	if p.Identity.Provider == "" || p.Identity.Model == "" || p.Identity.Family == "" || p.Identity.Version == "" {
		return domain.ProviderIdentity{}, errors.New("testkit: incomplete provider identity")
	}
	return p.Identity, nil
}

func (p *ScriptedProvider) Binding(context.Context) (contracts.RuntimeBinding, error) {
	identity, err := p.Probe(context.Background())
	if err != nil {
		return contracts.RuntimeBinding{}, err
	}
	return contracts.RuntimeBinding{Identity: identity, BinaryDigest: digest("binary:" + identity.Version), PolicyDigest: digest("policy:" + identity.Provider), FixtureDigest: digest("fixture:" + identity.Provider), AuthDigest: digest("auth:" + identity.Provider)}, nil
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
	paths := make([]string, 0, len(step.WriteFiles))
	for path := range step.WriteFiles {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		content := step.WriteFiles[path]
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
		return contracts.PhaseResult{Outcome: "malformed", Artifact: []byte("not-json"), Transcript: step.Transcript, Provider: p.Identity, UsageTrusted: true}, errors.New("testkit: malformed structured output")
	}
	if step.Behavior == ProviderOversized {
		return contracts.PhaseResult{Outcome: "oversized", Artifact: []byte(strings.Repeat("x", 2<<20)), Provider: p.Identity, UsageTrusted: true}, errors.New("testkit: oversized structured output")
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
	return WriteFixtureFile(root, name, content)
}

// WriteFixtureFile writes a test fixture file only after walking each parent
// component without following symlinks. It intentionally fails before creating
// a child of a symlinked directory, so hostile fixtures cannot mutate outside
// their worktree even on a rejected write.
func WriteFixtureFile(root, name string, content []byte) error {
	if root == "" || filepath.IsAbs(name) {
		return errors.New("testkit: invalid fixture path")
	}
	clean := filepath.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return errors.New("testkit: fixture path escapes worktree")
	}
	rootPath, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve fixture worktree: %w", err)
	}
	resolvedRoot, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return fmt.Errorf("resolve fixture worktree: %w", err)
	}
	rootInfo, err := os.Stat(resolvedRoot)
	if err != nil || !rootInfo.IsDir() {
		return errors.New("testkit: fixture worktree is not a directory")
	}

	parts := strings.Split(clean, string(filepath.Separator))
	parent := resolvedRoot
	for _, part := range parts[:len(parts)-1] {
		parent, err = fixtureDirectory(parent, part)
		if err != nil {
			return err
		}
	}
	path := filepath.Join(parent, parts[len(parts)-1])
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("testkit: fixture path is a symlink")
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("inspect fixture path: %w", err)
	}
	return os.WriteFile(path, content, 0o644)
}

func fixtureDirectory(parent, component string) (string, error) {
	path := filepath.Join(parent, component)
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.Mkdir(path, 0o755); err != nil && !os.IsExist(err) {
			return "", fmt.Errorf("create fixture directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return "", fmt.Errorf("inspect fixture directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("testkit: fixture path escapes worktree through symlink")
	}
	if !info.IsDir() {
		return "", errors.New("testkit: fixture path parent is not a directory")
	}
	return path, nil
}
