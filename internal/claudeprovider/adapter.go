package claudeprovider

import (
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providerjson"
)

// ObserveRuntime must be supplied by production composition's runtime/auth
// observer. It performs no model request. An observation is not a qualification
// signature: Store and the supervisor must independently admit every launch.
type ObserveRuntime func(context.Context) (contracts.RuntimeBinding, error)

type Adapter struct {
	route, executable, authHome string
	bound                       contracts.RuntimeBinding
	observe                     ObserveRuntime
}

var _ contracts.Provider = (*Adapter)(nil)
var ErrRuntime = errors.New("Claude runtime is unavailable or changed; qualification required")

// New pins one role to one measured binding. It neither launches a process nor
// grants qualification. Re-observation can only confirm that binding, never
// silently switch its model, credential class, or runtime after construction.
func New(route, executable, authHome string, bound contracts.RuntimeBinding, observe ObserveRuntime) (*Adapter, error) {
	family, ok := ModelFamily(bound.Identity.Model)
	if observe == nil || route == "" || len(route) > 128 || strings.TrimSpace(route) != route || strings.ContainsAny(route, "\x00\r\n") ||
		!ok || bound.Identity.Provider != "claude" || bound.Identity.Family != family || bound.Identity.Version == "" ||
		bound.AuthMode != AuthModeSubscription || bound.AuthDigest == "" || bound.BinaryDigest == "" || bound.PolicyDigest == "" || bound.FixtureDigest == "" {
		return nil, ErrRuntime
	}
	for _, p := range []string{executable, authHome} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || strings.ContainsRune(p, '\x00') {
			return nil, ErrRuntime
		}
	}
	return &Adapter{route: route, executable: executable, authHome: authHome, bound: bound, observe: observe}, nil
}

func (a *Adapter) Name() string {
	if a == nil {
		return ""
	}
	return a.route
}

func (a *Adapter) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	if err := ctx.Err(); err != nil {
		return contracts.RuntimeBinding{}, err
	}
	if a == nil || a.observe == nil {
		return contracts.RuntimeBinding{}, ErrRuntime
	}
	current, err := a.observe(ctx)
	if err != nil || current != a.bound {
		return contracts.RuntimeBinding{}, ErrRuntime
	}
	if err := ctx.Err(); err != nil {
		return contracts.RuntimeBinding{}, err
	}
	return current, nil
}

func (a *Adapter) Probe(ctx context.Context) (domain.ProviderIdentity, error) {
	binding, err := a.Binding(ctx)
	return binding.Identity, err
}

func (a *Adapter) Invocation(ctx context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	binding, err := a.Binding(ctx)
	if err != nil {
		return contracts.Invocation{}, err
	}
	if input.Provider != binding.Identity || input.AuthMode != binding.AuthMode {
		return contracts.Invocation{}, ErrRuntime
	}
	return Invocation(ctx, a.executable, a.authHome, input)
}

func (a *Adapter) Parse(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult) (contracts.PhaseResult, error) {
	if a == nil || input.Provider != a.bound.Identity || input.AuthMode != a.bound.AuthMode {
		return contracts.PhaseResult{Provider: input.Provider, Outcome: contracts.PhaseResultIndeterminate, FailureReason: contracts.ProviderFailureBinding}, ErrRuntime
	}
	// Do not invent monetary authority from Claude's list-price estimate.
	// The shared classifier leaves UsageTrusted false pending billing policy.
	terminal, err := TerminalStreamResult(ctx, input, command)
	if err != nil {
		return contracts.PhaseResult{Provider: input.Provider, Outcome: contracts.PhaseResultIndeterminate, FailureReason: contracts.ProviderFailureProtocol}, err
	}
	command.Stdout = terminal
	return providerjson.Command(ctx, input, command, true)
}
