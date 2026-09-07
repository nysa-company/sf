package cursorprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providerjson"
)

var ErrRuntime = errors.New("Cursor runtime is unavailable or changed; qualification required")

// ObserveRuntime must be supplied by SF composition. It is observation only;
// Store and Supervisor independently require signed native qualification.
type ObserveRuntime func(context.Context) (contracts.RuntimeBinding, error)

type Adapter struct {
	route, executable, authHome, display string
	bound                                contracts.RuntimeBinding
	observe                              ObserveRuntime
}

var _ contracts.Provider = (*Adapter)(nil)

// FixtureDigest binds the expected session label used to authenticate print-mode
// init to the exact requested catalog ID. A catalog observation alone never proves
// that the named role, filesystem, and drain fixtures have passed.
func FixtureDigest(model, display string) string {
	if _, ok := ModelFamily(model); !ok || display == "" || len(display) > 128 || strings.TrimSpace(display) != display || strings.ContainsAny(display, "\x00\r\n\t") {
		return ""
	}
	sum := sha256.Sum256([]byte("sf-cursor-fixture-v1\x00" + TrustedHooksPolicy + "\x00staged-cli,private-home,role-write,outside-read-denied,read-only,cancel-drain,session-terminal-v2\x00" + model + "\x00" + display))
	return hex.EncodeToString(sum[:])
}

// New is exec-free and cannot grant a runtime policy or qualification. The
// display label is covered by the signed binding's fixture digest, not taken
// from the result being parsed. Re-observation may not silently change it.
func New(route, executable, authHome, display string, bound contracts.RuntimeBinding, observe ObserveRuntime) (*Adapter, error) {
	family, ok := ModelFamily(bound.Identity.Model)
	if observe == nil || route == "" || len(route) > 128 || strings.TrimSpace(route) != route || strings.ContainsAny(route, "\x00\r\n") ||
		!ok || bound.Identity.Provider != "cursor" || bound.Identity.Family != family || bound.Identity.Version != "2026.09.02-c22c1a3" || bound.AuthMode != AuthModeBrowser ||
		bound.AuthDigest == "" || bound.BinaryDigest == "" || bound.PolicyDigest == "" || FixtureDigest(bound.Identity.Model, display) == "" || bound.FixtureDigest != FixtureDigest(bound.Identity.Model, display) {
		return nil, ErrRuntime
	}
	for _, p := range []string{executable, authHome} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || strings.ContainsAny(p, "\x00\r\n") {
			return nil, ErrRuntime
		}
	}
	return &Adapter{route: route, executable: executable, authHome: authHome, display: display, bound: bound, observe: observe}, nil
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
	b, err := a.Binding(ctx)
	return b.Identity, err
}

func (a *Adapter) Invocation(ctx context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	b, err := a.Binding(ctx)
	if err != nil {
		return contracts.Invocation{}, err
	}
	if input.Provider != b.Identity || input.AuthMode != b.AuthMode {
		return contracts.Invocation{}, ErrRuntime
	}
	return Invocation(ctx, a.executable, a.authHome, input)
}

func (a *Adapter) Parse(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult) (contracts.PhaseResult, error) {
	failed := contracts.PhaseResult{Provider: input.Provider, Outcome: contracts.PhaseResultIndeterminate, FailureReason: contracts.ProviderFailureProtocol}
	if a == nil || input.Provider != a.bound.Identity || input.AuthMode != a.bound.AuthMode {
		failed.FailureReason = contracts.ProviderFailureBinding
		return failed, ErrRuntime
	}
	if err := ctx.Err(); err != nil {
		return failed, err
	}
	if command.ExitCode != 0 || command.StdoutTruncated || command.StderrTruncated || len(command.Stderr) > providerjson.MaxResultBytes {
		return failed, providerjson.ErrProtocol
	}
	terminal, err := streamTerminal(command.Stdout, input.Worktree, a.display)
	if err != nil {
		return failed, err
	}
	command.Stdout = terminal
	// Token counts do not establish a dollar charge. Shared classification
	// preserves unknown billing and grants no fallback or retry authority.
	return providerjson.Command(ctx, input, command, false)
}
