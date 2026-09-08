package cursorprovider

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func cursorBindingFixture() contracts.RuntimeBinding {
	in := cursorInput()
	return contracts.RuntimeBinding{Identity: in.Provider, AuthMode: in.AuthMode, AuthDigest: "auth", BinaryDigest: "binary", PolicyDigest: "policy", FixtureDigest: FixtureDigest(in.Provider.Model, "qualified label")}
}

func TestAdapterPinsRuntimeAndDisplay(t *testing.T) {
	bound := cursorBindingFixture()
	for name, mutate := range map[string]func(*contracts.RuntimeBinding){
		"model":    func(b *contracts.RuntimeBinding) { b.Identity.Model = "auto" },
		"family":   func(b *contracts.RuntimeBinding) { b.Identity.Family = "other" },
		"provider": func(b *contracts.RuntimeBinding) { b.Identity.Provider = "claude" },
		"version":  func(b *contracts.RuntimeBinding) { b.Identity.Version = "other" },
		"auth":     func(b *contracts.RuntimeBinding) { b.AuthDigest = "other" },
		"billing":  func(b *contracts.RuntimeBinding) { b.AuthMode = "api" },
		"binary":   func(b *contracts.RuntimeBinding) { b.BinaryDigest = "other" },
		"policy":   func(b *contracts.RuntimeBinding) { b.PolicyDigest = "other" },
		"fixture":  func(b *contracts.RuntimeBinding) { b.FixtureDigest = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			current := bound
			a, err := New("builder", "/private/cursor-agent", "/private/auth", "qualified label", bound, func(context.Context) (contracts.RuntimeBinding, error) { return current, nil })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Invocation(t.Context(), cursorInput()); err != nil {
				t.Fatal(err)
			}
			mutate(&current)
			if _, err := a.Invocation(t.Context(), cursorInput()); err != ErrRuntime {
				t.Fatal("runtime drift admitted")
			}
		})
	}
	if _, err := New("builder", "/private/cursor-agent", "/private/auth", "changed label", bound, func(context.Context) (contracts.RuntimeBinding, error) { return bound, nil }); err != ErrRuntime {
		t.Fatal("unbound display accepted")
	}
}

func TestAdapterOnlyCompleteStreamCanGrantArtifactRepair(t *testing.T) {
	in, bound := cursorInput(), cursorBindingFixture()
	a, err := New("builder", "/private/cursor-agent", "/private/auth", "qualified label", bound, func(context.Context) (contracts.RuntimeBinding, error) {
		return bound, errors.New("sensitive observer output")
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Binding(t.Context()); err != ErrRuntime {
		t.Fatal("observer diagnostic escaped")
	}
	init := `{"type":"system","subtype":"init","apiKeySource":"login","cwd":"` + in.Worktree + `","model":"qualified label","session_id":"s"}` + "\n"
	terminal := `{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"{\"ok\":true}"}`
	result, err := a.Parse(t.Context(), in, contracts.CommandResult{Stdout: []byte(init + terminal)})
	if err != nil || result.Outcome != contracts.PhaseResultCompleted || result.UsageTrusted || result.UsageUnits != 0 {
		t.Fatal("valid result or unknown billing misclassified", err)
	}
	missingArtifact := strings.Replace(terminal, `,"result":"{\"ok\":true}"`, "", 1)
	result, err = a.Parse(t.Context(), in, contracts.CommandResult{Stdout: []byte(init + missingArtifact)})
	if err == nil || result.Outcome != contracts.PhaseResultInvalidArtifact {
		t.Fatal("complete missing artifact misclassified")
	}
	for name, cmd := range map[string]contracts.CommandResult{
		"no init":              {Stdout: []byte(terminal)},
		"model drift":          {Stdout: []byte(strings.Replace(init, "qualified label", "other", 1) + terminal)},
		"truncation":           {Stdout: []byte(init + terminal), StdoutTruncated: true},
		"exit":                 {Stdout: []byte(init + terminal), ExitCode: 1},
		"untyped server error": {Stdout: []byte(init), Stderr: []byte("Error: 503 service unavailable"), ExitCode: 1},
		"untyped retry hint":   {Stderr: []byte("Error: 503 retryable=true"), ExitCode: 1},
		"duplicate result":     {Stdout: []byte(init + terminal + "\n" + terminal)},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := a.Parse(t.Context(), in, cmd)
			if err == nil || r.Outcome != contracts.PhaseResultIndeterminate {
				t.Fatal("incomplete protocol granted authority")
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.Parse(ctx, in, contracts.CommandResult{Stdout: []byte(init + terminal)}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored")
	}
}
