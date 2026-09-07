package claudeprovider

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestAdapterPinsEveryRuntimeBindingField(t *testing.T) {
	input := claudeInput()
	bound := contracts.RuntimeBinding{Identity: input.Provider, AuthMode: input.AuthMode, BinaryDigest: "binary", PolicyDigest: "policy", FixtureDigest: "fixture", AuthDigest: "auth"}
	for name, mutate := range map[string]func(*contracts.RuntimeBinding){
		"model":   func(b *contracts.RuntimeBinding) { b.Identity.Model = "claude-sonnet-5" },
		"family":  func(b *contracts.RuntimeBinding) { b.Identity.Family = "other" },
		"version": func(b *contracts.RuntimeBinding) { b.Identity.Version = "changed" },
		"binary":  func(b *contracts.RuntimeBinding) { b.BinaryDigest = "changed" },
		"policy":  func(b *contracts.RuntimeBinding) { b.PolicyDigest = "changed" },
		"fixture": func(b *contracts.RuntimeBinding) { b.FixtureDigest = "changed" },
		"auth":    func(b *contracts.RuntimeBinding) { b.AuthDigest = "changed" },
		"billing": func(b *contracts.RuntimeBinding) { b.AuthMode = "api" },
	} {
		t.Run(name, func(t *testing.T) {
			current := bound
			a, err := New("builder", "/private/claude", "/private/auth", bound, func(context.Context) (contracts.RuntimeBinding, error) { return current, nil })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Invocation(context.Background(), input); err != nil {
				t.Fatal(err)
			}
			mutate(&current)
			if _, err := a.Invocation(context.Background(), input); !errors.Is(err, ErrRuntime) {
				t.Fatal("changed runtime accepted", err)
			}
			if _, err := a.Probe(context.Background()); !errors.Is(err, ErrRuntime) {
				t.Fatal("changed probe accepted", err)
			}
		})
	}
}

func TestAdapterDoesNotTrustEstimatedCostOrLeakProbeErrors(t *testing.T) {
	input := claudeInput()
	bound := contracts.RuntimeBinding{Identity: input.Provider, AuthMode: input.AuthMode, BinaryDigest: "binary", PolicyDigest: "policy", FixtureDigest: "fixture", AuthDigest: "auth"}
	a, err := New("builder", "/private/claude", "/private/auth", bound, func(context.Context) (contracts.RuntimeBinding, error) {
		return bound, errors.New("sensitive provider output")
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Binding(context.Background()); err != ErrRuntime {
		t.Fatal("raw error escaped")
	}
	command := contracts.CommandResult{Stdout: successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.01,"structured_output":{"ok":true}}`)}
	result, err := a.Parse(context.Background(), input, command)
	if err != nil || result.Outcome != contracts.PhaseResultCompleted || result.UsageTrusted || result.UsageUnits != 0 {
		t.Fatal("incorrect result accounting", err)
	}
	input.Provider.Model = "claude-sonnet-5"
	result, err = a.Parse(context.Background(), input, command)
	if err != ErrRuntime || result.Outcome != contracts.PhaseResultIndeterminate {
		t.Fatal("foreign role result accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := a.Binding(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled observation admitted")
	}
}

func TestAdapterStreamFramingDoesNotCreateArtifactRepairAuthority(t *testing.T) {
	input := claudeInput()
	bound := contracts.RuntimeBinding{Identity: input.Provider, AuthMode: input.AuthMode, BinaryDigest: "binary", PolicyDigest: "policy", FixtureDigest: "fixture", AuthDigest: "auth"}
	a, err := New("planner", "/private/claude", "/private/auth", bound, func(context.Context) (contracts.RuntimeBinding, error) { return bound, nil })
	if err != nil {
		t.Fatal(err)
	}
	complete := successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false}`)
	result, err := a.Parse(context.Background(), input, contracts.CommandResult{Stdout: complete})
	if err == nil || result.Outcome != contracts.PhaseResultInvalidArtifact {
		t.Fatal("complete missing artifact misclassified")
	}
	result, err = a.Parse(context.Background(), input, contracts.CommandResult{Stdout: []byte(`{"type":"result","subtype":"success","is_error":false}`)})
	if err == nil || result.Outcome != contracts.PhaseResultIndeterminate {
		t.Fatal("missing stream granted repair")
	}
}
