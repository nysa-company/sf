package cursorprovider

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func cursorInput() contracts.PhaseInput {
	return contracts.PhaseInput{Phase: domain.PhasePlanning, Worktree: "/private/worktree", AllowedPaths: []string{"src"}, Prompt: "untrusted --api-key bogus --resume other", Schema: []byte(`{"type":"object"}`), Timeout: time.Minute, Profile: contracts.ProfileGuarded, AuthMode: AuthModeBrowser, Provider: domain.ProviderIdentity{Provider: "cursor", Model: "gpt-5.6-luna-low", Family: "openai-gpt-5.6", Version: "2026.09.02-c22c1a3"}}
}

func TestCursorInvocationPinsRoleAndKeepsPromptOffArgv(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview} {
		input := cursorInput()
		input.Phase = phase
		inv, err := Invocation(t.Context(), "/private/cursor-agent", "/private/auth", input)
		if err != nil || !strings.Contains(string(inv.Stdin), input.Prompt) {
			t.Fatal("valid invocation refused")
		}
		args := strings.Join(inv.Argv, " ")
		write := phase == domain.PhaseBuild || phase == domain.PhaseVerification
		if strings.Contains(args, "--api-key") || strings.Contains(args, "--resume") || strings.Contains(args, "--force") || strings.Contains(args, "--mode ask") == write {
			t.Fatal("role or prompt leaked into command authority")
		}
		policy, err := Permissions(input)
		if err != nil || !strings.Contains(string(policy), "Shell(*)") || !strings.Contains(string(policy), "Mcp(*:*)") || strings.Contains(string(policy), "Write(**)") == write {
			t.Fatal("permission proposal not role bound")
		}
		inv.Argv = append(inv.Argv, "--approve-mcps")
		if MatchesInvocation(t.Context(), "/private/cursor-agent", "/private/auth", input, inv) {
			t.Fatal("changed proposal accepted")
		}
	}
}

func TestCursorInvocationRejectsUnboundInputs(t *testing.T) {
	for _, mutate := range []func(*contracts.PhaseInput){
		func(i *contracts.PhaseInput) { i.Provider.Model = "auto" },
		func(i *contracts.PhaseInput) { i.Provider.Family = "other" },
		func(i *contracts.PhaseInput) { i.Provider.Version = "next" },
		func(i *contracts.PhaseInput) { i.Profile = contracts.ProfileAutonomous },
		func(i *contracts.PhaseInput) { i.AuthMode = "api" },
		func(i *contracts.PhaseInput) { i.AllowedPaths = []string{"../escape"} },
		func(i *contracts.PhaseInput) { i.AllowedPaths = []string{"src)"} },
		func(i *contracts.PhaseInput) { i.Phase = domain.PhaseBuild; i.AllowedPaths = []string{"."} },
		func(i *contracts.PhaseInput) { i.RequestDigest = strings.Repeat("f", 64) },
		func(i *contracts.PhaseInput) { i.Timeout = time.Hour },
	} {
		input := cursorInput()
		mutate(&input)
		if _, err := Invocation(t.Context(), "/private/cursor-agent", "/private/auth", input); err == nil {
			t.Fatal("invalid input admitted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Invocation(ctx, "/private/cursor-agent", "/private/auth", cursorInput()); err == nil {
		t.Fatal("cancelled invocation admitted")
	}
}
