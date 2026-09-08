package claudeprovider

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func claudeInput() contracts.PhaseInput {
	return contracts.PhaseInput{Phase: domain.PhasePlanning, Worktree: "/private/worktree", Prompt: "untrusted --dangerously-skip-permissions prompt", Schema: []byte(`{"type":"object"}`), Timeout: time.Minute, Profile: contracts.ProfileGuarded, AuthMode: AuthModeSubscription, Provider: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-4-6", Family: "anthropic-claude", Version: "2.1.263"}}
}

func TestClaudeInvocationIsRoleBoundAndExecFree(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview} {
		input := claudeInput()
		input.Phase = phase
		inv, err := Invocation(context.Background(), "/private/claude", "/private/auth", input)
		if err != nil || string(inv.Stdin) != input.Prompt || inv.AuthHome != "/private/auth" {
			t.Fatalf("phase %s: %v", phase, err)
		}
		args := strings.Join(inv.Argv, " ")
		if strings.Contains(args, input.Prompt) || strings.Contains(args, "skip-permissions") || strings.Contains(args, "Bash") || strings.Contains(args, "--resume") {
			t.Fatal("prompt or expanded authority entered argv")
		}
		write := phase == domain.PhaseBuild || phase == domain.PhaseVerification
		if strings.Contains(args, "Read,Glob,Grep,Edit,Write") != write {
			t.Fatalf("wrong tools for %s", phase)
		}
		for _, flag := range []string{"--restricted", "--safe-mode", "--no-session-persistence", "dontAsk", "mcp__*", "--output-format stream-json --verbose"} {
			if !strings.Contains(args, flag) {
				t.Fatalf("missing %s", flag)
			}
		}
	}
}

func TestClaudeInvocationRejectsUnboundOrUnsupportedInputs(t *testing.T) {
	for _, change := range []func(*contracts.PhaseInput){
		func(i *contracts.PhaseInput) { i.Provider.Model = "sonnet" },
		func(i *contracts.PhaseInput) { i.Provider.Family = "independent-fiction" },
		func(i *contracts.PhaseInput) { i.Provider.Provider = "cursor" },
		func(i *contracts.PhaseInput) { i.AuthMode = "api" },
		func(i *contracts.PhaseInput) { i.Profile = contracts.ProfileAutonomous },
		func(i *contracts.PhaseInput) { i.Worktree = "/private/../worktree" },
		func(i *contracts.PhaseInput) { i.Timeout = time.Hour },
		func(i *contracts.PhaseInput) { i.RequestDigest = strings.Repeat("f", 64) },
	} {
		input := claudeInput()
		change(&input)
		if _, err := Invocation(context.Background(), "/private/claude", "/private/auth", input); err == nil {
			t.Fatal("invalid input admitted")
		}
	}
}

func TestClaudeBuilderInventoryGuidancePreservesAuthenticatedInput(t *testing.T) {
	input := claudeInput()
	input.Phase = domain.PhaseBuild
	input.Schema = workflowprompt.BuilderSchema()
	before, digest, err := contracts.CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	input.RequestDigest = digest
	inv, err := Invocation(context.Background(), "/private/claude", "/private/auth", input)
	if err != nil {
		t.Fatal(err)
	}
	for _, phrase := range []string{"changed_files is your implementation contribution", "not the whole branch diff", "exclude preserved verification-owned files", "do not manufacture edits"} {
		if !strings.Contains(string(inv.Stdin), phrase) {
			t.Errorf("missing Builder inventory guidance %q", phrase)
		}
	}
	after, afterDigest, err := contracts.CanonicalPhaseInput(input)
	if err != nil || string(before) != string(after) || digest != afterDigest {
		t.Fatal("adapter changed authenticated input")
	}
	if !strings.HasSuffix(string(inv.Stdin), input.Prompt) || !MatchesInvocation(context.Background(), "/private/claude", "/private/auth", input, inv) {
		t.Fatal("adapter lost the exact prompt or supervisor binding")
	}
	for _, phase := range []domain.Phase{domain.PhasePlanning, domain.PhaseVerification, domain.PhaseReview} {
		other := input
		other.Phase, other.RequestDigest = phase, ""
		proposal, err := Invocation(context.Background(), "/private/claude", "/private/auth", other)
		if err != nil || string(proposal.Stdin) != other.Prompt {
			t.Fatal("Builder-only guidance changed another role")
		}
	}
}

func TestClaudeSupervisorPolicyRejectsProposalTamper(t *testing.T) {
	input := claudeInput()
	for name, change := range map[string]func(*contracts.Invocation){
		"shell":             func(i *contracts.Invocation) { i.Argv = append(i.Argv, "--tools", "Bash") },
		"bypass":            func(i *contracts.Invocation) { i.Argv = append(i.Argv, "--dangerously-skip-permissions") },
		"resume":            func(i *contracts.Invocation) { i.Argv = append(i.Argv, "--resume", "another-session") },
		"positional prompt": func(i *contracts.Invocation) { i.Argv = append(i.Argv, "--", "replacement") },
		"stdin":             func(i *contracts.Invocation) { i.Stdin = []byte("different prompt") },
		"auth":              func(i *contracts.Invocation) { i.AuthHome = "/private/other" },
		"output":            func(i *contracts.Invocation) { i.CaptureLastMessage = true },
		"schema":            func(i *contracts.Invocation) { i.OutputSchema = []byte(`{}`) },
		"executable":        func(i *contracts.Invocation) { i.Argv[0] = "/private/other" },
	} {
		t.Run(name, func(t *testing.T) {
			proposal, err := Invocation(context.Background(), "/private/claude", "/private/auth", input)
			if err != nil || !MatchesInvocation(context.Background(), "/private/claude", "/private/auth", input, proposal) {
				t.Fatal("canonical invocation rejected")
			}
			change(&proposal)
			if MatchesInvocation(context.Background(), "/private/claude", "/private/auth", input, proposal) {
				t.Fatal("changed invocation accepted")
			}
		})
	}
}
