package workflowruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

func TestConfiguredRoleRefusesFallbackAndUnknown(t *testing.T) {
	for _, role := range []providercoord.Role{providercoord.RolePlanner, providercoord.RoleBuilder, providercoord.RoleReviewer} {
		for _, names := range [][]string{nil, {"codex", "claude"}, {"cursor"}, {"auto"}, {""}, {"claude"}, {"codex"}} {
			var effective config.Effective
			effective.Providers = config.ProviderOrder{Planner: names, Builder: names, Reviewer: names}
			name, err := configuredProvider(effective, role)
			valid := len(names) == 1 && (names[0] == "codex" || names[0] == "claude" || names[0] == "cursor")
			if valid && (err != nil || name != names[0]) || !valid && !errors.Is(err, ErrProviderOrder) {
				t.Fatalf("role=%s names=%v result=%s err=%v", role, names, name, err)
			}
		}
	}
}

func TestPhaseResultReadersAndLaunchMatchConfiguredClaude(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview} {
		t.Run(string(phase), func(t *testing.T) {
			request, evidence, coordinator, _, _ := phaseFixture(t)
			role, state := providercoord.RoleReviewer, domain.StateVerifying
			if phase == domain.PhaseBuild {
				role, state = providercoord.RoleBuilder, domain.StateBuilding
			}
			if phase == domain.PhaseReview {
				state = domain.StateReviewing
			}
			request.Phase, request.Ticket.State = phase, state
			effective, err := decodeTicketConfig(request.Ticket)
			if err != nil {
				t.Fatal(err)
			}
			effective.Providers = config.ProviderOrder{Planner: []string{"claude"}, Builder: []string{"claude"}, Reviewer: []string{"claude"}}
			// This is a reader/route mock, not an independently qualified pair.
			request.Ticket.ConfigSnapshot, request.Ticket.ConfigDigest, err = config.Snapshot(effective)
			if err != nil {
				t.Fatal(err)
			}
			evidence.ticket = request.Ticket
			key := store.ProviderAttemptResultKey{AttemptID: 42, Ref: request.Ticket.Ref, Phase: phase, Attempt: 1}
			result := phaseProviderResult(key, request, role)
			result.Claim.Binding.Identity.Provider = "claude"
			evidence.results[42] = result
			evidence.parsed[42] = phaseartifact.Parsed{Phase: phase, Provider: result.Claim.Binding.Identity, Verify: &phaseartifact.Verification{}, Builder: &phaseartifact.Builder{}, Reviewer: &phaseartifact.Reviewer{}}
			coordinator.result = providercoord.Result{Code: providercoord.Completed, ProviderResult: key}
			bindCoordinatorResult(t, coordinator, evidence, key, 0)
			runner := PhaseRunner{Store: evidence, Coordinator: coordinator}
			if _, _, _, err := runner.admit(context.Background(), request, state, string(role)); err != nil {
				t.Fatal(err)
			}
			input := result.Claim.Input
			input.Timeout = time.Minute
			if _, err := runner.run(context.Background(), request, evidence.project, evidence.worktree, role, input, phaseartifact.Validation{}); err != nil {
				t.Fatal(err)
			}
			if coordinator.request.ExpectedProvider != "claude" {
				t.Fatal("launch lost configured identity")
			}
			if _, _, err := runner.loadHistorical(context.Background(), key, request.Ticket, evidence.project, evidence.worktree, phase, role); err != nil {
				t.Fatal(err)
			}
			changed := evidence.results[42]
			changed.Claim.Binding.Identity.Provider = "codex"
			evidence.results[42] = changed
			if _, _, err := runner.loadHistorical(context.Background(), key, request.Ticket, evidence.project, evidence.worktree, phase, role); !errors.Is(err, ErrProviderResultInvalid) {
				t.Fatal("wrong historical provider accepted", err)
			}
		})
	}
}
