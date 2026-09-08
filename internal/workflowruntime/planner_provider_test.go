package workflowruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providercoord"
)

func TestPlannerUsesExactConfiguredClaudeWithoutFallback(t *testing.T) {
	for _, returned := range []string{"claude", "codex"} {
		t.Run(returned, func(t *testing.T) {
			request, evidence, coordinator := plannerFixture(t)
			effective, err := decodeTicketConfig(request.Ticket)
			if err != nil {
				t.Fatal(err)
			}
			effective.Providers.Planner = []string{"claude"}
			request.Ticket.ConfigSnapshot, request.Ticket.ConfigDigest, err = config.Snapshot(effective)
			if err != nil {
				t.Fatal(err)
			}
			evidence.result.Claim.Binding.Identity.Provider = returned
			evidence.parsed.Provider = evidence.result.Claim.Binding.Identity
			original := coordinator.onRun
			coordinator.onRun = func(r providercoord.Request) {
				original(r)
				input := evidence.result.Claim.Input
				input.Provider = evidence.result.Claim.Binding.Identity
				payload, digest, err := contracts.CanonicalPhaseInput(input)
				if err != nil {
					t.Fatal(err)
				}
				input.RequestDigest = digest
				evidence.result.Claim.Input = input
				evidence.result.Claim.RequestPayload, evidence.result.Claim.RequestDigest = payload, digest
			}
			_, err = (PlannerRunner{Store: evidence, Coordinator: coordinator}).RunArtifact(context.Background(), request)
			if returned == "claude" && err != nil {
				t.Fatal(err)
			}
			if returned != "claude" && !errors.Is(err, ErrProviderResultInvalid) {
				t.Fatalf("wrong provider: %v", err)
			}
			if coordinator.request.ExpectedProvider != "claude" {
				t.Fatal("lost configured provider")
			}
		})
	}
}
