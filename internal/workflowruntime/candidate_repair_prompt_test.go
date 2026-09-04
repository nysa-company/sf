package workflowruntime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func TestPhaseRunnerBuilderCarriesAuthenticatedRedCIDiagnosticIdentity(t *testing.T) {
	request, evidence, coordinator, _, _ := phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseBuild, domain.StateBuilding
	evidence.ticket = request.Ticket
	request.Plan, request.Verification = &evidence.plan, &evidence.verify
	evidence.repair = &store.CandidateRepairBuildContext{
		Ref:                      request.Ticket.Ref,
		TargetGeneration:         2,
		PredecessorGeneration:    1,
		PredecessorHeadSHA:       strings.Repeat("e", 40),
		PredecessorTreeSHA:       strings.Repeat("f", 40),
		PublicationWitnessDigest: "sha256:" + strings.Repeat("a", 64),
		EntryTicketVersion:       request.Ticket.Version,
		EntryFence:               request.Fence,
		Verification:             evidence.verify,
		Diagnostic: store.CandidateRepairDiagnostic{
			ObservationDigest:  "sha256:" + strings.Repeat("b", 64),
			RequiredSetDigest:  strings.Repeat("c", 64),
			DiagnosticDigest:   strings.Repeat("d", 64),
			TotalFailingChecks: 1,
			FailingChecks: []store.CandidateRepairDiagnosticCheck{{
				CanonicalName:           "test",
				ExternalID:              "check-test",
				NormalizedState:         "failure",
				FailingDiagnosticDigest: strings.Repeat("1", 64),
			}},
		},
	}
	key := store.ProviderAttemptResultKey{AttemptID: 41, Ref: request.Ticket.Ref, Phase: domain.PhaseBuild, Attempt: 1}
	result := phaseProviderResult(key, request, providercoord.RoleBuilder)
	builder := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "repaired", ChangedFiles: []string{"internal/feature.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	evidence.results[key.AttemptID] = result
	evidence.parsed[key.AttemptID] = phaseartifact.Parsed{Phase: domain.PhaseBuild, Provider: result.Claim.Binding.Identity, Builder: &builder}
	coordinator.result = providercoord.Result{Code: providercoord.Completed, ProviderResult: key}
	bindCoordinatorResult(t, coordinator, evidence, key, 0)

	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	marker := "CI_REPAIR="
	index := strings.Index(coordinator.request.Input.Prompt, marker)
	if index < 0 {
		t.Fatalf("builder prompt omitted CI repair identity: %s", coordinator.request.Input.Prompt)
	}
	var repair workflowprompt.BuilderRepair
	if err := json.Unmarshal([]byte(strings.TrimSpace(coordinator.request.Input.Prompt[index+len(marker):])), &repair); err != nil {
		t.Fatalf("decode repair prompt: %v", err)
	}
	if repair.Schema != workflowprompt.BuilderRepairSchema || repair.ObservationDigest != evidence.repair.Diagnostic.ObservationDigest || len(repair.FailingChecks) != 1 || repair.FailingChecks[0].Name != "test" || repair.FailingChecks[0].State != "failure" {
		t.Fatalf("repair prompt=%+v", repair)
	}
	if strings.Contains(coordinator.request.Input.Prompt, "lint failed") || strings.Contains(coordinator.request.Input.Prompt, "diagnostic_json") {
		t.Fatal("builder prompt exposed raw CI diagnostic content")
	}
}

func TestPhaseRunnerBuilderRefusesUnauthenticatedRepairContextBeforeProvider(t *testing.T) {
	request, evidence, coordinator, _, _ := phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseBuild, domain.StateBuilding
	evidence.ticket = request.Ticket
	request.Plan, request.Verification = &evidence.plan, &evidence.verify
	evidence.repairErr = store.ErrEvidenceConflict

	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); !errors.Is(err, ErrProviderResultInvalid) {
		t.Fatalf("unauthenticated repair context err=%v", err)
	}
	if coordinator.calls != 0 {
		t.Fatalf("unauthenticated repair context invoked provider: calls=%d", coordinator.calls)
	}
}

func TestPhaseRunnerBuilderRefusesRepairContextForDifferentVerification(t *testing.T) {
	request, evidence, coordinator, _, _ := phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseBuild, domain.StateBuilding
	evidence.ticket = request.Ticket
	request.Plan, request.Verification = &evidence.plan, &evidence.verify
	mismatched := evidence.verify
	mismatched.Revision.IntentDigest = strings.Repeat("0", 64)
	evidence.repair = &store.CandidateRepairBuildContext{Ref: request.Ticket.Ref, Verification: mismatched}

	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); !errors.Is(err, ErrProviderResultInvalid) {
		t.Fatalf("mismatched repair verification err=%v", err)
	}
	if coordinator.calls != 0 {
		t.Fatalf("mismatched repair verification invoked provider: calls=%d", coordinator.calls)
	}
}
