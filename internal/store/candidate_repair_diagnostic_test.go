package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestCandidateRepairBuildContextProjectsAuthenticatedRedCIDiagnosticIdentity(t *testing.T) {
	db, waiting, _, observation := redCIConsumptionFixture(t)
	defer db.Close()
	authority := redCICorrectionAuthority(t, waiting, observation)
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{
		Ref:               waiting.Ref,
		ObservationDigest: observation.ObservationDigest,
		ExpectedVersion:   waiting.Version,
		Fence:             observation.ObservedFence,
		CorrectionBudget:  &authority,
	}); err != nil {
		t.Fatal(err)
	}
	building, err := db.Ticket(t.Context(), waiting.Ref)
	if err != nil || building.State != domain.StateBuilding {
		t.Fatalf("building ticket=%+v err=%v", building, err)
	}
	fence := domain.Fence{LeaderEpoch: observation.ObservedFence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	context, err := db.CandidateRepairBuildContext(t.Context(), waiting.Ref, building.Version, fence)
	if err != nil {
		t.Fatal(err)
	}
	if context.Diagnostic.ObservationDigest != observation.ObservationDigest || context.Diagnostic.RequiredSetDigest != observation.RequiredSetDigest || context.Diagnostic.DiagnosticDigest != observation.DiagnosticDigest || context.Diagnostic.TotalFailingChecks != 1 || len(context.Diagnostic.FailingChecks) != 1 {
		t.Fatalf("repair diagnostic=%+v observation=%+v", context.Diagnostic, observation)
	}
	check := context.Diagnostic.FailingChecks[0]
	if check.CanonicalName != "lint" || check.ExternalID != "check-lint" || check.NormalizedState != "failure" || check.FailingDiagnosticDigest != observation.RequiredChecks[0].FailingDiagnosticDigest {
		t.Fatalf("repair check=%+v", check)
	}
	encoded, err := json.Marshal(context.Diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "lint failed") {
		t.Fatal("repair diagnostic projection exposed raw diagnostic text")
	}
}

func TestCandidateRepairDiagnosticProjectionIsSortedAndBounded(t *testing.T) {
	observation := CIObservation{
		Classification:    "red",
		ObservationDigest: "sha256:" + strings.Repeat("a", 64),
		RequiredSetDigest: strings.Repeat("b", 64),
		DiagnosticDigest:  strings.Repeat("c", 64),
	}
	for index := maxCandidateRepairPromptChecks + 3; index > 0; index-- {
		observation.RequiredChecks = append(observation.RequiredChecks, CIObservationCheck{
			CanonicalName:           fmt.Sprintf("check-%02d", index),
			ExternalID:              fmt.Sprintf("run-%02d", index),
			NormalizedState:         "failure",
			FailingDiagnosticDigest: strings.Repeat("d", 64),
			FailingDiagnosticText:   "must never be projected",
		})
	}
	projection, err := candidateRepairDiagnosticForObservation(observation)
	if err != nil {
		t.Fatal(err)
	}
	if projection.TotalFailingChecks != maxCandidateRepairPromptChecks+3 || len(projection.FailingChecks) != maxCandidateRepairPromptChecks {
		t.Fatalf("projection count=%d/%d", projection.TotalFailingChecks, len(projection.FailingChecks))
	}
	for index := 1; index < len(projection.FailingChecks); index++ {
		prior, current := projection.FailingChecks[index-1], projection.FailingChecks[index]
		if prior.CanonicalName+"\x00"+prior.ExternalID >= current.CanonicalName+"\x00"+current.ExternalID {
			t.Fatalf("projection not sorted at %d: %+v then %+v", index, prior, current)
		}
	}
}
