package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestGenericPostbuildRepairRefusesBeforeAnyPersistence(t *testing.T) {
	// Nil database internals prove refusal occurs before reads, drains or
	// writes, even when a caller supplies all normative guard names.
	_, err := (&Engine{}).Transition(context.Background(), contracts.TransitionRequest{
		From: domain.StateBuilding, Trigger: "postbuild_repair",
		Attributes: map[string]string{"failed_postbuild_evidence_exact": "true", "prepublication_only": "true", "retained_worktree_exact": "true", "correction_available": "true", "no_live_writer": "true"},
	})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("generic engine accepted repair: %v", err)
	}
	_, err = (&store.Store{}).Transition(context.Background(), store.Transition{From: domain.StateBuilding, To: domain.StateBuilding, Trigger: "postbuild_repair", EventPayload: "{}"})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("generic Store accepted repair: %v", err)
	}
}
