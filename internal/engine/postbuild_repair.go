package engine

import (
	"context"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// SignalPostbuildRepair delegates authority to the atomic Store boundary.
// Guard names describe its obligations; caller-supplied booleans cannot mint
// this transition through the generic Signal or Transition paths.
func (e *Engine) SignalPostbuildRepair(ctx context.Context, request store.PostbuildRepairRequest) (contracts.TransitionResult, error) {
	attributes := map[string]string{
		"failed_postbuild_evidence_exact": "true",
		"prepublication_only":             "true",
		"retained_worktree_exact":         "true",
		"correction_available":            "true",
		"no_live_writer":                  "true",
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ref, TicketVersion: request.ExpectedVersion, From: domain.StateBuilding, Trigger: "postbuild_repair", Fence: request.Fence, Attributes: attributes, EventPayload: "{}"}, func(ctx context.Context, transition store.Transition) (store.TransitionResult, error) {
		return e.store.TransitionPostbuildRepair(ctx, request)
	})
}

func (e *Engine) SignalPostbuildVerificationAmendmentRequest(ctx context.Context, request contracts.SignalRequest, key store.ProviderAttemptResultKey, snapshot store.PostbuildAmendmentSnapshot) (contracts.TransitionResult, error) {
	if request.From != domain.StateBuilding || key.Ref != request.Ticket || key.Phase != domain.PhaseBuild || request.EventPayload != "{}" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: "verification_amendment_requested", Fence: request.Fence, Attributes: map[string]string{"amendment_request_valid": "true", "correction_available": "true"}, EventPayload: "{}"}, func(ctx context.Context, transition store.Transition) (store.TransitionResult, error) {
		return e.store.TransitionPostbuildVerificationAmendmentRequest(ctx, transition, key, snapshot)
	})
}
