package localruntime

import (
	"context"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// DispatchPostbuildRepairAmendment consumes only an exact already-completed
// amendment request. It never invokes the general Worker, Git, or a provider;
// the resulting Verifying entry requires its own physical admission.
func (w Worker) DispatchPostbuildRepairAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, key store.ProviderAttemptResultKey) (workflowworker.RunResult, error) {
	if w.Store == nil || w.Engine == nil {
		return workflowworker.RunResult{Ref: ref}, workflowworker.ErrUnsupportedState
	}
	return dispatchPostbuildRepairAmendment(ctx, ref, version, fence, key, w.Store, w.Engine)
}

type postbuildAmendmentStore interface {
	Ticket(context.Context, domain.TicketRef) (store.Ticket, error)
	RuntimeAdmissionReady(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error)
	PostbuildRepairPendingAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (store.ProviderAttemptResultKey, error)
	AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error
}

type postbuildAmendmentEngine interface {
	SignalVerificationAmendmentRequest(context.Context, contracts.SignalRequest, store.ProviderAttemptResultKey) (contracts.TransitionResult, error)
}

func dispatchPostbuildRepairAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, key store.ProviderAttemptResultKey, database postbuildAmendmentStore, engine postbuildAmendmentEngine) (workflowworker.RunResult, error) {
	result := workflowworker.RunResult{Ref: ref, Version: version, Phase: domain.PhaseBuild}
	if ctx == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 || key.Ref != ref || key.Phase != domain.PhaseBuild || key.AttemptID <= 0 || key.Attempt <= 0 {
		return result, store.ErrEvidenceConflict
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	ticket, err := database.Ticket(ctx, ref)
	if err != nil {
		return result, err
	}
	result.State = ticket.State
	if ticket.Ref != ref || ticket.State != domain.StateBuilding || ticket.Version != version || ticket.RunnerEpoch != fence.RunnerEpoch {
		return result, store.ErrStaleFence
	}
	ready, err := database.RuntimeAdmissionReady(ctx, ref, version, fence)
	if err != nil {
		return result, err
	}
	if !ready {
		return result, store.ErrControlNotDrained
	}
	current, err := database.PostbuildRepairPendingAmendment(ctx, ref, version, fence)
	if err != nil {
		return result, err
	}
	if current != key {
		return result, store.ErrEvidenceConflict
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := database.AssertTicketFence(ctx, ref, version, fence); err != nil {
		return result, err
	}
	transition, err := engine.SignalVerificationAmendmentRequest(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: version, From: domain.StateBuilding, Fence: fence, EventPayload: "{}"}, key)
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = transition.To, transition.TicketVersion, true
	return result, nil
}
