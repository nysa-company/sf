package workflowworker

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

var errPostbuildRepairStarted = errors.New("bounded postbuild diagnosis entered")

// StopRejectedPostbuildAmendment is a replayable fail-closed disposition. The
// independent rejection remains immutable; no provider or Git operation runs.
func (w Worker) StopRejectedPostbuildAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (RunResult, error) {
	result := RunResult{Ref: ref, State: domain.StateBuilding, Version: version, Phase: domain.PhaseBuild}
	source, ok := w.Evidence.(interface {
		PostbuildVerificationAmendmentContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildVerificationAmendmentContext, error)
	})
	if !ok || w.Engine == nil {
		return result, store.ErrEvidenceConflict
	}
	proof, err := source.PostbuildVerificationAmendmentContext(ctx, ref, version, fence)
	if err != nil || proof.Decision != store.VerificationAmendmentRejected {
		return result, store.ErrEvidenceConflict
	}
	ticket, err := w.Evidence.Ticket(ctx, ref)
	if err != nil || ticket.State != domain.StateBuilding || ticket.Version != version || ticket.RunnerEpoch != fence.RunnerEpoch {
		return result, store.ErrStaleFence
	}
	transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: version, From: ticket.State, Trigger: "typed_blocker", Fence: fence, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"postbuild_amendment_rejected"}`})
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = transition.To, transition.TicketVersion, true
	return result, nil
}

type postbuildAmendmentPreparer interface {
	PreparePostbuildVerificationAmendment(context.Context, domain.TicketRef, uint64, domain.Fence, store.ProviderAttemptResultKey) (store.PostbuildAmendmentSnapshot, bool, error)
}

type postbuildAmendmentEngine interface {
	SignalPostbuildVerificationAmendmentRequest(context.Context, contracts.SignalRequest, store.ProviderAttemptResultKey, store.PostbuildAmendmentSnapshot) (contracts.TransitionResult, error)
}

// DispatchPostbuildRepairAmendment consumes only the already completed exact
// request. It does not enter the general phase runner or materialize a candidate.
func (w Worker) DispatchPostbuildRepairAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, key store.ProviderAttemptResultKey) (RunResult, error) {
	result := RunResult{Ref: ref, State: domain.StateBuilding, Version: version, Phase: domain.PhaseBuild}
	source, ok := w.Evidence.(interface {
		PostbuildRepairPendingAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (store.ProviderAttemptResultKey, error)
	})
	if !ok {
		return result, store.ErrEvidenceConflict
	}
	if current, err := source.PostbuildRepairPendingAmendment(ctx, ref, version, fence); err != nil || current != key {
		return result, store.ErrEvidenceConflict
	}
	ticket, err := w.Evidence.Ticket(ctx, ref)
	if err != nil || ticket.State != domain.StateBuilding || ticket.Version != version || ticket.RunnerEpoch != fence.RunnerEpoch {
		return result, store.ErrStaleFence
	}
	if err := w.signalVerificationAmendmentRequest(ctx, ticket, fence, key); err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = domain.StateVerifying, version+1, true
	return result, nil
}

type postbuildRepairPreparer interface {
	PreparePostbuildRepair(context.Context, PhaseRequest, store.ProviderAttemptResultKey, contracts.RepositoryCommandResultKey) (store.PostbuildRepairRequest, error)
}

type postbuildRepairEngine interface {
	SignalPostbuildRepair(context.Context, store.PostbuildRepairRequest) (contracts.TransitionResult, error)
}

func (w Worker) tryPostbuildRepair(ctx context.Context, request PhaseRequest, key store.ProviderAttemptResultKey, original error) error {
	var failure *PostbuildFailure
	if !errors.As(original, &failure) {
		return original
	}
	preparer, physicalOK := w.CandidateMaterializer.(postbuildRepairPreparer)
	engine, transitionOK := w.Engine.(postbuildRepairEngine)
	if !physicalOK || !transitionOK {
		return original
	}
	repair, err := preparer.PreparePostbuildRepair(ctx, request, key, failure.CommandResult)
	if err != nil {
		return original
	}
	if _, err := engine.SignalPostbuildRepair(ctx, repair); err != nil {
		return original
	}
	return errPostbuildRepairStarted
}
