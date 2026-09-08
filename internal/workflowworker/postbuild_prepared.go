package workflowworker

import (
	"context"
	"reflect"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// ResumePreparedPostbuildAmendment can only consume an existing accepted
// Reviewer. Unlike Run/verifyingAmendment it has no fallback to a new provider.
// The materializer must independently prove or finish the exact checkpoint.
func (w Worker) ResumePreparedPostbuildAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (RunResult, error) {
	result := RunResult{Ref: ref, State: domain.StateVerifying, Version: version, Phase: domain.PhaseVerification, Replayed: true}
	source, ok := w.Evidence.(interface {
		PostbuildVerificationAmendmentContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildVerificationAmendmentContext, error)
		PostbuildAmendmentCheckpointSnapshot(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildAmendmentCheckpointSnapshot, error)
		PostbuildAmendmentPreparedCheckpoint(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CommitObservation, bool, error)
	})
	if !ok || w.Engine == nil || ctx == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return result, store.ErrEvidenceConflict
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	ticket, err := w.Evidence.Ticket(ctx, ref)
	if err != nil || ticket.Ref != ref || ticket.Version != version || ticket.State != domain.StateVerifying || ticket.RunnerEpoch != fence.RunnerEpoch {
		return result, store.ErrStaleFence
	}
	proof, err := source.PostbuildVerificationAmendmentContext(ctx, ref, version, fence)
	if err != nil {
		return result, err
	}
	receipt, err := source.PostbuildAmendmentCheckpointSnapshot(ctx, ref, version, fence)
	if err != nil || receipt.CompanionBindingDigest != proof.Binding.BindingDigest {
		return result, store.ErrEvidenceConflict
	}
	prepared, found, err := source.PostbuildAmendmentPreparedCheckpoint(ctx, ref, version, fence)
	if err != nil || !found || prepared.ParentOID != proof.Repair.OriginalCheckpointOID {
		return result, store.ErrEvidenceConflict
	}
	reusable, err := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: version, Fence: fence})
	if err != nil || reusable.Key != receipt.Reviewer || reusable.Result.Claim.ExpectedVersion < proof.Amendment.TransitionTicketVersion || reusable.Parsed.Verify == nil {
		return result, store.ErrEvidenceConflict
	}
	decision, err := w.Evidence.VerificationAmendmentDecision(ctx, ref, version, fence, reusable.Key)
	if err != nil || decision != store.VerificationAmendmentAccepted {
		return result, store.ErrEvidenceConflict
	}
	planIdentity, err := w.storedPlanIdentity(ctx, ticket, proof.Plan, fence)
	if err != nil {
		return result, err
	}
	checked, err := source.PostbuildAmendmentCheckpointSnapshot(ctx, ref, version, fence)
	if err != nil || !reflect.DeepEqual(checked, receipt) {
		return result, store.ErrEvidenceConflict
	}
	if err := w.Evidence.AssertTicketFence(ctx, ref, version, fence); err != nil {
		return result, err
	}
	transitioned, replayed, err := w.resolveVerificationAmendment(ctx, ticket, fence, proof.Plan, planIdentity, proof.Amendment, reusable.Key, reusable.Result, reusable.Parsed, true)
	if err != nil {
		return result, err
	}
	current, err := w.Evidence.Ticket(ctx, ref)
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned, result.Replayed = current.State, current.Version, transitioned, replayed
	return result, nil
}
