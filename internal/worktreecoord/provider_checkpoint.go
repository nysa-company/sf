package worktreecoord

import (
	"context"
	"reflect"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// ProviderCheckpointInspection is a trusted composition result, not a signed
// retry capability. The supervisor must still own and drain the exact process;
// Store must recheck claim authority when committing its signed observation.
type ProviderCheckpointInspection struct {
	Checkpoint store.ProviderAttemptCheckpoint
	Digest     string
}

var _ contracts.RejectionCheckpointInspector = Coordinator{}

// InspectRejectionCheckpoint implements the supervisor's trusted inspection
// contract. The minimal local projection is only used to check returned
// metadata; both Store lookups authenticate the original complete request.
func (c Coordinator) InspectRejectionCheckpoint(ctx context.Context, request contracts.DrainRequest) (string, string, error) {
	if c.Store == nil {
		return "", "", ErrAuthentication
	}
	claim := store.ProviderAttemptClaim{ID: request.ClaimID, Ref: request.Ref, Phase: request.Phase,
		ExpectedVersion: request.ExpectedVersion, LeaderEpoch: request.LeaderEpoch, RunnerEpoch: request.RunnerEpoch,
		Worktree: request.Worktree, RequestDigest: request.RequestDigest}
	value, err := inspectProviderAttemptCheckpoint(ctx, claim, func(ctx context.Context, _ store.ProviderAttemptClaim) (store.ProviderAttemptCheckpoint, error) {
		return c.Store.ProviderAttemptCheckpointForRequest(ctx, request)
	}, c.AuthenticateExistingRegisteredWorktree)
	if err != nil {
		return "", "", err
	}
	return value.Checkpoint.ExpectedHead, value.Digest, nil
}

// InspectProviderAttemptCheckpoint never creates, repairs, stages or cleans a
// checkout. Both sides of physical inspection authenticate the same still-active
// claim. Provider writes (including ignored paths) or control/fence movement
// refuse. This relies on SF writer exclusion, not hostile same-UID containment.
func (c Coordinator) InspectProviderAttemptCheckpoint(ctx context.Context, claim store.ProviderAttemptClaim) (ProviderCheckpointInspection, error) {
	if c.Store == nil {
		return ProviderCheckpointInspection{}, ErrAuthentication
	}
	return inspectProviderAttemptCheckpoint(ctx, claim, c.Store.ProviderAttemptCheckpoint, c.AuthenticateExistingRegisteredWorktree)
}

func inspectProviderAttemptCheckpoint(ctx context.Context, claim store.ProviderAttemptClaim,
	load func(context.Context, store.ProviderAttemptClaim) (store.ProviderAttemptCheckpoint, error),
	inspect func(context.Context, domain.TicketRef, string) (store.StoredWorktree, error),
) (ProviderCheckpointInspection, error) {
	if ctx == nil || load == nil || inspect == nil {
		return ProviderCheckpointInspection{}, ErrAuthentication
	}
	if err := ctx.Err(); err != nil {
		return ProviderCheckpointInspection{}, err
	}
	before, err := load(ctx, claim)
	if err != nil {
		return ProviderCheckpointInspection{}, err
	}
	if before.AttemptID != claim.ID || before.Ref != claim.Ref || before.Phase != claim.Phase || before.Version != claim.ExpectedVersion || before.Fence.LeaderEpoch != claim.LeaderEpoch || before.Fence.RunnerEpoch != claim.RunnerEpoch || before.Worktree.Path != claim.Worktree || !validFullOID(before.ExpectedHead) {
		return ProviderCheckpointInspection{}, ErrAuthentication
	}
	observed, err := inspect(ctx, claim.Ref, before.ExpectedHead)
	if err != nil {
		return ProviderCheckpointInspection{}, err
	}
	if !reflect.DeepEqual(observed, before.Worktree) {
		return ProviderCheckpointInspection{}, ErrAuthentication
	}
	after, err := load(ctx, claim)
	if err != nil {
		return ProviderCheckpointInspection{}, err
	}
	if !reflect.DeepEqual(before, after) {
		return ProviderCheckpointInspection{}, ErrAuthentication
	}
	if err := ctx.Err(); err != nil {
		return ProviderCheckpointInspection{}, err
	}
	// Domain-separated canonical metadata contains no file contents/provider text.
	// Bind the attempt request as well as the full registered identity and HEAD.
	digest, err := store.ProviderAttemptCheckpointDigest(after, claim.RequestDigest)
	if err != nil {
		return ProviderCheckpointInspection{}, ErrAuthentication
	}
	return ProviderCheckpointInspection{Checkpoint: after, Digest: digest}, nil
}
