package worktreecoord

import (
	"context"
	"reflect"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type preparedAmendmentSource interface {
	postbuildAmendmentSource
	PostbuildAmendmentCheckpointSnapshot(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildAmendmentCheckpointSnapshot, error)
}

// AuthenticatePreparedPostbuildAmendment admits only the dedicated completion
// method, never a fresh provider. Protected index synchronization remains a
// separate leased Git operation that must verify the exact prepared tree.
func (c Coordinator) AuthenticatePreparedPostbuildAmendment(ctx context.Context, request EnsureRequest) (store.StoredWorktree, error) {
	if c.Store == nil {
		return store.StoredWorktree{}, ErrAuthentication
	}
	return authenticatePreparedPostbuildAmendment(ctx, request, c.Store, c.Git)
}

func authenticatePreparedPostbuildAmendment(ctx context.Context, request EnsureRequest, source preparedAmendmentSource, inspector postbuildAmendmentInspector) (store.StoredWorktree, error) {
	if request.Ref.Validate() != nil || request.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 {
		return store.StoredWorktree{}, ErrAuthentication
	}
	ctx, cancel := context.WithTimeout(ctx, postbuildAmendmentAdmissionTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return store.StoredWorktree{}, err
	}
	proof, err := source.PostbuildVerificationAmendmentContext(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || proof.Decision != "" {
		return store.StoredWorktree{}, ErrAuthentication
	}
	prepared, found, err := source.PostbuildAmendmentPreparedCheckpoint(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !found || prepared.ParentOID != proof.Binding.OriginalCheckpointOID || !validFullOID(prepared.CommitOID) || !validFullOID(prepared.TreeOID) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	receipt, err := source.PostbuildAmendmentCheckpointSnapshot(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || receipt.Ref != request.Ref || receipt.AmendmentTransitionVersion != proof.Amendment.TransitionTicketVersion || receipt.CompanionBindingDigest != proof.Binding.BindingDigest || receipt.ImplementationDigest != proof.Snapshot.ImplementationDigest || receipt.FullSnapshotDigest == "" {
		return store.StoredWorktree{}, ErrAuthentication
	}
	reuseRequest := store.LatestReusableProviderAttemptRequest{Ref: request.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: request.Version, Fence: request.Fence}
	reviewer, err := source.LatestReusableProviderAttempt(ctx, reuseRequest)
	if err != nil || reviewer.Key != receipt.Reviewer || reviewer.Result.Claim.ExpectedVersion < proof.Amendment.TransitionTicketVersion || reviewer.Parsed.Verify == nil || !reflect.DeepEqual(reviewer.Parsed.Verify.OwnedFiles, proof.Verification.Revision.OwnedFiles) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	project, err := source.Project(ctx, request.Ref.Channel, request.Ref.Project)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	path, err := source.TicketWorktreePath(request.Ref)
	if err != nil || proof.Worktree.Path != path || proof.Worktree.State != "registered" {
		return store.StoredWorktree{}, ErrAuthentication
	}
	worktree, identity, err := decodeWorktree(path, proof.Worktree.Branch, proof.Worktree.IdentityJSON)
	if err != nil || project.Channel != request.Ref.Channel || project.ID != request.Ref.Project || !sameIdentityJSON(proof.Worktree.IdentityJSON, identity) || identity.Worktree != path || identity.HeadRef != proof.Worktree.Branch || identity.Repository != project.Path || identity.BaseRef != project.BaseRef || identity.BaseHead != proof.Worktree.BaseSHA || reviewer.Result.Claim.Repository != project.Path || reviewer.Result.Claim.Worktree != path || reviewer.Result.Claim.WorktreeIdentity != string(proof.Worktree.IdentityJSON) || reviewer.Result.Claim.BaseSHA != proof.Worktree.BaseSHA {
		return store.StoredWorktree{}, ErrAuthentication
	}
	observed, err := inspector.InspectRetainedWorktree(ctx, worktree)
	if err != nil || observed.Changes.Head != prepared.CommitOID && observed.Changes.Head != prepared.ParentOID {
		return store.StoredWorktree{}, ErrUnready
	}
	if observed.Changes.Head == prepared.ParentOID && observed.Digest != receipt.FullSnapshotDigest {
		return store.StoredWorktree{}, ErrUnready
	}
	implementation, err := inspector.InspectRetainedImplementation(ctx, worktree, prepared.ParentOID, proof.Verification.Revision.OwnedFiles)
	if err != nil || implementation != receipt.ImplementationDigest {
		return store.StoredWorktree{}, ErrUnready
	}
	checked, err := source.PostbuildVerificationAmendmentContext(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !reflect.DeepEqual(checked, proof) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	childAgain, found, err := source.PostbuildAmendmentPreparedCheckpoint(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !found || childAgain != prepared {
		return store.StoredWorktree{}, ErrAuthentication
	}
	receiptAgain, err := source.PostbuildAmendmentCheckpointSnapshot(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !reflect.DeepEqual(receiptAgain, receipt) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	reviewerAgain, err := source.LatestReusableProviderAttempt(ctx, reuseRequest)
	if err != nil || !reflect.DeepEqual(reviewerAgain, reviewer) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	again, err := inspector.InspectRetainedWorktree(ctx, worktree)
	if err != nil || !reflect.DeepEqual(again, observed) {
		return store.StoredWorktree{}, ErrUnready
	}
	if err := ctx.Err(); err != nil {
		return store.StoredWorktree{}, err
	}
	if err := source.AssertTicketFence(ctx, request.Ref, request.Version, request.Fence); err != nil {
		return store.StoredWorktree{}, err
	}
	return proof.Worktree, nil
}
