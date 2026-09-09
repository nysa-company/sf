package workflowruntime

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func (m RepositoryMaterializer) PreparePostbuildVerificationAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, key store.ProviderAttemptResultKey) (store.PostbuildAmendmentSnapshot, bool, error) {
	var snapshot store.PostbuildAmendmentSnapshot
	if m.Store == nil {
		return snapshot, false, ErrRepositoryMaterialization
	}
	context, err := m.Store.PostbuildRepairContext(ctx, ref, version, fence)
	if errors.Is(err, store.ErrNotFound) {
		return snapshot, false, nil
	}
	if err != nil {
		return snapshot, true, err
	}
	pending, err := m.Store.PostbuildRepairPendingAmendment(ctx, ref, version, fence)
	if err != nil || pending != key {
		return snapshot, true, ErrRepositoryMaterialization
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest == nil || context.Plan.Document.Planner == nil {
		return snapshot, true, ErrRepositoryMaterialization
	}
	request := workflowworker.PhaseRequest{Worktree: context.Worktree}
	worktree, err := m.worktree(request)
	if err != nil || provider.Claim.Worktree != worktree.Path || provider.Claim.WorktreeIdentity != string(context.Worktree.IdentityJSON) || provider.Claim.BaseSHA != context.Worktree.BaseSHA {
		return snapshot, true, ErrRepositoryMaterialization
	}
	full, err := m.Git.InspectRetainedWorktree(ctx, worktree)
	if err != nil || full.Changes.Head != context.Repair.OriginalCheckpointOID {
		return snapshot, true, ErrRepositoryMaterialization
	}
	for _, path := range full.Changes.Paths {
		declared := false
		for _, item := range parsed.Builder.ChangedFiles {
			declared = declared || item == path
		}
		if !declared || !containsPath(context.Plan.Document.Planner.Paths, path) {
			return snapshot, true, ErrRepositoryMaterialization
		}
	}
	implementation, err := m.Git.InspectRetainedImplementation(ctx, worktree, context.Repair.OriginalCheckpointOID, context.Verification.Revision.OwnedFiles)
	if err != nil {
		return snapshot, true, err
	}
	rechecked, err := m.Git.InspectRetainedWorktree(ctx, worktree)
	if err != nil || rechecked.Digest != full.Digest {
		return snapshot, true, ErrRepositoryMaterialization
	}
	if checked, err := m.Store.PostbuildRepairPendingAmendment(ctx, ref, version, fence); err != nil || checked != key {
		return snapshot, true, ErrRepositoryMaterialization
	}
	protected, err := store.PostbuildAmendmentProtectedPathsDigest(context.Verification.Revision.OwnedFiles)
	if err != nil {
		return snapshot, true, err
	}
	return store.PostbuildAmendmentSnapshot{FullSnapshotDigest: full.Digest, ImplementationDigest: implementation, ProtectedPathsDigest: protected}, true, nil
}
