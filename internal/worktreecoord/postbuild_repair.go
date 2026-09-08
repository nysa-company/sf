package worktreecoord

import (
	"context"
	"reflect"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

// AuthenticatePostbuildRepair admits only the exact retained snapshot at the
// initial postbuild repair entry. It neither creates a checkout nor changes the
// pristine Ensure contract. Store must still exclude writers at actual launch.
// Once a fresh Builder has launched, this initial snapshot is not reusable.
func (c Coordinator) AuthenticatePostbuildRepair(ctx context.Context, request EnsureRequest) (store.StoredWorktree, error) {
	return c.authenticatePostbuildRepairMode(ctx, request, false)
}

// AuthenticateCompletedPostbuildRepair admits materialization of an exact fresh
// completed Builder result. Its changed bytes are no longer the initial repair
// snapshot; the completed result, unchanged checkpoint and protected scope are
// the authority. No incomplete attempt may use this path.
func (c Coordinator) AuthenticateCompletedPostbuildRepair(ctx context.Context, request EnsureRequest) (store.StoredWorktree, error) {
	return c.authenticatePostbuildRepairMode(ctx, request, true)
}

func (c Coordinator) authenticatePostbuildRepairMode(ctx context.Context, request EnsureRequest, completed bool) (store.StoredWorktree, error) {
	if c.Store == nil || request.Ref.Validate() != nil || request.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 {
		return store.StoredWorktree{}, ErrAuthentication
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	project, err := c.Store.Project(ctx, request.Ref.Channel, request.Ref.Project)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	path, err := c.Store.TicketWorktreePath(request.Ref)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	load := func(ctx context.Context) (store.PostbuildRepairBuildContext, error) {
		if completed {
			return c.Store.PostbuildRepairCompletedBuildContext(ctx, request.Ref, request.Version, request.Fence)
		}
		return c.Store.PostbuildRepairBuildContext(ctx, request.Ref, request.Version, request.Fence)
	}
	assert := func(ctx context.Context) error {
		return c.Store.AssertTicketFence(ctx, request.Ref, request.Version, request.Fence)
	}
	return authenticatePostbuildRepair(ctx, request, project, path, load, c.Git.InspectRetainedWorktree, assert, completed)
}

// The private seams exercise ordering and late revocation without constructing
// another Store authority. Production always supplies the authenticated reader.
func authenticatePostbuildRepair(ctx context.Context, request EnsureRequest, project store.Project, path string,
	load func(context.Context) (store.PostbuildRepairBuildContext, error),
	inspect func(context.Context, git.Worktree) (git.RetainedWorktreeInspection, error),
	assert func(context.Context) error,
	completed bool,
) (store.StoredWorktree, error) {
	if err := ctx.Err(); err != nil {
		return store.StoredWorktree{}, err
	}
	proof, err := load(ctx)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	worktree, err := postbuildRepairWorktree(request, project, path, proof, completed)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	observed, err := inspect(ctx, worktree)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	if (!completed && observed.Digest != proof.Repair.RetainedWorktreeDigest) || observed.Digest == "" || !refreshBuilderChangesMatch(observed.Changes, proof.Repair.OriginalCheckpointOID, proof.BuilderArtifact.ChangedFiles, proof.Plan.Document.Planner.Paths, proof.Verification.Revision.OwnedFiles) {
		return store.StoredWorktree{}, ErrUnready
	}
	checked, err := load(ctx)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	if !reflect.DeepEqual(proof, checked) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	if completed {
		// There is no predecessor byte digest for the fresh result. Require the
		// complete physical snapshot to remain identical across authority reload.
		after, err := inspect(ctx, worktree)
		if err != nil {
			return store.StoredWorktree{}, err
		}
		if !reflect.DeepEqual(observed, after) {
			return store.StoredWorktree{}, ErrUnready
		}
		checked, err = load(ctx)
		if err != nil {
			return store.StoredWorktree{}, err
		}
		if !reflect.DeepEqual(proof, checked) {
			return store.StoredWorktree{}, ErrAuthentication
		}
	}
	if err := ctx.Err(); err != nil {
		return store.StoredWorktree{}, err
	}
	if err := assert(ctx); err != nil {
		return store.StoredWorktree{}, err
	}
	return proof.Worktree, nil
}

func postbuildRepairWorktree(request EnsureRequest, project store.Project, path string, proof store.PostbuildRepairBuildContext, completed bool) (git.Worktree, error) {
	stored, repair, builder := proof.Worktree, proof.Repair, proof.Builder
	if repair.Ref != request.Ref || repair.EntryVersion == 0 || repair.EntryVersion > request.Version || repair.ExpectedVersion != repair.EntryVersion-1 || repair.BuilderResult.Ref != request.Ref || repair.BuilderResult.Phase != domain.PhaseBuild || repair.BuilderResult.AttemptID <= 0 || builder.Claim.ID != builder.AttemptID || builder.Claim.Ref != request.Ref || builder.Claim.Phase != domain.PhaseBuild || builder.Claim.Role != "builder" || proof.BuilderArtifact.AmendmentRequest != nil || proof.Plan.Document.Planner == nil || len(proof.Plan.Document.Planner.Paths) == 0 {
		return git.Worktree{}, ErrAuthentication
	}
	if completed {
		if builder.AttemptID <= repair.BuilderResult.AttemptID || builder.Claim.Attempt <= repair.BuilderResult.Attempt || builder.Claim.ExpectedVersion < repair.EntryVersion || builder.Claim.ExpectedVersion > request.Version || builder.TypedSHA256 == "" {
			return git.Worktree{}, ErrAuthentication
		}
	} else if repair.BuilderResult.AttemptID != builder.AttemptID || builder.TypedSHA256 != repair.BuilderTypedDigest {
		return git.Worktree{}, ErrAuthentication
	}
	if !validFullOID(repair.OriginalCheckpointOID) || repair.RetainedWorktreeDigest == "" || repair.BindingDigest == "" || proof.Verification.Checkpoint.CommitOID != repair.OriginalCheckpointOID || proof.Verification.Revision.CheckpointID != repair.OriginalCheckpointOID || proof.Verification.Revision.Revision != repair.VerificationRevision || !reflect.DeepEqual(proof.Verification.Revision.OwnedFiles, repair.Verification.Revision.OwnedFiles) {
		return git.Worktree{}, ErrAuthentication
	}
	if project.Channel != request.Ref.Channel || project.ID != request.Ref.Project || stored.State != "registered" || stored.Path != path || stored.Branch == "" || !validFullOID(stored.BaseSHA) || !validFullOID(stored.HeadSHA) {
		return git.Worktree{}, ErrAuthentication
	}
	worktree, identity, err := decodeWorktree(stored.Path, stored.Branch, stored.IdentityJSON)
	if err != nil || !sameIdentityJSON(stored.IdentityJSON, identity) || identity.Repository != project.Path || identity.Worktree != path || identity.HeadRef != stored.Branch || identity.BaseRef != project.BaseRef || identity.BaseHead != stored.BaseSHA || builder.Claim.Repository != project.Path || builder.Claim.Worktree != path || builder.Claim.WorktreeIdentity != string(stored.IdentityJSON) || builder.Claim.BaseSHA != stored.BaseSHA {
		return git.Worktree{}, ErrAuthentication
	}
	return worktree, nil
}
