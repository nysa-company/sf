package worktreecoord

import (
	"bytes"
	"context"
	"errors"
	"strings"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

// A completed Builder may leave its bounded source edits before postbuild
// materialization fails or the daemon exits. This is not provider retry: only
// the already authenticated result may be consumed, at the exact refresh HEAD.
// Failed/incomplete providers and ordinary dirty checkouts retain pristine Ensure.
func (c Coordinator) authenticateCompletedRefreshBuilder(ctx context.Context, request EnsureRequest, stored store.StoredWorktree, worktree git.Worktree) (bool, error) {
	ticket, err := c.Store.Ticket(ctx, request.Ref)
	if err != nil {
		return false, err
	}
	if ticket.State != domain.StateBuilding {
		return false, nil
	}
	refresh, err := c.Store.ProtectedBaseRefreshBuildContext(ctx, request.Ref, request.Version, request.Fence)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	reuseRequest := store.LatestReusableProviderAttemptRequest{Ref: request.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: request.Version, Fence: request.Fence}
	result, err := c.Store.LatestReusableProviderAttempt(ctx, reuseRequest)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if result.Parsed.Builder == nil {
		return false, ErrAuthentication
	}
	if result.Parsed.Builder.AmendmentRequest != nil {
		return false, nil
	}
	if result.Result.Claim.ExpectedVersion < refresh.Completion.Version || result.Result.Claim.BaseSHA != stored.BaseSHA || result.Result.Claim.WorktreeIdentity != string(stored.IdentityJSON) || stored.Path != refresh.Completion.Worktree.Path || stored.HeadSHA != refresh.Completion.Preparation.CommitOID || !bytes.Equal(stored.IdentityJSON, refresh.Completion.Worktree.IdentityJSON) {
		return false, ErrAuthentication
	}
	plan, err := c.Store.Plan(ctx, request.Ref)
	if err != nil || plan.Document.Planner == nil {
		return false, ErrAuthentication
	}
	changes, err := c.Git.InspectWorktreeChanges(ctx, worktree)
	if err != nil {
		return false, err
	}
	if !refreshBuilderChangesMatch(changes, refresh.Completion.Preparation.CommitOID, result.Parsed.Builder.ChangedFiles, plan.Document.Planner.Paths, refresh.Verification.Revision.OwnedFiles) {
		return false, ErrUnready
	}
	// Re-prove the immutable result and live authority after observing disk.
	checked, err := c.Store.LatestReusableProviderAttempt(ctx, reuseRequest)
	if err != nil || checked.Key != result.Key || checked.Result.TypedSHA256 != result.Result.TypedSHA256 {
		return false, ErrAuthentication
	}
	if err := c.Store.AssertTicketFence(ctx, request.Ref, request.Version, request.Fence); err != nil {
		return false, err
	}
	return true, nil
}

func refreshBuilderChangesMatch(changes git.WorktreeChanges, head string, declared, allowed, protected []string) bool {
	if head == "" || changes.Head != head || len(allowed) == 0 {
		return false
	}
	within := func(path string, scope []string) bool {
		for _, root := range scope {
			if root != "" && root != "." && (path == root || strings.HasPrefix(path, strings.TrimSuffix(root, "/")+"/")) {
				return true
			}
		}
		return false
	}
	for _, path := range changes.Paths {
		if !within(path, allowed) || within(path, protected) {
			return false
		}
		found := false
		for _, value := range declared {
			if value == path {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
