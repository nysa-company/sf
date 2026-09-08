package localruntime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// BaseRefreshCoordinator is a typed, Store-authenticated operation, not a
// general Git runner. Production injects baserefresh.Coordinator; integration
// tests can exercise dispatch without widening the native argv/stdin seam.
type BaseRefreshCoordinator interface {
	Refresh(context.Context, domain.TicketRef, uint64, domain.Fence, string) (store.ProtectedBaseRefreshCompletion, error)
}

// refreshProtectedBase runs before another publication/review action. Remote
// failures remain errors, not an inferred base advance. Once a reservation
// exists it takes precedence over ordinary checkout setup and is resumed only
// through the constrained Git refresh adapter.
func (w Worker) refreshProtectedBase(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, bool, error) {
	result := workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version}
	if !w.BaseRefreshEnabled || !w.PublicationEnabled || ticket.MergeMode != domain.MergeGuarded || ticket.Type == domain.TicketSpike {
		return result, false, nil
	}
	switch ticket.State {
	case domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing, domain.StateWaitingApproval:
	default:
		return result, false, nil
	}
	if w.BaseRefresh == nil {
		return result, true, ErrPublishingUnavailable
	}
	pending, found, err := w.Store.PendingProtectedBaseRefresh(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		return result, true, err
	}
	worktree, err := w.Store.Worktree(ctx, ticket.Ref)
	if err != nil {
		return result, true, err
	}
	var identity git.Identity
	if json.Unmarshal(worktree.IdentityJSON, &identity) != nil {
		return result, true, store.ErrEvidenceConflict
	}
	remote := git.PublicationRemoteObservation{BaseOID: pending.NewBaseSHA}
	if !found {
		remote, err = w.Publication.Git.ObservePublicationRemote(ctx, git.Worktree{Path: worktree.Path, Branch: worktree.Branch, Identity: identity})
		if err != nil {
			return result, true, err
		}
		if remote.BaseOID == worktree.BaseSHA {
			return result, false, nil
		}
	}
	// With a pending reservation, physical Git may already be at the exact
	// new identity. Its leased Apply adapter must authenticate old/new states
	// and remote refs; the ordinary old-identity observer cannot precede it.
	// A numbered published PR must still be the exact owned, unmerged open
	// source. BaseOID may advance; its equality to the old base is not required.
	// Without publication evidence Store separately requires zero PR effects,
	// so an effect-before-witness crash cannot be guessed into a rewrite.
	publication, err := w.Store.LoadHistoricalPublishedCandidate(ctx, ticket.Ref)
	if err == nil {
		if !found && remote.Candidate.OID != publication.PullRequest.HeadOID {
			return result, true, store.ErrPublicationEvidence
		}
		observer, ok := w.Publication.GitHub.(interface {
			ObservePublishedPullRequest(context.Context, contracts.PullRequestIdentity) (contracts.PublishedPullRequestObservation, error)
		})
		if !ok {
			return result, true, ErrPublishingUnavailable
		}
		observed, err := observer.ObservePublishedPullRequest(ctx, publication.PullRequest)
		if err != nil {
			return result, true, err
		}
		// GitHub can retain the PR's original base snapshot after the branch
		// advances. Accept that authenticated old snapshot or the exact fresh
		// remote tip, never an unrelated third value. The new base authority
		// still comes from the independent Git proof and leased refresh CAS.
		baseMatches := observed.Identity.BaseOID == publication.PullRequest.BaseOID || observed.Identity.BaseOID == remote.BaseOID
		if !samePublishedIdentity(observed.Identity, publication.PullRequest) || observed.State != "OPEN" || observed.Merged || observed.MergeCommit != "" || !baseMatches {
			return result, true, store.ErrPublicationEvidence
		}
	} else {
		if !errors.Is(err, store.ErrNotFound) {
			return result, true, err
		}
		// Before the first publication witness there is no authenticated
		// numbered PR/prior-push target to carry into the next generation.
		// A nonempty hosted branch must reconcile first, never be overwritten
		// merely because the local candidate shares its name.
		if !found && remote.Candidate.OID != "" {
			return result, true, store.ErrPublicationEvidence
		}
	}
	completed, err := w.BaseRefresh.Refresh(ctx, ticket.Ref, ticket.Version, fence, remote.BaseOID)
	if err != nil {
		return result, true, err
	}
	result.State, result.Version, result.Transitioned = domain.StateBuilding, completed.Version, true
	return result, true, nil
}
