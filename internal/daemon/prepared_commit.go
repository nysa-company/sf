package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

type lazyPreparedCommitObserver struct {
	runner  func() (git.Runner, error)
	resolve git.RegisteredWorktreeResolver
}

func (observer lazyPreparedCommitObserver) ObservePreparedCommit(ctx context.Context, claim contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error) {
	runner, err := observer.runner()
	if err != nil {
		return contracts.PreparedCommitObservation{}, err
	}
	return (git.PreparedCommitObserver{Runner: runner, Resolve: observer.resolve}).ObservePreparedCommit(ctx, claim)
}

// registeredWorktreeResolver is the production composition for the
// read-only Git observer. It derives every path/branch/base field from the
// immutable claim and accepts only the exact canonical identity JSON already
// registered by the worktree coordinator. Runner.ObserveCommit performs the
// final filesystem/config reauthentication before reading HEAD.
func registeredWorktreeResolver(database *store.Store) git.RegisteredWorktreeResolver {
	return func(ctx context.Context, claim contracts.GitMutationClaim) (git.Worktree, error) {
		if database == nil || claim.Operation != "commit" {
			return git.Worktree{}, errors.New("prepared commit requires a registered worktree store")
		}
		facts, err := database.GitMutationIntentFacts(ctx, claim.SemanticKey)
		if err != nil || facts.Claim != claim || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" {
			return git.Worktree{}, fmt.Errorf("%w: prepared commit claim does not match immutable intent", git.ErrIdentityMismatch)
		}
		registered, err := database.Worktree(ctx, claim.TicketRef)
		if err != nil {
			return git.Worktree{}, err
		}
		// Registration records creation (or refresh), not every phase's fence.
		// These upper bounds reject future provenance; they do not authorize a
		// counter gap. The exact immutable intent above and Store's final
		// ConfirmRecoveredPreparedCommit CAS remain the commit authority.
		if registered.State != "registered" || registered.Path != claim.Worktree || registered.Branch != claim.Branch || registered.BaseSHA != claim.ExpectedBaseOID || registered.TicketVersion > claim.TicketVersion || registered.Fence.LeaderEpoch > claim.LeaderEpoch || registered.Fence.RunnerEpoch > claim.RunnerEpoch {
			return git.Worktree{}, fmt.Errorf("%w: registered worktree does not match immutable commit claim", git.ErrIdentityMismatch)
		}
		var identity git.Identity
		if err := json.Unmarshal(registered.IdentityJSON, &identity); err != nil {
			return git.Worktree{}, fmt.Errorf("%w: registered worktree identity is malformed", git.ErrIdentityMismatch)
		}
		canonical, err := json.Marshal(identity)
		if err != nil || !bytes.Equal(canonical, registered.IdentityJSON) {
			return git.Worktree{}, fmt.Errorf("%w: registered worktree identity is not canonical", git.ErrIdentityMismatch)
		}
		if identity.Repository != claim.Repository || identity.Worktree != claim.Worktree || identity.HeadRef != claim.Branch || identity.BaseRef != claim.BaseRef || identity.BaseHead != claim.ExpectedBaseOID {
			return git.Worktree{}, fmt.Errorf("%w: registered worktree identity does not bind immutable commit claim", git.ErrIdentityMismatch)
		}
		return git.Worktree{Path: registered.Path, Branch: registered.Branch, Identity: identity}, nil
	}
}
