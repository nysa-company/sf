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
		registered, err := database.Worktree(ctx, claim.TicketRef)
		if err != nil {
			return git.Worktree{}, err
		}
		if registered.State != "registered" || registered.Path != claim.Worktree || registered.Branch != claim.Branch || registered.BaseSHA != claim.ExpectedBaseOID || registered.TicketVersion != claim.TicketVersion || registered.Fence.LeaderEpoch != claim.LeaderEpoch || registered.Fence.RunnerEpoch != claim.RunnerEpoch {
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
