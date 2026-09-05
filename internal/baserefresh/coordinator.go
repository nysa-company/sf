// Package baserefresh composes a Store-reserved protected-base refresh with
// exact Git proof, prepared-object application and fresh Builder admission.
package baserefresh

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

// Coordinator only composes the three existing authorities: Store
// derives an immutable reservation, Git authenticates and applies its prepared
// object, and Store admits the fresh Builder. It does not choose a remote tip,
// adopt a PR, approve a merge, or permit generic worktree repair.
type Coordinator struct {
	Store *store.Store
	Git   baseRefreshGit
}

type baseRefreshGit interface {
	VerifyExactProtectedBase(context.Context, contracts.ProtectedBranchWitness) error
	PrepareProtectedBaseRefresh(context.Context, []byte, contracts.GitMutationClaim) (git.BaseRefreshPreparation, error)
	ApplyProtectedBaseRefresh(context.Context, []byte, contracts.GitMutationClaim) (git.BaseRefreshApplied, error)
}

func refreshEffectFence(claim contracts.GitMutationClaim) store.EffectFence {
	return store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion,
		Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}
}

func (c Coordinator) uncertain(ctx context.Context, claim contracts.GitMutationClaim, cause error) error {
	persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err := c.Store.MarkEffectUncertain(persist, refreshEffectFence(claim))
	return errors.Join(cause, err)
}

// Refresh also resumes an existing reservation without substituting the
// caller's proposed tip. A further remote advance is refused by Git's exact
// check; it never silently consumes a second refresh budget.
func (c Coordinator) Refresh(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, newBase string) (store.ProtectedBaseRefreshCompletion, error) {
	if c.Store == nil || c.Git == nil || ctx == nil {
		return store.ProtectedBaseRefreshCompletion{}, store.ErrEvidenceConflict
	}
	reservation, found, err := c.Store.PendingProtectedBaseRefresh(ctx, ref, version, fence)
	if err != nil {
		return store.ProtectedBaseRefreshCompletion{}, err
	}
	if !found {
		proposal, err := c.Store.ProtectedBaseRefreshProofIntent(ctx, ref, version, fence, newBase)
		if err != nil {
			return store.ProtectedBaseRefreshCompletion{}, err
		}
		intent := proposal.Intent
		effect, err := c.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, TicketVersion: version, Fence: fence, Kind: "git/protected-ref-fetch", RequestDigest: intent.RequestDigest})
		if err != nil {
			return store.ProtectedBaseRefreshCompletion{}, err
		}
		if effect.State != store.EffectConfirmed {
			var claim contracts.GitMutationClaim
			switch effect.State {
			case store.EffectPlanned:
				claim, err = c.Store.IssueGitMutationClaim(ctx, intent)
			case store.EffectUncertain:
				// The Store permits only an exact same-endpoint private-ref
				// proof retry after proving all repository writers drained.
				claim, err = c.Store.ReclaimProtectedBaseRefreshProof(ctx, ref, version, fence, newBase)
			default:
				return store.ProtectedBaseRefreshCompletion{}, store.ErrGitMutationIntent
			}
			if err != nil {
				return store.ProtectedBaseRefreshCompletion{}, err
			}
			var identity git.Identity
			if err := json.Unmarshal(proposal.Worktree.IdentityJSON, &identity); err != nil {
				return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
			}
			witness := contracts.ProtectedBranchWitness{Repository: intent.Repository, Worktree: intent.Worktree, Origin: identity.Origin, ProtectedRef: intent.BaseRef, OriginalBaseOID: intent.ExpectedBaseOID, MergeOID: intent.ExpectedHeadOID, MutationClaim: claim}
			if err := c.Git.VerifyExactProtectedBase(ctx, witness); err != nil {
				return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
			}
			persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			_, err = c.Store.ConfirmEffect(persist, refreshEffectFence(claim), proposal.ObservedIdentity)
			cancel()
			if err != nil {
				return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
			}
		}
		// This writer independently authenticates all confirmed proof fields.
		reservation, err = c.Store.ReserveProtectedBaseRefresh(ctx, ref, version, fence, newBase)
		if err != nil {
			return store.ProtectedBaseRefreshCompletion{}, err
		}
	}
	return c.apply(ctx, reservation)
}

func (c Coordinator) apply(ctx context.Context, reservation store.ProtectedBaseRefresh) (store.ProtectedBaseRefreshCompletion, error) {
	intent := reservation.Mutation
	effect, err := c.Store.Effect(ctx, intent.SemanticKey)
	if err != nil {
		return store.ProtectedBaseRefreshCompletion{}, err
	}
	var claim contracts.GitMutationClaim
	if effect.State == store.EffectPlanned {
		// A crash may leave the atomic reservation's unlaunched effect at an
		// older fence. PlanEffect is the existing no-launch rebind authority;
		// Issue additionally proves the reservation's signed recovery chain.
		if _, err := c.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, TicketVersion: intent.TicketVersion, Fence: intent.Fence, Kind: "git/refresh-base", RequestDigest: intent.RequestDigest}); err != nil {
			return store.ProtectedBaseRefreshCompletion{}, err
		}
		claim, err = c.Store.IssueGitMutationClaim(ctx, intent)
	} else {
		claim, err = c.Store.ReclaimProtectedBaseRefresh(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
	}
	if err != nil {
		return store.ProtectedBaseRefreshCompletion{}, err
	}
	facts, err := c.Store.GitMutationIntentFacts(ctx, intent.SemanticKey)
	if err != nil {
		return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
	}
	if facts.PreparedCommitOID == "" {
		if _, err := c.Git.PrepareProtectedBaseRefresh(ctx, reservation.IntentPayload, claim); err != nil {
			return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
		}
	}
	applied, err := c.Git.ApplyProtectedBaseRefresh(ctx, reservation.IntentPayload, claim)
	if err != nil {
		return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
	}
	identity, err := json.Marshal(applied.Identity)
	if err != nil {
		return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
	}
	persist, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := c.Store.ConfirmEffect(persist, refreshEffectFence(claim), applied.Preparation.CommitOID); err != nil {
		return store.ProtectedBaseRefreshCompletion{}, c.uncertain(ctx, claim, err)
	}
	return c.Store.CompleteProtectedBaseRefresh(persist, claim, identity)
}
