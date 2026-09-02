// Package mergeproof composes GitHub's exact merged-PR observation with the
// separate Store-issued Git protected-ref proof.  GitHub never receives a Git
// mutation claim and Git never decides which PR may merge.
package mergeproof

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

type Coordinator struct {
	Store *store.Store
	Git   protectedBranchGit
}

var _ contracts.MergeBranchVerifier = Coordinator{}

type protectedBranchGit interface {
	Snapshot(context.Context, string, string) (git.Identity, error)
	VerifyProtectedBranch(context.Context, contracts.ProtectedBranchWitness) error
}

// VerifyProtectedBranch is called by the GitHub boundary only after GitHub
// has observed the exact source head as merged.  It locates that one durable
// merge intent, reserves a child protected-ref-fetch effect, and proves both
// ancestry and containment through git.Runner.
func (c Coordinator) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
	if c.Store == nil || c.Git == nil || repository.Host != "github.com" || repository.Owner == "" || repository.Name == "" || !oid(mergeCommit) || !oid(originalBaseOID) || len(mergeCommit) != len(originalBaseOID) {
		return contracts.ProtectedBranchObservation{}, fmt.Errorf("merge proof input is invalid")
	}
	intent, err := c.Store.MergeIntentForProof(ctx, repository.Host, repository.Owner, repository.Name, baseRef, originalBaseOID, mergeCommit)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	if intent.HeadOID == "" || intent.BaseRef != baseRef {
		return contracts.ProtectedBranchObservation{}, store.ErrEvidenceConflict
	}
	ticket, err := c.Store.Ticket(ctx, intent.Ref)
	if err != nil || (ticket.State != domain.StateMerging && ticket.State != domain.StateReconciling) {
		return contracts.ProtectedBranchObservation{}, store.ErrStaleFence
	}
	proof, err := c.Store.GuardedMergeProtectedRefFetchIntent(ctx, intent, mergeCommit)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	gitIntent := proof.Intent
	identity, err := c.Git.Snapshot(ctx, gitIntent.Worktree, baseRef)
	if err != nil || identity.Repository != gitIntent.Repository || identity.Origin != proof.Origin {
		return contracts.ProtectedBranchObservation{}, fmt.Errorf("merge proof worktree identity is unavailable")
	}
	version, fence, err := c.Store.CurrentTicketFence(ctx, intent.Ref)
	if err != nil || version != ticket.Version || fence.RunnerEpoch != ticket.RunnerEpoch {
		return contracts.ProtectedBranchObservation{}, store.ErrStaleFence
	}
	gitIntent.TicketVersion, gitIntent.Fence = version, fence
	// Check before PlanEffect as well as after it. A startup fence advances the
	// parent merge ticket, while an already-confirmed child proof intentionally
	// retains its immutable launch fence. PlanEffect correctly refuses to
	// reinterpret that confirmed effect under the new fence; this explicit
	// recovery path instead authenticates its immutable facts below.
	existing, err := c.Store.Effect(ctx, gitIntent.SemanticKey)
	if err == nil && existing.State == store.EffectConfirmed {
		if err := c.confirmedProofMatches(ctx, existing, gitIntent); err != nil {
			return contracts.ProtectedBranchObservation{}, err
		}
		return protectedObservation(repository, baseRef, mergeCommit, originalBaseOID), nil
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return contracts.ProtectedBranchObservation{}, err
	}
	if ticket.State == domain.StateReconciling {
		// The transition to reconciling happens only after the child proof was
		// confirmed. Recovery may reuse that exact proof, but it must never mint a
		// new Git claim after the parent merge is terminal.
		return contracts.ProtectedBranchObservation{}, store.ErrEvidenceConflict
	}
	if err == nil && existing.State == store.EffectUncertain {
		claim, err := c.Store.ReclaimProtectedRefFetch(ctx, gitIntent)
		if err != nil {
			return contracts.ProtectedBranchObservation{}, err
		}
		return c.executeProtectedProof(ctx, repository, gitIntent, identity.Origin, claim)
	}
	effect, err := c.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: gitIntent.SemanticKey, Ref: intent.Ref, Kind: "git/protected-ref-fetch", TicketVersion: version, Fence: fence, RequestDigest: gitIntent.RequestDigest})
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	// GitHub can apply the merge and then lose its response after this child
	// proof has been durably confirmed. A restart must authenticate that exact
	// immutable proof rather than trying to issue the already-confirmed Git
	// effect again. The Store fact reader validates both the intent and effect
	// copies, including every fenced immutable Git field.
	if effect.State == store.EffectConfirmed {
		if err := c.confirmedProofMatches(ctx, effect, gitIntent); err != nil {
			return contracts.ProtectedBranchObservation{}, err
		}
		return protectedObservation(repository, baseRef, mergeCommit, originalBaseOID), nil
	}
	if effect.State != store.EffectPlanned && effect.State != store.EffectFailed {
		return contracts.ProtectedBranchObservation{}, store.ErrGitMutationIntent
	}
	claim, err := c.Store.IssueGitMutationClaim(ctx, gitIntent)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	return c.executeProtectedProof(ctx, repository, gitIntent, identity.Origin, claim)
}

func (c Coordinator) executeProtectedProof(ctx context.Context, repository contracts.RepositoryIdentity, intent store.GitMutationIntent, origin string, claim contracts.GitMutationClaim) (contracts.ProtectedBranchObservation, error) {
	proof := contracts.ProtectedBranchWitness{Repository: intent.Repository, Worktree: intent.Worktree, Origin: origin, ProtectedRef: intent.BaseRef, OriginalBaseOID: intent.ExpectedBaseOID, MergeOID: intent.ExpectedHeadOID, MutationClaim: claim}
	if err := c.Git.VerifyProtectedBranch(ctx, proof); err != nil {
		return contracts.ProtectedBranchObservation{}, c.recordProtectedProofUncertainty(ctx, claim, err)
	}
	if _, err := c.Store.ConfirmEffect(ctx, effectFence(claim), intent.BaseRef+"@"+intent.ExpectedHeadOID); err != nil {
		return contracts.ProtectedBranchObservation{}, c.recordProtectedProofUncertainty(ctx, claim, err)
	}
	return protectedObservation(repository, intent.BaseRef, intent.ExpectedHeadOID, intent.ExpectedBaseOID), nil
}

// recordProtectedProofUncertainty must outlive the boundary context. A caller
// may cancel immediately after Git has started its deterministic local fetch;
// that cancellation never authorizes a later verifier to reinterpret the
// still-executing child as a fresh proof.
func (c Coordinator) recordProtectedProofUncertainty(ctx context.Context, claim contracts.GitMutationClaim, verifierErr error) error {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := c.Store.MarkEffectUncertain(persistCtx, effectFence(claim)); err != nil {
		// A response can be lost after ConfirmEffect commits. In that case the
		// confirmed immutable fact is already safer than a synthetic uncertain
		// overwrite, and the replay path will authenticate it exactly.
		if effect, readErr := c.Store.Effect(persistCtx, claim.SemanticKey); readErr == nil && effect.State == store.EffectConfirmed {
			return verifierErr
		}
		return errors.Join(verifierErr, fmt.Errorf("persist protected proof uncertainty: %w", err))
	}
	return verifierErr
}

func effectFence(claim contracts.GitMutationClaim) store.EffectFence {
	return store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}
}

func (c Coordinator) confirmedProofMatches(ctx context.Context, effect store.Effect, intent store.GitMutationIntent) error {
	facts, err := c.Store.GitMutationIntentFacts(ctx, intent.SemanticKey)
	if err != nil {
		return fmt.Errorf("%w: confirmed proof facts unavailable", store.ErrEvidenceConflict)
	}
	if effect != facts.Effect || facts.Effect.State != store.EffectConfirmed {
		return fmt.Errorf("%w: confirmed proof effect mismatch", store.ErrEvidenceConflict)
	}
	if facts.Claim.TicketRef != intent.Ref || facts.Claim.SemanticKey != intent.SemanticKey || facts.Claim.RequestDigest != intent.RequestDigest {
		return fmt.Errorf("%w: confirmed proof request mismatch", store.ErrEvidenceConflict)
	}
	if facts.Claim.Repository != intent.Repository || facts.Claim.Worktree != intent.Worktree || facts.Claim.Branch != intent.Branch {
		return fmt.Errorf("%w: confirmed proof checkout mismatch", store.ErrEvidenceConflict)
	}
	if facts.Claim.Operation != intent.Operation || facts.Claim.BaseRef != intent.BaseRef || facts.Claim.ExpectedBaseOID != intent.ExpectedBaseOID || facts.Claim.ExpectedHeadOID != intent.ExpectedHeadOID {
		return fmt.Errorf("%w: confirmed proof ref mismatch", store.ErrEvidenceConflict)
	}
	if facts.ObservedIdentity != intent.BaseRef+"@"+intent.ExpectedHeadOID {
		return fmt.Errorf("%w: confirmed proof observation mismatch", store.ErrEvidenceConflict)
	}
	return nil
}

func protectedObservation(repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) contracts.ProtectedBranchObservation {
	return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: mergeCommit, Contains: true}
}

func oid(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
