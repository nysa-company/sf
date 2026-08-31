// Package mergeproof composes GitHub's exact merged-PR observation with the
// separate Store-issued Git protected-ref proof.  GitHub never receives a Git
// mutation claim and Git never decides which PR may merge.
package mergeproof

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

type Coordinator struct {
	Store *store.Store
	Git   git.Runner
}

var _ contracts.MergeBranchVerifier = Coordinator{}

// VerifyProtectedBranch is called by the GitHub boundary only after GitHub
// has observed the exact source head as merged.  It locates that one durable
// merge intent, reserves a child protected-ref-fetch effect, and proves both
// ancestry and containment through git.Runner.
func (c Coordinator) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
	if c.Store == nil || repository.Host != "github.com" || repository.Owner == "" || repository.Name == "" || !oid(mergeCommit) || !oid(originalBaseOID) || len(mergeCommit) != len(originalBaseOID) {
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
	project, err := c.Store.Project(ctx, intent.Ref.Channel, intent.Ref.Project)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	worktree, err := c.Store.Worktree(ctx, intent.Ref)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	identity, err := c.Git.Snapshot(ctx, worktree.Path, baseRef)
	if err != nil || identity.Repository != project.Path || identity.Origin == "" {
		return contracts.ProtectedBranchObservation{}, fmt.Errorf("merge proof worktree identity is unavailable")
	}
	request := digest(intent.SemanticKey, baseRef, originalBaseOID, mergeCommit, identity.Origin)
	version, fence, err := c.Store.CurrentTicketFence(ctx, intent.Ref)
	if err != nil || version != ticket.Version || fence.RunnerEpoch != ticket.RunnerEpoch {
		return contracts.ProtectedBranchObservation{}, store.ErrStaleFence
	}
	gitIntent := store.GitMutationIntent{
		EffectFence: store.EffectFence{Ref: intent.Ref, TicketVersion: version, Fence: fence}, RequestDigest: request,
		Repository: project.Path, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "protected-ref-fetch",
		BaseRef: baseRef, ExpectedBaseOID: originalBaseOID, ExpectedHeadOID: mergeCommit,
	}
	gitIntent.SemanticKey = store.CanonicalGitMutationSemanticKey(gitIntent)
	// Check before PlanEffect as well as after it. A startup fence advances the
	// parent merge ticket, while an already-confirmed child proof intentionally
	// retains its immutable launch fence. PlanEffect correctly refuses to
	// reinterpret that confirmed effect under the new fence; this explicit
	// recovery path instead authenticates its immutable facts below.
	if existing, err := c.Store.Effect(ctx, gitIntent.SemanticKey); err == nil && existing.State == store.EffectConfirmed {
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
	effect, err := c.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: gitIntent.SemanticKey, Ref: intent.Ref, Kind: "git/protected-ref-fetch", TicketVersion: version, Fence: fence, RequestDigest: request})
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
	if err := c.Git.VerifyProtectedBranch(ctx, contracts.ProtectedBranchWitness{Repository: project.Path, Worktree: worktree.Path, Origin: identity.Origin, ProtectedRef: baseRef, OriginalBaseOID: originalBaseOID, MergeOID: mergeCommit, MutationClaim: claim}); err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	if _, err := c.Store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, baseRef+"@"+mergeCommit); err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	return protectedObservation(repository, baseRef, mergeCommit, originalBaseOID), nil
}

func (c Coordinator) confirmedProofMatches(ctx context.Context, effect store.Effect, intent store.GitMutationIntent) error {
	facts, err := c.Store.GitMutationIntentFacts(ctx, intent.SemanticKey)
	if err != nil || effect != facts.Effect || facts.Effect.State != store.EffectConfirmed || facts.Claim.TicketRef != intent.Ref || facts.Claim.SemanticKey != intent.SemanticKey || facts.Claim.RequestDigest != intent.RequestDigest || facts.Claim.Repository != intent.Repository || facts.Claim.Worktree != intent.Worktree || facts.Claim.Branch != intent.Branch || facts.Claim.Operation != intent.Operation || facts.Claim.BaseRef != intent.BaseRef || facts.Claim.ExpectedBaseOID != intent.ExpectedBaseOID || facts.Claim.ExpectedHeadOID != intent.ExpectedHeadOID || facts.ObservedIdentity != intent.BaseRef+"@"+intent.ExpectedHeadOID {
		return store.ErrEvidenceConflict
	}
	return nil
}

func protectedObservation(repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) contracts.ProtectedBranchObservation {
	return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: mergeCommit, Contains: true}
}

func digest(values ...string) string {
	h := sha256.New()
	for _, value := range values {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil))
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
