package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestGitCommitAcquireDistinguishesActiveAndQuarantinedCommand(t *testing.T) {
	for _, quarantine := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "quarantined"}[quarantine], func(t *testing.T) {
			db, ctx := openTestStore(t)
			holder, waiter := repositoryCommandContendingClaimsFixture(t, db, ctx)
			intent := GitMutationIntent{
				EffectFence:   EffectFence{Ref: waiter.TicketRef, TicketVersion: waiter.TicketVersion, Fence: domain.Fence{LeaderEpoch: waiter.LeaderEpoch, RunnerEpoch: waiter.RunnerEpoch}},
				RequestDigest: gitDigest("a"), Repository: waiter.Repository, Worktree: waiter.Worktree,
				Branch: waiter.Branch, Operation: "commit", BaseRef: waiter.BaseRef,
				ExpectedBaseOID: waiter.BaseSHA, ExpectedHeadOID: waiter.BaseSHA,
			}
			intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
			if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
				t.Fatal(err)
			}
			claim, err := db.IssueGitMutationClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := db.AcquireRepositoryCommand(ctx, holder)
			if err != nil {
				t.Fatal(err)
			}
			if quarantine {
				if err := lease.Quarantine(); err != nil {
					t.Fatal(err)
				}
			}
			acquired, err := db.AcquireGitMutation(ctx, claim)
			if acquired != nil || !errors.Is(err, ErrRepositoryCommandLease) || errors.Is(err, contracts.ErrGitMutationContended) == quarantine {
				t.Fatalf("lease=%T err=%v quarantine=%v", acquired, err, quarantine)
			}
			if quarantine {
				return
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			acquired, err = db.AcquireGitMutation(ctx, claim)
			if err != nil || acquired == nil {
				t.Fatalf("same claim after release: %T %v", acquired, err)
			}
			if _, err := db.AcquireGitMutation(ctx, claim); !errors.Is(err, ErrGitMutationLease) || errors.Is(err, contracts.ErrGitMutationContended) {
				t.Fatalf("same-semantic lease became retryable: %v", err)
			}
			if err := acquired.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
