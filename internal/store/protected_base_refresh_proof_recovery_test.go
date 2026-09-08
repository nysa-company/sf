package store

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProtectedBaseRefreshProofReclaimAdvancesOnlyExactCurrentClaim(t *testing.T) {
	db, ctx, current, fence := publicationLifecycleFixture(t)
	defer db.Close()
	newBase := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, newBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: fence, RequestDigest: proof.ContextDigest}); err != nil {
		t.Fatal(err)
	}
	old, err := db.IssueGitMutationClaim(ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: old.SemanticKey, Ref: old.TicketRef, TicketVersion: old.TicketVersion, Fence: domain.Fence{LeaderEpoch: old.LeaderEpoch, RunnerEpoch: old.RunnerEpoch, ClaimEpoch: old.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := db.ReclaimProtectedBaseRefreshProof(ctx, current.Ref, current.Version, fence, newBase)
	if err != nil || reclaimed.SemanticKey != old.SemanticKey || reclaimed.ClaimEpoch <= old.ClaimEpoch {
		t.Fatalf("reclaimed=%+v err=%v", reclaimed, err)
	}
	if _, err := db.AcquireGitMutation(ctx, old); err == nil {
		t.Fatal("old proof claim remained usable")
	}
	lease, err := db.AcquireGitMutation(ctx, reclaimed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimProtectedBaseRefreshProof(ctx, current.Ref, current.Version, fence, newBase); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("active lease reclaim=%v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: reclaimed.SemanticKey, Ref: reclaimed.TicketRef, TicketVersion: reclaimed.TicketVersion, Fence: domain.Fence{LeaderEpoch: reclaimed.LeaderEpoch, RunnerEpoch: reclaimed.RunnerEpoch, ClaimEpoch: reclaimed.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimProtectedBaseRefreshProof(ctx, current.Ref, current.Version, fence, strings.Repeat("8", 40)); err == nil {
		t.Fatal("different proof base reclaimed")
	}
	if _, err := db.ReclaimProtectedBaseRefreshProof(ctx, current.Ref, current.Version, domain.Fence{LeaderEpoch: fence.LeaderEpoch + 1, RunnerEpoch: fence.RunnerEpoch}, newBase); err == nil {
		t.Fatal("stale proof fence reclaimed")
	}
	claim := reclaimed
	for claim.ClaimEpoch < old.ClaimEpoch+maxProtectedBaseRefreshReclaims {
		if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
			t.Fatal(err)
		}
		claim, err = db.ReclaimProtectedBaseRefreshProof(ctx, current.Ref, current.Version, fence, newBase)
		if err != nil || claim.ClaimEpoch != reclaimed.ClaimEpoch+1 {
			t.Fatalf("bounded reclaim claim=%+v err=%v", claim, err)
		}
		reclaimed = claim
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	before, err := db.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimProtectedBaseRefreshProof(ctx, current.Ref, current.Version, fence, newBase); err == nil {
		t.Fatal("proof reclaim exceeded bounded attempts")
	}
	after, err := db.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("bounded refusal mutated facts before=%+v after=%+v err=%v", before, after, err)
	}
}
