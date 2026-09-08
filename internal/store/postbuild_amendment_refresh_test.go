package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

// Uses only typed Store writes, including the same bounded Git observations as
// the existing refresh Store fixtures. No SQL lifecycle/authority synthesis.
func assertPostbuildAmendmentPublishedRefreshRestart(t *testing.T, db *Store, ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, candidate domain.CandidateSnapshot) {
	t.Helper()
	if _, err := db.TransitionCandidate(ctx, Transition{Ref: ref, ExpectedVersion: version, Fence: fence, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", EventPayload: "{}"}, candidate); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	recordFixturePublication(t, db, ctx, ticket, fence)
	base := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, ref, ticket.Version, fence, base)
	if err != nil {
		t.Fatalf("post-amendment refresh proof: %v", err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: ref, Kind: "git/protected-ref-fetch", TicketVersion: ticket.Version, Fence: fence, RequestDigest: proof.ContextDigest}); err != nil {
		t.Fatal(err)
	}
	proofClaim, err := db.IssueGitMutationClaim(ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: proofClaim.SemanticKey, Ref: ref, TicketVersion: proofClaim.TicketVersion, Fence: domain.Fence{LeaderEpoch: proofClaim.LeaderEpoch, RunnerEpoch: proofClaim.RunnerEpoch, ClaimEpoch: proofClaim.ClaimEpoch}}, proof.ObservedIdentity); err != nil {
		t.Fatal(err)
	}
	reservation, err := db.ReserveProtectedBaseRefresh(ctx, ref, ticket.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, reservation.Mutation)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("8", 40), strings.Repeat("7", 40)
	if err := lease.(contracts.GitBaseRefreshPreparationLease).RecordBaseRefreshPreparation(ctx, commit, tree, [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: ref, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, commit); err != nil {
		t.Fatal(err)
	}
	var identity gitboundary.Identity
	if err := json.Unmarshal(reservation.Worktree.IdentityJSON, &identity); err != nil {
		t.Fatal(err)
	}
	identity.BaseHead = base
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := db.CompleteProtectedBaseRefresh(ctx, claim, raw)
	if err != nil {
		t.Fatalf("post-amendment completed refresh: %v", err)
	}
	version = completion.Version
	fence = completion.Fence
	assertSuperseded := func() {
		t.Helper()
		if _, err := db.PostbuildVerificationAmendmentContext(ctx, ref, version, fence); !errors.Is(err, ErrNotFound) {
			t.Fatalf("old amendment shadows refresh: %v", err)
		}
		if _, err := db.PostbuildRepairContext(ctx, ref, version, fence); !errors.Is(err, ErrNotFound) {
			t.Fatalf("old repair shadows refresh: %v", err)
		}
		if _, err := db.ProtectedBaseRefreshBuildContext(ctx, ref, version, fence); err != nil {
			t.Fatalf("fresh refresh authority: %v", err)
		}
	}
	assertSuperseded()
	for _, owner := range []string{"post-amendment-refresh-restart-1", "post-amendment-refresh-restart-2"} {
		leader, err := db.AcquireLeader(ctx, ref.Channel, owner)
		if err != nil {
			t.Fatal(err)
		}
		if superseded, err := postbuildRepairSupersededAt(ctx, db.db, ref, version, fence, false); err != nil || !superseded {
			t.Fatalf("historical refresh after leader acquisition: %t %v", superseded, err)
		}
		if changed, err := db.FenceRecoveredRunners(ctx, ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("refresh restart=%d err=%v", changed, err)
		}
		version++
		fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch + 1}
		assertSuperseded()
	}
}
