package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestProtectedBaseRefreshProofDerivesCurrentAuthority(t *testing.T) {
	db, ctx, current, fence := publicationLifecycleFixture(t)
	defer db.Close()
	newBase := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, newBase)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Intent.Operation != "protected-ref-fetch" || proof.Intent.ExpectedBaseOID != candidate.Snapshot.BaseSHA || proof.Intent.ExpectedHeadOID != newBase || proof.Intent.Worktree != proof.Worktree.Path || proof.Intent.Ref != current.Ref || proof.Intent.Fence != fence || proof.Intent.RequestDigest != proof.ContextDigest || !validGitIntent(proof.Intent) {
		t.Fatalf("underbound proof: %+v", proof.Intent)
	}
	replay, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, newBase)
	if err != nil || replay.Intent != proof.Intent {
		t.Fatalf("proposal replay changed: %v", err)
	}
	if _, err := db.Effect(ctx, proof.Intent.SemanticKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("proposal minted effect: %v", err)
	}
	var refreshes int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM protected_base_refresh_intents`).Scan(&refreshes); err != nil || refreshes != 0 {
		t.Fatalf("proposal reserved refresh: %d %v", refreshes, err)
	}
	for name, bad := range map[string]string{"same base": candidate.Snapshot.BaseSHA, "candidate head": candidate.Snapshot.HeadSHA, "missing": "", "mixed width": strings.Repeat("8", 64), "uppercase": strings.Repeat("A", 40)} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, bad); err == nil {
				t.Fatal("invalid base accepted")
			}
		})
	}
	for name, bad := range map[string]domain.Fence{"leader": {LeaderEpoch: fence.LeaderEpoch + 1, RunnerEpoch: fence.RunnerEpoch}, "runner": {LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch + 1}, "claim": {LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ClaimEpoch: 1}} {
		t.Run(name, func(t *testing.T) {
			if _, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, bad, newBase); err == nil {
				t.Fatal("stale fence accepted")
			}
		})
	}
	if _, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version-1, fence, newBase); err == nil {
		t.Fatal("old version accepted")
	}
	// An unrelated planned mutation must be settled before any refresh proof.
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: "refresh-unresolved", Ref: current.Ref, Kind: "draft_pr", TicketVersion: current.Version, Fence: fence, RequestDigest: "sha256:" + strings.Repeat("7", 64)}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, newBase); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("unresolved mutation: %v", err)
	}
}

func TestProtectedBaseRefreshProofRejectsNonGuardedPublication(t *testing.T) {
	db, ctx, current, fence := publicationLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeManual)
	defer db.Close()
	if _, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, strings.Repeat("9", 40)); err == nil {
		t.Fatal("manual external-merge race admitted")
	}
}

func TestProtectedBaseRefreshReservationRequiresExactConfirmedProof(t *testing.T) {
	db, ctx, current, fence := publicationLifecycleFixture(t)
	defer db.Close()
	base := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base); err == nil {
		t.Fatal("missing proof accepted")
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: fence, RequestDigest: proof.ContextDigest}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base); err == nil {
		t.Fatal("planned proof accepted")
	}
	claim, err := db.IssueGitMutationClaim(ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base); err == nil {
		t.Fatal("executing proof accepted")
	}
	// Store-boundary fixture: the real Git exact-tip tests independently prove
	// this observation. This is not an end-to-end refresh mutation test.
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: current.Ref, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, proof.ObservedIdentity); err != nil {
		t.Fatal(err)
	}
	reservation, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	if reservation.ID <= 0 || reservation.Mutation.Operation != "refresh-base" || reservation.Mutation.ExpectedBaseOID != base || reservation.Mutation.ExpectedHeadOID != reservation.Candidate.Snapshot.HeadSHA {
		t.Fatal("reservation lost exact transform inputs")
	}
	replay, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base)
	if err != nil || replay.ID != reservation.ID || replay.Mutation != reservation.Mutation || replay.IntentDigest != reservation.IntentDigest {
		t.Fatalf("lost response replay: %v", err)
	}
	if _, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, strings.Repeat("8", 40)); err == nil {
		t.Fatal("second refresh budget allocated")
	}
	after, err := db.Ticket(ctx, current.Ref)
	if err != nil || after.Version != current.Version || after.State != domain.StatePublishing {
		t.Fatal("reservation changed lifecycle before Git evidence")
	}
	worktree, err := db.Worktree(ctx, current.Ref)
	if err != nil || worktree.BaseSHA != reservation.Worktree.BaseSHA {
		t.Fatal("reservation changed worktree before Git evidence")
	}
	refreshClaim, err := db.IssueGitMutationClaim(ctx, reservation.Mutation)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, refreshClaim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireGitMutation(ctx, refreshClaim); err == nil {
		t.Fatal("second repository writer admitted")
	}
	recorder, ok := lease.(contracts.GitBaseRefreshPreparationLease)
	if !ok {
		t.Fatal("missing refresh preparation authority")
	}
	reader, ok := lease.(contracts.GitBaseRefreshPreparedReader)
	if !ok {
		t.Fatal("missing refresh prepared reader")
	}
	if empty, found, err := reader.PreparedBaseRefresh(ctx); err != nil || found || empty.CommitOID != "" {
		t.Fatalf("unexpected prepared-before state=%+v found=%v err=%v", empty, found, err)
	}
	parents := [2]string{reservation.Candidate.Snapshot.HeadSHA, base}
	commit, tree := strings.Repeat("8", 40), strings.Repeat("7", 40)
	if err := recorder.RecordBaseRefreshPreparation(ctx, commit, tree, [2]string{base, parents[0]}); err == nil {
		t.Fatal("reversed parent order accepted")
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, "", tree, parents); err == nil {
		t.Fatal("partial prepared tuple accepted")
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, commit, strings.Repeat("7", 64), parents); err == nil {
		t.Fatal("mixed object formats accepted")
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, commit, tree, parents); err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, commit, tree, parents); err != nil {
		t.Fatalf("prepared replay: %v", err)
	}
	prepared, found, err := reader.PreparedBaseRefresh(ctx)
	if err != nil || !found || prepared.CommitOID != commit || prepared.TreeOID != tree || prepared.Parents != parents {
		t.Fatalf("prepared getter=%+v found=%v err=%v", prepared, found, err)
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, strings.Repeat("6", 40), tree, parents); err == nil {
		t.Fatal("prepared identity was rewritten")
	}
	facts, err := db.GitMutationIntentFacts(ctx, refreshClaim.SemanticKey)
	if err != nil || facts.PreparedCommitOID != commit || facts.PreparedTreeOID != tree {
		t.Fatalf("durable prepared facts: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, commit, tree, parents); err == nil {
		t.Fatal("released nonce retained preparation authority")
	}
	// A fresh lease must recover the immutable prepared tuple rather than
	// minting or rewriting it.
	reacquired, err := db.AcquireGitMutation(ctx, refreshClaim)
	if err != nil {
		t.Fatal(err)
	}
	reader, ok = reacquired.(contracts.GitBaseRefreshPreparedReader)
	if !ok {
		t.Fatal("reacquired lease missing prepared reader")
	}
	got, found, err := reader.PreparedBaseRefresh(ctx)
	if err != nil || !found || got.CommitOID != commit || got.TreeOID != tree || got.Parents != parents {
		t.Fatalf("reacquired prepared=%+v found=%v err=%v", got, found, err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatal(err)
	}
	// Bypass an immutable trigger only in this disposable corruption fixture;
	// duplicate indexed fields must still agree with the canonical payload.
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER protected_base_refresh_intents_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE protected_base_refresh_intents SET source_digest=? WHERE refresh_id=?`, strings.Repeat("0", 64), reservation.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base); err == nil {
		t.Fatal("tampered indexed projection accepted")
	}
}

func TestProtectedBaseRefreshClaimRejectsUnreservedGenericEffect(t *testing.T) {
	db, ctx, current, fence := publicationLifecycleFixture(t)
	defer db.Close()
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, strings.Repeat("9", 40))
	if err != nil {
		t.Fatal(err)
	}
	intent := proof.Intent
	intent.Operation = "refresh-base"
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: current.Ref, Kind: "git/refresh-base", TicketVersion: current.Version, Fence: fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IssueGitMutationClaim(ctx, intent); err == nil {
		t.Fatal("generic effect bypassed refresh reservation")
	}
	effect, err := db.Effect(ctx, intent.SemanticKey)
	if err != nil || effect.State != EffectPlanned || effect.ClaimEpoch != 0 {
		t.Fatalf("failed claim changed effect: %+v %v", effect, err)
	}
}

func TestProtectedBaseRefreshReservationRejectsContainmentProof(t *testing.T) {
	db, ctx, current, fence := publicationLifecycleFixture(t)
	defer db.Close()
	base := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: fence, RequestDigest: proof.ContextDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: current.Ref, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, "main@"+base); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base); err == nil {
		t.Fatal("containment observation became exact base proof")
	}
	var count int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM protected_base_refresh_intents`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed proof consumed budget: %d %v", count, err)
	}
}
