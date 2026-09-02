package store

import (
	"strconv"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func guardedMergedObservation(identity contracts.PullRequestIdentity, mergeOID string) contracts.PublishedPullRequestObservation {
	return contracts.PublishedPullRequestObservation{
		Identity: identity, State: "MERGED", Merged: true, MergeCommit: mergeOID,
		BaseHeadOID: identity.BaseOID,
	}
}

func dropMergeIntentImmutability(t *testing.T, db *Store) {
	t.Helper()
	for _, trigger := range []string{"merge_intents_immutable_update", "merge_intents_immutable_delete"} {
		if _, err := db.db.ExecContext(t.Context(), "DROP TRIGGER "+trigger); err != nil {
			t.Fatalf("drop %s: %v", trigger, err)
		}
	}
}

func TestMergeReconciliationReadyRequiresSettledCurrentEvidence(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	completeFinalReview(t, fixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{Ref: waiting.Ref, ExpectedVersion: waiting.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}, ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 701, Decision: "approved"}}); err != nil {
		t.Fatal(err)
	}
	current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil || current.State != domain.StateMerging {
		t.Fatalf("merging ticket=%+v err=%v", current, err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
	head := publication.PullRequest.HeadOID
	intent := domain.MergeIntent{
		Ref: current.Ref, SemanticKey: "merge/" + string(current.Ref.Channel) + "/" + string(current.Ref.Project) + "/" + string(current.Ref.Ticket) + "/" + head,
		TicketVersion:  current.Version,
		RepositoryHost: publication.PullRequest.Repository.Host, RepositoryOwner: publication.PullRequest.Repository.Owner, RepositoryName: publication.PullRequest.Repository.Name, PullRequestNumber: publication.PullRequest.Number,
		HeadOwner: publication.PullRequest.HeadOwner, HeadRepository: publication.PullRequest.HeadRepository, HeadRef: publication.PullRequest.HeadRef, HeadOID: publication.PullRequest.HeadOID,
		BaseRef: publication.PullRequest.BaseRef, OriginalBaseOID: publication.PullRequest.BaseOID, ProtectionRuleID: "main", ProtectionKind: "classic", StrictStatusChecks: true, AdminEnforced: true, Method: "squash",
	}
	intent.RequestDigest = canonicalMergeRequestDigest(intent)
	readyKey := "merge-ready/" + string(current.Ref.Channel) + "/" + string(current.Ref.Project) + "/" + string(current.Ref.Ticket) + "/" + head
	readyPlan := EffectPlan{SemanticKey: readyKey, Ref: current.Ref, Kind: "pr_ready", TicketVersion: current.Version, Fence: fence, RequestDigest: canonicalReadyRequestDigest(intent)}
	if _, err := fixture.db.PlanEffect(fixture.ctx, readyPlan); err != nil {
		t.Fatal(err)
	}
	readyClaim, err := fixture.db.ClaimEffect(fixture.ctx, EffectFence{SemanticKey: readyKey, Ref: current.Ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !readyClaim.Claimed {
		t.Fatalf("ready claim=%+v err=%v", readyClaim, err)
	}
	readyFence := EffectFence{SemanticKey: readyKey, Ref: current.Ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: readyClaim.Effect.ClaimEpoch}}
	if _, err := fixture.db.ConfirmEffect(fixture.ctx, readyFence, "ready/"+head); err != nil {
		t.Fatal(err)
	}

	mergeKey := intent.SemanticKey
	mergePlan := EffectPlan{SemanticKey: mergeKey, Ref: current.Ref, Kind: "merge", TicketVersion: current.Version, Fence: fence, RequestDigest: intent.RequestDigest}
	if _, err := fixture.db.PlanEffect(fixture.ctx, mergePlan); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := fixture.db.ClaimEffect(fixture.ctx, EffectFence{SemanticKey: mergeKey, Ref: current.Ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("merge claim=%+v err=%v", mergeClaim, err)
	}
	intent.RequestDigest = mergeClaim.Effect.RequestDigest
	intent.TicketVersion = current.Version
	intent.LeaderEpoch = fixture.fence.LeaderEpoch
	intent.RunnerEpoch = current.RunnerEpoch
	intent.ClaimEpoch = mergeClaim.Effect.ClaimEpoch
	if err := fixture.db.RecordMergeIntent(fixture.ctx, intent); err != nil {
		t.Fatal(err)
	}
	observedPR := publication.PullRequest
	observedPR.BaseOID = head
	if err := fixture.db.RecordGuardedMergeObservation(fixture.ctx, intent, guardedMergedObservation(observedPR, head)); err != nil {
		t.Fatal(err)
	}
	proof, err := fixture.db.GuardedMergeProtectedRefFetchIntent(fixture.ctx, intent, head)
	if err != nil {
		t.Fatal(err)
	}
	proof.Intent.TicketVersion, proof.Intent.Fence = current.Version, fence
	if _, err := fixture.db.PlanEffect(fixture.ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: fence, RequestDigest: proof.Intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	proofClaim, err := fixture.db.IssueGitMutationClaim(fixture.ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	proofFence := EffectFence{SemanticKey: proofClaim.SemanticKey, Ref: proofClaim.TicketRef, TicketVersion: proofClaim.TicketVersion, Fence: domain.Fence{LeaderEpoch: proofClaim.LeaderEpoch, RunnerEpoch: proofClaim.RunnerEpoch, ClaimEpoch: proofClaim.ClaimEpoch}}
	if _, err := fixture.db.ConfirmEffect(fixture.ctx, proofFence, intent.BaseRef+"@"+head); err != nil {
		t.Fatal(err)
	}
	if ready, err := fixture.db.MergeReconciliationReady(fixture.ctx, current.Ref, current.Version, fence); err != nil || ready {
		t.Fatalf("unsettled merge was ready=%v err=%v", ready, err)
	}
	mergeFence := EffectFence{SemanticKey: mergeKey, Ref: current.Ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch}}
	if _, err := fixture.db.ConfirmEffect(fixture.ctx, mergeFence, "merged/"+head); err != nil {
		t.Fatal(err)
	}
	if ready, err := fixture.db.MergeReconciliationReady(fixture.ctx, current.Ref, current.Version, fence); err != nil || !ready {
		t.Fatalf("settled merge was ready=%v err=%v", ready, err)
	}
	dropMergeIntentImmutability(t, fixture.db)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE merge_intents SET request_digest='tampered' WHERE semantic_key=?`, mergeKey); err != nil {
		t.Fatal(err)
	}
	tamperCases := []struct {
		column string
		value  string
	}{
		{column: "request_digest", value: "tampered-request"},
		{column: "claim_epoch", value: "999"},
		{column: "pull_request_number", value: "99"},
		{column: "head_owner", value: "other-owner"},
		{column: "head_repository", value: "other-repository"},
		{column: "head_ref", value: "other-ref"},
		{column: "head_oid", value: "other-head"},
		{column: "base_ref", value: "other-base"},
		{column: "original_base_oid", value: "other-base-oid"},
	}
	for _, tc := range tamperCases {
		if _, err := fixture.db.db.ExecContext(fixture.ctx, "UPDATE merge_intents SET "+tc.column+"=? WHERE semantic_key=?", tc.value, mergeKey); err != nil {
			t.Fatal(err)
		}
		if ready, err := fixture.db.MergeReconciliationReady(fixture.ctx, current.Ref, current.Version, fence); err != nil || ready {
			t.Fatalf("tampered %s was ready=%v err=%v", tc.column, ready, err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, "UPDATE merge_intents SET "+tc.column+"=? WHERE semantic_key=?", intentValue(intent, tc.column), mergeKey); err != nil {
			t.Fatal(err)
		}
	}
	if ready, err := fixture.db.MergeReconciliationReady(fixture.ctx, current.Ref, current.Version-1, fence); err != nil || ready {
		t.Fatalf("stale version was ready=%v err=%v", ready, err)
	}
	wrongLeader := fence
	wrongLeader.LeaderEpoch++
	if ready, err := fixture.db.MergeReconciliationReady(fixture.ctx, current.Ref, current.Version, wrongLeader); err != nil || ready {
		t.Fatalf("stale leader was ready=%v err=%v", ready, err)
	}
}

// A merge can be applied by GitHub after its process handoff but before the
// exact merged-PR observation commits. Restart first changes the parent effect
// to uncertain and fences the ticket. The observation must still be recordable
// from that authenticated historical merge authority; accepting a generic
// promoted confirmation here would be an authority bypass.
func TestGuardedMergeObservationRecordsAfterCrashRecoveryFence(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	completeFinalReview(t, fixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{Ref: waiting.Ref, ExpectedVersion: waiting.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}, ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 911, Decision: "approved"}}); err != nil {
		t.Fatal(err)
	}
	merging, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil || merging.State != domain.StateMerging {
		t.Fatalf("merging ticket=%+v err=%v", merging, err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, merging.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: merging.RunnerEpoch}
	key := "merge/crash-observation/" + publication.PullRequest.HeadOID
	intent := domain.MergeIntent{Ref: merging.Ref, SemanticKey: key, TicketVersion: merging.Version, RepositoryHost: publication.PullRequest.Repository.Host, RepositoryOwner: publication.PullRequest.Repository.Owner, RepositoryName: publication.PullRequest.Repository.Name, PullRequestNumber: publication.PullRequest.Number, HeadOwner: publication.PullRequest.HeadOwner, HeadRepository: publication.PullRequest.HeadRepository, HeadRef: publication.PullRequest.HeadRef, HeadOID: publication.PullRequest.HeadOID, BaseRef: publication.PullRequest.BaseRef, OriginalBaseOID: publication.PullRequest.BaseOID, ProtectionRuleID: "main", ProtectionKind: "classic", StrictStatusChecks: true, AdminEnforced: true, Method: "squash"}
	intent.RequestDigest = canonicalMergeRequestDigest(intent)
	if _, err := fixture.db.PlanEffect(fixture.ctx, EffectPlan{SemanticKey: key, Ref: merging.Ref, Kind: "merge", TicketVersion: merging.Version, Fence: fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.db.ClaimEffect(fixture.ctx, EffectFence{SemanticKey: key, Ref: merging.Ref, TicketVersion: merging.Version, Fence: fence})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	intent.LeaderEpoch, intent.RunnerEpoch, intent.ClaimEpoch = claim.Effect.LeaderEpoch, claim.Effect.RunnerEpoch, claim.Effect.ClaimEpoch
	if err := fixture.db.RecordMergeIntent(fixture.ctx, intent); err != nil {
		t.Fatal(err)
	}

	newLeader, err := fixture.db.AcquireLeader(fixture.ctx, merging.Ref.Channel, "merge-observation-crash-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ReconcileEffects(fixture.ctx, merging.Ref.Channel, newLeader); err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, merging.Ref.Channel, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence changed=%d err=%v", changed, err)
	}
	recovered, err := fixture.db.Effect(fixture.ctx, key)
	recoveredTicket, ticketErr := fixture.db.Ticket(fixture.ctx, merging.Ref)
	if err != nil || ticketErr != nil || recovered.State != EffectUncertain || recovered.LeaderEpoch != newLeader || recovered.TicketVersion != intent.TicketVersion || recoveredTicket.Version == intent.TicketVersion || recoveredTicket.RunnerEpoch == intent.RunnerEpoch {
		t.Fatalf("recovered merge effect=%+v ticket=%+v intent=%+v err=%v ticket_err=%v", recovered, recoveredTicket, intent, err, ticketErr)
	}
	// A valid fast-forward/rebase result may retain the source OID. The
	// observation must preserve the two fields independently without treating
	// equality as a fabricated proof.
	mergeOID := intent.HeadOID
	if err := fixture.db.RecordGuardedMergeObservation(fixture.ctx, intent, guardedMergedObservation(publication.PullRequest, mergeOID)); err != nil {
		t.Fatalf("record authenticated post-fence observation: %v", err)
	}
	if err := fixture.db.RecordGuardedMergeObservation(fixture.ctx, intent, guardedMergedObservation(publication.PullRequest, mergeOID)); err != nil {
		t.Fatalf("replay exact post-fence observation: %v", err)
	}
	if got, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, mergeOID); err != nil || got != intent {
		t.Fatalf("recovered observation did not authorize the exact historical intent: got=%+v want=%+v err=%v", got, intent, err)
	}
	retargeted := strings.Repeat("e", len(intent.HeadOID))
	if retargeted == mergeOID {
		retargeted = strings.Repeat("d", len(intent.HeadOID))
	}
	if err := fixture.db.RecordGuardedMergeObservation(fixture.ctx, intent, guardedMergedObservation(publication.PullRequest, retargeted)); err == nil {
		t.Fatal("different post-merge OID retargeted immutable observation")
	}
	var count int
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM guarded_merge_observations WHERE semantic_key=? AND merge_oid=?`, key, mergeOID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("observation rows=%d err=%v", count, err)
	}
	// Model a direct durable one-row tamper outside Store. The append-only
	// trigger is defense in depth; the reader must also reject a row whose
	// canonical digest no longer binds its source/merge tuple.
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER guarded_merge_observations_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE guarded_merge_observations SET merge_oid=? WHERE semantic_key=?`, retargeted, key); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, mergeOID); err == nil {
		t.Fatal("tampered stored observation still authorized the original merge")
	}
	if _, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, retargeted); err == nil {
		t.Fatal("tampered stored observation authorized a retargeted merge")
	}
}

func TestGuardedMergeObservationBindsProtectionWitness(t *testing.T) {
	fixture, _, _ := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	intent, found, err := fixture.db.MergeIntent(fixture.ctx, "merge/rearm/armed/merging")
	if err != nil || !found {
		t.Fatalf("merge intent found=%v err=%v", found, err)
	}
	dropMergeIntentImmutability(t, fixture.db)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE merge_intents SET protection_rule_id='tampered' WHERE semantic_key=?`, intent.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, intent.HeadOID); err == nil {
		t.Fatal("tampered protection witness authorized guarded proof")
	}
}

func TestGuardedMergeObservationRejectsUnmergedResponse(t *testing.T) {
	fixture, _, _ := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	intent, found, err := fixture.db.MergeIntent(fixture.ctx, "merge/rearm/armed/merging")
	if err != nil || !found {
		t.Fatalf("merge intent found=%v err=%v", found, err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, intent.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observation := guardedMergedObservation(publication.PullRequest, intent.HeadOID)
	observation.State, observation.Merged, observation.MergeCommit = "OPEN", false, ""
	if err := fixture.db.RecordGuardedMergeObservation(fixture.ctx, intent, observation); err == nil {
		t.Fatal("unmerged response accepted as guarded merge observation")
	}
}

// A merge-shaped set of rows is not sufficient authority. In particular, a
// synthetic fixture without the authenticated publication/approval chain must
// remain ineligible for the scheduler optimization.
func TestMergeReconciliationReadyRejectsSyntheticIntentWithoutPublication(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "synthetic-merge"}
	leader, err := database.AcquireLeader(ctx, ref.Channel, "synthetic-merge-readiness")
	if err != nil {
		t.Fatal(err)
	}
	current := Ticket{Ref: ref, SourceDigest: "synthetic-merge-source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging, Version: 4, RunnerEpoch: 7}
	if err := database.CreateTicket(ctx, current); err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	head := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	readyKey := "merge-ready/dev/nysa/synthetic-merge/" + head
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: readyKey, Ref: ref, Kind: "pr_ready", TicketVersion: current.Version, Fence: fence, RequestDigest: "ready-request"}); err != nil {
		t.Fatal(err)
	}
	readyClaim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: readyKey, Ref: ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !readyClaim.Claimed {
		t.Fatalf("ready claim=%+v err=%v", readyClaim, err)
	}
	if _, err := database.ConfirmEffect(ctx, EffectFence{SemanticKey: readyKey, Ref: ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: readyClaim.Effect.ClaimEpoch}}, "ready/"+head); err != nil {
		t.Fatal(err)
	}
	mergeKey := "merge/dev/nysa/synthetic-merge/" + head
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: mergeKey, Ref: ref, Kind: "merge", TicketVersion: current.Version, Fence: fence, RequestDigest: "merge-request"}); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("merge claim=%+v err=%v", mergeClaim, err)
	}
	intent := domain.MergeIntent{Ref: ref, SemanticKey: mergeKey, RequestDigest: "merge-request", TicketVersion: current.Version, LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch, RepositoryHost: "github.com", RepositoryOwner: "nysa", RepositoryName: "app", PullRequestNumber: 7, HeadOwner: "nysa", HeadRepository: "app", HeadRef: "sf/synthetic-merge", HeadOID: head, BaseRef: "main", OriginalBaseOID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", ProtectionRuleID: "main", StrictStatusChecks: true, AdminEnforced: true, Method: "squash"}
	if err := database.RecordMergeIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	mergeFence := EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch}}
	if _, err := database.ConfirmEffect(ctx, mergeFence, "merged/"+head); err != nil {
		t.Fatal(err)
	}
	if ready, err := database.MergeReconciliationReady(ctx, ref, current.Version, fence); err != nil || ready {
		t.Fatalf("synthetic merge was ready=%v err=%v", ready, err)
	}
}

func intentValue(intent domain.MergeIntent, column string) string {
	switch column {
	case "request_digest":
		return intent.RequestDigest
	case "claim_epoch":
		return strconv.FormatUint(intent.ClaimEpoch, 10)
	case "pull_request_number":
		return strconv.Itoa(intent.PullRequestNumber)
	case "head_owner":
		return intent.HeadOwner
	case "head_repository":
		return intent.HeadRepository
	case "head_ref":
		return intent.HeadRef
	case "head_oid":
		return intent.HeadOID
	case "base_ref":
		return intent.BaseRef
	case "original_base_oid":
		return intent.OriginalBaseOID
	default:
		return ""
	}
}
