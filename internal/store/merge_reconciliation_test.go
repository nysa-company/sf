package store

import (
	"strconv"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

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
	readyKey := "merge-ready/" + string(current.Ref.Channel) + "/" + string(current.Ref.Project) + "/" + string(current.Ref.Ticket) + "/" + head
	readyPlan := EffectPlan{SemanticKey: readyKey, Ref: current.Ref, Kind: "pr_ready", TicketVersion: current.Version, Fence: fence, RequestDigest: "ready-request"}
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

	mergeKey := "merge/" + string(current.Ref.Channel) + "/" + string(current.Ref.Project) + "/" + string(current.Ref.Ticket) + "/" + head
	mergePlan := EffectPlan{SemanticKey: mergeKey, Ref: current.Ref, Kind: "merge", TicketVersion: current.Version, Fence: fence, RequestDigest: "merge-request"}
	if _, err := fixture.db.PlanEffect(fixture.ctx, mergePlan); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := fixture.db.ClaimEffect(fixture.ctx, EffectFence{SemanticKey: mergeKey, Ref: current.Ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("merge claim=%+v err=%v", mergeClaim, err)
	}
	intent := domain.MergeIntent{
		Ref: current.Ref, SemanticKey: mergeKey, RequestDigest: mergeClaim.Effect.RequestDigest,
		TicketVersion: current.Version, LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch,
		RepositoryHost: publication.PullRequest.Repository.Host, RepositoryOwner: publication.PullRequest.Repository.Owner, RepositoryName: publication.PullRequest.Repository.Name, PullRequestNumber: publication.PullRequest.Number,
		HeadOwner: publication.PullRequest.HeadOwner, HeadRepository: publication.PullRequest.HeadRepository, HeadRef: publication.PullRequest.HeadRef, HeadOID: publication.PullRequest.HeadOID,
		BaseRef: publication.PullRequest.BaseRef, OriginalBaseOID: publication.PullRequest.BaseOID, ProtectionRuleID: "main", StrictStatusChecks: true, AdminEnforced: true, Method: "squash",
	}
	if err := fixture.db.RecordMergeIntent(fixture.ctx, intent); err != nil {
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
