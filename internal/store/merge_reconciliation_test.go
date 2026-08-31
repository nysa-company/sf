package store

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestMergeReconciliationReadyRequiresSettledCurrentEvidence(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "settled-merge"}
	leader, err := database.AcquireLeader(ctx, ref.Channel, "merge-readiness")
	if err != nil {
		t.Fatal(err)
	}
	current := Ticket{Ref: ref, SourceDigest: "merge-readiness-source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging, Version: 4, RunnerEpoch: 7}
	if err := database.CreateTicket(ctx, current); err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	readyKey := "merge-ready/dev/nysa/settled-merge/" + strings.Repeat("a", 40)
	readyPlan := EffectPlan{SemanticKey: readyKey, Ref: ref, Kind: "pr_ready", TicketVersion: current.Version, Fence: fence, RequestDigest: "ready-request"}
	if _, err := database.PlanEffect(ctx, readyPlan); err != nil {
		t.Fatal(err)
	}
	readyClaim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: readyKey, Ref: ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !readyClaim.Claimed {
		t.Fatalf("ready claim=%+v err=%v", readyClaim, err)
	}
	readyFence := EffectFence{SemanticKey: readyKey, Ref: ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: readyClaim.Effect.ClaimEpoch}}
	if _, err := database.ConfirmEffect(ctx, readyFence, "ready/"+strings.Repeat("a", 40)); err != nil {
		t.Fatal(err)
	}

	mergeKey := "merge/dev/nysa/settled-merge/" + strings.Repeat("a", 40)
	mergePlan := EffectPlan{SemanticKey: mergeKey, Ref: ref, Kind: "merge", TicketVersion: current.Version, Fence: fence, RequestDigest: "merge-request"}
	if _, err := database.PlanEffect(ctx, mergePlan); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: current.Version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("merge claim=%+v err=%v", mergeClaim, err)
	}
	intent := domain.MergeIntent{
		Ref: ref, SemanticKey: mergeKey, RequestDigest: mergeClaim.Effect.RequestDigest,
		TicketVersion: current.Version, LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch,
		RepositoryHost: "github.com", RepositoryOwner: "nysa", RepositoryName: "app", PullRequestNumber: 7,
		HeadOwner: "nysa", HeadRepository: "app", HeadRef: "sf/settled-merge", HeadOID: strings.Repeat("a", 40),
		BaseRef: "main", OriginalBaseOID: strings.Repeat("b", 40), ProtectionRuleID: "main", StrictStatusChecks: true, AdminEnforced: true, Method: "squash",
	}
	if err := database.RecordMergeIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if ready, err := database.MergeReconciliationReady(ctx, ref, current.Version, fence); err != nil || ready {
		t.Fatalf("unsettled merge was ready=%v err=%v", ready, err)
	}
	mergeFence := EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch}}
	if _, err := database.ConfirmEffect(ctx, mergeFence, "merged/"+strings.Repeat("a", 40)); err != nil {
		t.Fatal(err)
	}
	if ready, err := database.MergeReconciliationReady(ctx, ref, current.Version, fence); err != nil || !ready {
		t.Fatalf("settled merge was ready=%v err=%v", ready, err)
	}
	if _, err := database.db.ExecContext(ctx, `UPDATE merge_intents SET request_digest='tampered' WHERE semantic_key=?`, mergeKey); err != nil {
		t.Fatal(err)
	}
	if ready, err := database.MergeReconciliationReady(ctx, ref, current.Version, fence); err != nil || ready {
		t.Fatalf("tampered merge was ready=%v err=%v", ready, err)
	}
}
