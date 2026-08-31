package store

import (
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestTypedBlockerRequiresCanonicalCodeAndPreservesItAcrossUnrelatedTransition(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-typed-blocker"}
	if err := database.CreateTicket(ctx, ticket(ref, "typed-blocker")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, ref.Channel, "typed-blocker")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-typed-blocker/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`{}`, `{"code":"not canonical"}`, `{"code":"../unsafe"}`} {
		if _, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: payload}); err == nil {
			t.Fatalf("invalid typed blocker payload accepted: %s", payload)
		}
	}
	blocked, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{"code":"publication_runtime_unavailable","reason":"fixture"}`})
	if err != nil {
		t.Fatal(err)
	}
	value, err := database.Ticket(ctx, ref)
	if err != nil || value.BlockedCode != "publication_runtime_unavailable" {
		t.Fatalf("blocked ticket=%+v err=%v", value, err)
	}
	if _, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePlanning, Trigger: "operator_recover", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	value, err = database.Ticket(ctx, ref)
	if err != nil || value.BlockedCode != "publication_runtime_unavailable" {
		t.Fatalf("unrelated transition cleared blocker=%+v err=%v", value, err)
	}
}
