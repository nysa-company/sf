package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPostbuildRejectedAmendmentBlockerCannotReopen(t *testing.T) {
	db, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-rejected-amendment"}
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: "rejected-amendment", Type: domain.TicketBug, MergeMode: domain.MergeGuarded, State: domain.StateBuilding}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, ref.Channel, "rejected-amendment")
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
	blocked, err := db.Transition(ctx, Transition{Ref: ref, ExpectedVersion: 1, From: domain.StateBuilding, To: domain.StateBlocked, ResumeState: domain.StateBuilding, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"postbuild_amendment_rejected"}`})
	if err != nil {
		t.Fatal(err)
	}
	for _, trigger := range []string{"operator_recover", "operator_resume", "operator_retry"} {
		if _, err := db.Transition(ctx, Transition{Ref: ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StateBuilding, Trigger: trigger, Fence: fence, EventPayload: `{}`}); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("%s reopened rejected proof: %v", trigger, err)
		}
	}
	current, err := db.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateBlocked || current.Version != blocked.Version || current.BlockedCode != postbuildAmendmentRejectedBlockerCode {
		t.Fatalf("changed rejected ticket: %+v %v", current, err)
	}
}
