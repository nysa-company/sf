package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPostbuildCommandFailureBlockerCannotReopenBuilding(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-postbuild-command-failed"}
	if err := database.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: "postbuild-command-failed", Type: domain.TicketBug, MergeMode: domain.MergeGuarded, State: domain.StateBuilding}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, ref.Channel, "postbuild-command-failed")
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
	blocked, err := database.Transition(ctx, Transition{
		Ref: ref, ExpectedVersion: 1, From: domain.StateBuilding, To: domain.StateBlocked,
		ResumeState: domain.StateBuilding, Trigger: "typed_blocker", Fence: fence,
		EventPayload: `{"code":"postbuild_command_failed"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateBlocked || current.ResumeState != domain.StateBuilding || current.BlockedCode != postbuildCommandFailedBlockerCode {
		t.Fatalf("blocked ticket=%+v err=%v", current, err)
	}
	if _, err := database.Transition(ctx, Transition{
		Ref: ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StateBuilding,
		Trigger: "operator_recover", Fence: fence, EventPayload: `{}`,
	}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("post-build blocker recovery err=%v", err)
	}
	if current, err = database.Ticket(ctx, ref); err != nil || current.State != domain.StateBlocked || current.Version != blocked.Version {
		t.Fatalf("ticket changed after refused recovery=%+v err=%v", current, err)
	}
}
