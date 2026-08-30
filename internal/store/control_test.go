package store

import (
	"errors"
	"sync"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestTransitionAndInvalidateRunnerIsAtomicAndFenced(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/SF-control/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	oldFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	result, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping,
		ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: oldFence, EventPayload: `{"operator_uid":501}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if stopping.State != domain.StateStopping || stopping.ResumeState != domain.StatePlanning || stopping.Version != result.Version || stopping.RunnerEpoch != started.RunnerEpoch+1 {
		t.Fatalf("stopping=%+v result=%+v", stopping, result)
	}
	if _, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, Trigger: "process_and_effects_drained", Fence: oldFence, EventPayload: "{}"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old runner completed after control transition: %v", err)
	}
	events, err := database.Events(ctx, domain.ChannelDev, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := events[len(events)-1]; got.Trigger != "operator_pause_or_take" || got.From != domain.StatePlanning || got.To != domain.StateStopping || got.TicketVersion != stopping.Version {
		t.Fatalf("control event=%+v", got)
	}
}

func TestConcurrentControlTransitionInvalidatesRunnerOnce(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-race"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-race-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	before, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	request := Transition{
		Ref: ref, ExpectedVersion: before.Version, From: before.State, To: domain.StateCancelling,
		Trigger: "operator_cancel", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: before.RunnerEpoch}, EventPayload: "{}",
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := database.TransitionAndInvalidateRunner(ctx, request)
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	success, stale := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrStaleFence):
			stale++
		default:
			t.Fatalf("unexpected control race error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	after, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.StateCancelling || after.Version != before.Version+1 || after.RunnerEpoch != before.RunnerEpoch+1 {
		t.Fatalf("after=%+v before=%+v", after, before)
	}
}

func TestTransitionAndInvalidateRunnerRejectsInvalidPayloadWithoutMutation(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-invalid"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-invalid-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	before, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: before.Version, From: before.State, To: domain.StateCancelling,
		Trigger: "operator_cancel", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: before.RunnerEpoch}, EventPayload: "not-json",
	})
	if err == nil {
		t.Fatal("invalid event payload was accepted")
	}
	after, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != before.State || after.Version != before.Version || after.RunnerEpoch != before.RunnerEpoch {
		t.Fatalf("invalid transition mutated ticket: before=%+v after=%+v", before, after)
	}
}
