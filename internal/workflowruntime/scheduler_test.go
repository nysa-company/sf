package workflowruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type fakeTickets struct {
	tickets []store.Ticket
	err     error
}

func (f fakeTickets) ListTickets(context.Context, domain.Channel) ([]store.Ticket, error) {
	return f.tickets, f.err
}

type fakeEnsure struct {
	calls []worktreecoord.EnsureRequest
	err   error
}

func (f *fakeEnsure) Ensure(_ context.Context, request worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	f.calls = append(f.calls, request)
	if f.err != nil {
		return store.StoredWorktree{}, f.err
	}
	return store.StoredWorktree{Path: "/tmp/wt", State: "registered"}, nil
}

type fakeWorker struct {
	calls []domain.TicketRef
	err   error
}

func (f *fakeWorker) Run(_ context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	f.calls = append(f.calls, ref)
	return workflowworker.RunResult{Ref: ref}, f.err
}

func ticket(ref domain.TicketRef, state domain.State) store.Ticket {
	return store.Ticket{Ref: ref, State: state, Version: 4, RunnerEpoch: 7}
}

func TestSchedulerIgnoresQueuedAndInvokesOneStableFirstTicket(t *testing.T) {
	queued := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "queued"}, domain.StateQueued)
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "b", Ticket: "second"}, domain.StatePlanning)
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StateVerifying)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := Scheduler{Channel: domain.ChannelDev, Tickets: fakeTickets{tickets: []store.Ticket{queued, second, first}}, Worktrees: ensurer, Worker: worker}
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9, RunnerEpoch: 7})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != first.Ref {
		t.Fatalf("result=%+v calls=%v", result, worker.calls)
	}
	if len(ensurer.calls) != 1 || ensurer.calls[0].Ref != first.Ref || ensurer.calls[0].Version != first.Version || ensurer.calls[0].Fence.LeaderEpoch != 9 {
		t.Fatalf("ensure calls=%+v", ensurer.calls)
	}
}

func TestSchedulerDoesNotRunSecondPhaseAndBusyIsBenign(t *testing.T) {
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StatePlanning)
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "second"}, domain.StateBuilding)
	ensurer := &fakeEnsure{err: store.ErrBusy}
	worker := &fakeWorker{}
	scheduler := Scheduler{Channel: domain.ChannelDev, Tickets: fakeTickets{tickets: []store.Ticket{second, first}}, Worktrees: ensurer, Worker: worker}
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1, RunnerEpoch: 7})
	if result.Outcome != OutcomeBusy || len(worker.calls) != 0 || len(ensurer.calls) != 2 {
		t.Fatalf("result=%+v ensure=%d worker=%d", result, len(ensurer.calls), len(worker.calls))
	}
}

func TestSchedulerBindsEachTicketRunnerEpochToTheSameLeader(t *testing.T) {
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StatePlanning)
	first.RunnerEpoch = 3
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "second"}, domain.StatePlanning)
	second.RunnerEpoch = 8
	ensurer := &fakeEnsure{err: store.ErrBusy}
	scheduler := Scheduler{Channel: domain.ChannelDev, Tickets: fakeTickets{tickets: []store.Ticket{first, second}}, Worktrees: ensurer, Worker: &fakeWorker{}}
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 12})
	if result.Outcome != OutcomeBusy || len(ensurer.calls) != 2 || ensurer.calls[0].Fence != (domain.Fence{LeaderEpoch: 12, RunnerEpoch: 3}) || ensurer.calls[1].Fence != (domain.Fence{LeaderEpoch: 12, RunnerEpoch: 8}) {
		t.Fatalf("result=%+v calls=%+v", result, ensurer.calls)
	}
}

func TestSchedulerCancellationAndInvalidFenceAreTyped(t *testing.T) {
	worker := &fakeWorker{}
	ensurer := &fakeEnsure{}
	scheduler := Scheduler{Channel: domain.ChannelDev, Tickets: fakeTickets{}, Worktrees: ensurer, Worker: worker}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result := scheduler.Tick(ctx, domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}); result.Outcome != OutcomeCanceled || !errors.Is(result.Err, ErrCanceled) {
		t.Fatalf("canceled result=%+v", result)
	}
	if result := scheduler.Tick(context.Background(), domain.Fence{}); result.Outcome != OutcomeReadiness || !errors.Is(result.Err, ErrReadiness) {
		t.Fatalf("invalid fence result=%+v", result)
	}
}

func TestSchedulerMapsInProgressWithoutProviderText(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}
	ensurer := &fakeEnsure{err: worktreecoord.ErrInProgress}
	scheduler := Scheduler{Channel: domain.ChannelDev, Tickets: fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, Worktrees: ensurer, Worker: &fakeWorker{}}
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1, RunnerEpoch: 7})
	if result.Outcome != OutcomeInProgress || !errors.Is(result.Err, ErrInProgress) || result.Err.Error() != ErrInProgress.Error() {
		t.Fatalf("result=%+v", result)
	}
}
