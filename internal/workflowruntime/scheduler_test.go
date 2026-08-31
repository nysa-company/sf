package workflowruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type fakeTickets struct {
	tickets []store.Ticket
	err     error
}

type currentFakeTickets struct {
	fakeTickets
	current store.Ticket
}

func (f currentFakeTickets) Ticket(context.Context, domain.TicketRef) (store.Ticket, error) {
	return f.current, nil
}

func (f fakeTickets) ListTickets(context.Context, domain.Channel) ([]store.Ticket, error) {
	return f.tickets, f.err
}

func (f fakeTickets) Ticket(_ context.Context, ref domain.TicketRef) (store.Ticket, error) {
	for _, ticket := range f.tickets {
		if ticket.Ref == ref {
			return ticket, nil
		}
	}
	return store.Ticket{}, store.ErrNotFound
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
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{queued, second, first}}, ensurer, worker)
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9, RunnerEpoch: 7})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != first.Ref {
		t.Fatalf("result=%+v calls=%v", result, worker.calls)
	}
	if len(ensurer.calls) != 1 || ensurer.calls[0].Ref != first.Ref || ensurer.calls[0].Version != first.Version || ensurer.calls[0].Fence.LeaderEpoch != 9 {
		t.Fatalf("ensure calls=%+v", ensurer.calls)
	}
}

func TestSchedulerInvokesReviewingTicket(t *testing.T) {
	reviewing := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "review"}, domain.StateReviewing)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{reviewing}}, ensurer, worker)
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != reviewing.Ref {
		t.Fatalf("result=%+v calls=%v", result, worker.calls)
	}
}

func TestSchedulerDoesNotRunSecondPhaseAndBusyIsBenign(t *testing.T) {
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StatePlanning)
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "second"}, domain.StateBuilding)
	ensurer := &fakeEnsure{err: store.ErrBusy}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{second, first}}, ensurer, worker)
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1, RunnerEpoch: 7})
	if result.Outcome != OutcomeBusy || len(worker.calls) != 0 || len(ensurer.calls) != 2 {
		t.Fatalf("result=%+v ensure=%d worker=%d", result, len(ensurer.calls), len(worker.calls))
	}
}

func TestSchedulerAdmitsPublishingExactlyOnceAndLeavesWaitingCIInert(t *testing.T) {
	publishing := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "publishing"}, domain.StatePublishing)
	waiting := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "b", Ticket: "waiting"}, domain.StateWaitingCI)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{waiting, publishing}}, ensurer, worker)
	fence := domain.Fence{LeaderEpoch: 9}
	first := scheduler.Tick(context.Background(), fence)
	if first.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != publishing.Ref {
		t.Fatalf("first result=%+v calls=%v", first, worker.calls)
	}
	if len(ensurer.calls) != 1 || ensurer.calls[0].Ref != publishing.Ref {
		t.Fatalf("first ensure calls=%v", ensurer.calls)
	}
	idle := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{waiting}}, &fakeEnsure{}, &fakeWorker{})
	result := idle.Tick(context.Background(), fence)
	if result.Outcome != OutcomeIdle {
		t.Fatalf("waiting_ci result=%+v", result)
	}
}

func TestSchedulerPrePublishingAdmissionInvokesBlockerWithoutWorktree(t *testing.T) {
	publishing := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "publishing"}, domain.StatePublishing)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{publishing}}, ensurer, worker)
	scheduler.AdmitPublishing = false
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || len(ensurer.calls) != 0 {
		t.Fatalf("pre-publishing result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerBindsEachTicketRunnerEpochToTheSameLeader(t *testing.T) {
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StatePlanning)
	first.RunnerEpoch = 3
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "second"}, domain.StatePlanning)
	second.RunnerEpoch = 8
	ensurer := &fakeEnsure{err: store.ErrBusy}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{first, second}}, ensurer, &fakeWorker{})
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 12})
	if result.Outcome != OutcomeBusy || len(ensurer.calls) != 2 || ensurer.calls[0].Fence != (domain.Fence{LeaderEpoch: 12, RunnerEpoch: 3}) || ensurer.calls[1].Fence != (domain.Fence{LeaderEpoch: 12, RunnerEpoch: 8}) {
		t.Fatalf("result=%+v calls=%+v", result, ensurer.calls)
	}
}

func TestSchedulerCancellationAndInvalidFenceAreTyped(t *testing.T) {
	worker := &fakeWorker{}
	ensurer := &fakeEnsure{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{}, ensurer, worker)
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
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, ensurer, &fakeWorker{})
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1, RunnerEpoch: 7})
	if result.Outcome != OutcomeInProgress || !errors.Is(result.Err, ErrInProgress) || result.Err.Error() != ErrInProgress.Error() {
		t.Fatalf("result=%+v", result)
	}
}

func TestSchedulerRejectsAdmissionAfterDurableTicketChange(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-durable-stop"}
	snapshot := ticket(ref, domain.StatePlanning)
	current := snapshot
	current.State = domain.StateStopping
	current.ResumeState = domain.StatePlanning
	current.Version++
	ensurer := &fakeEnsure{}
	scheduler := NewScheduler(domain.ChannelDev, currentFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{snapshot}}, current: current}, ensurer, &fakeWorker{})
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1})
	if result.Outcome != OutcomeStale || !errors.Is(result.Err, ErrStale) || len(ensurer.calls) != 0 {
		t.Fatalf("stale durable state admitted work: result=%+v ensure=%d", result, len(ensurer.calls))
	}
}

func TestDirectSchedulerConstructionCannotBypassAdmission(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-no-admission"}
	literal := &Scheduler{}
	if result := literal.Tick(context.Background(), domain.Fence{LeaderEpoch: 1}); result.Outcome != OutcomeReadiness || !errors.Is(result.Err, ErrInvalidScheduler) {
		t.Fatalf("literal scheduler bypassed admission: %+v", result)
	}
	if _, err := NewRuntime(literal, time.Millisecond); !errors.Is(err, ErrInvalidScheduler) {
		t.Fatalf("literal runtime=%v, want invalid scheduler", err)
	}
	if err := (&Runtime{Scheduler: literal, Interval: time.Millisecond, workers: 1}).Start(context.Background(), domain.Fence{LeaderEpoch: 1}); !errors.Is(err, ErrInvalidScheduler) {
		t.Fatalf("literal Runtime.Start=%v, want invalid scheduler", err)
	}
	var absent *admission
	if _, _, admitted := absent.Begin(context.Background(), ref, 1, 1, 1); admitted {
		t.Fatal("nil Admission admitted work")
	}
	if err := absent.Stop(context.Background(), ref); !errors.Is(err, ErrInvalidScheduler) {
		t.Fatalf("nil Admission.Stop=%v", err)
	}
}

func TestRearmTokenRejectsLifecycleChangeBeforeExactBegin(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-token-race"}
	admission := newAdmission()
	if err := admission.Stop(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := admission.Rearm(ref, 6, 1, 7, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	proved := ticket(ref, domain.StatePlanning)
	proved.Version, proved.RunnerEpoch = 6, 7
	changed := proved
	changed.Version++
	changed.State = domain.StateVerifying
	ensurer := &fakeEnsure{}
	scheduler := NewScheduler(domain.ChannelDev, currentFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{changed}}, current: changed}, ensurer, &fakeWorker{})
	scheduler.admission = admission
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1})
	if result.Outcome != OutcomeCanceled || len(ensurer.calls) != 0 {
		t.Fatalf("changed identity consumed rearm token: result=%+v ensures=%d", result, len(ensurer.calls))
	}
	_, end, admitted := admission.Begin(context.Background(), ref, proved.Version, 1, proved.RunnerEpoch)
	if !admitted {
		t.Fatal("stale lifecycle change cleared the exact token")
	}
	end()
}
