package workflowruntime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

type blockingWorker struct {
	mu    sync.Mutex
	calls int
}

func (w *blockingWorker) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	w.mu.Lock()
	w.calls++
	w.mu.Unlock()
	<-ctx.Done()
	return workflowworker.RunResult{Ref: ref}, ctx.Err()
}

func (w *blockingWorker) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func TestRuntimeCancelWaitsForInFlightTickAndStartsNoSecondTick(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-1"}
	worker := &blockingWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, &fakeEnsure{}, worker)
	runtime, err := NewRuntime(scheduler, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 3}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for worker.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("runtime did not enter its first tick")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	runtime.Cancel()
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := worker.count(); got != 1 {
		t.Fatalf("worker calls=%d, cancellation started another tick", got)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWaitCanBeBoundedWithoutDetachingLoop(t *testing.T) {
	worker := &blockingWorker{}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-2"}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, &fakeEnsure{}, worker)
	runtime, err := NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 4}); err != nil {
		t.Fatal(err)
	}
	for worker.count() == 0 {
		time.Sleep(time.Millisecond)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := runtime.Wait(waitCtx); err == nil {
		t.Fatal("bounded Wait returned before cancellation")
	}
	// Wait's context does not abandon the goroutine; Close still joins it.
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if worker.count() != 1 {
		t.Fatal("loop survived Close with an extra tick")
	}
}
