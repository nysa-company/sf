package workflowruntime

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

const (
	concurrencyStressSeeds = 50
	concurrencyStressWait  = 3 * time.Second
)

// TestRuntimePoolConcurrencyStress exercises the supported two-loop Runtime,
// not provider delivery or a complete ticket lifecycle. Each deterministic
// seed changes only the TicketSource ordering and the drained target; Scheduler
// must still admit the same first two unique ticket identities concurrently.
func TestRuntimePoolConcurrencyStress(t *testing.T) {
	for seed := 0; seed < concurrencyStressSeeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			tickets, ordered := stressTicketSnapshot(int64(seed))
			worker := newPoolBlockingWorker()
			scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: tickets}, poolEnsure{}, worker)
			runtime, err := NewRuntimeWithConfig(scheduler, RuntimeConfig{Interval: time.Hour, Workers: 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Start(t.Context(), domain.Fence{LeaderEpoch: uint64(100 + seed)}); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				runtime.Cancel()
				ctx, cancel := context.WithTimeout(context.Background(), concurrencyStressWait)
				defer cancel()
				_ = runtime.Wait(ctx)
			})

			admitted := receiveStressRefs(t, worker.entered, 2)
			assertStressRefSet(t, admitted, ordered[:2])
			assertStressWorkerSnapshot(t, worker, ordered[:2], 2)

			target := ordered[seed%2]
			drainCtx, cancelDrain := context.WithTimeout(t.Context(), concurrencyStressWait)
			err = runtime.ControlBundle().Drain(drainCtx, target)
			cancelDrain()
			if err != nil {
				t.Fatalf("Drain(%v): %v", target, err)
			}
			assertStressRefSet(t, receiveStressRefs(t, worker.exited, 1), []domain.TicketRef{target})
			calls, active, maxActive := worker.snapshot()
			if calls[target] != 1 || len(active) != 1 || maxActive != 2 {
				t.Fatalf("target drain changed unrelated activity: calls=%v active=%v max=%d", calls, active, maxActive)
			}
			sibling := ordered[(seed+1)%2]
			if _, live := active[sibling]; !live {
				t.Fatalf("target drain canceled sibling %v: active=%v", sibling, active)
			}

			runtime.Cancel()
			waitCtx, cancelWait := context.WithTimeout(t.Context(), concurrencyStressWait)
			err = runtime.Wait(waitCtx)
			cancelWait()
			if err != nil {
				t.Fatalf("runtime shutdown: %v", err)
			}
			assertStressRefSet(t, receiveStressRefs(t, worker.exited, 1), []domain.TicketRef{sibling})
			assertStressWorkersGone(t, worker, ordered[:2], 2)
		})
	}
}

// TestSchedulerFourCallerConcurrencyStress deliberately does not widen the
// production Runtime worker cap of two. Four synchronized callers drive the
// same Scheduler directly to adversarially exercise its shared admission map.
// This is a simulated runtime lifecycle boundary, not a claim that v1 admits a
// four-worker production pool or delivers provider results in this test.
func TestSchedulerFourCallerConcurrencyStress(t *testing.T) {
	for seed := 0; seed < concurrencyStressSeeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			tickets, ordered := stressTicketSnapshot(int64(seed + concurrencyStressSeeds))
			worker := newPoolBlockingWorker()
			scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: tickets}, poolEnsure{}, worker)
			driverCtx, cancelDriver := context.WithCancel(t.Context())
			defer cancelDriver()
			start := make(chan struct{})
			results := make(chan TickResult, 4)
			for caller := 0; caller < 4; caller++ {
				go func() {
					<-start
					results <- scheduler.Tick(driverCtx, domain.Fence{LeaderEpoch: uint64(1_000 + seed)})
				}()
			}
			close(start)

			admitted := receiveStressRefs(t, worker.entered, 4)
			assertStressRefSet(t, admitted, ordered[:4])
			assertStressWorkerSnapshot(t, worker, ordered[:4], 4)

			target := ordered[seed%4]
			drainCtx, cancelDrain := context.WithTimeout(t.Context(), concurrencyStressWait)
			err := scheduler.admission.Stop(drainCtx, target)
			cancelDrain()
			if err != nil {
				t.Fatalf("admission Stop(%v): %v", target, err)
			}
			assertStressRefSet(t, receiveStressRefs(t, worker.exited, 1), []domain.TicketRef{target})
			calls, active, maxActive := worker.snapshot()
			if calls[target] != 1 || len(active) != 3 || maxActive != 4 {
				t.Fatalf("target stop changed caller capacity: calls=%v active=%v max=%d", calls, active, maxActive)
			}
			for _, sibling := range ordered[:4] {
				if sibling == target {
					continue
				}
				if _, live := active[sibling]; !live {
					t.Fatalf("target stop canceled sibling %v: active=%v", sibling, active)
				}
			}

			cancelDriver()
			remaining := make([]domain.TicketRef, 0, 3)
			for _, ref := range ordered[:4] {
				if ref != target {
					remaining = append(remaining, ref)
				}
			}
			assertStressRefSet(t, receiveStressRefs(t, worker.exited, 3), remaining)
			resultRefs := make([]domain.TicketRef, 0, 4)
			for _, result := range receiveStressResults(t, results, 4) {
				if result.Outcome != OutcomeCanceled {
					t.Fatalf("canceled stress tick result=%+v", result)
				}
				resultRefs = append(resultRefs, result.Ref)
			}
			assertStressRefSet(t, resultRefs, ordered[:4])
			assertStressWorkersGone(t, worker, ordered[:4], 4)
		})
	}
}

func stressTicketSnapshot(seed int64) ([]store.Ticket, []domain.TicketRef) {
	refs := []domain.TicketRef{
		{Channel: domain.ChannelDev, Project: "alpha", Ticket: "SF-stress-07"},
		{Channel: domain.ChannelDev, Project: "beta", Ticket: "SF-stress-01"},
		{Channel: domain.ChannelDev, Project: "alpha", Ticket: "SF-stress-03"},
		{Channel: domain.ChannelDev, Project: "gamma", Ticket: "SF-stress-02"},
		{Channel: domain.ChannelDev, Project: "beta", Ticket: "SF-stress-04"},
		{Channel: domain.ChannelDev, Project: "alpha", Ticket: "SF-stress-05"},
		{Channel: domain.ChannelDev, Project: "gamma", Ticket: "SF-stress-06"},
		{Channel: domain.ChannelDev, Project: "beta", Ticket: "SF-stress-08"},
	}
	ordered := append([]domain.TicketRef(nil), refs...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Project != ordered[j].Project {
			return ordered[i].Project < ordered[j].Project
		}
		return ordered[i].Ticket < ordered[j].Ticket
	})
	tickets := make([]store.Ticket, 0, len(refs)*2)
	for index, ref := range refs {
		tickets = append(tickets, ticket(ref, domain.StatePlanning))
		if index%2 == 0 {
			// Duplicate source rows model stale/replayed listing data. Admission,
			// rather than source cleanliness, owns exactly-once activity.
			tickets = append(tickets, ticket(ref, domain.StatePlanning))
		}
	}
	random := rand.New(rand.NewSource(seed))
	random.Shuffle(len(tickets), func(i, j int) { tickets[i], tickets[j] = tickets[j], tickets[i] })
	return tickets, ordered
}

func receiveStressRefs(t *testing.T, values <-chan domain.TicketRef, count int) []domain.TicketRef {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), concurrencyStressWait)
	defer cancel()
	result := make([]domain.TicketRef, 0, count)
	for len(result) < count {
		select {
		case value := <-values:
			result = append(result, value)
		case <-ctx.Done():
			t.Fatalf("received refs=%v, want %d: %v", result, count, ctx.Err())
		}
	}
	return result
}

func receiveStressResults(t *testing.T, values <-chan TickResult, count int) []TickResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), concurrencyStressWait)
	defer cancel()
	result := make([]TickResult, 0, count)
	for len(result) < count {
		select {
		case value := <-values:
			result = append(result, value)
		case <-ctx.Done():
			t.Fatalf("received results=%d, want %d: %v", len(result), count, ctx.Err())
		}
	}
	return result
}

func assertStressRefSet(t *testing.T, got, want []domain.TicketRef) {
	t.Helper()
	counts := make(map[domain.TicketRef]int, len(got))
	for _, ref := range got {
		counts[ref]++
	}
	if len(got) != len(want) || len(counts) != len(want) {
		t.Fatalf("refs=%v, want one each of %v", got, want)
	}
	for _, ref := range want {
		if counts[ref] != 1 {
			t.Fatalf("refs=%v, want exactly one %v", got, ref)
		}
	}
}

func assertStressWorkerSnapshot(t *testing.T, worker *poolBlockingWorker, admitted []domain.TicketRef, capacity int) {
	t.Helper()
	calls, active, maxActive := worker.snapshot()
	if len(calls) != capacity || len(active) != capacity || maxActive != capacity {
		t.Fatalf("worker snapshot calls=%v active=%v max=%d capacity=%d", calls, active, maxActive, capacity)
	}
	for _, ref := range admitted {
		if calls[ref] != 1 {
			t.Fatalf("worker calls=%v, want exactly one %v", calls, ref)
		}
		if _, live := active[ref]; !live {
			t.Fatalf("worker active=%v, want %v", active, ref)
		}
	}
}

func assertStressWorkersGone(t *testing.T, worker *poolBlockingWorker, admitted []domain.TicketRef, capacity int) {
	t.Helper()
	calls, active, maxActive := worker.snapshot()
	if len(calls) != capacity || len(active) != 0 || maxActive != capacity {
		t.Fatalf("shutdown snapshot calls=%v active=%v max=%d capacity=%d", calls, active, maxActive, capacity)
	}
	for _, ref := range admitted {
		if calls[ref] != 1 {
			t.Fatalf("shutdown calls=%v, want exactly one %v", calls, ref)
		}
	}
}
