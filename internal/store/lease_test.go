package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func createLeaseTicket(t *testing.T, database *Store, index int) domain.TicketRef {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID(fmt.Sprintf("SF-lease-%d", index))}
	if err := database.CreateTicket(context.Background(), ticket(ref, fmt.Sprintf("lease-digest-%d", index))); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestLeaseSetIsAtomicIdempotentAndFenced(t *testing.T) {
	database, ctx := openTestStore(t)
	first := createLeaseTicket(t, database, 1)
	second := createLeaseTicket(t, database, 2)
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "lease-daemon")
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
	at := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	requests := []LeaseRequest{{Scope: "provider", Resource: "cursor/v1", Capacity: 1}, {Scope: "global", Resource: "machine", Capacity: 2}}
	acquired, err := database.AcquireLeases(ctx, first, 1, fence, requests, at)
	if err != nil || len(acquired) != 2 {
		t.Fatalf("acquired=%+v err=%v", acquired, err)
	}
	replayed, err := database.AcquireLeases(ctx, first, 1, fence, requests, at.Add(time.Hour))
	if err != nil || len(replayed) != 2 || !replayed[0].AcquiredAt.Equal(acquired[0].AcquiredAt) {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}

	// Project capacity is tentatively available and sorts before provider. The
	// full provider dimension must roll the project insert back.
	_, err = database.AcquireLeases(ctx, second, 1, fence, []LeaseRequest{
		{Scope: "project", Resource: "nysa", Capacity: 2},
		{Scope: "provider", Resource: "cursor/v1", Capacity: 1},
	}, at)
	if !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
	leases, err := database.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 2 {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
	for _, lease := range leases {
		if lease.Ref != first {
			t.Fatalf("partial lease escaped rollback: %+v", leases)
		}
	}

	updated, err := database.InvalidateRunner(ctx, first, 1, fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ReleaseLeases(ctx, first, updated.Version, fence); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old runner released leases: %v", err)
	}
	stale, err := database.StaleLeases(ctx, domain.ChannelDev, leader)
	if err != nil || len(stale) != 2 {
		t.Fatalf("stale=%+v err=%v", stale, err)
	}
	stillHeld, err := database.Leases(ctx, domain.ChannelDev)
	if err != nil || len(stillHeld) != 2 {
		t.Fatalf("runner invalidation released capacity without drain proof: leases=%+v err=%v", stillHeld, err)
	}
	released, err := database.ReleaseInvalidatedLeases(ctx, first, 1, leader)
	if err != nil || released != 2 {
		t.Fatalf("released=%d err=%v", released, err)
	}
}

func TestLeaseCapacityIsBoundedUnderConcurrency(t *testing.T) {
	database, ctx := openTestStore(t)
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "concurrent-lease-daemon")
	if err != nil {
		t.Fatal(err)
	}
	refs := []domain.TicketRef{
		createLeaseTicket(t, database, 10),
		createLeaseTicket(t, database, 11),
		createLeaseTicket(t, database, 12),
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	results := make(chan error, len(refs))
	for _, ref := range refs {
		ref := ref
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			_, err := database.AcquireLeases(callCtx, ref, 1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 2}}, time.Now().UTC())
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	var admitted, full int
	for err := range results {
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrLeaseCapacity):
			full++
		default:
			t.Fatalf("unexpected acquisition error: %v", err)
		}
	}
	if admitted != 2 || full != 1 {
		t.Fatalf("admitted=%d full=%d", admitted, full)
	}
	leases, err := database.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 2 {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
}
