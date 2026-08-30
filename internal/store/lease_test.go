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

func TestFenceRecoveredRunnersRollsBackAllTicketsAtGlobalLedgerCap(t *testing.T) {
	database, ctx := openTestStore(t)
	setupProviderProject(t, database, ctx)
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "global-ledger-cap-0")
	if err != nil {
		t.Fatal(err)
	}
	capped := setupProviderTicket(t, database, ctx, "SF-z-capped", leader)
	for recovery := 1; recovery <= 64; recovery++ {
		leader, err = database.AcquireLeader(ctx, domain.ChannelDev, fmt.Sprintf("global-ledger-cap-%d", recovery))
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
			t.Fatalf("cap seed recovery %d changed=%d err=%v", recovery, changed, err)
		}
	}
	uncapped := setupProviderTicket(t, database, ctx, "SF-a-uncapped", leader)
	beforeCapped, err := database.Ticket(ctx, capped.Ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeUncapped, err := database.Ticket(ctx, uncapped.Ref)
	if err != nil {
		t.Fatal(err)
	}
	leader, err = database.AcquireLeader(ctx, domain.ChannelDev, "global-ledger-cap-refusal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err == nil {
		t.Fatal("global recovery cap accepted a new leader")
	}
	afterCapped, err := database.Ticket(ctx, capped.Ref)
	if err != nil || afterCapped.Version != beforeCapped.Version || afterCapped.RunnerEpoch != beforeCapped.RunnerEpoch {
		t.Fatalf("capped ticket mutated before=%+v after=%+v err=%v", beforeCapped, afterCapped, err)
	}
	afterUncapped, err := database.Ticket(ctx, uncapped.Ref)
	if err != nil || afterUncapped.Version != beforeUncapped.Version || afterUncapped.RunnerEpoch != beforeUncapped.RunnerEpoch {
		t.Fatalf("uncapped ticket escaped startup rollback before=%+v after=%+v err=%v", beforeUncapped, afterUncapped, err)
	}
	var cappedRows, uncappedRows int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, capped.Ref.Channel, capped.Ref.Project, capped.Ref.Ticket).Scan(&cappedRows); err != nil || cappedRows != 64 {
		t.Fatalf("capped ledger rows=%d err=%v", cappedRows, err)
	}
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, uncapped.Ref.Channel, uncapped.Ref.Project, uncapped.Ref.Ticket).Scan(&uncappedRows); err != nil || uncappedRows != 0 {
		t.Fatalf("uncapped ledger rows=%d err=%v", uncappedRows, err)
	}
}

func staleCapacityFixture(t *testing.T, scopes []LeaseRequest) (*Store, context.Context, domain.TicketRef, uint64, uint64, []Lease) {
	t.Helper()
	database, ctx := openTestStore(t)
	ref := createLeaseTicket(t, database, 90)
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "lease-adoption")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	leases, err := database.AcquireLeases(ctx, ref, 1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}, scopes, at)
	if err != nil {
		t.Fatal(err)
	}
	current, err := database.InvalidateRunner(ctx, ref, 1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return database, ctx, ref, leader, current.RunnerEpoch, leases
}

func TestAdoptInvalidatedLeasesTransfersCapacityWithoutChangingSlots(t *testing.T) {
	database, ctx, ref, leader, currentRunner, before := staleCapacityFixture(t, []LeaseRequest{
		{Scope: "global", Resource: "machine", Capacity: 1},
		{Scope: "project", Resource: "nysa", Capacity: 1},
	})
	adopted, err := database.AdoptInvalidatedLeases(ctx, ref, 1, leader)
	if err != nil || adopted != 2 {
		t.Fatalf("adopted=%d err=%v", adopted, err)
	}
	after, err := database.Leases(ctx, domain.ChannelDev)
	if err != nil || len(after) != len(before) {
		t.Fatalf("leases=%+v err=%v", after, err)
	}
	for index, lease := range after {
		if lease.Ref != ref || lease.RunnerEpoch != currentRunner || lease.Scope != before[index].Scope || lease.ScopeKey != before[index].ScopeKey || !lease.AcquiredAt.Equal(before[index].AcquiredAt) {
			t.Fatalf("lease[%d]=%+v before=%+v", index, lease, before[index])
		}
	}
	// Ownership moves, rather than freeing capacity for a second ticket.
	second := createLeaseTicket(t, database, 91)
	if _, err := database.AcquireLeases(ctx, second, 1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}}, time.Now().UTC()); !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("adoption freed capacity: %v", err)
	}
	adopted, err = database.AdoptInvalidatedLeases(ctx, ref, 1, leader)
	if err != nil || adopted != 0 {
		t.Fatalf("adoption replay adopted=%d err=%v", adopted, err)
	}
}

func TestAdoptInvalidatedLeasesFencesLeaderAndRunner(t *testing.T) {
	database, ctx, ref, leader, _, _ := staleCapacityFixture(t, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}})
	if _, err := database.AdoptInvalidatedLeases(ctx, ref, 1, leader+1); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale leader adoption=%v", err)
	}
	if _, err := database.AdoptInvalidatedLeases(ctx, ref, 2, leader); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("non-stale runner adoption=%v", err)
	}
	if _, err := database.AdoptInvalidatedLeases(ctx, ref, 1, leader); err != nil {
		t.Fatalf("exact stale adoption=%v", err)
	}
}

func TestAdoptInvalidatedLeasesRefusesProviderScopesAndLiveProviderAttempts(t *testing.T) {
	t.Run("provider scope", func(t *testing.T) {
		database, ctx, ref, leader, _, _ := staleCapacityFixture(t, []LeaseRequest{{Scope: "provider", Resource: "provider/v1", Capacity: 1}})
		if _, err := database.AdoptInvalidatedLeases(ctx, ref, 1, leader); !errors.Is(err, ErrLeaseAdoption) {
			t.Fatalf("provider scope adoption=%v", err)
		}
	})
	t.Run("active or quarantined attempt", func(t *testing.T) {
		for _, state := range []string{"active", "quarantined"} {
			t.Run(state, func(t *testing.T) {
				database, ctx, ref, leader, _, _ := staleCapacityFixture(t, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}})
				outcome, launch := "running", "launching"
				if state == "quarantined" {
					outcome, launch = "undrained_recovery", "quarantined"
				}
				if _, err := database.db.ExecContext(ctx, `INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,launch_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, "planning", 1, "test", "model", "family", "v1", outcome, "planner", state, 0, time.Now().UTC().Format(time.RFC3339Nano), "", launch); err != nil {
					t.Fatal(err)
				}
				if _, err := database.AdoptInvalidatedLeases(ctx, ref, 1, leader); !errors.Is(err, ErrLeaseAdoption) {
					t.Fatalf("%s provider adoption=%v", state, err)
				}
			})
		}
	})
}
