package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProviderRetryReacquiresFrozenTicketCapacityAtomically(t *testing.T) {
	// Runtime replay requires confirmed creation authority, not merely a
	// mutable worktree registration. Reuse the authenticated retry fixture.
	fixture := newProviderRetryWorktreeFixture(t, "SF-retry-capacity", 40)
	db, ctx := fixture.db, fixture.ctx
	ticket, fence := fixture.ticket, fixture.fence
	leader := fence.LeaderEpoch
	if _, err := db.AcquireLeases(ctx, ticket.Ref, ticket.Version, fence, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 2}, {Scope: "project", Resource: "provider", Capacity: 2}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		claim, err := db.BeginProviderAttempt(ctx, fixture.request(t, domain.PhasePlanning, "planner", runtime(fixture.builder)))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	sealProviderRetryTest(t, db, ctx, ticket.Ref)
	if _, err := db.ReleaseLeases(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	first := startCapacityOccupant(t, db, ctx, ticket.Ref.Project, "SF-capacity-first", leader)
	second := startCapacityOccupant(t, db, ctx, ticket.Ref.Project, "SF-capacity-second", leader)
	transition := Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePaused, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_retry", Fence: fence}
	before, err := runtimeControlFrom(ctx, db.db, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderRetry(ctx, transition); !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("full capacity retry: %v", err)
	}
	after, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || after.State != ticket.State || after.Version != ticket.Version || after.ResumeState != ticket.ResumeState {
		t.Fatalf("refused retry advanced ticket: %v", err)
	}
	control, err := runtimeControlFrom(ctx, db.db, ticket.Ref)
	if err != nil || !reflect.DeepEqual(before, control) {
		t.Fatalf("refused retry advanced control: %v", err)
	}
	assertCapacityCount(t, db, ctx, ticket.Ref, 0)
	for _, query := range []string{
		`SELECT COUNT(*) FROM provider_retry_epochs WHERE channel=? AND project_id=? AND ticket_id=?`,
		`SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='operator_retry'`,
	} {
		var count int
		if err := db.db.QueryRowContext(ctx, query, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&count); err != nil || count != 0 {
			t.Fatalf("refusal wrote retry authority: count=%d err=%v", count, err)
		}
	}
	// Later project policy is not allowed to rewrite this ticket's frozen two
	// slots. With slot zero occupied, success below requires original slot one.
	project, err := db.Project(ctx, ticket.Ref.Channel, ticket.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.ApplyProjectConfiguration(ctx, nextProjectConfiguration(t, project, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReleaseLeases(ctx, second.Ref, second.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: second.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderRetry(ctx, transition); err != nil {
		t.Fatal(err)
	}
	assertCapacityCount(t, db, ctx, ticket.Ref, 2)
	resumed, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := db.ProviderRetryReplay(ctx, resumed); err != nil || !replay {
		t.Fatalf("retry replay=%v err=%v", replay, err)
	}
	if replay, err := db.ProviderRetryRuntimeReplay(ctx, resumed); err != nil || replay != ProviderRetryNeedsRearm {
		t.Fatalf("sealed runtime replay=%v err=%v", replay, err)
	}
	if _, err := db.TransitionProviderRetry(ctx, transition); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("duplicate transition=%v", err)
	}
	assertCapacityCount(t, db, ctx, ticket.Ref, 2)
	// Once the lower slot becomes free, reauthentication must reuse the exact
	// higher-slot ownership, not allocate a duplicate in the newly free slot.
	if _, err := db.ReleaseLeases(ctx, first.Ref, first.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: first.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	if err := db.write(ctx, func(conn *sql.Conn) error { return reacquireTicketCapacity(ctx, conn, ticket.Ref, ticket.RunnerEpoch) }); err != nil {
		t.Fatal(err)
	}
	assertCapacityCount(t, db, ctx, ticket.Ref, 2)
}

func startCapacityOccupant(t *testing.T, db *Store, ctx context.Context, project domain.ProjectID, id string, leader uint64) Ticket {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: project, Ticket: domain.TicketID(id)}
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: sha256Digest([]byte(id)), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, _, err := db.StartWithProjectOwnership(ctx, ref, 1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}, "dev/"+string(project)+"/"+id, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return ticket
}

func assertCapacityCount(t *testing.T, db *Store, ctx context.Context, ref domain.TicketRef, want int) {
	t.Helper()
	var count int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope IN ('global','project')`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != want {
		t.Fatalf("capacity leases=%d want=%d err=%v", count, want, err)
	}
}

func TestResumeCapacityRollsBackPartialAcquisitionAndRejectsStaleOwner(t *testing.T) {
	db, ctx := openTestStore(t)
	setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "partial-capacity")
	if err != nil {
		t.Fatal(err)
	}
	target := setupProviderTicket(t, db, ctx, "SF-partial-target", leader)
	for _, id := range []string{"SF-partial-first", "SF-partial-second"} {
		owner := setupProviderTicket(t, db, ctx, id, leader)
		if _, err := db.AcquireLeases(ctx, owner.Ref, owner.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: owner.RunnerEpoch}, []LeaseRequest{{Scope: "project", Resource: "provider", Capacity: 2}}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.write(ctx, func(conn *sql.Conn) error { return reacquireTicketCapacity(ctx, conn, target.Ref, target.RunnerEpoch) }); !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("partial capacity=%v", err)
	}
	assertCapacityCount(t, db, ctx, target.Ref, 0)
	if _, err := db.AcquireLeases(ctx, target.Ref, target.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: target.RunnerEpoch}, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 2}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := db.write(ctx, func(conn *sql.Conn) error {
		return reacquireTicketCapacity(ctx, conn, target.Ref, target.RunnerEpoch+1)
	}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale capacity=%v", err)
	}
	assertCapacityCount(t, db, ctx, target.Ref, 1)
}

func TestOrdinaryVerifyingResumeRequiresTicketCapacity(t *testing.T) {
	db, ctx, leader, ticket := operatorSourceResumePhaseFixture(t, true)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	if _, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateStopping, ResumeState: domain.StateVerifying, Trigger: "operator_pause_or_take", Fence: fence, EventPayload: `{"intent":"pause","operator":"sofia","operator_uid":501}`}); err != nil {
		t.Fatal(err)
	}
	stopped, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = stopped.RunnerEpoch
	worktree, err := db.Worktree(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	baseline := TakeoverRemoteBaseline{Registered: true, WorktreePath: worktree.Path, WorktreeBranch: worktree.Branch, WorktreeIdentity: sha256Digest(worktree.IdentityJSON), BaseOID: worktree.BaseSHA}
	drain, err := json.Marshal(map[string]any{"drained": true, "intent": "pause", "remote": baseline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteControlTransition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: stopped.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StateVerifying, Trigger: "process_and_effects_drained", Fence: fence, EventPayload: string(drain)}); err != nil {
		t.Fatal(err)
	}
	paused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	first := startCapacityOccupant(t, db, ctx, ticket.Ref.Project, "SF-resume-first", leader)
	startCapacityOccupant(t, db, ctx, ticket.Ref.Project, "SF-resume-second", leader)
	transition := Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateVerifying, Trigger: "operator_resume", Fence: fence, EventPayload: `{"change_kind":"none","changed_files":null,"intent":"resume","operator":"sofia"}`}
	if _, err := db.Transition(ctx, transition); !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("ordinary resume capacity=%v", err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || current.Version != paused.Version || current.State != domain.StatePaused {
		t.Fatalf("refused resume advanced: %v", err)
	}
	assertCapacityCount(t, db, ctx, ticket.Ref, 0)
	if _, err := db.ReleaseLeases(ctx, first.Ref, first.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: first.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(ctx, transition); err != nil {
		t.Fatal(err)
	}
	assertCapacityCount(t, db, ctx, ticket.Ref, 2)
}
