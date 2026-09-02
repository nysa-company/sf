package store

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestStartWithOwnershipRecordsPlanningProviderPhaseEntry(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-start-provider-entry"}
	if err := database.CreateTicket(ctx, ticket(ref, "start-provider-entry")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "start-provider-entry")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, observed, err := database.StartWithOwnership(ctx, ref, queued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}, "dev/nysa/SF-start-provider-entry/planning", []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 2}, {Scope: "project", Resource: "nysa", Capacity: 2}}, time.Now().UTC())
	if err != nil || observed {
		t.Fatalf("start=%+v observed=%v err=%v", started, observed, err)
	}
	assertStartPlanningProviderPhaseEntry(t, database, ctx, started, leader)
}

func TestStartWithProjectOwnershipRecordsPlanningEntryAndConfigurationSnapshot(t *testing.T) {
	database, ctx := openTestStore(t)
	project := testConfigurationProject(t, "start-configured", "/tmp/start-configured", 2)
	if err := database.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: project.Channel, Project: project.ID, Ticket: "SF-start-configured"}
	if err := database.CreateTicket(ctx, ticket(ref, "start-configured")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "start-configured")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, observed, err := database.StartWithProjectOwnership(ctx, ref, queued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}, "dev/start-configured/SF-start-configured/planning", time.Now().UTC())
	if err != nil || observed {
		t.Fatalf("start=%+v observed=%v err=%v", started, observed, err)
	}
	if started.ConfigGeneration != project.ConfigGeneration || started.ConfigDigest != project.ConfigDigest || !bytes.Equal(started.ConfigSnapshot, project.ConfigSnapshot) {
		t.Fatalf("ticket did not bind configured snapshot: %+v", started)
	}
	assertStartPlanningProviderPhaseEntry(t, database, ctx, started, leader)
}

func TestStartWithProjectOwnershipGenerationZeroRecordsPlanningEntry(t *testing.T) {
	database, ctx := openTestStore(t)
	project := Project{Channel: domain.ChannelDev, ID: "start-generation-zero", Path: "/tmp/start-generation-zero", BaseRef: "main"}
	if err := database.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: project.Channel, Project: project.ID, Ticket: "SF-start-generation-zero"}
	if err := database.CreateTicket(ctx, ticket(ref, "start-generation-zero")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "start-generation-zero")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, observed, err := database.StartWithProjectOwnership(ctx, ref, queued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}, "dev/start-generation-zero/SF-start-generation-zero/planning", time.Now().UTC())
	if err != nil || observed {
		t.Fatalf("start=%+v observed=%v err=%v", started, observed, err)
	}
	if started.ConfigGeneration != 0 || started.ConfigDigest != "" || len(started.ConfigSnapshot) != 0 {
		t.Fatalf("generation-zero admission bound configuration: %+v", started)
	}
	assertStartPlanningProviderPhaseEntry(t, database, ctx, started, leader)
}

func TestStartWithProjectOwnershipRollsBackWhenPlanningEntryCannotPersist(t *testing.T) {
	database, ctx := openTestStore(t)
	project := testConfigurationProject(t, "start-rollback", "/tmp/start-rollback", 2)
	if err := database.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: project.Channel, Project: project.ID, Ticket: "SF-start-rollback"}
	if err := database.CreateTicket(ctx, ticket(ref, "start-rollback")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "start-rollback")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	baselineCounts := make(map[string]int)
	for _, table := range []string{"leases", "workflow_owners", "events", "provider_phase_entries", "runner_start_authorities"} {
		var count int
		if err := database.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE channel=? AND project_id=? AND ticket_id=?", ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil {
			t.Fatalf("baseline count %s: %v", table, err)
		}
		baselineCounts[table] = count
	}
	if _, err := database.db.ExecContext(ctx, `CREATE TRIGGER test_start_provider_phase_entry_failure BEFORE INSERT ON provider_phase_entries BEGIN SELECT RAISE(ABORT, 'injected planning entry failure'); END`); err != nil {
		t.Fatal(err)
	}

	if _, _, err := database.StartWithProjectOwnership(ctx, ref, queued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}, "dev/start-rollback/SF-start-rollback/planning", time.Now().UTC()); err == nil {
		t.Fatal("start unexpectedly succeeded after provider phase-entry failure")
	}
	after, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != queued.State || after.Version != queued.Version || after.RunnerEpoch != queued.RunnerEpoch || after.WorkflowID != queued.WorkflowID || after.ConfigGeneration != queued.ConfigGeneration || after.ConfigDigest != queued.ConfigDigest || !bytes.Equal(after.ConfigSnapshot, queued.ConfigSnapshot) {
		t.Fatalf("failed admission changed ticket: queued=%+v after=%+v", queued, after)
	}
	for table, baseline := range baselineCounts {
		var count int
		if err := database.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE channel=? AND project_id=? AND ticket_id=?", ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != baseline {
			t.Fatalf("failed admission changed %s rows: before=%d after=%d", table, baseline, count)
		}
	}
	loadedProject, err := database.Project(ctx, project.Channel, project.ID)
	if err != nil || loadedProject.ConfigGeneration != project.ConfigGeneration || loadedProject.ConfigDigest != project.ConfigDigest || !bytes.Equal(loadedProject.ConfigSnapshot, project.ConfigSnapshot) {
		t.Fatalf("failed admission changed project configuration: project=%+v err=%v", loadedProject, err)
	}
}

func assertStartPlanningProviderPhaseEntry(t *testing.T, database *Store, ctx context.Context, started Ticket, leader uint64) {
	t.Helper()
	var version, entryLeader, runner uint64
	var from, state domain.State
	var trigger string
	if err := database.db.QueryRowContext(ctx, `SELECT entry_ticket_version,entry_leader_epoch,entry_runner_epoch,entry_from_state,entry_state,entry_trigger FROM provider_phase_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=?`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket, domain.PhasePlanning).Scan(&version, &entryLeader, &runner, &from, &state, &trigger); err != nil {
		t.Fatal(err)
	}
	if version != started.Version || entryLeader != leader || runner != started.RunnerEpoch || from != domain.StateQueued || state != domain.StatePlanning || trigger != "operator_start" {
		t.Fatalf("planning entry version=%d leader=%d runner=%d from=%s state=%s trigger=%q", version, entryLeader, runner, from, state, trigger)
	}
}
