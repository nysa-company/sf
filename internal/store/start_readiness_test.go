package store

import (
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestStartReadinessRefusesConfigurationAdvanceWithoutMutation(t *testing.T) {
	db, ctx := openTestStore(t)
	project := testConfigurationProject(t, "start-readiness", "/tmp/start-readiness", 2)
	if err := db.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: project.Channel, Project: project.ID, Ticket: "SF-start-readiness"}
	if err := db.CreateTicket(ctx, ticket(ref, "start-readiness")); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, ref.Channel, "start-readiness")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	checked, _, err := db.StartPreflight(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	current, _, err := db.ApplyProjectConfiguration(ctx, nextProjectConfiguration(t, project, 1))
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}
	workflow := "dev/start-readiness/SF-start-readiness/planning"
	_, _, err = db.StartWithCheckedProjectOwnership(ctx, ref, queued.Version, fence, workflow, time.Now().UTC(), checked)
	if !errors.Is(err, ErrStartConfigurationChanged) {
		t.Fatalf("stale check: %v", err)
	}
	after, err := db.Ticket(ctx, ref)
	if err != nil || after.State != domain.StateQueued || after.Version != queued.Version || after.WorkflowID != "" {
		t.Fatalf("refusal changed ticket: %+v %v", after, err)
	}
	for _, table := range []string{"leases", "workflow_owners", "provider_phase_entries", "runner_start_authorities"} {
		var count int
		if err := db.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE channel=? AND project_id=? AND ticket_id=?", ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != 0 {
			t.Fatalf("refusal created %s: count=%d err=%v", table, count, err)
		}
	}
	started, observed, err := db.StartWithCheckedProjectOwnership(ctx, ref, queued.Version, fence, workflow, time.Now().UTC(), current)
	if err != nil || observed || started.ConfigDigest != current.ConfigDigest || started.ConfigGeneration != current.ConfigGeneration {
		t.Fatalf("fresh check: %+v observed=%v err=%v", started, observed, err)
	}
	assertStartPlanningProviderPhaseEntry(t, db, ctx, started, leader)
	_, observed, err = db.StartWithCheckedProjectOwnership(ctx, ref, started.Version, fence, workflow, time.Now().UTC(), current)
	if err != nil || !observed {
		t.Fatalf("replay: observed=%v err=%v", observed, err)
	}
}
