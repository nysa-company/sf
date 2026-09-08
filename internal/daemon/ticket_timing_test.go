package daemon

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestTicketTimingIncludesQueuePauseAndClampsClock(t *testing.T) {
	created := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	for _, state := range []domain.State{domain.StateQueued, domain.StatePlanning, domain.StatePaused} {
		value := store.Ticket{State: state, CreatedAt: created, MaxDuration: time.Hour}
		view := ticketTiming(value, created.Add(20*time.Minute))
		if view["age_seconds"] != int64(1200) || view["remaining_seconds"] != int64(2400) || view["deadline_elapsed"] != false {
			t.Fatalf("state=%s view=%v", state, view)
		}
		view = ticketTiming(value, created.Add(2*time.Hour))
		if view["remaining_seconds"] != int64(0) || view["deadline_elapsed"] != true {
			t.Fatalf("expired=%v", view)
		}
		view = ticketTiming(value, created.Add(-time.Hour))
		if view["age_seconds"] != int64(0) || view["remaining_seconds"] != int64(3600) {
			t.Fatalf("future created=%v", view)
		}
	}
	terminal := ticketTiming(store.Ticket{State: domain.StateDone, CreatedAt: created, MaxDuration: time.Hour}, created.Add(2*time.Hour))
	if _, ok := terminal["remaining_seconds"]; ok {
		t.Fatal("terminal ticket showed active countdown")
	}
	if ticketTiming(store.Ticket{}, created)["available"] != false {
		t.Fatal("invented missing budget")
	}
}

func TestStatusBudgetClockUsesStoredSubmissionBudget(t *testing.T) {
	d, paths, _ := testDaemon(t)
	path := writeTicket(t, t.TempDir(), "Budget clock")
	if code, output, _ := executeCLI(t, context.Background(), paths, "submit", path, "--project", "demo", "--json"); code != 0 {
		t.Fatalf("submit=%s", output)
	}
	code, output, _ := executeCLI(t, context.Background(), paths, "status", "SF-test-1", "--json")
	var response api.Response
	var data struct {
		Budget struct {
			Available bool   `json:"available"`
			Deadline  string `json:"deadline_at"`
			Remaining int64  `json:"remaining_seconds"`
		} `json:"budget_clock"`
	}
	if code != 0 || json.Unmarshal([]byte(output), &response) != nil || json.Unmarshal(response.Data, &data) != nil {
		t.Fatalf("status=%s", output)
	}
	stored, err := d.store.TicketByID(context.Background(), domain.ChannelStable, "SF-test-1")
	if err != nil {
		t.Fatal(err)
	}
	if !data.Budget.Available || data.Budget.Deadline != stored.CreatedAt.Add(stored.MaxDuration).UTC().Format(time.RFC3339Nano) || data.Budget.Remaining != int64(stored.MaxDuration/time.Second) {
		t.Fatalf("budget=%+v ticket=%+v", data.Budget, stored)
	}
}
