package store

import (
	"encoding/json"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProviderBlockedRecoveryAfterLeaderReplacement(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same-leader"
		if restart {
			name = "replacement-leader"
		}
		t.Run(name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			setupProviderProject(t, db, ctx)
			leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "blocked-regression-original")
			if err != nil {
				t.Fatal(err)
			}
			ticket := setupProviderTicket(t, db, ctx, "SF-blocked-restart-regression", leader)
			originalLeader := leader
			fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
			blocked, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"host_repair_required"}`})
			if err != nil {
				t.Fatal(err)
			}
			if restart {
				leader, err = db.AcquireLeader(ctx, domain.ChannelDev, "blocked-regression-replacement")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
					t.Fatalf("startup with preserved blocked ticket: %v", err)
				}
				fence.LeaderEpoch = leader
			}
			controlPayload := `{"intent":"recover","operator":"fixture"}`
			recovered, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_recover", Fence: fence, EventPayload: controlPayload})
			if err != nil {
				t.Fatalf("recover authenticated provider blocker after restart=%v: %v", restart, err)
			}
			current, readErr := db.Ticket(ctx, ticket.Ref)
			if readErr != nil || current.State != domain.StatePlanning || recovered.Version != blocked.Version+1 {
				t.Fatalf("unexpected recovery result: %+v", recovered)
			}
			entry, err := loadCurrentProviderPhaseEntry(ctx, db.db, ticket.Ref, domain.PhasePlanning, current.Version, current.RunnerEpoch, leader)
			if err != nil || entry.Version != ticket.Version {
				t.Fatalf("recovery must retain authenticated phase and attempt window: %+v %v", entry, err)
			}
			if err := validateProviderAttemptEndpointAdvance(ctx, db.db, ticket.Ref, domain.PhasePlanning,
				providerAttemptEndpoint{version: ticket.Version, runner: ticket.RunnerEpoch, leader: originalLeader},
				providerAttemptEndpoint{version: current.Version, runner: current.RunnerEpoch, leader: leader}); err != nil {
				t.Fatalf("attempt accounting failed to cross authenticated recovery: %v", err)
			}
			var raw string
			if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE id=?`, recovered.EventID).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var bridge providerBlockedLeaderBridge
			if err := json.Unmarshal([]byte(raw), &bridge); err != nil || string(bridge.Control) != controlPayload || bridge.PriorLeader != originalLeader || bridge.Leader != leader {
				t.Fatalf("recovery lost operator or endpoint evidence: %+v %v", bridge, err)
			}
			if restart {
				if _, err := db.db.ExecContext(ctx, `UPDATE events SET payload='{"intent":"recover"}' WHERE id=?`, recovered.EventID); err != nil {
					t.Fatal(err)
				}
				if _, err := loadCurrentProviderPhaseEntry(ctx, db.db, ticket.Ref, domain.PhasePlanning, current.Version, current.RunnerEpoch, leader); err == nil {
					t.Fatal("leader replacement accepted without its durable endpoint bridge")
				}
				if _, err := db.db.ExecContext(ctx, `UPDATE events SET payload=? WHERE id=?`, raw, recovered.EventID); err != nil {
					t.Fatal(err)
				}
			}
			nextLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "blocked-regression-next")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, nextLeader); err != nil {
				t.Fatalf("restart after blocked recovery: %v", err)
			}
			current, err = db.Ticket(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := loadCurrentProviderPhaseEntry(ctx, db.db, ticket.Ref, domain.PhasePlanning, current.Version, current.RunnerEpoch, nextLeader); err != nil {
				t.Fatalf("phase entry after subsequent restart: %v", err)
			}
		})
	}
}
