package store

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
)

func guardedRecoveryFixture(t *testing.T, maximum domain.MergeMode) (*Store, context.Context, Ticket, uint64) {
	t.Helper()
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID("SF-guarded-recovery-" + string(maximum))}
	if err := database.CreateTicket(ctx, ticket(ref, "guarded-recovery-"+string(maximum))); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, ref.Channel, "guarded-recovery-"+string(maximum))
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/"+string(ref.Ticket)+"/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	machine := config.DefaultMachineLimits()
	project := config.DefaultProject("nysa", "/tmp/nysa")
	project.MergeMode = maximum
	if maximum == domain.MergeAutonomous {
		machine.AllowAutonomous = true
	}
	effective, err := config.Resolve(machine, project, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	// This represents a legacy or misconfigured autonomous ticket. The direct
	// Store recovery path must prove the frozen project maximum itself rather
	// than trusting an Engine attribute that claims guarded is permitted.
	if _, err := database.db.ExecContext(ctx, `UPDATE tickets SET merge_mode='autonomous',config_generation=9,config_digest=?,config_snapshot_bytes=? WHERE channel=? AND project_id=? AND id=?`, digest, snapshot, ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	blocked, err := database.Transition(ctx, Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning,
		Trigger: "typed_blocker", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{"code":"autonomy_ineligible"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := database.Ticket(ctx, ref)
	if err != nil || stored.Version != blocked.Version || stored.State != domain.StateBlocked {
		t.Fatalf("blocked=%+v stored=%+v err=%v", blocked, stored, err)
	}
	return database, ctx, stored, leader
}

func guardedRecoveryTransition(ticket Ticket, leader uint64) Transition {
	return Transition{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateBlocked, To: domain.StateBuilding,
		Trigger: "operator_recover_as_guarded", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch},
		EventPayload: `{"intent":"recover","mode":"guarded","blocked_code":"autonomy_ineligible"}`,
	}
}

func TestTransitionRecoverAsGuardedUsesFrozenProjectMaximum(t *testing.T) {
	for _, test := range []struct {
		maximum domain.MergeMode
		allow   bool
	}{
		{maximum: domain.MergeManual, allow: false},
		{maximum: domain.MergeGuarded, allow: true},
		{maximum: domain.MergeAutonomous, allow: true},
	} {
		t.Run(string(test.maximum), func(t *testing.T) {
			database, ctx, blocked, leader := guardedRecoveryFixture(t, test.maximum)
			result, err := database.Transition(ctx, guardedRecoveryTransition(blocked, leader))
			if !test.allow {
				if !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("manual guarded recovery err=%v, want evidence conflict", err)
				}
				stored, getErr := database.Ticket(ctx, blocked.Ref)
				if getErr != nil || stored.State != blocked.State || stored.Version != blocked.Version || stored.RunnerEpoch != blocked.RunnerEpoch || stored.MergeMode != blocked.MergeMode || stored.BlockedCode != blocked.BlockedCode {
					t.Fatalf("manual recovery mutated ticket=%+v before=%+v err=%v", stored, blocked, getErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			stored, getErr := database.Ticket(ctx, blocked.Ref)
			if getErr != nil || result.Version != blocked.Version+1 || stored.State != domain.StateBuilding || stored.MergeMode != domain.MergeGuarded || stored.BlockedCode != "" {
				t.Fatalf("result=%+v ticket=%+v err=%v", result, stored, getErr)
			}
		})
	}
}

func TestTransitionRecoverAsGuardedRejectsForgedPayloadAndPublicationEvidence(t *testing.T) {
	database, ctx, blocked, leader := guardedRecoveryFixture(t, domain.MergeGuarded)
	forged := guardedRecoveryTransition(blocked, leader)
	forged.EventPayload = `{"intent":"recover","mode":"guarded","blocked_code":"other"}`
	if _, err := database.Transition(ctx, forged); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("forged recovery attributes/payload err=%v, want evidence conflict", err)
	}
	if _, err := database.PlanEffect(ctx, EffectPlan{
		SemanticKey: "guarded-recovery-publication-effect", Ref: blocked.Ref, Kind: "branch_push", TicketVersion: blocked.Version,
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: blocked.RunnerEpoch}, RequestDigest: "guarded-recovery-publication-request",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Transition(ctx, guardedRecoveryTransition(blocked, leader)); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("publication evidence recovery err=%v, want control not drained", err)
	}
	stored, err := database.Ticket(ctx, blocked.Ref)
	if err != nil || stored.State != blocked.State || stored.Version != blocked.Version || stored.RunnerEpoch != blocked.RunnerEpoch || stored.MergeMode != blocked.MergeMode || stored.BlockedCode != blocked.BlockedCode {
		t.Fatalf("forged recovery mutated ticket=%+v before=%+v err=%v", stored, blocked, err)
	}
}
