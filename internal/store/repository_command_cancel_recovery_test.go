package store

import (
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestUnleasedRepositoryCommandCancellationCompletesAfterStartupRecovery(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "cancel-unleased-restart")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: intent.Ref, ExpectedVersion: intent.TicketVersion,
		From: domain.StatePlanning, To: domain.StateCancelling,
		Trigger: "operator_cancel", Fence: intent.Fence, EventPayload: `{}`,
	}); err != nil {
		t.Fatal(err)
	}
	control, err := db.ControlProof(ctx, intent.Ref)
	if err != nil || control.Drained() {
		t.Fatalf("unresolved issued command must prevent drain: drained=%v err=%v", control.Drained(), err)
	}
	leader, err := db.AcquireLeader(ctx, intent.Ref.Channel, "cancel-unleased-recovery")
	if err != nil {
		t.Fatal(err)
	}
	// This is the production startup order, before effect reconciliation and
	// runner fencing. Absence of a lease is closed-gate evidence, not success.
	if err := db.RecoverUnleasedRepositoryCommands(ctx, intent.Ref.Channel, leader); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileEffects(ctx, intent.Ref.Channel, leader); err != nil {
		t.Fatal(err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, intent.Ref.Channel, leader); err != nil {
		t.Fatal(err)
	}
	if lease, err := db.AcquireRepositoryCommand(ctx, claim); err == nil || lease != nil {
		t.Fatal("old issued claim launched after recovery")
	}
	effect, err := db.Effect(ctx, intent.SemanticKey)
	if err != nil || effect.State != EffectFailed {
		t.Fatalf("unleased command effect=%+v err=%v", effect, err)
	}
	if _, err := db.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}); err == nil {
		t.Fatal("recovery fabricated command evidence")
	}
	control, err = db.ControlProof(ctx, intent.Ref)
	if err != nil || !control.Drained() {
		t.Fatalf("recovered command still prevents drain: drained=%v err=%v", control.Drained(), err)
	}
	current, err := db.Ticket(ctx, intent.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteControlTransition(ctx, Transition{
		Ref: intent.Ref, ExpectedVersion: current.Version,
		From: domain.StateCancelling, To: domain.StateCancelled,
		Trigger: "process_and_effects_drained", EventPayload: `{}`,
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch},
	}); err != nil {
		t.Fatal(err)
	}
	current, err = db.Ticket(ctx, intent.Ref)
	if err != nil || current.State != domain.StateCancelled {
		t.Fatalf("cancel completion=%+v err=%v", current, err)
	}
}
