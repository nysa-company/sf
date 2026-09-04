package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestPendingProviderResultIndeterminateSurvivesRecoveryAndBlocksCanonically(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "result-indeterminate")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-result-indeterminate", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "result_indeterminate", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	})); !errors.Is(err, ErrProviderResultIndeterminate) {
		t.Fatalf("indeterminate result admitted a new attempt: %v", err)
	}
	pending, found, err := db.PendingProviderResultIndeterminate(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence)
	if err != nil || !found || pending.ID != claim.ID || !sameImmutableProviderAttemptClaim(pending, claim) {
		t.Fatalf("initial pending=%+v found=%v err=%v", pending, found, err)
	}

	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "result-indeterminate-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
		t.Fatalf("recovery changed=%d err=%v", changed, err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence = domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}
	pending, found, err = db.PendingProviderResultIndeterminate(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence)
	if err != nil || !found || pending.ID != claim.ID || pending.ExpectedVersion != claim.ExpectedVersion || pending.RunnerEpoch != claim.RunnerEpoch {
		t.Fatalf("recovered pending=%+v found=%v err=%v", pending, found, err)
	}

	blocked, err := db.Transition(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning,
		To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence,
		EventPayload: `{"code":"provider_result_indeterminate","caller_data":"must_not_persist"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || current.State != domain.StateBlocked || current.ResumeState != domain.StatePlanning || current.BlockedCode != providerResultIndeterminateCode || current.Version != blocked.Version {
		t.Fatalf("blocked ticket=%+v err=%v", current, err)
	}
	var raw string
	if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, blocked.Version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var payload providerTerminalBlockerPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Schema != providerTerminalBlockerSchema || payload.Code != providerResultIndeterminateCode || payload.Phase != domain.PhasePlanning || payload.Role != "planner" || payload.ProviderAttemptID != claim.ID || payload.ProviderAttempt != claim.Attempt || payload.BindingDigest != claim.BindingDigest || payload.Outcome != "result_indeterminate" || payload.NextAction == "" || raw == `{"code":"provider_result_indeterminate","caller_data":"must_not_persist"}` {
		t.Fatalf("canonical payload=%s decoded=%+v", raw, payload)
	}
	for _, trigger := range []string{"operator_recover", "operator_resume", "operator_retry", "arbitrary"} {
		if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StateBlocked, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: trigger, Fence: fence, EventPayload: `{}`}); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("nonrecoverable blocker escaped through %s: %v", trigger, err)
		}
	}
	if _, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: current.Version, From: domain.StateBlocked, To: domain.StateCancelling, Trigger: "operator_cancel", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("blocked ticket could not be cancelled: %v", err)
	}
}

func TestInterruptedProviderRepairInvocationFailureBlocksAdmissionAndCanonicallyBlocks(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-invocation-failure")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-repair-invocation-failure", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{
			Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
			Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner),
			ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
		})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair == nil || second.Input.Repair.PriorAttempt != first.Attempt || second.Input.Repair.PriorRequestDigest != first.RequestDigest {
		t.Fatalf("second attempt lacks Store repair context: %+v", second.Input.Repair)
	}
	if err := db.FailProviderAttemptBeforeLaunch(ctx, second, ticket.Version, fence, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	pending, found, err := db.PendingProviderRepairUnavailable(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence)
	if err != nil || !found || pending.ID != second.ID || !sameImmutableProviderAttemptClaim(pending, second) {
		t.Fatalf("interrupted repair pending=%+v found=%v err=%v", pending, found, err)
	}
	if _, err := db.BeginProviderAttempt(ctx, request()); !errors.Is(err, ErrProviderRepairUnavailable) {
		t.Fatalf("interrupted repair admitted a new attempt: %v", err)
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("interrupted repair was accepted as ordinary exhaustion: %v", err)
	}
	blocked, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"provider_repair_unavailable","outcome":"forged"}`})
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, blocked.Version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var payload providerTerminalBlockerPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload.Code != providerRepairUnavailableCode || payload.ProviderAttemptID != second.ID || payload.Outcome != "invocation_failed" {
		t.Fatalf("interrupted repair blocker payload=%s decoded=%+v err=%v", raw, payload, err)
	}
	if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("interrupted repair blocker recovered generically: %v", err)
	}
	if _, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StateCancelling, Trigger: "operator_cancel", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("interrupted repair blocker could not be cancelled: %v", err)
	}
}

func TestPendingProviderRepairUnavailableRejectsTamperedTerminalPair(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-pair-tamper")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-repair-pair-tamper", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FailProviderAttemptBeforeLaunch(ctx, second, ticket.Version, fence, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE phase_runs SET outcome='failed' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, domain.PhasePlanning, second.Attempt); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.PendingProviderRepairUnavailable(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("tampered repair terminal pair accepted: %v", err)
	}
}

func TestInterruptedProviderRepairRecoveredCancellationBlocksAdmission(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	oldLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-recovery-old")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-repair-recovery", oldLeader), oldLeader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: oldLeader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, second, contracts.ProviderLaunch{PID: int(second.ID), PGID: int(second.ID), BootIdentity: "repair-recovery", ProcessStartIdentity: "repair-recovery-start", Worktree: second.Worktree}); err != nil {
		t.Fatal(err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-recovery-new")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, newLeader, providerTestSigner.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered runners changed=%d err=%v", changed, err)
	}
	active, err := db.ActiveProviderAttempts(ctx, domain.ChannelDev)
	if err != nil || len(active) != 1 || active[0].ID != second.ID {
		t.Fatalf("active repair=%+v err=%v", active, err)
	}
	if err := db.RecoverProviderAttemptClaimWithProof(ctx, active[0], newLeader, proof(t, active[0].ProviderAttemptClaim), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence = domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}
	pending, found, err := db.PendingProviderRepairUnavailable(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence)
	if err != nil || !found || pending.ID != second.ID {
		t.Fatalf("recovered repair pending=%+v found=%v err=%v", pending, found, err)
	}
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); !errors.Is(err, ErrProviderRepairUnavailable) {
		t.Fatalf("recovered interrupted repair admitted a new attempt: %v", err)
	}
}

func TestInterruptedProviderRepairCurrentCancellationBlocksAdmission(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-current-cancellation")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-repair-current-cancellation", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "cancelled", "cancelled", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	pending, found, err := db.PendingProviderRepairUnavailable(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence)
	if err != nil || !found || pending.ID != second.ID {
		t.Fatalf("cancelled repair pending=%+v found=%v err=%v", pending, found, err)
	}
	if _, err := db.BeginProviderAttempt(ctx, request()); !errors.Is(err, ErrProviderRepairUnavailable) {
		t.Fatalf("cancelled repair admitted a new attempt: %v", err)
	}
	blocked, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"provider_repair_unavailable"}`})
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, blocked.Version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var payload providerTerminalBlockerPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload.ProviderAttemptID != second.ID || payload.Outcome != "cancelled" {
		t.Fatalf("cancelled repair payload=%s decoded=%+v err=%v", raw, payload, err)
	}
}

func TestInvocationThenOrdinaryInvalidArtifactExhaustsWithoutRepairReason(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "invocation-then-invalid")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-invocation-then-invalid", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FailProviderAttemptBeforeLaunch(ctx, first, ticket.Version, fence, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair != nil {
		t.Fatalf("ordinary fallback unexpectedly received repair context: %+v", second.Input.Repair)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := db.PendingProviderRepairUnavailable(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence); err != nil || found {
		t.Fatalf("ordinary fallback misclassified as interrupted repair: found=%v err=%v", found, err)
	}
	paused, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, paused.Version).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var payload providerExhaustionPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload.Reason != "" {
		t.Fatalf("ordinary mixed exhaustion payload=%s decoded=%+v err=%v", raw, payload, err)
	}
}

func TestRecoveredOrdinaryAttemptAndSecondTerminalAttemptExhaust(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	oldLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ordinary-recovery-old")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-ordinary-recovery-exhaustion", oldLeader), oldLeader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: oldLeader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, first, contracts.ProviderLaunch{PID: int(first.ID), PGID: int(first.ID), BootIdentity: "ordinary-recovery", ProcessStartIdentity: "ordinary-recovery-start", Worktree: first.Worktree}); err != nil {
		t.Fatal(err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ordinary-recovery-new")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, newLeader, providerTestSigner.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered runners changed=%d err=%v", changed, err)
	}
	active, err := db.ActiveProviderAttempts(ctx, domain.ChannelDev)
	if err != nil || len(active) != 1 || active[0].ID != first.ID {
		t.Fatalf("recovered active attempt=%+v err=%v", active, err)
	}
	if err := db.RecoverProviderAttemptClaimWithProof(ctx, active[0], newLeader, proof(t, active[0].ProviderAttemptClaim), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence = domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}
	second, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair != nil {
		t.Fatalf("ordinary recovered retry unexpectedly received repair context: %+v", second.Input.Repair)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	paused, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence})
	if err != nil {
		t.Fatalf("recovered ordinary pair did not exhaust: %v", err)
	}
	if paused.Version != ticket.Version+1 {
		t.Fatalf("pause version=%d want=%d", paused.Version, ticket.Version+1)
	}
}

func TestRecoveredOrdinaryAttemptAndSecondInvalidArtifactExhaust(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	oldLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ordinary-recovery-invalid-old")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-ordinary-recovery-invalid", oldLeader), oldLeader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: oldLeader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, first, contracts.ProviderLaunch{PID: int(first.ID), PGID: int(first.ID), BootIdentity: "ordinary-recovery-invalid", ProcessStartIdentity: "ordinary-recovery-invalid-start", Worktree: first.Worktree}); err != nil {
		t.Fatal(err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ordinary-recovery-invalid-new")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, newLeader, providerTestSigner.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered runners changed=%d err=%v", changed, err)
	}
	active, err := db.ActiveProviderAttempts(ctx, domain.ChannelDev)
	if err != nil || len(active) != 1 || active[0].ID != first.ID {
		t.Fatalf("recovered active attempt=%+v err=%v", active, err)
	}
	if err := db.RecoverProviderAttemptClaimWithProof(ctx, active[0], newLeader, proof(t, active[0].ProviderAttemptClaim), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence = domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}
	second, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair != nil {
		t.Fatalf("ordinary recovered fallback unexpectedly received repair context: %+v", second.Input.Repair)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatalf("recovered ordinary plus invalid fallback did not exhaust: %v", err)
	}
}

func TestInvalidArtifactRepairAcrossRecoveryExhaustsAndRetries(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	oldLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "invalid-repair-recovery-old")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-invalid-repair-recovery", oldLeader), oldLeader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: oldLeader, RunnerEpoch: ticket.RunnerEpoch}
	begin := func() ProviderAttemptClaim {
		claim, beginErr := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
		if beginErr != nil {
			t.Fatal(beginErr)
		}
		return claim
	}
	failInvalid := func(claim ProviderAttemptClaim) {
		if finishErr := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); finishErr != nil {
			t.Fatal(finishErr)
		}
	}
	first := begin()
	failInvalid(first)
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "invalid-repair-recovery-new")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered runners changed=%d err=%v", changed, err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence = domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}
	second := begin()
	if second.Attempt != first.Attempt+1 || second.Input.Repair == nil || second.Input.Repair.PriorAttempt != first.Attempt || second.Input.Repair.PriorRequestDigest != first.RequestDigest {
		t.Fatalf("recovered repair binding=%+v first=%+v second=%+v", second.Input.Repair, first, second)
	}
	failInvalid(second)
	paused, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence})
	if err != nil {
		t.Fatalf("recovered invalid repair did not exhaust: %v", err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil || ticket.Version != paused.Version || ticket.State != domain.StatePaused {
		t.Fatalf("initial recovered pause ticket=%+v err=%v", ticket, err)
	}
	transitionProviderRetryCompatibilityTest(t, db, ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePaused, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_retry", Fence: fence})
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	third := begin()
	if third.Attempt != 3 || third.Input.Repair != nil {
		t.Fatalf("retry attempt three=%+v repair=%+v", third, third.Input.Repair)
	}
	failInvalid(third)
	fourth := begin()
	if fourth.Attempt != 4 || fourth.Input.Repair == nil || fourth.Input.Repair.PriorAttempt != third.Attempt || fourth.Input.Repair.PriorRequestDigest != third.RequestDigest {
		t.Fatalf("retry attempt four=%+v repair=%+v", fourth, fourth.Input.Repair)
	}
	failInvalid(fourth)
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatalf("retry repair pair did not exhaust: %v", err)
	}
}

func TestInvalidArtifactRepairAcrossPauseResumeAndRecoveryExhausts(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "invalid-control-recovery-old")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-invalid-control-recovery", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	first, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	stopping, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: fence, EventPayload: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	stoppingTicket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := db.CompleteControlTransition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stoppingTicket.RunnerEpoch}, EventPayload: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	pausedTicket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: pausedTicket.RunnerEpoch}, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	recoveryLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "invalid-control-recovery-new")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, recoveryLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered runners changed=%d err=%v", changed, err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	openExactRuntimeAdmission(t, db, ticket.Ref)
	fence = domain.Fence{LeaderEpoch: recoveryLeader, RunnerEpoch: ticket.RunnerEpoch}
	if ticket.State != domain.StatePlanning {
		t.Fatalf("recovered control ticket=%+v", ticket)
	}
	if err := db.AssertTicketFence(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		t.Fatalf("recovered control fence ticket=%+v fence=%+v err=%v", ticket, fence, err)
	}
	second, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair == nil || second.Input.Repair.PriorAttempt != first.Attempt || second.Input.Repair.PriorRequestDigest != first.RequestDigest {
		t.Fatalf("control/recovery repair=%+v", second.Input.Repair)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatalf("invalid repair across control/recovery did not exhaust: %v", err)
	}
}

func TestInvalidArtifactRepairAcrossTypedBlockRecoveryExhausts(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "invalid-block-recovery")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-invalid-block-recovery", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	first, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	blocked, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"host_repair_required"}`})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair == nil || second.Input.Repair.PriorAttempt != first.Attempt || second.Input.Repair.PriorRequestDigest != first.RequestDigest {
		t.Fatalf("typed-block repair=%+v", second.Input.Repair)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatalf("invalid repair across typed blocker did not exhaust: %v", err)
	}
}

func TestCurrentCancelledOrdinaryAttemptAndSecondInvalidArtifactExhaust(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ordinary-current-cancelled")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-ordinary-current-cancelled", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "cancelled", "cancelled", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair != nil {
		t.Fatalf("ordinary cancelled fallback unexpectedly received repair context: %+v", second.Input.Repair)
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatalf("cancelled ordinary plus invalid fallback did not exhaust: %v", err)
	}
}

func TestLegacyFailedRepairPairIsRepairUnavailableNotExhaustion(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "legacy-failed-repair")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-legacy-failed-repair", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	request := func() ProviderAttemptRequest {
		return supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})
	}
	first, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, first, proof(t, first), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginProviderAttempt(ctx, request())
	if err != nil {
		t.Fatal(err)
	}
	if second.Input.Repair == nil {
		t.Fatal("repair attempt is missing Store marker")
	}
	if err := db.FinishProviderAttempt(ctx, second, proof(t, second), ticket.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	pending, found, err := db.PendingProviderRepairUnavailable(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence)
	if err != nil || !found || pending.ID != second.ID {
		t.Fatalf("legacy failed repair pending=%+v found=%v err=%v", pending, found, err)
	}
	if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("legacy failed repair entered generic exhaustion: %v", err)
	}
}

func TestProviderRepairUnavailableBlockerRequiresExactPendingRepair(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-unavailable-blocker")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-repair-unavailable", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"provider_repair_unavailable","outcome":"forged"}`}); err != nil {
		t.Fatalf("exact pending repair did not block: %v", err)
	}
	var raw string
	if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version+1).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var payload providerTerminalBlockerPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil || payload.Code != providerRepairUnavailableCode || payload.Outcome != "invalid_artifact" || payload.ProviderAttemptID != claim.ID {
		t.Fatalf("repair blocker payload=%s decoded=%+v err=%v", raw, payload, err)
	}
}

func TestPendingProviderResultIndeterminateRejectsMismatchedPhaseTampering(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "result-indeterminate-tamper")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-result-indeterminate-tamper", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "result_indeterminate", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE phase_runs SET outcome='failed' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, domain.PhasePlanning, claim.Attempt); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.PendingProviderResultIndeterminate(ctx, ticket.Ref, domain.PhasePlanning, "planner", ticket.Version, fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("mismatched phase lifecycle accepted: %v", err)
	}
}

func TestV53ProviderResultIndeterminateMigration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v52.sqlite")
	createDatabaseAtVersion(t, path, 52)
	db, err := OpenChannel(ctx, path, t.TempDir(), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("schema=%d err=%v", version, err)
	}
	for _, name := range []string{"provider_attempt_state_outcome_insert", "provider_attempt_state_outcome_update", "phase_run_state_outcome_insert", "phase_run_state_outcome_update"} {
		var statement string
		if err := db.db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?`, name).Scan(&statement); err != nil || !strings.Contains(statement, "result_indeterminate") {
			t.Fatalf("trigger %s missing indeterminate outcome: %q err=%v", name, statement, err)
		}
	}
}
