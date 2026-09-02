package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestStoppingRecoveryWithFailedVerificationAttemptWithoutArtifact(t *testing.T) {
	database, ctx := openTestStore(t)
	digest := setupProviderProject(t, database, ctx)
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "stopping-failed-verification")
	if err != nil {
		t.Fatal(err)
	}
	ticket := setupProviderTicket(t, database, ctx, "SF-stopping-failed-verification", leader)
	planner, reviewer := setupProviderPair(t, database, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	planning, err := database.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	plannerDocument := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"works"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test"}, Details: "proof"}, Paths: []string{"main.go"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"risk"}}
	planRaw := []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"proof"},"paths":["main.go"],"commands":[["go","test"]],"risks":["risk"]}`)
	if _, err := database.CompleteProviderAttemptSuccess(ctx, planning, proof(t, planning), ticket.Version, fence, contracts.PhaseResult{Provider: planning.Binding.Identity, Artifact: planRaw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	planKey := ProviderAttemptResultKey{AttemptID: planning.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: planning.Attempt}
	if _, err := database.RecordPlan(ctx, PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &plannerDocument, ProviderResult: &planKey, Acceptance: plannerDocument.Acceptance, ProofKind: string(plannerDocument.Proof.Kind), Paths: plannerDocument.Paths, Commands: plannerDocument.Commands, Risks: plannerDocument.Risks}}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionPlan(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	ticket, err = database.Ticket(ctx, ticket.Ref)
	if err != nil || ticket.State != domain.StateVerifying || ticket.Version != 3 || ticket.RunnerEpoch != 1 {
		t.Fatalf("verification entry=%+v err=%v", ticket, err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	claim, err := database.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhaseVerification, Role: "reviewer", Binding: runtime(reviewer),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	var attemptState, outcome, launchState string
	if err := database.db.QueryRowContext(ctx, `SELECT state,outcome,launch_state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&attemptState, &outcome, &launchState); err != nil || attemptState != "failed" || outcome != "invalid_artifact" || launchState != "drained" {
		t.Fatalf("failed verifier state=%s/%s/%s err=%v", attemptState, outcome, launchState, err)
	}
	var artifacts int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&artifacts); err != nil || artifacts != 0 {
		t.Fatalf("failed verifier artifacts=%d err=%v", artifacts, err)
	}
	secondLeader, err := database.AcquireLeader(ctx, ticket.Ref.Channel, "stopping-failed-verification-pre-stop-first-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, ticket.Ref.Channel, secondLeader); err != nil || changed != 1 {
		t.Fatalf("first pre-stop recovery changed=%d err=%v", changed, err)
	}
	ticket, err = database.Ticket(ctx, ticket.Ref)
	if err != nil || ticket.State != domain.StateVerifying || ticket.Version != 4 || ticket.RunnerEpoch != 2 {
		t.Fatalf("first pre-stop recovery ticket=%+v err=%v", ticket, err)
	}
	thirdLeader, err := database.AcquireLeader(ctx, ticket.Ref.Channel, "stopping-failed-verification-pre-stop-second-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, ticket.Ref.Channel, thirdLeader); err != nil || changed != 1 {
		t.Fatalf("second pre-stop recovery changed=%d err=%v", changed, err)
	}
	ticket, err = database.Ticket(ctx, ticket.Ref)
	if err != nil || ticket.State != domain.StateVerifying || ticket.Version != 5 || ticket.RunnerEpoch != 3 {
		t.Fatalf("second pre-stop recovery ticket=%+v err=%v", ticket, err)
	}
	fence = domain.Fence{LeaderEpoch: thirdLeader, RunnerEpoch: ticket.RunnerEpoch}

	stoppedResult, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying,
		To: domain.StateStopping, ResumeState: domain.StateVerifying,
		Trigger: "operator_pause_or_take", Fence: fence, EventPayload: `{"intent":"take"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.Ticket(ctx, ticket.Ref)
	if err != nil || stopping.State != domain.StateStopping || stopping.Version != stoppedResult.Version || stopping.RunnerEpoch != ticket.RunnerEpoch+1 {
		t.Fatalf("stopping=%+v result=%+v err=%v", stopping, stoppedResult, err)
	}

	var sequence int
	var name, path string
	if err := database.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	fourthLeader, err := reopened.AcquireLeader(ctx, ticket.Ref.Channel, "stopping-failed-verification-post-stop-first-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ticket.Ref.Channel, fourthLeader); err != nil || changed != 1 {
		t.Fatalf("first stopping recovery changed=%d err=%v", changed, err)
	}
	firstRecovered, err := reopened.Ticket(ctx, ticket.Ref)
	if err != nil || firstRecovered.State != domain.StateStopping || firstRecovered.Version != stopping.Version+1 || firstRecovered.RunnerEpoch != stopping.RunnerEpoch+1 {
		t.Fatalf("first recovered=%+v stopping=%+v err=%v", firstRecovered, stopping, err)
	}
	var priorVersion, priorRunner, priorLeader, recoveredVersion, recoveredRunner, recoveredLeader uint64
	if err := reopened.db.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&priorVersion, &priorRunner, &priorLeader, &recoveredVersion, &recoveredRunner, &recoveredLeader); err != nil || priorVersion != stopping.Version || priorRunner != stopping.RunnerEpoch || priorLeader != thirdLeader || recoveredVersion != firstRecovered.Version || recoveredRunner != firstRecovered.RunnerEpoch || recoveredLeader != fourthLeader {
		t.Fatalf("first stopping recovery ledger prior=%d/%d/%d recovered=%d/%d/%d err=%v", priorVersion, priorRunner, priorLeader, recoveredVersion, recoveredRunner, recoveredLeader, err)
	}

	fifthLeader, err := reopened.AcquireLeader(ctx, ticket.Ref.Channel, "stopping-failed-verification-post-stop-second-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ticket.Ref.Channel, fifthLeader); err != nil || changed != 1 {
		t.Fatalf("second stopping recovery changed=%d err=%v", changed, err)
	}
	secondRecovered, err := reopened.Ticket(ctx, ticket.Ref)
	if err != nil || secondRecovered.State != domain.StateStopping || secondRecovered.Version != firstRecovered.Version+1 || secondRecovered.RunnerEpoch != firstRecovered.RunnerEpoch+1 {
		t.Fatalf("second recovered=%+v first=%+v err=%v", secondRecovered, firstRecovered, err)
	}
	if err := reopened.db.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&priorVersion, &priorRunner, &priorLeader, &recoveredVersion, &recoveredRunner, &recoveredLeader); err != nil || priorVersion != firstRecovered.Version || priorRunner != firstRecovered.RunnerEpoch || priorLeader != fourthLeader || recoveredVersion != secondRecovered.Version || recoveredRunner != secondRecovered.RunnerEpoch || recoveredLeader != fifthLeader {
		t.Fatalf("second stopping recovery ledger prior=%d/%d/%d recovered=%d/%d/%d err=%v", priorVersion, priorRunner, priorLeader, recoveredVersion, recoveredRunner, recoveredLeader, err)
	}
	if _, err := reopened.CompleteControlTransition(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: secondRecovered.Version, From: domain.StateStopping,
		To: domain.StatePaused, ResumeState: domain.StateVerifying,
		Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: fifthLeader, RunnerEpoch: secondRecovered.RunnerEpoch}, EventPayload: `{"drained":true}`,
	}); err != nil {
		t.Fatalf("complete recovered stopping control: %v", err)
	}
	paused, err := reopened.Ticket(ctx, ticket.Ref)
	if err != nil || paused.State != domain.StatePaused || paused.ResumeState != domain.StateVerifying || paused.Version != secondRecovered.Version+1 || paused.RunnerEpoch != secondRecovered.RunnerEpoch {
		t.Fatalf("paused=%+v recovered=%+v err=%v", paused, secondRecovered, err)
	}
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&artifacts); err != nil || artifacts != 0 {
		t.Fatalf("recovery fabricated verifier artifact count=%d err=%v", artifacts, err)
	}
	var stops int
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='operator_pause_or_take' AND from_state='verifying' AND to_state='stopping'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&stops); err != nil || stops != 1 {
		t.Fatalf("operator pause/take event count=%d err=%v", stops, err)
	}
}

func TestStoppingRecoveryRejectsTamperedControlEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Store, Ticket)
	}{
		{
			name: "missing-control",
			mutate: func(t *testing.T, database *Store, stopping Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, stopping.Ref.Channel, stopping.Ref.Project, stopping.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "opened-control",
			mutate: func(t *testing.T, database *Store, stopping Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `UPDATE runtime_ticket_controls SET state='open' WHERE channel=? AND project_id=? AND ticket_id=?`, stopping.Ref.Channel, stopping.Ref.Project, stopping.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "mismatched-authority",
			mutate: func(t *testing.T, database *Store, stopping Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `UPDATE runtime_ticket_controls SET authority_runner_epoch=authority_runner_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, stopping.Ref.Channel, stopping.Ref.Project, stopping.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "duplicate-state-change",
			mutate: func(t *testing.T, database *Store, stopping Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, stopping.Ref.Channel, stopping.Ref.Project, stopping.Ref.Ticket, stopping.Version, "typed_blocker", domain.StatePlanning, domain.StateBlocked, `{}`, "2026-09-02T00:00:00Z"); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, ref, leader, started := runtimeControlTicket(t)
			_, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{
				Ref: ref, ExpectedVersion: started.Version, From: started.State,
				To: domain.StateStopping, ResumeState: started.State, Trigger: "operator_pause_or_take",
				Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{}`,
			})
			if err != nil {
				t.Fatal(err)
			}
			stopping, err := database.Ticket(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(t, database, stopping)
			newLeader, err := database.AcquireLeader(t.Context(), ref.Channel, "stopping-tampered-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.FenceRecoveredRunners(t.Context(), ref.Channel, newLeader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("tampered stopping control startup fence err=%v, want publication evidence", err)
			}
		})
	}
}

func TestStoppingRecoveryRejectsMismatchedResumeState(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	_, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{
		Ref: ref, ExpectedVersion: started.Version, From: started.State,
		To: domain.StateStopping, ResumeState: started.State, Trigger: "operator_pause_or_take",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stoppingTicket, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(t.Context(), `UPDATE tickets SET resume_state=? WHERE channel=? AND project_id=? AND id=?`, domain.StateBuilding, ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	newLeader, err := database.AcquireLeader(t.Context(), ref.Channel, "stopping-resume-tampered-restart")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.FenceRecoveredRunners(t.Context(), ref.Channel, newLeader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("resume-tampered stopping control startup fence err=%v, want publication evidence", err)
	}
	if current, err := database.Ticket(t.Context(), ref); err != nil || current.Version != stoppingTicket.Version || current.RunnerEpoch != stoppingTicket.RunnerEpoch {
		t.Fatalf("tampered stopping ticket advanced=%+v stopping=%+v err=%v", current, stoppingTicket, err)
	}
}

// Cancellation is still a pre-publication disposition.  In particular, a
// merge observer must not turn an operator cancellation into a proof request
// merely because the ticket is temporarily in the cancelling state or the
// daemon was restarted between the two control calls.
func TestMergeObservationPrePublicationRemainsTrueAfterOperatorCancelRestart(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	ctx := t.Context()

	pre, err := database.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("initial pre-publication classification=%v err=%v", pre, err)
	}
	if _, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning,
		To: domain.StateCancelling, Trigger: "operator_cancel",
		Fence:        domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch},
		EventPayload: `{"operator":"test"}`,
	}); err != nil {
		t.Fatalf("operator cancel: %v", err)
	}
	cancelling, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if cancelling.State != domain.StateCancelling || cancelling.Version != started.Version+1 || cancelling.RunnerEpoch != started.RunnerEpoch+1 {
		t.Fatalf("durable cancelling ticket=%+v started=%+v", cancelling, started)
	}
	pre, err = database.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("cancelling pre-publication classification=%v err=%v", pre, err)
	}

	var sequence int
	var name, path string
	if err := database.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	newLeader, err := reopened.AcquireLeader(ctx, ref.Channel, "cancel-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ref.Channel, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered cancellation changed=%d err=%v", changed, err)
	}
	recovered, err := reopened.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != domain.StateCancelling || recovered.Version != cancelling.Version+1 || recovered.RunnerEpoch != cancelling.RunnerEpoch+1 {
		t.Fatalf("reopened cancelling ticket=%+v before=%+v", recovered, cancelling)
	}
	pre, err = reopened.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("reopened cancelling pre-publication classification=%v err=%v", pre, err)
	}
	secondLeader, err := reopened.AcquireLeader(ctx, ref.Channel, "cancel-second-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ref.Channel, secondLeader); err != nil || changed != 1 {
		t.Fatalf("second cancellation fence changed=%d err=%v", changed, err)
	}
	recovered, err = reopened.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	pre, err = reopened.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("second recovered cancellation classification=%v err=%v", pre, err)
	}
	if _, err := reopened.CompleteControlTransition(ctx, Transition{
		Ref: recovered.Ref, ExpectedVersion: recovered.Version, From: domain.StateCancelling,
		To: domain.StateCancelled, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: recovered.RunnerEpoch}, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("complete cancellation after restart: %v", err)
	}
	cancelled, err := reopened.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != domain.StateCancelled {
		t.Fatalf("reopened cancellation completion=%+v", cancelled)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ref.Channel, secondLeader); err != nil || changed != 0 {
		t.Fatalf("terminal cancellation startup fence changed=%d err=%v", changed, err)
	}
	events, err := reopened.Events(ctx, ref.Channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var cancelEvents int
	for _, event := range events {
		if event.Ref == ref && event.Trigger == "operator_cancel" && event.From == domain.StatePlanning && event.To == domain.StateCancelling {
			cancelEvents++
		}
	}
	if cancelEvents != 1 {
		t.Fatalf("operator cancel event count=%d events=%+v", cancelEvents, events)
	}
}

type recoveredStoppingCancellationFixture struct {
	database        *Store
	ref             domain.TicketRef
	leader          uint64
	stoppingVersion uint64
	stoppingRunner  uint64
	stoppingLeader  uint64
	pausedVersion   uint64
	cancelling      Ticket
}

func recoveredStoppingCancellation(t *testing.T) recoveredStoppingCancellationFixture {
	t.Helper()
	database, ref, leader, current := runtimeControlTicket(t)
	ctx := t.Context()
	for index := 0; index < 3; index++ {
		var err error
		leader, err = database.AcquireLeader(ctx, ref.Channel, "pre-stop-recovery")
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := database.FenceRecoveredRunners(ctx, ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("pre-stop recovery changed=%d err=%v", changed, err)
		}
		current = databaseTicket(t, database, ctx, ref)
	}
	stoppingLeader := leader
	stopped, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: current.Version, From: domain.StatePlanning,
		To: domain.StateStopping, ResumeState: domain.StatePlanning,
		Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{"intent":"take"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopping := databaseTicket(t, database, ctx, ref)
	if stopping.State != domain.StateStopping || stopping.Version != stopped.Version || stopping.RunnerEpoch != current.RunnerEpoch+1 {
		t.Fatalf("stopping=%+v stop=%+v current=%+v", stopping, stopped, current)
	}
	stoppingVersion := stopping.Version
	for index := 0; index < 6; index++ {
		leader, err = database.AcquireLeader(ctx, ref.Channel, "stopping-recovery")
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := database.FenceRecoveredRunners(ctx, ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("stopping recovery changed=%d err=%v", changed, err)
		}
		stopping = databaseTicket(t, database, ctx, ref)
	}
	if _, err := database.CompleteControlTransition(ctx, Transition{
		Ref: ref, ExpectedVersion: stopping.Version, From: domain.StateStopping,
		To: domain.StatePaused, ResumeState: domain.StatePlanning,
		Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: `{"drained":true,"intent":"take"}`,
	}); err != nil {
		t.Fatal(err)
	}
	paused := databaseTicket(t, database, ctx, ref)
	if paused.State != domain.StatePaused || paused.Version != stopping.Version+1 || paused.RunnerEpoch != stopping.RunnerEpoch {
		t.Fatalf("paused=%+v stopping=%+v", paused, stopping)
	}
	if _, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateCancelling, Trigger: "operator_cancel",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}, EventPayload: `{"operator":"test"}`,
	}); err != nil {
		t.Fatal(err)
	}
	cancelling := databaseTicket(t, database, ctx, ref)
	if cancelling.State != domain.StateCancelling || cancelling.Version != paused.Version+1 || cancelling.RunnerEpoch != paused.RunnerEpoch+1 {
		t.Fatalf("cancelling=%+v paused=%+v", cancelling, paused)
	}
	return recoveredStoppingCancellationFixture{database: database, ref: ref, leader: leader, stoppingVersion: stoppingVersion, stoppingRunner: current.RunnerEpoch + 1, stoppingLeader: stoppingLeader, pausedVersion: paused.Version, cancelling: cancelling}
}

func TestMergeObservationPrePublicationAcceptsCancelledRecoveredStoppingLineage(t *testing.T) {
	fixture := recoveredStoppingCancellation(t)
	pre, err := fixture.database.MergeObservationPrePublication(t.Context(), fixture.ref)
	if err != nil || !pre {
		t.Fatalf("cancelled recovered stopping classified pre-publication=%v err=%v ticket=%+v", pre, err, fixture.cancelling)
	}
	leader, err := fixture.database.AcquireLeader(t.Context(), fixture.ref.Channel, "cancel-after-stopping-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.database.FenceRecoveredRunners(t.Context(), fixture.ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("cancel recovery changed=%d err=%v", changed, err)
	}
	pre, err = fixture.database.MergeObservationPrePublication(t.Context(), fixture.ref)
	if err != nil || !pre {
		t.Fatalf("recovered cancellation classified pre-publication=%v err=%v", pre, err)
	}
}

func insertRecoveredStoppingForgery(t *testing.T, fixture recoveredStoppingCancellationFixture, value RunnerRecoveryLedger) {
	t.Helper()
	value.Ref = fixture.ref
	value.CreatedAt = time.Now().UTC()
	value.RecoveryDigest = runnerRecoveryDigest(value)
	if _, err := fixture.database.db.ExecContext(t.Context(), `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.PriorTicketVersion, value.PriorRunnerEpoch, value.PriorLeaderEpoch, value.TicketVersion, value.RunnerEpoch, value.LeaderEpoch, value.RecoveryDigest, value.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}

func TestMergeObservationPrePublicationRejectsTamperedRecoveredStoppingCancellationLineage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, recoveredStoppingCancellationFixture)
	}{
		{
			name: "missing-stopping-recovery-ledger",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				if _, err := fixture.database.db.ExecContext(t.Context(), `DROP TRIGGER runner_recovery_ledger_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				changed, err := fixture.database.db.ExecContext(t.Context(), `DELETE FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, fixture.ref.Channel, fixture.ref.Project, fixture.ref.Ticket, fixture.stoppingVersion+1)
				if err != nil {
					t.Fatalf("delete stopping recovery err=%v", err)
				}
				count, _ := changed.RowsAffected()
				if count != 1 {
					t.Fatalf("delete stopping recovery err=%v", err)
				}
			},
		},
		{
			name: "modified-stopping-recovery-ledger",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				if _, err := fixture.database.db.ExecContext(t.Context(), `DROP TRIGGER runner_recovery_ledger_immutable_update`); err != nil {
					t.Fatal(err)
				}
				changed, err := fixture.database.db.ExecContext(t.Context(), `UPDATE runner_recovery_ledger SET leader_epoch=leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, fixture.ref.Channel, fixture.ref.Project, fixture.ref.Ticket, fixture.pausedVersion-1)
				if err != nil {
					t.Fatalf("modify stopping recovery err=%v", err)
				}
				count, _ := changed.RowsAffected()
				if count != 1 {
					t.Fatalf("modify stopping recovery err=%v", err)
				}
			},
		},
		{
			name: "missing-drained-event",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				changed, err := fixture.database.db.ExecContext(t.Context(), `DELETE FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='process_and_effects_drained'`, fixture.ref.Channel, fixture.ref.Project, fixture.ref.Ticket, fixture.pausedVersion)
				if err != nil {
					t.Fatalf("delete drained event err=%v", err)
				}
				count, _ := changed.RowsAffected()
				if count != 1 {
					t.Fatalf("delete drained event err=%v", err)
				}
			},
		},
		{
			name: "post-publication-stop-source",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				changed, err := fixture.database.db.ExecContext(t.Context(), `UPDATE events SET from_state='publishing' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_pause_or_take'`, fixture.ref.Channel, fixture.ref.Project, fixture.ref.Ticket, fixture.stoppingVersion)
				if err != nil {
					t.Fatalf("change stopping source err=%v", err)
				}
				count, _ := changed.RowsAffected()
				if count != 1 {
					t.Fatalf("change stopping source err=%v", err)
				}
			},
		},
		{
			name: "recovery-at-stop-baseline",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				insertRecoveredStoppingForgery(t, fixture, RunnerRecoveryLedger{PriorTicketVersion: fixture.stoppingVersion - 1, PriorRunnerEpoch: fixture.stoppingRunner - 1, PriorLeaderEpoch: fixture.stoppingLeader, TicketVersion: fixture.stoppingVersion, RunnerEpoch: fixture.stoppingRunner, LeaderEpoch: fixture.stoppingLeader + 1})
			},
		},
		{
			name: "recovery-at-drained-endpoint",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				pausedRunner := fixture.cancelling.RunnerEpoch - 1
				insertRecoveredStoppingForgery(t, fixture, RunnerRecoveryLedger{PriorTicketVersion: fixture.pausedVersion - 1, PriorRunnerEpoch: pausedRunner - 1, PriorLeaderEpoch: fixture.leader - 1, TicketVersion: fixture.pausedVersion, RunnerEpoch: pausedRunner, LeaderEpoch: fixture.leader})
			},
		},
		{
			name: "recovery-at-cancel-endpoint",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				insertRecoveredStoppingForgery(t, fixture, RunnerRecoveryLedger{PriorTicketVersion: fixture.cancelling.Version - 1, PriorRunnerEpoch: fixture.cancelling.RunnerEpoch - 1, PriorLeaderEpoch: fixture.leader, TicketVersion: fixture.cancelling.Version, RunnerEpoch: fixture.cancelling.RunnerEpoch, LeaderEpoch: fixture.leader + 1})
			},
		},
		{
			name: "state-event-at-recovery-version",
			mutate: func(t *testing.T, fixture recoveredStoppingCancellationFixture) {
				if _, err := fixture.database.db.ExecContext(t.Context(), `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, fixture.ref.Channel, fixture.ref.Project, fixture.ref.Ticket, fixture.stoppingVersion+1, "forged_recovery_state", domain.StateStopping, domain.StatePaused, `{}`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := recoveredStoppingCancellation(t)
			tc.mutate(t, fixture)
			if pre, err := fixture.database.MergeObservationPrePublication(t.Context(), fixture.ref); err != nil || pre {
				t.Fatalf("tampered recovered stopping cancellation classified pre-publication=%v err=%v", pre, err)
			}
		})
	}
}

func directPausedCancellation(t *testing.T) recoveredStoppingCancellationFixture {
	t.Helper()
	database, ref, leader, started := runtimeControlTicket(t)
	stopping, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning,
		To: domain.StateStopping, ResumeState: domain.StatePlanning,
		Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{"intent":"take"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped := databaseTicket(t, database, t.Context(), ref)
	if _, err := database.CompleteControlTransition(t.Context(), Transition{
		Ref: ref, ExpectedVersion: stopped.Version, From: domain.StateStopping,
		To: domain.StatePaused, ResumeState: domain.StatePlanning,
		Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopped.RunnerEpoch}, EventPayload: `{"drained":true,"intent":"take"}`,
	}); err != nil {
		t.Fatal(err)
	}
	paused := databaseTicket(t, database, t.Context(), ref)
	if _, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{
		Ref: ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateCancelling, Trigger: "operator_cancel",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}, EventPayload: `{"operator":"test"}`,
	}); err != nil {
		t.Fatal(err)
	}
	cancelling := databaseTicket(t, database, t.Context(), ref)
	return recoveredStoppingCancellationFixture{database: database, ref: ref, leader: leader, stoppingVersion: stopping.Version, stoppingRunner: stopped.RunnerEpoch, stoppingLeader: leader, pausedVersion: paused.Version, cancelling: cancelling}
}

func TestMergeObservationPrePublicationRejectsRecoveryAtDirectPauseLifecycleEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		forge func(recoveredStoppingCancellationFixture) RunnerRecoveryLedger
	}{
		{
			name: "stop",
			forge: func(fixture recoveredStoppingCancellationFixture) RunnerRecoveryLedger {
				return RunnerRecoveryLedger{PriorTicketVersion: fixture.stoppingVersion - 1, PriorRunnerEpoch: fixture.stoppingRunner - 1, PriorLeaderEpoch: fixture.stoppingLeader, TicketVersion: fixture.stoppingVersion, RunnerEpoch: fixture.stoppingRunner, LeaderEpoch: fixture.stoppingLeader + 1}
			},
		},
		{
			name: "drain",
			forge: func(fixture recoveredStoppingCancellationFixture) RunnerRecoveryLedger {
				pausedRunner := fixture.cancelling.RunnerEpoch - 1
				return RunnerRecoveryLedger{PriorTicketVersion: fixture.pausedVersion - 1, PriorRunnerEpoch: pausedRunner - 1, PriorLeaderEpoch: fixture.leader, TicketVersion: fixture.pausedVersion, RunnerEpoch: pausedRunner, LeaderEpoch: fixture.leader + 1}
			},
		},
		{
			name: "cancel",
			forge: func(fixture recoveredStoppingCancellationFixture) RunnerRecoveryLedger {
				return RunnerRecoveryLedger{PriorTicketVersion: fixture.cancelling.Version - 1, PriorRunnerEpoch: fixture.cancelling.RunnerEpoch - 1, PriorLeaderEpoch: fixture.leader, TicketVersion: fixture.cancelling.Version, RunnerEpoch: fixture.cancelling.RunnerEpoch, LeaderEpoch: fixture.leader + 1}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := directPausedCancellation(t)
			insertRecoveredStoppingForgery(t, fixture, tc.forge(fixture))
			if pre, err := fixture.database.MergeObservationPrePublication(t.Context(), fixture.ref); err != nil || pre {
				t.Fatalf("direct pause lifecycle recovery classified pre-publication=%v err=%v", pre, err)
			}
		})
	}
}

func TestPrePublicationCancellationProofFailsClosedWithoutExactAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Store, Ticket)
	}{
		{
			name: "missing-sealed-control",
			mutate: func(t *testing.T, database *Store, cancelling Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, cancelling.Ref.Channel, cancelling.Ref.Project, cancelling.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "duplicate-state-change",
			mutate: func(t *testing.T, database *Store, cancelling Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, cancelling.Ref.Channel, cancelling.Ref.Project, cancelling.Ref.Ticket, cancelling.Version, "typed_blocker", domain.StatePlanning, domain.StateBlocked, `{}`, "2026-09-02T00:00:00Z"); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, ref, leader, started := runtimeControlTicket(t)
			if _, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{
				Ref: ref, ExpectedVersion: started.Version, From: started.State,
				To: domain.StateCancelling, Trigger: "operator_cancel",
				Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{}`,
			}); err != nil {
				t.Fatal(err)
			}
			cancelling := databaseTicket(t, database, t.Context(), ref)
			tc.mutate(t, database, cancelling)
			if pre, err := database.MergeObservationPrePublication(t.Context(), ref); err != nil || pre {
				t.Fatalf("malformed cancellation classified pre-publication=%v err=%v", pre, err)
			}
			newLeader, err := database.AcquireLeader(t.Context(), ref.Channel, "malformed-cancel-restart")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.FenceRecoveredRunners(t.Context(), ref.Channel, newLeader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("malformed cancellation startup fence err=%v, want publication evidence", err)
			}
		})
	}
}

func TestMergeObservationPrePublicationAllowlist(t *testing.T) {
	for _, target := range []domain.State{
		domain.StateQueued, domain.StatePlanning, domain.StateVerifying,
		domain.StateBuilding, domain.StateStopping, domain.StatePaused,
		domain.StateBlocked, domain.StateCancelling,
	} {
		t.Run(string(target), func(t *testing.T) {
			database, ctx := openTestStore(t)
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID("SF-prepub-" + string(target))}
			if err := database.CreateTicket(ctx, ticket(ref, "prepub-"+string(target))); err != nil {
				t.Fatal(err)
			}
			leader, err := database.AcquireLeader(ctx, ref.Channel, "prepub-"+string(target))
			if err != nil {
				t.Fatal(err)
			}
			current, err := database.Ticket(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if target != domain.StateQueued {
				current, err = database.StartOrAdopt(ctx, ref, current.Version, "prepub/"+string(target), domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
				if err != nil {
					t.Fatal(err)
				}
				switch target {
				case domain.StateVerifying, domain.StateBuilding:
					current = providerState(t, database, ctx, current, leader, target)
				case domain.StateStopping, domain.StatePaused, domain.StateBlocked, domain.StateCancelling, domain.StateCancelled:
					current = prePublicationControlState(t, database, ctx, current, leader, target)
				}
			}
			pre, err := database.MergeObservationPrePublication(ctx, ref)
			if err != nil || !pre {
				t.Fatalf("state=%s pre-publication classification=%v err=%v ticket=%+v", target, pre, err, current)
			}
			if target != domain.StateCancelling {
				if _, err := database.TransitionAndInvalidateRunner(ctx, Transition{
					Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
					To: domain.StateCancelling, Trigger: "operator_cancel",
					Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{"prepublication":true}`,
				}); err != nil {
					t.Fatalf("cancel from %s: %v", target, err)
				}
				current = databaseTicket(t, database, ctx, ref)
				pre, err = database.MergeObservationPrePublication(ctx, ref)
				if err != nil || !pre {
					t.Fatalf("cancel origin=%s pre-publication classification=%v err=%v ticket=%+v", target, pre, err, current)
				}
			}
		})
	}
}

func prePublicationControlState(t *testing.T, database *Store, ctx context.Context, current Ticket, leader uint64, target domain.State) Ticket {
	t.Helper()
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	if target == domain.StateStopping || target == domain.StatePaused || target == domain.StateBlocked {
		stopping, err := database.TransitionAndInvalidateRunner(ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
			To: domain.StateStopping, ResumeState: current.State, Trigger: "operator_pause_or_take",
			Fence: fence, EventPayload: `{}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		current = databaseTicket(t, database, ctx, current.Ref)
		if target == domain.StateStopping {
			return current
		}
		if _, err := database.CompleteControlTransition(ctx, Transition{
			Ref: current.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping,
			To: domain.StatePaused, ResumeState: current.ResumeState,
			Trigger: "process_and_effects_drained",
			Fence:   domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{}`,
		}); err != nil {
			t.Fatal(err)
		}
		current = databaseTicket(t, database, ctx, current.Ref)
		if target == domain.StatePaused {
			return current
		}
		if _, err := database.Transition(ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StatePaused,
			To: domain.StateBlocked, ResumeState: current.ResumeState, Trigger: "operator_resume",
			Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{}`,
		}); err != nil {
			t.Fatal(err)
		}
		return databaseTicket(t, database, ctx, current.Ref)
	}

	stopping, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
		To: domain.StateCancelling, Trigger: "operator_cancel", Fence: fence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	current = databaseTicket(t, database, ctx, current.Ref)
	if target == domain.StateCancelling {
		return current
	}
	if _, err := database.CompleteControlTransition(ctx, Transition{
		Ref: current.Ref, ExpectedVersion: stopping.Version, From: domain.StateCancelling,
		To: domain.StateCancelled, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{}`,
	}); err != nil {
		t.Fatal(err)
	}
	return databaseTicket(t, database, ctx, current.Ref)
}

func databaseTicket(t *testing.T, database *Store, ctx context.Context, ref domain.TicketRef) Ticket {
	t.Helper()
	value, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// A caller-controlled payload cannot manufacture a pre-publication proof for
// a ticket that already crossed into publication.  The classifier is
// intentionally read-only and derives its answer from durable effects and
// merge intents, not from an operator's claimed "merge not observed" fact.
func TestMergeObservationPrePublicationRejectsForgedPostPublicationProof(t *testing.T) {
	fixture, current, leader := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	defer fixture.db.Close()

	pre, err := fixture.db.MergeObservationPrePublication(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if pre {
		t.Fatal("post-publication ticket was classified as pre-publication")
	}
	// Also exercise the pure proof predicate with the most favorable forged
	// counts.  The ticket state itself remains an independent publication
	// boundary, so zero caller-supplied effects cannot override it.
	if (TicketControlProof{Ticket: current}).StrictlyPrePublication() {
		t.Fatal("forged zero-effect proof was accepted for a post-publication ticket")
	}
	if _, err := fixture.db.TransitionAndInvalidateRunner(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
		To: domain.StateCancelling, Trigger: "operator_cancel",
		Fence:        domain.Fence{LeaderEpoch: leader.LeaderEpoch, RunnerEpoch: current.RunnerEpoch},
		EventPayload: `{"prepublication":true,"merge_not_observed":true}`,
	}); err != nil {
		t.Fatalf("post-publication cancel fixture: %v", err)
	}
	pre, err = fixture.db.MergeObservationPrePublication(fixture.ctx, current.Ref)
	if err != nil || pre {
		t.Fatalf("post-publication forged classification=%v err=%v", pre, err)
	}
}
