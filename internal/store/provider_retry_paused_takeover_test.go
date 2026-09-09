package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProviderRetryProtectedBaseRefreshPausedTakeoverRejectsUnboundEvidence(t *testing.T) {
	for _, mode := range []string{"source_leader", "source_version", "terminal_pair", "retry_event", "epoch_digest", "hidden_ledger"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := providerRetryRefreshFixture(t)
			from := f.ticket
			prior := f.fence
			paused := f.exhaust(t, "invalid_artifact")
			leader, err := f.db.AcquireLeader(f.ctx, paused.Ref.Channel, "paused-takeover-negative")
			if err != nil {
				t.Fatal(err)
			}
			leader, err = f.db.AcquireLeader(f.ctx, paused.Ref.Channel, "paused-takeover-negative-next")
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := f.db.FenceRecoveredRunners(f.ctx, paused.Ref.Channel, leader); err != nil || changed != 0 {
				t.Fatalf("paused fence changed=%d err=%v", changed, err)
			}
			f.fence.LeaderEpoch = leader
			if err := f.db.SealRuntimeControl(f.ctx, paused.Ref); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.TransitionProviderRetry(f.ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateBuilding, ResumeState: domain.StateBuilding, Trigger: "operator_retry", Fence: f.fence}); err != nil {
				t.Fatal(err)
			}
			f.reload(t)
			if err := validateProviderRetryAdvance(f.ctx, f.db.db, from.Ref, domain.PhaseBuild, from.Version, from.RunnerEpoch, prior.LeaderEpoch, f.ticket.Version, f.ticket.RunnerEpoch, leader); err != nil {
				t.Fatalf("valid paused takeover prerequisite: %v", err)
			}
			sourceVersion, sourceLeader := from.Version, prior.LeaderEpoch
			query := ""
			switch mode {
			case "source_leader":
				sourceLeader = leader - 1
			case "source_version":
				sourceVersion--
			case "terminal_pair":
				query = `UPDATE phase_runs SET outcome='cancelled' WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND attempt=(SELECT MAX(attempt) FROM provider_attempts WHERE channel=phase_runs.channel AND project_id=phase_runs.project_id AND ticket_id=phase_runs.ticket_id AND phase='build')`
			case "retry_event":
				query = `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='operator_retry'`
			case "epoch_digest":
				if _, err := f.db.db.ExecContext(f.ctx, `DROP TRIGGER provider_retry_epochs_immutable_update`); err != nil {
					t.Fatal(err)
				}
				query = `UPDATE provider_retry_epochs SET retry_digest='` + strings.Repeat("f", 64) + `' WHERE channel=? AND project_id=? AND ticket_id=?`
			case "hidden_ledger":
				step := RunnerRecoveryLedger{Ref: from.Ref, PriorTicketVersion: from.Version, PriorRunnerEpoch: from.RunnerEpoch, PriorLeaderEpoch: prior.LeaderEpoch, TicketVersion: from.Version + 1, RunnerEpoch: from.RunnerEpoch + 1, LeaderEpoch: leader, CreatedAt: time.Now().UTC()}
				step.RecoveryDigest = runnerRecoveryDigest(step)
				if _, err := f.db.db.ExecContext(f.ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, from.Ref.Channel, from.Ref.Project, from.Ref.Ticket, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch, step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch, step.RecoveryDigest, step.CreatedAt.Format(time.RFC3339Nano)); err != nil {
					t.Fatal(err)
				}
			}
			if query != "" {
				result, err := f.db.db.ExecContext(f.ctx, query, from.Ref.Channel, from.Ref.Project, from.Ref.Ticket)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 {
					t.Fatalf("tamper %s rows=%d err=%v", mode, n, err)
				}
			}
			if err := validateProviderRetryAdvance(f.ctx, f.db.db, from.Ref, domain.PhaseBuild, sourceVersion, from.RunnerEpoch, sourceLeader, f.ticket.Version, f.ticket.RunnerEpoch, leader); !errors.Is(err, ErrPublicationEvidence) && !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("tamper %s accepted: %v", mode, err)
			}
		})
	}
}
