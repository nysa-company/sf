package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

const (
	ciPollMaxAttempts = 12
	ciPollDeadline    = 3 * time.Hour
	ciPollInitialWait = 30 * time.Second
	ciPollMaximumWait = 10 * time.Minute
)

// CIPollAdmission is the durable result of one scheduler tick. Due is the
// only result that authorizes an external CI read. Expired pauses the ticket
// with an explicit operator action, without creating another observation.
type CIPollAdmission struct {
	Due       bool
	Expired   bool
	Attempt   int
	NextPoll  time.Time
	Deadline  time.Time
	PauseCode string
}

func ciPollBackoff(completed int) time.Duration {
	if completed <= 0 {
		return 0
	}
	delay := ciPollInitialWait
	for i := 1; i < completed && delay < ciPollMaximumWait; i++ {
		delay *= 2
	}
	if delay > ciPollMaximumWait {
		return ciPollMaximumWait
	}
	return delay
}

// AdmitCIPoll persists a bounded per-candidate schedule before the worker
// reads GitHub. The append-only attempt rows make restart replay deterministic
// and ensure a 250ms scheduler cannot create unbounded observations.
func (s *Store) AdmitCIPoll(ctx context.Context, ref domain.TicketRef, fence domain.Fence, at time.Time) (CIPollAdmission, error) {
	if ref.Validate() != nil || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || !canonicalPublicationTimestamp(at) {
		return CIPollAdmission{}, ErrCIObservation
	}
	at = at.UTC()
	var admission CIPollAdmission
	err := s.ciWrite(ctx, ref, func(conn *sql.Conn) error {
		publication, err := loadCICurrentPublication(ctx, conn, ref)
		if err != nil {
			return ErrCIObservation
		}
		var state domain.State
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return normalizeBusy(ctx, err)
		}
		if state != domain.StateWaitingCI || version == 0 || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
			return ErrStaleFence
		}
		var firstRaw, deadlineRaw string
		var maximum, attempts int
		err = conn.QueryRowContext(ctx, `SELECT first_polled_at,deadline_at,max_attempts FROM ci_poll_schedules WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=?`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&firstRaw, &deadlineRaw, &maximum)
		if err == sql.ErrNoRows {
			deadline := at.Add(ciPollDeadline)
			if _, err = conn.ExecContext(ctx, `INSERT INTO ci_poll_schedules(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,first_polled_at,deadline_at,max_attempts) VALUES(?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, at.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano), ciPollMaxAttempts); err != nil {
				return err
			}
			firstRaw, deadlineRaw, maximum = at.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano), ciPollMaxAttempts
		} else if err != nil {
			return normalizeBusy(ctx, err)
		}
		first, firstErr := time.Parse(time.RFC3339Nano, firstRaw)
		deadline, deadlineErr := time.Parse(time.RFC3339Nano, deadlineRaw)
		if firstErr != nil || deadlineErr != nil || maximum < 1 || maximum > 64 || !deadline.After(first) {
			return ErrCIObservation
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_poll_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=?`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&attempts); err != nil {
			return normalizeBusy(ctx, err)
		}
		admission.Deadline = deadline
		if !at.Before(deadline) || attempts >= maximum {
			admission.Expired = true
			if !at.Before(deadline) {
				admission.PauseCode = "ci_poll_deadline_exhausted"
			} else {
				admission.PauseCode = "ci_poll_attempts_exhausted"
			}
			payload, marshalErr := json.Marshal(map[string]any{"code": admission.PauseCode, "reason": "required CI checks did not reach a terminal result before the bounded polling limit", "next_action": "inspect the required checks, then explicitly resume waiting_ci after resolving the delay"})
			if marshalErr != nil {
				return ErrCIObservation
			}
			if result, updateErr := conn.ExecContext(ctx, `UPDATE tickets SET state='paused',resume_state='waiting_ci',version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='waiting_ci' AND version=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, version, runner); updateErr != nil {
				return updateErr
			} else if count, _ := result.RowsAffected(); count != 1 {
				return ErrStaleFence
			}
			_, err = conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, version+1, "ci_poll_exhausted", domain.StateWaitingCI, domain.StatePaused, string(payload), at.Format(time.RFC3339Nano))
			return err
		}
		if attempts > 0 {
			var lastRaw string
			if err := conn.QueryRowContext(ctx, `SELECT polled_at FROM ci_poll_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=? ORDER BY attempt DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&lastRaw); err != nil {
				return ErrCIObservation
			}
			last, parseErr := time.Parse(time.RFC3339Nano, lastRaw)
			if parseErr != nil {
				return ErrCIObservation
			}
			admission.NextPoll = last.Add(ciPollBackoff(attempts))
			if at.Before(admission.NextPoll) {
				return nil
			}
		}
		attempts++
		if _, err := conn.ExecContext(ctx, `INSERT INTO ci_poll_attempts(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,attempt,polled_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, attempts, at.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		admission.Due, admission.Attempt = true, attempts
		admission.NextPoll = at.Add(ciPollBackoff(attempts))
		return nil
	})
	return admission, err
}

func canonicalPublicationTimestamp(at time.Time) bool {
	_, ok := canonicalPublicationTime(at.UTC())
	return ok
}
