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

type ciPollRetryEpoch struct {
	initialAttempts                                 int
	exhaustedVersion, resumeVersion, leader, runner uint64
	resumedAt, deadline                             time.Time
	digest                                          string
}

type ciPollResumePair struct {
	exhaustedVersion uint64
	resumeVersion    uint64
	resumedAt        time.Time
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
// reads GitHub. A single exact operator-resume event may mint retry epoch 2;
// it gets a fresh bounded deadline and attempt budget. A later exhaustion is
// terminal for this candidate and directs the operator to reconcile or submit
// a new ticket instead of advertising a retry that cannot be admitted.
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
		_, deadline, maximum, err := loadOrCreateCIPollSchedule(ctx, conn, ref, publication, at)
		if err != nil {
			return err
		}
		attempts, last, err := loadCIPollAttempts(ctx, conn, ref, publication)
		if err != nil {
			return err
		}
		retry, hasRetry, err := loadCIPollRetryEpoch(ctx, conn, ref, publication)
		if err != nil {
			return err
		}
		epochBase, epochDeadline := 0, deadline
		if hasRetry {
			if retry.initialAttempts < 1 || retry.initialAttempts > maximum || retry.resumeVersion != retry.exhaustedVersion+1 || retry.resumeVersion > version || retry.leader == 0 || retry.runner == 0 || !retry.deadline.After(retry.resumedAt) || retry.digest != ciPollRetryEpochDigest(ref, publication, retry.initialAttempts, retry.exhaustedVersion, retry.resumeVersion, retry.leader, retry.runner, retry.resumedAt, retry.deadline) {
				return ErrCIObservation
			}
			epochBase, epochDeadline = retry.initialAttempts, retry.deadline
		}
		if attempts < epochBase || attempts > epochBase+maximum {
			return ErrCIObservation
		}
		epochAttempts := attempts - epochBase
		admission.Deadline = epochDeadline
		if !at.Before(epochDeadline) || epochAttempts >= maximum {
			if !hasRetry {
				retry, authorized, authErr := authorizeCIPollRetry(ctx, conn, ref, publication, attempts, version, runner, leader, at)
				if authErr != nil {
					return authErr
				}
				if !authorized {
					return pauseCIPoll(ctx, conn, ref, version, runner, at, !at.Before(epochDeadline), false, &admission)
				}
				epochBase, epochDeadline, epochAttempts = retry.initialAttempts, retry.deadline, 0
				hasRetry, admission.Deadline = true, epochDeadline
			}
			if hasRetry && (!at.Before(epochDeadline) || epochAttempts >= maximum) {
				return pauseCIPoll(ctx, conn, ref, version, runner, at, !at.Before(epochDeadline), true, &admission)
			}
		}
		if epochAttempts > 0 {
			admission.NextPoll = last.Add(ciPollBackoff(epochAttempts))
			if at.Before(admission.NextPoll) {
				return nil
			}
		}
		attempts++
		if _, err := conn.ExecContext(ctx, `INSERT INTO ci_poll_attempts(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,attempt,polled_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, attempts, at.Format(time.RFC3339Nano)); err != nil {
			return err
		}
		admission.Due, admission.Attempt = true, epochAttempts+1
		admission.NextPoll = at.Add(ciPollBackoff(epochAttempts + 1))
		return nil
	})
	return admission, err
}

func loadOrCreateCIPollSchedule(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, publication PublishedCandidateEvidence, at time.Time) (time.Time, time.Time, int, error) {
	var firstRaw, deadlineRaw string
	var maximum int
	err := conn.QueryRowContext(ctx, `SELECT first_polled_at,deadline_at,max_attempts FROM ci_poll_schedules WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=?`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&firstRaw, &deadlineRaw, &maximum)
	if err == sql.ErrNoRows {
		deadline := at.Add(ciPollDeadline)
		if _, err = conn.ExecContext(ctx, `INSERT INTO ci_poll_schedules(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,first_polled_at,deadline_at,max_attempts) VALUES(?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, at.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano), ciPollMaxAttempts); err != nil {
			return time.Time{}, time.Time{}, 0, err
		}
		return at, deadline, ciPollMaxAttempts, nil
	}
	if err != nil {
		return time.Time{}, time.Time{}, 0, normalizeBusy(ctx, err)
	}
	first, firstErr := parsePublicationTime(firstRaw)
	deadline, deadlineErr := parsePublicationTime(deadlineRaw)
	if firstErr != nil || deadlineErr != nil || maximum != ciPollMaxAttempts || !deadline.Equal(first.Add(ciPollDeadline)) {
		return time.Time{}, time.Time{}, 0, ErrCIObservation
	}
	return first, deadline, maximum, nil
}

func loadCIPollAttempts(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, publication PublishedCandidateEvidence) (int, time.Time, error) {
	var count, maximum int
	var lastRaw string
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(attempt),0),COALESCE((SELECT polled_at FROM ci_poll_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=? ORDER BY attempt DESC LIMIT 1),'') FROM ci_poll_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=?`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&count, &maximum, &lastRaw)
	if err != nil {
		return 0, time.Time{}, normalizeBusy(ctx, err)
	}
	if count != maximum {
		return 0, time.Time{}, ErrCIObservation
	}
	if count == 0 {
		return 0, time.Time{}, nil
	}
	last, err := parsePublicationTime(lastRaw)
	if err != nil || !canonicalPublicationTimestamp(last) {
		return 0, time.Time{}, ErrCIObservation
	}
	return count, last, nil
}

func loadCIPollRetryEpoch(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, publication PublishedCandidateEvidence) (ciPollRetryEpoch, bool, error) {
	var value ciPollRetryEpoch
	var resumedRaw, deadlineRaw string
	err := q.QueryRowContext(ctx, `SELECT initial_attempts,exhaustion_ticket_version,resume_ticket_version,resume_leader_epoch,resume_runner_epoch,resumed_at,deadline_at,retry_digest FROM ci_poll_retry_epochs WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND publication_witness_digest=?`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest).Scan(&value.initialAttempts, &value.exhaustedVersion, &value.resumeVersion, &value.leader, &value.runner, &resumedRaw, &deadlineRaw, &value.digest)
	if err == sql.ErrNoRows {
		return ciPollRetryEpoch{}, false, nil
	}
	if err != nil {
		return ciPollRetryEpoch{}, false, normalizeBusy(ctx, err)
	}
	var parseErr error
	value.resumedAt, parseErr = parsePublicationTime(resumedRaw)
	if parseErr == nil {
		value.deadline, parseErr = parsePublicationTime(deadlineRaw)
	}
	if parseErr != nil || !value.deadline.Equal(value.resumedAt.Add(ciPollDeadline)) || !validCIAuthorityDigest(value.digest) {
		return ciPollRetryEpoch{}, false, ErrCIObservation
	}
	return value, true, nil
}

// findCIPollResumePair locates the one initial exhaustion/resume control pair
// for this candidate. The pair is not tied to a fixed ticket version: pending
// observations and signed runner recovery rows may occupy arbitrary versions
// between publication and exhaustion.
func findCIPollResumePair(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, baselineVersion, liveVersion uint64) (ciPollResumePair, bool, error) {
	if baselineVersion == 0 || liveVersion < baselineVersion {
		return ciPollResumePair{}, false, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT ticket_version,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND trigger='ci_poll_exhausted' AND from_state='waiting_ci' AND to_state='paused' ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baselineVersion, liveVersion)
	if err != nil {
		return ciPollResumePair{}, false, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var pair ciPollResumePair
	var found bool
	for rows.Next() {
		var exhausted uint64
		var payload string
		if err := rows.Scan(&exhausted, &payload); err != nil {
			return ciPollResumePair{}, false, err
		}
		var value struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal([]byte(payload), &value); err != nil {
			return ciPollResumePair{}, false, ErrCIObservation
		}
		if value.Code != "ci_poll_attempts_exhausted" && value.Code != "ci_poll_deadline_exhausted" {
			continue
		}
		if exhausted == ^uint64(0) || exhausted+1 > liveVersion || !authenticateCIPollResume(ctx, q, ref, exhausted, exhausted+1) {
			return ciPollResumePair{}, false, nil
		}
		var resumedRaw string
		if err := q.QueryRowContext(ctx, `SELECT created_at FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, exhausted+1).Scan(&resumedRaw); err != nil {
			return ciPollResumePair{}, false, normalizeBusy(ctx, err)
		}
		resumedAt, err := parsePublicationTime(resumedRaw)
		if err != nil {
			return ciPollResumePair{}, false, ErrCIObservation
		}
		if found {
			return ciPollResumePair{}, false, ErrCIObservation
		}
		pair = ciPollResumePair{exhaustedVersion: exhausted, resumeVersion: exhausted + 1, resumedAt: resumedAt}
		found = true
	}
	if err := rows.Err(); err != nil {
		return ciPollResumePair{}, false, err
	}
	return pair, found, nil
}

func authorizeCIPollRetry(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, publication PublishedCandidateEvidence, attempts int, version, runner, leader uint64, at time.Time) (ciPollRetryEpoch, bool, error) {
	if publication.CurrentTicketVersion > ^uint64(0)-3 {
		return ciPollRetryEpoch{}, false, nil
	}
	waitingVersion := publication.CurrentTicketVersion + 1
	if version <= waitingVersion || runner < publication.CurrentFence.RunnerEpoch || publication.CurrentFence.LeaderEpoch == 0 {
		return ciPollRetryEpoch{}, false, nil
	}
	pair, authorized, err := findCIPollResumePair(ctx, conn, ref, waitingVersion, version)
	if err != nil || !authorized {
		if err != nil {
			return ciPollRetryEpoch{}, false, err
		}
		return ciPollRetryEpoch{}, false, nil
	}
	if validationErr := validateCIRecoveryLedger(ctx, conn, ref, waitingVersion, publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch, version, runner, leader); validationErr != nil {
		return ciPollRetryEpoch{}, false, nil
	}
	if at.Before(pair.resumedAt) {
		return ciPollRetryEpoch{}, false, ErrCIObservation
	}
	deadline := pair.resumedAt.Add(ciPollDeadline)
	if attempts < 1 || attempts > ciPollMaxAttempts {
		return ciPollRetryEpoch{}, false, ErrCIObservation
	}
	value := ciPollRetryEpoch{initialAttempts: attempts, exhaustedVersion: pair.exhaustedVersion, resumeVersion: pair.resumeVersion, leader: publication.CurrentFence.LeaderEpoch, runner: publication.CurrentFence.RunnerEpoch, resumedAt: pair.resumedAt, deadline: deadline}
	value.digest = ciPollRetryEpochDigest(ref, publication, value.initialAttempts, value.exhaustedVersion, value.resumeVersion, value.leader, value.runner, value.resumedAt, value.deadline)
	if _, err := conn.ExecContext(ctx, `INSERT INTO ci_poll_retry_epochs(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,initial_attempts,exhaustion_ticket_version,resume_ticket_version,resume_leader_epoch,resume_runner_epoch,resumed_at,deadline_at,retry_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, value.initialAttempts, value.exhaustedVersion, value.resumeVersion, value.leader, value.runner, value.resumedAt.Format(time.RFC3339Nano), value.deadline.Format(time.RFC3339Nano), value.digest); err != nil {
		return ciPollRetryEpoch{}, false, err
	}
	return value, true, nil
}

// authenticateCIPollResume permits the one operator resume which establishes
// retry epoch 2. It deliberately excludes retry-epoch terminal codes, so the
// generic publication resume path cannot turn a consumed retry window into a
// third polling budget.
func authenticateCIPollResume(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, pausedVersion, resumedVersion uint64) bool {
	if pausedVersion == 0 || resumedVersion != pausedVersion+1 {
		return false
	}
	var exhaustedCount, resumedCount int
	var payload string
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(payload),'') FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='ci_poll_exhausted' AND from_state='waiting_ci' AND to_state='paused'`, ref.Channel, ref.Project, ref.Ticket, pausedVersion).Scan(&exhaustedCount, &payload); err != nil || exhaustedCount != 1 {
		return false
	}
	var value struct {
		Code string `json:"code"`
	}
	if json.Unmarshal([]byte(payload), &value) != nil || (value.Code != "ci_poll_attempts_exhausted" && value.Code != "ci_poll_deadline_exhausted") {
		return false
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='waiting_ci'`, ref.Channel, ref.Project, ref.Ticket, resumedVersion).Scan(&resumedCount); err != nil {
		return false
	}
	if resumedCount != 1 {
		return false
	}
	var total int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version IN (?,?)`, ref.Channel, ref.Project, ref.Ticket, pausedVersion, resumedVersion).Scan(&total); err != nil {
		return false
	}
	return total == 2
}

func ciPollRetryEpochDigest(ref domain.TicketRef, publication PublishedCandidateEvidence, initialAttempts int, exhaustedVersion, resumeVersion, leader, runner uint64, resumedAt, deadline time.Time) string {
	body, err := json.Marshal(struct {
		Ref              domain.TicketRef `json:"ref"`
		Generation       uint64           `json:"generation"`
		Head             string           `json:"head"`
		Tree             string           `json:"tree"`
		Witness          string           `json:"witness"`
		InitialAttempts  int              `json:"initial_attempts"`
		ExhaustedVersion uint64           `json:"exhausted_version"`
		ResumeVersion    uint64           `json:"resume_version"`
		Leader           uint64           `json:"leader"`
		Runner           uint64           `json:"runner"`
		ResumedAt        string           `json:"resumed_at"`
		Deadline         string           `json:"deadline"`
	}{ref, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA, publication.WitnessDigest, initialAttempts, exhaustedVersion, resumeVersion, leader, runner, resumedAt.Format(time.RFC3339Nano), deadline.Format(time.RFC3339Nano)})
	if err != nil {
		return ""
	}
	return ciAuthorityDigest(body)
}

func pauseCIPoll(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version, runner uint64, at time.Time, deadline, retry bool, admission *CIPollAdmission) error {
	admission.Expired = true
	if retry {
		if deadline {
			admission.PauseCode = "ci_poll_retry_deadline_exhausted"
		} else {
			admission.PauseCode = "ci_poll_retry_attempts_exhausted"
		}
	} else if deadline {
		admission.PauseCode = "ci_poll_deadline_exhausted"
	} else {
		admission.PauseCode = "ci_poll_attempts_exhausted"
	}
	nextAction := "inspect the required checks, then explicitly resume waiting_ci once after resolving the delay"
	if retry {
		nextAction = "submit a fresh ticket or reconcile the CI result manually; the one bounded CI retry window has been consumed"
	}
	payload, err := json.Marshal(map[string]any{"code": admission.PauseCode, "reason": "required CI checks did not reach a terminal result before the bounded polling limit", "next_action": nextAction})
	if err != nil {
		return ErrCIObservation
	}
	result, err := conn.ExecContext(ctx, `UPDATE tickets SET state='paused',resume_state='waiting_ci',version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='waiting_ci' AND version=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, version, runner)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count != 1 {
		return ErrStaleFence
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, version+1, "ci_poll_exhausted", domain.StateWaitingCI, domain.StatePaused, string(payload), at.Format(time.RFC3339Nano))
	return err
}

func canonicalPublicationTimestamp(at time.Time) bool {
	_, ok := canonicalPublicationTime(at)
	return ok
}
