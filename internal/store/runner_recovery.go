package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// RunnerRecoveryLedger is the authenticated proof for one durable +1/+1
// runner fence. A zero prior leader means the ticket predates this ledger and
// has no recoverable leader predecessor; publication/waiting tickets always
// bind their prior leader to the publication witness before using the row.
type RunnerRecoveryLedger struct {
	Ref                domain.TicketRef
	PriorTicketVersion uint64
	PriorRunnerEpoch   uint64
	PriorLeaderEpoch   uint64
	TicketVersion      uint64
	RunnerEpoch        uint64
	LeaderEpoch        uint64
	RecoveryDigest     string
	CreatedAt          time.Time
}

func validateWaitingRecoveryLedger(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, baselineVersion, baselineRunner, baselineLeader, liveVersion, liveRunner, liveLeader uint64) error {
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	if _, found, err := loadRunnerRecoveryAt(ctx, q, ref, baselineVersion); err != nil {
		return err
	} else if found {
		// The transition version is the exact publication witness successor;
		// recovery rows may begin only after it. A row here would be an
		// unproven extra state advance, not a zero-recovery replay.
		return ErrPublicationEvidence
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baselineVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	expectedVersion, expectedRunner, expectedLeader := baselineVersion, baselineRunner, baselineLeader
	var count int
	for rows.Next() {
		count++
		if count > 64 {
			return ErrPublicationEvidence
		}
		var step RunnerRecoveryLedger
		var createdAt string
		if err := rows.Scan(&step.PriorTicketVersion, &step.PriorRunnerEpoch, &step.PriorLeaderEpoch, &step.TicketVersion, &step.RunnerEpoch, &step.LeaderEpoch, &step.RecoveryDigest, &createdAt); err != nil {
			return err
		}
		step.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
		if err != nil {
			return ErrPublicationEvidence
		}
		step.Ref = ref
		if !validRunnerRecovery(step) || step.PriorTicketVersion != expectedVersion || step.PriorRunnerEpoch != expectedRunner || step.PriorLeaderEpoch != expectedLeader || step.TicketVersion != expectedVersion+1 || step.RunnerEpoch != expectedRunner+1 || step.LeaderEpoch <= expectedLeader {
			return ErrPublicationEvidence
		}
		var events int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&events); err != nil || events != 0 {
			return ErrPublicationEvidence
		}
		expectedVersion, expectedRunner, expectedLeader = step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if liveVersion != expectedVersion || liveRunner != expectedRunner || liveLeader != expectedLeader {
		return ErrPublicationEvidence
	}
	return nil
}

func runnerRecoveryPayload(value RunnerRecoveryLedger) ([]byte, error) {
	return json.Marshal(struct {
		Channel            domain.Channel   `json:"channel"`
		Project            domain.ProjectID `json:"project"`
		Ticket             domain.TicketID  `json:"ticket"`
		PriorTicketVersion uint64           `json:"prior_ticket_version"`
		PriorRunnerEpoch   uint64           `json:"prior_runner_epoch"`
		PriorLeaderEpoch   uint64           `json:"prior_leader_epoch"`
		TicketVersion      uint64           `json:"ticket_version"`
		RunnerEpoch        uint64           `json:"runner_epoch"`
		LeaderEpoch        uint64           `json:"leader_epoch"`
		CreatedAt          string           `json:"created_at"`
	}{value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.PriorTicketVersion, value.PriorRunnerEpoch, value.PriorLeaderEpoch, value.TicketVersion, value.RunnerEpoch, value.LeaderEpoch, value.CreatedAt.UTC().Format(time.RFC3339Nano)})
}

func validRunnerRecovery(value RunnerRecoveryLedger) bool {
	if value.Ref.Validate() != nil || value.PriorTicketVersion == 0 || value.PriorRunnerEpoch == 0 || value.TicketVersion != value.PriorTicketVersion+1 || value.RunnerEpoch != value.PriorRunnerEpoch+1 || value.LeaderEpoch == 0 || (value.PriorLeaderEpoch > 0 && value.LeaderEpoch <= value.PriorLeaderEpoch) || value.CreatedAt.IsZero() || !validClaimDigest(value.RecoveryDigest) {
		return false
	}
	payload, err := runnerRecoveryPayload(value)
	return err == nil && publicationIdentityDigest(payload) == value.RecoveryDigest
}

func loadRunnerRecoveryAt(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, version uint64) (RunnerRecoveryLedger, bool, error) {
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	var value RunnerRecoveryLedger
	var createdAt string
	err := q.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&value.PriorTicketVersion, &value.PriorRunnerEpoch, &value.PriorLeaderEpoch, &value.TicketVersion, &value.RunnerEpoch, &value.LeaderEpoch, &value.RecoveryDigest, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RunnerRecoveryLedger{}, false, nil
	}
	if err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	value.Ref = ref
	value.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
	if err != nil {
		return RunnerRecoveryLedger{}, false, ErrPublicationEvidence
	}
	return value, true, nil
}

func loadLatestRunnerRecovery(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef) (RunnerRecoveryLedger, bool, error) {
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	var value RunnerRecoveryLedger
	var createdAt string
	err := q.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&value.PriorTicketVersion, &value.PriorRunnerEpoch, &value.PriorLeaderEpoch, &value.TicketVersion, &value.RunnerEpoch, &value.LeaderEpoch, &value.RecoveryDigest, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RunnerRecoveryLedger{}, false, nil
	}
	if err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	value.Ref = ref
	value.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
	if err != nil {
		return RunnerRecoveryLedger{}, false, ErrPublicationEvidence
	}
	return value, true, nil
}

// validateRunnerRecoveryCardinality enforces the ticket-wide, lifetime cap.
// Publication recovery has a later semantic baseline, but no phase is allowed
// to mint a second 64-row recovery budget.
func validateRunnerRecoveryCardinality(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef) error {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil {
		return err
	}
	if count > 64 {
		return ErrPublicationEvidence
	}
	return nil
}

func parseRunnerRecoveryTime(raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339Nano) != raw {
		return time.Time{}, errors.New("invalid runner recovery timestamp")
	}
	return parsed, nil
}

func authenticateRunnerRecoveryStep(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, priorVersion uint64, priorFence domain.Fence, version uint64, fence domain.Fence) error {
	step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version)
	if err != nil || !found || !validRunnerRecovery(step) || step.PriorTicketVersion != priorVersion || step.PriorRunnerEpoch != priorFence.RunnerEpoch || step.PriorLeaderEpoch != priorFence.LeaderEpoch || step.TicketVersion != version || step.RunnerEpoch != fence.RunnerEpoch || step.LeaderEpoch != fence.LeaderEpoch {
		return ErrPublicationEvidence
	}
	return nil
}

func recordRunnerRecovery(ctx context.Context, conn *sql.Conn, value RunnerRecoveryLedger) error {
	if value.CreatedAt.IsZero() {
		return ErrPublicationEvidence
	}
	if !validRunnerRecovery(value) {
		return ErrPublicationEvidence
	}
	_, err := conn.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.PriorTicketVersion, value.PriorRunnerEpoch, value.PriorLeaderEpoch, value.TicketVersion, value.RunnerEpoch, value.LeaderEpoch, value.RecoveryDigest, value.CreatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func runnerRecoveryDigest(value RunnerRecoveryLedger) string {
	payload, _ := runnerRecoveryPayload(value)
	return publicationIdentityDigest(payload)
}

func (s *Store) publicationRecoveryBaseline(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version, runner uint64) (uint64, bool, error) {
	publication, found, err := loadPublicationEvidenceRow(ctx, conn, ref)
	if err != nil || !found {
		return 0, false, err
	}
	if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
		return 0, false, err
	}
	// A publishing witness may be consumed by the one-version
	// publishing->waiting_ci transition before its first recovery fence. In
	// that case the current ticket is exactly one version beyond the witness,
	// with the same runner; this is still the witness's authenticated leader
	// predecessor, not a counter-based inference.
	if (publication.CurrentTicketVersion != version && publication.CurrentTicketVersion+1 != version) || publication.CurrentFence.RunnerEpoch != runner {
		return 0, false, nil
	}
	return publication.CurrentFence.LeaderEpoch, true, nil
}
