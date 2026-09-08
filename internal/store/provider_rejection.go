package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

var ErrProviderServerRejection = errors.New("provider server rejection evidence is invalid")
var ErrProviderRetryBackoff = errors.New("provider retry backoff has not elapsed")

// ProviderRetryBackoffError carries only the authenticated persisted deadline.
// Waiting does not grant admission; BeginProviderAttempt must be called again.
type ProviderRetryBackoffError struct{ NotBefore time.Time }

func (e *ProviderRetryBackoffError) Error() string { return ErrProviderRetryBackoff.Error() }
func (e *ProviderRetryBackoffError) Unwrap() error { return ErrProviderRetryBackoff }

const providerServerRejected = "server_rejected"

// PendingProviderServerRejection pins the remaining retry to its authenticated
// source runtime before the coordinator probes any provider or fallback.
func (s *Store) PendingProviderServerRejection(ctx context.Context, ref domain.TicketRef, phase domain.Phase, role string, version uint64, fence domain.Fence) (ProviderAttemptClaim, bool, error) {
	var claim ProviderAttemptClaim
	var found bool
	err := s.write(ctx, func(conn *sql.Conn) error {
		var err error
		claim, found, err = s.pendingProviderTerminalClaim(ctx, conn, ref, phase, role, version, fence, providerServerRejected, true, true)
		return err
	})
	return claim, found, err
}

// ProviderRejectionRetryCheckpoint identifies an active attempt immediately
// following a signed rejection in the same phase entry. Physical inspection
// must happen under this new attempt's exclusion, not just before its backoff.
// The returned metadata is not itself a filesystem observation.
func (s *Store) ProviderRejectionRetryCheckpoint(ctx context.Context, claim ProviderAttemptClaim) (ProviderAttemptCheckpoint, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProviderAttemptCheckpoint{}, false, err
	}
	defer tx.Rollback()
	source, err := loadAuthenticatedProviderAttemptClaim(ctx, tx, claim.ID)
	if err != nil || !sameImmutableProviderAttemptClaim(claim, source) {
		return ProviderAttemptCheckpoint{}, false, ErrProviderAttempt
	}
	var priorID int64
	err = tx.QueryRowContext(ctx, `SELECT a.id FROM provider_phase_attempt_entries current
		JOIN provider_phase_attempt_entries prior ON prior.channel=current.channel AND prior.project_id=current.project_id AND prior.ticket_id=current.ticket_id AND prior.phase=current.phase AND prior.entry_ticket_version=current.entry_ticket_version AND prior.attempt=current.attempt-1
		JOIN provider_attempts a ON a.id=prior.provider_attempt_id
		LEFT JOIN provider_server_rejections sr ON sr.provider_attempt_id=a.id
		WHERE current.provider_attempt_id=? AND (a.outcome='server_rejected' OR sr.provider_attempt_id IS NOT NULL)`, claim.ID).Scan(&priorID)
	if errors.Is(err, sql.ErrNoRows) {
		return ProviderAttemptCheckpoint{}, false, nil
	}
	if err != nil {
		return ProviderAttemptCheckpoint{}, false, err
	}
	prior, err := loadAuthenticatedProviderAttemptClaim(ctx, tx, priorID)
	if err != nil || prior.Binding != claim.Binding || prior.Role != claim.Role {
		return ProviderAttemptCheckpoint{}, false, ErrProviderServerRejection
	}
	if _, _, err := loadServerRejectionFrom(ctx, tx, prior); err != nil {
		return ProviderAttemptCheckpoint{}, false, err
	}
	checkpoint, err := s.providerAttemptCheckpointFrom(ctx, tx, claim)
	return checkpoint, err == nil, err
}

// FinishProviderAttemptWithServerRejection is the only writer of rejection
// evidence. Receipt, failed attempt/phase and exact lease release share one
// transaction. Generic failure APIs cannot mint this outcome.
func (s *Store) FinishProviderAttemptWithServerRejection(ctx context.Context, claim ProviderAttemptClaim, drain contracts.DrainProof, receipt contracts.ServerRejectionAttestation, finished time.Time) error {
	return s.finishProviderAttemptWithReceipt(ctx, claim, drain, claim.ExpectedVersion, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, "failed", providerServerRejected, 0, finished, nil, nil, &receipt)
}

func (s *Store) recordServerRejectionFrom(ctx context.Context, conn *sql.Conn, claim ProviderAttemptClaim, receipt contracts.ServerRejectionAttestation, finished time.Time) error {
	raw, digest, deadline, err := canonicalServerRejection(claim, receipt)
	if err != nil || finished.UnixNano() < receipt.Evidence.ObservedUnixNanos || finished.Sub(time.Unix(0, receipt.Evidence.ObservedUnixNanos)) > 30*time.Second {
		return ErrProviderServerRejection
	}
	checkpoint, err := s.providerAttemptCheckpointFrom(ctx, conn, claim)
	if err != nil {
		return err
	}
	checkpointDigest, err := ProviderAttemptCheckpointDigest(checkpoint, claim.RequestDigest)
	if err != nil || checkpoint.ExpectedHead != receipt.Evidence.CheckpointHeadOID || checkpointDigest != receipt.Evidence.CheckpointDigest {
		return ErrProviderServerRejection
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO provider_server_rejections(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,canonical_receipt,receipt_sha256,observed_unix_nanos,not_before_unix_nanos) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Role, claim.Attempt, raw, digest, receipt.Evidence.ObservedUnixNanos, deadline)
	return err
}

func loadServerRejectionFrom(ctx context.Context, q rowQueryer, claim ProviderAttemptClaim) (contracts.ServerRejectionAttestation, int64, error) {
	var raw []byte
	var digest string
	var observed, deadline int64
	err := q.QueryRowContext(ctx, `SELECT canonical_receipt,receipt_sha256,observed_unix_nanos,not_before_unix_nanos FROM provider_server_rejections WHERE provider_attempt_id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND role=? AND attempt=?`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Role, claim.Attempt).Scan(&raw, &digest, &observed, &deadline)
	if err != nil {
		return contracts.ServerRejectionAttestation{}, 0, ErrProviderServerRejection
	}
	value, err := decodeServerRejection(claim, raw, digest, observed, deadline)
	if err != nil {
		return contracts.ServerRejectionAttestation{}, 0, err
	}
	var terminal int
	err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts a JOIN phase_runs p ON p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.phase=a.phase AND p.attempt=a.attempt WHERE a.id=? AND a.state='failed' AND a.outcome='server_rejected' AND a.launch_state='drained' AND p.state=a.state AND p.outcome=a.outcome AND p.leader_epoch=a.leader_epoch AND p.runner_epoch=a.runner_epoch AND p.expected_ticket_version=a.expected_ticket_version AND p.provider=a.provider AND p.model=a.model AND p.family=a.family AND p.provider_version=a.version AND p.worktree_identity=a.worktree_identity AND p.base_sha=a.base_sha AND a.finished_at<>'' AND p.completed_at=a.finished_at AND NOT EXISTS(SELECT 1 FROM provider_attempt_results WHERE provider_attempt_id=a.id)`, claim.ID).Scan(&terminal)
	if err != nil || terminal != 1 {
		return contracts.ServerRejectionAttestation{}, 0, ErrProviderServerRejection
	}
	return value, deadline, nil
}

func serverRejectionAdmission(ctx context.Context, conn *sql.Conn, r ProviderAttemptRequest, entry providerPhaseEntry, input contracts.PhaseInput, entryRuns int) error {
	rows, err := conn.QueryContext(ctx, `SELECT a.id FROM provider_phase_attempt_entries pe JOIN provider_attempts a ON a.id=pe.provider_attempt_id LEFT JOIN provider_server_rejections sr ON sr.provider_attempt_id=a.id WHERE pe.channel=? AND pe.project_id=? AND pe.ticket_id=? AND pe.phase=? AND pe.entry_ticket_version=? AND (a.outcome='server_rejected' OR sr.provider_attempt_id IS NOT NULL) ORDER BY a.attempt`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, entry.Version)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
		if len(ids) > 4 {
			rows.Close()
			return ErrProviderServerRejection
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		claim, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, id)
		if err != nil || claim.Binding != r.Binding || claim.Role != r.Role {
			return ErrProviderServerRejection
		}
		_, deadline, err := loadServerRejectionFrom(ctx, conn, claim)
		if err != nil {
			return err
		}
		if r.At.UnixNano() < deadline {
			return &ProviderRetryBackoffError{NotBefore: time.Unix(0, deadline)}
		}
		if claim.Attempt == input.Attempt-1 && entryRuns%2 == 1 {
			if err := validateProviderAttemptEndpointAdvance(ctx, conn, r.Ref, r.Phase, providerAttemptEndpoint{version: claim.ExpectedVersion, runner: claim.RunnerEpoch, leader: claim.LeaderEpoch}, providerAttemptEndpoint{version: r.ExpectedVersion, runner: r.Fence.RunnerEpoch, leader: r.Fence.LeaderEpoch}); err != nil {
				return err
			}
			if input.Timeout <= 0 || input.Timeout > claim.Input.Timeout {
				return ErrProviderServerRejection
			}
			proposed := input
			proposed.Attempt, proposed.ExpectedVersion, proposed.LeaderEpoch, proposed.RunnerEpoch = claim.Input.Attempt, claim.Input.ExpectedVersion, claim.Input.LeaderEpoch, claim.Input.RunnerEpoch
			proposed.Timeout = claim.Input.Timeout
			if !contracts.PhaseInputMatchesAuthenticatedClaim(proposed, claim.Input, claim.RequestDigest) {
				return ErrProviderServerRejection
			}
		}
	}
	return nil
}

// canonicalServerRejection authenticates the exact immutable source claim.
// The deadline is derived solely from signed observation time, not load time;
// reopening or replaying cannot extend or shorten the backoff. The last CLI
// internal delay is not treated as a final HTTP Retry-After header.
func canonicalServerRejection(claim ProviderAttemptClaim, value contracts.ServerRejectionAttestation) ([]byte, string, int64, error) {
	if !contracts.VerifyServerRejection(claim.SupervisorKey, drainRequestForClaim(claim), value) || value.Evidence.ObservedUnixNanos > int64(^uint64(0)>>1)-int64(3*time.Second) {
		return nil, "", 0, ErrProviderServerRejection
	}
	// A deterministic, bounded spread avoids simultaneous retries without
	// using fresh randomness after restart. StreamDigest has verified hex shape.
	delay := 2*time.Second + time.Duration(value.Evidence.StreamDigest[0]%10)*100*time.Millisecond
	deadline := value.Evidence.ObservedUnixNanos + int64(delay)
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 131072 {
		return nil, "", 0, ErrProviderServerRejection
	}
	return raw, rawDigest(raw), deadline, nil
}

func decodeServerRejection(claim ProviderAttemptClaim, raw []byte, digest string, observed, deadline int64) (contracts.ServerRejectionAttestation, error) {
	if len(raw) == 0 || len(raw) > 131072 || rawDigest(raw) != digest {
		return contracts.ServerRejectionAttestation{}, ErrProviderServerRejection
	}
	var value contracts.ServerRejectionAttestation
	if json.Unmarshal(raw, &value) != nil {
		return contracts.ServerRejectionAttestation{}, ErrProviderServerRejection
	}
	canonical, wantDigest, wantDeadline, err := canonicalServerRejection(claim, value)
	if err != nil || !bytes.Equal(raw, canonical) || digest != wantDigest || observed != value.Evidence.ObservedUnixNanos || deadline != wantDeadline {
		return contracts.ServerRejectionAttestation{}, ErrProviderServerRejection
	}
	return value, nil
}

// v60 reserves immutable rejection evidence and its once-derived retry time.
// It does not backfill legacy failures or make them eligible for retry.
// Only the atomic signed finish boundary may populate this table;
// no generic observation/write API is exposed.
var migrationV60 = append([]string{
	`CREATE TABLE provider_server_rejections (
		provider_attempt_id INTEGER PRIMARY KEY,
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		phase TEXT NOT NULL, role TEXT NOT NULL, attempt INTEGER NOT NULL CHECK(attempt>0),
		canonical_receipt BLOB NOT NULL CHECK(length(canonical_receipt)>0 AND length(canonical_receipt)<=131072 AND json_valid(CAST(canonical_receipt AS TEXT))),
		receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64 AND receipt_sha256 NOT GLOB '*[^0-9a-f]*'),
		observed_unix_nanos INTEGER NOT NULL CHECK(observed_unix_nanos>0),
		not_before_unix_nanos INTEGER NOT NULL CHECK(not_before_unix_nanos>observed_unix_nanos AND not_before_unix_nanos-observed_unix_nanos<=30000000000),
		FOREIGN KEY(channel,project_id,ticket_id,phase,role,attempt,provider_attempt_id)
		REFERENCES provider_attempts(channel,project_id,ticket_id,phase,role,attempt,id)
	)`,
	`CREATE TRIGGER provider_server_rejections_immutable_update BEFORE UPDATE ON provider_server_rejections BEGIN SELECT RAISE(ABORT,'provider rejection is immutable'); END`,
	`CREATE TRIGGER provider_server_rejections_immutable_delete BEFORE DELETE ON provider_server_rejections BEGIN SELECT RAISE(ABORT,'provider rejection is append-only'); END`,
}, serverRejectionOutcomeMigration()...)

func serverRejectionOutcomeMigration() []string {
	var statements []string
	for _, table := range []string{"provider_attempts", "phase_runs"} {
		prefix, completed, quarantine := "provider_attempt", "NEW.outcome='completed'", " OR (NEW.state='quarantined' AND NEW.outcome IN ('undrained','undrained_recovery'))"
		if table == "phase_runs" {
			prefix, completed, quarantine = "phase_run", "NEW.outcome IN ('completed','passed')", ""
		}
		for _, action := range []string{"insert", "update"} {
			name := prefix + "_state_outcome_" + action
			when := "INSERT"
			if action == "update" {
				when = "UPDATE OF state,outcome"
			}
			statements = append(statements, "DROP TRIGGER "+name, fmt.Sprintf(`CREATE TRIGGER %s BEFORE %s ON %s WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND %s) OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery'))%s OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable','invocation_failed','result_indeterminate','server_rejected'))) BEGIN SELECT RAISE(ABORT,'invalid provider phase outcome'); END`, name, when, table, completed, quarantine))
		}
	}
	return statements
}
