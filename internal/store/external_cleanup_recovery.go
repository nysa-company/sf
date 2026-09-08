package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/hostidentity"
)

var ErrExternalRecovery = errors.New("external cleanup recovery evidence is unavailable or inconsistent")
var ErrExternalRecoveryReboot = errors.New("external cleanup recovery requires a new boot on the checkpointed host")

type ExternalCleanupRecoveryStatus struct {
	State   string `json:"state"`
	Changed bool   `json:"changed"`
}

type externalCleanupCheckpoint struct {
	Schema               string
	QuarantineObservedAt string
	Reason               string
	Host                 hostidentity.Identity
	Channel              domain.Channel
	Leader               uint64
	RecordedAt           string
}

func recoveryBytes(value any) ([]byte, string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(payload)
	return payload, hex.EncodeToString(digest[:]), nil
}

func validRecoveryTime(value string) bool {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && parsed.UTC().Format(time.RFC3339Nano) == value
}

func loadExternalCleanupCheckpoint(ctx context.Context, conn *sql.Conn, observedAt, reason string) (externalCleanupCheckpoint, string, error) {
	var raw, digest string
	var checkpoint externalCleanupCheckpoint
	if err := conn.QueryRowContext(ctx, `SELECT checkpoint,checkpoint_digest FROM external_cleanup_recovery_checkpoints WHERE quarantine_observed_at=?`, observedAt).Scan(&raw, &digest); err != nil {
		return checkpoint, "", err
	}
	if len(raw) > 4096 || json.Unmarshal([]byte(raw), &checkpoint) != nil {
		return checkpoint, "", ErrExternalRecovery
	}
	canonical, actual, err := recoveryBytes(checkpoint)
	if err != nil || !bytes.Equal(canonical, []byte(raw)) || actual != digest || checkpoint.Schema != "sf.external-cleanup-checkpoint/v1" || checkpoint.QuarantineObservedAt != observedAt || checkpoint.Reason != reason || reason != "cleanup_uncertain" || !validRecoveryTime(observedAt) || !validRecoveryTime(checkpoint.RecordedAt) || !checkpoint.Host.Valid() || !checkpoint.Channel.Valid() || checkpoint.Leader == 0 {
		return checkpoint, "", ErrExternalRecovery
	}
	return checkpoint, digest, nil
}

// PrepareExternalCleanupRecovery records current OS facts without opening the
// gate. In particular, it works while the in-memory mutation gate is latched.
// Callers cannot supply or self-attest host/boot evidence.
func (s *Store) PrepareExternalCleanupRecovery(ctx context.Context, channel domain.Channel, leader uint64) (ExternalCleanupRecoveryStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	host, err := hostidentity.Observe(ctx)
	if err != nil {
		return ExternalCleanupRecoveryStatus{}, err
	}
	return s.prepareExternalCleanupRecoveryAt(ctx, channel, leader, host)
}

func (s *Store) prepareExternalCleanupRecoveryAt(ctx context.Context, channel domain.Channel, leader uint64, host hostidentity.Identity) (ExternalCleanupRecoveryStatus, error) {
	status := ExternalCleanupRecoveryStatus{}
	if !channel.Valid() || leader == 0 || !host.Valid() {
		return status, ErrExternalRecovery
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := assertRepositoryRecoveryLeader(ctx, conn, channel, leader); err != nil {
			return err
		}
		var observedAt, reason string
		err := conn.QueryRowContext(ctx, `SELECT observed_at,reason FROM external_mutation_quarantine WHERE singleton=1`).Scan(&observedAt, &reason)
		if err == sql.ErrNoRows {
			status.State = "clear"
			return nil
		}
		if err != nil {
			return err
		}
		if reason != "cleanup_uncertain" || !validRecoveryTime(observedAt) {
			return ErrExternalRecovery
		}
		checkpoint, _, err := loadExternalCleanupCheckpoint(ctx, conn, observedAt, reason)
		if err == sql.ErrNoRows {
			checkpoint = externalCleanupCheckpoint{"sf.external-cleanup-checkpoint/v1", observedAt, reason, host, channel, leader, time.Now().UTC().Format(time.RFC3339Nano)}
			payload, digest, err := recoveryBytes(checkpoint)
			if err != nil {
				return err
			}
			if _, err = conn.ExecContext(ctx, `INSERT INTO external_cleanup_recovery_checkpoints(quarantine_observed_at,checkpoint,checkpoint_digest) VALUES(?,?,?)`, observedAt, string(payload), digest); err != nil {
				return err
			}
			status.Changed = true
		} else if err != nil {
			return err
		}
		if checkpoint.Channel != channel || checkpoint.Leader > leader || checkpoint.Host.MachineDigest != host.MachineDigest {
			return ErrExternalRecovery
		}
		status.State = "reboot_required"
		if checkpoint.Host.BootID != host.BootID {
			status.State = "recovery_ready"
		}
		return nil
	})
	return status, err
}

// RecoverExternalCleanup retires only the exact checkpointed quarantine after
// a same-machine reboot. Effects remain uncertain for normal reconciliation.
func (s *Store) RecoverExternalCleanup(ctx context.Context, channel domain.Channel, leader uint64) (ExternalCleanupRecoveryStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	host, err := hostidentity.Observe(ctx)
	if err != nil {
		return ExternalCleanupRecoveryStatus{}, err
	}
	return s.recoverExternalCleanupAt(ctx, channel, leader, host)
}

func inspectExternalCleanupRecovery(ctx context.Context, conn *sql.Conn, channel domain.Channel, leader uint64, host hostidentity.Identity) (externalCleanupCheckpoint, string, error) {
	if err := assertRepositoryRecoveryLeader(ctx, conn, channel, leader); err != nil {
		if err == sql.ErrNoRows {
			return externalCleanupCheckpoint{}, "", ErrStaleFence
		}
		return externalCleanupCheckpoint{}, "", err
	}
	var observedAt, reason string
	if err := conn.QueryRowContext(ctx, `SELECT observed_at,reason FROM external_mutation_quarantine WHERE singleton=1`).Scan(&observedAt, &reason); err != nil {
		return externalCleanupCheckpoint{}, "", err
	}
	checkpoint, digest, err := loadExternalCleanupCheckpoint(ctx, conn, observedAt, reason)
	if err != nil || checkpoint.Channel != channel || checkpoint.Leader > leader || checkpoint.Host.MachineDigest != host.MachineDigest {
		return externalCleanupCheckpoint{}, "", ErrExternalRecovery
	}
	if checkpoint.Host.BootID == host.BootID {
		return externalCleanupCheckpoint{}, "", ErrExternalRecoveryReboot
	}
	return checkpoint, digest, nil
}

func (s *Store) recoverExternalCleanupAt(ctx context.Context, channel domain.Channel, leader uint64, host hostidentity.Identity) (ExternalCleanupRecoveryStatus, error) {
	status := ExternalCleanupRecoveryStatus{}
	if !channel.Valid() || leader == 0 || !host.Valid() {
		return status, ErrExternalRecovery
	}
	// Explain an unmet reboot prerequisite even when the old in-memory gate
	// is intentionally held forever. This inspection grants no authority;
	// repeat every check after acquiring the gate before retiring anything.
	err := s.write(ctx, func(conn *sql.Conn) error {
		_, _, err := inspectExternalCleanupRecovery(ctx, conn, channel, leader, host)
		if err == sql.ErrNoRows {
			status.State = "clear"
			return nil
		}
		return err
	})
	if err != nil || status.State == "clear" {
		return status, err
	}
	if err := s.mutations.lock(ctx); err != nil {
		return status, err
	}
	defer s.mutations.unlock()
	err = s.write(ctx, func(conn *sql.Conn) error {
		checkpoint, digest, err := inspectExternalCleanupRecovery(ctx, conn, channel, leader, host)
		if err == sql.ErrNoRows {
			status.State = "clear"
			return nil
		}
		if err != nil {
			return err
		}
		observedAt, reason := checkpoint.QuarantineObservedAt, checkpoint.Reason
		proof := struct {
			CheckpointDigest string
			Host             hostidentity.Identity
			Channel          domain.Channel
			Leader           uint64
			RecoveredAt      string
		}{digest, host, channel, leader, time.Now().UTC().Format(time.RFC3339Nano)}
		payload, recoveryDigest, err := recoveryBytes(proof)
		if err != nil {
			return err
		}
		if _, err = conn.ExecContext(ctx, `INSERT INTO external_cleanup_recoveries(quarantine_observed_at,checkpoint_digest,recovery,recovery_digest) VALUES(?,?,?,?)`, observedAt, digest, string(payload), recoveryDigest); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM external_mutation_quarantine WHERE singleton=1 AND reason=? AND observed_at=?`, reason, observedAt)
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil || rows != 1 {
			return ErrExternalRecovery
		}
		status.State = "recovered"
		status.Changed = true
		return nil
	})
	return status, err
}

var migrationV58 = []string{
	`CREATE TABLE external_cleanup_recovery_checkpoints(quarantine_observed_at TEXT PRIMARY KEY, checkpoint TEXT NOT NULL CHECK(json_valid(checkpoint) AND length(checkpoint)<=4096), checkpoint_digest TEXT NOT NULL CHECK(length(checkpoint_digest)=64), UNIQUE(quarantine_observed_at,checkpoint_digest))`,
	`CREATE TABLE external_cleanup_recoveries(quarantine_observed_at TEXT PRIMARY KEY, checkpoint_digest TEXT NOT NULL, recovery TEXT NOT NULL CHECK(json_valid(recovery) AND length(recovery)<=4096), recovery_digest TEXT NOT NULL CHECK(length(recovery_digest)=64), FOREIGN KEY(quarantine_observed_at,checkpoint_digest) REFERENCES external_cleanup_recovery_checkpoints(quarantine_observed_at,checkpoint_digest))`,
	`CREATE TRIGGER external_cleanup_checkpoints_immutable_update BEFORE UPDATE ON external_cleanup_recovery_checkpoints BEGIN SELECT RAISE(ABORT,'external cleanup checkpoint is immutable'); END`,
	`CREATE TRIGGER external_cleanup_checkpoints_immutable_delete BEFORE DELETE ON external_cleanup_recovery_checkpoints BEGIN SELECT RAISE(ABORT,'external cleanup checkpoint is immutable'); END`,
	`CREATE TRIGGER external_cleanup_recoveries_immutable_update BEFORE UPDATE ON external_cleanup_recoveries BEGIN SELECT RAISE(ABORT,'external cleanup recovery is immutable'); END`,
	`CREATE TRIGGER external_cleanup_recoveries_immutable_delete BEFORE DELETE ON external_cleanup_recoveries BEGIN SELECT RAISE(ABORT,'external cleanup recovery is immutable'); END`,
}
