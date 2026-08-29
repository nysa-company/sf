package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
)

// integrity checks the database before schema code trusts it. PRAGMA results
// are intentionally read through the application pool, whose DSN enables
// foreign keys on every connection.
func (s *Store) integrity(ctx context.Context) error {
	var foreignKeys int
	if err := s.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return normalizeBusy(ctx, fmt.Errorf("read foreign_keys pragma: %w", err))
	}
	if foreignKeys != 1 {
		return fmt.Errorf("sqlite foreign_keys pragma is disabled")
	}
	var result string
	if err := s.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return normalizeBusy(ctx, fmt.Errorf("sqlite integrity check: %w", err))
	}
	if result != "ok" {
		return fmt.Errorf("sqlite integrity check failed: %s", result)
	}
	return nil
}

func (s *Store) validateSchema(ctx context.Context) error {
	if err := s.integrity(ctx); err != nil {
		return err
	}
	for version, expected := range migrationChecksums {
		var actual string
		if err := s.db.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=?`, version).Scan(&actual); err != nil {
			return fmt.Errorf("read migration %d checksum: %w", version, err)
		}
		if actual != expected {
			return fmt.Errorf("migration %d checksum mismatch", version)
		}
	}
	for table, columns := range requiredSchema {
		if err := hasColumns(ctx, s.db, table, columns...); err != nil {
			return err
		}
	}
	for table, target := range requiredForeignKeys {
		if err := hasForeignKey(ctx, s.db, table, target); err != nil {
			return err
		}
	}
	rows, err := s.db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return fmt.Errorf("foreign key check found a violation")
	}
	return rows.Err()
}

var requiredForeignKeys = map[string]string{
	"tickets":           "projects",
	"phase_runs":        "tickets",
	"events":            "tickets",
	"effects":           "tickets",
	"approvals":         "tickets",
	"worktrees":         "tickets",
	"provider_attempts": "tickets",
	"leases":            "tickets",
	"plans":             "tickets",
	"verifications":     "tickets",
}

func hasForeignKey(ctx context.Context, db *sql.DB, table, target string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+table+")")
	if err != nil {
		return fmt.Errorf("inspect foreign keys for %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, seq int
		var referenced, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &referenced, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return err
		}
		if referenced == target {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return fmt.Errorf("required foreign key %s -> %s is missing", table, target)
}

var requiredSchema = map[string][]string{
	"projects":          {"channel", "id", "canonical_path"},
	"tickets":           {"channel", "project_id", "id", "version", "runner_epoch", "workflow_id"},
	"phase_runs":        {"phase", "attempt", "expected_ticket_version"},
	"events":            {"ticket_version", "trigger", "from_state", "to_state"},
	"effects":           {"semantic_key", "claim_epoch", "observed_identity"},
	"approvals":         {"reviewed_head", "operator_uid", "invalidated"},
	"worktrees":         {"path", "branch_ref"},
	"provider_attempts": {"phase", "attempt", "provider"},
	"leases":            {"scope", "scope_key", "runner_epoch"},
	"plans":             {"ticket_id", "digest", "body"},
	"verifications":     {"ticket_id", "intent_digest", "proof_digest"},
}

func hasColumns(ctx context.Context, db *sql.DB, table string, required ...string) error {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return fmt.Errorf("inspect table %s: %w", table, err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		seen[name] = true
	}
	for _, column := range required {
		if !seen[column] {
			return fmt.Errorf("required schema column %s.%s is missing", table, column)
		}
	}
	return rows.Err()
}

// Backup creates an online SQLite snapshot without copying a live WAL file.
// The destination must not already exist; SQLite's VACUUM INTO preserves the
// source database while readers and short writes continue normally.
func (s *Store) Backup(ctx context.Context, destination string) error {
	if destination == "" || filepath.Clean(destination) == "." {
		return fmt.Errorf("backup destination is required")
	}
	_, err := s.db.ExecContext(ctx, "VACUUM INTO ?", destination)
	return normalizeBusy(ctx, err)
}
