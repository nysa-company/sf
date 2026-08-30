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
	for _, index := range requiredIndexes {
		if err := hasIndex(ctx, s.db, index); err != nil {
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
	"project_configurations":   "projects",
	"tickets":                  "projects",
	"phase_runs":               "tickets",
	"events":                   "tickets",
	"effects":                  "tickets",
	"approvals":                "tickets",
	"worktrees":                "tickets",
	"provider_attempts":        "tickets",
	"leases":                   "tickets",
	"plans":                    "tickets",
	"verifications":            "tickets",
	"verification_revisions":   "tickets",
	"candidate_snapshots":      "tickets",
	"invalidation_receipts":    "candidate_snapshots",
	"ticket_counters":          "tickets",
	"ticket_budget_uses":       "tickets",
	"branch_allocations":       "tickets",
	"provider_pair_selections": "provider_qualifications",
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
	"schema_migrations":        {"version", "applied_at", "checksum"},
	"daemon_instances":         {"channel", "leader_epoch", "identity"},
	"projects":                 {"channel", "id", "canonical_path", "current_config_generation"},
	"project_configurations":   {"channel", "project_id", "generation", "digest", "snapshot_bytes", "created_at"},
	"tickets":                  {"channel", "project_id", "id", "version", "runner_epoch", "workflow_id", "title", "problem", "acceptance_json", "source_bytes", "priority", "created_at", "max_duration_ns", "max_cost_micro_usd", "config_generation", "config_digest", "config_snapshot_bytes"},
	"workflow_owners":          {"channel", "project_id", "ticket_id", "workflow_id"},
	"phase_runs":               {"phase", "attempt", "expected_ticket_version"},
	"events":                   {"ticket_version", "trigger", "from_state", "to_state"},
	"effects":                  {"semantic_key", "claim_epoch", "observed_identity"},
	"approvals":                {"reviewed_head", "operator_uid", "invalidated"},
	"worktrees":                {"path", "branch_ref"},
	"provider_attempts":        {"phase", "attempt", "provider", "role", "state", "usage_units", "started_at", "finished_at", "qualification_id", "binding_digest", "provider_lease_key"},
	"leases":                   {"scope", "scope_key", "runner_epoch"},
	"plans":                    {"ticket_id", "digest", "body"},
	"verifications":            {"ticket_id", "intent_digest", "proof_digest", "current_revision"},
	"verification_revisions":   {"revision", "intent_bytes", "proof_bytes", "owned_files_json", "checkpoint_id"},
	"candidate_snapshots":      {"generation", "base_sha", "head_sha", "tree_sha", "command_policy_digest"},
	"invalidation_receipts":    {"generation", "kind", "reason"},
	"ticket_counters":          {"kind", "used", "limit_count"},
	"ticket_budget_uses":       {"kind", "request_id", "ticket_version"},
	"branch_allocations":       {"authority_key", "channel", "project_id", "ticket_id", "branch_ref", "created_at"},
	"provider_qualifications":  {"id", "channel", "run_id", "provider", "model", "family", "provider_version", "binary_digest", "policy_digest", "fixture_digest", "profile", "failed_probes_json", "reason_code", "created_at"},
	"provider_pair_selections": {"channel", "builder_qualification_id", "reviewer_qualification_id", "selected_at"},
}

type indexRequirement struct {
	table   string
	name    string
	columns []string
	partial bool
}

var requiredIndexes = []indexRequirement{
	{table: "projects", columns: []string{"channel", "canonical_path"}},
	{table: "project_configurations", columns: []string{"channel", "project_id", "digest"}},
	{table: "tickets", name: "active_ticket_source_digest", columns: []string{"channel", "project_id", "source_digest"}, partial: true},
	{table: "tickets", name: "ticket_workflow_id", columns: []string{"channel", "workflow_id"}, partial: true},
	{table: "tickets", name: "ticket_channel_id", columns: []string{"channel", "id"}},
	{table: "workflow_owners", columns: []string{"channel", "workflow_id"}},
	{table: "phase_runs", name: "one_active_phase_per_ticket", columns: []string{"channel", "project_id", "ticket_id"}, partial: true},
	{table: "effects", name: "active_effect_claim", columns: []string{"channel", "project_id", "ticket_id", "effect_kind"}, partial: true},
	{table: "approvals", name: "current_approval_per_head", columns: []string{"channel", "project_id", "ticket_id", "reviewed_head", "operator_uid"}, partial: true},
	{table: "worktrees", columns: []string{"channel", "path"}},
	{table: "worktrees", columns: []string{"channel", "branch_ref"}},
	{table: "provider_attempts", columns: []string{"channel", "project_id", "ticket_id", "phase", "attempt", "provider"}},
	{table: "provider_attempts", name: "one_active_provider_attempt", columns: []string{"channel", "project_id", "ticket_id"}, partial: true},
	{table: "verification_revisions", columns: []string{"channel", "project_id", "ticket_id", "intent_digest", "proof_digest", "checkpoint_id"}},
	{table: "candidate_snapshots", columns: []string{"channel", "project_id", "ticket_id", "generation"}},
	{table: "invalidation_receipts", columns: []string{"channel", "project_id", "ticket_id", "generation", "kind"}},
	{table: "branch_allocations", columns: []string{"channel", "project_id", "ticket_id"}},
	{table: "branch_allocations", columns: []string{"channel", "branch_ref"}},
}

func hasIndex(ctx context.Context, db *sql.DB, required indexRequirement) error {
	rows, err := db.QueryContext(ctx, "PRAGMA index_list("+required.table+")")
	if err != nil {
		return fmt.Errorf("inspect indexes for %s: %w", required.table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			return err
		}
		if unique != 1 || partial != boolInt(required.partial) || (required.name != "" && name != required.name) {
			continue
		}
		columns, err := indexColumns(ctx, db, name)
		if err != nil {
			return err
		}
		if sameColumns(columns, required.columns) {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return fmt.Errorf("required %sunique index on %s(%v) is missing", map[bool]string{true: "partial ", false: ""}[required.partial], required.table, required.columns)
}

func indexColumns(ctx context.Context, db *sql.DB, index string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA index_info("+index+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var seqno, cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

func sameColumns(actual, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index := range actual {
		if actual[index] != expected[index] {
			return false
		}
	}
	return true
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
