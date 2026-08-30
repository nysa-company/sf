package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
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
	for _, required := range requiredForeignKeys {
		if err := hasForeignKey(ctx, s.db, required.table, required.target); err != nil {
			return err
		}
	}
	for _, index := range requiredIndexes {
		if err := hasIndex(ctx, s.db, index); err != nil {
			return err
		}
	}
	for _, trigger := range []string{"provider_attempt_results_immutable_update", "provider_attempt_results_immutable_delete", "plan_result_bindings_immutable_update", "plan_result_bindings_immutable_delete", "verification_result_bindings_immutable_update", "verification_result_bindings_immutable_delete", "candidate_result_bindings_immutable_update", "candidate_result_bindings_immutable_delete", "repository_command_results_immutable_update", "repository_command_results_immutable_delete", "verification_command_result_bindings_immutable_update", "verification_command_result_bindings_immutable_delete", "candidate_command_result_bindings_immutable_update", "candidate_command_result_bindings_immutable_delete", "publication_evidence_immutable_update", "publication_evidence_immutable_delete", "publication_evidence_rebinds_immutable_update", "publication_evidence_rebinds_immutable_delete", "publication_transition_evidence_immutable_update", "publication_transition_evidence_immutable_delete", "runner_recovery_ledger_immutable_update", "runner_recovery_ledger_immutable_delete"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?`, trigger).Scan(&count); err != nil || count != 1 {
			return fmt.Errorf("required trigger %s is missing", trigger)
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

type foreignKeyRequirement struct {
	table  string
	target string
}

var requiredForeignKeys = []foreignKeyRequirement{
	{table: "project_configurations", target: "projects"},
	{table: "tickets", target: "projects"},
	{table: "phase_runs", target: "tickets"},
	{table: "events", target: "tickets"},
	{table: "effects", target: "tickets"},
	{table: "approvals", target: "tickets"},
	{table: "worktrees", target: "tickets"},
	{table: "provider_attempts", target: "tickets"},
	{table: "provider_attempt_results", target: "provider_attempts"},
	{table: "provider_attempt_results", target: "tickets"},
	{table: "leases", target: "tickets"},
	{table: "plans", target: "tickets"},
	{table: "verifications", target: "tickets"},
	{table: "verification_revisions", target: "tickets"},
	{table: "plan_result_bindings", target: "tickets"},
	{table: "plan_result_bindings", target: "provider_attempt_results"},
	{table: "verification_result_bindings", target: "verification_revisions"},
	{table: "verification_result_bindings", target: "provider_attempt_results"},
	{table: "candidate_snapshots", target: "tickets"},
	{table: "candidate_result_bindings", target: "candidate_snapshots"},
	{table: "candidate_result_bindings", target: "provider_attempt_results"},
	{table: "invalidation_receipts", target: "candidate_snapshots"},
	{table: "ticket_counters", target: "tickets"},
	{table: "ticket_budget_uses", target: "tickets"},
	{table: "branch_allocations", target: "tickets"},
	{table: "provider_pair_selections", target: "provider_qualifications"},
	{table: "merge_intents", target: "tickets"},
	{table: "merge_intents", target: "effects"},
	{table: "git_mutation_intents", target: "tickets"},
	{table: "git_mutation_intents", target: "effects"},
	{table: "git_mutation_leases", target: "git_mutation_intents"},
	{table: "repository_command_intents", target: "tickets"},
	{table: "repository_command_intents", target: "effects"},
	{table: "repository_command_leases", target: "repository_command_intents"},
	{table: "repository_command_process_groups", target: "repository_command_leases"},
	{table: "repository_command_results", target: "effects"},
	{table: "repository_command_results", target: "tickets"},
	{table: "verification_command_result_bindings", target: "verification_revisions"},
	{table: "verification_command_result_bindings", target: "repository_command_results"},
	{table: "verification_command_result_bindings", target: "tickets"},
	{table: "candidate_command_result_bindings", target: "candidate_snapshots"},
	{table: "candidate_command_result_bindings", target: "repository_command_results"},
	{table: "candidate_command_result_bindings", target: "tickets"},
	{table: "publication_evidence", target: "tickets"},
	{table: "publication_evidence_rebinds", target: "publication_evidence"},
	{table: "runner_recovery_ledger", target: "tickets"},
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
	"schema_migrations":                    {"version", "applied_at", "checksum"},
	"daemon_instances":                     {"channel", "leader_epoch", "identity"},
	"projects":                             {"channel", "id", "canonical_path", "current_config_generation"},
	"project_configurations":               {"channel", "project_id", "generation", "digest", "snapshot_bytes", "created_at"},
	"tickets":                              {"channel", "project_id", "id", "version", "runner_epoch", "workflow_id", "title", "problem", "acceptance_json", "source_bytes", "priority", "created_at", "max_duration_ns", "max_cost_micro_usd", "config_generation", "config_digest", "config_snapshot_bytes"},
	"workflow_owners":                      {"channel", "project_id", "ticket_id", "workflow_id"},
	"phase_runs":                           {"phase", "attempt", "expected_ticket_version", "provider", "model", "family", "provider_version", "started_at", "outcome"},
	"events":                               {"ticket_version", "trigger", "from_state", "to_state"},
	"effects":                              {"semantic_key", "claim_epoch", "observed_identity"},
	"approvals":                            {"reviewed_head", "operator_uid", "invalidated"},
	"worktrees":                            {"path", "branch_ref"},
	"provider_attempts":                    {"phase", "attempt", "provider", "role", "state", "usage_units", "started_at", "finished_at", "qualification_id", "binding_digest", "provider_lease_key", "leader_epoch", "runner_epoch", "expected_ticket_version", "auth_digest", "auth_mode", "launch_state", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity", "worktree_path"},
	"provider_attempt_inputs":              {"provider_attempt_id", "request_digest", "canonical_input", "created_at"},
	"provider_attempt_results":             {"provider_attempt_id", "raw_artifact", "raw_sha256", "typed_artifact", "typed_sha256", "validation", "validation_sha256", "transcript_sha256", "request_digest", "leader_epoch", "runner_epoch", "expected_ticket_version", "repository_path", "worktree_path", "worktree_identity", "base_sha"},
	"leases":                               {"scope", "scope_key", "runner_epoch"},
	"plans":                                {"ticket_id", "digest", "body"},
	"verifications":                        {"ticket_id", "intent_digest", "proof_digest", "current_revision"},
	"verification_revisions":               {"revision", "intent_bytes", "proof_bytes", "owned_files_json", "checkpoint_id"},
	"plan_result_bindings":                 {"plan_digest", "binding_ticket_version", "leader_epoch", "runner_epoch", "provider_attempt_id", "provider_attempt"},
	"verification_result_bindings":         {"revision", "binding_ticket_version", "leader_epoch", "runner_epoch", "provider_attempt_id", "provider_attempt", "checkpoint_commit_oid", "checkpoint_parent_oid", "checkpoint_tree_oid"},
	"candidate_snapshots":                  {"generation", "base_sha", "head_sha", "tree_sha", "command_policy_digest", "builder_evidence_digest"},
	"candidate_result_bindings":            {"generation", "binding_ticket_version", "leader_epoch", "runner_epoch", "provider_attempt_id", "provider_attempt", "commit_parent_oid"},
	"invalidation_receipts":                {"generation", "kind", "reason"},
	"ticket_counters":                      {"kind", "used", "limit_count"},
	"ticket_budget_uses":                   {"kind", "request_id", "ticket_version"},
	"branch_allocations":                   {"authority_key", "channel", "project_id", "ticket_id", "branch_ref", "created_at"},
	"provider_qualifications":              {"id", "channel", "run_id", "provider", "model", "family", "provider_version", "binary_digest", "policy_digest", "fixture_digest", "profile", "failed_probes_json", "reason_code", "created_at", "auth_digest", "auth_mode", "probe_digest", "attested_leader_epoch", "attestation_signature"},
	"provider_pair_selections":             {"channel", "builder_qualification_id", "reviewer_qualification_id", "selected_at"},
	"merge_intents":                        {"semantic_key", "original_base_oid", "head_oid", "base_ref", "protection_rule_id", "protection_kind", "protection_checks_digest", "strict_status_checks", "admin_enforced", "active_ruleset_count"},
	"external_mutation_quarantine":         {"singleton", "reason", "observed_at"},
	"git_mutation_intents":                 {"semantic_key", "request_digest", "ticket_version", "leader_epoch", "runner_epoch", "claim_epoch", "repository_path", "worktree_path", "branch_ref", "operation", "base_ref", "expected_base_oid", "expected_head_oid", "prepared_commit_oid", "prepared_tree_oid", "prior_remote_observed", "prior_remote_oid"},
	"git_mutation_leases":                  {"repository_path", "semantic_key", "nonce", "state", "launch_state", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity", "prepared_commit_oid", "prepared_tree_oid", "prior_remote_observed", "prior_remote_oid"},
	"repository_command_intents":           {"semantic_key", "repository_path", "worktree_path", "worktree_identity", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest"},
	"repository_command_leases":            {"repository_path", "semantic_key", "nonce", "state", "launch_state", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity"},
	"repository_command_process_groups":    {"repository_path", "semantic_key", "nonce", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity"},
	"repository_command_results":           {"semantic_key", "claim_epoch", "channel", "project_id", "ticket_id", "request_digest", "ticket_version", "leader_epoch", "runner_epoch", "repository_path", "worktree_path", "worktree_identity", "branch_ref", "base_ref", "base_sha", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest", "exit_code", "stdout", "stderr", "output_last_message", "stdout_truncated", "stderr_truncated", "output_last_message_truncated", "duration_ns", "observed_at", "stdout_digest", "stderr_digest", "output_last_message_digest", "result_digest", "created_at"},
	"verification_command_result_bindings": {"revision", "binding_ticket_version", "leader_epoch", "runner_epoch", "semantic_key", "claim_epoch", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest", "expected_outcome"},
	"candidate_command_result_bindings":    {"generation", "binding_ticket_version", "leader_epoch", "runner_epoch", "semantic_key", "claim_epoch", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest"},
	"publication_evidence":                 {"channel", "project_id", "ticket_id", "ticket_version", "leader_epoch", "runner_epoch", "candidate_generation", "candidate_ticket_version", "candidate_leader_epoch", "candidate_runner_epoch", "candidate_base_sha", "candidate_head_sha", "candidate_tree_sha", "candidate_source_digest", "candidate_verification_intent_digest", "candidate_proof_digest", "candidate_command_policy_digest", "candidate_builder_evidence_digest", "candidate_builder_attempt_id", "candidate_builder_attempt", "candidate_commit_parent_oid", "candidate_command_semantic_key", "candidate_command_claim_epoch", "candidate_command_ticket_version", "candidate_command_leader_epoch", "candidate_command_runner_epoch", "candidate_command_digest", "candidate_command_spec_digest", "candidate_command_policy_claim_digest", "candidate_command_executable_path", "candidate_command_executable_digest", "config_generation", "config_digest", "config_snapshot_digest", "worktree_path", "worktree_branch_ref", "worktree_state", "worktree_ticket_version", "worktree_leader_epoch", "worktree_runner_epoch", "worktree_identity_json", "worktree_identity_digest", "worktree_base_sha", "remote_branch_ref", "remote_branch_oid", "remote_base_oid", "push_effect_semantic_key", "push_effect_kind", "push_effect_request_digest", "push_effect_claim_epoch", "push_effect_observed_identity", "github_host", "github_owner", "github_name", "github_pr_number", "github_head_owner", "github_head_repository", "github_head_ref", "github_head_oid", "github_base_ref", "github_base_oid", "github_state", "github_draft", "github_factory_owned", "github_observed_at", "pr_effect_semantic_key", "pr_effect_kind", "pr_effect_request_digest", "pr_effect_claim_epoch", "pr_effect_observed_identity", "build_transition_created_at", "witness_digest", "created_at"},
	"publication_evidence_rebinds":         {"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "prior_witness_digest", "prior_ticket_version", "prior_leader_epoch", "prior_runner_epoch", "ticket_version", "leader_epoch", "runner_epoch", "rebind_digest", "created_at"},
	"publication_transition_evidence":      {"channel", "project_id", "ticket_id", "witness_digest", "witness_created_at", "ticket_version", "event_created_at"},
	"runner_recovery_ledger":               {"channel", "project_id", "ticket_id", "prior_ticket_version", "prior_runner_epoch", "prior_leader_epoch", "ticket_version", "runner_epoch", "leader_epoch", "recovery_digest", "created_at"},
}

type indexRequirement struct {
	table   string
	name    string
	columns []string
	partial bool
	// nonUnique is explicit because all pre-v21 requirements protect durable
	// uniqueness; the Git recovery index is a named lookup accelerator.
	nonUnique bool
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
	{table: "provider_attempt_results", name: "provider_attempt_results_fence", columns: []string{"channel", "project_id", "ticket_id", "phase", "attempt", "leader_epoch", "runner_epoch", "expected_ticket_version"}, nonUnique: true},
	{table: "verification_revisions", columns: []string{"channel", "project_id", "ticket_id", "intent_digest", "proof_digest", "checkpoint_id"}},
	{table: "candidate_snapshots", columns: []string{"channel", "project_id", "ticket_id", "generation"}},
	{table: "invalidation_receipts", columns: []string{"channel", "project_id", "ticket_id", "generation", "kind"}},
	{table: "branch_allocations", columns: []string{"channel", "project_id", "ticket_id"}},
	{table: "branch_allocations", columns: []string{"channel", "branch_ref"}},
	{table: "git_mutation_leases", name: "git_mutation_lease_recovery", columns: []string{"channel", "state", "launch_state"}, nonUnique: true},
	{table: "repository_command_leases", name: "repository_command_lease_recovery", columns: []string{"channel", "state", "launch_state"}, nonUnique: true},
	{table: "repository_command_process_groups", name: "repository_command_process_group_recovery", columns: []string{"repository_path", "semantic_key", "nonce"}, nonUnique: true},
	{table: "repository_command_results", name: "repository_command_results_ticket", columns: []string{"channel", "project_id", "ticket_id", "worktree_path", "base_sha", "created_at"}, nonUnique: true},
	{table: "publication_evidence", name: "publication_evidence_witness", columns: []string{"witness_digest"}},
	{table: "publication_evidence", name: "publication_evidence_current", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha"}, nonUnique: true},
	{table: "publication_evidence_rebinds", name: "publication_evidence_rebind_digest", columns: []string{"rebind_digest"}},
	{table: "runner_recovery_ledger", name: "runner_recovery_ledger_digest", columns: []string{"recovery_digest"}},
	{table: "runner_recovery_ledger", name: "runner_recovery_ledger_ticket", columns: []string{"channel", "project_id", "ticket_id", "ticket_version"}, nonUnique: true},
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
		expectedUnique := 1
		if required.nonUnique {
			expectedUnique = 0
		}
		if unique != expectedUnique || partial != boolInt(required.partial) || (required.name != "" && name != required.name) {
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
	kind := "unique "
	if required.nonUnique {
		kind = ""
	}
	return fmt.Errorf("required %s%sindex on %s(%v) is missing", map[bool]string{true: "partial ", false: ""}[required.partial], kind, required.table, required.columns)
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
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || destination == string(filepath.Separator) {
		return fmt.Errorf("backup destination must be absolute and clean")
	}
	if err := validateBackupDirectory(filepath.Dir(destination)); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("backup destination already exists")
		}
		return fmt.Errorf("inspect backup destination: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, "VACUUM INTO ?", destination); err != nil {
		_ = os.Remove(destination)
		return normalizeBusy(ctx, err)
	}
	return validateBackupFile(destination)
}
