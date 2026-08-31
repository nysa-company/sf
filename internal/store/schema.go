package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	for _, required := range requiredCompositeForeignKeys {
		if err := hasExactForeignKey(ctx, s.db, required); err != nil {
			return err
		}
	}
	for _, index := range requiredIndexes {
		if err := hasIndex(ctx, s.db, index); err != nil {
			return err
		}
	}
	for _, trigger := range []string{"provider_attempt_results_immutable_update", "provider_attempt_results_immutable_delete", "plan_result_bindings_immutable_update", "plan_result_bindings_immutable_delete", "verification_result_bindings_immutable_update", "verification_result_bindings_immutable_delete", "candidate_result_bindings_immutable_update", "candidate_result_bindings_immutable_delete", "repository_command_results_immutable_update", "repository_command_results_immutable_delete", "verification_command_result_bindings_immutable_update", "verification_command_result_bindings_immutable_delete", "candidate_command_result_bindings_immutable_update", "candidate_command_result_bindings_immutable_delete", "publication_evidence_immutable_update", "publication_evidence_immutable_delete", "publication_evidence_rebinds_immutable_update", "publication_evidence_rebinds_immutable_delete", "publication_transition_evidence_immutable_update", "publication_transition_evidence_immutable_delete", "runner_recovery_ledger_immutable_update", "runner_recovery_ledger_immutable_delete", "runner_start_authorities_immutable_update", "runner_start_authorities_immutable_delete", "ci_observations_immutable_update", "ci_observations_immutable_delete", "ci_observation_checks_immutable_update", "ci_observation_checks_immutable_delete", "ci_transition_evidence_immutable_update", "ci_transition_evidence_immutable_delete", "ci_transition_evidence_requires_checks", "ci_required_check_policies_immutable_update", "ci_required_check_policies_immutable_delete", "candidate_repair_bindings_immutable_update", "candidate_repair_bindings_immutable_delete", "candidate_repair_completions_immutable_update", "candidate_repair_completions_immutable_delete", "final_review_repair_boundaries_immutable_update", "final_review_repair_boundaries_immutable_delete"} {
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

type foreignKeyColumn struct{ from, to string }

type compositeForeignKeyRequirement struct {
	table, target string
	columns       []foreignKeyColumn
}

func compositeForeignKey(table, target string, columns ...foreignKeyColumn) compositeForeignKeyRequirement {
	return compositeForeignKeyRequirement{table: table, target: target, columns: columns}
}

// v41 authority rows use exact composite keys. Parent-table presence alone is
// insufficient: accepting a subset or differently ordered mapping would let a
// valid value from another candidate, ticket, or fence satisfy the FK.
var requiredCompositeForeignKeys = []compositeForeignKeyRequirement{
	compositeForeignKey("ci_required_check_policies", "candidate_snapshots",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"candidate_generation", "generation"}, foreignKeyColumn{"candidate_head_sha", "head_sha"}, foreignKeyColumn{"candidate_tree_sha", "tree_sha"}),
	compositeForeignKey("ci_required_check_policies", "publication_evidence",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"candidate_generation", "candidate_generation"}, foreignKeyColumn{"candidate_head_sha", "candidate_head_sha"}, foreignKeyColumn{"candidate_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"publication_witness_digest", "witness_digest"}),
	compositeForeignKey("ci_observations", "candidate_snapshots",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"candidate_generation", "generation"}, foreignKeyColumn{"candidate_head_sha", "head_sha"}, foreignKeyColumn{"candidate_tree_sha", "tree_sha"}),
	compositeForeignKey("ci_observations", "publication_evidence",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"candidate_generation", "candidate_generation"}, foreignKeyColumn{"candidate_head_sha", "candidate_head_sha"}, foreignKeyColumn{"candidate_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"publication_witness_digest", "witness_digest"}, foreignKeyColumn{"pr_host", "github_host"}, foreignKeyColumn{"pr_owner", "github_owner"}, foreignKeyColumn{"pr_repo", "github_name"}, foreignKeyColumn{"pr_number", "github_pr_number"}, foreignKeyColumn{"pr_head_owner", "github_head_owner"}, foreignKeyColumn{"pr_head_repo", "github_head_repository"}, foreignKeyColumn{"pr_head_ref", "github_head_ref"}, foreignKeyColumn{"pr_head_oid", "github_head_oid"}, foreignKeyColumn{"pr_base_ref", "github_base_ref"}, foreignKeyColumn{"pr_base_oid", "github_base_oid"}, foreignKeyColumn{"pr_open", "github_open"}, foreignKeyColumn{"pr_draft", "github_draft"}, foreignKeyColumn{"pr_factory_owned", "github_factory_owned"}),
	compositeForeignKey("ci_observation_checks", "ci_observations",
		foreignKeyColumn{"observation_id", "observation_id"}, foreignKeyColumn{"observation_digest", "observation_digest"}),
	compositeForeignKey("ci_transition_evidence", "events",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"ticket_version", "ticket_version"}, foreignKeyColumn{"event_id", "id"}, foreignKeyColumn{"event_created_at", "created_at"}, foreignKeyColumn{"prior_state", "from_state"}, foreignKeyColumn{"resulting_state", "to_state"}, foreignKeyColumn{"resulting_trigger", "trigger"}),
	compositeForeignKey("ci_transition_evidence", "ci_observations",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"candidate_generation", "candidate_generation"}, foreignKeyColumn{"candidate_head_sha", "candidate_head_sha"}, foreignKeyColumn{"candidate_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"observation_classification", "classification"}, foreignKeyColumn{"observation_digest", "observation_digest"}, foreignKeyColumn{"observation_ticket_version", "observed_ticket_version"}, foreignKeyColumn{"observation_leader_epoch", "observed_leader_epoch"}, foreignKeyColumn{"observation_runner_epoch", "observed_runner_epoch"}),
	compositeForeignKey("ci_transition_evidence", "publication_evidence",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"candidate_generation", "candidate_generation"}, foreignKeyColumn{"candidate_head_sha", "candidate_head_sha"}, foreignKeyColumn{"candidate_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"prior_publication_witness_digest", "witness_digest"}),
	compositeForeignKey("candidate_repair_bindings", "candidate_snapshots",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"predecessor_generation", "generation"}, foreignKeyColumn{"predecessor_head_sha", "head_sha"}, foreignKeyColumn{"predecessor_tree_sha", "tree_sha"}),
	compositeForeignKey("candidate_repair_bindings", "ci_observations",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"predecessor_generation", "candidate_generation"}, foreignKeyColumn{"predecessor_head_sha", "candidate_head_sha"}, foreignKeyColumn{"predecessor_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"red_observation_classification", "classification"}, foreignKeyColumn{"red_observation_digest", "observation_digest"}),
	compositeForeignKey("candidate_repair_bindings", "publication_evidence",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"predecessor_generation", "candidate_generation"}, foreignKeyColumn{"predecessor_head_sha", "candidate_head_sha"}, foreignKeyColumn{"predecessor_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"predecessor_publication_witness_digest", "witness_digest"}, foreignKeyColumn{"pr_host", "github_host"}, foreignKeyColumn{"pr_owner", "github_owner"}, foreignKeyColumn{"pr_repo", "github_name"}, foreignKeyColumn{"pr_number", "github_pr_number"}, foreignKeyColumn{"branch_ref", "remote_branch_ref"}, foreignKeyColumn{"remote_head_oid", "remote_branch_oid"}, foreignKeyColumn{"base_ref", "github_base_ref"}, foreignKeyColumn{"remote_base_oid", "remote_base_oid"}),
	compositeForeignKey("candidate_repair_bindings", "ci_transition_evidence",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"predecessor_generation", "candidate_generation"}, foreignKeyColumn{"predecessor_head_sha", "candidate_head_sha"}, foreignKeyColumn{"predecessor_tree_sha", "candidate_tree_sha"}, foreignKeyColumn{"red_observation_classification", "observation_classification"}, foreignKeyColumn{"red_observation_digest", "observation_digest"}, foreignKeyColumn{"predecessor_publication_witness_digest", "prior_publication_witness_digest"}, foreignKeyColumn{"consumed_ticket_version", "observation_ticket_version"}, foreignKeyColumn{"consumed_leader_epoch", "observation_leader_epoch"}, foreignKeyColumn{"consumed_runner_epoch", "observation_runner_epoch"}, foreignKeyColumn{"red_transition_ticket_version", "ticket_version"}, foreignKeyColumn{"red_transition_digest", "transition_digest"}),
	compositeForeignKey("candidate_repair_bindings", "ticket_budget_uses",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"correction_budget_kind", "kind"}, foreignKeyColumn{"correction_budget_request_id", "request_id"}, foreignKeyColumn{"consumed_ticket_version", "ticket_version"}, foreignKeyColumn{"consumed_leader_epoch", "leader_epoch"}, foreignKeyColumn{"consumed_runner_epoch", "runner_epoch"}),
	compositeForeignKey("final_review_repair_boundaries", "ticket_budget_uses",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"correction_budget_kind", "kind"}, foreignKeyColumn{"correction_budget_request_id", "request_id"}, foreignKeyColumn{"consumed_ticket_version", "ticket_version"}, foreignKeyColumn{"consumed_leader_epoch", "leader_epoch"}, foreignKeyColumn{"consumed_runner_epoch", "runner_epoch"}),
	compositeForeignKey("candidate_repair_completions", "candidate_repair_bindings",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"target_generation", "target_generation"}),
	compositeForeignKey("candidate_repair_completions", "provider_attempts",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"builder_result_phase", "phase"}, foreignKeyColumn{"builder_result_role", "role"}, foreignKeyColumn{"builder_result_attempt", "attempt"}, foreignKeyColumn{"builder_result_attempt_id", "id"}),
	compositeForeignKey("candidate_repair_completions", "provider_attempt_results",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"builder_result_phase", "phase"}, foreignKeyColumn{"builder_result_role", "role"}, foreignKeyColumn{"builder_result_attempt", "attempt"}, foreignKeyColumn{"builder_result_attempt_id", "provider_attempt_id"}),
	compositeForeignKey("candidate_repair_completions", "candidate_result_bindings",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"target_generation", "generation"}, foreignKeyColumn{"builder_binding_ticket_version", "binding_ticket_version"}, foreignKeyColumn{"builder_binding_leader_epoch", "leader_epoch"}, foreignKeyColumn{"builder_binding_runner_epoch", "runner_epoch"}, foreignKeyColumn{"builder_result_attempt_id", "provider_attempt_id"}, foreignKeyColumn{"builder_result_attempt", "provider_attempt"}),
	compositeForeignKey("candidate_repair_completions", "candidate_snapshots",
		foreignKeyColumn{"channel", "channel"}, foreignKeyColumn{"project_id", "project_id"}, foreignKeyColumn{"ticket_id", "ticket_id"}, foreignKeyColumn{"target_generation", "generation"}, foreignKeyColumn{"final_candidate_head_sha", "head_sha"}, foreignKeyColumn{"final_candidate_tree_sha", "tree_sha"}),
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
	{table: "runner_start_authorities", target: "tickets"},
	{table: "runtime_ticket_controls", target: "tickets"},
	{table: "ci_observations", target: "tickets"},
	{table: "ci_observations", target: "candidate_snapshots"},
	{table: "ci_observations", target: "publication_evidence"},
	{table: "ci_required_check_policies", target: "tickets"},
	{table: "ci_required_check_policies", target: "candidate_snapshots"},
	{table: "ci_required_check_policies", target: "publication_evidence"},
	{table: "ci_observation_checks", target: "ci_observations"},
	{table: "ci_transition_evidence", target: "tickets"},
	{table: "ci_transition_evidence", target: "events"},
	{table: "ci_transition_evidence", target: "ci_observations"},
	{table: "ci_transition_evidence", target: "publication_evidence"},
	{table: "candidate_repair_bindings", target: "tickets"},
	{table: "candidate_repair_bindings", target: "candidate_snapshots"},
	{table: "candidate_repair_bindings", target: "publication_evidence"},
	{table: "candidate_repair_bindings", target: "ci_observations"},
	{table: "candidate_repair_completions", target: "candidate_repair_bindings"},
	{table: "candidate_repair_completions", target: "candidate_snapshots"},
	{table: "candidate_repair_completions", target: "provider_attempt_results"},
	{table: "candidate_repair_completions", target: "provider_attempts"},
	{table: "candidate_repair_completions", target: "candidate_result_bindings"},
	{table: "final_review_repair_boundaries", target: "tickets"},
	{table: "final_review_repair_boundaries", target: "ticket_budget_uses"},
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

func hasExactForeignKey(ctx context.Context, db *sql.DB, required compositeForeignKeyRequirement) error {
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_list("+required.table+")")
	if err != nil {
		return fmt.Errorf("inspect foreign keys for %s: %w", required.table, err)
	}
	defer rows.Close()
	type group struct {
		target  string
		columns []foreignKeyColumn
	}
	groups := map[int]*group{}
	for rows.Next() {
		var id, seq int
		var target, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &target, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return err
		}
		if groups[id] == nil {
			groups[id] = &group{target: target}
		}
		groups[id].columns = append(groups[id].columns, foreignKeyColumn{from: from, to: to})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	want := append([]foreignKeyColumn(nil), required.columns...)
	sort.Slice(want, func(i, j int) bool { return want[i].from < want[j].from })
	for _, candidate := range groups {
		if candidate.target != required.target {
			continue
		}
		actual := append([]foreignKeyColumn(nil), candidate.columns...)
		sort.Slice(actual, func(i, j int) bool { return actual[i].from < actual[j].from })
		if sameForeignKeyColumns(actual, want) {
			return nil
		}
	}
	return fmt.Errorf("required exact foreign key %s -> %s is missing or mismatched", required.table, required.target)
}

func sameForeignKeyColumns(actual, expected []foreignKeyColumn) bool {
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
	"ticket_budget_uses":                   {"kind", "request_id", "ticket_version", "leader_epoch", "runner_epoch"},
	"branch_allocations":                   {"authority_key", "channel", "project_id", "ticket_id", "branch_ref", "created_at"},
	"provider_qualifications":              {"id", "channel", "run_id", "provider", "model", "family", "provider_version", "binary_digest", "policy_digest", "fixture_digest", "profile", "failed_probes_json", "reason_code", "created_at", "auth_digest", "auth_mode", "probe_digest", "attested_leader_epoch", "attestation_signature"},
	"provider_pair_selections":             {"channel", "builder_qualification_id", "reviewer_qualification_id", "selected_at"},
	"merge_intents":                        {"semantic_key", "original_base_oid", "head_owner", "head_repository", "head_ref", "head_oid", "base_ref", "protection_rule_id", "protection_kind", "protection_checks_digest", "strict_status_checks", "admin_enforced", "active_ruleset_count"},
	"external_mutation_quarantine":         {"singleton", "reason", "observed_at"},
	"git_mutation_intents":                 {"semantic_key", "request_digest", "ticket_version", "leader_epoch", "runner_epoch", "claim_epoch", "repository_path", "worktree_path", "branch_ref", "operation", "base_ref", "expected_base_oid", "expected_head_oid", "prepared_commit_oid", "prepared_tree_oid", "prior_remote_observed", "prior_remote_oid"},
	"git_mutation_leases":                  {"repository_path", "semantic_key", "nonce", "state", "launch_state", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity", "prepared_commit_oid", "prepared_tree_oid", "prior_remote_observed", "prior_remote_oid"},
	"repository_command_intents":           {"semantic_key", "repository_path", "worktree_path", "worktree_identity", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest"},
	"repository_command_leases":            {"repository_path", "semantic_key", "nonce", "state", "launch_state", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity"},
	"repository_command_process_groups":    {"repository_path", "semantic_key", "nonce", "process_pid", "process_pgid", "process_boot_identity", "process_start_identity"},
	"repository_command_results":           {"semantic_key", "claim_epoch", "channel", "project_id", "ticket_id", "request_digest", "ticket_version", "leader_epoch", "runner_epoch", "repository_path", "worktree_path", "worktree_identity", "branch_ref", "base_ref", "base_sha", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest", "exit_code", "stdout", "stderr", "output_last_message", "stdout_truncated", "stderr_truncated", "output_last_message_truncated", "duration_ns", "observed_at", "stdout_digest", "stderr_digest", "output_last_message_digest", "result_digest", "created_at"},
	"verification_command_result_bindings": {"revision", "binding_ticket_version", "leader_epoch", "runner_epoch", "semantic_key", "claim_epoch", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest", "expected_outcome"},
	"candidate_command_result_bindings":    {"generation", "binding_ticket_version", "leader_epoch", "runner_epoch", "semantic_key", "claim_epoch", "command_digest", "spec_digest", "policy_digest", "executable_path", "executable_digest"},
	"publication_evidence":                 {"channel", "project_id", "ticket_id", "ticket_version", "leader_epoch", "runner_epoch", "candidate_generation", "candidate_ticket_version", "candidate_leader_epoch", "candidate_runner_epoch", "candidate_base_sha", "candidate_head_sha", "candidate_tree_sha", "candidate_source_digest", "candidate_verification_intent_digest", "candidate_proof_digest", "candidate_command_policy_digest", "candidate_builder_evidence_digest", "candidate_builder_attempt_id", "candidate_builder_attempt", "candidate_commit_parent_oid", "candidate_command_semantic_key", "candidate_command_claim_epoch", "candidate_command_ticket_version", "candidate_command_leader_epoch", "candidate_command_runner_epoch", "candidate_command_digest", "candidate_command_spec_digest", "candidate_command_policy_claim_digest", "candidate_command_executable_path", "candidate_command_executable_digest", "config_generation", "config_digest", "config_snapshot_digest", "worktree_path", "worktree_branch_ref", "worktree_state", "worktree_ticket_version", "worktree_leader_epoch", "worktree_runner_epoch", "worktree_identity_json", "worktree_identity_digest", "worktree_base_sha", "remote_branch_ref", "remote_branch_oid", "remote_base_oid", "push_effect_semantic_key", "push_effect_kind", "push_effect_request_digest", "push_effect_claim_epoch", "push_effect_observed_identity", "github_host", "github_owner", "github_name", "github_pr_number", "github_head_owner", "github_head_repository", "github_head_ref", "github_head_oid", "github_base_ref", "github_base_oid", "github_state", "github_open", "github_draft", "github_factory_owned", "github_observed_at", "pr_effect_semantic_key", "pr_effect_kind", "pr_effect_request_digest", "pr_effect_claim_epoch", "pr_effect_observed_identity", "build_transition_created_at", "witness_digest", "created_at"},
	"publication_evidence_rebinds":         {"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "prior_witness_digest", "prior_ticket_version", "prior_leader_epoch", "prior_runner_epoch", "ticket_version", "leader_epoch", "runner_epoch", "rebind_digest", "created_at"},
	"publication_transition_evidence":      {"channel", "project_id", "ticket_id", "witness_digest", "witness_created_at", "ticket_version", "event_created_at"},
	"runner_recovery_ledger":               {"channel", "project_id", "ticket_id", "prior_ticket_version", "prior_runner_epoch", "prior_leader_epoch", "ticket_version", "runner_epoch", "leader_epoch", "recovery_digest", "created_at"},
	"runtime_ticket_controls":              {"channel", "project_id", "ticket_id", "state", "generation", "stop_version", "stop_leader_epoch", "stop_runner_epoch", "authority_version", "authority_leader_epoch", "authority_runner_epoch", "updated_at"},
	"ci_observations":                      {"observation_id", "channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "publication_witness_digest", "policy_witness_digest", "pr_host", "pr_owner", "pr_repo", "pr_number", "pr_head_owner", "pr_head_repo", "pr_head_ref", "pr_head_oid", "pr_base_ref", "pr_base_oid", "pr_factory_owned", "pr_open", "pr_draft", "observed_ticket_version", "observed_leader_epoch", "observed_runner_epoch", "observed_at", "required_set_digest", "required_check_count", "classification", "diagnostic_digest", "diagnostic_text", "diagnostic_json", "observation_digest"},
	"ci_required_check_policies":           {"policy_id", "channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "publication_witness_digest", "protected_branch_ref", "protected_branch_oid", "policy_source_digest", "authenticated_principal", "policy_witness_digest", "required_set_digest", "required_check_count", "required_checks_json", "created_at"},
	"ci_observation_checks":                {"observation_id", "observation_digest", "canonical_name", "external_id", "normalized_state", "failing_diagnostic_digest", "failing_diagnostic_text"},
	"ci_transition_evidence":               {"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "ticket_version", "event_id", "event_created_at", "observation_classification", "observation_digest", "observation_ticket_version", "observation_leader_epoch", "observation_runner_epoch", "prior_publication_witness_digest", "prior_state", "resulting_state", "resulting_trigger", "transition_digest", "created_at"},
	"candidate_repair_bindings":            {"channel", "project_id", "ticket_id", "target_generation", "predecessor_generation", "predecessor_head_sha", "predecessor_tree_sha", "predecessor_publication_witness_digest", "pr_host", "pr_owner", "pr_repo", "pr_number", "branch_ref", "remote_head_oid", "base_ref", "remote_base_oid", "red_observation_digest", "red_observation_classification", "red_transition_ticket_version", "red_transition_digest", "correction_budget_kind", "correction_budget_request_id", "consumed_ticket_version", "consumed_leader_epoch", "consumed_runner_epoch", "repair_context_digest", "created_at"},
	"candidate_repair_completions":         {"channel", "project_id", "ticket_id", "target_generation", "builder_result_attempt_id", "builder_result_attempt", "builder_result_phase", "builder_result_role", "builder_binding_ticket_version", "builder_binding_leader_epoch", "builder_binding_runner_epoch", "final_candidate_head_sha", "final_candidate_tree_sha", "completion_digest", "completed_at"},
	"final_review_repair_boundaries":       {"channel", "project_id", "ticket_id", "target_state", "transition_ticket_version", "reviewer_attempt_id", "reviewer_attempt", "reviewer_typed_sha256", "prior_verification_revision", "amendment_reason", "requester", "correction_budget_kind", "correction_budget_request_id", "consumed_ticket_version", "consumed_leader_epoch", "consumed_runner_epoch", "created_at"},
	"runner_start_authorities":             {"channel", "project_id", "ticket_id", "start_ticket_version", "runner_epoch", "leader_epoch", "workflow_id", "workflow_digest", "created_at", "authority_digest"},
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
	{table: "runtime_ticket_controls", name: "runtime_ticket_controls_state", columns: []string{"channel", "state"}, nonUnique: true},
	{table: "candidate_snapshots", name: "candidate_snapshot_generation_head", columns: []string{"channel", "project_id", "ticket_id", "generation", "head_sha"}},
	{table: "candidate_snapshots", name: "candidate_snapshot_generation_head_tree", columns: []string{"channel", "project_id", "ticket_id", "generation", "head_sha", "tree_sha"}},
	{table: "publication_evidence", name: "publication_evidence_candidate_witness", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "witness_digest"}},
	{table: "publication_evidence", name: "publication_evidence_candidate_witness_tree", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "witness_digest"}},
	{table: "publication_evidence", name: "publication_evidence_ci_authority", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "witness_digest", "github_host", "github_owner", "github_name", "github_pr_number", "github_head_owner", "github_head_repository", "github_head_ref", "github_head_oid", "github_base_ref", "github_base_oid", "github_open", "github_draft", "github_factory_owned"}},
	{table: "publication_evidence", name: "publication_evidence_repair_authority", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "witness_digest", "github_host", "github_owner", "github_name", "github_pr_number", "remote_branch_ref", "remote_branch_oid", "github_base_ref", "remote_base_oid"}},
	{table: "provider_attempts", name: "provider_attempts_builder_identity", columns: []string{"channel", "project_id", "ticket_id", "phase", "role", "attempt", "id"}},
	{table: "provider_attempt_results", name: "provider_attempt_results_builder_identity", columns: []string{"channel", "project_id", "ticket_id", "phase", "role", "attempt", "provider_attempt_id"}},
	{table: "candidate_result_bindings", name: "candidate_result_bindings_completion_identity", columns: []string{"channel", "project_id", "ticket_id", "generation", "binding_ticket_version", "leader_epoch", "runner_epoch", "provider_attempt_id", "provider_attempt"}},
	{table: "events", name: "events_identity", columns: []string{"channel", "project_id", "ticket_id", "ticket_version", "id", "created_at"}},
	{table: "events", name: "events_transition_identity", columns: []string{"channel", "project_id", "ticket_id", "ticket_version", "id", "created_at", "from_state", "to_state", "trigger"}},
	{table: "ticket_budget_uses", name: "ticket_budget_uses_correction_identity", columns: []string{"channel", "project_id", "ticket_id", "kind", "request_id", "ticket_version", "leader_epoch", "runner_epoch"}},
	{table: "ci_observations", name: "ci_observations_latest", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "observed_at", "observation_id"}, nonUnique: true},
	{table: "ci_required_check_policies", name: "ci_required_check_policies_latest", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "policy_id"}, nonUnique: true},
	{table: "ci_required_check_policies", name: "ci_required_check_policies_digest", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "publication_witness_digest"}},
	{table: "ci_required_check_policies", name: "ci_required_check_policies_witness", columns: []string{"policy_witness_digest"}},
	{table: "ci_observation_checks", name: "ci_observation_checks_observation", columns: []string{"observation_id", "canonical_name", "external_id"}, nonUnique: true},
	{table: "ci_transition_evidence", name: "ci_transition_evidence_chain", columns: []string{"channel", "project_id", "ticket_id", "ticket_version", "event_id"}, nonUnique: true},
	{table: "candidate_repair_bindings", name: "candidate_repair_bindings_predecessor", columns: []string{"channel", "project_id", "ticket_id", "predecessor_generation", "predecessor_head_sha"}, nonUnique: true},
	{table: "candidate_repair_bindings", name: "candidate_repair_bindings_target", columns: []string{"channel", "project_id", "ticket_id", "target_generation"}, nonUnique: true},
	{table: "ci_observations", name: "ci_observations_candidate_digest", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "observation_digest"}},
	{table: "ci_observations", name: "ci_observations_candidate_tree_digest", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "observation_digest"}},
	{table: "ci_observations", name: "ci_observations_candidate_tree_classification_digest", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "classification", "observation_digest"}},
	{table: "ci_observations", name: "ci_observations_transition_authority", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "classification", "observation_digest", "observed_ticket_version", "observed_leader_epoch", "observed_runner_epoch"}},
	{table: "ci_observations", name: "ci_observations_digest", columns: []string{"observation_digest"}},
	{table: "ci_transition_evidence", name: "ci_transition_evidence_digest", columns: []string{"transition_digest"}},
	{table: "ci_transition_evidence", name: "ci_transition_evidence_red_identity", columns: []string{"channel", "project_id", "ticket_id", "ticket_version", "observation_classification", "transition_digest"}},
	{table: "ci_transition_evidence", name: "ci_transition_evidence_authority", columns: []string{"channel", "project_id", "ticket_id", "candidate_generation", "candidate_head_sha", "candidate_tree_sha", "observation_classification", "observation_digest", "prior_publication_witness_digest", "observation_ticket_version", "observation_leader_epoch", "observation_runner_epoch", "ticket_version", "transition_digest"}},
	{table: "candidate_repair_bindings", name: "candidate_repair_bindings_predecessor_lineage", columns: []string{"channel", "project_id", "ticket_id", "predecessor_generation", "predecessor_head_sha", "predecessor_tree_sha"}, nonUnique: true},
	{table: "candidate_repair_bindings", name: "candidate_repair_bindings_context_digest", columns: []string{"repair_context_digest"}},
	{table: "candidate_repair_completions", name: "candidate_repair_completions_digest", columns: []string{"completion_digest"}},
	{table: "runner_start_authorities", name: "runner_start_authority_digest", columns: []string{"authority_digest"}},
	{table: "runner_start_authorities", name: "runner_start_authority_ticket", columns: []string{"channel", "project_id", "ticket_id"}, nonUnique: true},
	{table: "final_review_repair_boundaries", name: "final_review_repair_boundaries_target", columns: []string{"channel", "project_id", "ticket_id", "target_state", "transition_ticket_version"}, nonUnique: true},
	{table: "final_review_repair_boundaries", name: "final_review_repair_boundaries_reviewer", columns: []string{"channel", "project_id", "ticket_id", "reviewer_attempt_id", "reviewer_attempt", "transition_ticket_version"}},
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
