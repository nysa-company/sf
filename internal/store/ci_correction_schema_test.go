package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	_ "modernc.org/sqlite"
)

func TestCICorrectionV41SchemaIsAppendOnlyAndValidated(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	if err := database.validateSchema(ctx); err != nil {
		t.Fatalf("v41 schema validation: %v", err)
	}
	var version int
	if err := database.db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion || len(migrationChecksums) != schemaVersion {
		t.Fatalf("schema version/checksum history=%d/%d", version, len(migrationChecksums))
	}
	for _, table := range []string{"ci_required_check_policies", "ci_observations", "ci_observation_checks", "ci_transition_evidence", "candidate_repair_bindings", "candidate_repair_completions", "ci_poll_schedules", "ci_poll_attempts", "ci_poll_retry_epochs"} {
		var count int
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("required table %s count=%d err=%v", table, count, err)
		}
	}
}

func TestCIV41RequiredContextDigestIndexIsValidated(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	if _, err := database.db.ExecContext(ctx, `DROP INDEX candidate_repair_bindings_context_digest`); err != nil {
		t.Fatal(err)
	}
	if err := database.validateSchema(ctx); err == nil {
		t.Fatal("schema validation accepted a missing repair-context digest index")
	}
}

type foreignKeyPair struct{ from, to string }

func assertExactForeignKey(t *testing.T, db *sql.DB, table, parent string, want ...foreignKeyPair) {
	t.Helper()
	rows, err := db.Query(`PRAGMA foreign_key_list(` + table + `)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	groups := map[int][]foreignKeyPair{}
	parents := map[int]string{}
	for rows.Next() {
		var id, seq int
		var referenced, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &referenced, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatal(err)
		}
		parents[id] = referenced
		groups[id] = append(groups[id], foreignKeyPair{from: from, to: to})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for id, pairs := range groups {
		if parents[id] != parent || len(pairs) != len(want) {
			continue
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].from < pairs[j].from })
		expected := append([]foreignKeyPair(nil), want...)
		sort.Slice(expected, func(i, j int) bool { return expected[i].from < expected[j].from })
		if fmt.Sprint(pairs) == fmt.Sprint(expected) {
			return
		}
	}
	t.Fatalf("%s is missing exact FK to %s: %v", table, parent, want)
}

func TestCICorrectionCompositeForeignKeysAreExact(t *testing.T) {
	database, _ := openTestStore(t)
	defer database.Close()
	for _, check := range []struct {
		table, parent string
		pairs         []foreignKeyPair
	}{
		{"ci_observations", "candidate_snapshots", []foreignKeyPair{{"candidate_generation", "generation"}, {"candidate_head_sha", "head_sha"}, {"candidate_tree_sha", "tree_sha"}, {"channel", "channel"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"ci_observations", "publication_evidence", []foreignKeyPair{{"candidate_generation", "candidate_generation"}, {"candidate_head_sha", "candidate_head_sha"}, {"candidate_tree_sha", "candidate_tree_sha"}, {"channel", "channel"}, {"pr_base_oid", "github_base_oid"}, {"pr_base_ref", "github_base_ref"}, {"pr_draft", "github_draft"}, {"pr_factory_owned", "github_factory_owned"}, {"pr_head_oid", "github_head_oid"}, {"pr_head_owner", "github_head_owner"}, {"pr_head_ref", "github_head_ref"}, {"pr_head_repo", "github_head_repository"}, {"pr_host", "github_host"}, {"pr_open", "github_open"}, {"pr_owner", "github_owner"}, {"pr_repo", "github_name"}, {"pr_number", "github_pr_number"}, {"project_id", "project_id"}, {"publication_witness_digest", "witness_digest"}, {"ticket_id", "ticket_id"}}},
		{"ci_transition_evidence", "events", []foreignKeyPair{{"channel", "channel"}, {"event_created_at", "created_at"}, {"event_id", "id"}, {"prior_state", "from_state"}, {"project_id", "project_id"}, {"resulting_state", "to_state"}, {"resulting_trigger", "trigger"}, {"ticket_id", "ticket_id"}, {"ticket_version", "ticket_version"}}},
		{"ci_transition_evidence", "ci_observations", []foreignKeyPair{{"candidate_generation", "candidate_generation"}, {"candidate_head_sha", "candidate_head_sha"}, {"candidate_tree_sha", "candidate_tree_sha"}, {"channel", "channel"}, {"observation_classification", "classification"}, {"observation_digest", "observation_digest"}, {"observation_leader_epoch", "observed_leader_epoch"}, {"observation_runner_epoch", "observed_runner_epoch"}, {"observation_ticket_version", "observed_ticket_version"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"ci_transition_evidence", "publication_evidence", []foreignKeyPair{{"candidate_generation", "candidate_generation"}, {"candidate_head_sha", "candidate_head_sha"}, {"candidate_tree_sha", "candidate_tree_sha"}, {"channel", "channel"}, {"prior_publication_witness_digest", "witness_digest"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_bindings", "candidate_snapshots", []foreignKeyPair{{"channel", "channel"}, {"predecessor_generation", "generation"}, {"predecessor_head_sha", "head_sha"}, {"predecessor_tree_sha", "tree_sha"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_bindings", "publication_evidence", []foreignKeyPair{{"branch_ref", "remote_branch_ref"}, {"channel", "channel"}, {"base_ref", "github_base_ref"}, {"predecessor_generation", "candidate_generation"}, {"predecessor_head_sha", "candidate_head_sha"}, {"predecessor_publication_witness_digest", "witness_digest"}, {"predecessor_tree_sha", "candidate_tree_sha"}, {"pr_host", "github_host"}, {"pr_owner", "github_owner"}, {"pr_repo", "github_name"}, {"pr_number", "github_pr_number"}, {"project_id", "project_id"}, {"remote_base_oid", "remote_base_oid"}, {"remote_head_oid", "remote_branch_oid"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_bindings", "ci_observations", []foreignKeyPair{{"channel", "channel"}, {"predecessor_generation", "candidate_generation"}, {"predecessor_head_sha", "candidate_head_sha"}, {"predecessor_tree_sha", "candidate_tree_sha"}, {"project_id", "project_id"}, {"red_observation_classification", "classification"}, {"red_observation_digest", "observation_digest"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_bindings", "ci_transition_evidence", []foreignKeyPair{{"channel", "channel"}, {"consumed_leader_epoch", "observation_leader_epoch"}, {"consumed_runner_epoch", "observation_runner_epoch"}, {"consumed_ticket_version", "observation_ticket_version"}, {"predecessor_generation", "candidate_generation"}, {"predecessor_head_sha", "candidate_head_sha"}, {"predecessor_tree_sha", "candidate_tree_sha"}, {"project_id", "project_id"}, {"predecessor_publication_witness_digest", "prior_publication_witness_digest"}, {"red_observation_classification", "observation_classification"}, {"red_observation_digest", "observation_digest"}, {"red_transition_digest", "transition_digest"}, {"red_transition_ticket_version", "ticket_version"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_bindings", "ticket_budget_uses", []foreignKeyPair{{"channel", "channel"}, {"consumed_leader_epoch", "leader_epoch"}, {"consumed_runner_epoch", "runner_epoch"}, {"consumed_ticket_version", "ticket_version"}, {"correction_budget_kind", "kind"}, {"correction_budget_request_id", "request_id"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_completions", "provider_attempts", []foreignKeyPair{{"builder_result_attempt", "attempt"}, {"builder_result_attempt_id", "id"}, {"builder_result_phase", "phase"}, {"builder_result_role", "role"}, {"channel", "channel"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_completions", "provider_attempt_results", []foreignKeyPair{{"builder_result_attempt", "attempt"}, {"builder_result_attempt_id", "provider_attempt_id"}, {"builder_result_phase", "phase"}, {"builder_result_role", "role"}, {"channel", "channel"}, {"project_id", "project_id"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_completions", "candidate_result_bindings", []foreignKeyPair{{"builder_binding_leader_epoch", "leader_epoch"}, {"builder_binding_runner_epoch", "runner_epoch"}, {"builder_binding_ticket_version", "binding_ticket_version"}, {"builder_result_attempt", "provider_attempt"}, {"builder_result_attempt_id", "provider_attempt_id"}, {"channel", "channel"}, {"project_id", "project_id"}, {"target_generation", "generation"}, {"ticket_id", "ticket_id"}}},
		{"candidate_repair_completions", "candidate_snapshots", []foreignKeyPair{{"channel", "channel"}, {"final_candidate_head_sha", "head_sha"}, {"final_candidate_tree_sha", "tree_sha"}, {"project_id", "project_id"}, {"target_generation", "generation"}, {"ticket_id", "ticket_id"}}},
	} {
		assertExactForeignKey(t, database.db, check.table, check.parent, check.pairs...)
	}
}

func TestCIV46PollingForeignKeysAreExact(t *testing.T) {
	database, _ := openTestStore(t)
	defer database.Close()
	for _, check := range []struct {
		table, parent string
		pairs         []foreignKeyPair
	}{
		{"ci_poll_schedules", "tickets", []foreignKeyPair{{"channel", "channel"}, {"project_id", "project_id"}, {"ticket_id", "id"}}},
		{"ci_poll_schedules", "publication_evidence", []foreignKeyPair{{"candidate_generation", "candidate_generation"}, {"candidate_head_sha", "candidate_head_sha"}, {"candidate_tree_sha", "candidate_tree_sha"}, {"channel", "channel"}, {"project_id", "project_id"}, {"publication_witness_digest", "witness_digest"}, {"ticket_id", "ticket_id"}}},
		{"ci_poll_attempts", "ci_poll_schedules", []foreignKeyPair{{"candidate_generation", "candidate_generation"}, {"candidate_head_sha", "candidate_head_sha"}, {"candidate_tree_sha", "candidate_tree_sha"}, {"channel", "channel"}, {"project_id", "project_id"}, {"publication_witness_digest", "publication_witness_digest"}, {"ticket_id", "ticket_id"}}},
		{"ci_poll_retry_epochs", "ci_poll_schedules", []foreignKeyPair{{"candidate_generation", "candidate_generation"}, {"candidate_head_sha", "candidate_head_sha"}, {"candidate_tree_sha", "candidate_tree_sha"}, {"channel", "channel"}, {"project_id", "project_id"}, {"publication_witness_digest", "publication_witness_digest"}, {"ticket_id", "ticket_id"}}},
	} {
		assertExactForeignKey(t, database.db, check.table, check.parent, check.pairs...)
	}
}

func TestCIV41CompositeForeignKeyTamperingRejectsOpenAndReadOnly(t *testing.T) {
	ctx := context.Background()
	for _, requirement := range requiredCompositeForeignKeys {
		requirement := requirement
		t.Run(requirement.table+"_"+requirement.target, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tampered.sqlite")
			database, err := Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			var original string
			if err := raw.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, requirement.table).Scan(&original); err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			pattern := regexp.MustCompile(`,\s*FOREIGN KEY\([^)]*\)\s+REFERENCES\s+` + regexp.QuoteMeta(requirement.target) + `\([^)]*\)`)
			corrupted := pattern.ReplaceAllString(original, "")
			if corrupted == original {
				_ = raw.Close()
				t.Fatalf("could not remove %s -> %s from table SQL", requirement.table, requirement.target)
			}
			if _, err := raw.ExecContext(ctx, `PRAGMA writable_schema=ON`); err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			if _, err := raw.ExecContext(ctx, `UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?`, corrupted, requirement.table); err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			var schemaVersion int
			if err := raw.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			if _, err := raw.ExecContext(ctx, fmt.Sprintf(`PRAGMA schema_version=%d`, schemaVersion+1)); err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			if _, err := raw.ExecContext(ctx, `PRAGMA writable_schema=OFF`); err != nil {
				_ = raw.Close()
				t.Fatal(err)
			}
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(ctx, path); err == nil {
				_ = reopened.Close()
				t.Fatal("writable open accepted a tampered composite foreign key")
			}
			if reopened, err := OpenReadOnly(ctx, path); err == nil {
				_ = reopened.Close()
				t.Fatal("read-only open accepted a tampered composite foreign key")
			}
		})
	}
}

func TestCIV41PartialCompositeForeignKeyTamperingRejectsOpenAndReadOnly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "partial-tampered.sqlite")
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	const table, target = "ci_transition_evidence", "ci_observations"
	var original string
	if err := raw.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&original); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	reference := strings.Index(original, "REFERENCES "+target)
	if reference < 0 {
		_ = raw.Close()
		t.Fatal("could not locate composite FK reference")
	}
	fkStart := strings.LastIndex(original[:reference], "FOREIGN KEY(")
	parentClose := strings.Index(original[reference:], ")")
	if fkStart < 0 || parentClose < 0 {
		_ = raw.Close()
		t.Fatal("could not locate composite FK mapping")
	}
	clauseEnd := reference + parentClose + 1
	clause := original[fkStart:clauseEnd]
	if !strings.Contains(clause, ",observation_runner_epoch") || !strings.Contains(clause, ",observed_runner_epoch") {
		_ = raw.Close()
		t.Fatal("composite FK did not contain the expected runner-fence pair")
	}
	clause = strings.Replace(clause, ",observation_runner_epoch", "", 1)
	clause = strings.Replace(clause, ",observed_runner_epoch", "", 1)
	corrupted := original[:fkStart] + clause + original[clauseEnd:]
	if _, err := raw.ExecContext(ctx, `PRAGMA writable_schema=ON`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?`, corrupted, table); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	var schemaVersion int
	if err := raw.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, fmt.Sprintf(`PRAGMA schema_version=%d`, schemaVersion+1)); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA writable_schema=OFF`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(ctx, path); err == nil {
		_ = reopened.Close()
		t.Fatal("writable open accepted a partial composite foreign key")
	}
	if reopened, err := OpenReadOnly(ctx, path); err == nil {
		_ = reopened.Close()
		t.Fatal("read-only open accepted a partial composite foreign key")
	}
}

func TestCIV41TamperedSchemaRefusesBeforeStartupRecoveryMutation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "populated-tampered.sqlite")
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-populated-tamper"}
	if err := database.CreateProject(ctx, Project{Channel: ref.Channel, ID: ref.Project, Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: strings.Repeat("s", 64), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded}); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`INSERT INTO ticket_counters(channel,project_id,ticket_id,kind,used,limit_count) VALUES('dev','nysa','SF-populated-tamper','correction',1,2)`,
		`INSERT INTO ticket_budget_uses(channel,project_id,ticket_id,kind,request_id,ticket_version,leader_epoch,runner_epoch,created_at) VALUES('dev','nysa','SF-populated-tamper','correction','persisted-budget',1,1,1,'2026-08-30T00:00:00Z')`,
		`INSERT INTO runtime_ticket_controls(channel,project_id,ticket_id,state,generation,stop_version,stop_leader_epoch,stop_runner_epoch,authority_version,authority_leader_epoch,authority_runner_epoch,updated_at) VALUES('dev','nysa','SF-populated-tamper','open',1,1,1,1,1,1,1,'2026-08-30T00:00:00Z')`,
	} {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			_ = raw.Close()
			t.Fatal(err)
		}
	}
	before := snapshotAuthorityTables(t, raw)
	requirement := compositeForeignKey("ci_transition_evidence", "ci_observations")
	var original string
	if err := raw.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, requirement.table).Scan(&original); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`,\s*FOREIGN KEY\([^)]*\)\s+REFERENCES\s+` + regexp.QuoteMeta(requirement.target) + `\([^)]*\)`)
	corrupted := pattern.ReplaceAllString(original, "")
	if corrupted == original {
		_ = raw.Close()
		t.Fatal("could not tamper populated v41 schema")
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA writable_schema=ON`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?`, corrupted, requirement.table); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	var schemaVersion int
	if err := raw.QueryRowContext(ctx, `PRAGMA schema_version`).Scan(&schemaVersion); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, fmt.Sprintf(`PRAGMA schema_version=%d`, schemaVersion+1)); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA writable_schema=OFF`); err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(ctx, path); err == nil {
		_ = reopened.Close()
		t.Fatal("writable open accepted a populated tampered schema")
	}
	raw, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	after := snapshotAuthorityTables(t, raw)
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatalf("startup failure mutated durable authority: before=%v after=%v", before, after)
	}
}

func snapshotAuthorityTables(t *testing.T, db *sql.DB) map[string]string {
	t.Helper()
	snapshot := make(map[string]string)
	for _, table := range []string{"tickets", "events", "ticket_counters", "ticket_budget_uses", "runtime_ticket_controls"} {
		rows, err := db.Query(`SELECT * FROM ` + table + ` ORDER BY rowid`)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		var values []string
		for rows.Next() {
			row := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for index := range row {
				pointers[index] = &row[index]
			}
			if err := rows.Scan(pointers...); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			values = append(values, fmt.Sprintf("%#v", row))
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		snapshot[table] = fmt.Sprint(values)
	}
	return snapshot
}

type v41Fixture struct {
	ref                         domain.TicketRef
	head1, tree1, head2, tree2  string
	witness, observation, trans string
	eventCreated                string
	eventID                     int64
}

func insertArgs(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func insertID(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	result, err := db.Exec(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func insertV41Fixture(t *testing.T, database *Store, ctx context.Context) v41Fixture {
	return insertV41FixtureOptions(t, database, ctx, true, true)
}

func insertV41FixtureOptions(t *testing.T, database *Store, ctx context.Context, withBinding, withCompletion bool) v41Fixture {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-ci-authority"}
	if err := database.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: strings.Repeat("s", 64), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	f := v41Fixture{ref: ref, head1: strings.Repeat("a", 40), tree1: strings.Repeat("b", 40), head2: strings.Repeat("c", 40), tree2: strings.Repeat("d", 40), witness: "sha256:" + strings.Repeat("1", 64), observation: "sha256:" + strings.Repeat("2", 64), trans: "sha256:" + strings.Repeat("3", 64), eventCreated: "2026-08-30T00:00:00Z"}
	base := strings.Repeat("e", 40)
	plain := func(ch byte) string { return strings.Repeat(string(ch), 64) }
	insertArgs(t, database.db, `INSERT INTO candidate_snapshots(channel,project_id,ticket_id,generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 1, 1, 7, 2, base, f.head1, f.tree1, plain('f'), plain('0'), plain('1'), plain('2'), f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO candidate_snapshots(channel,project_id,ticket_id,generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 2, 2, 7, 2, base, f.head2, f.tree2, plain('a'), plain('b'), plain('c'), plain('d'), f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO provider_attempts(id,channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,binding_digest,provider_lease_key,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,supervisor_key,launch_state,process_pid,process_pgid,process_started_at,process_start_identity,process_boot_identity,auth_digest,auth_mode) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, 1, ref.Channel, ref.Project, ref.Ticket, "build", 1, "provider", "model", "family", "1", "completed", "builder", "completed", 1, f.eventCreated, f.eventCreated, plain('3'), "lease", 7, 2, 1, "/repo", "/worktree", "identity", base, []byte(strings.Repeat("k", 32)), "released", 0, 0, "", "start", "boot", plain('4'), "api")
	insertArgs(t, database.db, `INSERT INTO provider_attempt_results(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,provider,model,family,provider_version,request_digest,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,raw_artifact,raw_sha256,typed_artifact,typed_sha256,validation,validation_sha256,transcript_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, 1, ref.Channel, ref.Project, ref.Ticket, "build", "builder", 1, "provider", "model", "family", "1", plain('5'), 7, 2, 1, "/repo", "/worktree", "identity", base, []byte("raw"), plain('6'), []byte("{}"), plain('7'), []byte("{}"), plain('8'), "", f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO candidate_result_bindings(channel,project_id,ticket_id,generation,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,commit_parent_oid) VALUES(?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 2, 2, 7, 2, 1, 1, base)
	insertArgs(t, database.db, `INSERT INTO publication_evidence(channel,project_id,ticket_id,ticket_version,leader_epoch,runner_epoch,candidate_generation,candidate_ticket_version,candidate_leader_epoch,candidate_runner_epoch,candidate_base_sha,candidate_head_sha,candidate_tree_sha,candidate_source_digest,candidate_verification_intent_digest,candidate_proof_digest,candidate_command_policy_digest,candidate_builder_evidence_digest,candidate_builder_attempt_id,candidate_builder_attempt,candidate_commit_parent_oid,candidate_command_semantic_key,candidate_command_claim_epoch,candidate_command_ticket_version,candidate_command_leader_epoch,candidate_command_runner_epoch,candidate_command_digest,candidate_command_spec_digest,candidate_command_policy_claim_digest,candidate_command_executable_path,candidate_command_executable_digest,config_generation,config_digest,config_snapshot_digest,worktree_path,worktree_branch_ref,worktree_state,worktree_ticket_version,worktree_leader_epoch,worktree_runner_epoch,worktree_identity_json,worktree_identity_digest,worktree_base_sha,remote_branch_ref,remote_branch_oid,remote_base_oid,push_effect_semantic_key,push_effect_kind,push_effect_request_digest,push_effect_claim_epoch,push_effect_observed_identity,github_host,github_owner,github_name,github_pr_number,github_head_owner,github_head_repository,github_head_ref,github_head_oid,github_base_ref,github_base_oid,github_state,github_draft,github_factory_owned,github_observed_at,pr_effect_semantic_key,pr_effect_kind,pr_effect_request_digest,pr_effect_claim_epoch,pr_effect_observed_identity,witness_digest,created_at) VALUES(`+strings.TrimSuffix(strings.Repeat("?,", 72), ",")+")", ref.Channel, ref.Project, ref.Ticket, 1, 7, 2, 1, 1, 7, 2, base, f.head1, f.tree1, plain('f'), plain('0'), plain('1'), plain('2'), plain('3'), 1, 1, strings.Repeat("p", 40), "command", 1, 1, 7, 2, plain('4'), plain('5'), plain('6'), "/bin/true", plain('7'), 1, plain('8'), plain('9'), "/worktree", "branch", "registered", 1, 7, 2, []byte("{}"), plain('a'), base, "branch", f.head1, base, "push", "push", plain('b'), 1, "push-observation", "github.com", "owner", "repo", 1, "owner", "repo", "branch", f.head1, "main", base, "OPEN", 1, 1, f.eventCreated, "pr", "pr", plain('c'), 1, "pr-observation", f.witness, f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO ci_observations(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 1, f.head1, f.tree1, f.witness, "github.com", "owner", "repo", 1, "owner", "repo", "branch", f.head1, "main", base, 1, 1, 1, 1, 7, 2, f.eventCreated, plain('d'), 1, "red", plain('e'), "failure", `{"checks":["lint"]}`, f.observation)
	insertArgs(t, database.db, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) VALUES(1,?,?,?,?,?,?)`, f.observation, "lint", "check-1", "failure", plain('f'), "lint failed")
	f.eventID = insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 2, "checks_red", "waiting_ci", "paused", "{}", f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 1, f.head1, f.tree1, 2, f.eventID, f.eventCreated, "red", f.observation, 1, 7, 2, f.witness, "waiting_ci", "paused", "checks_red", f.trans, f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO ticket_budget_uses(channel,project_id,ticket_id,kind,request_id,ticket_version,leader_epoch,runner_epoch,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, "correction", "budget-1", 1, 7, 2, f.eventCreated)
	if withBinding {
		insertArgs(t, database.db, `INSERT INTO candidate_repair_bindings(channel,project_id,ticket_id,target_generation,predecessor_generation,predecessor_head_sha,predecessor_tree_sha,predecessor_publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,branch_ref,remote_head_oid,base_ref,remote_base_oid,red_observation_digest,red_observation_classification,red_transition_ticket_version,red_transition_digest,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,repair_context_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 2, 1, f.head1, f.tree1, f.witness, "github.com", "owner", "repo", 1, "branch", f.head1, "main", base, f.observation, "red", 2, f.trans, "correction", "budget-1", 1, 7, 2, "sha256:"+plain('0'), f.eventCreated)
	}
	if withCompletion {
		insertArgs(t, database.db, `INSERT INTO candidate_repair_completions(channel,project_id,ticket_id,target_generation,builder_result_attempt_id,builder_result_attempt,builder_result_phase,builder_result_role,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 2, 1, 1, "build", "builder", 2, 7, 2, f.head2, f.tree2, "sha256:"+plain('9'), f.eventCreated)
	}
	return f
}

func expectInsertError(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err == nil {
		t.Fatalf("invalid authority row was accepted for %s", query)
	}
}

func TestCIV41AuthorityChainAndNegativeBindings(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	f := insertV41Fixture(t, database, ctx)
	for _, statement := range []string{
		`UPDATE ci_observations SET diagnostic_text='mutated' WHERE observation_id=1`,
		`DELETE FROM ci_observation_checks WHERE observation_id=1`,
		`UPDATE ci_transition_evidence SET resulting_trigger='mutated' WHERE ticket_id=?`,
		`DELETE FROM candidate_repair_bindings WHERE target_generation=2`,
		`UPDATE candidate_repair_completions SET completed_at='mutated' WHERE target_generation=2`,
	} {
		var err error
		if strings.Contains(statement, "?") {
			_, err = database.db.ExecContext(ctx, statement, f.ref.Ticket)
		} else {
			_, err = database.db.ExecContext(ctx, statement)
		}
		if err == nil {
			t.Fatalf("mutable authority statement accepted: %s", statement)
		}
	}
	observationInsert := `INSERT INTO ci_observations(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest) SELECT channel,project_id,?,candidate_generation,candidate_head_sha,?, ?,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,?,? FROM ci_observations WHERE observation_id=1`
	expectInsertError(t, database.db, observationInsert, f.ref.Ticket, f.tree2, f.witness, `{}`, "sha256:"+strings.Repeat("8", 64))
	expectInsertError(t, database.db, observationInsert, f.ref.Ticket+"-other", f.tree1, "sha256:"+strings.Repeat("7", 64), `{}`, "sha256:"+strings.Repeat("9", 64))
	expectInsertError(t, database.db, observationInsert, f.ref.Ticket, f.tree1, f.witness, "not-json", "sha256:"+strings.Repeat("a", 64))
	// A fresh observation fence avoids the temporal check masking the event
	// identity mismatch.
	wrongEventObservation := "sha256:" + strings.Repeat("5", 64)
	wrongEventObservationInsert := `INSERT INTO ci_observations(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest) SELECT channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,?,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,?,diagnostic_digest,diagnostic_text,diagnostic_json,? FROM ci_observations WHERE observation_id=1`
	insertArgs(t, database.db, wrongEventObservationInsert, 2, "red", wrongEventObservation)
	insertArgs(t, database.db, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) SELECT observation_id,observation_digest,?,?,?,?,? FROM ci_observations WHERE observation_digest=?`, "lint", "check-wrong-event", "failure", strings.Repeat("4", 64), "lint failed", wrongEventObservation)
	event3 := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 3, "checks_red", "waiting_ci", "paused", "{}", f.eventCreated)
	expectInsertError(t, database.db, `INSERT INTO ci_transition_evidence SELECT channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,3,?, ?,classification,observation_digest,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,publication_witness_digest,"waiting_ci","paused","checks_red",?,? FROM ci_observations WHERE observation_digest=?`, event3, "wrong-time", "sha256:"+strings.Repeat("b", 64), f.eventCreated, wrongEventObservation)
	budgetDB, budgetCtx := openTestStore(t)
	budgetFixture := insertV41FixtureOptions(t, budgetDB, budgetCtx, false, false)
	budgetArgs := []any{budgetFixture.ref.Channel, budgetFixture.ref.Project, budgetFixture.ref.Ticket, 2, 1, budgetFixture.head1, budgetFixture.tree1, budgetFixture.witness, "github.com", "owner", "repo", 1, "branch", budgetFixture.head1, "main", strings.Repeat("e", 40), budgetFixture.observation, "red", 2, budgetFixture.trans, "correction", "missing-budget", 1, 7, 2, "sha256:" + strings.Repeat("c", 64), budgetFixture.eventCreated}
	expectInsertError(t, budgetDB.db, `INSERT INTO candidate_repair_bindings(channel,project_id,ticket_id,target_generation,predecessor_generation,predecessor_head_sha,predecessor_tree_sha,predecessor_publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,branch_ref,remote_head_oid,base_ref,remote_base_oid,red_observation_digest,red_observation_classification,red_transition_ticket_version,red_transition_digest,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,repair_context_digest,created_at) VALUES(`+strings.TrimSuffix(strings.Repeat("?,", len(budgetArgs)), ",")+`)`, budgetArgs...)
	lineageDB, lineageCtx := openTestStore(t)
	lineageFixture := insertV41FixtureOptions(t, lineageDB, lineageCtx, false, false)
	lineageArgs := []any{lineageFixture.ref.Channel, lineageFixture.ref.Project, lineageFixture.ref.Ticket, 2, 1, lineageFixture.head1, lineageFixture.tree1, lineageFixture.witness, "github.com", "owner", "repo", 1, "branch", lineageFixture.head1, "main", strings.Repeat("e", 40), lineageFixture.observation, "red", 2, "sha256:" + strings.Repeat("x", 64), "correction", "budget-1", 1, 7, 2, "sha256:" + strings.Repeat("c", 64), lineageFixture.eventCreated}
	expectInsertError(t, lineageDB.db, `INSERT INTO candidate_repair_bindings(channel,project_id,ticket_id,target_generation,predecessor_generation,predecessor_head_sha,predecessor_tree_sha,predecessor_publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,branch_ref,remote_head_oid,base_ref,remote_base_oid,red_observation_digest,red_observation_classification,red_transition_ticket_version,red_transition_digest,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,repair_context_digest,created_at) VALUES(`+strings.TrimSuffix(strings.Repeat("?,", len(lineageArgs)), ",")+`)`, lineageArgs...)
	completionDB, completionCtx := openTestStore(t)
	completionFixture := insertV41FixtureOptions(t, completionDB, completionCtx, true, false)
	insertArgs(t, completionDB.db, `INSERT INTO provider_attempts SELECT 2,channel,project_id,ticket_id,phase,2,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,qualification_id,binding_digest,provider_lease_key,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,supervisor_key,launch_state,process_pid,process_pgid,process_started_at,process_start_identity,process_boot_identity,auth_digest,auth_mode FROM provider_attempts WHERE id=1`)
	completionArgs := func(attemptID, attempt int, tree, digest string) []any {
		return []any{completionFixture.ref.Channel, completionFixture.ref.Project, completionFixture.ref.Ticket, 2, attemptID, attempt, "build", "builder", 2, 7, 2, completionFixture.head2, tree, digest, completionFixture.eventCreated}
	}
	completionSQL := `INSERT INTO candidate_repair_completions(channel,project_id,ticket_id,target_generation,builder_result_attempt_id,builder_result_attempt,builder_result_phase,builder_result_role,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at) VALUES(` + strings.TrimSuffix(strings.Repeat("?,", 15), ",") + `)`
	expectInsertError(t, completionDB.db, completionSQL, completionArgs(99, 1, completionFixture.tree2, "sha256:"+strings.Repeat("d", 64))...)
	expectInsertError(t, completionDB.db, completionSQL, completionArgs(2, 2, completionFixture.tree2, "sha256:"+strings.Repeat("f", 64))...)
	insertArgs(t, completionDB.db, `INSERT INTO provider_attempt_results SELECT 2,channel,project_id,ticket_id,phase,role,2,provider,model,family,provider_version,request_digest,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,raw_artifact,raw_sha256,typed_artifact,typed_sha256,validation,validation_sha256,transcript_sha256,created_at FROM provider_attempt_results WHERE provider_attempt_id=1`)
	expectInsertError(t, completionDB.db, completionSQL, completionArgs(2, 2, completionFixture.tree2, "sha256:"+strings.Repeat("0", 64))...)
	expectInsertError(t, completionDB.db, completionSQL, completionArgs(1, 1, completionFixture.tree1, "sha256:"+strings.Repeat("e", 64))...)
}

func TestCIV41TransitionClassificationMappings(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	f := insertV41Fixture(t, database, ctx)
	observationCopy := `INSERT INTO ci_observations(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest) SELECT channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,?,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,?,?,diagnostic_digest,diagnostic_text,diagnostic_json,? FROM ci_observations WHERE observation_id=1`
	insertArgs(t, database.db, observationCopy, 2, 1, "pending", "sha256:"+strings.Repeat("6", 64))
	pendingDigest := "sha256:" + strings.Repeat("6", 64)
	insertArgs(t, database.db, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) SELECT observation_id,observation_digest,?,?,?,?,? FROM ci_observations WHERE observation_digest=?`, "lint", "check-pending", "pending", "", "", pendingDigest)
	insertArgs(t, database.db, observationCopy, 3, 1, "green", "sha256:"+strings.Repeat("7", 64))
	greenDigest := "sha256:" + strings.Repeat("7", 64)
	insertArgs(t, database.db, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) SELECT observation_id,observation_digest,?,?,?,?,? FROM ci_observations WHERE observation_digest=?`, "lint", "check-green", "success", "", "", greenDigest)
	pendingEvent := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 3, "checks_pending", "waiting_ci", "waiting_ci", "{}", f.eventCreated)
	greenEvent := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 4, "checks_green", "waiting_ci", "reviewing", "{}", f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 3, pendingEvent, f.eventCreated, "pending", pendingDigest, 2, 7, 2, f.witness, "waiting_ci", "waiting_ci", "checks_pending", "sha256:"+strings.Repeat("8", 64), f.eventCreated)
	insertArgs(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 4, greenEvent, f.eventCreated, "green", greenDigest, 3, 7, 2, f.witness, "waiting_ci", "reviewing", "checks_green", "sha256:"+strings.Repeat("9", 64), f.eventCreated)
	badEvent := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 5, "checks_green", "waiting_ci", "reviewing", "{}", f.eventCreated)
	wrongMappingObservation := "sha256:" + strings.Repeat("c", 64)
	insertArgs(t, database.db, observationCopy, 4, 1, "pending", wrongMappingObservation)
	insertArgs(t, database.db, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) SELECT observation_id,observation_digest,?,?,?,?,? FROM ci_observations WHERE observation_digest=?`, "lint", "check-mismatch", "pending", "", "", wrongMappingObservation)
	expectInsertError(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 5, badEvent, f.eventCreated, "pending", wrongMappingObservation, 4, 7, 2, f.witness, "waiting_ci", "reviewing", "checks_green", "sha256:"+strings.Repeat("a", 64), f.eventCreated)
	// The observation fence is part of the exact transition authority key;
	// changing only that fence must fail even when the digest remains valid.
	fenceEvent := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 6, "checks_pending", "waiting_ci", "waiting_ci", "{}", f.eventCreated)
	expectInsertError(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 6, fenceEvent, f.eventCreated, "red", f.observation, 5, 7, 2, f.witness, "waiting_ci", "waiting_ci", "checks_pending", "sha256:"+strings.Repeat("e", 64), f.eventCreated)
	// A non-empty required set cannot be transitioned without at least one
	// normalized check row; exercise both red and green classifications.
	noCheckRed := "sha256:" + strings.Repeat("f", 64)
	insertArgs(t, database.db, observationCopy, 6, 1, "red", noCheckRed)
	noCheckRedEvent := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 7, "checks_red", "waiting_ci", "paused", "{}", f.eventCreated)
	expectInsertError(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 7, noCheckRedEvent, f.eventCreated, "red", noCheckRed, 6, 7, 2, f.witness, "waiting_ci", "paused", "checks_red", "sha256:"+strings.Repeat("b", 64), f.eventCreated)
	noCheckGreen := "sha256:" + strings.Repeat("0", 64)
	insertArgs(t, database.db, observationCopy, 8, 1, "green", noCheckGreen)
	noCheckGreenEvent := insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 9, "checks_green", "waiting_ci", "reviewing", "{}", f.eventCreated)
	expectInsertError(t, database.db, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 9, noCheckGreenEvent, f.eventCreated, "green", noCheckGreen, 8, 7, 2, f.witness, "waiting_ci", "reviewing", "checks_green", "sha256:"+strings.Repeat("1", 64), f.eventCreated)
	insertObservation := func(version, count int, classification, digest string) {
		insertArgs(t, database.db, observationCopy, version, count, classification, digest)
	}
	insertCheck := func(digest, name, state string) {
		diagnosticDigest, diagnosticText := "", ""
		if state == "failure" || state == "cancelled" {
			diagnosticDigest, diagnosticText = strings.Repeat("4", 64), "check failed"
		}
		insertArgs(t, database.db, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) SELECT observation_id,observation_digest,?,?,?,?,? FROM ci_observations WHERE observation_digest=?`, name, name+"-id", state, diagnosticDigest, diagnosticText, digest)
	}
	newEvent := func(version int, trigger, toState string) int64 {
		return insertID(t, database.db, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, version, trigger, "waiting_ci", toState, "{}", f.eventCreated)
	}
	transitionInsert := `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	// Each reducer mismatch below has a valid event, publication, candidate,
	// observation fence, and digest. Only the reducer/count guard should reject it.
	greenFailure := "sha256:" + strings.Repeat("a", 64)
	insertObservation(10, 1, "green", greenFailure)
	insertCheck(greenFailure, "green-failure", "failure")
	expectInsertError(t, database.db, transitionInsert, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 11, newEvent(11, "checks_green", "reviewing"), f.eventCreated, "green", greenFailure, 10, 7, 2, f.witness, "waiting_ci", "reviewing", "checks_green", "sha256:"+strings.Repeat("3", 64), f.eventCreated)
	redSuccess := "sha256:" + strings.Repeat("b", 64)
	insertObservation(12, 1, "red", redSuccess)
	insertCheck(redSuccess, "red-success", "success")
	expectInsertError(t, database.db, transitionInsert, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 13, newEvent(13, "checks_red", "paused"), f.eventCreated, "red", redSuccess, 12, 7, 2, f.witness, "waiting_ci", "paused", "checks_red", "sha256:"+strings.Repeat("4", 64), f.eventCreated)
	pendingSuccess := "sha256:" + strings.Repeat("d", 64)
	insertObservation(14, 1, "pending", pendingSuccess)
	insertCheck(pendingSuccess, "pending-success", "success")
	expectInsertError(t, database.db, transitionInsert, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 15, newEvent(15, "checks_pending", "waiting_ci"), f.eventCreated, "pending", pendingSuccess, 14, 7, 2, f.witness, "waiting_ci", "waiting_ci", "checks_pending", "sha256:"+strings.Repeat("5", 64), f.eventCreated)
	mixedPending := "sha256:" + strings.Repeat("e", 64)
	insertObservation(16, 2, "pending", mixedPending)
	insertCheck(mixedPending, "mixed-failure", "failure")
	insertCheck(mixedPending, "mixed-pending", "pending")
	expectInsertError(t, database.db, transitionInsert, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 17, newEvent(17, "checks_pending", "waiting_ci"), f.eventCreated, "pending", mixedPending, 16, 7, 2, f.witness, "waiting_ci", "waiting_ci", "checks_pending", "sha256:"+strings.Repeat("7", 64), f.eventCreated)
	countSmall := "sha256:" + strings.Repeat("g", 64)
	insertObservation(18, 1, "pending", countSmall)
	insertCheck(countSmall, "count-small-pending", "pending")
	insertCheck(countSmall, "count-small-success", "success")
	expectInsertError(t, database.db, transitionInsert, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 19, newEvent(19, "checks_pending", "waiting_ci"), f.eventCreated, "pending", countSmall, 18, 7, 2, f.witness, "waiting_ci", "waiting_ci", "checks_pending", "sha256:"+strings.Repeat("8", 64), f.eventCreated)
	countLarge := "sha256:" + strings.Repeat("h", 64)
	insertObservation(20, 2, "pending", countLarge)
	insertCheck(countLarge, "count-large-pending", "pending")
	expectInsertError(t, database.db, transitionInsert, f.ref.Channel, f.ref.Project, f.ref.Ticket, 1, f.head1, f.tree1, 21, newEvent(21, "checks_pending", "waiting_ci"), f.eventCreated, "pending", countLarge, 20, 7, 2, f.witness, "waiting_ci", "waiting_ci", "checks_pending", "sha256:"+strings.Repeat("9", 64), f.eventCreated)
}

func TestCIV41MigrationFailsClosedForLegacyWaitingCI(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	createDatabaseAtVersion(t, path, 40)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.ExecContext(ctx, `INSERT INTO daemon_instances(channel,leader_epoch,identity,updated_at) VALUES('dev',7,'legacy-daemon','now'); INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('dev','legacy-ci','/legacy-ci','main'); INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('dev','legacy-ci','SF-legacy-ci','legacy-source','feature','guarded','waiting_ci',4,2,'legacy-ci-workflow')`)
	if closeErr := raw.Close(); err != nil || closeErr != nil {
		t.Fatalf("seed legacy waiting-ci err=%v close=%v", err, closeErr)
	}
	database, err := OpenChannel(ctx, path, filepath.Join(t.TempDir(), "backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var state, resume, code string
	var version int
	if err := database.db.QueryRowContext(ctx, `SELECT state,resume_state,blocked_code,version FROM tickets WHERE channel='dev' AND project_id='legacy-ci' AND id='SF-legacy-ci'`).Scan(&state, &resume, &code, &version); err != nil {
		t.Fatal(err)
	}
	if state != "blocked" || resume != "waiting_ci" || code != "legacy_ci_observation_unverifiable" || version != 5 {
		t.Fatalf("legacy waiting-ci disposition=%s/%s/%s/v%d", state, resume, code, version)
	}
}

func TestCIV41RowsSurviveV42V43MigrationAndRemainImmutable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v41-ci.sqlite")
	createDatabaseAtVersion(t, path, 41)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('dev','nysa','/nysa','main')`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	// Reuse the real v41 authority fixture against the staged raw database;
	// only the migration runner is bypassed, not any fixture or FK/trigger.
	staged := &Store{db: raw, commit: func(ctx context.Context, conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, "COMMIT")
		return err
	}}
	insertV41Fixture(t, staged, ctx)
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	backups := filepath.Join(t.TempDir(), "backups")
	if err := os.Mkdir(backups, 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := OpenChannel(ctx, path, backups, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var observations, checks, evidence, bindings, completions int
	for table, target := range map[string]*int{
		"ci_observations":              &observations,
		"ci_observation_checks":        &checks,
		"ci_transition_evidence":       &evidence,
		"candidate_repair_bindings":    &bindings,
		"candidate_repair_completions": &completions,
	} {
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if observations != 1 || checks != 1 || evidence != 1 || bindings != 1 || completions != 1 {
		t.Fatalf("v41 authority rows lost during v42-v45 migration: observations=%d checks=%d evidence=%d bindings=%d completions=%d", observations, checks, evidence, bindings, completions)
	}
	var version int
	if err := database.db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("migrated schema=%d err=%v", version, err)
	}
	if _, err := database.db.ExecContext(ctx, `UPDATE ci_observations SET diagnostic_text='tampered' WHERE observation_id=1`); err == nil {
		t.Fatal("migrated CI observation lost immutable trigger")
	}
	var startAuthorityTrigger int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='runner_start_authorities_immutable_update'`).Scan(&startAuthorityTrigger); err != nil || startAuthorityTrigger != 1 {
		t.Fatalf("migrated runner start authority lost immutable trigger: count=%d err=%v", startAuthorityTrigger, err)
	}
	files, err := filepath.Glob(filepath.Join(backups, "sf-schema-v*-to-v*.sqlite"))
	if err != nil || len(files) != 1 {
		t.Fatalf("stable v41 migration backup=%v err=%v", files, err)
	}
	backup, err := sql.Open("sqlite", files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var backupVersion, backupObservations int
	if err := backup.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&backupVersion); err != nil {
		t.Fatal(err)
	}
	if err := backup.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_observations`).Scan(&backupObservations); err != nil {
		t.Fatal(err)
	}
	if backupVersion != 41 || backupObservations != 1 {
		t.Fatalf("backup lost staged v41 authority rows: version=%d observations=%d", backupVersion, backupObservations)
	}
	if info, err := os.Stat(files[0]); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("invalid migration backup info=%v err=%v", info, err)
	}
}

func TestV41PlanningWithoutRunnerStartAuthorityBlocksDuringV45UpgradeAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v41-planning.sqlite")
	createDatabaseAtVersion(t, path, 41)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.ExecContext(ctx, `INSERT INTO daemon_instances(channel,leader_epoch,identity,updated_at) VALUES('stable',7,'legacy-daemon','now'); INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('stable','legacy-planning','/legacy-planning','main'); INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('stable','legacy-planning','SF-legacy-planning','legacy-source','feature','guarded','planning',2,1,'legacy-planning-workflow')`)
	if closeErr := raw.Close(); err != nil || closeErr != nil {
		t.Fatalf("seed v41 planning err=%v close=%v", err, closeErr)
	}
	backups := filepath.Join(t.TempDir(), "backups")
	if err := os.Mkdir(backups, 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := OpenChannel(ctx, path, backups, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "legacy-planning", Ticket: "SF-legacy-planning"}
	assertDisposition := func() {
		var state, resume, code, trigger, from, to, payload string
		var version, events, authorities int
		if err := database.db.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code,version FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &resume, &code, &version); err != nil {
			t.Fatal(err)
		}
		if state != "blocked" || resume != "" || code != "legacy_runner_start_authority_unverifiable" || version != 3 {
			t.Fatalf("planning disposition=%s/%s/%s/v%d", state, resume, code, version)
		}
		if err := database.db.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=3`, ref.Channel, ref.Project, ref.Ticket).Scan(&trigger, &from, &to, &payload); err != nil || trigger != "typed_blocker" || from != "planning" || to != "blocked" || !strings.Contains(payload, "submit a fresh ticket") {
			t.Fatalf("planning blocker event=%s/%s/%s payload=%s err=%v", trigger, from, to, payload, err)
		}
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&events); err != nil || events != 1 {
			t.Fatalf("planning events=%d err=%v", events, err)
		}
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_start_authorities WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&authorities); err != nil || authorities != 0 {
			t.Fatalf("unexpected reconstructed authority=%d err=%v", authorities, err)
		}
	}
	assertDisposition()
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = OpenChannel(ctx, path, backups, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	assertDisposition()
	files, err := filepath.Glob(filepath.Join(backups, "sf-schema-v041-to-v045-*.sqlite"))
	if err != nil || len(files) != 1 {
		t.Fatalf("v41-to-v45 backup=%v err=%v", files, err)
	}
	backup, err := sql.Open("sqlite", files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var backupVersion, backupPlanning int
	if err := backup.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&backupVersion); err != nil {
		t.Fatal(err)
	}
	if err := backup.QueryRowContext(ctx, `SELECT COUNT(*) FROM tickets WHERE state='planning' AND version=2 AND runner_epoch=1`).Scan(&backupPlanning); err != nil {
		t.Fatal(err)
	}
	if backupVersion != 41 || backupPlanning != 1 {
		t.Fatalf("backup changed legacy planning evidence: schema=%d planning=%d", backupVersion, backupPlanning)
	}
}
