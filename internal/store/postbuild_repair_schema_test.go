package store

import (
	"database/sql"
	"strings"
	"testing"
)

func TestPostbuildRepairV61RequiredSchema(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	if err := database.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	var ddl string
	if err := database.db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='postbuild_repair_entries'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ddl, "REFERENCES provider_phase_entries(channel,project_id,ticket_id,phase,entry_ticket_version) DEFERRABLE INITIALLY DEFERRED") {
		t.Fatal("phase entry must permit insertion after the repair binding within the same transaction")
	}
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "tickets", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "id"})
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "provider_attempts", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"builder_result_phase", "phase"}, foreignKeyPair{"builder_result_role", "role"}, foreignKeyPair{"builder_result_attempt", "attempt"}, foreignKeyPair{"builder_result_attempt_id", "id"})
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "provider_attempt_results", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"builder_result_phase", "phase"}, foreignKeyPair{"builder_result_role", "role"}, foreignKeyPair{"builder_result_attempt", "attempt"}, foreignKeyPair{"builder_result_attempt_id", "provider_attempt_id"})
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "repository_command_results", foreignKeyPair{"failed_command_semantic_key", "semantic_key"}, foreignKeyPair{"failed_command_claim_epoch", "claim_epoch"})
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "verification_revisions", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"verification_revision", "revision"})
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "ticket_budget_uses", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"correction_budget_kind", "kind"}, foreignKeyPair{"correction_budget_request_id", "request_id"}, foreignKeyPair{"consumed_ticket_version", "ticket_version"}, foreignKeyPair{"consumed_leader_epoch", "leader_epoch"}, foreignKeyPair{"consumed_runner_epoch", "runner_epoch"})
	assertExactForeignKey(t, database.db, "postbuild_repair_entries", "provider_phase_entries", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"phase", "phase"}, foreignKeyPair{"entry_ticket_version", "entry_ticket_version"})
}

func TestPostbuildRepairV61MissingSchemaRejected(t *testing.T) {
	for _, statement := range []string{
		"DROP TRIGGER postbuild_repair_entries_immutable_update",
		"DROP TRIGGER postbuild_repair_entries_immutable_delete",
		"DROP INDEX postbuild_repair_entries_failed_command",
		"DROP INDEX postbuild_repair_entries_binding_digest",
	} {
		t.Run(statement, func(t *testing.T) {
			database, ctx := openTestStore(t)
			defer database.Close()
			if _, err := database.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
			if err := database.validateSchema(ctx); err == nil {
				t.Fatal("missing required repair schema accepted")
			}
		})
	}
}

// This isolated migration fixture deliberately disables foreign keys and has no
// provider, ticket or command rows. It exercises storage shape and immutability,
// and cannot be used as authenticated repair admission evidence.
func TestPostbuildRepairV61ShapeAndImmutability(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	for _, statement := range migrationV61 {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	const insertSQL = `INSERT INTO postbuild_repair_entries(channel,project_id,ticket_id,entry_ticket_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,phase,builder_result_attempt_id,builder_result_attempt,builder_result_phase,builder_result_role,failed_command_semantic_key,failed_command_claim_epoch,verification_revision,original_checkpoint_oid,retained_worktree_digest,failed_result_digest,builder_typed_digest,correction_budget_kind,correction_budget_request_id,binding_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	digest := "sha256:" + strings.Repeat("a", 64)
	valid := []any{"dev", "schema-project", "schema-ticket", 2, 1, 1, 1, "build", 1, 1, "build", "builder", "failed-command", 1, 1, strings.Repeat("b", 40), digest, digest, strings.Repeat("c", 64), "correction", "budget-request", digest, "now"}
	for _, invalid := range []struct {
		name  string
		index int
		value any
	}{
		{"channel", 0, "other"},
		{"entry version", 3, 3},
		{"consumed version", 4, 0},
		{"leader fence", 5, 0},
		{"runner fence", 6, 0},
		{"entry phase", 7, "review"},
		{"builder ID", 8, 0},
		{"builder attempt", 9, 0},
		{"builder phase", 10, "planning"},
		{"builder role", 11, "reviewer"},
		{"command key", 12, ""},
		{"command epoch", 13, 0},
		{"verification revision", 14, 0},
		{"checkpoint width", 15, "abc"},
		{"checkpoint hex", 15, strings.Repeat("z", 40)},
		{"retained digest type", 16, "sha512:" + strings.Repeat("a", 64)},
		{"retained digest hex", 16, "sha256:" + strings.Repeat("z", 64)},
		{"result digest type", 17, "sha512:" + strings.Repeat("a", 64)},
		{"result digest hex", 17, "sha256:" + strings.Repeat("z", 64)},
		{"builder digest width", 18, digest},
		{"builder digest hex", 18, strings.Repeat("z", 64)},
		{"budget kind", 19, "fallback"},
		{"budget request", 20, ""},
		{"binding digest type", 21, "sha512:" + strings.Repeat("a", 64)},
		{"binding digest hex", 21, "sha256:" + strings.Repeat("z", 64)},
		{"created at", 22, ""},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			values := append([]any(nil), valid...)
			values[invalid.index] = invalid.value
			if _, err := db.Exec(insertSQL, values...); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
				t.Fatalf("expected shape constraint failure, got %v", err)
			}
		})
	}
	if _, err := db.Exec(insertSQL, valid...); err != nil {
		t.Fatal(err)
	}
	reused := append([]any(nil), valid...)
	reused[3], reused[4], reused[21] = 3, 2, "sha256:"+strings.Repeat("d", 64)
	if _, err := db.Exec(insertSQL, reused...); err == nil || !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Fatalf("failed command reused for another entry: %v", err)
	}
	for _, statement := range []string{
		"UPDATE postbuild_repair_entries SET created_at='later'",
		"DELETE FROM postbuild_repair_entries",
	} {
		if _, err := db.Exec(statement); err == nil || (!strings.Contains(err.Error(), "immutable") && !strings.Contains(err.Error(), "append-only")) {
			t.Fatalf("repair binding mutation accepted: %v", err)
		}
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM postbuild_repair_entries WHERE created_at='now'").Scan(&count); err != nil || count != 1 {
		t.Fatalf("original repair binding changed: count=%d err=%v", count, err)
	}
}
