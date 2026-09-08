package store

import "testing"

func TestPostbuildAmendmentCompanionSchemaRequired(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	if err := database.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	assertExactForeignKey(t, database.db, "postbuild_amendment_checkpoint_snapshots", "postbuild_amendment_snapshots", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"amendment_transition_version", "amendment_transition_version"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_checkpoint_snapshots", "provider_attempt_results", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"reviewer_phase", "phase"}, foreignKeyPair{"reviewer_role", "role"}, foreignKeyPair{"reviewer_attempt", "attempt"}, foreignKeyPair{"reviewer_attempt_id", "provider_attempt_id"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_checkpoint_snapshots", "repository_command_results", foreignKeyPair{"command_semantic_key", "semantic_key"}, foreignKeyPair{"command_claim_epoch", "claim_epoch"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_snapshots", "verification_amendment_requests", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"amendment_transition_version", "transition_ticket_version"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_snapshots", "postbuild_repair_entries", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"repair_entry_version", "entry_ticket_version"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_snapshots", "provider_attempts", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"builder_phase", "phase"}, foreignKeyPair{"builder_role", "role"}, foreignKeyPair{"builder_attempt", "attempt"}, foreignKeyPair{"builder_attempt_id", "id"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_snapshots", "provider_attempt_results", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"builder_phase", "phase"}, foreignKeyPair{"builder_role", "role"}, foreignKeyPair{"builder_attempt", "attempt"}, foreignKeyPair{"builder_attempt_id", "provider_attempt_id"})
	assertExactForeignKey(t, database.db, "postbuild_amendment_snapshots", "verification_revisions", foreignKeyPair{"channel", "channel"}, foreignKeyPair{"project_id", "project_id"}, foreignKeyPair{"ticket_id", "ticket_id"}, foreignKeyPair{"verification_revision", "revision"})
}

func TestPostbuildAmendmentMissingSchemaRejected(t *testing.T) {
	for _, statement := range []string{
		"DROP TRIGGER postbuild_amendment_checkpoint_snapshots_immutable_update",
		"DROP TRIGGER postbuild_amendment_checkpoint_snapshots_immutable_delete",
		"DROP TRIGGER postbuild_amendment_snapshots_immutable_update",
		"DROP TRIGGER postbuild_amendment_snapshots_immutable_delete",
		"DROP INDEX postbuild_amendment_snapshots_builder",
		"DROP INDEX postbuild_amendment_snapshots_binding",
	} {
		t.Run(statement, func(t *testing.T) {
			database, ctx := openTestStore(t)
			defer database.Close()
			if _, err := database.db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
			if err := database.validateSchema(ctx); err == nil {
				t.Fatal("missing companion schema accepted")
			}
		})
	}
}
