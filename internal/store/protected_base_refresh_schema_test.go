package store

import (
	"strings"
	"testing"
)

func TestProtectedBaseRefreshV56SchemaIsAppendOnly(t *testing.T) {
	database, ctx := openTestStore(t)
	defer database.Close()
	if err := database.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestProtectedBaseRefreshV56Constraints(t *testing.T) {
	makeTicket := ticket
	db, ctx, current, fence := publicationLifecycleFixture(t)
	defer db.Close()
	other := current.Ref
	other.Ticket += "-other"
	if err := db.CreateTicket(ctx, makeTicket(other, "refresh-schema-other")); err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := db.Worktree(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit SQL fixtures exercise schema constraints only. These planned
	// effects and shape-only payloads are NOT authenticated production evidence.
	digest := func(s string) string { return "sha256:" + strings.Repeat(s, 64) }
	key := func(s string) string { return "refresh-schema-" + s }
	for _, effect := range []string{"proof", "refresh"} {
		if _, err := db.db.ExecContext(ctx, `INSERT INTO effects(semantic_key,channel,project_id,ticket_id,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, key(effect), current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, "refresh", "planned", current.Version, fence.LeaderEpoch, fence.RunnerEpoch, 0, digest("a")); err != nil {
			t.Fatal(err)
		}
	}
	const intentSQL = `INSERT INTO protected_base_refresh_intents(channel,project_id,ticket_id,ticket_version,leader_epoch,runner_epoch,source_digest,config_generation,config_digest,config_snapshot_digest,intent_json,intent_digest,old_candidate_generation,old_candidate_head_sha,old_candidate_tree_sha,old_candidate_base_sha,worktree_path,worktree_identity_json,worktree_identity_digest,new_base_sha,base_proof_digest,base_proof_semantic_key,refresh_effect_semantic_key,refresh_effect_request_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	const preparationSQL = `INSERT INTO protected_base_refresh_preparations(refresh_id,channel,project_id,ticket_id,old_candidate_head_sha,new_base_sha,prepared_commit_oid,prepared_tree_oid,prepared_parent_1_oid,prepared_parent_2_oid,prepared_digest,prepared_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`
	const completionSQL = `INSERT INTO protected_base_refresh_completions(refresh_id,channel,project_id,ticket_id,prepared_commit_oid,prepared_tree_oid,prepared_parent_1_oid,prepared_parent_2_oid,effective_base_sha,effective_worktree_identity_json,effective_worktree_identity_digest,completion_ticket_version,completion_leader_epoch,completion_runner_epoch,completion_digest,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	anchor, newBase := strings.Repeat("1", 40), strings.Repeat("2", 40)
	intent := []any{current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, current.Version, fence.LeaderEpoch, fence.RunnerEpoch, current.SourceDigest, current.ConfigGeneration, current.ConfigDigest, current.ConfigDigest, `{}`, digest("a"), candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, candidate.Snapshot.TreeSHA, candidate.Snapshot.BaseSHA, worktree.Path, string(worktree.IdentityJSON), digest("b"), newBase, digest("c"), key("proof"), key("refresh"), digest("d"), "now"}
	result, err := db.db.ExecContext(ctx, intentSQL, intent...)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	expectConstraint := func(name, fragment, query string, args ...any) {
		t.Run(name, func(t *testing.T) {
			_, err := db.db.ExecContext(ctx, query, args...)
			if err == nil || !strings.Contains(err.Error(), fragment) {
				t.Fatalf("expected %q constraint, got %v", fragment, err)
			}
		})
	}
	expectConstraint("one per ticket", "UNIQUE constraint failed", intentSQL, intent...)
	preparation := []any{id, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, candidate.Snapshot.HeadSHA, newBase, anchor, anchor, candidate.Snapshot.HeadSHA, newBase, digest("e"), "now"}
	// Malformed insertions precede valid rows so a duplicate primary key cannot
	// mask missing lineage/width checks. The other ticket really exists.
	wrongParents := append([]any(nil), preparation...)
	wrongParents[8], wrongParents[9] = newBase, candidate.Snapshot.HeadSHA
	expectConstraint("parent order", "CHECK constraint failed", preparationSQL, wrongParents...)
	wrongWidth := append([]any(nil), preparation...)
	wrongWidth[6], wrongWidth[7] = strings.Repeat("3", 64), strings.Repeat("3", 64)
	expectConstraint("object width", "CHECK constraint failed", preparationSQL, wrongWidth...)
	crossTicket := append([]any(nil), preparation...)
	crossTicket[3] = other.Ticket
	expectConstraint("preparation ticket lineage", "FOREIGN KEY constraint failed", preparationSQL, crossTicket...)
	if _, err := db.db.ExecContext(ctx, preparationSQL, preparation...); err != nil {
		t.Fatal(err)
	}
	completion := []any{id, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, anchor, anchor, candidate.Snapshot.HeadSHA, newBase, newBase, `{}`, digest("f"), current.Version + 1, fence.LeaderEpoch, fence.RunnerEpoch, digest("0"), "now"}
	wrongBase := append([]any(nil), completion...)
	wrongBase[8] = candidate.Snapshot.HeadSHA
	expectConstraint("effective base", "CHECK constraint failed", completionSQL, wrongBase...)
	crossCompletion := append([]any(nil), completion...)
	crossCompletion[3] = other.Ticket
	expectConstraint("completion ticket lineage", "FOREIGN KEY constraint failed", completionSQL, crossCompletion...)
	if _, err := db.db.ExecContext(ctx, completionSQL, completion...); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"protected_base_refresh_intents", "protected_base_refresh_preparations", "protected_base_refresh_completions"} {
		expectConstraint(table+" update", "immutable", "UPDATE "+table+" SET project_id=project_id WHERE refresh_id=?", id)
		expectConstraint(table+" delete", "append-only", "DELETE FROM "+table+" WHERE refresh_id=?", id)
	}
}
