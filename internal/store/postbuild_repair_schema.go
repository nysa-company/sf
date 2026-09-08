package store

// v61 records a new pre-publication Builder entry without changing the completed
// predecessor. These structural bindings are not admission authority: the Store
// transition must authenticate the command outcome, retained edits and fences.
var migrationV61 = []string{
	`CREATE TABLE postbuild_repair_entries (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		entry_ticket_version INTEGER NOT NULL CHECK(entry_ticket_version>0),
		consumed_ticket_version INTEGER NOT NULL CHECK(consumed_ticket_version>0 AND entry_ticket_version=consumed_ticket_version+1),
		consumed_leader_epoch INTEGER NOT NULL CHECK(consumed_leader_epoch>0), consumed_runner_epoch INTEGER NOT NULL CHECK(consumed_runner_epoch>0),
		phase TEXT NOT NULL CHECK(phase='build'),
		builder_result_attempt_id INTEGER NOT NULL CHECK(builder_result_attempt_id>0), builder_result_attempt INTEGER NOT NULL CHECK(builder_result_attempt>0),
		builder_result_phase TEXT NOT NULL CHECK(builder_result_phase='build'), builder_result_role TEXT NOT NULL CHECK(builder_result_role='builder'),
		failed_command_semantic_key TEXT NOT NULL CHECK(length(failed_command_semantic_key)>0), failed_command_claim_epoch INTEGER NOT NULL CHECK(failed_command_claim_epoch>0),
		verification_revision INTEGER NOT NULL CHECK(verification_revision>0),
		original_checkpoint_oid TEXT NOT NULL CHECK(length(original_checkpoint_oid) IN (40,64) AND original_checkpoint_oid NOT GLOB '*[^0-9a-f]*'),
		retained_worktree_digest TEXT NOT NULL CHECK(length(retained_worktree_digest)=71 AND substr(retained_worktree_digest,1,7)='sha256:' AND substr(retained_worktree_digest,8) NOT GLOB '*[^0-9a-f]*'),
		failed_result_digest TEXT NOT NULL CHECK(length(failed_result_digest)=71 AND substr(failed_result_digest,1,7)='sha256:' AND substr(failed_result_digest,8) NOT GLOB '*[^0-9a-f]*'),
		builder_typed_digest TEXT NOT NULL CHECK(length(builder_typed_digest)=64 AND builder_typed_digest NOT GLOB '*[^0-9a-f]*'),
		correction_budget_kind TEXT NOT NULL CHECK(correction_budget_kind='correction'), correction_budget_request_id TEXT NOT NULL CHECK(length(correction_budget_request_id)>0),
		binding_digest TEXT NOT NULL CHECK(length(binding_digest)=71 AND substr(binding_digest,1,7)='sha256:' AND substr(binding_digest,8) NOT GLOB '*[^0-9a-f]*'),
		created_at TEXT NOT NULL CHECK(length(created_at) BETWEEN 1 AND 128),
		PRIMARY KEY(channel,project_id,ticket_id,entry_ticket_version),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id),
		FOREIGN KEY(channel,project_id,ticket_id,builder_result_phase,builder_result_role,builder_result_attempt,builder_result_attempt_id) REFERENCES provider_attempts(channel,project_id,ticket_id,phase,role,attempt,id),
		FOREIGN KEY(channel,project_id,ticket_id,builder_result_phase,builder_result_role,builder_result_attempt,builder_result_attempt_id) REFERENCES provider_attempt_results(channel,project_id,ticket_id,phase,role,attempt,provider_attempt_id),
		FOREIGN KEY(failed_command_semantic_key,failed_command_claim_epoch) REFERENCES repository_command_results(semantic_key,claim_epoch),
		FOREIGN KEY(channel,project_id,ticket_id,verification_revision) REFERENCES verification_revisions(channel,project_id,ticket_id,revision),
		FOREIGN KEY(channel,project_id,ticket_id,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch) REFERENCES ticket_budget_uses(channel,project_id,ticket_id,kind,request_id,ticket_version,leader_epoch,runner_epoch),
		FOREIGN KEY(channel,project_id,ticket_id,phase,entry_ticket_version) REFERENCES provider_phase_entries(channel,project_id,ticket_id,phase,entry_ticket_version) DEFERRABLE INITIALLY DEFERRED
	)`,
	`CREATE UNIQUE INDEX postbuild_repair_entries_failed_command ON postbuild_repair_entries(failed_command_semantic_key,failed_command_claim_epoch)`,
	`CREATE UNIQUE INDEX postbuild_repair_entries_binding_digest ON postbuild_repair_entries(binding_digest)`,
	`CREATE TRIGGER postbuild_repair_entries_immutable_update BEFORE UPDATE ON postbuild_repair_entries BEGIN SELECT RAISE(ABORT,'postbuild repair entry is immutable'); END`,
	`CREATE TRIGGER postbuild_repair_entries_immutable_delete BEFORE DELETE ON postbuild_repair_entries BEGIN SELECT RAISE(ABORT,'postbuild repair entry is append-only'); END`,
}
