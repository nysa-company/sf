package store

// This companion belongs to the unshipped v61 migration. Only the dedicated
// amendment request transaction can append it alongside the normative request.
var postbuildAmendmentSchema = []string{
	`CREATE TABLE postbuild_amendment_snapshots (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		amendment_transition_version INTEGER NOT NULL CHECK(amendment_transition_version>1), repair_entry_version INTEGER NOT NULL CHECK(repair_entry_version>1 AND repair_entry_version<amendment_transition_version),
		consumed_ticket_version INTEGER NOT NULL CHECK(consumed_ticket_version+1=amendment_transition_version), consumed_leader_epoch INTEGER NOT NULL CHECK(consumed_leader_epoch>0), consumed_runner_epoch INTEGER NOT NULL CHECK(consumed_runner_epoch>0),
		builder_attempt_id INTEGER NOT NULL CHECK(builder_attempt_id>0), builder_attempt INTEGER NOT NULL CHECK(builder_attempt>0), builder_phase TEXT NOT NULL CHECK(builder_phase='build'), builder_role TEXT NOT NULL CHECK(builder_role='builder'),
		builder_typed_digest TEXT NOT NULL CHECK(length(builder_typed_digest)=64 AND builder_typed_digest NOT GLOB '*[^0-9a-f]*'),
		verification_revision INTEGER NOT NULL CHECK(verification_revision>0), original_checkpoint_oid TEXT NOT NULL CHECK(length(original_checkpoint_oid) IN (40,64) AND original_checkpoint_oid NOT GLOB '*[^0-9a-f]*'),
		full_snapshot_digest TEXT NOT NULL CHECK(length(full_snapshot_digest)=71 AND substr(full_snapshot_digest,1,7)='sha256:' AND substr(full_snapshot_digest,8) NOT GLOB '*[^0-9a-f]*'),
		implementation_digest TEXT NOT NULL CHECK(length(implementation_digest)=71 AND substr(implementation_digest,1,7)='sha256:' AND substr(implementation_digest,8) NOT GLOB '*[^0-9a-f]*'),
		protected_paths_digest TEXT NOT NULL CHECK(length(protected_paths_digest)=71 AND substr(protected_paths_digest,1,7)='sha256:' AND substr(protected_paths_digest,8) NOT GLOB '*[^0-9a-f]*'),
		binding_digest TEXT NOT NULL CHECK(length(binding_digest)=71 AND substr(binding_digest,1,7)='sha256:' AND substr(binding_digest,8) NOT GLOB '*[^0-9a-f]*'), created_at TEXT NOT NULL CHECK(length(created_at) BETWEEN 1 AND 128),
		PRIMARY KEY(channel,project_id,ticket_id,amendment_transition_version),
		FOREIGN KEY(channel,project_id,ticket_id,amendment_transition_version) REFERENCES verification_amendment_requests(channel,project_id,ticket_id,transition_ticket_version),
		FOREIGN KEY(channel,project_id,ticket_id,repair_entry_version) REFERENCES postbuild_repair_entries(channel,project_id,ticket_id,entry_ticket_version),
		FOREIGN KEY(channel,project_id,ticket_id,builder_phase,builder_role,builder_attempt,builder_attempt_id) REFERENCES provider_attempts(channel,project_id,ticket_id,phase,role,attempt,id),
		FOREIGN KEY(channel,project_id,ticket_id,builder_phase,builder_role,builder_attempt,builder_attempt_id) REFERENCES provider_attempt_results(channel,project_id,ticket_id,phase,role,attempt,provider_attempt_id),
		FOREIGN KEY(channel,project_id,ticket_id,verification_revision) REFERENCES verification_revisions(channel,project_id,ticket_id,revision)
	)`,
	`CREATE UNIQUE INDEX postbuild_amendment_snapshots_builder ON postbuild_amendment_snapshots(channel,project_id,ticket_id,builder_attempt_id)`,
	`CREATE UNIQUE INDEX postbuild_amendment_snapshots_binding ON postbuild_amendment_snapshots(binding_digest)`,
	`CREATE TRIGGER postbuild_amendment_snapshots_immutable_update BEFORE UPDATE ON postbuild_amendment_snapshots BEGIN SELECT RAISE(ABORT,'postbuild amendment snapshot is immutable'); END`,
	`CREATE TRIGGER postbuild_amendment_snapshots_immutable_delete BEFORE DELETE ON postbuild_amendment_snapshots BEGIN SELECT RAISE(ABORT,'postbuild amendment snapshot is append-only'); END`,
	`CREATE TABLE postbuild_amendment_checkpoint_snapshots (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		amendment_transition_version INTEGER NOT NULL CHECK(amendment_transition_version>1),
		ticket_version INTEGER NOT NULL CHECK(ticket_version>=amendment_transition_version), leader_epoch INTEGER NOT NULL CHECK(leader_epoch>0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch>0),
		reviewer_attempt_id INTEGER NOT NULL CHECK(reviewer_attempt_id>0), reviewer_attempt INTEGER NOT NULL CHECK(reviewer_attempt>0), reviewer_phase TEXT NOT NULL CHECK(reviewer_phase='verification'), reviewer_role TEXT NOT NULL CHECK(reviewer_role='reviewer'),
		command_semantic_key TEXT NOT NULL, command_claim_epoch INTEGER NOT NULL CHECK(command_claim_epoch>0),
		full_snapshot_digest TEXT NOT NULL CHECK(length(full_snapshot_digest)=71 AND substr(full_snapshot_digest,1,7)='sha256:' AND substr(full_snapshot_digest,8) NOT GLOB '*[^0-9a-f]*'),
		implementation_digest TEXT NOT NULL CHECK(length(implementation_digest)=71 AND substr(implementation_digest,1,7)='sha256:' AND substr(implementation_digest,8) NOT GLOB '*[^0-9a-f]*'),
		companion_binding_digest TEXT NOT NULL CHECK(length(companion_binding_digest)=71 AND substr(companion_binding_digest,1,7)='sha256:' AND substr(companion_binding_digest,8) NOT GLOB '*[^0-9a-f]*'),
		binding_digest TEXT NOT NULL CHECK(length(binding_digest)=71 AND substr(binding_digest,1,7)='sha256:' AND substr(binding_digest,8) NOT GLOB '*[^0-9a-f]*'), created_at TEXT NOT NULL,
		PRIMARY KEY(channel,project_id,ticket_id,amendment_transition_version),
		FOREIGN KEY(channel,project_id,ticket_id,amendment_transition_version) REFERENCES postbuild_amendment_snapshots(channel,project_id,ticket_id,amendment_transition_version),
		FOREIGN KEY(channel,project_id,ticket_id,reviewer_phase,reviewer_role,reviewer_attempt,reviewer_attempt_id) REFERENCES provider_attempt_results(channel,project_id,ticket_id,phase,role,attempt,provider_attempt_id),
		FOREIGN KEY(command_semantic_key,command_claim_epoch) REFERENCES repository_command_results(semantic_key,claim_epoch)
	)`,
	`CREATE TRIGGER postbuild_amendment_checkpoint_snapshots_immutable_update BEFORE UPDATE ON postbuild_amendment_checkpoint_snapshots BEGIN SELECT RAISE(ABORT,'postbuild amendment checkpoint snapshot is immutable'); END`,
	`CREATE TRIGGER postbuild_amendment_checkpoint_snapshots_immutable_delete BEFORE DELETE ON postbuild_amendment_checkpoint_snapshots BEGIN SELECT RAISE(ABORT,'postbuild amendment checkpoint snapshot is append-only'); END`,
}
