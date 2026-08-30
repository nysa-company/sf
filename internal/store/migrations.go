package store

var migrationV1 = []string{
	`CREATE TABLE IF NOT EXISTS daemon_instances (
		channel TEXT PRIMARY KEY CHECK(channel IN ('stable', 'dev')),
		leader_epoch INTEGER NOT NULL, identity TEXT NOT NULL, updated_at TEXT NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS projects (
		channel TEXT NOT NULL CHECK(channel IN ('stable', 'dev')),
		id TEXT NOT NULL, canonical_path TEXT NOT NULL, base_ref TEXT NOT NULL,
		PRIMARY KEY(channel, id), UNIQUE(channel, canonical_path)
	)`,
	`CREATE TABLE IF NOT EXISTS tickets (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, id TEXT NOT NULL,
		source_digest TEXT NOT NULL, ticket_type TEXT NOT NULL CHECK(ticket_type IN ('bug','feature','refactor','infrastructure','documentation','spike')),
		merge_mode TEXT NOT NULL CHECK(merge_mode IN ('manual','guarded','autonomous')),
		state TEXT NOT NULL CHECK(state IN ('queued','planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling','paused','blocked','done','external_merged','cancelled')),
		resume_state TEXT CHECK(resume_state IS NULL OR resume_state IN ('queued','planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling','paused','blocked')),
		version INTEGER NOT NULL CHECK(version > 0),
		runner_epoch INTEGER NOT NULL, workflow_id TEXT NOT NULL, blocked_code TEXT NOT NULL DEFAULT '',
		PRIMARY KEY(channel, project_id, id),
		FOREIGN KEY(channel, project_id) REFERENCES projects(channel, id)
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS active_ticket_source_digest
		ON tickets(channel, project_id, source_digest)
		WHERE state NOT IN ('done', 'external_merged', 'cancelled')`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ticket_workflow_id
		ON tickets(channel, workflow_id) WHERE workflow_id <> ''`,
	`CREATE TABLE IF NOT EXISTS workflow_owners (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		workflow_id TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id), UNIQUE(channel, workflow_id),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE IF NOT EXISTS phase_runs (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		phase TEXT NOT NULL CHECK(phase IN ('planning','verification','build','publish','review','merge','reconcile')),
		attempt INTEGER NOT NULL CHECK(attempt > 0), state TEXT NOT NULL CHECK(state IN ('active','completed','failed','cancelled')),
		leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, completed_at TEXT,
		PRIMARY KEY(channel, project_id, ticket_id, phase, attempt),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS one_active_phase_per_ticket
		ON phase_runs(channel, project_id, ticket_id) WHERE state = 'active'`,
	`CREATE TABLE IF NOT EXISTS events (
		id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, project_id TEXT NOT NULL,
		ticket_id TEXT NOT NULL, ticket_version INTEGER NOT NULL, trigger TEXT NOT NULL,
		from_state TEXT NOT NULL, to_state TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL,
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE IF NOT EXISTS effects (
		semantic_key TEXT PRIMARY KEY, channel TEXT NOT NULL, project_id TEXT NOT NULL,
		ticket_id TEXT NOT NULL, effect_kind TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('planned','executing','confirmed','uncertain','failed')),
		ticket_version INTEGER NOT NULL, leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL,
		claim_epoch INTEGER NOT NULL, request_digest TEXT NOT NULL, observed_identity TEXT NOT NULL DEFAULT '',
		claimed_at TEXT, FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS active_effect_claim ON effects(channel, project_id, ticket_id, effect_kind)
		WHERE state = 'executing'`,
	`CREATE TABLE IF NOT EXISTS approvals (
		id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, project_id TEXT NOT NULL,
		ticket_id TEXT NOT NULL, reviewed_head TEXT NOT NULL, operator_uid INTEGER NOT NULL,
		decision TEXT NOT NULL CHECK(decision IN ('approved','rejected')), invalidated INTEGER NOT NULL DEFAULT 0 CHECK(invalidated IN (0,1)), created_at TEXT NOT NULL,
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS current_approval_per_head
		ON approvals(channel, project_id, ticket_id, reviewed_head, operator_uid, decision) WHERE invalidated = 0`,
	`CREATE TABLE IF NOT EXISTS worktrees (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		path TEXT NOT NULL, branch_ref TEXT NOT NULL, state TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id), UNIQUE(channel, path), UNIQUE(channel, branch_ref),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE IF NOT EXISTS provider_attempts (
		id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL, project_id TEXT NOT NULL,
		ticket_id TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('planning','verification','build','publish','review','merge','reconcile')), attempt INTEGER NOT NULL CHECK(attempt > 0),
		provider TEXT NOT NULL, model TEXT NOT NULL, family TEXT NOT NULL, version TEXT NOT NULL,
		outcome TEXT NOT NULL, FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id),
		UNIQUE(channel, project_id, ticket_id, phase, attempt, provider)
	)`,
	`CREATE TABLE IF NOT EXISTS leases (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, scope TEXT NOT NULL, scope_key TEXT NOT NULL,
		ticket_id TEXT NOT NULL, runner_epoch INTEGER NOT NULL, acquired_at TEXT NOT NULL,
		PRIMARY KEY(channel, scope, scope_key),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE IF NOT EXISTS plans (channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL, PRIMARY KEY(channel, project_id, ticket_id))`,
	`CREATE TABLE IF NOT EXISTS verifications (channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, intent_digest TEXT NOT NULL, proof_digest TEXT NOT NULL, PRIMARY KEY(channel, project_id, ticket_id))`,
}

// v2 upgrades the first private development schema. Rebuilding the two
// artifact tables is required because SQLite cannot add a foreign key with
// ALTER TABLE. This remains a single transaction, so an interrupted upgrade
// leaves the v1 shape intact.
var migrationV2 = []string{
	`ALTER TABLE schema_migrations ADD COLUMN checksum TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN expected_ticket_version INTEGER NOT NULL DEFAULT 0`,
	`CREATE TABLE plans_v2 (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, digest TEXT NOT NULL, body TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`INSERT INTO plans_v2(channel, project_id, ticket_id, digest, body) SELECT channel, project_id, ticket_id, digest, body FROM plans`,
	`DROP TABLE plans`,
	`ALTER TABLE plans_v2 RENAME TO plans`,
	`CREATE TABLE verifications_v2 (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, intent_digest TEXT NOT NULL, proof_digest TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`INSERT INTO verifications_v2(channel, project_id, ticket_id, intent_digest, proof_digest) SELECT channel, project_id, ticket_id, intent_digest, proof_digest FROM verifications`,
	`DROP TABLE verifications`,
	`ALTER TABLE verifications_v2 RENAME TO verifications`,
}

// v3 preserves original effect ticket identity during recovery and makes a
// current operator decision mutually exclusive for a reviewed ticket head.
var migrationV3 = []string{
	`DROP INDEX current_approval_per_head`,
	`CREATE UNIQUE INDEX current_approval_per_head
		ON approvals(channel, project_id, ticket_id, reviewed_head, operator_uid) WHERE invalidated = 0`,
}

// v4 makes the immutable local Markdown ticket and its parsed display fields
// part of the sole SQLite authority. Empty defaults preserve private dev rows
// created before ticket ingestion was wired.
var migrationV4 = []string{
	`ALTER TABLE tickets ADD COLUMN title TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE tickets ADD COLUMN problem TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE tickets ADD COLUMN acceptance_json TEXT NOT NULL DEFAULT '[]'`,
	`ALTER TABLE tickets ADD COLUMN source_bytes BLOB NOT NULL DEFAULT X''`,
	`ALTER TABLE tickets ADD COLUMN priority TEXT NOT NULL DEFAULT 'normal' CHECK(priority IN ('low','normal','high'))`,
	`ALTER TABLE tickets ADD COLUMN created_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z'`,
}

// v5 persists the optional ticket-local ceilings in integer units. The daemon
// still resolves them against stricter project and machine policy before work.
var migrationV5 = []string{
	`ALTER TABLE tickets ADD COLUMN max_duration_ns INTEGER NOT NULL DEFAULT 0 CHECK(max_duration_ns >= 0)`,
	`ALTER TABLE tickets ADD COLUMN max_cost_micro_usd INTEGER NOT NULL DEFAULT 0 CHECK(max_cost_micro_usd >= 0)`,
}

// v6 makes the operator-facing SF identifier unambiguous within a channel, so
// lifecycle commands never need a project argument to resolve a ticket.
var migrationV6 = []string{
	`CREATE UNIQUE INDEX ticket_channel_id ON tickets(channel, id)`,
}

// v7 adds the fenced evidence authority. Evidence is deliberately normalized:
// large opaque transcripts stay outside SQLite, while bounded typed artifacts,
// identities, and invalidation receipts remain in the sole recovery authority.
var migrationV7 = []string{
	`ALTER TABLE plans ADD COLUMN artifact_bytes BLOB NOT NULL DEFAULT X''`,
	`ALTER TABLE plans ADD COLUMN ticket_version INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE plans ADD COLUMN leader_epoch INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE plans ADD COLUMN runner_epoch INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE plans ADD COLUMN created_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z'`,
	`ALTER TABLE verifications ADD COLUMN current_revision INTEGER NOT NULL DEFAULT 0`,
	`CREATE TABLE verification_revisions (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		revision INTEGER NOT NULL CHECK(revision > 0), ticket_version INTEGER NOT NULL CHECK(ticket_version > 0),
		leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		intent_digest TEXT NOT NULL, intent_bytes BLOB NOT NULL, proof_digest TEXT NOT NULL, proof_bytes BLOB NOT NULL,
		owned_files_json TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
		amends_revision INTEGER, amendment_reason TEXT NOT NULL DEFAULT '', requester TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id, revision),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id),
		CHECK((amends_revision IS NULL AND amendment_reason='' AND requester='') OR (amends_revision IS NOT NULL AND amendment_reason<>'' AND requester<>''))
	)`,
	`CREATE TABLE candidate_snapshots (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		generation INTEGER NOT NULL CHECK(generation > 0), ticket_version INTEGER NOT NULL CHECK(ticket_version > 0),
		leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		base_sha TEXT NOT NULL, head_sha TEXT NOT NULL, tree_sha TEXT NOT NULL, source_digest TEXT NOT NULL,
		verification_intent_digest TEXT NOT NULL, proof_digest TEXT NOT NULL, command_policy_digest TEXT NOT NULL,
		created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id, generation),
		UNIQUE(channel, project_id, ticket_id, head_sha),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE invalidation_receipts (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		generation INTEGER NOT NULL CHECK(generation > 0), kind TEXT NOT NULL CHECK(kind IN ('plan','verification_intent','proof_result','github_checks','final_review','approval')),
		ticket_version INTEGER NOT NULL CHECK(ticket_version > 0), reason TEXT NOT NULL, created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id, generation, kind),
		FOREIGN KEY(channel, project_id, ticket_id, generation) REFERENCES candidate_snapshots(channel, project_id, ticket_id, generation)
	)`,
	`ALTER TABLE phase_runs ADD COLUMN provider TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN model TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN family TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN provider_version TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN worktree_identity TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN base_sha TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN started_at TEXT`,
	`ALTER TABLE phase_runs ADD COLUMN failed_at TEXT`,
	`ALTER TABLE phase_runs ADD COLUMN outcome TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE phase_runs ADD COLUMN usage_json TEXT NOT NULL DEFAULT '{}'`,
	`ALTER TABLE worktrees ADD COLUMN identity_json TEXT NOT NULL DEFAULT '{}'`,
	`ALTER TABLE worktrees ADD COLUMN base_sha TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE worktrees ADD COLUMN head_sha TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE worktrees ADD COLUMN ticket_version INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE worktrees ADD COLUMN leader_epoch INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE worktrees ADD COLUMN runner_epoch INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE approvals ADD COLUMN ticket_version INTEGER NOT NULL DEFAULT 0`,
	`CREATE TABLE ticket_counters (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		kind TEXT NOT NULL CHECK(kind IN ('correction','fallback')), used INTEGER NOT NULL DEFAULT 0 CHECK(used >= 0),
		limit_count INTEGER NOT NULL CHECK((kind='correction' AND limit_count=2) OR (kind='fallback' AND limit_count=1)),
		PRIMARY KEY(channel, project_id, ticket_id, kind),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id), CHECK(used <= limit_count)
	)`,
	`CREATE TABLE ticket_budget_uses (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('correction','fallback')),
		request_id TEXT NOT NULL, ticket_version INTEGER NOT NULL CHECK(ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0), created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id, kind, request_id),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE UNIQUE INDEX verification_revision_identity ON verification_revisions(channel, project_id, ticket_id, intent_digest, proof_digest, checkpoint_id)`,
	`CREATE INDEX candidate_snapshot_current ON candidate_snapshots(channel, project_id, ticket_id, generation DESC)`,
	`CREATE INDEX invalidation_receipt_ticket ON invalidation_receipts(channel, project_id, ticket_id, generation)`,
}

// v8 makes configuration authority durable. A project points at its current
// immutable generation, while a ticket copies the exact canonical bytes and
// digest when queued work first enters planning. Later registration therefore
// cannot change an active ticket's command or provider authority.
var migrationV8 = []string{
	`ALTER TABLE projects ADD COLUMN current_config_generation INTEGER NOT NULL DEFAULT 0 CHECK(current_config_generation >= 0)`,
	`CREATE TABLE project_configurations (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation > 0),
		digest TEXT NOT NULL CHECK(length(digest)=64), snapshot_bytes BLOB NOT NULL CHECK(length(snapshot_bytes) BETWEEN 1 AND 65536), created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, generation), UNIQUE(channel, project_id, digest),
		FOREIGN KEY(channel, project_id) REFERENCES projects(channel, id)
	)`,
	`ALTER TABLE tickets ADD COLUMN config_generation INTEGER NOT NULL DEFAULT 0 CHECK(config_generation >= 0)`,
	`ALTER TABLE tickets ADD COLUMN config_digest TEXT NOT NULL DEFAULT '' CHECK(config_digest='' OR length(config_digest)=64)`,
	`ALTER TABLE tickets ADD COLUMN config_snapshot_bytes BLOB NOT NULL DEFAULT X'' CHECK(length(config_snapshot_bytes) <= 65536)`,
}

// v9 persists the unguessable, channel-prefixed branch identity allocated for
// each ticket. Git receives only this SQLite-backed authority; it never keeps
// an independent branch-name ledger.
var migrationV9 = []string{
	`CREATE TABLE branch_allocations (
		authority_key TEXT PRIMARY KEY, channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		branch_ref TEXT NOT NULL, created_at TEXT NOT NULL,
		UNIQUE(channel, project_id, ticket_id), UNIQUE(channel, branch_ref),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
}

// v10 records exact, sanitized provider qualification verdicts and one
// channel-local independent pair. This is not a per-product release ledger:
// it contains no product, prompt, transcript, filesystem path, or credential.
var migrationV10 = []string{
	`CREATE TABLE provider_qualifications (
		id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT NOT NULL CHECK(channel IN ('stable','dev')),
		run_id TEXT NOT NULL CHECK(length(run_id)=32), provider TEXT NOT NULL CHECK(length(provider) BETWEEN 1 AND 48),
		model TEXT NOT NULL CHECK(length(model) BETWEEN 1 AND 200), family TEXT NOT NULL CHECK(length(family) BETWEEN 1 AND 100),
		provider_version TEXT NOT NULL CHECK(length(provider_version) BETWEEN 1 AND 200),
		binary_digest TEXT NOT NULL CHECK(length(binary_digest)=64), policy_digest TEXT NOT NULL CHECK(length(policy_digest)=64),
		fixture_digest TEXT NOT NULL CHECK(length(fixture_digest)=64),
		profile TEXT NOT NULL CHECK(profile IN ('disabled','qualified_guarded','autonomous_eligible')),
		failed_probes_json TEXT NOT NULL CHECK(length(failed_probes_json) BETWEEN 2 AND 16384),
		reason_code TEXT NOT NULL CHECK(length(reason_code) <= 100), created_at TEXT NOT NULL,
		UNIQUE(channel, run_id),
		CHECK((profile='disabled' AND failed_probes_json<>'[]' AND reason_code<>'') OR (profile<>'disabled' AND failed_probes_json='[]' AND reason_code=''))
	)`,
	`CREATE INDEX provider_qualification_latest ON provider_qualifications(channel, provider, model, family, provider_version, id DESC)`,
	`CREATE TABLE provider_pair_selections (
		channel TEXT PRIMARY KEY CHECK(channel IN ('stable','dev')),
		builder_qualification_id INTEGER NOT NULL, reviewer_qualification_id INTEGER NOT NULL, selected_at TEXT NOT NULL,
		FOREIGN KEY(builder_qualification_id) REFERENCES provider_qualifications(id),
		FOREIGN KEY(reviewer_qualification_id) REFERENCES provider_qualifications(id),
		CHECK(builder_qualification_id <> reviewer_qualification_id)
	)`,
}

// v11 adds fenced, qualification-backed provider attempts. It deliberately
// references the existing qualification authority rather than introducing a
// mutable provider-binding ledger.
var migrationV11 = []string{
	`ALTER TABLE provider_attempts ADD COLUMN role TEXT NOT NULL DEFAULT 'planner' CHECK(role IN ('planner','builder','reviewer'))`,
	`ALTER TABLE provider_attempts ADD COLUMN state TEXT NOT NULL DEFAULT 'completed' CHECK(state IN ('active','completed','failed','cancelled','quarantined'))`,
	`ALTER TABLE provider_attempts ADD COLUMN usage_units INTEGER NOT NULL DEFAULT 0 CHECK(usage_units >= 0)`,
	`ALTER TABLE provider_attempts ADD COLUMN started_at TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE provider_attempts ADD COLUMN finished_at TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE provider_attempts ADD COLUMN qualification_id INTEGER`,
	`ALTER TABLE provider_attempts ADD COLUMN binding_digest TEXT NOT NULL DEFAULT '' CHECK(length(binding_digest) IN (0,64))`,
	`ALTER TABLE provider_attempts ADD COLUMN provider_lease_key TEXT NOT NULL DEFAULT ''`,
	`CREATE UNIQUE INDEX one_active_provider_attempt ON provider_attempts(channel, project_id, ticket_id) WHERE state='active'`,
	`CREATE INDEX provider_attempt_recovery ON provider_attempts(channel, state, started_at)`,
}

// v12 binds each provider attempt to the exact fenced launch that created it.
// The defaults preserve readability of pre-v11 rows; new admission always
// writes non-zero values and validates them before completion.
var migrationV12 = []string{
	`ALTER TABLE provider_attempts ADD COLUMN leader_epoch INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE provider_attempts ADD COLUMN runner_epoch INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE provider_attempts ADD COLUMN expected_ticket_version INTEGER NOT NULL DEFAULT 0`,
}

// v13 binds every runnable provider claim to its durable Git identity and to
// a supervisor verification key. Pre-v13 claims cannot establish either fact,
// so they are explicitly failed/quarantined rather than being resumed.
var migrationV13 = []string{
	`ALTER TABLE provider_pair_selections ADD COLUMN planner_qualification_id INTEGER REFERENCES provider_qualifications(id)`,
	`UPDATE provider_pair_selections SET planner_qualification_id=builder_qualification_id WHERE planner_qualification_id IS NULL`,
	`ALTER TABLE provider_attempts ADD COLUMN repository_path TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE provider_attempts ADD COLUMN worktree_path TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE provider_attempts ADD COLUMN worktree_identity TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE provider_attempts ADD COLUMN base_sha TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE provider_attempts ADD COLUMN supervisor_key BLOB NOT NULL DEFAULT X''`,
	`UPDATE provider_attempts SET state='failed', outcome='legacy_unverifiable', finished_at=CASE WHEN finished_at='' THEN started_at ELSE finished_at END WHERE expected_ticket_version=0 OR qualification_id IS NULL OR binding_digest='' OR leader_epoch=0 OR runner_epoch=0 OR repository_path='' OR worktree_path='' OR worktree_identity='' OR base_sha='' OR length(supervisor_key)<>32`,
	`CREATE TRIGGER provider_attempt_state_outcome_insert BEFORE INSERT ON provider_attempts WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome='completed') OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='quarantined' AND NEW.outcome IN ('undrained','undrained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable'))) BEGIN SELECT RAISE(ABORT,'invalid provider state/outcome'); END`,
	`CREATE TRIGGER provider_attempt_state_outcome_update BEFORE UPDATE OF state,outcome ON provider_attempts WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome='completed') OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='quarantined' AND NEW.outcome IN ('undrained','undrained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable'))) BEGIN SELECT RAISE(ABORT,'invalid provider state/outcome'); END`,
	`CREATE TRIGGER phase_run_state_outcome_update BEFORE UPDATE OF state,outcome ON phase_runs WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome='completed') OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable'))) BEGIN SELECT RAISE(ABORT,'invalid phase state/outcome'); END`,
}

// v14 reserves durable launch lifecycle fields. A launch that dies before its
// PID/PGID is recorded remains unrecoverable and is quarantined, never freed.
var migrationV14 = []string{
	`ALTER TABLE provider_attempts ADD COLUMN launch_state TEXT NOT NULL DEFAULT 'legacy_unverifiable' CHECK(launch_state IN ('launching','released','drained','quarantined','legacy_unverifiable'))`,
	`ALTER TABLE provider_attempts ADD COLUMN process_pid INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE provider_attempts ADD COLUMN process_pgid INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE provider_attempts ADD COLUMN process_started_at TEXT NOT NULL DEFAULT ''`,
	`UPDATE provider_attempts SET launch_state=CASE WHEN state IN ('active','quarantined') THEN 'quarantined' ELSE 'legacy_unverifiable' END`,
}
var migrationV15 = []string{
	`ALTER TABLE daemon_instances ADD COLUMN recovery_public_key BLOB NOT NULL DEFAULT X''`,
}

// v16 adds the OS-observed start identity used to reject a reused PID during
// recovery. Existing active rows deliberately remain active: startup can
// quarantine them without ever signalling an identity it cannot prove.
var migrationV16 = []string{
	`ALTER TABLE provider_attempts ADD COLUMN process_start_identity TEXT NOT NULL DEFAULT ''`,
}

// v17 makes the host boot epoch part of the launch identity. A changed boot
// proves an old process group is dead; a missing value remains fail-closed.
var migrationV17 = []string{
	`ALTER TABLE provider_attempts ADD COLUMN process_boot_identity TEXT NOT NULL DEFAULT ''`,
}

// v18 reconciles the provider runtime's closed phase lifecycle with the
// existing evidence API. Earlier private schemas allowed generic successful
// evidence ("passed") and left control-cancelled rows at outcome="running";
// normalize those rows, then enforce one coherent state/outcome relation for
// every future insert and update.
var migrationV18 = []string{
	`DROP TRIGGER IF EXISTS phase_run_state_outcome_update`,
	`UPDATE phase_runs SET outcome='running' WHERE state='active' AND outcome=''`,
	`UPDATE phase_runs SET outcome='completed' WHERE state='completed' AND outcome=''`,
	`UPDATE phase_runs SET outcome='failed' WHERE state='failed' AND outcome=''`,
	`UPDATE phase_runs SET outcome='cancelled' WHERE state='cancelled' AND outcome IN ('','running')`,
	`CREATE TRIGGER phase_run_state_outcome_insert BEFORE INSERT ON phase_runs WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome IN ('completed','passed')) OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable'))) BEGIN SELECT RAISE(ABORT,'invalid phase state/outcome'); END`,
	`CREATE TRIGGER phase_run_state_outcome_update BEFORE UPDATE OF state,outcome ON phase_runs WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome IN ('completed','passed')) OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable'))) BEGIN SELECT RAISE(ABORT,'invalid phase state/outcome'); END`,
}
