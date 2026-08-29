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
