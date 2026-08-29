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
