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

// v19 keeps structured merge reconciliation evidence and makes cleanup
// uncertainty survive restart rather than trusting an in-memory gate latch.
var migrationV19 = []string{
	`CREATE TABLE merge_intents (
		semantic_key TEXT PRIMARY KEY, channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		request_digest TEXT NOT NULL, ticket_version INTEGER NOT NULL, leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, claim_epoch INTEGER NOT NULL,
		repository_host TEXT NOT NULL, repository_owner TEXT NOT NULL, repository_name TEXT NOT NULL, pull_request_number INTEGER NOT NULL,
		head_oid TEXT NOT NULL, base_ref TEXT NOT NULL, original_base_oid TEXT NOT NULL, protection_rule_id TEXT NOT NULL, strict_status_checks INTEGER NOT NULL CHECK(strict_status_checks IN (0,1)), method TEXT NOT NULL,
		created_at TEXT NOT NULL,
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id),
		FOREIGN KEY(semantic_key) REFERENCES effects(semantic_key)
	)`,
	`CREATE TABLE external_mutation_quarantine (
		singleton INTEGER PRIMARY KEY CHECK(singleton=1), reason TEXT NOT NULL, observed_at TEXT NOT NULL
	)`,
}

var migrationV20 = []string{
	`ALTER TABLE merge_intents ADD COLUMN admin_enforced INTEGER NOT NULL DEFAULT 0 CHECK(admin_enforced IN (0,1))`,
	`ALTER TABLE merge_intents ADD COLUMN active_ruleset_count INTEGER NOT NULL DEFAULT 0 CHECK(active_ruleset_count >= 0)`,
}

// v21 adds the sole durable authority for Git mutations. A mutation intent is
// issued only from an already-planned effect, and its complete claim is
// immutable. Repository exclusion is deliberately not
// time based: a crashed Git child remains held until startup has proved the
// recorded process group drained or quarantined it.
var migrationV21 = []string{
	`CREATE TABLE git_mutation_intents (
		semantic_key TEXT PRIMARY KEY,
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		request_digest TEXT NOT NULL, ticket_version INTEGER NOT NULL,
		leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, claim_epoch INTEGER NOT NULL,
		repository_path TEXT NOT NULL, worktree_path TEXT NOT NULL, branch_ref TEXT NOT NULL,
		operation TEXT NOT NULL, base_ref TEXT NOT NULL, expected_base_oid TEXT NOT NULL, expected_head_oid TEXT NOT NULL,
		created_at TEXT NOT NULL,
		FOREIGN KEY(semantic_key) REFERENCES effects(semantic_key),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE git_mutation_leases (
		repository_path TEXT PRIMARY KEY,
		semantic_key TEXT NOT NULL UNIQUE, nonce BLOB NOT NULL UNIQUE CHECK(length(nonce)=32),
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		request_digest TEXT NOT NULL, ticket_version INTEGER NOT NULL,
		leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, claim_epoch INTEGER NOT NULL,
		worktree_path TEXT NOT NULL, branch_ref TEXT NOT NULL, operation TEXT NOT NULL,
		base_ref TEXT NOT NULL, expected_base_oid TEXT NOT NULL, expected_head_oid TEXT NOT NULL,
		state TEXT NOT NULL CHECK(state IN ('active','quarantined')),
		launch_state TEXT NOT NULL CHECK(launch_state IN ('unrecorded','launching','released','drained','quarantined')),
		process_pid INTEGER NOT NULL DEFAULT 0, process_pgid INTEGER NOT NULL DEFAULT 0,
		process_boot_identity TEXT NOT NULL DEFAULT '', process_start_identity TEXT NOT NULL DEFAULT '',
		acquired_at TEXT NOT NULL, launched_at TEXT NOT NULL DEFAULT '',
		FOREIGN KEY(semantic_key) REFERENCES git_mutation_intents(semantic_key)
	)`,
	`CREATE INDEX git_mutation_lease_recovery ON git_mutation_leases(channel, state, launch_state)`,
}

var migrationV22 = []string{
	`ALTER TABLE provider_attempts ADD COLUMN auth_digest TEXT NOT NULL DEFAULT '' CHECK(length(auth_digest) IN (0,64))`,
	`UPDATE provider_attempts SET state='failed',outcome='legacy_unverifiable',finished_at=CASE WHEN finished_at='' THEN started_at ELSE finished_at END WHERE state IN ('active','quarantined') AND auth_digest=''`,
}

// v23 makes a passing Codex qualification an auditable, supervisor-signed
// observation. Existing rows remain readable but cannot admit a Codex route
// until re-qualified through the current daemon.
var migrationV23 = []string{
	`ALTER TABLE provider_qualifications ADD COLUMN auth_digest TEXT NOT NULL DEFAULT '' CHECK(length(auth_digest) IN (0,64))`,
	`ALTER TABLE provider_qualifications ADD COLUMN probe_digest TEXT NOT NULL DEFAULT '' CHECK(length(probe_digest) IN (0,64))`,
	`ALTER TABLE provider_qualifications ADD COLUMN attested_leader_epoch INTEGER NOT NULL DEFAULT 0 CHECK(attested_leader_epoch >= 0)`,
	`ALTER TABLE provider_qualifications ADD COLUMN attestation_signature BLOB NOT NULL DEFAULT X'' CHECK(length(attestation_signature) IN (0,64))`,
}

// v24 binds the non-secret Codex credential class to both qualification and
// a launched attempt. Existing records deliberately have no mode and therefore
// cannot admit Codex until a current daemon re-qualifies them.
var migrationV24 = []string{
	`ALTER TABLE provider_qualifications ADD COLUMN auth_mode TEXT NOT NULL DEFAULT '' CHECK(length(auth_mode) <= 64)`,
	`ALTER TABLE provider_attempts ADD COLUMN auth_mode TEXT NOT NULL DEFAULT '' CHECK(length(auth_mode) <= 64)`,
}

// v25 is the append-only authority for the complete provider launch input.
// Older active claims cannot prove the prompt/schema/path/profile/timeout
// that was launched, so they are deliberately made unrecoverable.
var migrationV25 = []string{
	`CREATE TABLE provider_attempt_inputs (
		provider_attempt_id INTEGER PRIMARY KEY,
		request_digest TEXT NOT NULL CHECK(length(request_digest)=64),
		canonical_input BLOB NOT NULL CHECK(length(canonical_input) BETWEEN 2 AND 2097152),
		created_at TEXT NOT NULL,
		FOREIGN KEY(provider_attempt_id) REFERENCES provider_attempts(id)
	)`,
	`CREATE TRIGGER provider_attempt_inputs_immutable_update BEFORE UPDATE ON provider_attempt_inputs BEGIN SELECT RAISE(ABORT,'provider attempt input is immutable'); END`,
	`CREATE TRIGGER provider_attempt_inputs_immutable_delete BEFORE DELETE ON provider_attempt_inputs BEGIN SELECT RAISE(ABORT,'provider attempt input is append-only'); END`,
	`UPDATE provider_attempts SET state='failed',outcome='legacy_unverifiable',finished_at=CASE WHEN finished_at='' THEN started_at ELSE finished_at END,launch_state='legacy_unverifiable' WHERE state IN ('active','quarantined')`,
}

// v26 completes the v25 fail-closed upgrade. A provider attempt and its
// phase run are one lifecycle: retaining an active phase after its legacy
// provider claim became unverifiable would permanently block future work.
var migrationV26 = []string{
	`UPDATE phase_runs SET state='failed',completed_at=COALESCE(completed_at,started_at),outcome='legacy_unverifiable'
	 WHERE state='active' AND EXISTS(
		SELECT 1 FROM provider_attempts a WHERE a.channel=phase_runs.channel AND a.project_id=phase_runs.project_id AND a.ticket_id=phase_runs.ticket_id AND a.phase=phase_runs.phase AND a.attempt=phase_runs.attempt AND a.state='failed' AND a.outcome='legacy_unverifiable'
	 )`,
}

// v27 repairs all incomplete v25/v26 provider lifecycles. Only an active
// provider claim with a structurally valid immutable launch input and exact
// phase identity may keep an active phase; every other row is terminal so the
// active-phase unique index cannot deadlock future admission.
var migrationV27 = []string{
	`DROP TRIGGER IF EXISTS provider_attempt_state_outcome_insert`,
	`DROP TRIGGER IF EXISTS provider_attempt_state_outcome_update`,
	`DROP TRIGGER IF EXISTS phase_run_state_outcome_insert`,
	`DROP TRIGGER IF EXISTS phase_run_state_outcome_update`,
	`CREATE TRIGGER provider_attempt_state_outcome_insert BEFORE INSERT ON provider_attempts WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome='completed') OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='quarantined' AND NEW.outcome IN ('undrained','undrained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable','invocation_failed'))) BEGIN SELECT RAISE(ABORT,'invalid provider state/outcome'); END`,
	`CREATE TRIGGER provider_attempt_state_outcome_update BEFORE UPDATE OF state,outcome ON provider_attempts WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome='completed') OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='quarantined' AND NEW.outcome IN ('undrained','undrained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable','invocation_failed'))) BEGIN SELECT RAISE(ABORT,'invalid provider state/outcome'); END`,
	`CREATE TRIGGER phase_run_state_outcome_insert BEFORE INSERT ON phase_runs WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome IN ('completed','passed')) OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable','invocation_failed'))) BEGIN SELECT RAISE(ABORT,'invalid phase state/outcome'); END`,
	`CREATE TRIGGER phase_run_state_outcome_update BEFORE UPDATE OF state,outcome ON phase_runs WHEN NOT ((NEW.state='active' AND NEW.outcome='running') OR (NEW.state='completed' AND NEW.outcome IN ('completed','passed')) OR (NEW.state='cancelled' AND NEW.outcome IN ('cancelled','drained_recovery')) OR (NEW.state='failed' AND NEW.outcome IN ('failed','invalid_artifact','budget_exhausted','legacy_unverifiable','invocation_failed'))) BEGIN SELECT RAISE(ABORT,'invalid phase state/outcome'); END`,
	`UPDATE provider_attempts SET state='failed',outcome='legacy_unverifiable',finished_at=CASE WHEN finished_at='' THEN started_at ELSE finished_at END,launch_state='legacy_unverifiable'
	 WHERE state='active' AND NOT EXISTS(
		SELECT 1 FROM phase_runs p JOIN provider_attempt_inputs i ON i.provider_attempt_id=provider_attempts.id
		WHERE p.channel=provider_attempts.channel AND p.project_id=provider_attempts.project_id AND p.ticket_id=provider_attempts.ticket_id AND p.phase=provider_attempts.phase AND p.attempt=provider_attempts.attempt AND p.state='active'
		AND p.provider=provider_attempts.provider AND p.model=provider_attempts.model AND p.family=provider_attempts.family AND p.provider_version=provider_attempts.version AND p.leader_epoch=provider_attempts.leader_epoch AND p.runner_epoch=provider_attempts.runner_epoch AND p.expected_ticket_version=provider_attempts.expected_ticket_version AND p.worktree_identity=provider_attempts.worktree_identity AND p.base_sha=provider_attempts.base_sha
		AND length(i.request_digest)=64 AND i.request_digest NOT GLOB '*[^0-9a-f]*' AND length(i.canonical_input) BETWEEN 2 AND 2097152 AND json_valid(CAST(i.canonical_input AS TEXT))=1
		AND json_extract(CAST(i.canonical_input AS TEXT),'$.Ticket.Channel')=p.channel AND json_extract(CAST(i.canonical_input AS TEXT),'$.Ticket.Project')=p.project_id AND json_extract(CAST(i.canonical_input AS TEXT),'$.Ticket.Ticket')=p.ticket_id AND json_extract(CAST(i.canonical_input AS TEXT),'$.Phase')=p.phase AND json_extract(CAST(i.canonical_input AS TEXT),'$.Attempt')=p.attempt AND json_extract(CAST(i.canonical_input AS TEXT),'$.LeaderEpoch')=p.leader_epoch AND json_extract(CAST(i.canonical_input AS TEXT),'$.RunnerEpoch')=p.runner_epoch AND json_extract(CAST(i.canonical_input AS TEXT),'$.ExpectedVersion')=p.expected_ticket_version AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Provider')=p.provider AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Model')=p.model AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Family')=p.family AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Version')=p.provider_version
	 )`,
	`UPDATE phase_runs SET state='failed',completed_at=COALESCE(completed_at,started_at),outcome='legacy_unverifiable'
	 WHERE state='active' AND NOT EXISTS(
		SELECT 1 FROM provider_attempts a JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id
		WHERE a.channel=phase_runs.channel AND a.project_id=phase_runs.project_id AND a.ticket_id=phase_runs.ticket_id AND a.phase=phase_runs.phase AND a.attempt=phase_runs.attempt AND a.state='active'
		AND a.provider=phase_runs.provider AND a.model=phase_runs.model AND a.family=phase_runs.family AND a.version=phase_runs.provider_version AND a.leader_epoch=phase_runs.leader_epoch AND a.runner_epoch=phase_runs.runner_epoch AND a.expected_ticket_version=phase_runs.expected_ticket_version AND a.worktree_identity=phase_runs.worktree_identity AND a.base_sha=phase_runs.base_sha
		AND length(i.request_digest)=64 AND i.request_digest NOT GLOB '*[^0-9a-f]*' AND length(i.canonical_input) BETWEEN 2 AND 2097152 AND json_valid(CAST(i.canonical_input AS TEXT))=1
		AND json_extract(CAST(i.canonical_input AS TEXT),'$.Ticket.Channel')=phase_runs.channel AND json_extract(CAST(i.canonical_input AS TEXT),'$.Ticket.Project')=phase_runs.project_id AND json_extract(CAST(i.canonical_input AS TEXT),'$.Ticket.Ticket')=phase_runs.ticket_id AND json_extract(CAST(i.canonical_input AS TEXT),'$.Phase')=phase_runs.phase AND json_extract(CAST(i.canonical_input AS TEXT),'$.Attempt')=phase_runs.attempt AND json_extract(CAST(i.canonical_input AS TEXT),'$.LeaderEpoch')=phase_runs.leader_epoch AND json_extract(CAST(i.canonical_input AS TEXT),'$.RunnerEpoch')=phase_runs.runner_epoch AND json_extract(CAST(i.canonical_input AS TEXT),'$.ExpectedVersion')=phase_runs.expected_ticket_version AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Provider')=phase_runs.provider AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Model')=phase_runs.model AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Family')=phase_runs.family AND json_extract(CAST(i.canonical_input AS TEXT),'$.Provider.Version')=phase_runs.provider_version
	 )`,
	`DELETE FROM leases WHERE scope='provider' AND EXISTS(SELECT 1 FROM provider_attempts a WHERE a.channel=leases.channel AND a.project_id=leases.project_id AND a.ticket_id=leases.ticket_id AND a.runner_epoch=leases.runner_epoch AND a.provider_lease_key=leases.scope_key AND a.state='failed' AND a.outcome='legacy_unverifiable')`,
}

// v28 is a schema marker for the Go-level immutable provider-input audit.
// The audit deliberately runs at every writable Store open: SQLite JSON shape
// predicates cannot prove canonical encoding, SHA-256 authority, or all
// PhaseInput admission semantics.
var migrationV28 = []string{}

// v29 is append-only durable evidence for a successful provider completion.
// Digests are unprefixed, lowercase SHA-256 hex; phaseartifact.Parse exposes a
// historical "sha256:" display digest, so Store deliberately converts it at
// this boundary rather than storing two spellings of the same authority.
var migrationV29 = []string{
	`CREATE TABLE provider_attempt_results (
		provider_attempt_id INTEGER PRIMARY KEY,
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		phase TEXT NOT NULL, role TEXT NOT NULL, attempt INTEGER NOT NULL,
		provider TEXT NOT NULL, model TEXT NOT NULL, family TEXT NOT NULL, provider_version TEXT NOT NULL,
		request_digest TEXT NOT NULL CHECK(length(request_digest)=64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
		leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, expected_ticket_version INTEGER NOT NULL,
		repository_path TEXT NOT NULL, worktree_path TEXT NOT NULL, worktree_identity TEXT NOT NULL, base_sha TEXT NOT NULL,
		raw_artifact BLOB NOT NULL CHECK(length(raw_artifact) BETWEEN 1 AND 1048576), raw_sha256 TEXT NOT NULL CHECK(length(raw_sha256)=64 AND raw_sha256 NOT GLOB '*[^0-9a-f]*'),
		typed_artifact BLOB NOT NULL CHECK(length(typed_artifact) BETWEEN 2 AND 2097152), typed_sha256 TEXT NOT NULL CHECK(length(typed_sha256)=64 AND typed_sha256 NOT GLOB '*[^0-9a-f]*'),
		validation BLOB NOT NULL CHECK(length(validation) BETWEEN 2 AND 65536), validation_sha256 TEXT NOT NULL CHECK(length(validation_sha256)=64 AND validation_sha256 NOT GLOB '*[^0-9a-f]*'),
		transcript_sha256 TEXT NOT NULL CHECK(length(transcript_sha256) IN (0,64) AND transcript_sha256 NOT GLOB '*[^0-9a-f]*'), created_at TEXT NOT NULL,
		FOREIGN KEY(provider_attempt_id) REFERENCES provider_attempts(id),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE INDEX provider_attempt_results_fence ON provider_attempt_results(channel,project_id,ticket_id,phase,attempt,leader_epoch,runner_epoch,expected_ticket_version)`,
	`CREATE TRIGGER provider_attempt_results_immutable_update BEFORE UPDATE ON provider_attempt_results BEGIN SELECT RAISE(ABORT,'provider attempt result is immutable'); END`,
	`CREATE TRIGGER provider_attempt_results_immutable_delete BEFORE DELETE ON provider_attempt_results BEGIN SELECT RAISE(ABORT,'provider attempt result is append-only'); END`,
	// No pre-v29 completion has immutable result evidence.  Retire every such
	// apparent success atomically rather than allowing a restart to consume a
	// model answer it cannot authenticate.
	`UPDATE provider_attempts SET state='failed',outcome='invalid_artifact',launch_state='drained',finished_at=CASE WHEN finished_at='' THEN started_at ELSE finished_at END WHERE state='completed' AND outcome='completed' AND NOT EXISTS(SELECT 1 FROM provider_attempt_results r WHERE r.provider_attempt_id=provider_attempts.id)`,
	`UPDATE phase_runs SET state='failed',outcome='invalid_artifact',completed_at=COALESCE(completed_at,started_at) WHERE state='completed' AND outcome IN ('completed','passed') AND NOT EXISTS(SELECT 1 FROM provider_attempts a JOIN provider_attempt_results r ON r.provider_attempt_id=a.id WHERE a.channel=phase_runs.channel AND a.project_id=phase_runs.project_id AND a.ticket_id=phase_runs.ticket_id AND a.phase=phase_runs.phase AND a.attempt=phase_runs.attempt AND a.provider=phase_runs.provider AND a.model=phase_runs.model AND a.family=phase_runs.family AND a.version=phase_runs.provider_version AND a.leader_epoch=phase_runs.leader_epoch AND a.runner_epoch=phase_runs.runner_epoch AND a.expected_ticket_version=phase_runs.expected_ticket_version AND a.worktree_identity=phase_runs.worktree_identity AND a.base_sha=phase_runs.base_sha AND a.state='completed' AND a.outcome='completed')`,
	`DELETE FROM leases WHERE scope='provider' AND EXISTS(SELECT 1 FROM provider_attempts a WHERE a.channel=leases.channel AND a.project_id=leases.project_id AND a.ticket_id=leases.ticket_id AND a.runner_epoch=leases.runner_epoch AND a.provider_lease_key=leases.scope_key AND a.state='failed' AND a.outcome='invalid_artifact')`,
}

// v30 records whether a guarded merge used classic branch protection or an
// exact repository ruleset, plus the canonical required-check witness for the
// latter. Existing merge intents remain classic and deliberately have no
// ruleset check digest.
var migrationV30 = []string{
	`ALTER TABLE merge_intents ADD COLUMN protection_kind TEXT NOT NULL DEFAULT '' CHECK(protection_kind IN ('','classic','ruleset'))`,
	`ALTER TABLE merge_intents ADD COLUMN protection_checks_digest TEXT NOT NULL DEFAULT ''`,
}

// v31 binds every candidate to the canonical Builder artifact that produced
// it.  Existing candidates intentionally receive the blank default and are
// rejected by readers rather than being retroactively asserted as evidence.
var migrationV31 = []string{
	`ALTER TABLE candidate_snapshots ADD COLUMN builder_evidence_digest TEXT NOT NULL DEFAULT ''`,
}

// v32 preserves the exact observations made immediately before the two Git
// mutations whose outcomes must be reconciled after a restart.  Blank/0 are
// deliberately ambiguous defaults for pre-v32 rows: old activity is never
// retroactively asserted as a recovered fact.
var migrationV32 = []string{
	`ALTER TABLE git_mutation_intents ADD COLUMN prepared_commit_oid TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE git_mutation_intents ADD COLUMN prepared_tree_oid TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE git_mutation_intents ADD COLUMN prior_remote_observed INTEGER NOT NULL DEFAULT 0 CHECK(prior_remote_observed IN (0,1))`,
	`ALTER TABLE git_mutation_intents ADD COLUMN prior_remote_oid TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE git_mutation_leases ADD COLUMN prepared_commit_oid TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE git_mutation_leases ADD COLUMN prepared_tree_oid TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE git_mutation_leases ADD COLUMN prior_remote_observed INTEGER NOT NULL DEFAULT 0 CHECK(prior_remote_observed IN (0,1))`,
	`ALTER TABLE git_mutation_leases ADD COLUMN prior_remote_oid TEXT NOT NULL DEFAULT ''`,
}

// v33 adds the credential-free, guarded repository-command authority.  It is
// intentionally additive: v1-v32 migration bytes are compatibility evidence
// and must never be rewritten.  An active lease is the repository-wide third
// writer class alongside provider and Git mutation leases.
var migrationV33 = []string{
	`CREATE TABLE repository_command_intents (
		semantic_key TEXT PRIMARY KEY, channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		request_digest TEXT NOT NULL, ticket_version INTEGER NOT NULL, leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, claim_epoch INTEGER NOT NULL,
		repository_path TEXT NOT NULL, worktree_path TEXT NOT NULL, worktree_identity TEXT NOT NULL, branch_ref TEXT NOT NULL, base_ref TEXT NOT NULL, base_sha TEXT NOT NULL,
		command_digest TEXT NOT NULL, spec_digest TEXT NOT NULL, policy_digest TEXT NOT NULL, executable_path TEXT NOT NULL, executable_digest TEXT NOT NULL, created_at TEXT NOT NULL,
		FOREIGN KEY(semantic_key) REFERENCES effects(semantic_key), FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id)
	)`,
	`CREATE TABLE repository_command_leases (
		repository_path TEXT PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, nonce BLOB NOT NULL UNIQUE CHECK(length(nonce)=32),
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, request_digest TEXT NOT NULL,
		ticket_version INTEGER NOT NULL, leader_epoch INTEGER NOT NULL, runner_epoch INTEGER NOT NULL, claim_epoch INTEGER NOT NULL,
		worktree_path TEXT NOT NULL, worktree_identity TEXT NOT NULL, branch_ref TEXT NOT NULL, base_ref TEXT NOT NULL, base_sha TEXT NOT NULL,
		command_digest TEXT NOT NULL, spec_digest TEXT NOT NULL, policy_digest TEXT NOT NULL, executable_path TEXT NOT NULL, executable_digest TEXT NOT NULL,
		state TEXT NOT NULL CHECK(state IN ('active','quarantined')),
		launch_state TEXT NOT NULL CHECK(launch_state IN ('unrecorded','released','drained','quarantined')),
		process_pid INTEGER NOT NULL DEFAULT 0, process_pgid INTEGER NOT NULL DEFAULT 0, process_boot_identity TEXT NOT NULL DEFAULT '', process_start_identity TEXT NOT NULL DEFAULT '',
		acquired_at TEXT NOT NULL, launched_at TEXT NOT NULL DEFAULT '', FOREIGN KEY(semantic_key) REFERENCES repository_command_intents(semantic_key)
	)`,
	`CREATE INDEX repository_command_lease_recovery ON repository_command_leases(channel, state, launch_state)`,
	`CREATE TABLE repository_command_process_groups (
		repository_path TEXT NOT NULL, semantic_key TEXT NOT NULL, nonce BLOB NOT NULL,
		process_pid INTEGER NOT NULL, process_pgid INTEGER NOT NULL, process_boot_identity TEXT NOT NULL, process_start_identity TEXT NOT NULL,
		PRIMARY KEY(repository_path, process_pid, process_start_identity),
		FOREIGN KEY(repository_path) REFERENCES repository_command_leases(repository_path) ON DELETE CASCADE
	)`,
	`CREATE INDEX repository_command_process_group_recovery ON repository_command_process_groups(repository_path, semantic_key, nonce)`,
}

// v34 binds reusable reviewer and builder results to durable workflow
// artifacts.  Event payloads are projections only and are never authority.
var migrationV34 = []string{
	`CREATE TABLE plan_result_bindings (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, plan_digest TEXT NOT NULL,
		binding_ticket_version INTEGER NOT NULL CHECK(binding_ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		provider_attempt_id INTEGER NOT NULL CHECK(provider_attempt_id > 0), provider_attempt INTEGER NOT NULL CHECK(provider_attempt > 0),
		PRIMARY KEY(channel,project_id,ticket_id,binding_ticket_version,leader_epoch,runner_epoch),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id),
		FOREIGN KEY(provider_attempt_id) REFERENCES provider_attempt_results(provider_attempt_id)
	)`,
	`CREATE TABLE verification_result_bindings (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, revision INTEGER NOT NULL,
		binding_ticket_version INTEGER NOT NULL CHECK(binding_ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		provider_attempt_id INTEGER NOT NULL CHECK(provider_attempt_id > 0), provider_attempt INTEGER NOT NULL CHECK(provider_attempt > 0),
		checkpoint_commit_oid TEXT NOT NULL CHECK(length(checkpoint_commit_oid) > 0), checkpoint_parent_oid TEXT NOT NULL CHECK(length(checkpoint_parent_oid) > 0), checkpoint_tree_oid TEXT NOT NULL CHECK(length(checkpoint_tree_oid) > 0),
		PRIMARY KEY(channel,project_id,ticket_id,revision,binding_ticket_version,leader_epoch,runner_epoch),
		FOREIGN KEY(channel,project_id,ticket_id,revision) REFERENCES verification_revisions(channel,project_id,ticket_id,revision),
		FOREIGN KEY(provider_attempt_id) REFERENCES provider_attempt_results(provider_attempt_id),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE TABLE candidate_result_bindings (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, generation INTEGER NOT NULL,
		binding_ticket_version INTEGER NOT NULL CHECK(binding_ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		provider_attempt_id INTEGER NOT NULL CHECK(provider_attempt_id > 0), provider_attempt INTEGER NOT NULL CHECK(provider_attempt > 0), commit_parent_oid TEXT NOT NULL CHECK(length(commit_parent_oid) > 0),
		PRIMARY KEY(channel,project_id,ticket_id,generation,binding_ticket_version,leader_epoch,runner_epoch),
		FOREIGN KEY(channel,project_id,ticket_id,generation) REFERENCES candidate_snapshots(channel,project_id,ticket_id,generation),
		FOREIGN KEY(provider_attempt_id) REFERENCES provider_attempt_results(provider_attempt_id),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE TRIGGER plan_result_bindings_immutable_update BEFORE UPDATE ON plan_result_bindings BEGIN SELECT RAISE(ABORT,'plan result binding is append-only'); END`,
	`CREATE TRIGGER plan_result_bindings_immutable_delete BEFORE DELETE ON plan_result_bindings BEGIN SELECT RAISE(ABORT,'plan result binding is append-only'); END`,
	`CREATE TRIGGER verification_result_bindings_immutable_update BEFORE UPDATE ON verification_result_bindings BEGIN SELECT RAISE(ABORT,'verification result binding is append-only'); END`,
	`CREATE TRIGGER verification_result_bindings_immutable_delete BEFORE DELETE ON verification_result_bindings BEGIN SELECT RAISE(ABORT,'verification result binding is append-only'); END`,
	`CREATE TRIGGER candidate_result_bindings_immutable_update BEFORE UPDATE ON candidate_result_bindings BEGIN SELECT RAISE(ABORT,'candidate result binding is append-only'); END`,
	`CREATE TRIGGER candidate_result_bindings_immutable_delete BEFORE DELETE ON candidate_result_bindings BEGIN SELECT RAISE(ABORT,'candidate result binding is append-only'); END`,
}

// v35 is the blank-factory boundary for the v34 workflow-evidence bindings.
// A legacy phase artifact cannot be asserted as a current recovery fact merely
// because its older projection resembles one. Preserve a ticket only when its
// existing phase artifact has the exact completed result binding required by
// v34; otherwise make the operator boundary explicit and require a fresh
// ticket. Each migration version runs in one SQLite write transaction, so the
// blocked state and its explaining event commit together.
var migrationV35 = []string{
	`UPDATE tickets
		SET state='blocked', resume_state=state, blocked_code='legacy_workflow_evidence_unverifiable', version=version+1
		WHERE state='planning' AND EXISTS(
			SELECT 1 FROM plans p
			WHERE p.channel=tickets.channel AND p.project_id=tickets.project_id AND p.ticket_id=tickets.id
			AND NOT EXISTS(
				SELECT 1 FROM plan_result_bindings b
				JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
				JOIN provider_attempts a ON a.id=r.provider_attempt_id
					AND a.attempt=b.provider_attempt
					AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
					AND a.phase='planning' AND a.role='planner' AND a.state='completed' AND a.outcome='completed'
				WHERE b.channel=p.channel AND b.project_id=p.project_id AND b.ticket_id=p.ticket_id AND b.plan_digest=p.digest
			)
		)`,
	`UPDATE tickets
		SET state='blocked', resume_state=state, blocked_code='legacy_workflow_evidence_unverifiable', version=version+1
		WHERE state='verifying' AND (
			NOT EXISTS(
				SELECT 1 FROM plans p
				WHERE p.channel=tickets.channel AND p.project_id=tickets.project_id AND p.ticket_id=tickets.id
				AND EXISTS(
					SELECT 1 FROM plan_result_bindings b
					JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
					JOIN provider_attempts a ON a.id=r.provider_attempt_id
						AND a.attempt=b.provider_attempt
						AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
						AND a.phase='planning' AND a.role='planner' AND a.state='completed' AND a.outcome='completed'
					WHERE b.channel=p.channel AND b.project_id=p.project_id AND b.ticket_id=p.ticket_id AND b.plan_digest=p.digest
				)
			)
			OR EXISTS(
				SELECT 1 FROM verifications v JOIN verification_revisions vr
				ON vr.channel=v.channel AND vr.project_id=v.project_id AND vr.ticket_id=v.ticket_id AND vr.revision=v.current_revision
				WHERE v.channel=tickets.channel AND v.project_id=tickets.project_id AND v.ticket_id=tickets.id
				AND NOT EXISTS(
					SELECT 1 FROM verification_result_bindings b
					JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
					JOIN provider_attempts a ON a.id=r.provider_attempt_id
						AND a.attempt=b.provider_attempt
						AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
						AND a.phase='verification' AND a.role='reviewer' AND a.state='completed' AND a.outcome='completed'
					WHERE b.channel=vr.channel AND b.project_id=vr.project_id AND b.ticket_id=vr.ticket_id AND b.revision=vr.revision
						AND b.checkpoint_commit_oid=vr.checkpoint_id
				)
			)
		)`,
	`UPDATE tickets
		SET state='blocked', resume_state=state, blocked_code='legacy_workflow_evidence_unverifiable', version=version+1
		WHERE state='building' AND (
			NOT EXISTS(
				SELECT 1 FROM plans p
				WHERE p.channel=tickets.channel AND p.project_id=tickets.project_id AND p.ticket_id=tickets.id
				AND EXISTS(
					SELECT 1 FROM plan_result_bindings b
					JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
					JOIN provider_attempts a ON a.id=r.provider_attempt_id
						AND a.attempt=b.provider_attempt
						AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
						AND a.phase='planning' AND a.role='planner' AND a.state='completed' AND a.outcome='completed'
					WHERE b.channel=p.channel AND b.project_id=p.project_id AND b.ticket_id=p.ticket_id AND b.plan_digest=p.digest
				)
			)
			OR
			NOT EXISTS(
				SELECT 1 FROM verifications v JOIN verification_revisions vr
				ON vr.channel=v.channel AND vr.project_id=v.project_id AND vr.ticket_id=v.ticket_id AND vr.revision=v.current_revision
				WHERE v.channel=tickets.channel AND v.project_id=tickets.project_id AND v.ticket_id=tickets.id
				AND EXISTS(
					SELECT 1 FROM verification_result_bindings b
					JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
					JOIN provider_attempts a ON a.id=r.provider_attempt_id
						AND a.attempt=b.provider_attempt
						AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
						AND a.phase='verification' AND a.role='reviewer' AND a.state='completed' AND a.outcome='completed'
					WHERE b.channel=vr.channel AND b.project_id=vr.project_id AND b.ticket_id=vr.ticket_id AND b.revision=vr.revision
						AND b.checkpoint_commit_oid=vr.checkpoint_id
				)
			)
			OR EXISTS(
				SELECT 1 FROM candidate_snapshots c
				WHERE c.channel=tickets.channel AND c.project_id=tickets.project_id AND c.ticket_id=tickets.id
					AND c.generation=(SELECT MAX(latest.generation) FROM candidate_snapshots latest WHERE latest.channel=c.channel AND latest.project_id=c.project_id AND latest.ticket_id=c.ticket_id)
					AND NOT EXISTS(
						SELECT 1 FROM candidate_result_bindings b
						JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
						JOIN provider_attempts a ON a.id=r.provider_attempt_id
							AND a.attempt=b.provider_attempt
							AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
							AND a.phase='build' AND a.role='builder' AND a.state='completed' AND a.outcome='completed'
						WHERE b.channel=c.channel AND b.project_id=c.project_id AND b.ticket_id=c.ticket_id AND b.generation=c.generation
					)
			)
		)`,
	`INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at)
		SELECT channel,project_id,id,version,'typed_blocker',resume_state,'blocked','{"code":"legacy_workflow_evidence_unverifiable","reason":"legacy workflow evidence is unverifiable","next_action":"start a fresh ticket"}',strftime('%Y-%m-%dT%H:%M:%fZ','now')
		FROM tickets
		WHERE state='blocked' AND blocked_code='legacy_workflow_evidence_unverifiable' AND resume_state IN ('planning','verifying','building')`,
}

// v36 makes a repository-command terminal observation durable authority. It
// intentionally does not alter v33's claim/lease rows or v34's provider
// bindings: both are historical compatibility evidence. Result rows copy the
// complete Store-issued claim and are keyed by (semantic_key, claim_epoch), so
// a safe retry cannot silently replace an observation from an earlier claim.
var migrationV36 = []string{
	`CREATE TABLE repository_command_results (
		semantic_key TEXT NOT NULL, claim_epoch INTEGER NOT NULL CHECK(claim_epoch > 0),
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		request_digest TEXT NOT NULL, ticket_version INTEGER NOT NULL CHECK(ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		repository_path TEXT NOT NULL, worktree_path TEXT NOT NULL, worktree_identity TEXT NOT NULL, branch_ref TEXT NOT NULL, base_ref TEXT NOT NULL, base_sha TEXT NOT NULL,
		command_digest TEXT NOT NULL, spec_digest TEXT NOT NULL, policy_digest TEXT NOT NULL, executable_path TEXT NOT NULL, executable_digest TEXT NOT NULL,
		exit_code INTEGER NOT NULL, stdout BLOB NOT NULL CHECK(length(stdout) <= 65536), stderr BLOB NOT NULL CHECK(length(stderr) <= 65536), output_last_message BLOB NOT NULL CHECK(length(output_last_message) <= 1048576),
		stdout_truncated INTEGER NOT NULL CHECK(stdout_truncated IN (0,1)), stderr_truncated INTEGER NOT NULL CHECK(stderr_truncated IN (0,1)), output_last_message_truncated INTEGER NOT NULL CHECK(output_last_message_truncated IN (0,1)),
		duration_ns INTEGER NOT NULL CHECK(duration_ns >= 0), observed_at TEXT NOT NULL,
		stdout_digest TEXT NOT NULL CHECK(length(stdout_digest)=71), stderr_digest TEXT NOT NULL CHECK(length(stderr_digest)=71), output_last_message_digest TEXT NOT NULL CHECK(length(output_last_message_digest)=71), result_digest TEXT NOT NULL CHECK(length(result_digest)=71),
		created_at TEXT NOT NULL,
		PRIMARY KEY(semantic_key,claim_epoch),
		FOREIGN KEY(semantic_key) REFERENCES effects(semantic_key),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE INDEX repository_command_results_ticket ON repository_command_results(channel,project_id,ticket_id,worktree_path,base_sha,created_at)`,
	`CREATE TRIGGER repository_command_results_immutable_update BEFORE UPDATE ON repository_command_results BEGIN SELECT RAISE(ABORT,'repository command result is immutable'); END`,
	`CREATE TRIGGER repository_command_results_immutable_delete BEFORE DELETE ON repository_command_results BEGIN SELECT RAISE(ABORT,'repository command result is append-only'); END`,
	`CREATE TABLE verification_command_result_bindings (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision > 0),
		binding_ticket_version INTEGER NOT NULL CHECK(binding_ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		semantic_key TEXT NOT NULL, claim_epoch INTEGER NOT NULL CHECK(claim_epoch > 0), command_digest TEXT NOT NULL, spec_digest TEXT NOT NULL, policy_digest TEXT NOT NULL, executable_path TEXT NOT NULL, executable_digest TEXT NOT NULL, expected_outcome TEXT NOT NULL CHECK(expected_outcome IN ('red','missing','baseline','dry_run','check_failed','report_ready')),
		PRIMARY KEY(channel,project_id,ticket_id,revision),
		FOREIGN KEY(channel,project_id,ticket_id,revision) REFERENCES verification_revisions(channel,project_id,ticket_id,revision),
		FOREIGN KEY(semantic_key,claim_epoch) REFERENCES repository_command_results(semantic_key,claim_epoch),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE TABLE candidate_command_result_bindings (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation > 0),
		binding_ticket_version INTEGER NOT NULL CHECK(binding_ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		semantic_key TEXT NOT NULL, claim_epoch INTEGER NOT NULL CHECK(claim_epoch > 0), command_digest TEXT NOT NULL, spec_digest TEXT NOT NULL, policy_digest TEXT NOT NULL, executable_path TEXT NOT NULL, executable_digest TEXT NOT NULL,
		PRIMARY KEY(channel,project_id,ticket_id,generation),
		FOREIGN KEY(channel,project_id,ticket_id,generation) REFERENCES candidate_snapshots(channel,project_id,ticket_id,generation),
		FOREIGN KEY(semantic_key,claim_epoch) REFERENCES repository_command_results(semantic_key,claim_epoch),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE TRIGGER verification_command_result_bindings_immutable_update BEFORE UPDATE ON verification_command_result_bindings BEGIN SELECT RAISE(ABORT,'verification command result binding is append-only'); END`,
	`CREATE TRIGGER verification_command_result_bindings_immutable_delete BEFORE DELETE ON verification_command_result_bindings BEGIN SELECT RAISE(ABORT,'verification command result binding is append-only'); END`,
	`CREATE TRIGGER candidate_command_result_bindings_immutable_update BEFORE UPDATE ON candidate_command_result_bindings BEGIN SELECT RAISE(ABORT,'candidate command result binding is append-only'); END`,
	`CREATE TRIGGER candidate_command_result_bindings_immutable_delete BEFORE DELETE ON candidate_command_result_bindings BEGIN SELECT RAISE(ABORT,'candidate command result binding is append-only'); END`,
	// A legacy verification artifact without the new command binding is not
	// equivalent to no artifact. A just-entered verifying ticket with a valid
	// plan and no reviewer artifact remains resumable so it can create fresh
	// command evidence. Building always consumes verification evidence, and a
	// legacy candidate is likewise not promotable.
	`UPDATE tickets SET state='blocked',resume_state=state,blocked_code='legacy_repository_command_evidence_unverifiable',version=version+1
		WHERE state='verifying' AND EXISTS(
			SELECT 1 FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
			WHERE v.channel=tickets.channel AND v.project_id=tickets.project_id AND v.ticket_id=tickets.id
			AND NOT EXISTS(SELECT 1 FROM verification_command_result_bindings b WHERE b.channel=r.channel AND b.project_id=r.project_id AND b.ticket_id=r.ticket_id AND b.revision=r.revision)
		)`,
	`UPDATE tickets SET state='blocked',resume_state=state,blocked_code='legacy_repository_command_evidence_unverifiable',version=version+1
		WHERE state='building' AND (
			NOT EXISTS(
				SELECT 1 FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
				JOIN verification_command_result_bindings b ON b.channel=r.channel AND b.project_id=r.project_id AND b.ticket_id=r.ticket_id AND b.revision=r.revision
				JOIN repository_command_results cr ON cr.semantic_key=b.semantic_key AND cr.claim_epoch=b.claim_epoch
				WHERE v.channel=tickets.channel AND v.project_id=tickets.project_id AND v.ticket_id=tickets.id
					AND ((b.expected_outcome IN ('red','missing','check_failed') AND cr.exit_code<>0) OR (b.expected_outcome IN ('baseline','dry_run','report_ready') AND cr.exit_code=0))
			)
			OR EXISTS(
				SELECT 1 FROM candidate_snapshots c WHERE c.channel=tickets.channel AND c.project_id=tickets.project_id AND c.ticket_id=tickets.id
				AND c.generation=(SELECT MAX(latest.generation) FROM candidate_snapshots latest WHERE latest.channel=c.channel AND latest.project_id=c.project_id AND latest.ticket_id=c.ticket_id)
				AND NOT EXISTS(SELECT 1 FROM candidate_command_result_bindings b JOIN repository_command_results cr ON cr.semantic_key=b.semantic_key AND cr.claim_epoch=b.claim_epoch WHERE b.channel=c.channel AND b.project_id=c.project_id AND b.ticket_id=c.ticket_id AND b.generation=c.generation AND cr.exit_code=0 AND cr.policy_digest=('sha256:' || c.command_policy_digest))
			)
		)`,
	`INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at)
		SELECT channel,project_id,id,version,'typed_blocker',resume_state,'blocked','{"code":"legacy_repository_command_evidence_unverifiable","reason":"legacy repository command evidence is unverifiable","next_action":"start a fresh ticket"}',strftime('%Y-%m-%dT%H:%M:%fZ','now')
		FROM tickets WHERE state='blocked' AND blocked_code='legacy_repository_command_evidence_unverifiable' AND resume_state IN ('verifying','building')`,
}

// v37 records the complete, authenticated handoff from a local candidate to
// the remote draft PR. It is intentionally a single append-only witness per
// candidate generation/head; corrected candidates retain their own history.
// A pre-v37 publishing/waiting ticket has no possible witness in this schema,
// so migration makes that uncertainty explicit and recoverable instead of
// leaving an apparent publish success.
var migrationV37 = []string{
	`CREATE TABLE publication_evidence (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		ticket_version INTEGER NOT NULL CHECK(ticket_version > 0), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > 0),
		candidate_generation INTEGER NOT NULL CHECK(candidate_generation > 0), candidate_ticket_version INTEGER NOT NULL CHECK(candidate_ticket_version > 0), candidate_leader_epoch INTEGER NOT NULL CHECK(candidate_leader_epoch > 0), candidate_runner_epoch INTEGER NOT NULL CHECK(candidate_runner_epoch > 0), candidate_base_sha TEXT NOT NULL, candidate_head_sha TEXT NOT NULL, candidate_tree_sha TEXT NOT NULL,
		candidate_source_digest TEXT NOT NULL CHECK(length(candidate_source_digest)=64), candidate_verification_intent_digest TEXT NOT NULL CHECK(length(candidate_verification_intent_digest)=64), candidate_proof_digest TEXT NOT NULL CHECK(length(candidate_proof_digest)=64), candidate_command_policy_digest TEXT NOT NULL CHECK(length(candidate_command_policy_digest)=64), candidate_builder_evidence_digest TEXT NOT NULL CHECK(length(candidate_builder_evidence_digest)=64),
		candidate_builder_attempt_id INTEGER NOT NULL CHECK(candidate_builder_attempt_id > 0), candidate_builder_attempt INTEGER NOT NULL CHECK(candidate_builder_attempt > 0), candidate_commit_parent_oid TEXT NOT NULL,
		candidate_command_semantic_key TEXT NOT NULL, candidate_command_claim_epoch INTEGER NOT NULL CHECK(candidate_command_claim_epoch > 0), candidate_command_ticket_version INTEGER NOT NULL CHECK(candidate_command_ticket_version > 0), candidate_command_leader_epoch INTEGER NOT NULL CHECK(candidate_command_leader_epoch > 0), candidate_command_runner_epoch INTEGER NOT NULL CHECK(candidate_command_runner_epoch > 0), candidate_command_digest TEXT NOT NULL, candidate_command_spec_digest TEXT NOT NULL, candidate_command_policy_claim_digest TEXT NOT NULL, candidate_command_executable_path TEXT NOT NULL, candidate_command_executable_digest TEXT NOT NULL,
		config_generation INTEGER NOT NULL CHECK(config_generation > 0), config_digest TEXT NOT NULL CHECK(length(config_digest)=64), config_snapshot_digest TEXT NOT NULL CHECK(length(config_snapshot_digest)=64),
		worktree_path TEXT NOT NULL, worktree_branch_ref TEXT NOT NULL, worktree_state TEXT NOT NULL, worktree_ticket_version INTEGER NOT NULL CHECK(worktree_ticket_version > 0), worktree_leader_epoch INTEGER NOT NULL CHECK(worktree_leader_epoch > 0), worktree_runner_epoch INTEGER NOT NULL CHECK(worktree_runner_epoch > 0), worktree_identity_json BLOB NOT NULL, worktree_identity_digest TEXT NOT NULL CHECK(length(worktree_identity_digest)=64), worktree_base_sha TEXT NOT NULL,
		remote_branch_ref TEXT NOT NULL, remote_branch_oid TEXT NOT NULL, remote_base_oid TEXT NOT NULL,
		push_effect_semantic_key TEXT NOT NULL, push_effect_kind TEXT NOT NULL, push_effect_request_digest TEXT NOT NULL, push_effect_claim_epoch INTEGER NOT NULL CHECK(push_effect_claim_epoch > 0), push_effect_observed_identity TEXT NOT NULL,
		github_host TEXT NOT NULL CHECK(github_host='github.com'), github_owner TEXT NOT NULL, github_name TEXT NOT NULL, github_pr_number INTEGER NOT NULL CHECK(github_pr_number > 0), github_head_owner TEXT NOT NULL, github_head_repository TEXT NOT NULL, github_head_ref TEXT NOT NULL, github_head_oid TEXT NOT NULL, github_base_ref TEXT NOT NULL, github_base_oid TEXT NOT NULL, github_state TEXT NOT NULL CHECK(github_state='OPEN'), github_draft INTEGER NOT NULL CHECK(github_draft=1), github_factory_owned INTEGER NOT NULL CHECK(github_factory_owned=1), github_observed_at TEXT NOT NULL,
		pr_effect_semantic_key TEXT NOT NULL, pr_effect_kind TEXT NOT NULL, pr_effect_request_digest TEXT NOT NULL, pr_effect_claim_epoch INTEGER NOT NULL CHECK(pr_effect_claim_epoch > 0), pr_effect_observed_identity TEXT NOT NULL,
		witness_digest TEXT NOT NULL CHECK(length(witness_digest)=71), created_at TEXT NOT NULL,
		PRIMARY KEY(channel, project_id, ticket_id, candidate_generation, candidate_head_sha),
		FOREIGN KEY(channel, project_id, ticket_id) REFERENCES tickets(channel, project_id, id),
		CHECK(remote_branch_ref=worktree_branch_ref), CHECK(remote_branch_oid=candidate_head_sha), CHECK(github_head_oid=candidate_head_sha), CHECK(github_base_oid=remote_base_oid), CHECK(worktree_base_sha=candidate_base_sha), CHECK(remote_base_oid=worktree_base_sha)
	)`,
	`CREATE INDEX publication_evidence_current ON publication_evidence(channel,project_id,ticket_id,candidate_generation DESC,candidate_head_sha)`,
	`CREATE UNIQUE INDEX publication_evidence_witness ON publication_evidence(witness_digest)`,
	`CREATE TRIGGER publication_evidence_immutable_update BEFORE UPDATE ON publication_evidence BEGIN SELECT RAISE(ABORT,'publication evidence is immutable'); END`,
	`CREATE TRIGGER publication_evidence_immutable_delete BEFORE DELETE ON publication_evidence BEGIN SELECT RAISE(ABORT,'publication evidence is append-only'); END`,
	`CREATE TABLE publication_evidence_rebinds (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		candidate_generation INTEGER NOT NULL CHECK(candidate_generation > 0), candidate_head_sha TEXT NOT NULL,
		prior_witness_digest TEXT NOT NULL CHECK(length(prior_witness_digest)=71), prior_ticket_version INTEGER NOT NULL CHECK(prior_ticket_version > 0), prior_leader_epoch INTEGER NOT NULL CHECK(prior_leader_epoch > 0), prior_runner_epoch INTEGER NOT NULL CHECK(prior_runner_epoch > 0),
		ticket_version INTEGER NOT NULL CHECK(ticket_version > prior_ticket_version), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch > prior_runner_epoch),
		rebind_digest TEXT NOT NULL CHECK(length(rebind_digest)=71), created_at TEXT NOT NULL,
		PRIMARY KEY(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,ticket_version),
		FOREIGN KEY(channel,project_id,ticket_id,candidate_generation,candidate_head_sha) REFERENCES publication_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha)
	)`,
	`CREATE UNIQUE INDEX publication_evidence_rebind_digest ON publication_evidence_rebinds(rebind_digest)`,
	`CREATE TRIGGER publication_evidence_rebinds_immutable_update BEFORE UPDATE ON publication_evidence_rebinds BEGIN SELECT RAISE(ABORT,'publication evidence rebind is immutable'); END`,
	`CREATE TRIGGER publication_evidence_rebinds_immutable_delete BEFORE DELETE ON publication_evidence_rebinds BEGIN SELECT RAISE(ABORT,'publication evidence rebind is append-only'); END`,
	`UPDATE tickets SET state='blocked',resume_state=state,blocked_code='legacy_publication_evidence_unverifiable',version=version+1 WHERE state IN ('publishing','waiting_ci')`,
	`INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) SELECT channel,project_id,id,version,'typed_blocker',resume_state,'blocked','{"code":"legacy_publication_evidence_unverifiable","reason":"publication evidence predates the authenticated witness schema","next_action":"reconcile the external publication or start a fresh ticket"}',strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM tickets WHERE state='blocked' AND blocked_code='legacy_publication_evidence_unverifiable' AND resume_state IN ('publishing','waiting_ci')`,
}

// v38 records every durable runner fencing advance. Publication recovery and
// waiting-ci replay consume these rows as the only proof of a +1/+1 recovery;
// a changed ticket counter alone is never sufficient. Existing v37
// publication rows are preserved only when their current state is an exact,
// unadvanced publishing or baseline waiting-ci identity.
var migrationV38 = []string{
	`CREATE TABLE runner_recovery_ledger (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		prior_ticket_version INTEGER NOT NULL CHECK(prior_ticket_version > 0), prior_runner_epoch INTEGER NOT NULL CHECK(prior_runner_epoch > 0), prior_leader_epoch INTEGER NOT NULL CHECK(prior_leader_epoch >= 0),
		ticket_version INTEGER NOT NULL CHECK(ticket_version=prior_ticket_version+1), runner_epoch INTEGER NOT NULL CHECK(runner_epoch=prior_runner_epoch+1), leader_epoch INTEGER NOT NULL CHECK(leader_epoch > 0),
		recovery_digest TEXT NOT NULL CHECK(length(recovery_digest)=71), created_at TEXT NOT NULL,
		PRIMARY KEY(channel,project_id,ticket_id,ticket_version),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE UNIQUE INDEX runner_recovery_ledger_digest ON runner_recovery_ledger(recovery_digest)`,
	`CREATE INDEX runner_recovery_ledger_ticket ON runner_recovery_ledger(channel,project_id,ticket_id,ticket_version)`,
	`CREATE TRIGGER runner_recovery_ledger_immutable_update BEFORE UPDATE ON runner_recovery_ledger BEGIN SELECT RAISE(ABORT,'runner recovery ledger is immutable'); END`,
	`CREATE TRIGGER runner_recovery_ledger_immutable_delete BEFORE DELETE ON runner_recovery_ledger BEGIN SELECT RAISE(ABORT,'runner recovery ledger is append-only'); END`,
	`UPDATE tickets SET state='blocked',resume_state=state,blocked_code='legacy_publication_recovery_unverifiable',version=version+1 WHERE EXISTS(SELECT 1 FROM publication_evidence p WHERE p.channel=tickets.channel AND p.project_id=tickets.project_id AND p.ticket_id=tickets.id) AND NOT EXISTS(SELECT 1 FROM publication_evidence p WHERE p.channel=tickets.channel AND p.project_id=tickets.project_id AND p.ticket_id=tickets.id AND p.candidate_generation=(SELECT MAX(latest.candidate_generation) FROM publication_evidence latest WHERE latest.channel=p.channel AND latest.project_id=p.project_id AND latest.ticket_id=p.ticket_id) AND (SELECT COUNT(*) FROM publication_evidence latest WHERE latest.channel=p.channel AND latest.project_id=p.project_id AND latest.ticket_id=p.ticket_id AND latest.candidate_generation=p.candidate_generation)=1 AND ((tickets.state='publishing' AND tickets.version=p.ticket_version AND tickets.runner_epoch=p.runner_epoch) OR (tickets.state='waiting_ci' AND tickets.version=p.ticket_version+1 AND tickets.runner_epoch=p.runner_epoch AND (SELECT COUNT(*) FROM events e WHERE e.channel=p.channel AND e.project_id=p.project_id AND e.ticket_id=p.ticket_id AND e.ticket_version=p.ticket_version+1 AND e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci')=1 AND NOT EXISTS(SELECT 1 FROM events e WHERE e.channel=p.channel AND e.project_id=p.project_id AND e.ticket_id=p.ticket_id AND e.ticket_version=p.ticket_version+1 AND NOT(e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci')))))`,
	`INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) SELECT channel,project_id,id,version,'typed_blocker',resume_state,'blocked','{"code":"legacy_publication_recovery_unverifiable","reason":"publication recovery advanced without an authenticated runner ledger","next_action":"start a fresh ticket"}',strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM tickets WHERE state='blocked' AND blocked_code='legacy_publication_recovery_unverifiable' AND resume_state IN ('publishing','waiting_ci')`,
}

// v39 repairs the initially shipped publication ledger. v37/v38 witness and
// rebind digests did not cover their creation timestamps, and SQLite cannot
// retrospectively prove them with the Go canonicalizer. There is no safe
// active legacy subset, so fail closed rather than resume a forged publication.
var migrationV39 = []string{
	`ALTER TABLE publication_evidence ADD COLUMN build_transition_created_at TEXT NOT NULL DEFAULT ''`,
	`CREATE TABLE publication_transition_evidence (
		channel TEXT NOT NULL CHECK(channel IN ('stable','dev')), project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		witness_digest TEXT NOT NULL CHECK(length(witness_digest)=71), witness_created_at TEXT NOT NULL,
		ticket_version INTEGER NOT NULL CHECK(ticket_version>0), event_created_at TEXT NOT NULL,
		PRIMARY KEY(channel,project_id,ticket_id,ticket_version),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE UNIQUE INDEX publication_transition_evidence_witness ON publication_transition_evidence(channel,project_id,ticket_id,witness_digest)`,
	`CREATE TRIGGER publication_transition_evidence_immutable_update BEFORE UPDATE ON publication_transition_evidence BEGIN SELECT RAISE(ABORT,'publication transition evidence is immutable'); END`,
	`CREATE TRIGGER publication_transition_evidence_immutable_delete BEFORE DELETE ON publication_transition_evidence BEGIN SELECT RAISE(ABORT,'publication transition evidence is append-only'); END`,
	`UPDATE tickets SET state='blocked',resume_state=state,blocked_code='legacy_publication_timestamp_unverifiable',version=version+1 WHERE state IN ('publishing','waiting_ci')`,
	`INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) SELECT channel,project_id,id,version,'typed_blocker',resume_state,'blocked','{"code":"legacy_publication_timestamp_unverifiable","reason":"publication timestamps predate authenticated digest coverage","next_action":"reconcile the external publication or start a fresh ticket"}',strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM tickets WHERE state='blocked' AND blocked_code='legacy_publication_timestamp_unverifiable' AND resume_state IN ('publishing','waiting_ci')`,
}
