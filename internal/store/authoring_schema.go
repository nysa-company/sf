package store

var migrationV62 = []string{
	`CREATE TABLE authoring_sessions(channel TEXT NOT NULL CHECK(channel IN ('stable','dev')),id TEXT NOT NULL,purpose TEXT NOT NULL CHECK(purpose IN ('ticket_draft','home_intent')),project_id TEXT NOT NULL,capability BLOB NOT NULL,auth_digest TEXT NOT NULL,context_digest TEXT NOT NULL CHECK(length(context_digest)=64),created_at TEXT NOT NULL,PRIMARY KEY(channel,id),FOREIGN KEY(channel,project_id) REFERENCES projects(channel,id))`,
	`CREATE TABLE authoring_turns(channel TEXT NOT NULL,session_id TEXT NOT NULL,turn_key TEXT NOT NULL,turn INTEGER NOT NULL CHECK(turn BETWEEN 1 AND 4),claim BLOB NOT NULL,state TEXT NOT NULL CHECK(state IN ('reserved','launched','uncertain','completed')),launch BLOB NOT NULL DEFAULT X'',outcome TEXT NOT NULL DEFAULT '',result BLOB NOT NULL DEFAULT X'',created_at TEXT NOT NULL,finished_at TEXT NOT NULL DEFAULT '',PRIMARY KEY(channel,session_id,turn_key),UNIQUE(channel,session_id,turn),FOREIGN KEY(channel,session_id) REFERENCES authoring_sessions(channel,id))`,
	`CREATE UNIQUE INDEX authoring_one_undrained_channel ON authoring_turns(channel) WHERE state IN ('reserved','launched','uncertain')`,
	`CREATE TRIGGER authoring_session_immutable BEFORE UPDATE ON authoring_sessions BEGIN SELECT RAISE(ABORT,'authoring session is immutable'); END`,
	`CREATE TRIGGER authoring_turn_identity_immutable BEFORE UPDATE ON authoring_turns WHEN NEW.channel<>OLD.channel OR NEW.session_id<>OLD.session_id OR NEW.turn_key<>OLD.turn_key OR NEW.turn<>OLD.turn OR NEW.claim<>OLD.claim OR (OLD.state='completed') BEGIN SELECT RAISE(ABORT,'authoring claim or completed turn is immutable'); END`,
}
