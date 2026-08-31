package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestStableChannelBacksUpBeforeSchemaMigration(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "stable.sqlite")
	createDatabaseAtVersion(t, path, schemaVersion-1)
	backups := filepath.Join(root, "backups")
	if err := os.Mkdir(backups, 0o700); err != nil {
		t.Fatal(err)
	}

	database, err := OpenChannel(ctx, path, backups, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := rawSchemaVersion(t, path); got != schemaVersion {
		t.Fatalf("migrated schema=%d want=%d", got, schemaVersion)
	}
	files, err := filepath.Glob(filepath.Join(backups, "sf-schema-v*-to-v*.sqlite"))
	if err != nil || len(files) != 1 {
		t.Fatalf("backups=%v err=%v", files, err)
	}
	if got := rawSchemaVersion(t, files[0]); got != schemaVersion-1 {
		t.Fatalf("backup schema=%d want=%d", got, schemaVersion-1)
	}
	info, err := os.Lstat(files[0])
	if err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("backup info=%v err=%v", info, err)
	}
}

func TestDevelopmentMigrationDoesNotTouchStableBackupDirectory(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "dev.sqlite")
	createDatabaseAtVersion(t, path, schemaVersion-1)
	backups := filepath.Join(root, "stable-backups-must-remain-absent")
	database, err := OpenChannel(ctx, path, backups, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := os.Lstat(backups); !os.IsNotExist(err) {
		t.Fatalf("dev migration touched backup directory: %v", err)
	}
}

func TestPrivateSchemaV10UpgradesThroughProviderMigrations(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "dev.sqlite")
	createDatabaseAtVersion(t, path, 10)
	database, err := OpenChannel(ctx, path, filepath.Join(root, "unused-backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if got := rawSchemaVersion(t, path); got != schemaVersion {
		t.Fatalf("migrated schema=%d want=%d", got, schemaVersion)
	}
}

func TestRepositoryCommandV36UpgradeAndRequiredSchema(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "dev.sqlite")
	createDatabaseAtVersion(t, path, 32)
	database, err := OpenChannel(ctx, path, filepath.Join(root, "unused-backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if got := rawSchemaVersion(t, path); got != schemaVersion {
		t.Fatalf("migrated schema=%d want=%d", got, schemaVersion)
	}
	if err := database.validateSchema(ctx); err != nil {
		t.Fatalf("v36 required schema: %v", err)
	}
}

func TestPublicationV37FailsClosedForPopulatedLegacyPublish(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "v36.sqlite")
	createDatabaseAtVersion(t, path, 36)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('stable','legacy-publication','/legacy-publication','main'); INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('stable','legacy-publication','SF-legacy-publication','legacy-source','feature','guarded','publishing',9,3,'legacy-publication-workflow'),('stable','legacy-publication','SF-legacy-waiting','legacy-waiting-source','feature','guarded','waiting_ci',12,4,'legacy-waiting-workflow')`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	backups := filepath.Join(root, "backups")
	if err := os.Mkdir(backups, 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := OpenChannel(ctx, path, backups, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.validateSchema(ctx); err != nil {
		t.Fatalf("v37 required schema after populated upgrade: %v", err)
	}
	for _, legacy := range []struct {
		id, resume string
		version    uint64
	}{
		{id: "SF-legacy-publication", resume: "publishing", version: 10},
		{id: "SF-legacy-waiting", resume: "waiting_ci", version: 13},
	} {
		var state, gotResume, code string
		var version uint64
		if err := database.db.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code,version FROM tickets WHERE channel='stable' AND project_id='legacy-publication' AND id=?`, legacy.id).Scan(&state, &gotResume, &code, &version); err != nil {
			t.Fatal(err)
		}
		if state != "blocked" || gotResume != legacy.resume || code != "legacy_publication_evidence_unverifiable" || version != legacy.version {
			t.Fatalf("legacy publication %s was not failed closed: %s/%s/%s/v%d", legacy.id, state, gotResume, code, version)
		}
		var eventCount int
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel='stable' AND project_id='legacy-publication' AND ticket_id=? AND ticket_version=? AND trigger='typed_blocker' AND from_state=? AND to_state='blocked'`, legacy.id, legacy.version, legacy.resume).Scan(&eventCount); err != nil || eventCount != 1 {
			t.Fatalf("legacy publication %s blocker event count=%d err=%v", legacy.id, eventCount, err)
		}
	}
	files, err := filepath.Glob(filepath.Join(backups, "sf-schema-v036-to-v044-*.sqlite"))
	if err != nil || len(files) != 1 {
		t.Fatalf("v36 populated backup files=%v err=%v", files, err)
	}
	if got := rawSchemaVersion(t, files[0]); got != 36 {
		t.Fatalf("populated backup schema=%d want=36", got)
	}
}

// A v37 database can contain a publication row without any v38 recovery
// ledger. Migration preserves only the exact, unadvanced witness identities;
// every advanced or ambiguous row receives a typed blocker and remains
// recoverable through the pre-migration backup.
func TestPublicationV37ToV38DispositionForPopulatedRows(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "v37.sqlite")
	createDatabaseAtVersion(t, path, 37)
	seedV37PublicationRows(t, path)
	backups := filepath.Join(root, "backups")
	if err := os.Mkdir(backups, 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := OpenChannel(ctx, path, backups, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		id, state, resume, code string
		version                 int
	}{
		{id: "SF-v37-safe-publishing", state: "blocked", resume: "publishing", code: "legacy_publication_timestamp_unverifiable", version: 10},
		{id: "SF-v37-safe-waiting", state: "blocked", resume: "waiting_ci", code: "legacy_publication_timestamp_unverifiable", version: 11},
		{id: "SF-v37-unsafe-advanced", state: "blocked", resume: "publishing", code: "legacy_publication_recovery_unverifiable", version: 12},
	} {
		var state, resume, code string
		var version int
		if err := database.db.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),COALESCE(blocked_code,''),version FROM tickets WHERE channel='stable' AND project_id='v37-disposition' AND id=?`, want.id).Scan(&state, &resume, &code, &version); err != nil {
			t.Fatal(err)
		}
		if state != want.state || resume != want.resume || code != want.code || version != want.version {
			t.Fatalf("%s disposition=%s/%s/%s/v%d want=%s/%s/%s/v%d", want.id, state, resume, code, version, want.state, want.resume, want.code, want.version)
		}
	}
	files, err := filepath.Glob(filepath.Join(backups, "sf-schema-v037-to-v044-*.sqlite"))
	if err != nil || len(files) != 1 {
		t.Fatalf("v37 backup files=%v err=%v", files, err)
	}
	if got := rawSchemaVersion(t, files[0]); got != 37 {
		t.Fatalf("v37 backup schema=%d", got)
	}
}

// seedV37PublicationRows is deliberately a migration fixture, not a
// production setup path. It discovers the v37 columns so the fixture remains
// explicit about every NOT NULL field while avoiding a second production
// serializer. Only the migration's state/version disposition is under test.
func seedV37PublicationRows(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON; INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('stable','v37-disposition','/v37-disposition','main'); INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES ('stable','v37-disposition','SF-v37-safe-publishing','source-publishing','feature','guarded','publishing',9,3,'v37-publishing'),('stable','v37-disposition','SF-v37-safe-waiting','source-waiting','feature','guarded','waiting_ci',10,3,'v37-waiting'),('stable','v37-disposition','SF-v37-unsafe-advanced','source-unsafe','feature','guarded','publishing',11,4,'v37-unsafe')`); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(publication_evidence)`)
	if err != nil {
		t.Fatal(err)
	}
	var columns []string
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	argsFor := func(ticket string, version, runner int) []any {
		args := make([]any, len(columns))
		marker := fmt.Sprintf("%x", sha256.Sum256([]byte(ticket)))
		hex := marker[:40]
		for i, column := range columns {
			switch {
			case column == "channel":
				args[i] = "stable"
			case column == "project_id":
				args[i] = "v37-disposition"
			case column == "ticket_id":
				args[i] = ticket
			case column == "ticket_version":
				args[i] = version
			case column == "runner_epoch":
				args[i] = runner
			case column == "leader_epoch":
				args[i] = 1
			case column == "candidate_generation", column == "candidate_ticket_version", column == "candidate_leader_epoch", column == "candidate_runner_epoch":
				args[i] = 1
			case strings.HasSuffix(column, "attempt_id"):
				args[i] = 1
			case strings.HasSuffix(column, "attempt"), strings.HasSuffix(column, "claim_epoch"), column == "github_pr_number":
				args[i] = 1
			case column == "github_draft", column == "github_factory_owned":
				args[i] = 1
			case column == "github_host":
				args[i] = "github.com"
			case column == "github_state":
				args[i] = "OPEN"
			case column == "github_observed_at", column == "created_at":
				args[i] = time.Now().UTC().Format(time.RFC3339Nano)
			case column == "worktree_identity_json":
				args[i] = []byte(`{}`)
			case strings.Contains(column, "sha") || strings.Contains(column, "oid"):
				args[i] = hex
			case strings.Contains(column, "digest"):
				if column == "witness_digest" || column == "prior_witness_digest" || column == "rebind_digest" {
					args[i] = "sha256:" + marker
				} else {
					args[i] = strings.Repeat("b", 64)
				}
			default:
				args[i] = "v37"
			}
		}
		return args
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(columns)), ",")
	insert := fmt.Sprintf("INSERT INTO publication_evidence(%s) VALUES(%s)", strings.Join(columns, ","), placeholders)
	for _, row := range []struct {
		id   string
		v, r int
	}{{"SF-v37-safe-publishing", 9, 3}, {"SF-v37-safe-waiting", 9, 3}, {"SF-v37-unsafe-advanced", 9, 3}} {
		args := argsFor(row.id, row.v, row.r)
		if _, err := db.ExecContext(ctx, insert, args...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES('stable','v37-disposition','SF-v37-safe-waiting',10,'effects_confirmed','publishing','waiting_ci','{}',?)`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}

func TestV35BlocksOnlyUnboundLegacyWorkflowArtifacts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v34.sqlite")
	createDatabaseAtVersion(t, path, 34)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA foreign_keys=ON; INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('dev','v35','/v35','main')`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	for _, fixture := range []struct {
		id    string
		state string
		bound bool
		fresh bool
	}{
		{id: "SF-v35-plan-legacy", state: "planning"},
		{id: "SF-v35-verify-legacy", state: "verifying"},
		{id: "SF-v35-build-legacy", state: "building"},
		{id: "SF-v35-verify-no-artifacts", state: "verifying", fresh: true},
		{id: "SF-v35-plan-bound", state: "planning", bound: true},
		{id: "SF-v35-verify-bound", state: "verifying", bound: true},
		{id: "SF-v35-build-bound", state: "building", bound: true},
		{id: "SF-v35-plan-fresh", state: "planning", bound: true},
	} {
		seedV35WorkflowFixture(t, raw, fixture.id, fixture.state, fixture.bound, fixture.fresh || strings.HasSuffix(fixture.id, "-fresh"))
	}
	seedV35WorkflowFixture(t, raw, "SF-v35-verify-plan-bound", "planning", true, false)
	if _, err := raw.Exec(`UPDATE tickets SET state='verifying' WHERE channel='dev' AND project_id='v35' AND id='SF-v35-verify-plan-bound'`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	seedV35WorkflowFixture(t, raw, "SF-v35-build-no-candidate", "verifying", true, false)
	if _, err := raw.Exec(`UPDATE tickets SET state='building' WHERE channel='dev' AND project_id='v35' AND id='SF-v35-build-no-candidate'`); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	database, err := OpenChannel(ctx, path, filepath.Join(t.TempDir(), "backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if got := rawSchemaVersion(t, path); got != schemaVersion {
		t.Fatalf("migrated schema=%d want=%d", got, schemaVersion)
	}
	for _, legacy := range []struct{ id, state string }{
		{id: "SF-v35-plan-legacy", state: "planning"},
		{id: "SF-v35-verify-legacy", state: "verifying"},
		{id: "SF-v35-build-legacy", state: "building"},
		{id: "SF-v35-verify-no-artifacts", state: "verifying"},
	} {
		id, state := legacy.id, legacy.state
		var gotState, resume, code, payload string
		var version int
		if err := database.db.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code,version FROM tickets WHERE channel='dev' AND project_id='v35' AND id=?`, id).Scan(&gotState, &resume, &code, &version); err != nil {
			t.Fatal(err)
		}
		if gotState != "blocked" || resume != state || code != "legacy_workflow_evidence_unverifiable" || version != 2 {
			t.Fatalf("legacy %s ticket=%s/%s/%s/v%d", state, gotState, resume, code, version)
		}
		if err := database.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel='dev' AND project_id='v35' AND ticket_id=? AND trigger='typed_blocker' AND from_state=? AND to_state='blocked'`, id, state).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if payload != `{"code":"legacy_workflow_evidence_unverifiable","reason":"legacy workflow evidence is unverifiable","next_action":"start a fresh ticket"}` {
			t.Fatalf("legacy %s payload=%s", state, payload)
		}
		var eventCount int
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel='dev' AND project_id='v35' AND ticket_id=? AND trigger='typed_blocker'`, id).Scan(&eventCount); err != nil || eventCount != 1 {
			t.Fatalf("legacy %s blocker events=%d err=%v", state, eventCount, err)
		}
	}
	for _, id := range []string{"SF-v35-plan-bound", "SF-v35-plan-fresh", "SF-v35-verify-plan-bound"} {
		var state, resume, code string
		var version int
		if err := database.db.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code,version FROM tickets WHERE channel='dev' AND project_id='v35' AND id=?`, id).Scan(&state, &resume, &code, &version); err != nil {
			t.Fatal(err)
		}
		if state == "blocked" || resume != "" || code != "" || version != 1 {
			t.Fatalf("bound/fresh ticket %s changed to %s/%s/%s/v%d", id, state, resume, code, version)
		}
		var eventCount int
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel='dev' AND project_id='v35' AND ticket_id=? AND trigger='typed_blocker'`, id).Scan(&eventCount); err != nil || eventCount != 0 {
			t.Fatalf("bound/fresh ticket %s blocker events=%d err=%v", id, eventCount, err)
		}
	}
	for _, legacy := range []struct{ id, resume string }{
		{id: "SF-v35-verify-bound", resume: "verifying"},
		{id: "SF-v35-build-bound", resume: "building"},
		{id: "SF-v35-build-no-candidate", resume: "building"},
	} {
		var state, resume, code, payload string
		var version, count int
		if err := database.db.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code,version FROM tickets WHERE channel='dev' AND project_id='v35' AND id=?`, legacy.id).Scan(&state, &resume, &code, &version); err != nil {
			t.Fatal(err)
		}
		if state != "blocked" || resume != legacy.resume || code != "legacy_repository_command_evidence_unverifiable" || version != 2 {
			t.Fatalf("v36 command-evidence blocker %s=%s/%s/%s/v%d", legacy.id, state, resume, code, version)
		}
		if err := database.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel='dev' AND project_id='v35' AND ticket_id=? AND trigger='typed_blocker' AND from_state=? AND to_state='blocked'`, legacy.id, legacy.resume).Scan(&payload); err != nil || payload != `{"code":"legacy_repository_command_evidence_unverifiable","reason":"legacy repository command evidence is unverifiable","next_action":"start a fresh ticket"}` {
			t.Fatalf("v36 command-evidence event %s=%q err=%v", legacy.id, payload, err)
		}
		if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel='dev' AND project_id='v35' AND ticket_id=? AND trigger='typed_blocker'`, legacy.id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("v36 command-evidence events %s=%d err=%v", legacy.id, count, err)
		}
	}
}

func seedV35WorkflowFixture(t *testing.T, raw *sql.DB, id, state string, bound, fresh bool) {
	t.Helper()
	if _, err := raw.Exec(`INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('dev','v35',?,?, 'feature','guarded',?,1,1,?)`, id, "source/"+id, state, "v35/"+id); err != nil {
		t.Fatal(err)
	}
	if fresh {
		return
	}
	insertResult := func(phase, role string, attempt int) int64 {
		result, err := raw.Exec(`INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha) VALUES('dev','v35',?,?,?,?,?,?,'1','completed',?,'completed',0,'now','now',1,1,1,'/v35','/v35/worktree','identity','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`, id, phase, attempt, "provider", "model", "family", role)
		if err != nil {
			t.Fatal(err)
		}
		attemptID, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		if _, err := raw.Exec(`INSERT INTO provider_attempt_results(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,provider,model,family,provider_version,request_digest,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,raw_artifact,raw_sha256,typed_artifact,typed_sha256,validation,validation_sha256,transcript_sha256,created_at) VALUES(?,'dev','v35',?,?,?,?,'provider','model','family','1',?,1,1,1,'/v35','/v35/worktree','identity','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',X'7B7D',?,X'7B7D',?,X'7B7D',?,'','now')`, attemptID, id, phase, role, attempt, digest, digest, digest, digest); err != nil {
			t.Fatal(err)
		}
		return attemptID
	}
	insertPlan := func(withBinding bool) {
		if _, err := raw.Exec(`INSERT INTO plans(channel,project_id,ticket_id,digest,body,artifact_bytes,ticket_version,leader_epoch,runner_epoch,created_at) VALUES('dev','v35',?,'plan','{}',X'7B7D',1,1,1,'now')`, id); err != nil {
			t.Fatal(err)
		}
		if withBinding {
			attemptID := insertResult("planning", "planner", 1)
			if _, err := raw.Exec(`INSERT INTO plan_result_bindings(channel,project_id,ticket_id,plan_digest,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt) VALUES('dev','v35',?,'plan',1,1,1,?,1)`, id, attemptID); err != nil {
				t.Fatal(err)
			}
		}
	}
	insertVerification := func(withReviewerBinding bool) {
		const checkpoint = "cccccccccccccccccccccccccccccccccccccccc"
		if _, err := raw.Exec(`INSERT INTO verification_revisions(channel,project_id,ticket_id,revision,ticket_version,leader_epoch,runner_epoch,intent_digest,intent_bytes,proof_digest,proof_bytes,owned_files_json,checkpoint_id,amends_revision,amendment_reason,requester,created_at) VALUES('dev','v35',?,1,1,1,1,'intent',X'7B7D','proof',X'7B7D','[]',?,NULL,'','','now'); INSERT INTO verifications(channel,project_id,ticket_id,intent_digest,proof_digest,current_revision) VALUES('dev','v35',?,'intent','proof',1)`, id, checkpoint, id); err != nil {
			t.Fatal(err)
		}
		if withReviewerBinding {
			attemptID := insertResult("verification", "reviewer", 1)
			if _, err := raw.Exec(`INSERT INTO verification_result_bindings(channel,project_id,ticket_id,revision,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,checkpoint_commit_oid,checkpoint_parent_oid,checkpoint_tree_oid) VALUES('dev','v35',?,1,1,1,1,?,1,?,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')`, id, attemptID, checkpoint); err != nil {
				t.Fatal(err)
			}
		}
	}
	switch state {
	case "planning":
		insertPlan(bound)
	case "verifying":
		insertPlan(true)
		insertVerification(bound)
	case "building":
		// A building ticket requires a valid reviewer binding before the
		// candidate's builder binding is considered. Seed that prerequisite
		// for both fixtures so the unbound case isolates the missing builder
		// evidence.
		insertPlan(true)
		insertVerification(true)
		if _, err := raw.Exec(`INSERT INTO candidate_snapshots(channel,project_id,ticket_id,generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest,created_at) VALUES('dev','v35',?,1,1,1,1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','dddddddddddddddddddddddddddddddddddddddd','source','intent','proof','policy','builder','now')`, id); err != nil {
			t.Fatal(err)
		}
		if bound {
			attemptID := insertResult("build", "builder", 2)
			if _, err := raw.Exec(`INSERT INTO candidate_result_bindings(channel,project_id,ticket_id,generation,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,commit_parent_oid) VALUES('dev','v35',?,1,1,1,1,?,2,'cccccccccccccccccccccccccccccccccccccccc')`, id, attemptID); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestV26ClosesPhaseRunForV25LegacyProviderClaim(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v24.sqlite")
	createDatabaseAtVersion(t, path, 24)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.ExecContext(ctx, `INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('dev','legacy','/legacy','main'); INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('dev','legacy','SF-v26','source','feature','guarded','building',1,1,'legacy-v26'); INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,outcome,started_at) VALUES('dev','legacy','SF-v26','build',1,'active',1,1,1,'running','now'); INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,started_at) VALUES('dev','legacy','SF-v26','build',1,'legacy','model','family','1','running','builder','active','now')`)
	if err != nil {
		_ = raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := OpenChannel(ctx, path, filepath.Join(t.TempDir(), "backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var providerState, phaseState, phaseOutcome string
	if err := database.db.QueryRowContext(ctx, `SELECT state FROM provider_attempts WHERE project_id='legacy' AND ticket_id='SF-v26'`).Scan(&providerState); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRowContext(ctx, `SELECT state,outcome FROM phase_runs WHERE project_id='legacy' AND ticket_id='SF-v26'`).Scan(&phaseState, &phaseOutcome); err != nil {
		t.Fatal(err)
	}
	if providerState != "failed" || phaseState != "failed" || phaseOutcome != "legacy_unverifiable" {
		t.Fatalf("legacy lifecycle was left active: provider=%s phase=%s/%s", providerState, phaseState, phaseOutcome)
	}
}

func TestV27ReconcilesOrphanAndMismatchedActivePhaseRuns(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		version  int
		provider bool
		mismatch bool
	}{
		{name: "v24 orphan", version: 24},
		{name: "v25 mismatched provider", version: 25, provider: true, mismatch: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.sqlite")
			createDatabaseAtVersion(t, path, scenario.version)
			seedMigrationPhase(t, path, scenario.provider, scenario.mismatch)
			database, err := OpenChannel(context.Background(), path, filepath.Join(t.TempDir(), "backups"), domain.ChannelDev)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			var state, outcome string
			if err := database.db.QueryRow(`SELECT state,outcome FROM phase_runs WHERE channel='dev' AND project_id='migration' AND ticket_id='SF-v27' AND attempt=1`).Scan(&state, &outcome); err != nil || state != "failed" || outcome != "legacy_unverifiable" {
				t.Fatalf("unprovable phase remained active: %s/%s err=%v", state, outcome, err)
			}
			if _, err := database.db.Exec(`INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,provider,model,family,provider_version,worktree_identity,base_sha,started_at,outcome) VALUES('dev','migration','SF-v27','build',2,'active',1,1,1,'provider','model','family','1','identity','` + strings.Repeat("a", 40) + `','now','running')`); err != nil {
				t.Fatalf("closed orphan still blocked subsequent phase attempt: %v", err)
			}
		})
	}
}

func TestV27PreservesProvableV25ActiveAttempt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	createDatabaseAtVersion(t, path, 25)
	seedMigrationPhase(t, path, true, false)
	database, err := OpenChannel(context.Background(), path, filepath.Join(t.TempDir(), "backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var phaseState, attemptState string
	if err := database.db.QueryRow(`SELECT state FROM phase_runs WHERE channel='dev' AND project_id='migration' AND ticket_id='SF-v27'`).Scan(&phaseState); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRow(`SELECT state FROM provider_attempts WHERE channel='dev' AND project_id='migration' AND ticket_id='SF-v27'`).Scan(&attemptState); err != nil {
		t.Fatal(err)
	}
	if phaseState != "active" || attemptState != "active" {
		t.Fatalf("provable v25 claim was not preserved: phase=%s attempt=%s", phaseState, attemptState)
	}
}

func seedMigrationPhase(t *testing.T, path string, provider, mismatch bool) {
	t.Helper()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	base := strings.Repeat("a", 40)
	if _, err := raw.Exec(`INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('dev','migration','/migration','main'); INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('dev','migration','SF-v27','source','feature','guarded','building',1,1,'migration-v27'); INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,provider,model,family,provider_version,worktree_identity,base_sha,started_at,outcome) VALUES('dev','migration','SF-v27','build',1,'active',1,1,1,'provider','model','family','1','identity','` + base + `','now','running')`); err != nil {
		t.Fatal(err)
	}
	if !provider {
		return
	}
	model := "model"
	if mismatch {
		model = "other-model"
	}
	binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "provider", Model: model, Family: "family", Version: "1"}, BinaryDigest: strings.Repeat("d", 64), PolicyDigest: strings.Repeat("e", 64), FixtureDigest: strings.Repeat("f", 64), AuthDigest: strings.Repeat("b", 64)}
	if _, err := raw.Exec(`INSERT INTO provider_qualifications(channel,run_id,provider,model,family,provider_version,binary_digest,policy_digest,fixture_digest,profile,failed_probes_json,reason_code,created_at,auth_digest,probe_digest,auth_mode) VALUES('dev','11111111111111111111111111111111','provider',?,'family','1',?,?,?,'qualified_guarded','[]','','now',?,'','')`, model, binding.BinaryDigest, binding.PolicyDigest, binding.FixtureDigest, binding.AuthDigest); err != nil {
		t.Fatal(err)
	}
	result, err := raw.Exec(`INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,started_at,qualification_id,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,supervisor_key,auth_digest,auth_mode,binding_digest,provider_lease_key,launch_state) VALUES('dev','migration','SF-v27','build',1,'provider','` + model + `','family','1','running','builder','active','now',1,1,1,1,'/migration','/migration','identity','` + base + `',X'0102030405060708091011121314151617181920212223242526272829303132','` + binding.AuthDigest + `','','` + bindingDigest(binding) + `','provider/key','launching')`)
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	input := contracts.PhaseInput{Ticket: domain.TicketRef{Channel: domain.ChannelDev, Project: "migration", Ticket: "SF-v27"}, Phase: domain.PhaseBuild, Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, Prompt: "migration", Repository: "/migration", Worktree: "/migration", WorktreeIdentity: "identity", BaseSHA: base, AllowedPaths: []string{"."}, Provider: domain.ProviderIdentity{Provider: "provider", Model: model, Family: "family", Version: "1"}, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}
	payload, digest, err := contracts.CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO provider_attempt_inputs(provider_attempt_id,request_digest,canonical_input,created_at) VALUES(?,?,?,'now')`, id, digest, payload); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`INSERT INTO leases(channel,project_id,scope,scope_key,ticket_id,runner_epoch,acquired_at) VALUES('dev','migration','provider','provider/key','SF-v27',1,'now')`); err != nil {
		t.Fatal(err)
	}
}

func TestV28ReconcilesInvalidV25CanonicalInputsBeforeAnyRecovery(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB)
	}{
		{"wrong digest", func(t *testing.T, raw *sql.DB) {
			if _, err := raw.Exec(`DROP TRIGGER provider_attempt_inputs_immutable_update; UPDATE provider_attempt_inputs SET request_digest=?`, strings.Repeat("0", 64)); err != nil {
				t.Fatal(err)
			}
		}},
		{"noncanonical bytes", func(t *testing.T, raw *sql.DB) {
			var payload []byte
			if err := raw.QueryRow(`SELECT canonical_input FROM provider_attempt_inputs`).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			payload = append([]byte(" \n"), payload...)
			sum := sha256.Sum256(payload)
			if _, err := raw.Exec(`DROP TRIGGER provider_attempt_inputs_immutable_update; UPDATE provider_attempt_inputs SET canonical_input=?,request_digest=?`, payload, fmt.Sprintf("%x", sum)); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing prompt", func(t *testing.T, raw *sql.DB) {
			var payload []byte
			if err := raw.QueryRow(`SELECT canonical_input FROM provider_attempt_inputs`).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			input, err := contracts.DecodeCanonicalPhaseInput(payload)
			if err != nil {
				t.Fatal(err)
			}
			input.Prompt = ""
			payload, digest, err := contracts.CanonicalPhaseInput(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := raw.Exec(`DROP TRIGGER provider_attempt_inputs_immutable_update; UPDATE provider_attempt_inputs SET canonical_input=?,request_digest=?`, payload, digest); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.sqlite")
			createDatabaseAtVersion(t, path, 25)
			seedMigrationPhase(t, path, true, false)
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, raw)
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}
			database, err := Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			var attemptState, providerOutcome, phaseState, phaseOutcome string
			if err := database.db.QueryRow(`SELECT state,outcome FROM provider_attempts WHERE ticket_id='SF-v27'`).Scan(&attemptState, &providerOutcome); err != nil {
				t.Fatal(err)
			}
			if err := database.db.QueryRow(`SELECT state,outcome FROM phase_runs WHERE ticket_id='SF-v27'`).Scan(&phaseState, &phaseOutcome); err != nil {
				t.Fatal(err)
			}
			if attemptState != "failed" || providerOutcome != "legacy_unverifiable" || phaseState != "failed" || phaseOutcome != "legacy_unverifiable" {
				t.Fatalf("invalid input was not terminal: provider=%s/%s phase=%s/%s", attemptState, providerOutcome, phaseState, phaseOutcome)
			}
			var leases int
			if err := database.db.QueryRow(`SELECT COUNT(*) FROM leases WHERE scope='provider' AND scope_key='provider/key'`).Scan(&leases); err != nil || leases != 0 {
				t.Fatalf("unlaunched invalid claim retained lease=%d err=%v", leases, err)
			}
			// The released capacity is observable through actual Store admission,
			// not merely by inspecting the lease table.
			digest := setupProviderProject(t, database, context.Background())
			leader, _ := database.AcquireLeader(context.Background(), domain.ChannelDev, "v28-test")
			ticket := setupProviderTicket(t, database, context.Background(), "SF-v28-next", leader)
			builder, _ := setupProviderPair(t, database, context.Background())
			ticket = providerState(t, database, context.Background(), ticket, leader, domain.StateBuilding)
			if _, err := database.BeginProviderAttempt(context.Background(), supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); err != nil {
				t.Fatalf("subsequent Begin remained blocked: %v", err)
			}
		})
	}
}

func TestV28QuarantinesReleasedInvalidInputAndSurfacesRecoveryBlocker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	createDatabaseAtVersion(t, path, 25)
	seedMigrationPhase(t, path, true, false)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`DROP TRIGGER provider_attempt_inputs_immutable_update; UPDATE provider_attempt_inputs SET request_digest=?; UPDATE provider_attempts SET launch_state='released',process_pid=123,process_pgid=123,process_boot_identity='boot',process_start_identity='start'`, strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var attemptState, phaseState string
	if err := database.db.QueryRow(`SELECT state FROM provider_attempts WHERE ticket_id='SF-v27'`).Scan(&attemptState); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRow(`SELECT state FROM phase_runs WHERE ticket_id='SF-v27'`).Scan(&phaseState); err != nil {
		t.Fatal(err)
	}
	if attemptState != "quarantined" || phaseState != "active" {
		t.Fatalf("released invalid input was not fail-closed: attempt=%s phase=%s", attemptState, phaseState)
	}
	var leases int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM leases WHERE scope='provider' AND scope_key='provider/key'`).Scan(&leases); err != nil || leases != 1 {
		t.Fatalf("released invalid claim lost its lease=%d err=%v", leases, err)
	}
	if _, err := database.ActiveProviderAttempts(context.Background(), domain.ChannelDev); !errors.Is(err, ErrProviderRecoveryBlocked) {
		t.Fatalf("invalid released claim did not surface typed recovery blocker: %v", err)
	}
}

func TestV29RetiresResultlessLegacySuccessAndProviderLease(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "v28.sqlite")
	createDatabaseAtVersion(t, path, 28)
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.ExecContext(ctx, `PRAGMA foreign_keys=ON;
		INSERT INTO daemon_instances(channel,leader_epoch,identity,updated_at) VALUES('dev',1,'legacy','now');
		INSERT INTO projects(channel,id,canonical_path,base_ref) VALUES('dev','p','/tmp/p','main');
		INSERT INTO tickets(channel,project_id,id,source_digest,ticket_type,merge_mode,state,version,runner_epoch,workflow_id) VALUES('dev','p','T','source','feature','guarded','building',1,1,'wf');
		INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,provider,model,family,provider_version,worktree_identity,base_sha,started_at,outcome) VALUES('dev','p','T','build',1,'completed',1,1,1,'legacy','m','f','v','wt','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','now','passed');
		INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,provider,model,family,provider_version,worktree_identity,base_sha,started_at,outcome) VALUES('dev','p','T','planning',2,'completed',1,1,1,'orphan','m','f','v','wt','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','now','completed');
		INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,provider,model,family,provider_version,worktree_identity,base_sha,started_at,outcome) VALUES('dev','p','T','verification',3,'completed',1,1,1,'mismatch','m','f','v','wt','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','now','passed');
		INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,leader_epoch,runner_epoch,expected_ticket_version,worktree_identity,base_sha,provider_lease_key) VALUES('dev','p','T','build',1,'legacy','m','f','v','completed',1,1,1,'wt','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','');
		INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,leader_epoch,runner_epoch,expected_ticket_version,worktree_identity,base_sha,provider_lease_key) VALUES('dev','p','T','verification',3,'other','m','f','v','completed',1,1,1,'wt','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','other-lease');
		INSERT INTO leases(channel,project_id,scope,scope_key,ticket_id,runner_epoch,acquired_at) VALUES('dev','p','provider','','T',1,'now')`)
	if err != nil {
		raw.Close()
		t.Fatal(err)
	}
	raw.Close()
	db, err := OpenChannel(ctx, path, filepath.Join(root, "backups"), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var attemptState, attemptOutcome, phaseState, phaseOutcome string
	if err := db.db.QueryRowContext(ctx, `SELECT state,outcome FROM provider_attempts WHERE channel='dev' AND project_id='p' AND ticket_id='T'`).Scan(&attemptState, &attemptOutcome); err != nil || attemptState != "failed" || attemptOutcome != "invalid_artifact" {
		t.Fatalf("attempt=%s/%s err=%v", attemptState, attemptOutcome, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT state,outcome FROM phase_runs WHERE channel='dev' AND project_id='p' AND ticket_id='T' AND phase='build'`).Scan(&phaseState, &phaseOutcome); err != nil || phaseState != "failed" || phaseOutcome != "invalid_artifact" {
		t.Fatalf("phase=%s/%s err=%v", phaseState, phaseOutcome, err)
	}
	var unsafeSuccess int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel='dev' AND project_id='p' AND ticket_id='T' AND state='completed' AND outcome IN ('completed','passed')`).Scan(&unsafeSuccess); err != nil || unsafeSuccess != 0 {
		t.Fatalf("remaining unsafe phases=%d err=%v", unsafeSuccess, err)
	}
	var results, leases int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results`).Scan(&results); err != nil || results != 0 {
		t.Fatalf("results=%d err=%v", results, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE scope='provider'`).Scan(&leases); err != nil || leases != 0 {
		t.Fatalf("leases=%d err=%v", leases, err)
	}
}

func TestFutureAndForeignSchemasRefuseBeforePragmaOrBackupMutation(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name  string
		setup string
	}{
		{name: "future", setup: `CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL, checksum TEXT NOT NULL); INSERT INTO schema_migrations VALUES(999,'now','future')`},
		{name: "foreign", setup: `CREATE TABLE unrelated_authority(id INTEGER PRIMARY KEY)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "state.sqlite")
			raw, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := raw.ExecContext(ctx, `PRAGMA journal_mode=DELETE; `+test.setup); err != nil {
				t.Fatal(err)
			}
			if err := raw.Close(); err != nil {
				t.Fatal(err)
			}
			backups := filepath.Join(root, "backups")
			if err := os.Mkdir(backups, 0o700); err != nil {
				t.Fatal(err)
			}
			if database, err := OpenChannel(ctx, path, backups, domain.ChannelStable); err == nil {
				_ = database.Close()
				t.Fatal("unrecognized schema opened")
			}
			entries, err := os.ReadDir(backups)
			if err != nil || len(entries) != 0 {
				t.Fatalf("refusal created backup entries=%v err=%v", entries, err)
			}
			raw, err = sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			var journal string
			if err := raw.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&journal); err != nil || journal != "delete" {
				t.Fatalf("journal=%q err=%v", journal, err)
			}
		})
	}
}

func TestStableMigrationRefusesUnsafeBackupDirectoryWithoutChangingSchema(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "stable.sqlite")
	createDatabaseAtVersion(t, path, schemaVersion-1)
	backups := filepath.Join(root, "backups")
	if err := os.Mkdir(backups, 0o755); err != nil {
		t.Fatal(err)
	}
	if database, err := OpenChannel(ctx, path, backups, domain.ChannelStable); err == nil {
		_ = database.Close()
		t.Fatal("migration used an unsafe backup directory")
	}
	if got := rawSchemaVersion(t, path); got != schemaVersion-1 {
		t.Fatalf("refused migration changed schema to %d", got)
	}
}

func TestBackupRefusesExistingDestination(t *testing.T) {
	database, ctx := openTestStore(t)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(directory, "existing.sqlite")
	if err := os.WriteFile(destination, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := database.Backup(ctx, destination); err == nil {
		t.Fatal("backup overwrote an existing destination")
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "sentinel" {
		t.Fatalf("existing destination changed: %q err=%v", data, err)
	}
}

func createDatabaseAtVersion(t *testing.T, path string, target int) {
	t.Helper()
	if target < 1 || target >= schemaVersion {
		t.Fatalf("unsupported test schema target %d", target)
	}
	ctx := context.Background()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys=ON; CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for version := 1; version <= target; version++ {
		for _, statement := range testMigration(version) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("migration %d: %v", version, err)
			}
		}
		if version == 1 {
			if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES(1,'now')`); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if version == 2 {
			if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET checksum=? WHERE version=1`, migrationChecksums[1]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at, checksum) VALUES(?,'now',?)`, version, migrationChecksums[version]); err != nil {
			t.Fatal(err)
		}
	}
}

func testMigration(version int) []string {
	switch version {
	case 1:
		return migrationV1
	case 2:
		return migrationV2
	case 3:
		return migrationV3
	case 4:
		return migrationV4
	case 5:
		return migrationV5
	case 6:
		return migrationV6
	case 7:
		return migrationV7
	case 8:
		return migrationV8
	case 9:
		return migrationV9
	case 10:
		return migrationV10
	case 11:
		return migrationV11
	case 12:
		return migrationV12
	case 13:
		return migrationV13
	case 14:
		return migrationV14
	case 15:
		return migrationV15
	case 16:
		return migrationV16
	case 17:
		return migrationV17
	case 18:
		return migrationV18
	case 19:
		return migrationV19
	case 20:
		return migrationV20
	case 21:
		return migrationV21
	case 22:
		return migrationV22
	case 23:
		return migrationV23
	case 24:
		return migrationV24
	case 25:
		return migrationV25
	case 26:
		return migrationV26
	case 27:
		return migrationV27
	case 28:
		return migrationV28
	case 29:
		return migrationV29
	case 30:
		return migrationV30
	case 31:
		return migrationV31
	case 32:
		return migrationV32
	case 33:
		return migrationV33
	case 34:
		return migrationV34
	case 35:
		return migrationV35
	case 36:
		return migrationV36
	case 37:
		return migrationV37
	case 38:
		return migrationV38
	case 39:
		return migrationV39
	case 40:
		return migrationV40
	case 41:
		return migrationV41
	case 42:
		return migrationV42
	case 43:
		return migrationV43
	case 44:
		return migrationV44
	default:
		return nil
	}
}

func rawSchemaVersion(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}
