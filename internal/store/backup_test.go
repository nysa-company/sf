package store

import (
	"context"
	"database/sql"
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
	result, err := raw.Exec(`INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,started_at,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,supervisor_key,auth_digest,auth_mode,binding_digest,provider_lease_key,launch_state) VALUES('dev','migration','SF-v27','build',1,'provider','` + model + `','family','1','running','builder','active','now',1,1,1,'/migration','/migration','identity','` + base + `',X'0102030405060708091011121314151617181920212223242526272829303132','` + strings.Repeat("b", 64) + `','','` + strings.Repeat("c", 64) + `','provider/key','launching')`)
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
