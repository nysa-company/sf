package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

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
