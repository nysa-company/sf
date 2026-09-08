package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type openPolicy struct {
	backupBeforeMigration bool
	backupDir             string
	now                   func() time.Time
}

// OpenChannel is the production migration owner. Stable state is snapshotted
// before any recognized older schema is changed; development state remains
// isolated and may migrate without creating a stable backup.
func OpenChannel(ctx context.Context, path, backupDir string, channel domain.Channel) (*Store, error) {
	if !channel.Valid() {
		return nil, fmt.Errorf("valid channel is required")
	}
	policy := openPolicy{}
	if channel == domain.ChannelStable {
		policy = openPolicy{backupBeforeMigration: true, backupDir: backupDir, now: time.Now}
	}
	return open(ctx, path, policy)
}

func existingDatabase(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect sqlite path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("sqlite path must be a regular non-symlink file")
	}
	return info.Size() > 0, nil
}

// inspectStoredSchema performs only reads. It runs before journal-mode
// configuration so a future or foreign database is refused without changing
// its pragmas or creating a misleading backup.
func inspectStoredSchema(ctx context.Context, db *sql.DB) (version int, recognized bool, err error) {
	var migrationTables int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&migrationTables); err != nil {
		return 0, false, fmt.Errorf("inspect migration authority: %w", err)
	}
	if migrationTables == 0 {
		var applicationTables int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&applicationTables); err != nil {
			return 0, false, fmt.Errorf("inspect database tables: %w", err)
		}
		if applicationTables != 0 {
			return 0, false, fmt.Errorf("existing database has no recognized migration authority")
		}
		return 0, false, nil
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return 0, false, fmt.Errorf("read stored schema version: %w", err)
	}
	return version, true, nil
}

type migrationAuthorityColumn struct {
	cid          int
	name         string
	kind         string
	notNull      int
	defaultValue sql.NullString
	primaryKey   int
}

// validateStoredMigrationHistory is the read-only trust preflight for an
// existing database. It must run before backup, pragma configuration, or any
// migration: a recorded checksum is authority for the schema being upgraded,
// not an error to discover after a newer migration has already committed.
//
// Schema v1 predates the checksum column. Accept only that exact two-column
// authority shape; every later version must carry one contiguous, matching
// checksum row for every migration through the advertised maximum version.
func validateStoredMigrationHistory(ctx context.Context, db *sql.DB, storedVersion int) error {
	if storedVersion < 1 {
		return fmt.Errorf("stored migration authority contains no applied version")
	}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(schema_migrations)`)
	if err != nil {
		return fmt.Errorf("inspect stored migration authority shape: %w", err)
	}
	var columns []migrationAuthorityColumn
	for rows.Next() {
		var column migrationAuthorityColumn
		if err := rows.Scan(&column.cid, &column.name, &column.kind, &column.notNull, &column.defaultValue, &column.primaryKey); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read stored migration authority shape: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read stored migration authority shape: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close stored migration authority shape: %w", err)
	}
	exactBase := len(columns) >= 2 &&
		columns[0].cid == 0 && columns[0].name == "version" && columns[0].kind == "INTEGER" && columns[0].notNull == 0 && !columns[0].defaultValue.Valid && columns[0].primaryKey == 1 &&
		columns[1].cid == 1 && columns[1].name == "applied_at" && columns[1].kind == "TEXT" && columns[1].notNull == 1 && !columns[1].defaultValue.Valid && columns[1].primaryKey == 0
	if storedVersion == 1 {
		if !exactBase || len(columns) != 2 {
			return fmt.Errorf("stored schema version 1 has an invalid pre-checksum migration authority shape")
		}
		var count, minimum, maximum int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MIN(version),0),COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&count, &minimum, &maximum); err != nil {
			return fmt.Errorf("read stored schema version 1 history: %w", err)
		}
		if count != 1 || minimum != 1 || maximum != 1 {
			return fmt.Errorf("stored schema version 1 migration history is not exact")
		}
		return nil
	}
	if !exactBase || len(columns) != 3 ||
		columns[2].cid != 2 || columns[2].name != "checksum" || columns[2].kind != "TEXT" || columns[2].notNull != 1 || !columns[2].defaultValue.Valid || columns[2].defaultValue.String != "''" || columns[2].primaryKey != 0 {
		return fmt.Errorf("stored schema version %d has an invalid checksummed migration authority shape", storedVersion)
	}
	history, err := db.QueryContext(ctx, `SELECT version,checksum FROM schema_migrations ORDER BY version`)
	if err != nil {
		return fmt.Errorf("read stored migration history: %w", err)
	}
	expectedVersion := 1
	for history.Next() {
		var version int
		var actual string
		if err := history.Scan(&version, &actual); err != nil {
			_ = history.Close()
			return fmt.Errorf("read stored migration history: %w", err)
		}
		if version != expectedVersion {
			_ = history.Close()
			return fmt.Errorf("stored migration history is not contiguous at version %d", expectedVersion)
		}
		expected, ok := migrationChecksums[version]
		if !ok {
			_ = history.Close()
			return fmt.Errorf("stored migration %d has no supported checksum", version)
		}
		if actual != expected {
			_ = history.Close()
			return fmt.Errorf("stored migration %d checksum mismatch", version)
		}
		expectedVersion++
	}
	if err := history.Err(); err != nil {
		_ = history.Close()
		return fmt.Errorf("read stored migration history: %w", err)
	}
	if err := history.Close(); err != nil {
		return fmt.Errorf("close stored migration history: %w", err)
	}
	if expectedVersion != storedVersion+1 {
		return fmt.Errorf("stored migration history ends at version %d, expected %d", expectedVersion-1, storedVersion)
	}
	return nil
}

// validateCandidateRepairCompatibility protects the v55 trust-boundary
// change. A pre-v55 binding is rooted in publication evidence but does not
// authenticate its consumed recovery prefix. No ticket state can therefore
// prove that the associated remote PR was not merged while this process was
// offline. Preserve every nonterminal ticket for the last compatible runtime
// to observe/reconcile (or to cancel only after exact merge absence).
func validateCandidateRepairCompatibility(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, storedVersion int) error {
	if storedVersion < 41 {
		return nil
	}
	query := `SELECT t.channel,t.project_id,t.id,t.state
		FROM candidate_repair_bindings b JOIN tickets t
		ON t.channel=b.channel AND t.project_id=b.project_id AND t.id=b.ticket_id
		WHERE t.state NOT IN ('done','external_merged','cancelled')`
	if storedVersion >= 55 {
		query += ` AND b.consumed_recovery_prefix_digest=''`
	}
	query += ` ORDER BY t.channel,t.project_id,t.id LIMIT 1`
	var channel, project, ticket, state string
	err := q.QueryRowContext(ctx, query).Scan(&channel, &project, &ticket, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect candidate repair migration compatibility: %w", err)
	}
	if storedVersion < 55 {
		return fmt.Errorf("upgrade refused before schema, ticket, effect, or evidence mutation: database schema %d cannot upgrade to schema 55 while legacy candidate repair ticket %s/%s/%s is nonterminal in state %s; compatible_schema=54: run the exact previous channel binary that supports schema 54 to authenticate and reconcile any published merge, or cancel only after it proves exact merge absence, move the ticket to done, external_merged, or cancelled, then retry the upgrade", storedVersion, channel, project, ticket, state)
	}
	return fmt.Errorf("database schema %d contains nonterminal candidate repair ticket %s/%s/%s with an empty authenticated recovery-prefix digest; restore a trusted backup before startup", storedVersion, channel, project, ticket)
}

func (s *Store) backupBeforeMigration(ctx context.Context, directory string, from, to int, now func() time.Time) error {
	if now == nil {
		now = time.Now
	}
	if err := validateBackupDirectory(directory); err != nil {
		return err
	}
	stamp := now().UTC().Format("20060102T150405.000000000Z")
	for suffix := 0; suffix < 100; suffix++ {
		name := fmt.Sprintf("sf-schema-v%03d-to-v%03d-%s.sqlite", from, to, stamp)
		if suffix != 0 {
			name = fmt.Sprintf("sf-schema-v%03d-to-v%03d-%s-%02d.sqlite", from, to, stamp, suffix)
		}
		destination := filepath.Join(directory, name)
		if _, err := os.Lstat(destination); errors.Is(err, os.ErrNotExist) {
			return s.Backup(ctx, destination)
		} else if err != nil {
			return fmt.Errorf("inspect backup destination: %w", err)
		}
	}
	return fmt.Errorf("could not allocate a unique migration backup name")
}

func validateBackupDirectory(directory string) error {
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory || directory == string(filepath.Separator) {
		return fmt.Errorf("backup directory must be absolute and clean")
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect backup directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("backup directory must be a real non-symlink directory")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("backup directory permissions %04o are not owner-only", info.Mode().Perm())
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("backup directory ownership is unavailable")
	}
	if stat.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("backup directory must be owned by the current user")
	}
	return nil
}

func validateBackupFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() == 0 {
		return fmt.Errorf("backup is not a nonempty regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) || stat.Nlink != 1 {
		return fmt.Errorf("backup ownership or link identity is unsafe")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure backup permissions: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open completed backup: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return fmt.Errorf("backup identity changed after creation")
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync completed backup: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open backup directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync backup directory: %w", err)
	}
	return nil
}
