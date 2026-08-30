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
