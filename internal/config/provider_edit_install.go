package config

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type ProviderConfigEdit struct {
	Changed    bool
	BackupPath string
}

// EditProviderPreset edits an existing source file under the canonical config
// lock. It does not mutate Store generations or active-ticket authority. A
// durable original-byte backup is retained before atomic replacement, including
// on failure; callers report it rather than silently rolling back user changes.
func (plan *ProjectConfigPlan) EditProviderPreset(ctx context.Context, name string, machine MachineLimits, preset string) (ProviderConfigEdit, error) {
	var result ProviderConfigEdit
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if plan == nil || plan.lock == nil || !plan.exists {
		return result, errors.New("provider editing requires an existing .sf/config.toml; use init --providers for a missing config")
	}
	if err := plan.validateEditSource(); err != nil {
		return result, err
	}
	updated, err := RewriteProviderPreset(plan.prepared, preset)
	if err != nil {
		return result, err
	}
	if _, _, _, err := loadProjectData(plan.Repository, name, machine, nil, updated, true); err != nil {
		return result, err
	}
	if bytes.Equal(updated, plan.prepared) {
		return result, nil
	}
	backup := "config.toml.before-providers-" + rand.Text()
	if _, err := plan.lock.writeExclusiveConfigFile(backup, plan.prepared); err != nil {
		return result, err
	}
	result.BackupPath = filepath.Join(plan.Repository, ".sf", backup)
	if err := unix.Fsync(plan.lock.directory); err != nil {
		return result, err
	}
	temporary := ".config.toml.providers-" + rand.Text()
	info, err := plan.lock.writeExclusiveConfigFile(temporary, updated)
	if err != nil {
		return result, err
	}
	defer unix.Unlinkat(plan.lock.directory, temporary, 0)
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := plan.validateEditSource(); err != nil {
		return result, err
	}
	if err := unix.Renameat(plan.lock.directory, temporary, plan.lock.directory, "config.toml"); err != nil {
		return result, err
	}
	result.Changed = true
	plan.prepared, plan.preparedDigest, plan.preparedInfo = append([]byte(nil), updated...), configDigest(updated), info
	if err := unix.Fsync(plan.lock.directory); err != nil {
		return result, err
	}
	if err := plan.validateEditSource(); err != nil {
		return result, err
	}
	return result, nil
}

func (plan *ProjectConfigPlan) validateEditSource() error {
	if err := plan.ValidateUnchanged(); err != nil {
		return err
	}
	var opened, named unix.Stat_t
	if err := unix.Fstat(plan.lock.directory, &opened); err != nil {
		return err
	}
	if err := unix.Fstatat(plan.lock.root, ".sf", &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || named.Dev != opened.Dev || named.Ino != opened.Ino {
		return errors.New("configuration directory changed during provider edit")
	}
	return nil
}

func (lock *nysaConfigLock) writeExclusiveConfigFile(name string, data []byte) (os.FileInfo, error) {
	if lock == nil || lock.directory < 0 || filepath.Base(name) != name || len(data) > MaxFileBytes {
		return nil, errors.New("invalid configuration staging request")
	}
	fd, err := unix.Openat(lock.directory, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), "provider-config-stage")
	defer file.Close()
	complete := false
	defer func() {
		if complete {
			return
		}
		// Remove only our own incomplete random-name stage, never a replacement.
		var opened, named unix.Stat_t
		if unix.Fstat(fd, &opened) == nil && unix.Fstatat(lock.directory, name, &named, unix.AT_SYMLINK_NOFOLLOW) == nil && opened.Dev == named.Dev && opened.Ino == named.Ino {
			_ = unix.Unlinkat(lock.directory, name, 0)
		}
	}()
	if n, err := file.Write(data); err != nil || n != len(data) {
		return nil, errors.New("could not write complete configuration stage")
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	complete = true
	return info, nil
}
