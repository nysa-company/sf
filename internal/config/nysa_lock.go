package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const nysaConfigLockName = ".nysa-api-pure-init.lock"

// afterNysaConfigLink is test-only synchronization for the post-link identity
// check. Production leaves it nil; it is deliberately not exported.
var afterNysaConfigLink func()

// nysaConfigLock owns a descriptor for the real .sf directory and a canonical
// advisory lock inside it. Keeping the directory descriptor open makes all
// bootstrap reads, links, and rollback unambiguous even if a pathname is
// renamed after initialization starts.
type nysaConfigLock struct {
	directory int
	file      int
}

func acquireNysaConfigLock(repository string) (*nysaConfigLock, error) {
	if !filepathCleanAbsolute(repository) {
		return nil, errors.New("profile configuration repository is not an absolute clean path")
	}
	root, err := unix.Open(repository, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open repository for profile configuration: %w", err)
	}
	closeRoot := func(err error) (*nysaConfigLock, error) { _ = unix.Close(root); return nil, err }
	created := false
	if err := unix.Mkdirat(root, ".sf", 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
		return closeRoot(fmt.Errorf("create profile configuration directory: %w", err))
	} else if err == nil {
		created = true
	}
	directory, err := unix.Openat(root, ".sf", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return closeRoot(errors.New("project configuration directory must be a real directory"))
	}
	if created {
		// Best-effort directory persistence: a crash must not make the lock file
		// appear in an unpersisted replacement directory on filesystems that
		// support directory fsync.
		_ = unix.Fsync(root)
	}
	_ = unix.Close(root)
	var info unix.Stat_t
	if err := unix.Fstat(directory, &info); err != nil || info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Uid != uint32(os.Getuid()) {
		_ = unix.Close(directory)
		return nil, errors.New("project configuration directory is not a current-user real directory")
	}
	if err := unix.Fchmod(directory, 0o700); err != nil {
		_ = unix.Close(directory)
		return nil, fmt.Errorf("secure project configuration directory: %w", err)
	}
	file, err := unix.Openat(directory, nysaConfigLockName, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		_ = unix.Close(directory)
		return nil, fmt.Errorf("open profile configuration lock: %w", err)
	}
	var lockInfo unix.Stat_t
	if err := unix.Fstat(file, &lockInfo); err != nil || lockInfo.Mode&unix.S_IFMT != unix.S_IFREG || lockInfo.Uid != uint32(os.Getuid()) || lockInfo.Mode&0o022 != 0 {
		_ = unix.Close(file)
		_ = unix.Close(directory)
		return nil, errors.New("profile configuration lock must be a protected regular non-symlink file")
	}
	if err := unix.Flock(file, unix.LOCK_EX); err != nil {
		_ = unix.Close(file)
		_ = unix.Close(directory)
		return nil, fmt.Errorf("lock profile configuration: %w", err)
	}
	return &nysaConfigLock{directory: directory, file: file}, nil
}

func (lock *nysaConfigLock) Close() error {
	if lock == nil {
		return nil
	}
	var result error
	if lock.file >= 0 {
		if err := unix.Flock(lock.file, unix.LOCK_UN); err != nil && !errors.Is(err, unix.EBADF) {
			result = errors.Join(result, err)
		}
		if err := unix.Close(lock.file); err != nil && !errors.Is(err, unix.EBADF) {
			result = errors.Join(result, err)
		}
		lock.file = -1
	}
	if lock.directory >= 0 {
		if err := unix.Close(lock.directory); err != nil && !errors.Is(err, unix.EBADF) {
			result = errors.Join(result, err)
		}
		lock.directory = -1
	}
	return result
}

func (lock *nysaConfigLock) readOptional(name string) ([]byte, bool, error) {
	if lock == nil || lock.directory < 0 || name != "config.toml" {
		return nil, false, errors.New("profile configuration lock is unavailable")
	}
	fd, err := unix.Openat(lock.directory, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if errors.Is(err, unix.ENOENT) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("open configuration: %w", err)
	}
	file := os.NewFile(uintptr(fd), "profile-config")
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o022 != 0 {
		return nil, false, errors.New("configuration must be a protected regular non-symlink file")
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxFileBytes+1))
	if err != nil || len(data) > MaxFileBytes {
		return nil, false, errors.New("configuration exceeds bounded profile limit")
	}
	return data, true, nil
}

func (lock *nysaConfigLock) fileIdentity(name string) (os.FileInfo, error) {
	if lock == nil || lock.directory < 0 || name != "config.toml" {
		return nil, errors.New("profile configuration lock is unavailable")
	}
	fd, err := unix.Openat(lock.directory, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open configuration: %w", err)
	}
	file := os.NewFile(uintptr(fd), "profile-config")
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o022 != 0 {
		return nil, errors.New("configuration must be a protected regular non-symlink file")
	}
	return info, nil
}

func (lock *nysaConfigLock) install(data []byte) (os.FileInfo, error) {
	if lock == nil || lock.directory < 0 || len(data) == 0 || len(data) > MaxFileBytes {
		return nil, errors.New("profile configuration lock is unavailable")
	}
	name := fmt.Sprintf(".config.toml.sf-%d", time.Now().UnixNano())
	fd, err := unix.Openat(lock.directory, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create profile configuration: %w", err)
	}
	file := os.NewFile(uintptr(fd), "profile-config-temporary")
	remove := func() { _ = unix.Unlinkat(lock.directory, name, 0) }
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		remove()
		return nil, errors.New("write profile configuration")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		remove()
		return nil, errors.New("sync profile configuration")
	}
	temporaryInfo, err := file.Stat()
	if err != nil || !temporaryInfo.Mode().IsRegular() || temporaryInfo.Mode()&0o022 != 0 {
		_ = file.Close()
		remove()
		return nil, errors.New("profile configuration temporary file is not a protected regular file")
	}
	var temporaryStat unix.Stat_t
	if err := unix.Fstat(fd, &temporaryStat); err != nil || temporaryStat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = file.Close()
		remove()
		return nil, errors.New("profile configuration temporary inode is unavailable")
	}
	if err := file.Close(); err != nil {
		remove()
		return nil, errors.New("close profile configuration")
	}
	if err := unix.Linkat(lock.directory, name, lock.directory, "config.toml", 0); err != nil {
		remove()
		if errors.Is(err, unix.EEXIST) {
			return nil, errors.New("project configuration appeared while initializing; rerun init after reviewing it")
		}
		return nil, fmt.Errorf("install profile configuration: %w", err)
	}
	if afterNysaConfigLink != nil {
		afterNysaConfigLink()
	}
	remove()
	_ = unix.Fsync(lock.directory)
	var installedStat unix.Stat_t
	verifyErr := errors.New("installed profile configuration identity changed")
	if statErr := unix.Fstatat(lock.directory, "config.toml", &installedStat, unix.AT_SYMLINK_NOFOLLOW); statErr == nil && installedStat.Mode&unix.S_IFMT == unix.S_IFREG && installedStat.Dev == temporaryStat.Dev && installedStat.Ino == temporaryStat.Ino {
		installed, identityErr := lock.fileIdentity("config.toml")
		if identityErr == nil && os.SameFile(temporaryInfo, installed) {
			return installed, nil
		}
		if identityErr != nil {
			verifyErr = identityErr
		}
	} else if statErr != nil {
		verifyErr = statErr
	}
	// The link is ours only when the post-link descriptor names the temporary
	// inode. Remove exactly that inode; a replacement is never unlinked.
	rollbackErr := lock.unlinkConfigIfSameInode(temporaryStat)
	if rollbackErr != nil {
		return temporaryInfo, errors.Join(verifyErr, rollbackErr)
	}
	if verifyErr != nil {
		return nil, fmt.Errorf("verify installed profile configuration: %w", verifyErr)
	}
	return nil, errors.New("installed profile configuration identity changed")
}

func (lock *nysaConfigLock) unlinkConfigIfSameInode(want unix.Stat_t) error {
	var got unix.Stat_t
	if err := unix.Fstatat(lock.directory, "config.toml", &got, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	if got.Mode&unix.S_IFMT != unix.S_IFREG || got.Dev != want.Dev || got.Ino != want.Ino {
		return errors.New("profile configuration changed; refusing rollback")
	}
	if err := unix.Unlinkat(lock.directory, "config.toml", 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	_ = unix.Fsync(lock.directory)
	return nil
}

func (lock *nysaConfigLock) rollback(want os.FileInfo) error {
	if lock == nil || want == nil {
		return nil
	}
	got, err := lock.fileIdentity("config.toml")
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil || !os.SameFile(want, got) {
		return errors.New("profile configuration changed; refusing rollback")
	}
	if err := unix.Unlinkat(lock.directory, "config.toml", 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	_ = unix.Fsync(lock.directory)
	return nil
}

// These operations are dir-FD/openat/no-follow on Darwin as well as Linux.
func filepathCleanAbsolute(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value
}
