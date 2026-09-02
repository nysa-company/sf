package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const nysaConfigLockName = ".nysa-api-pure-init.lock"

const (
	configLockWaitLimit = 30 * time.Second
	configLockPoll      = 10 * time.Millisecond
)

// afterNysaConfigLink is test-only synchronization for the post-link identity
// check. Production leaves it nil; it is deliberately not exported.
var afterNysaConfigLink func()

// nysaConfigLock owns a descriptor for the real .sf directory and a canonical
// advisory lock inside it. Keeping the directory descriptor open makes all
// bootstrap reads, links, and rollback unambiguous even if a pathname is
// renamed after initialization starts.
type nysaConfigLock struct {
	// root remains open for the lifetime of every lock. In the absent-.sf
	// case its flock is the canonical no-config admission lock; in both cases
	// its descriptor authenticates the repository identity through final apply.
	root       int
	rootLocked bool
	directory  int
	file       int
}

// RepositoryIdentity is the device/inode identity of an opened canonical
// repository root. It is intentionally opaque: callers may carry it across a
// local operation, but cannot manufacture a path-only equivalent.
type RepositoryIdentity struct {
	device uint64
	inode  uint64
}

// CaptureRepositoryIdentity opens the current repository root without
// following symlinks and returns its identity for a later locked operation.
func CaptureRepositoryIdentity(repository string) (RepositoryIdentity, error) {
	if !filepathCleanAbsolute(repository) {
		return RepositoryIdentity{}, errors.New("repository identity path must be absolute and clean")
	}
	root, err := unix.Open(repository, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return RepositoryIdentity{}, fmt.Errorf("open repository identity: %w", err)
	}
	defer unix.Close(root)
	var info unix.Stat_t
	if err := unix.Fstat(root, &info); err != nil || info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Uid != uint32(os.Getuid()) || info.Mode&0o022 != 0 {
		return RepositoryIdentity{}, errors.New("repository root is not a protected current-user real directory")
	}
	return RepositoryIdentity{device: uint64(info.Dev), inode: uint64(info.Ino)}, nil
}

func acquireNysaConfigLock(repository string) (*nysaConfigLock, error) {
	return acquireProjectConfigLockContext(context.Background(), repository, true)
}

// acquireExistingProjectConfigLock takes the same descriptor-backed lock as
// the bootstrap path, but never creates .sf. Configuration generation apply
// freezes either a committed source configuration or the authenticated absence
// used by the normal repository command-detection path.
func acquireExistingProjectConfigLock(repository string) (*nysaConfigLock, error) {
	return acquireProjectConfigLockContext(context.Background(), repository, false)
}

func acquireProjectConfigLock(repository string, createDirectory bool) (*nysaConfigLock, error) {
	return acquireProjectConfigLockContext(context.Background(), repository, createDirectory)
}

// acquireProjectConfigLockContext uses non-blocking flock attempts so a
// cancelled CLI request cannot remain stuck in the kernel waiting for another
// initializer. The caller's deadline is honored, with a finite upper bound
// for callers that pass context.Background().
func acquireProjectConfigLockContext(ctx context.Context, repository string, createDirectory bool) (*nysaConfigLock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	lockCtx, cancel := context.WithTimeout(ctx, configLockWaitLimit)
	defer cancel()
	if !filepathCleanAbsolute(repository) {
		return nil, errors.New("profile configuration repository is not an absolute clean path")
	}
	root, err := unix.Open(repository, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open repository for profile configuration: %w", err)
	}
	closeRoot := func(err error) (*nysaConfigLock, error) {
		_ = unix.Flock(root, unix.LOCK_UN)
		_ = unix.Close(root)
		return nil, err
	}
	// Every configuration operation takes the repository-root lock before it
	// inspects or creates .sf. This is the outer lock for both init and apply;
	// a nested .sf lock is acquired only afterwards, so supported callers have
	// one fixed root -> .sf order and never close/reopen the root descriptor.
	if err := flockContext(lockCtx, root); err != nil {
		_ = unix.Close(root)
		return nil, fmt.Errorf("lock repository configuration root: %w", err)
	}
	var rootInfo unix.Stat_t
	if err := unix.Fstat(root, &rootInfo); err != nil || rootInfo.Mode&unix.S_IFMT != unix.S_IFDIR || rootInfo.Uid != uint32(os.Getuid()) || rootInfo.Mode&0o022 != 0 {
		return closeRoot(errors.New("project repository is not a current-user real directory"))
	}
	created := false
	if createDirectory {
		if err := unix.Mkdirat(root, ".sf", 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
			return closeRoot(fmt.Errorf("create profile configuration directory: %w", err))
		} else if err == nil {
			created = true
		}
	}
	directory, err := unix.Openat(root, ".sf", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if !createDirectory && errors.Is(err, unix.ENOENT) {
			return &nysaConfigLock{root: root, rootLocked: true, directory: -1, file: -1}, nil
		}
		return closeRoot(errors.New("project configuration directory must be a real directory"))
	}
	if created {
		// Best-effort directory persistence: a crash must not make the lock file
		// appear in an unpersisted replacement directory on filesystems that
		// support directory fsync.
		_ = unix.Fsync(root)
	}
	var info unix.Stat_t
	if err := unix.Fstat(directory, &info); err != nil || info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Uid != uint32(os.Getuid()) {
		_ = unix.Close(directory)
		_ = unix.Close(root)
		return nil, errors.New("project configuration directory is not a current-user real directory")
	}
	if err := unix.Fchmod(directory, 0o700); err != nil {
		_ = unix.Close(directory)
		_ = unix.Close(root)
		return nil, fmt.Errorf("secure project configuration directory: %w", err)
	}
	file, err := unix.Openat(directory, nysaConfigLockName, unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		_ = unix.Close(directory)
		_ = unix.Close(root)
		return nil, fmt.Errorf("open profile configuration lock: %w", err)
	}
	var lockInfo unix.Stat_t
	if err := unix.Fstat(file, &lockInfo); err != nil || lockInfo.Mode&unix.S_IFMT != unix.S_IFREG || lockInfo.Uid != uint32(os.Getuid()) || lockInfo.Mode&0o022 != 0 {
		_ = unix.Close(file)
		_ = unix.Close(directory)
		_ = unix.Close(root)
		return nil, errors.New("profile configuration lock must be a protected regular non-symlink file")
	}
	if err := flockContext(lockCtx, file); err != nil {
		_ = unix.Close(file)
		_ = unix.Close(directory)
		_ = unix.Close(root)
		return nil, fmt.Errorf("lock profile configuration: %w", err)
	}
	return &nysaConfigLock{root: root, rootLocked: true, directory: directory, file: file}, nil
}

func flockContext(ctx context.Context, fd int) error {
	for {
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			return err
		}
		timer := time.NewTimer(configLockPoll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
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
	if lock.root >= 0 {
		if lock.rootLocked {
			if err := unix.Flock(lock.root, unix.LOCK_UN); err != nil && !errors.Is(err, unix.EBADF) {
				result = errors.Join(result, err)
			}
		}
		if err := unix.Close(lock.root); err != nil && !errors.Is(err, unix.EBADF) {
			result = errors.Join(result, err)
		}
		lock.root = -1
		lock.rootLocked = false
	}
	return result
}

func (lock *nysaConfigLock) validateRepositoryIdentity(repository string, expected RepositoryIdentity) error {
	if lock == nil || lock.root < 0 || expected.device == 0 || expected.inode == 0 || !filepathCleanAbsolute(repository) {
		return errors.New("project repository identity is unavailable")
	}
	var opened, named unix.Stat_t
	if err := unix.Fstat(lock.root, &opened); err != nil || opened.Mode&unix.S_IFMT != unix.S_IFDIR || opened.Uid != uint32(os.Getuid()) || opened.Mode&0o022 != 0 || uint64(opened.Dev) != expected.device || uint64(opened.Ino) != expected.inode {
		return errors.New("project repository identity changed during configuration apply")
	}
	if err := unix.Lstat(repository, &named); err != nil || named.Mode&unix.S_IFMT != unix.S_IFDIR || named.Mode&unix.S_IFLNK != 0 || uint64(named.Dev) != expected.device || uint64(named.Ino) != expected.inode {
		return errors.New("project repository path changed during configuration apply")
	}
	return nil
}

func (lock *nysaConfigLock) readOptional(name string) ([]byte, bool, error) {
	if lock == nil || name != "config.toml" {
		return nil, false, errors.New("profile configuration lock is unavailable")
	}
	if lock.directory < 0 {
		if lock.root < 0 {
			return nil, false, errors.New("profile configuration lock is unavailable")
		}
		var info unix.Stat_t
		err := unix.Fstatat(lock.root, ".sf", &info, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, unix.ENOENT) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("inspect configuration directory: %w", err)
		}
		return nil, false, errors.New("project configuration appeared while applying it")
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
