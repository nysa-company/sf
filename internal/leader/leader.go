// Package leader owns the channel-specific local daemon lease. The file lock
// prevents cooperative duplicate daemons; inode validation detects a replaced
// lock path, while the database leader epoch remains the mutation fence.
package leader

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/nysa-company/sf/internal/domain"
)

var (
	ErrLeaderExists  = errors.New("another daemon holds the channel leader lease")
	ErrLeaseReplaced = errors.New("daemon leader lease path was replaced")
)

const schema = "sf.leader/v1"

type Metadata struct {
	Schema     string         `json:"schema"`
	Channel    domain.Channel `json:"channel"`
	Identity   string         `json:"identity"`
	PID        int            `json:"pid"`
	AcquiredAt time.Time      `json:"acquired_at"`
}

type identity struct {
	device uint64
	inode  uint64
}

type Lease struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	identity identity
	metadata Metadata
	closed   bool
}

func Acquire(path string, channel domain.Channel, daemonIdentity string) (*Lease, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("leader lock path must be absolute")
	}
	if !channel.Valid() || daemonIdentity == "" || len(daemonIdentity) > 256 {
		return nil, errors.New("valid channel and bounded daemon identity are required")
	}
	if err := secureParent(filepath.Dir(path)); err != nil {
		return nil, err
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open leader lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	closeFile := func() { _ = file.Close() }
	opened, err := fileIdentity(file)
	if err != nil {
		closeFile()
		return nil, err
	}
	if err := validateFile(file); err != nil {
		closeFile()
		return nil, err
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		closeFile()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrLeaderExists
		}
		return nil, fmt.Errorf("acquire leader lock: %w", err)
	}
	release := func() {
		_ = unix.Flock(fd, unix.LOCK_UN)
		closeFile()
	}
	current, err := pathIdentity(path)
	if err != nil || current != opened {
		release()
		return nil, ErrLeaseReplaced
	}
	metadata := Metadata{Schema: schema, Channel: channel, Identity: daemonIdentity, PID: os.Getpid(), AcquiredAt: time.Now().UTC()}
	if err := writeMetadata(file, metadata); err != nil {
		release()
		return nil, err
	}
	current, err = pathIdentity(path)
	if err != nil || current != opened {
		release()
		return nil, ErrLeaseReplaced
	}
	return &Lease{file: file, path: path, identity: opened, metadata: metadata}, nil
}

func (lease *Lease) Metadata() Metadata { return lease.metadata }

// Validate is called before durable leadership acquisition and before the
// daemon accepts mutating work. A replaced path cannot be silently repaired by
// the stale process.
func (lease *Lease) Validate() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || lease.file == nil {
		return errors.New("leader lease is closed")
	}
	opened, err := fileIdentity(lease.file)
	if err != nil || opened != lease.identity {
		return ErrLeaseReplaced
	}
	current, err := pathIdentity(lease.path)
	if err != nil || current != lease.identity {
		return ErrLeaseReplaced
	}
	return validateFile(lease.file)
}

func (lease *Lease) Close() error {
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return nil
	}
	lease.closed = true
	unlockErr := unix.Flock(int(lease.file.Fd()), unix.LOCK_UN)
	closeErr := lease.file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

func ReadMetadata(path string) (Metadata, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return Metadata{}, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	if err := validateFile(file); err != nil {
		return Metadata{}, err
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return Metadata{}, err
	}
	if len(data) > 4096 {
		return Metadata{}, errors.New("leader metadata exceeds 4096 bytes")
	}
	var metadata Metadata
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return Metadata{}, fmt.Errorf("decode leader metadata: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Metadata{}, errors.New("leader metadata contains trailing data")
	}
	if metadata.Schema != schema || !metadata.Channel.Valid() || metadata.Identity == "" || metadata.PID <= 0 || metadata.AcquiredAt.IsZero() {
		return Metadata{}, errors.New("invalid leader metadata")
	}
	return metadata, nil
}

func writeMetadata(file *os.File, metadata Metadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > 4096 {
		return errors.New("leader metadata exceeds 4096 bytes")
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("truncate leader metadata: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek leader metadata: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write leader metadata: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync leader metadata: %w", err)
	}
	return nil
}

func validateFile(file *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fmt.Errorf("inspect leader lock: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 {
		return errors.New("leader lock must be one regular file")
	}
	if stat.Uid != uint32(os.Getuid()) {
		return errors.New("leader lock is not owned by the current user")
	}
	if stat.Mode&0o077 != 0 {
		if err := file.Chmod(0o600); err != nil {
			return fmt.Errorf("secure leader lock: %w", err)
		}
	}
	return nil
}

func fileIdentity(file *os.File) (identity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return identity{}, err
	}
	return identity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, nil
}

func pathIdentity(path string) (identity, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		return identity{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uint32(os.Getuid()) || stat.Mode&0o077 != 0 {
		return identity{}, errors.New("leader lock path is not an owner-only regular file")
	}
	return identity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, nil
}

func secureParent(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create leader directory: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		return fmt.Errorf("inspect leader directory: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("leader directory must be a real directory")
	}
	if stat.Uid != uint32(os.Getuid()) {
		return errors.New("leader directory is not owned by the current user")
	}
	if stat.Mode&0o077 != 0 {
		if err := os.Chmod(path, 0o700); err != nil {
			return fmt.Errorf("secure leader directory: %w", err)
		}
	}
	return nil
}
