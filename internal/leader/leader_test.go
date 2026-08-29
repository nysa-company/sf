package leader

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestOwnerOnlySingletonLeaseAndMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run", "leader.lock")
	first, err := Acquire(path, domain.ChannelDev, "daemon-one")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := first.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(path, domain.ChannelDev, "daemon-two"); !errors.Is(err, ErrLeaderExists) {
		t.Fatalf("second acquire error=%v", err)
	}
	metadata, err := ReadMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Channel != domain.ChannelDev || metadata.Identity != "daemon-one" || metadata.PID != os.Getpid() {
		t.Fatalf("metadata=%+v", metadata)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("lock mode=%#o", info.Mode().Perm())
	}
}

func TestReplacementInvalidatesOldLeaseAndCanBeReacquired(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "leader.lock")
	oldPath := filepath.Join(directory, "old.lock")
	first, err := Acquire(path, domain.ChannelDev, "daemon-one")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err := os.Rename(path, oldPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := first.Validate(); !errors.Is(err, ErrLeaseReplaced) {
		t.Fatalf("old lease validation=%v", err)
	}
	second, err := Acquire(path, domain.ChannelDev, "daemon-two")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestSymlinkLockAndParentAreRejected(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, "leader.lock")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(link, domain.ChannelDev, "daemon"); err == nil {
		t.Fatal("symlink lock accepted")
	}

	realParent := filepath.Join(directory, "real-parent")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	parentLink := filepath.Join(directory, "parent-link")
	if err := os.Symlink(realParent, parentLink); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(filepath.Join(parentLink, "leader.lock"), domain.ChannelDev, "daemon"); err == nil {
		t.Fatal("symlink parent accepted")
	}
}

func TestCloseIsIdempotentAndAllowsCooperativeRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "leader.lock")
	lease, err := Acquire(path, domain.ChannelStable, "stable-one")
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Acquire(path, domain.ChannelStable, "stable-two")
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
}
