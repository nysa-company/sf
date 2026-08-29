package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPrepareChannelCreatesOnlyPrivateChannelTree(t *testing.T) {
	home := t.TempDir()
	paths, err := PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareChannel(paths); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{paths.Root, filepath.Dir(paths.Socket), paths.Logs, paths.Events, paths.Worktrees, paths.Backups} {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("path=%s info=%v err=%v", path, info, err)
		}
	}
	stable, _ := PathsFor(home, domain.ChannelStable)
	if _, err := os.Lstat(stable.Root); !os.IsNotExist(err) {
		t.Fatalf("stable channel was touched: %v", err)
	}
}

func TestPrepareChannelRejectsSymlinkedControlledComponent(t *testing.T) {
	home := t.TempDir()
	paths, _ := PathsFor(home, domain.ChannelDev)
	applicationRoot := filepath.Dir(paths.Root)
	if err := os.MkdirAll(applicationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(target, paths.Root); err != nil {
		t.Fatal(err)
	}
	if err := PrepareChannel(paths); err == nil {
		t.Fatal("symlinked channel root was accepted")
	}
}

func TestPrepareChannelRejectsEscapingChild(t *testing.T) {
	root := filepath.Join(t.TempDir(), "root")
	paths := ChannelPaths{Root: root, Database: filepath.Join(root, "sf.sqlite"), Machine: filepath.Join(root, "machine.toml"), Socket: filepath.Join(root, "run", "sf.sock"), Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "..", "escape")}
	if err := PrepareChannel(paths); err == nil {
		t.Fatal("escaping child was accepted")
	}
}
