package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestProviderEditRetainsBackupAndRejectsConcurrentSource(t *testing.T) {
	for _, changed := range []bool{false, true} {
		repo, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(repo, ".sf"), 0700); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(repo, ".sf", "config.toml")
		source := []byte("# preserve\n[commands]\nverify=['go','test','./...']\nreview=['go','test','./...']\n")
		if err := os.WriteFile(file, source, 0600); err != nil {
			t.Fatal(err)
		}
		plan, err := PrepareProjectConfigContext(context.Background(), repo)
		if err != nil {
			t.Fatal(err)
		}
		if changed {
			if err := os.WriteFile(file, append(source, []byte("# concurrent\n")...), 0600); err != nil {
				t.Fatal(err)
			}
		}
		edit, err := plan.EditProviderPreset(context.Background(), "p", DefaultMachineLimits(), "claude-codex")
		if changed {
			if err == nil || edit.Changed || edit.BackupPath != "" {
				t.Fatal("concurrent source was overwritten")
			}
		} else {
			if err != nil || !edit.Changed || edit.BackupPath == "" {
				t.Fatalf("edit=%+v err=%v", edit, err)
			}
			backup, err := os.ReadFile(edit.BackupPath)
			if err != nil || string(backup) != string(source) {
				t.Fatal("original backup missing", err)
			}
			if err := plan.ValidateUnchanged(); err != nil {
				t.Fatal(err)
			}
			replay, err := plan.EditProviderPreset(context.Background(), "p", DefaultMachineLimits(), "claude-codex")
			if err != nil || replay.Changed || replay.BackupPath != "" {
				t.Fatal("replay rewrote source", err)
			}
		}
		if err := plan.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProviderEditRejectsCancelledContextAndDirectorySwap(t *testing.T) {
	for _, swap := range []bool{false, true} {
		repo, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(repo, ".sf")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "config.toml"), []byte("# original\n"), 0600); err != nil {
			t.Fatal(err)
		}
		plan, err := PrepareProjectConfigContext(context.Background(), repo)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		if swap {
			if err := os.Rename(dir, filepath.Join(repo, "retained")); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
		} else {
			cancel()
		}
		edit, err := plan.EditProviderPreset(ctx, "p", DefaultMachineLimits(), "claude-codex")
		cancel()
		if err == nil || edit.Changed || edit.BackupPath != "" {
			t.Fatal("cancelled/swapped source accepted")
		}
		_ = plan.Close()
	}
}
