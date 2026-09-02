package codexruntime

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveRequiresExactPrivateSiblingBundle(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		root, codex, _ := runtimeFixture(t)
		bundle, err := Resolve(codex)
		if err != nil || bundle.Codex.Path != codex || bundle.Host.Path != filepath.Join(root, CodeModeHost) || bundle.Digest == "" {
			t.Fatalf("bundle=%+v err=%v", bundle, err)
		}
	})
	t.Run("missing helper", func(t *testing.T) {
		_, codex, host := runtimeFixture(t)
		if err := os.Remove(host); err != nil {
			t.Fatal(err)
		}
		if _, err := Resolve(codex); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Resolve error=%v, want unavailable", err)
		}
	})
	t.Run("symlinked helper", func(t *testing.T) {
		root, codex, host := runtimeFixture(t)
		moved := filepath.Join(root, "moved-host")
		if err := os.Rename(host, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(moved, host); err != nil {
			t.Fatal(err)
		}
		if _, err := Resolve(codex); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Resolve error=%v, want unavailable", err)
		}
	})
	t.Run("writable helper", func(t *testing.T) {
		_, codex, host := runtimeFixture(t)
		if err := os.Chmod(host, 0o720); err != nil {
			t.Fatal(err)
		}
		if _, err := Resolve(codex); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("Resolve error=%v, want unavailable", err)
		}
	})
}

func TestBundleDigestAndCopyRejectHelperMutation(t *testing.T) {
	root, codex, host := runtimeFixture(t)
	bundle, err := Resolve(codex)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(host, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host, []byte("#!/bin/sh\nexit 9\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := Verify(bundle); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Verify mutation error=%v, want unavailable", err)
	}
	fresh, err := Resolve(codex)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Digest == bundle.Digest {
		t.Fatal("helper mutation preserved bundle digest")
	}
	stage := filepath.Join(root, "stage")
	if err := os.Mkdir(stage, 0o700); err != nil {
		t.Fatal(err)
	}
	stagedCodex, err := CopyTo(fresh, stage)
	if err != nil {
		t.Fatal(err)
	}
	if stagedCodex != filepath.Join(stage, "codex") {
		t.Fatalf("staged codex=%q", stagedCodex)
	}
	if info, err := os.Lstat(filepath.Join(stage, CodeModeHost)); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o500 {
		t.Fatalf("staged helper info=%v err=%v", info, err)
	}
}

func runtimeFixture(t *testing.T) (root, codex, host string) {
	t.Helper()
	root = t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	root = canonical
	codex = filepath.Join(root, "codex")
	host = filepath.Join(root, CodeModeHost)
	for path, contents := range map[string][]byte{
		codex: []byte("#!/bin/sh\nexit 0\n"),
		host:  []byte("#!/bin/sh\nexit 0\n"),
	} {
		if err := os.WriteFile(path, contents, 0o500); err != nil {
			t.Fatal(err)
		}
	}
	return root, codex, host
}
