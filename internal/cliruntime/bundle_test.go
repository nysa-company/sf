package cliruntime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T, kind string) (string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entry := "claude"
	files := []string{entry}
	if kind == "cursor" {
		entry = "cursor-agent"
		files = []string{entry, "node", "index.js", "native.node"}
	}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte("fixture "+name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	return root, filepath.Join(root, entry)
}

func TestRuntimeSnapshotBindsCompleteClosure(t *testing.T) {
	for _, kind := range []string{"claude", "cursor"} {
		t.Run(kind, func(t *testing.T) {
			root, path := fixture(t, kind)
			b, err := Resolve(context.Background(), kind, path)
			if err != nil {
				t.Fatal(err)
			}
			parent, _ := filepath.EvalSymlinks(t.TempDir())
			stage, err := b.Stage(context.Background(), parent)
			if err != nil {
				t.Fatal(err)
			}
			if stage.Digest() != b.Digest() || stage.Executable() == path {
				t.Fatal("wrong snapshot identity")
			}
			member := filepath.Base(path)
			if kind == "cursor" {
				member = "native.node"
			}
			if err := os.WriteFile(filepath.Join(root, member), []byte("replacement"), 0700); err != nil {
				t.Fatal(err)
			}
			if _, err := b.Stage(context.Background(), parent); err == nil {
				t.Fatal("changed closure staged under old digest")
			}
			again, err := Resolve(context.Background(), kind, stage.Executable())
			if err != nil || again.Digest() != b.Digest() {
				t.Fatal("source mutation changed private stage")
			}
		})
	}
}

func TestRuntimeSnapshotRejectsUnsafeMembers(t *testing.T) {
	for _, name := range []string{"symlink", "writable", "missing", "extra", "cancel", "entrylimit"} {
		t.Run(name, func(t *testing.T) {
			root, path := fixture(t, "cursor")
			b, err := Resolve(context.Background(), "cursor", path)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch name {
			case "symlink":
				if err := os.Symlink("index.js", filepath.Join(root, "link.js")); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(filepath.Join(root, "index.js"), 0666); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(filepath.Join(root, "node")); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(root, "new.js"), []byte("new"), 0600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "entrylimit":
				for i := 0; i < MaxEntries; i++ {
					if err := os.WriteFile(filepath.Join(root, stringName(i)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			parent, _ := filepath.EvalSymlinks(t.TempDir())
			if _, err := b.Stage(ctx, parent); err == nil {
				t.Fatal("unsafe/changed runtime accepted")
			}
		})
	}
}

func stringName(i int) string {
	// Fixed-width names preserve deterministic lexical directory ordering.
	buf := []byte("0000.js")
	for n := 3; n >= 0; n-- {
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf)
}

// Explicit opt-in: reads installed runtime bytes and copies them into a private
// test directory. No model, credential, process or network call occurs.
func TestInstalledRuntimeSnapshots(t *testing.T) {
	for _, kind := range []string{"claude", "cursor"} {
		path := os.Getenv("SF_TEST_" + strings.ToUpper(kind) + "_RUNTIME")
		if path == "" {
			continue
		}
		t.Run(kind, func(t *testing.T) {
			b, err := Resolve(context.Background(), kind, path)
			if err != nil {
				t.Fatal(err)
			}
			parent, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			stage, err := b.Stage(context.Background(), parent)
			if err != nil {
				t.Fatal(err)
			}
			if stage.Digest() != b.Digest() {
				t.Fatal("staged digest mismatch")
			}
			t.Logf("authenticated %d members, staged digest %s", len(b.members), b.Digest())
		})
	}
}
