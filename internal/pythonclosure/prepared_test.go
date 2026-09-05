package pythonclosure

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func preparedFixture(t *testing.T) (string, int, string, Environment) {
	t.Helper()
	runtimeRoot, _, _ := fixture(t)
	if err := os.Chmod(filepath.Join(runtimeRoot, "lib/sample.py"), 0500); err != nil {
		t.Fatal(err)
	}
	depsRoot, _, _ := fixture(t)
	channel := t.TempDir()
	for _, root := range []string{runtimeRoot, depsRoot, channel} {
		if err := os.Chmod(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	runtimeManifest, err := CaptureDirectoryFD(t.Context(), openRoot(t, runtimeRoot))
	if err != nil {
		t.Fatal(err)
	}
	depsManifest, err := CaptureDirectoryFD(t.Context(), openRoot(t, depsRoot))
	if err != nil {
		t.Fatal(err)
	}
	e := Environment{EnvironmentVersion, runtimeManifest, depsManifest, "lib/sample.py", "sha256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("b", 64)}
	data, digest, err := e.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(channel, digest[7:])
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(runtimeRoot, filepath.Join(root, "runtime")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(depsRoot, filepath.Join(root, "dependencies")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "environment.json"), data, 0400); err != nil {
		t.Fatal(err)
	}
	return channel, openRoot(t, channel), digest, e
}

func TestPreparedRetainsDescriptorsAndClosesWithoutDeleting(t *testing.T) {
	channel, fd, digest, e := preparedFixture(t)
	p, err := OpenPreparedDirectoryFD(t.Context(), fd, digest, e.LockDigest, e.BootstrapDigest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	if p.Interpreter() != e.Interpreter {
		t.Fatal("interpreter changed")
	}
	root := filepath.Join(channel, digest[7:])
	retained := root + "-retained"
	if err := os.Rename(root, retained); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.Revalidate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if other, err := OpenPreparedDirectoryFD(t.Context(), fd, digest, e.LockDigest, e.BootstrapDigest); err == nil {
		other.Close()
		t.Fatal("opened replacement")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if p.RuntimeFD() != -1 || p.DependenciesFD() != -1 || p.Interpreter() != "" || p.Revalidate(t.Context()) == nil {
		t.Fatal("closed handle remained usable")
	}
	if _, err := os.Stat(retained); err != nil {
		t.Fatal("Close deleted evidence", err)
	}
}

func TestPreparedRejectsMalformedOrForeignSnapshot(t *testing.T) {
	for _, kind := range []string{"wrong channel", "root symlink", "runtime symlink", "manifest symlink", "manifest fifo", "public channel", "public snapshot", "public manifest", "changed dependency", "wrong lock", "wrong bootstrap", "bad digest", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			channel, fd, digest, e := preparedFixture(t)
			root := filepath.Join(channel, digest[7:])
			ctx := t.Context()
			var err error
			switch kind {
			case "wrong channel":
				other := t.TempDir()
				if err := os.Chmod(other, 0700); err != nil {
					t.Fatal(err)
				}
				fd = openRoot(t, other)
			case "root symlink", "runtime symlink", "manifest symlink":
				target := root
				if kind == "runtime symlink" {
					target = filepath.Join(root, "runtime")
				}
				if kind == "manifest symlink" {
					target = filepath.Join(root, "environment.json")
				}
				err = os.Rename(target, target+"-original")
				if err == nil {
					err = os.Symlink(target+"-original", target)
				}
			case "manifest fifo":
				target := filepath.Join(root, "environment.json")
				err = os.Rename(target, target+"-original")
				if err == nil {
					err = unix.Mkfifo(target, 0600)
				}
			case "public channel":
				err = os.Chmod(channel, 0755)
			case "public snapshot":
				err = os.Chmod(root, 0755)
			case "public manifest":
				err = os.Chmod(filepath.Join(root, "environment.json"), 0644)
			case "changed dependency":
				err = os.WriteFile(filepath.Join(root, "dependencies/lib/sample.py"), []byte("abd"), 0600)
			case "wrong lock":
				e.LockDigest = "sha256:" + strings.Repeat("c", 64)
			case "wrong bootstrap":
				e.BootstrapDigest = "sha256:" + strings.Repeat("c", 64)
			case "bad digest":
				digest = "../../other"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err != nil {
				t.Fatal(err)
			}
			p, err := OpenPreparedDirectoryFD(ctx, fd, digest, e.LockDigest, e.BootstrapDigest)
			if err == nil {
				p.Close()
				t.Fatal("accepted invalid snapshot")
			}
		})
	}
}
