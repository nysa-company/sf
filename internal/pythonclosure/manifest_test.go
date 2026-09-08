package pythonclosure

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func fixture(t *testing.T) (string, int, Manifest) {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "lib"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "lib", "sample.py"), []byte("abc"), 0600); err != nil {
		t.Fatal(err)
	}
	fd := openRoot(t, root)
	m, err := CaptureDirectoryFD(t.Context(), fd)
	if err != nil {
		t.Fatal(err)
	}
	return root, fd, m
}

func openRoot(t *testing.T, root string) int {
	t.Helper()
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unix.Close(fd) })
	return fd
}

func TestManifestBindsBytesAndPreservesDirectoryCursor(t *testing.T) {
	_, fd, manifest := fixture(t)
	if len(manifest.Entries) != 2 || manifest.Entries[1].SHA256 != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("unexpected manifest: %+v", manifest)
	}
	for i := 0; i < 3; i++ {
		if err := VerifyDirectoryFD(t.Context(), fd, manifest); err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
	}
	data, digest, err := manifest.Canonical()
	if err != nil || len(data) == 0 || len(digest) != 71 {
		t.Fatalf("canonical: %s %s %v", data, digest, err)
	}
}

func TestManifestRejectsFilesystemChanges(t *testing.T) {
	for _, name := range []string{"bytes", "mode", "extra", "missing", "file symlink", "directory symlink", "fifo"} {
		t.Run(name, func(t *testing.T) {
			root, fd, manifest := fixture(t)
			file := filepath.Join(root, "lib/sample.py")
			var err error
			switch name {
			case "bytes":
				err = os.WriteFile(file, []byte("abd"), 0600)
			case "mode":
				err = os.Chmod(file, 0700)
			case "extra":
				err = os.WriteFile(filepath.Join(root, "extra.py"), nil, 0600)
			case "missing":
				err = os.Remove(file)
			case "file symlink":
				err = os.Remove(file)
				if err == nil {
					err = os.Symlink("../outside", file)
				}
			case "directory symlink":
				err = os.Symlink(t.TempDir(), filepath.Join(root, "alias"))
			case "fifo":
				err = unix.Mkfifo(filepath.Join(root, "fifo"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := VerifyDirectoryFD(t.Context(), fd, manifest); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted mutation: %v", err)
			}
		})
	}
}

func TestManifestUsesAuthenticatedDescriptorAfterPathReplacement(t *testing.T) {
	root, fd, manifest := fixture(t)
	old := root + "-retained"
	if err := os.Rename(root, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(old) })
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "other"), []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyDirectoryFD(t.Context(), fd, manifest); err != nil {
		t.Fatalf("original descriptor redirected: %v", err)
	}
	if err := VerifyDirectoryFD(t.Context(), openRoot(t, root), manifest); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replacement accepted: %v", err)
	}
}

func TestManifestBoundsAndCancellation(t *testing.T) {
	_, fd, _ := fixture(t)
	for _, bound := range []limits{{1, 32, 64, 64}, {10, 0, 64, 64}, {10, 32, 2, 64}, {10, 32, 64, 2}} {
		if _, err := capture(t.Context(), fd, bound); !errors.Is(err, ErrLimit) {
			t.Fatalf("bound %+v: %v", bound, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CaptureDirectoryFD(ctx, fd); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := CaptureDirectoryFD(nil, fd); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context: %v", err)
	}
	if _, err := CaptureDirectoryFD(t.Context(), -1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("bad descriptor: %v", err)
	}
	empty := openRoot(t, t.TempDir())
	if _, err := CaptureDirectoryFD(t.Context(), empty); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty: %v", err)
	}
}

func TestManifestRejectsMalformedEvidence(t *testing.T) {
	_, _, good := fixture(t)
	for _, name := range []string{"version", "duplicate", "order", "parent", "escape", "absolute", "mode", "size", "hash", "uppercase", "directory hash", "kind"} {
		t.Run(name, func(t *testing.T) {
			m := Manifest{Version: good.Version, Entries: append([]Entry(nil), good.Entries...)}
			switch name {
			case "version":
				m.Version = "other"
			case "duplicate":
				m.Entries = append(m.Entries, m.Entries[1])
			case "order":
				m.Entries[0], m.Entries[1] = m.Entries[1], m.Entries[0]
			case "parent":
				m.Entries = m.Entries[1:]
			case "escape":
				m.Entries[1].Path = "../outside"
			case "absolute":
				m.Entries[1].Path = "/outside"
			case "mode":
				m.Entries[1].Mode = 04755
			case "size":
				m.Entries[1].Size = -1
			case "hash":
				m.Entries[1].SHA256 = strings.Repeat("g", 64)
			case "uppercase":
				m.Entries[1].SHA256 = strings.ToUpper(m.Entries[1].SHA256)
			case "directory hash":
				m.Entries[0].SHA256 = strings.Repeat("a", 64)
			case "kind":
				m.Entries[1].Kind = "symlink"
			}
			if _, _, err := m.Canonical(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted %+v: %v", m, err)
			}
		})
	}
}
