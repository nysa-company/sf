package pythonclosure

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func scratchFixture(t *testing.T) (string, int) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	return root, openRoot(t, root)
}

func TestScratchUsageDoesNotFollowLinksOrConsumeCursor(t *testing.T) {
	root, fd := scratchFixture(t)
	if err := os.WriteFile(filepath.Join(root, "data"), []byte("abc"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/missing/outside", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		got, err := InspectScratchDirectoryFD(t.Context(), fd)
		if err != nil || got.Entries != 2 || got.LogicalBytes != int64(3+len("/missing/outside")) {
			t.Fatalf("usage: %+v %v", got, err)
		}
	}
	if err := os.Rename(root, root+"-held"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Rename(root+"-held", root) })
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	// The retained descriptor still observes the original two entries.
	got, err := InspectScratchDirectoryFD(t.Context(), fd)
	if err != nil || got.Entries != 2 {
		t.Fatal("reopened root pathname")
	}
}

func TestScratchBounds(t *testing.T) {
	for _, kind := range []string{"file", "total", "entries", "depth", "fifo", "public", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			root, fd := scratchFixture(t)
			ctx := t.Context()
			switch kind {
			case "file", "total":
				count, size := 1, int64(ScratchFileBytes+1)
				if kind == "total" {
					count, size = 9, ScratchFileBytes
				}
				for i := 0; i < count; i++ {
					f, err := os.Create(filepath.Join(root, fmt.Sprint(i)))
					if err != nil {
						t.Fatal(err)
					}
					err = f.Truncate(size)
					f.Close()
					if err != nil {
						t.Fatal(err)
					}
				}
			case "entries":
				for i := 0; i <= ScratchEntries; i++ {
					f, err := os.Create(filepath.Join(root, fmt.Sprint(i)))
					if err != nil {
						t.Fatal(err)
					}
					f.Close()
				}
			case "depth":
				if err := os.MkdirAll(filepath.Join(root, strings.Repeat("d/", 33)), 0700); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(filepath.Join(root, "pipe"), 0600); err != nil {
					t.Fatal(err)
				}
			case "public":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			_, err := InspectScratchDirectoryFD(ctx, fd)
			if err == nil {
				t.Fatal("accepted exceeded/invalid scratch")
			}
			if kind == "file" || kind == "total" || kind == "depth" {
				if !errors.Is(err, ErrLimit) {
					t.Fatalf("expected limit: %v", err)
				}
			}
		})
	}
}
