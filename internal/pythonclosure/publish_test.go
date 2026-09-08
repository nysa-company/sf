package pythonclosure

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func privatePublicationRoot(t *testing.T) (string, int) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	return root, openRoot(t, root)
}

func TestPublishPreparedIsExclusiveAndIdempotent(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS publication")
	}
	stage, stagedFD, digest, env := preparedFixture(t)
	root, fd := privatePublicationRoot(t)
	retained, err := OpenPreparedDirectoryFD(t.Context(), stagedFD, digest, env.LockDigest, env.BootstrapDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Close()
	published, err := PublishPreparedDirectoryFD(t.Context(), fd, stagedFD, digest, env.LockDigest, env.BootstrapDigest)
	if err != nil || !published {
		t.Fatalf("publish=%v error=%v", published, err)
	}
	if err := retained.Revalidate(t.Context()); err != nil {
		t.Fatal("publication invalidated retained snapshot", err)
	}
	if _, err := os.Lstat(filepath.Join(stage, digest[7:])); !os.IsNotExist(err) {
		t.Fatal("staging snapshot was not moved", err)
	}
	before, err := os.Stat(filepath.Join(root, digest[7:]))
	if err != nil {
		t.Fatal(err)
	}
	published, err = PublishPreparedDirectoryFD(t.Context(), fd, stagedFD, digest, env.LockDigest, env.BootstrapDigest)
	if err != nil || published {
		t.Fatalf("replay=%v error=%v", published, err)
	}
	after, err := os.Stat(filepath.Join(root, digest[7:]))
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("replay replaced snapshot", err)
	}
}

func TestPublishPreparedDoesNotRepairCorruptExistingSnapshot(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS publication")
	}
	stage, stagedFD, digest, env := preparedFixture(t)
	root, fd, existingDigest, _ := preparedFixture(t)
	if digest != existingDigest {
		t.Fatal("fixture content differs")
	}
	path := filepath.Join(root, digest[7:], "runtime", "unbound")
	if err := os.WriteFile(path, []byte("retain evidence"), 0400); err != nil {
		t.Fatal(err)
	}
	if published, err := PublishPreparedDirectoryFD(t.Context(), fd, stagedFD, digest, env.LockDigest, env.BootstrapDigest); err == nil || published {
		t.Fatalf("corrupt replay=%v error=%v", published, err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "retain evidence" {
		t.Fatal("existing evidence changed", err)
	}
	if _, err := os.Stat(filepath.Join(stage, digest[7:])); err != nil {
		t.Fatal("unpublished source removed", err)
	}
}

func TestPublishPreparedRefusesWithoutReplacing(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS publication")
	}
	for _, kind := range []string{"cancelled", "wrong lock", "public parent", "same parent", "partial destination", "symlink destination", "changed source"} {
		t.Run(kind, func(t *testing.T) {
			stage, stagedFD, digest, env := preparedFixture(t)
			root, fd := privatePublicationRoot(t)
			ctx := t.Context()
			target := filepath.Join(root, digest[7:])
			switch kind {
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "wrong lock":
				env.LockDigest = env.BootstrapDigest
			case "public parent":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "same parent":
				fd = stagedFD
			case "partial destination":
				if err := os.Mkdir(target, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink destination":
				if err := os.Symlink(filepath.Join(stage, digest[7:]), target); err != nil {
					t.Fatal(err)
				}
			case "changed source":
				if err := os.WriteFile(filepath.Join(stage, digest[7:], "runtime", "extra"), []byte("unbound"), 0400); err != nil {
					t.Fatal(err)
				}
			}
			if published, err := PublishPreparedDirectoryFD(ctx, fd, stagedFD, digest, env.LockDigest, env.BootstrapDigest); err == nil || published {
				t.Fatalf("refusal=%v error=%v", published, err)
			}
			if _, err := os.Stat(filepath.Join(stage, digest[7:])); err != nil {
				t.Fatal("source deleted on refusal", err)
			}
			if kind == "partial destination" || kind == "symlink destination" {
				info, err := os.Lstat(target)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "symlink destination" && info.Mode()&os.ModeSymlink == 0 {
					t.Fatal("link replaced")
				}
			}
		})
	}
}

func TestPublishPreparedConcurrentExactContent(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS publication")
	}
	_, a, digest, env := preparedFixture(t)
	_, b, otherDigest, _ := preparedFixture(t)
	if digest != otherDigest {
		t.Fatal("fixture content differs")
	}
	_, fd := privatePublicationRoot(t)
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	errors := make(chan error, 2)
	for _, source := range []int{a, b} {
		wg.Add(1)
		go func(source int) {
			defer wg.Done()
			published, err := PublishPreparedDirectoryFD(t.Context(), fd, source, digest, env.LockDigest, env.BootstrapDigest)
			results <- published
			errors <- err
		}(source)
	}
	wg.Wait()
	count := 0
	for range 2 {
		if <-results {
			count++
		}
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if count != 1 {
		t.Fatalf("published %d snapshots", count)
	}
}
