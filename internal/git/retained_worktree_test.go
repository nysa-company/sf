package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestRetainedWorktreeFingerprintBindsEditsAndIndexWithoutMutation(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-retained")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	inspect := func() {
		t.Helper()
		before := rawGit(t, path, "status", "--porcelain=v1", "--untracked-files=all")
		index := rawGit(t, path, "ls-files", "--stage")
		head := rawGit(t, path, "rev-parse", "HEAD")
		value, err := runner.InspectRetainedWorktree(ctx, worktree)
		if err != nil || value.Changes.Head != head || !strings.HasPrefix(value.Digest, "sha256:") || len(value.Digest) != 71 || seen[value.Digest] {
			t.Fatalf("inspection=%+v err=%v duplicate=%v", value, err, seen[value.Digest])
		}
		seen[value.Digest] = true
		replay, err := runner.InspectRetainedWorktree(ctx, worktree)
		if err != nil || replay.Digest != value.Digest {
			t.Fatalf("unstable inspection: %+v %v", replay, err)
		}
		if rawGit(t, path, "status", "--porcelain=v1", "--untracked-files=all") != before || rawGit(t, path, "ls-files", "--stage") != index || rawGit(t, path, "rev-parse", "HEAD") != head {
			t.Fatal("inspection mutated worktree/index/head")
		}
	}
	write := func(name, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(path, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	inspect()
	write("src/main.txt", "changed\n")
	inspect()
	rawGit(t, path, "add", "src/main.txt")
	inspect() // Same file bytes, different staged/unstaged partition.
	write("src/main.txt", "changed again\n")
	inspect()
	write("src/new.txt", "untracked\x00bytes")
	inspect()
	write("src/new.txt", "untracked\x00other")
	inspect()
	if err := os.Remove(filepath.Join(path, "src/main.txt")); err != nil {
		t.Fatal(err)
	}
	inspect()
	if err := os.Chmod(filepath.Join(path, "src/new.txt"), 0700); err != nil {
		t.Fatal(err)
	}
	inspect()
	foreign := worktree
	foreign.Identity.WorktreeIno++
	if _, err := runner.InspectRetainedWorktree(ctx, foreign); err == nil {
		t.Fatal("accepted foreign worktree identity")
	}
	// Registration authenticates the checkout, not a retained source HEAD.
	// Store callers must compare the observed HEAD against their exact witness.
	rawGit(t, path, "add", ".")
	rawGit(t, path, "commit", "-m", "foreign source head")
	changedHead, err := runner.InspectRetainedWorktree(ctx, worktree)
	if err != nil || changedHead.Changes.Head == worktree.Identity.BaseHead || seen[changedHead.Digest] {
		t.Fatalf("foreign head was not distinguished: %+v %v", changedHead, err)
	}
}

func TestRetainedWorktreeRefusesIgnoredFiles(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-ignored-retained")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{".gitignore": "hidden\n", "hidden": "must refuse\n"} {
		if err := os.WriteFile(filepath.Join(path, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runner.InspectRetainedWorktree(ctx, worktree); err == nil {
		t.Fatal("accepted ignored file")
	}
}

func TestRetainedFileReaderBoundsAndNoFollow(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "source"), []byte("source bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("source", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(root, filepath.Join(root, "directory-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "large"), make([]byte, retainedWorktreeMaxFileBytes+1), 0600); err != nil {
		t.Fatal(err)
	}
	pinned, err := openPinnedDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	for _, path := range []string{"link", "directory-link/source", "large", "../source", "."} {
		budget := retainedWorktreeMaxBytes
		if _, err := readRetainedFile(context.Background(), int(pinned.file.Fd()), path, &budget); err == nil {
			t.Fatalf("accepted %q", path)
		}
	}
	budget := 1
	if _, err := readRetainedFile(context.Background(), int(pinned.file.Fd()), "source", &budget); err == nil {
		t.Fatal("exceeded aggregate byte budget")
	}
	budget = retainedWorktreeMaxBytes
	deleted, err := readRetainedFile(context.Background(), int(pinned.file.Fd()), "missing/file", &budget)
	if err != nil || !deleted.Deleted {
		t.Fatalf("deletion=%+v %v", deleted, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readRetainedFile(ctx, int(pinned.file.Fd()), "source", &budget); err == nil {
		t.Fatal("ignored cancellation")
	}
}
