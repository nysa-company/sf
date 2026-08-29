package git

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func rawGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func fixture(t *testing.T) (context.Context, Runner, string, string) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	remote := filepath.Join(root, "remote.git")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	rawGit(t, root, "init", "--bare", remote)
	rawGit(t, repository, "init", "-b", "main")
	rawGit(t, repository, "config", "user.name", "fixture")
	rawGit(t, repository, "config", "user.email", "fixture@example.test")
	if err := os.MkdirAll(filepath.Join(repository, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "src", "main.txt"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rawGit(t, repository, "add", ".")
	rawGit(t, repository, "commit", "-m", "base")
	rawGit(t, repository, "remote", "add", "origin", remote)
	rawGit(t, repository, "push", "origin", "main:refs/heads/main")
	return ctx, Runner{Home: filepath.Join(root, "git-home")}, repository, remote
}

func TestAllocatorPersistsCryptoRandomTicketBranch(t *testing.T) {
	allocator := Allocator{Root: t.TempDir()}
	first, err := allocator.Allocate(domain.ChannelDev, "nysa", "SF-29")
	if err != nil {
		t.Fatal(err)
	}
	second, err := allocator.Allocate(domain.ChannelDev, "nysa", "SF-29")
	if err != nil {
		t.Fatal(err)
	}
	other, err := allocator.Allocate(domain.ChannelStable, "nysa", "SF-29")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first == other || !strings.HasPrefix(first, "sf/dev/nysa/SF-29-") || len(strings.Split(first, "-")) < 2 {
		t.Fatalf("branch identities first=%q second=%q other=%q", first, second, other)
	}
}

func TestWorktreeCommitPushAndLostResponseReconciliation(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := (Allocator{Root: t.TempDir()}).Allocate(domain.ChannelDev, "project", "SF-43")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "src", "main.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	head, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: "sha256:candidate", Timestamp: time.Unix(1, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}})
	if err != nil {
		t.Fatal(err)
	}
	pushed, err := runner.Push(ctx, worktree)
	if err != nil || pushed != head {
		t.Fatalf("push=%q err=%v", pushed, err)
	}
	// A response lost after the server accepted the ref is reconciled by a
	// fresh exact remote observation, so a replay cannot create a second push.
	replayed, err := runner.Push(ctx, worktree)
	if err != nil || replayed != head {
		t.Fatalf("replay=%q err=%v", replayed, err)
	}
	remoteHead := rawGit(t, repository, "ls-remote", "--heads", "origin", "refs/heads/"+branch)
	if !strings.HasPrefix(remoteHead, head+"\t") {
		t.Fatalf("remote=%q head=%q", remoteHead, head)
	}
}

func TestWorktreeRemovalAndDiffHostileFixturesRefuse(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := (Allocator{Root: t.TempDir()}).Allocate(domain.ChannelDev, "project", "SF-41")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(path, "src", "escape")); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("symlink validation=%v", err)
	}
	if err := os.Remove(filepath.Join(path, "src", "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(path, "src", "main.txt"), filepath.Join(path, "src", "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("hardlink validation=%v", err)
	}
	if err := os.Remove(filepath.Join(path, "src", "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(path, "src", "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("special-file validation=%v", err)
	}
	if err := os.Remove(filepath.Join(path, "src", "pipe")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(path, "src", "nested", ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("nested-repository validation=%v", err)
	}
	if err := os.RemoveAll(filepath.Join(path, "src", "nested")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "src", "untracked"), []byte("dirty"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.RemoveWorktree(ctx, repository, worktree, WorktreeState{}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("dirty remove=%v", err)
	}
	if err := runner.RemoveWorktree(ctx, repository, worktree, WorktreeState{Taken: true}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("taken remove=%v", err)
	}
}

func TestIdentityRejectsControlPlaneEnvironment(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := (Allocator{Root: t.TempDir()}).Allocate(domain.ChannelDev, "project", "SF-env")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "attacker"))
	if err := runner.InspectWorktree(ctx, worktree); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("GIT_DIR identity=%v", err)
	}
}

func TestUnexpectedRemoteHeadNeverOverwritten(t *testing.T) {
	ctx, runner, repository, remote := fixture(t)
	branch, err := (Allocator{Root: t.TempDir()}).Allocate(domain.ChannelDev, "project", "SF-remote")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "src", "main.txt"), []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: "sha256:one", Timestamp: time.Unix(2, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Push(ctx, worktree); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "other")
	rawGit(t, t.TempDir(), "clone", remote, other)
	rawGit(t, other, "checkout", branch)
	rawGit(t, other, "config", "user.name", "other")
	rawGit(t, other, "config", "user.email", "other@example.test")
	if err := os.WriteFile(filepath.Join(other, "src", "remote.txt"), []byte("remote\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rawGit(t, other, "add", ".")
	rawGit(t, other, "commit", "-m", "remote")
	rawGit(t, other, "push", "origin", branch+":refs/heads/"+branch)
	if err := os.WriteFile(filepath.Join(path, "src", "local.txt"), []byte("local\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: "sha256:local", Timestamp: time.Unix(3, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Push(ctx, worktree); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("unexpected remote push=%v", err)
	}
}

func TestRunnerExactArgvScrubsHooksAndCredentialEnvironment(t *testing.T) {
	var got []string
	var environment []string
	runner := Runner{Home: t.TempDir(), Run: func(_ context.Context, binary string, argv, env []string) ([]byte, error) {
		if binary != "git" {
			t.Fatalf("binary=%q", binary)
		}
		got, environment = argv, env
		return []byte("deadbeef\n"), nil
	}}
	if _, err := runner.one(context.Background(), "/repo", "rev-parse", "HEAD"); err != nil {
		t.Fatal(err)
	}
	want := []string{"-C", "/repo", "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always", "rev-parse", "HEAD"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv=%q", got)
	}
	joined := strings.Join(environment, "\n")
	for _, forbidden := range []string{"GIT_DIR=", "GIT_WORK_TREE=", "GIT_CONFIG=", "HOME="} {
		if forbidden != "HOME=" && strings.Contains(joined, forbidden) {
			t.Fatalf("environment leaks %s", forbidden)
		}
	}
	if !strings.Contains(joined, "HOME="+runner.Home) || !strings.Contains(joined, "GIT_TERMINAL_PROMPT=0") {
		t.Fatalf("environment missing isolation: %q", joined)
	}
}
