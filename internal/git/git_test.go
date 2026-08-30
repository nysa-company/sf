package git

import (
	"bytes"
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

type memoryBranchAuthority struct{ branches map[string]string }

func (a *memoryBranchAuthority) LoadBranch(_ context.Context, key string) (string, error) {
	return a.branches[key], nil
}

func (a *memoryBranchAuthority) LoadOrStoreBranch(_ context.Context, key, proposed string) (string, error) {
	if existing := a.branches[key]; existing != "" {
		return existing, nil
	}
	a.branches[key] = proposed
	return proposed, nil
}

func allocatorForTest() Allocator {
	return Allocator{
		Authority: &memoryBranchAuthority{branches: map[string]string{}},
		Random:    bytes.NewReader(bytes.Repeat([]byte{0x42}, 128)),
	}
}

type lookupBranchAuthority struct {
	stored string
	called bool
}

func (a *lookupBranchAuthority) LoadBranch(_ context.Context, _ string) (string, error) {
	a.called = true
	return a.stored, nil
}

func (a *lookupBranchAuthority) LoadOrStoreBranch(_ context.Context, _ string, proposed string) (string, error) {
	return proposed, nil
}

func TestAllocatorReadsPersistedBranchBeforeRandomGeneration(t *testing.T) {
	stored := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	authority := &lookupBranchAuthority{stored: stored}
	allocator := Allocator{Authority: authority, Random: errorReader{}}
	branch, err := allocator.Allocate(context.Background(), domain.ChannelDev, "project", "SF-existing")
	if err != nil || branch != stored || !authority.called {
		t.Fatalf("persisted branch=%q err=%v lookup=%v", branch, err, authority.called)
	}
}

func TestAllocatorRejectsPersistedBranchFromAnotherChannel(t *testing.T) {
	authority := &lookupBranchAuthority{stored: "sf/stable/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"}
	if _, err := (Allocator{Authority: authority, Random: errorReader{}}).Allocate(context.Background(), domain.ChannelDev, "project", "SF-existing"); err == nil {
		t.Fatal("persisted branch crossed channel boundary")
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("random generation must not run") }

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

func TestAllocatorDelegatesBranchPersistenceToAuthority(t *testing.T) {
	allocator := allocatorForTest()
	first, err := allocator.Allocate(context.Background(), domain.ChannelDev, "nysa", "SF-29")
	if err != nil {
		t.Fatal(err)
	}
	second, err := allocator.Allocate(context.Background(), domain.ChannelDev, "nysa", "SF-29")
	if err != nil {
		t.Fatal(err)
	}
	other, err := allocator.Allocate(context.Background(), domain.ChannelStable, "nysa", "SF-29")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first == other || !strings.HasPrefix(first, "sf/dev/") || len(strings.Split(first, "-")) < 2 {
		t.Fatalf("branch identities first=%q second=%q other=%q", first, second, other)
	}
}

func TestWorktreeCommitPushAndLostResponseReconciliation(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-43")
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
	head, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("candidate")), Timestamp: time.Unix(1, 0), BaseRef: "main", ExpectedParent: worktree.Identity.BaseHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}})
	if err != nil {
		t.Fatal(err)
	}
	pushed, err := runner.Push(ctx, worktree, head)
	if err != nil || pushed != head {
		t.Fatalf("push=%q err=%v", pushed, err)
	}
	// A response lost after the server accepted the ref is reconciled by a
	// fresh exact remote observation, so a replay cannot create a second push.
	replayed, err := runner.Push(ctx, worktree, head)
	if err != nil || replayed != head {
		t.Fatalf("replay=%q err=%v", replayed, err)
	}
	remoteHead := rawGit(t, repository, "ls-remote", "--heads", "origin", "refs/heads/"+branch)
	if !strings.HasPrefix(remoteHead, head+"\t") {
		t.Fatalf("remote=%q head=%q", remoteHead, head)
	}
	if replay, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("candidate")), Timestamp: time.Unix(1, 0), BaseRef: "main", ExpectedParent: worktree.Identity.BaseHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err != nil || replay != head {
		t.Fatalf("commit replay=%q err=%v", replay, err)
	}
}

func TestCreateWorktreeAdoptsExistingAuthenticatedPath(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-adopt")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	first, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	second, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity != second.Identity || first.Path != second.Path {
		t.Fatalf("adopted identity changed: first=%+v second=%+v", first, second)
	}
}

func TestGitCancellationKillsProcessGroup(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "git-wrapper")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n(sleep 5) &\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "directory")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := (Runner{Binary: script, Home: filepath.Join(root, "home")}).one(ctx, directory, "status")
	if err == nil {
		t.Fatal("canceled git command succeeded")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("descendant cleanup exceeded bound: %s", elapsed)
	}
}

func TestCleanupUsesIndependentContextAfterCancellation(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-cancel-cleanup")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	_, err = runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := openPinnedDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	pointer := filepath.Join(path, ".git")
	if err := os.WriteFile(pointer, []byte("invalid\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	err = runner.cleanupCreatedWorktree(canceled, repository, path, branch, "main", pinned)
	_ = pinned.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("worktree remained: %v", statErr)
	}
	if output, branchErr := exec.Command("git", "-C", repository, "rev-parse", "--verify", "refs/heads/"+branch).CombinedOutput(); branchErr == nil {
		t.Fatalf("branch remained after canceled cleanup: %q", output)
	}
}

func TestWorktreeRemovalAndDiffHostileFixturesRefuse(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-41")
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
	if err := os.WriteFile(filepath.Join(path, "src", "executable"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("executable-mode validation=%v", err)
	}
	if err := os.Remove(filepath.Join(path, "src", "executable")); err != nil {
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
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-env")
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
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-remote")
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
	firstHead, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("one")), Timestamp: time.Unix(2, 0), BaseRef: "main", ExpectedParent: worktree.Identity.BaseHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Push(ctx, worktree, firstHead); err != nil {
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
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("local")), Timestamp: time.Unix(3, 0), BaseRef: "main", ExpectedParent: firstHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Push(ctx, worktree, rawGit(t, path, "rev-parse", "HEAD")); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("unexpected remote push=%v", err)
	}
}

func TestRunnerExactArgvScrubsHooksAndCredentialEnvironment(t *testing.T) {
	var got []string
	var environment []string
	directory := t.TempDir()
	t.Setenv("SF_HOST_SECRET", "must-not-reach-git")
	runner := Runner{Home: filepath.Join(t.TempDir(), "isolated-home"), Run: func(_ context.Context, binary string, argv, env []string) ([]byte, error) {
		if binary != "/usr/bin/git" {
			t.Fatalf("binary=%q", binary)
		}
		got, environment = argv, env
		return []byte("deadbeef\n"), nil
	}}
	if _, err := runner.one(context.Background(), directory, "rev-parse", "HEAD"); err != nil {
		t.Fatal(err)
	}
	want := []string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always",
		"-c", "credential.helper=", "-c", "core.fsmonitor=false", "-c", "core.sshCommand=",
		"-c", "core.askPass=", "-c", "core.pager=", "-c", "commit.gpgsign=false",
		"-c", "tag.gpgsign=false", "-c", "interactive.diffFilter=", "rev-parse", "HEAD"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("argv=%q", got)
	}
	joined := strings.Join(environment, "\n")
	for _, forbidden := range []string{"GIT_DIR=", "GIT_WORK_TREE=", "GIT_CONFIG=", "SF_HOST_SECRET="} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("environment leaks %s", forbidden)
		}
	}
	if !strings.Contains(joined, "HOME="+runner.Home) || !strings.Contains(joined, "GIT_TERMINAL_PROMPT=0") {
		t.Fatalf("environment missing isolation: %q", joined)
	}
}

func TestRunnerRequiresSecureAbsoluteHomeAndBoundsSeamOutput(t *testing.T) {
	if _, err := (Runner{}).one(context.Background(), "/repo", "rev-parse", "HEAD"); err == nil {
		t.Fatal("empty HOME was accepted")
	}
	home := filepath.Join(t.TempDir(), "broad-home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(home, 0o755); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	if _, err := (Runner{Home: home}).one(context.Background(), directory, "rev-parse", "HEAD"); err == nil {
		t.Fatal("broad HOME was accepted")
	}
	runner := Runner{Home: filepath.Join(t.TempDir(), "isolated-home"), Run: func(context.Context, string, []string, []string) ([]byte, error) {
		return bytes.Repeat([]byte("x"), maxGitOutput+1), nil
	}}
	if _, err := runner.one(context.Background(), directory, "rev-parse", "HEAD"); !errors.Is(err, ErrOutputBound) {
		t.Fatalf("bounded output=%v", err)
	}
}

func TestOriginAndHooksRefuseAmbiguityAndSymlinkEscape(t *testing.T) {
	for _, origin := range []string{
		"git@example.test:owner/repository.git",
		"http://example.test/owner/repository.git",
		"https://token@example.test/owner/repository.git",
		"https://example.test/owner/repository.git?token=x",
		"relative/repository.git",
	} {
		if _, err := safeOrigin(origin); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("origin %q err=%v", origin, err)
		}
	}
	root := t.TempDir()
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(root, "hook")); err != nil {
		t.Fatal(err)
	}
	if _, err := treeDigest(root); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("hook symlink=%v", err)
	}
}

func TestDiffAllowsPreexistingExecutableAndSymlinkButRejectsChangedOnes(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	if err := os.WriteFile(filepath.Join(repository, "src", "baseline-executable"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("main.txt", filepath.Join(repository, "src", "baseline-link")); err != nil {
		t.Fatal(err)
	}
	rawGit(t, repository, "add", ".")
	rawGit(t, repository, "commit", "-m", "baseline fixtures")
	rawGit(t, repository, "push", "origin", "main:refs/heads/main")
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-baseline")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "src", "main.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, worktree.Path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); err != nil {
		t.Fatalf("preexisting baseline files were rejected: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path, "src", "new-executable"), []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runner.ValidateDiff(ctx, worktree.Path, "main", DiffPolicy{AllowedPaths: []string{"src"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("changed executable=%v", err)
	}
}

func TestExactOIDRefPathAndChangedPathValidation(t *testing.T) {
	if validOID("deadbeef") || validRef("main..bad") || validRef("--detach") || validRepoPath("../escape") || validRepoPath("dir\\escape") {
		t.Fatal("unsafe identity value accepted")
	}
	if !validOID(strings.Repeat("a", 40)) || !validRef("refs/heads/main") || !validRepoPath("src/main.go") {
		t.Fatal("valid identity value refused")
	}
}

func TestPreflightAndRemovalRequireAuthenticatedPrimaryRepository(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	if err := runner.PreflightRepository(ctx, repository, "main"); err != nil {
		t.Fatal(err)
	}
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-primary")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "other")
	if err := os.Mkdir(other, 0o700); err != nil {
		t.Fatal(err)
	}
	rawGit(t, other, "init", "-b", "main")
	if err := runner.RemoveWorktree(ctx, other, worktree, WorktreeState{}); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("foreign primary removal=%v", err)
	}
}

func TestCommitBoundsUntrustedEvidenceAndMessage(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-commit-bound")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: strings.Repeat("x", 201), Timestamp: time.Now(), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err == nil {
		t.Fatal("oversized evidence accepted")
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: "sha256:test", Timestamp: time.Now(), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err == nil {
		t.Fatal("noncanonical evidence digest accepted")
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("test")), Message: "bad\x00message", Timestamp: time.Now(), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err == nil {
		t.Fatal("unsafe message accepted")
	}
}

func TestCommitRevalidatesResultingTreeAfterStagingRace(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-post-commit")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree.Path, "src", "main.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	injected := false
	runner.Run = func(ctx context.Context, binary string, argv, env []string) ([]byte, error) {
		if !injected && slicesContainPrefix(argv, "commit") {
			injected = true
			if err := os.WriteFile(filepath.Join(worktree.Path, "outside.txt"), []byte("raced\n"), 0o600); err != nil {
				return nil, err
			}
			stage := exec.CommandContext(ctx, binary, "-C", worktree.Path, "add", "--", "outside.txt")
			stage.Env = env
			if output, err := stage.CombinedOutput(); err != nil {
				return output, err
			}
		}
		command := exec.CommandContext(ctx, binary, argv...)
		command.Env = env
		return command.CombinedOutput()
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("candidate")), Timestamp: time.Unix(4, 0), BaseRef: "main", ExpectedParent: worktree.Identity.BaseHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("post-commit staged race=%v", err)
	}
	if !injected {
		t.Fatal("staging race fixture did not run")
	}
}

func TestCommitRejectsIndexRaceBeforeWriteTreeWithoutAdvancingBranch(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-write-tree-race")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree.Path, "src", "main.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	injected := false
	runner.Run = func(runCtx context.Context, binary string, argv, env []string) ([]byte, error) {
		if !injected && slicesContain(argv, "write-tree") {
			injected = true
			if err := os.WriteFile(filepath.Join(worktree.Path, "outside.txt"), []byte("raced\n"), 0o600); err != nil {
				return nil, err
			}
			stage := exec.CommandContext(runCtx, binary, "-C", worktree.Path, "add", "--", "outside.txt")
			stage.Env = env
			if output, err := stage.CombinedOutput(); err != nil {
				return output, err
			}
		}
		command := exec.CommandContext(runCtx, binary, argv...)
		command.Env = env
		return command.CombinedOutput()
	}
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("candidate")), Timestamp: time.Unix(6, 0), BaseRef: "main", ExpectedParent: worktree.Identity.BaseHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("write-tree index race=%v", err)
	}
	if !injected {
		t.Fatal("write-tree race fixture did not run")
	}
	if head := rawGit(t, worktree.Path, "rev-parse", "HEAD"); head != worktree.Identity.BaseHead {
		t.Fatalf("unsafe tree advanced candidate branch: got=%q want=%q", head, worktree.Identity.BaseHead)
	}
}

func slicesContain(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func slicesContainPrefix(values []string, prefix string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}

func TestHTTPSCredentialHelperConfigurationFailsClosed(t *testing.T) {
	root := t.TempDir()
	runner := Runner{Home: filepath.Join(root, "git-home"), GHBinary: filepath.Join(root, "gh"), GHConfigDir: filepath.Join(root, "gh-config")}
	if _, err := runner.one(context.Background(), root, "status"); !errors.Is(err, ErrHTTPSCredentialBoundary) {
		t.Fatalf("credential helper configuration=%v", err)
	}
	if strings.Contains(strings.Join((Runner{}).commandArgs("."), "\x00"), "credential.helper=!") {
		t.Fatal("shell-form credential helper remains in git argv")
	}
	branch := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	if _, err := (Runner{}).Push(context.Background(), Worktree{Branch: branch, Identity: Identity{PushOrigin: "https://example.test/owner/repository.git"}}, strings.Repeat("a", 40)); !errors.Is(err, ErrHTTPSCredentialBoundary) {
		t.Fatalf("HTTPS publication did not fail closed: %v", err)
	}
}

func TestGitDeadlineIsCapped(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "directory")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()
	runner := Runner{Home: filepath.Join(root, "git-home"), Run: func(ctx context.Context, _ string, _ []string, _ []string) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 2*time.Minute+time.Second {
			t.Fatalf("git deadline was not capped: %v", deadline)
		}
		return []byte("ok\n"), nil
	}}
	if _, err := runner.one(parent, directory, "status"); err != nil {
		t.Fatal(err)
	}
}

func TestValidateDiffRenameIncludesDeletedEndpoint(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-rename")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(worktree.Path, "src", "main.txt"), filepath.Join(worktree.Path, "safe.txt")); err != nil {
		t.Fatal(err)
	}
	// The new endpoint is allowed, but the deleted endpoint is outside policy.
	// Rename detection must not hide that deletion.
	if err := runner.ValidateDiff(ctx, worktree.Path, "main", DiffPolicy{AllowedPaths: []string{"safe.txt"}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("rename deletion bypassed policy: %v", err)
	}
}

func TestSnapshotRejectsGitPointerSymlinkAndHardlink(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-pointer")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	pointer := filepath.Join(worktree.Path, ".git")
	contents, err := os.ReadFile(pointer)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(pointer); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "pointer"), pointer); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Snapshot(ctx, worktree.Path, "main"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("git pointer symlink accepted: %v", err)
	}
	if err := os.Remove(pointer); err != nil {
		t.Fatal(err)
	}
	backing := filepath.Join(t.TempDir(), "pointer")
	if err := os.WriteFile(backing, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(backing, pointer); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Snapshot(ctx, worktree.Path, "main"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("git pointer hardlink accepted: %v", err)
	}
}

func TestInspectRejectsWorktreePathRebinding(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-path")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	worktree.Path = filepath.Join(t.TempDir(), "attacker")
	if err := runner.InspectWorktree(ctx, worktree); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("rebound worktree path accepted: %v", err)
	}
}

func TestSnapshotBindsRequestedRepositoryAndTopLevel(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-bind")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign")
	if err := os.Mkdir(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.snapshotExpected(ctx, foreign, worktree.Path, "main"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("foreign requested repository accepted: %v", err)
	}
}

func TestSnapshotRejectsHooksRootSymlink(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-hooks")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	common := worktree.Identity.CommonDir
	hooks := filepath.Join(common, "hooks")
	backup := hooks + ".real"
	if err := os.Rename(hooks, backup); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(backup, hooks)
	if err := os.Symlink(filepath.Join(t.TempDir(), "foreign-hooks"), hooks); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Snapshot(ctx, worktree.Path, "main"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("hooks root symlink accepted: %v", err)
	}
}

func TestRemoveRejectsWorktreeReplacementAtEffect(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-remove-race")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	targetPath := worktree.Path
	replacement := targetPath + ".replacement"
	replaced := false
	runner.Run = func(runCtx context.Context, binary string, argv, env []string) ([]byte, error) {
		if !replaced && slicesContain(argv, "remove") && slicesContain(argv, targetPath) {
			replaced = true
			if err := os.Rename(targetPath, replacement); err != nil {
				return nil, err
			}
			if err := os.Mkdir(targetPath, 0o700); err != nil {
				return nil, err
			}
			return nil, nil
		}
		command := exec.CommandContext(runCtx, binary, argv...)
		command.Env = env
		return command.CombinedOutput()
	}
	if err := runner.RemoveWorktree(ctx, repository, worktree, WorktreeState{}); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("worktree replacement reached removal effect: %v", err)
	}
	if !replaced {
		t.Fatal("removal race fixture did not run")
	}
	if _, err := os.Stat(replacement); err != nil {
		t.Fatalf("replacement fixture disappeared: %v", err)
	}
}

func TestCreateWorktreeCleansAfterSnapshotAuthenticationFailure(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-cleanup")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	canonicalPath, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	canonicalPath = filepath.Join(canonicalPath, filepath.Base(path))
	var sabotage bool
	runner.Run = func(runCtx context.Context, binary string, argv, env []string) ([]byte, error) {
		if sabotage && slicesContain(argv, "--show-toplevel") && slicesContain(argv, canonicalPath) {
			sabotage = false
			pointer := filepath.Join(path, ".git")
			if err := os.Remove(pointer); err != nil {
				return nil, err
			}
			if err := os.WriteFile(pointer, []byte("not a git pointer\n"), 0o600); err != nil {
				return nil, err
			}
		}
		command := exec.CommandContext(runCtx, binary, argv...)
		command.Env = env
		return command.CombinedOutput()
	}
	sabotage = true
	if _, err := runner.CreateWorktree(ctx, repository, path, branch, "main"); err == nil {
		t.Fatal("snapshot authentication failure was accepted")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed worktree remained after cleanup: %v", err)
	}
	command := exec.Command("git", "-C", repository, "rev-parse", "--verify", "refs/heads/"+branch)
	if output, err := command.CombinedOutput(); err == nil {
		t.Fatalf("failed worktree branch remained: %q", output)
	}
}

func TestSnapshotRejectsCommandBearingRepositoryConfig(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	for _, key := range []string{
		"credential.helper",
		"filter.evil.clean",
		"core.fsmonitor",
		"url.https://evil/.insteadOf",
		"core.gitproxy",
		"core.pager",
		"interactive.diffFilter",
		"remote.origin.uploadpack",
		"submodule.evil.update",
	} {
		marker := filepath.Join(t.TempDir(), "must-not-run")
		value := marker
		if key == "credential.helper" {
			value = "!touch " + marker
		} else if key == "url.https://evil/.insteadOf" {
			value = "https://trusted/"
		}
		if err := exec.Command("git", "-C", repository, "config", key, value).Run(); err != nil {
			t.Fatal(err)
		}
		branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", domain.TicketID("SF-config-"+key))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "worktree")
		worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main")
		if !errors.Is(err, ErrIdentityMismatch) || worktree.Path != "" {
			t.Fatalf("command-bearing config %q accepted: worktree=%+v err=%v", key, worktree, err)
		}
		if _, statErr := os.Lstat(marker); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("command-bearing config %q executed helper: %v", key, statErr)
		}
	}
}

func TestSnapshotBindsPushURLAndUsesItForPublication(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	pushRemote := filepath.Join(t.TempDir(), "push.git")
	rawGit(t, t.TempDir(), "init", "--bare", pushRemote)
	rawGit(t, repository, "remote", "set-url", "--push", "origin", pushRemote)
	canonicalPushRemote, err := filepath.EvalSymlinks(pushRemote)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-push-url")
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := runner.CreateWorktree(ctx, repository, filepath.Join(t.TempDir(), "worktree"), branch, "main")
	if err != nil {
		t.Fatal(err)
	}
	if worktree.Identity.PushOrigin != canonicalPushRemote {
		t.Fatalf("push origin=%q want=%q", worktree.Identity.PushOrigin, canonicalPushRemote)
	}
	if err := os.WriteFile(filepath.Join(worktree.Path, "src", "push.txt"), []byte("push\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	head, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("push")), Timestamp: time.Unix(5, 0), BaseRef: "main", ExpectedParent: worktree.Identity.BaseHead, Policy: DiffPolicy{AllowedPaths: []string{"src"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Push(ctx, worktree, head); err != nil {
		t.Fatal(err)
	}
	if got := rawGit(t, pushRemote, "rev-parse", "refs/heads/"+branch); got != head {
		t.Fatalf("push remote head=%q want=%q", got, head)
	}
}

func TestCommandRejectsDirectoryReplacementDuringRun(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "directory")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	replacement := directory + ".replacement"
	runner := Runner{Home: filepath.Join(t.TempDir(), "home"), Run: func(_ context.Context, _ string, _ []string, _ []string) ([]byte, error) {
		if err := os.Rename(directory, replacement); err != nil {
			return nil, err
		}
		return nil, os.Mkdir(directory, 0o700)
	}}
	if _, err := runner.one(context.Background(), directory, "status"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("directory replacement accepted: %v", err)
	}
}

func TestStrictBranchGrammarMatchesGit(t *testing.T) {
	valid := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	if _, err := validateAllocatedBranch(domain.ChannelDev, valid); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		"sf/stable/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef",
		"sf/dev/0123456789abcdef/0123456789abcdef-no-random-suffix",
		"sf/dev/.foo/0123456789abcdef-0123456789abcdef0123456789abcdef",
		"sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcde.",
	} {
		if _, err := validateAllocatedBranch(domain.ChannelDev, invalid); err == nil {
			t.Fatalf("invalid branch accepted: %q", invalid)
		}
	}
	if err := exec.Command("git", "check-ref-format", valid).Run(); err != nil {
		t.Fatalf("valid allocated branch rejected by Git: %v", err)
	}
}
