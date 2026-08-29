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
	head, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("candidate")), Timestamp: time.Unix(1, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}})
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
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("one")), Timestamp: time.Unix(2, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err != nil {
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
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("local")), Timestamp: time.Unix(3, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Push(ctx, worktree); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("unexpected remote push=%v", err)
	}
}

func TestRunnerExactArgvScrubsHooksAndCredentialEnvironment(t *testing.T) {
	var got []string
	var environment []string
	t.Setenv("SF_HOST_SECRET", "must-not-reach-git")
	runner := Runner{Home: filepath.Join(t.TempDir(), "isolated-home"), Run: func(_ context.Context, binary string, argv, env []string) ([]byte, error) {
		if binary != "/usr/bin/git" {
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
	if _, err := (Runner{Home: home}).one(context.Background(), "/repo", "rev-parse", "HEAD"); err == nil {
		t.Fatal("broad HOME was accepted")
	}
	runner := Runner{Home: filepath.Join(t.TempDir(), "isolated-home"), Run: func(context.Context, string, []string, []string) ([]byte, error) {
		return bytes.Repeat([]byte("x"), maxGitOutput+1), nil
	}}
	if _, err := runner.one(context.Background(), "/repo", "rev-parse", "HEAD"); !errors.Is(err, ErrOutputBound) {
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
	if validOID("deadbeef") || validRef("main..bad") || validRepoPath("../escape") || validRepoPath("dir\\escape") {
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
		if !injected && slicesContain(argv, "commit") {
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
	if _, err := runner.Commit(ctx, worktree, CommitRequest{EvidenceDigest: digest([]byte("candidate")), Timestamp: time.Unix(4, 0), BaseRef: "main", Policy: DiffPolicy{AllowedPaths: []string{"src"}}}); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("post-commit staged race=%v", err)
	}
	if !injected {
		t.Fatal("staging race fixture did not run")
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

func TestHTTPSCredentialHelperUsesExplicitGHConfigOnly(t *testing.T) {
	var argv, env []string
	root := t.TempDir()
	ghBinary := filepath.Join(root, "gh")
	ghConfig := filepath.Join(root, "gh-config")
	if err := os.WriteFile(ghBinary, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(ghConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Home: filepath.Join(root, "git-home"), GHBinary: ghBinary, GHConfigDir: ghConfig, Run: func(_ context.Context, _ string, gotArgv, gotEnv []string) ([]byte, error) {
		argv, env = gotArgv, gotEnv
		return []byte("ok\n"), nil
	}}
	if _, err := runner.one(context.Background(), "/repo", "status"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(argv, "\x00"), "credential.helper=!"+ghBinary+" auth git-credential") || !strings.Contains(strings.Join(env, "\n"), "GH_CONFIG_DIR="+runner.GHConfigDir) {
		t.Fatalf("missing explicit HTTPS helper argv=%q env=%q", argv, env)
	}
}

func TestGitDeadlineIsCappedAndCredentialHelperRejectsShellSyntax(t *testing.T) {
	root := t.TempDir()
	parent, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()
	runner := Runner{Home: filepath.Join(root, "git-home"), Run: func(ctx context.Context, _ string, _ []string, _ []string) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 2*time.Minute+time.Second {
			t.Fatalf("git deadline was not capped: %v", deadline)
		}
		return []byte("ok\n"), nil
	}}
	if _, err := runner.one(parent, "/repo", "status"); err != nil {
		t.Fatal(err)
	}
	unsafeBinary := filepath.Join(root, "gh;touch-pwned")
	if err := os.WriteFile(unsafeBinary, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "gh-config")
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	unsafe := Runner{Home: filepath.Join(root, "other-home"), GHBinary: unsafeBinary, GHConfigDir: config, Run: runner.Run}
	if _, err := unsafe.one(context.Background(), "/repo", "status"); err == nil {
		t.Fatal("shell-bearing credential helper path was accepted")
	}
}
