package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/executionpolicy"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

func TestRepositoryCommandRejectsCustomExecutableNamedGit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := resolveFixedExecutable(path); err == nil {
		t.Fatal("custom executable named git was accepted")
	}
}

func TestRepositoryCommandDrainerReapsRecordedGroup(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "sleep 30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	start, err := processStartIdentity(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := hostBootIdentity()
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(waited)
	}()
	d := RepositoryCommandDrainer{SoftDrain: 10 * time.Millisecond, HardDrain: 500 * time.Millisecond}
	if err := d.DrainRepositoryCommand(context.Background(), contracts.RepositoryCommandLaunch{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: boot, ProcessStartIdentity: start}); err == nil {
		t.Fatal("restart drainer accepted a vanished leader/old PGID as proof")
	}
	<-waited
}

func TestRepositoryCommandDrainerFailsClosedOnUnclearIdentity(t *testing.T) {
	d := RepositoryCommandDrainer{}
	if err := d.DrainRepositoryCommand(context.Background(), contracts.RepositoryCommandLaunch{PID: 42, PGID: 42}); err == nil {
		t.Fatal("drainer accepted an identity without boot/start proofs")
	}
}

func TestRepositoryCommandPreflightRefusesUnsupportedPlatformBeforeLaunch(t *testing.T) {
	if repositoryCommandPlatformAvailable("linux") || !repositoryCommandPlatformAvailable("darwin") {
		t.Fatal("platform guard accepted unsupported host or rejected darwin")
	}
}

func TestRepositoryCommandWorktreeReplacementBetweenPreflightAndOpenRefuses(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	replacement := filepath.Join(root, "replacement")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replacement, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(worktree)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	identity := gitboundary.Identity{Repository: root, RepositoryDev: 1, RepositoryIno: 1, Worktree: worktree, WorktreeDev: uint64(stat.Dev), WorktreeIno: uint64(stat.Ino), GitFile: worktree + "/.git", GitFileDev: 1, GitFileIno: 1, CommonDir: root + "/.git", CommonDirDev: 1, CommonDirIno: 1, Origin: "ssh://example.test/repo", PushOrigin: "/tmp/origin", PushOriginDev: 1, PushOriginIno: 1, BaseRef: "main", BaseHead: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", HeadRef: "branch", ConfigHash: "x", HooksHash: "y"}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: root, Worktree: worktree, WorktreeIdentity: string(raw), BaseRef: "main", BaseSHA: identity.BaseHead, Branch: "branch"}
	s := RepositoryCommandSupervisor{beforeWorktreeOpen: func() {
		if err := os.Rename(worktree, filepath.Join(root, "old")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, worktree); err != nil {
			t.Fatal(err)
		}
	}}
	if opened, err := s.openAuthenticatedWorktree(claim, identity); err == nil {
		_ = opened.Close()
		t.Fatal("replacement worktree was accepted")
	}
}

func TestRepositoryCommandStagesExecutableBeforeFinalPathReplacement(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "tool")
	original := []byte("#!/bin/sh\necho original\n")
	if err := os.WriteFile(source, original, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(original)
	staged, err := stageExecutable(source, "sha256:"+hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(staged))
	if err := os.WriteFile(source, []byte("#!/bin/sh\necho replacement\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(staged)
	if err != nil || string(got) != string(original) {
		t.Fatalf("staged executable changed after source replacement: %q err=%v", got, err)
	}
}

type repositoryTestLease struct {
	mu     sync.Mutex
	launch contracts.RepositoryCommandLaunch
	groups []contracts.RepositoryCommandLaunch
}

func (l *repositoryTestLease) Check(context.Context) error { return nil }
func (l *repositoryTestLease) Release() error              { return nil }
func (l *repositoryTestLease) Quarantine() error           { return nil }
func (l *repositoryTestLease) RecordRepositoryCommandLaunch(_ context.Context, v contracts.RepositoryCommandLaunch) error {
	l.mu.Lock()
	l.launch = v
	l.mu.Unlock()
	return nil
}
func (l *repositoryTestLease) FinishRepositoryCommandLaunch(context.Context, contracts.RepositoryCommandLaunch) error {
	return nil
}
func (l *repositoryTestLease) RecordRepositoryCommandProcessGroup(_ context.Context, v contracts.RepositoryCommandLaunch) error {
	l.mu.Lock()
	l.groups = append(l.groups, v)
	l.mu.Unlock()
	return nil
}
func (l *repositoryTestLease) groupCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.groups)
}

// This is the real command path: compiled sf gate -> Seatbelt-confined Go
// driver -> durable per-test process-group handshake -> strict sandboxed test
// binary. It catches a broken -exec word split or inherited-FD handshake that
// unit tests of the profile alone cannot see.
func TestRepositoryCommandRunsGoTestThroughTrackedStrictGate(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository command product boundary is macOS")
	}
	repo := t.TempDir()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	mustRepositoryCommand(t, repo, "git", "init")
	mustRepositoryCommand(t, repo, "git", "checkout", "-b", "main")
	mustRepositoryCommand(t, repo, "git", "config", "user.email", "sf@example.test")
	mustRepositoryCommand(t, repo, "git", "config", "user.name", "SF")
	mustRepositoryCommand(t, repo, "git", "remote", "add", "origin", "ssh://git@ssh.github.com:443/example/repository.git")
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module example.test/repository\n\ngo 1.25.0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "proof_test.go"), []byte("package repository\nimport \"testing\"\nfunc TestProof(t *testing.T) {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustRepositoryCommand(t, repo, "git", "add", ".")
	mustRepositoryCommand(t, repo, "git", "commit", "-m", "initial")
	worktree := filepath.Join(t.TempDir(), "worktree")
	mustRepositoryCommand(t, repo, "git", "worktree", "add", "-b", "ticket/proof", worktree)
	helper := filepath.Join(t.TempDir(), "sf-git-exec")
	buildHelper := exec.Command("go", "build", "-o", helper, "./cmd/sf-git-exec")
	buildHelper.Dir = root
	if out, err := buildHelper.CombinedOutput(); err != nil {
		t.Fatalf("build Git helper: %v: %s", err, out)
	}
	gitHome := filepath.Join(t.TempDir(), "git-home")
	if err := os.Mkdir(gitHome, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := gitboundary.Runner{Binary: "/usr/bin/git", ExecHelper: helper, Home: gitHome}
	identity, err := runner.Snapshot(context.Background(), worktree, "main")
	if err != nil {
		t.Fatal(err)
	}
	worktree = identity.Worktree
	identityRaw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	goPath, err := resolveFixedExecutable("go")
	if err != nil {
		t.Fatal(err)
	}
	if err := authenticateRepositorySourceExecutable(goPath); err != nil {
		t.Skipf("approved Go source executable is unavailable: %v", err)
	}
	goBytes, err := os.ReadFile(goPath)
	if err != nil {
		t.Fatal(err)
	}
	goSum := sha256.Sum256(goBytes)
	spec := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: worktree, Timeout: 20 * time.Second, Profile: contracts.ProfileGuarded}
	policy, err := executionpolicy.NewCommandSnapshot(spec.Argv)
	if err != nil {
		t.Fatal(err)
	}
	argv, _ := json.Marshal(spec.Argv)
	argvSum := sha256.Sum256(argv)
	stdinSum := sha256.Sum256(nil)
	specBytes, _ := json.Marshal(struct {
		Argv        []string
		Directory   string
		Timeout     int64
		Profile     contracts.ExecutionProfile
		StdinDigest string
	}{spec.Argv, spec.Directory, spec.Timeout.Nanoseconds(), spec.Profile, "sha256:" + hex.EncodeToString(stdinSum[:])})
	specSum := sha256.Sum256(specBytes)
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "proof", Ticket: "proof"}, SemanticKey: "repository-command/proof", RequestDigest: "sha256:" + strings.Repeat("a", 64), TicketVersion: 1, LeaderEpoch: 1, RunnerEpoch: 1, ClaimEpoch: 1, Repository: identity.Repository, Worktree: worktree, WorktreeIdentity: string(identityRaw), Branch: identity.HeadRef, BaseRef: identity.BaseRef, BaseSHA: identity.BaseHead, CommandDigest: "sha256:" + hex.EncodeToString(argvSum[:]), SpecDigest: "sha256:" + hex.EncodeToString(specSum[:]), PolicyDigest: policy.Digest(), ExecutablePath: goPath, ExecutableDigest: "sha256:" + hex.EncodeToString(goSum[:])}
	if err := runner.Reauthenticate(context.Background(), identity); err != nil {
		t.Fatalf("preflight Git reauthentication: %v", err)
	}
	if _, err := parseRepositoryIdentity(claim); err != nil {
		t.Fatalf("preflight repository identity parse: %v; identity=%+v", err, identity)
	}
	sf := filepath.Join(t.TempDir(), "sf")
	build := exec.Command("go", "build", "-o", sf, "./cmd/sf")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sf gate: %v: %s", err, out)
	}
	lease := &repositoryTestLease{}
	result, err := (RepositoryCommandSupervisor{Executable: sf, GitRunner: runner, SoftDrain: 250 * time.Millisecond, HardDrain: 250 * time.Millisecond}).Run(context.Background(), claim, spec, policy, lease)
	if err != nil || !result.Observed || result.ExitCode != 0 {
		t.Fatalf("repository Go verification result=%+v err=%v", result, err)
	}
	if lease.groupCount() == 0 {
		t.Fatal("Go test binary crossed no durable process-group gate")
	}
}

func mustRepositoryCommand(t *testing.T, dir string, argv ...string) {
	t.Helper()
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%s: %v: %s", fmt.Sprint(argv), err, out)
	}
}
