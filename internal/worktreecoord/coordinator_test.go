package worktreecoord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

var coordinatorExecHelper string

func TestMain(m *testing.M) {
	root, err := os.Getwd()
	if err != nil {
		os.Exit(2)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			os.Exit(2)
		}
		root = parent
	}
	dir, err := os.MkdirTemp("", "sf-worktreecoord-")
	if err != nil {
		os.Exit(2)
	}
	coordinatorExecHelper = filepath.Join(dir, "sf-git-exec")
	build := exec.Command("go", "build", "-o", coordinatorExecHelper, "./cmd/sf-git-exec")
	build.Dir = root
	if err := build.Run(); err != nil {
		_ = os.RemoveAll(dir)
		os.Exit(2)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type coordinatorFixture struct {
	db      *store.Store
	runner  git.Runner
	project store.Project
	ref     domain.TicketRef
	request EnsureRequest
}

func setupCoordinator(t *testing.T, ticket string) coordinatorFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	repository := filepath.Join(root, "repository")
	remote := filepath.Join(root, "remote.git")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	mustGit(t, root, "init", "--bare", remote)
	mustGit(t, repository, "init", "-b", "main")
	mustGit(t, repository, "config", "user.name", "fixture")
	mustGit(t, repository, "config", "user.email", "fixture@example.test")
	if err := os.MkdirAll(filepath.Join(repository, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "src", "main.txt"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustGit(t, repository, "add", ".")
	mustGit(t, repository, "commit", "-m", "base")
	mustGit(t, repository, "remote", "add", "origin", remote)
	mustGit(t, repository, "push", "origin", "main:refs/heads/main")
	canonicalRepository, err := filepath.EvalSymlinks(repository)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, filepath.Join(root, "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	project := store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: canonicalRepository, BaseRef: "main"}
	if err := db.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: project.ID, Ticket: domain.TicketID(ticket)}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source-" + ticket, Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "worktreecoord-test")
	if err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, current.Version, "dev/nysa/"+ticket+"/worktree", domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	runner := git.Runner{Home: filepath.Join(root, "git-home"), ExecHelper: coordinatorExecHelper, TestLocalTransport: true, MutationAuthority: db}
	return coordinatorFixture{db: db, runner: runner, project: project, ref: ref, request: EnsureRequest{Ref: ref, Version: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}}
}

func mustGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", directory}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}
func coordinatorFor(f coordinatorFixture) Coordinator { return Coordinator{Store: f.db, Git: f.runner} }

func TestEnsureFreshAndIdempotent(t *testing.T) {
	f := setupCoordinator(t, "SF-fresh")
	ctx := context.Background()
	c := coordinatorFor(f)
	first, err := c.Ensure(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.Ensure(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Path != second.Path || first.Branch != second.Branch || first.IdentityJSON == nil {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if got := mustGit(t, f.project.Path, "worktree", "list", "--porcelain"); strings.Count(got, "worktree "+first.Path) != 1 {
		t.Fatalf("expected one linked worktree:\n%s", got)
	}
	// Registration records the pristine creation witness, not a permanent HEAD
	// lock. A later clean candidate commit remains the same authenticated
	// worktree and creation Ensure must not reject it.
	if err := os.WriteFile(filepath.Join(first.Path, "src", "candidate.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustGit(t, first.Path, "add", "src/candidate.txt")
	mustGit(t, first.Path, "commit", "-m", "candidate")
	third, err := c.Ensure(ctx, f.request)
	if err != nil || third.Path != first.Path {
		t.Fatalf("clean candidate head was rejected: worktree=%+v err=%v", third, err)
	}
}

func TestEnsureReconcilesCreationResponseLossAfterReopen(t *testing.T) {
	f := setupCoordinator(t, "SF-response-loss")
	ctx := context.Background()
	path, err := f.db.TicketWorktreePath(f.ref)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := (git.Allocator{Authority: f.db}).Allocate(ctx, f.ref.Channel, f.ref.Project, f.ref.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	repository, base, err := f.runner.ObserveRepositoryBase(ctx, f.project.Path, f.project.BaseRef)
	if err != nil {
		t.Fatal(err)
	}
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: f.ref, TicketVersion: f.request.Version, Fence: f.request.Fence}, RequestDigest: ensureDigest(f.ref, repository, path, branch, f.project.BaseRef, base), Repository: repository, Worktree: path, Branch: branch, Operation: "create-worktree", BaseRef: f.project.BaseRef, ExpectedBaseOID: base, ExpectedHeadOID: base}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if _, err := f.db.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: f.ref, Kind: "git/create-worktree", TicketVersion: f.request.Version, Fence: f.request.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureExactParent(path); err != nil {
		t.Fatal(err)
	}
	if _, err := f.runner.CreateWorktree(ctx, repository, path, branch, f.project.BaseRef, claim); err != nil {
		t.Fatal(err)
	}
	// The linked directory is visible before the original claimant has
	// confirmed the effect. A second caller must wait, not race to snapshot and
	// register the still-executing creation.
	waitCtx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if _, err := coordinatorFor(f).Ensure(waitCtx, f.request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("executing creation was adopted instead of waiting: %v", err)
	}
	if _, err := f.db.Worktree(ctx, f.ref); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("executing creation was registered early: %v", err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(ctx, filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(path))), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	// The original cleanup will close again; this replacement is deliberately
	// used only through the reopened durable authority.
	f.db = reopened
	f.runner.MutationAuthority = reopened
	// Match daemon recovery ordering: the stranded effect becomes uncertain
	// under a new leader, then every active ticket receives a new version and
	// runner fence before this coordinator is allowed to adopt the old Git fact.
	newLeader, err := reopened.AcquireLeader(ctx, domain.ChannelDev, "recovered-daemon")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ReconcileEffects(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	current, err := reopened.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	f.request = EnsureRequest{Ref: f.ref, Version: current.Version, Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}}
	got, err := coordinatorFor(f).Ensure(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if got.Path != path || got.Branch != branch {
		t.Fatalf("reconciled=%+v", got)
	}
}

func TestEnsureRefusesStaleFenceBeforeCreatingPath(t *testing.T) {
	f := setupCoordinator(t, "SF-stale")
	ctx := context.Background()
	path, err := f.db.TicketWorktreePath(f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.AcquireLeader(ctx, domain.ChannelDev, "replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinatorFor(f).Ensure(ctx, f.request); !errors.Is(err, store.ErrStaleFence) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale ensure created %s: %v", path, err)
	}
}

func TestEnsureConcurrentCallersCreateExactlyOneWorktree(t *testing.T) {
	f := setupCoordinator(t, "SF-concurrent")
	c := coordinatorFor(f)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := c.Ensure(ctx, f.request); results <- err }()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent ensure: %v", err)
		}
	}
	got := mustGit(t, f.project.Path, "worktree", "list", "--porcelain")
	path, _ := f.db.TicketWorktreePath(f.ref)
	if strings.Count(got, "worktree "+path) != 1 {
		t.Fatalf("expected one worktree:\n%s", got)
	}
}

func TestEnsureQuarantinesReplacedRegisteredPath(t *testing.T) {
	f := setupCoordinator(t, "SF-tamper")
	ctx := context.Background()
	c := coordinatorFor(f)
	registered, err := c.Ensure(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(registered.Path, registered.Path+"-foreign"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(registered.Path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Ensure(ctx, f.request); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("replacement accepted: %v", err)
	}
}

func TestEnsureExcludesActiveRepositoryCommandWriter(t *testing.T) {
	f := setupCoordinator(t, "SF-command-owner")
	ctx := context.Background()
	owner, err := coordinatorFor(f).Ensure(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	var identity git.Identity
	if err := json.Unmarshal(owner.IdentityJSON, &identity); err != nil {
		t.Fatal(err)
	}
	d := func(s string) string { sum := sha256.Sum256([]byte(s)); return "sha256:" + hex.EncodeToString(sum[:]) }
	intent := store.RepositoryCommandIntent{EffectFence: store.EffectFence{SemanticKey: "repository-command/SF-command-owner", Ref: f.ref, TicketVersion: f.request.Version, Fence: f.request.Fence}, RequestDigest: d("request"), Repository: f.project.Path, Worktree: owner.Path, WorktreeIdentity: string(owner.IdentityJSON), Branch: owner.Branch, BaseRef: f.project.BaseRef, BaseSHA: owner.BaseSHA, CommandDigest: d("command"), SpecDigest: d("spec"), PolicyDigest: d("policy"), ExecutablePath: "/usr/bin/true", ExecutableDigest: d("executable")}
	if _, err := f.db.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: f.ref, Kind: "repository_command", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	// A different ticket shares the repository and must be refused before its
	// Git mutation can create a second linked worktree.
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: f.project.ID, Ticket: "SF-command-blocked"}
	if err := f.db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source-blocked", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	current, err := f.db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := f.db.StartOrAdopt(ctx, ref, current.Version, "dev/nysa/blocked", domain.Fence{LeaderEpoch: f.request.Fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	blocked := EnsureRequest{Ref: ref, Version: started.Version, Fence: domain.Fence{LeaderEpoch: f.request.Fence.LeaderEpoch, RunnerEpoch: started.RunnerEpoch}}
	path, _ := f.db.TicketWorktreePath(ref)
	if _, err := coordinatorFor(f).Ensure(ctx, blocked); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("repository command writer did not exclude Git: %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("blocked ensure created %s: %v", path, err)
	}
}
