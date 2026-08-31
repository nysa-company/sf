package mergeproof

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

// TestVerifyProtectedBranchReusesConfirmedProofAfterLostResponse models the
// exact crash boundary that matters for guarded merge recovery: GitHub has
// merged, the protected-ref proof has committed, but the caller loses its
// response before it can settle the parent merge effect. The second coordinator
// instance must prove the existing immutable Store/Git binding and never issue
// another protected-ref mutation claim.
func TestVerifyProtectedBranchReusesConfirmedProofAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	repository, worktree, remote, originalBase, mergeCommit := mergeProofRepository(t)
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-mergeproof"}
	if err := database.CreateProject(ctx, store.Project{Channel: ref.Channel, ID: ref.Project, Path: repository, BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "mergeproof", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, ref.Channel, "mergeproof")
	if err != nil {
		t.Fatal(err)
	}
	version, fence, err := database.CurrentTicketFence(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	runner := git.Runner{Binary: "/usr/bin/git", Home: filepath.Join(t.TempDir(), "home"), ExecHelper: buildGitExecHelper(t), MutationAuthority: database, TestLocalTransport: true}
	if err := os.Mkdir(runner.Home, 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := runner.Snapshot(ctx, worktree, "main")
	if err != nil {
		t.Fatal(err)
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: version, Fence: fence, Path: worktree, Branch: "proof", IdentityJSON: identityJSON, BaseSHA: originalBase, HeadSHA: mergeCommit}); err != nil {
		t.Fatal(err)
	}
	mergeKey := "merge/dev/app/SF-mergeproof"
	if _, err := database.PlanEffect(ctx, store.EffectPlan{SemanticKey: mergeKey, Ref: ref, Kind: "merge", TicketVersion: version, Fence: fence, RequestDigest: "merge-request"}); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := database.ClaimEffect(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("merge claim=%+v err=%v", mergeClaim, err)
	}
	intent := domain.MergeIntent{Ref: ref, SemanticKey: mergeKey, RequestDigest: "merge-request", TicketVersion: version, LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch, RepositoryHost: "github.com", RepositoryOwner: "example", RepositoryName: "app", PullRequestNumber: 17, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/app/SF-mergeproof", HeadOID: mergeCommit, BaseRef: "main", OriginalBaseOID: originalBase, ProtectionRuleID: "strict", StrictStatusChecks: true, AdminEnforced: true, Method: "squash"}
	if err := database.RecordMergeIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	coordinator := Coordinator{Store: database, Git: runner}
	repositoryID := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	first, err := coordinator.VerifyProtectedBranch(ctx, repositoryID, "main", mergeCommit, originalBase)
	if err != nil || !first.Contains {
		t.Fatalf("first proof=%+v err=%v", first, err)
	}
	// A new coordinator models a restart after the proof commit but before the
	// GitHub caller received it. The durable proof must be reused exactly.
	second, err := (Coordinator{Store: database, Git: runner}).VerifyProtectedBranch(ctx, repositoryID, "main", mergeCommit, originalBase)
	if err != nil || second != first {
		t.Fatalf("replayed proof=%+v first=%+v err=%v", second, first, err)
	}
	proofEffect, err := database.Effect(ctx, store.CanonicalGitMutationSemanticKey(store.GitMutationIntent{EffectFence: store.EffectFence{Ref: ref}, RequestDigest: proofRequestDigest(intent.SemanticKey, "main", originalBase, mergeCommit, identity.Origin), Repository: repository, Worktree: worktree, Branch: "proof", Operation: "protected-ref-fetch", BaseRef: "main", ExpectedBaseOID: originalBase, ExpectedHeadOID: mergeCommit}))
	if err != nil || proofEffect.State != store.EffectConfirmed {
		t.Fatalf("durable proof=%+v err=%v", proofEffect, err)
	}
	if got := gitOutput(t, repository, "ls-remote", remote, "refs/heads/main"); !strings.Contains(got, mergeCommit) {
		t.Fatalf("protected ref changed unexpectedly: %q", got)
	}
}

func proofRequestDigest(values ...string) string { return digest(values...) }

func mergeProofRepository(t *testing.T) (repository, worktree, remote, originalBase, mergeCommit string) {
	t.Helper()
	root := t.TempDir()
	repository, remote = filepath.Join(root, "repo"), filepath.Join(root, "remote.git")
	gitRun(t, root, "init", "--bare", remote)
	gitRun(t, root, "init", "-b", "main", repository)
	gitRun(t, repository, "config", "user.email", "sf@example.test")
	gitRun(t, repository, "config", "user.name", "sf test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, repository, "add", "README.md")
	gitRun(t, repository, "commit", "-m", "base")
	gitRun(t, repository, "remote", "add", "origin", remote)
	gitRun(t, repository, "push", "-u", "origin", "main")
	originalBase = strings.TrimSpace(gitOutput(t, repository, "rev-parse", "main^{commit}"))
	gitRun(t, repository, "checkout", "-b", "feature")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("merged\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, repository, "add", "README.md")
	gitRun(t, repository, "commit", "-m", "feature")
	gitRun(t, repository, "checkout", "main")
	gitRun(t, repository, "merge", "--ff-only", "feature")
	gitRun(t, repository, "push", "origin", "main")
	mergeCommit = strings.TrimSpace(gitOutput(t, repository, "rev-parse", "main^{commit}"))
	worktree = filepath.Join(root, "proof-worktree")
	gitRun(t, repository, "worktree", "add", "-b", "proof", worktree, "main")
	repository, _ = filepath.EvalSymlinks(repository)
	worktree, _ = filepath.EvalSymlinks(worktree)
	remote, _ = filepath.EvalSymlinks(remote)
	return repository, worktree, remote, originalBase, mergeCommit
}

func gitRun(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
}

func gitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
	return string(output)
}

func buildGitExecHelper(t *testing.T) string {
	t.Helper()
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		next := filepath.Dir(root)
		if next == root {
			t.Fatal("repository root not found")
		}
		root = next
	}
	helper := filepath.Join(t.TempDir(), "sf-git-exec")
	build := exec.Command("go", "build", "-o", helper, "./cmd/sf-git-exec")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sf-git-exec: %v: %s", err, output)
	}
	return helper
}
