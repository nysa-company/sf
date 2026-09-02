package mergeproof

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
	fixture := newMergeProofCoordinatorFixture(t)
	first, err := (Coordinator{Store: fixture.database, Git: fixture.runner}).VerifyProtectedBranch(fixture.ctx, fixture.repositoryID, "main", fixture.mergeCommit, fixture.originalBase)
	if err != nil || !first.Contains {
		t.Fatalf("first proof=%+v err=%v", first, err)
	}
	// A new coordinator models a restart after the proof commit but before the
	// GitHub caller received it. The durable proof must be reused exactly.
	second, err := (Coordinator{Store: fixture.database, Git: fixture.runner}).VerifyProtectedBranch(fixture.ctx, fixture.repositoryID, "main", fixture.mergeCommit, fixture.originalBase)
	if err != nil || second != first {
		t.Fatalf("replayed proof=%+v first=%+v err=%v", second, first, err)
	}
	proof, err := fixture.database.GuardedMergeProtectedRefFetchIntent(fixture.ctx, fixture.intent, fixture.mergeCommit)
	if err != nil || proof.Origin != fixture.identity.Origin {
		t.Fatalf("durable protected proof intent=%+v err=%v", proof, err)
	}
	proofEffect, err := fixture.database.Effect(fixture.ctx, proof.Intent.SemanticKey)
	if err != nil || proofEffect.State != store.EffectConfirmed {
		t.Fatalf("durable proof=%+v err=%v", proofEffect, err)
	}
	if got := gitOutput(t, fixture.repository, "ls-remote", fixture.remote, "refs/heads/main"); !strings.Contains(got, fixture.mergeCommit) {
		t.Fatalf("protected ref changed unexpectedly: %q", got)
	}
}

// TestVerifyProtectedBranchReclaimsUncertainProofAfterInterruptedVerifier
// proves the boundary that used to strand the child as executing: the runner
// has crossed the Store lease boundary, loses its response, and the verifier
// context is cancelled.  The independent uncertainty write must survive that
// cancellation before the exact deterministic proof can be reissued.
func TestVerifyProtectedBranchReclaimsUncertainProofAfterInterruptedVerifier(t *testing.T) {
	fixture := newMergeProofCoordinatorFixture(t)
	ctx, cancel := context.WithCancel(fixture.ctx)
	runner := &interruptedProtectedProofRunner{database: fixture.database, identity: fixture.identity, interrupt: cancel}
	coordinator := Coordinator{Store: fixture.database, Git: runner}
	if _, err := coordinator.VerifyProtectedBranch(ctx, fixture.repositoryID, "main", fixture.mergeCommit, fixture.originalBase); !errors.Is(err, errProtectedProofInterrupted) {
		t.Fatalf("interrupted verifier=%v", err)
	}
	proof, err := fixture.database.GuardedMergeProtectedRefFetchIntent(fixture.ctx, fixture.intent, fixture.mergeCommit)
	if err != nil {
		t.Fatal(err)
	}
	uncertain, err := fixture.database.Effect(fixture.ctx, proof.Intent.SemanticKey)
	if err != nil || uncertain.State != store.EffectUncertain {
		t.Fatalf("interrupted proof effect=%+v err=%v", uncertain, err)
	}
	if len(runner.claims) != 1 {
		t.Fatalf("first verifier calls=%d", len(runner.claims))
	}
	second, err := coordinator.VerifyProtectedBranch(fixture.ctx, fixture.repositoryID, "main", fixture.mergeCommit, fixture.originalBase)
	if err != nil || !second.Contains {
		t.Fatalf("reclaimed proof=%+v err=%v", second, err)
	}
	if len(runner.claims) != 2 {
		t.Fatalf("verifier calls=%d", len(runner.claims))
	}
	firstClaim, secondClaim := runner.claims[0], runner.claims[1]
	if firstClaim.SemanticKey != secondClaim.SemanticKey || firstClaim.RequestDigest != secondClaim.RequestDigest ||
		firstClaim.Repository != secondClaim.Repository || firstClaim.Worktree != secondClaim.Worktree || firstClaim.Branch != secondClaim.Branch ||
		firstClaim.Operation != "protected-ref-fetch" || secondClaim.Operation != "protected-ref-fetch" ||
		firstClaim.BaseRef != secondClaim.BaseRef || firstClaim.ExpectedBaseOID != secondClaim.ExpectedBaseOID || firstClaim.ExpectedHeadOID != secondClaim.ExpectedHeadOID ||
		secondClaim.ClaimEpoch != firstClaim.ClaimEpoch+1 {
		t.Fatalf("proof reclaim changed immutable binding first=%+v second=%+v", firstClaim, secondClaim)
	}
	confirmed, err := fixture.database.Effect(fixture.ctx, proof.Intent.SemanticKey)
	if err != nil || confirmed.State != store.EffectConfirmed {
		t.Fatalf("reclaimed proof effect=%+v err=%v", confirmed, err)
	}
}

var errProtectedProofInterrupted = errors.New("protected proof verifier interrupted")

type interruptedProtectedProofRunner struct {
	database  *store.Store
	identity  git.Identity
	claims    []contracts.GitMutationClaim
	interrupt func()
}

func (r *interruptedProtectedProofRunner) Snapshot(context.Context, string, string) (git.Identity, error) {
	return r.identity, nil
}

func (r *interruptedProtectedProofRunner) VerifyProtectedBranch(ctx context.Context, witness contracts.ProtectedBranchWitness) error {
	lease, err := r.database.AcquireGitMutation(ctx, witness.MutationClaim)
	if err != nil {
		return err
	}
	r.claims = append(r.claims, witness.MutationClaim)
	if err := lease.Release(); err != nil {
		return err
	}
	if r.interrupt != nil {
		r.interrupt()
		r.interrupt = nil
		return errProtectedProofInterrupted
	}
	return nil
}

type mergeProofCoordinatorFixture struct {
	ctx          context.Context
	database     *store.Store
	runner       git.Runner
	repository   string
	remote       string
	repositoryID contracts.RepositoryIdentity
	identity     git.Identity
	intent       domain.MergeIntent
	originalBase string
	mergeCommit  string
}

func newMergeProofCoordinatorFixture(t *testing.T) mergeProofCoordinatorFixture {
	t.Helper()
	ctx := context.Background()
	repository, worktree, remote, originalBase, sourceHead, mergeCommit := mergeProofRepository(t)
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
	intent := domain.MergeIntent{Ref: ref, SemanticKey: mergeKey, TicketVersion: version, RepositoryHost: "github.com", RepositoryOwner: "example", RepositoryName: "app", PullRequestNumber: 17, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/app/SF-mergeproof", HeadOID: sourceHead, BaseRef: "main", OriginalBaseOID: originalBase, ProtectionRuleID: "strict", ProtectionKind: "classic", StrictStatusChecks: true, AdminEnforced: true, Method: "squash"}
	intent.RequestDigest = mergeRequestDigestForTest(intent)
	if _, err := database.PlanEffect(ctx, store.EffectPlan{SemanticKey: mergeKey, Ref: ref, Kind: "merge", TicketVersion: version, Fence: fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := database.ClaimEffect(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("merge claim=%+v err=%v", mergeClaim, err)
	}
	intent.LeaderEpoch = leader
	intent.RunnerEpoch = fence.RunnerEpoch
	intent.ClaimEpoch = mergeClaim.Effect.ClaimEpoch
	if err := database.RecordMergeIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}
	repositoryID := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	mergedIdentity := contracts.PullRequestIdentity{Repository: repositoryID, Number: 17, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/app/SF-mergeproof", HeadOID: sourceHead, BaseRef: "main", BaseOID: mergeCommit, FactoryOwned: true}
	wrongSource := mergedIdentity
	wrongSource.HeadOID = mergeCommit
	wrongObservation := contracts.PublishedPullRequestObservation{Identity: wrongSource, State: "MERGED", Merged: true, MergeCommit: mergeCommit, BaseHeadOID: wrongSource.BaseOID}
	if err := database.RecordGuardedMergeObservation(ctx, intent, wrongObservation); !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("post-merge observation accepted a substituted source head: %v", err)
	}
	mergedObservation := contracts.PublishedPullRequestObservation{Identity: mergedIdentity, State: "MERGED", Merged: true, MergeCommit: mergeCommit, BaseHeadOID: mergedIdentity.BaseOID}
	if err := database.RecordGuardedMergeObservation(ctx, intent, mergedObservation); err != nil {
		t.Fatal(err)
	}
	selected, err := database.MergeIntentForProof(ctx, repositoryID.Host, repositoryID.Owner, repositoryID.Name, "main", originalBase, mergeCommit)
	if err != nil || selected != intent {
		t.Fatalf("select exact guarded merge intent=%+v want=%+v err=%v", selected, intent, err)
	}
	if _, err := database.GuardedMergeProtectedRefFetchIntent(ctx, intent, mergeCommit); err != nil {
		t.Fatalf("derive exact protected-ref proof intent: %v", err)
	}
	if _, err := database.MergeIntentForProof(ctx, repositoryID.Host, repositoryID.Owner, repositoryID.Name, "main", originalBase, sourceHead); err == nil {
		t.Fatal("source head selected a protected proof without its exact observed merge commit")
	}
	return mergeProofCoordinatorFixture{ctx: ctx, database: database, runner: runner, repository: repository, remote: remote, repositoryID: repositoryID, identity: identity, intent: intent, originalBase: originalBase, mergeCommit: mergeCommit}
}

func mergeRequestDigestForTest(intent domain.MergeIntent) string {
	input := "merge\x00" + intent.RepositoryOwner + "/" + intent.RepositoryName + "\x00" + intent.HeadOwner + "\x00" + intent.HeadRepository + "\x00" + intent.HeadRef + "\x00" + intent.HeadOID + "\x00" + intent.BaseRef + "\x00" + intent.OriginalBaseOID
	for _, value := range []string{intent.HeadOID, intent.Method, intent.OriginalBaseOID, intent.OriginalBaseOID, intent.OriginalBaseOID, intent.OriginalBaseOID} {
		input += "\x00" + value
	}
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func mergeProofRepository(t *testing.T) (repository, worktree, remote, originalBase, sourceHead, mergeCommit string) {
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
	sourceHead = strings.TrimSpace(gitOutput(t, repository, "rev-parse", "HEAD^{commit}"))
	gitRun(t, repository, "checkout", "main")
	if err := os.WriteFile(filepath.Join(repository, "MAIN.md"), []byte("main advance\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitRun(t, repository, "add", "MAIN.md")
	gitRun(t, repository, "commit", "-m", "main advance")
	gitRun(t, repository, "merge", "--no-ff", "feature", "-m", "merge feature")
	gitRun(t, repository, "push", "origin", "main")
	mergeCommit = strings.TrimSpace(gitOutput(t, repository, "rev-parse", "main^{commit}"))
	if mergeCommit == sourceHead {
		t.Fatal("fixture must model a post-merge commit distinct from the source head")
	}
	// Preserve the factory's authenticated local protected-base snapshot while
	// leaving the simulated GitHub remote advanced to the post-merge commit.
	// The proof boundary must learn that advance only through its bounded
	// protected-ref fetch, not through a pre-updated local main ref.
	gitRun(t, repository, "checkout", "--detach", originalBase)
	gitRun(t, repository, "branch", "-f", "main", originalBase)
	worktree = filepath.Join(root, "proof-worktree")
	// The factory worktree remains bound to the reviewed protected-base
	// snapshot. The remote protected ref advances independently after GitHub
	// merges; mergeproof fetches that ref into its private proof namespace.
	gitRun(t, repository, "worktree", "add", "-b", "proof", worktree, originalBase)
	repository, _ = filepath.EvalSymlinks(repository)
	worktree, _ = filepath.EvalSymlinks(worktree)
	remote, _ = filepath.EvalSymlinks(remote)
	return repository, worktree, remote, originalBase, sourceHead, mergeCommit
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
