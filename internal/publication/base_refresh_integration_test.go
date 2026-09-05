package publication_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/baserefresh"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/localruntime"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/publication"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

var errBaseRefreshApplyResponseLost = errors.New("fixture lost base-refresh apply response")
var errBaseRefreshProofResponseLost = errors.New("fixture lost base-refresh proof response")

// TestBaseRefreshLostApplyResponseRecoversThroughLocalRuntime composes the
// production local-runtime dispatch, Store reservation/recovery, exact remote
// proof and real Git merge-tree preparation. The final paired-ref write uses a
// narrowly typed test adapter because publication fixtures inject Runner.Run,
// while the native update-ref --stdin gate deliberately refuses injected
// runners. Native Git package tests separately cover that production gate;
// this test does not claim to replace them.
func TestBaseRefreshLostApplyResponseRecoversThroughLocalRuntime(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	oldTicket, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldWorktree, err := f.db.Worktree(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldCandidate, err := f.db.HistoricalCandidate(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(oldCandidate, f.candidate) || oldTicket.State != domain.StatePublishing {
		t.Fatalf("fixture ticket=%+v candidate=%+v want=%+v", oldTicket, oldCandidate, f.candidate)
	}

	// newPublicationFixture predates hosted worktree creation and therefore
	// starts with the named local base fallback. Seed the equivalent
	// ticket-private base ref before exercising the refresh contract.
	baseRef := baseRefreshIntegrationWorktreeBaseRef(oldWorktree.Branch)
	runGit(t, oldWorktree.Path, "update-ref", baseRef, oldWorktree.BaseSHA)
	if got := gitOutput(t, oldWorktree.Path, "rev-parse", "--verify", baseRef+"^{commit}"); got != oldWorktree.BaseSHA {
		t.Fatalf("fixture pinned base=%s want=%s", got, oldWorktree.BaseSHA)
	}

	newBase := advanceBaseRefreshIntegrationMain(t, f.bare)
	if newBase == oldWorktree.BaseSHA || newBase == oldCandidate.Snapshot.HeadSHA {
		t.Fatalf("non-distinct refresh base=%s old=%s candidate=%s", newBase, oldWorktree.BaseSHA, oldCandidate.Snapshot.HeadSHA)
	}
	assertBaseRefreshIntegrationRemote(t, f, oldWorktree, newBase)

	adapter := &baseRefreshIntegrationGit{store: f.db, runner: f.runner, loseNextApply: true}
	worker := baseRefreshIntegrationWorker(f, adapter)
	first, err := worker.Run(ctx, f.ref, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: oldTicket.RunnerEpoch})
	if !errors.Is(err, errBaseRefreshApplyResponseLost) {
		t.Fatalf("first refresh result=%+v err=%v", first, err)
	}
	if first.State != domain.StatePublishing || first.Version != oldTicket.Version {
		t.Fatalf("lost response advanced runtime result=%+v", first)
	}
	pending, found, err := f.db.PendingProtectedBaseRefresh(ctx, f.ref, oldTicket.Version, f.fence)
	if err != nil || !found {
		t.Fatalf("pending refresh found=%v err=%v", found, err)
	}
	refreshEffect, err := f.db.Effect(ctx, pending.Mutation.SemanticKey)
	if err != nil || refreshEffect.State != store.EffectUncertain {
		t.Fatalf("lost response effect=%+v err=%v", refreshEffect, err)
	}
	if adapter.verifyCalls != 1 || adapter.prepareCalls != 1 || adapter.applyCalls != 1 || adapter.refTransactions != 1 {
		t.Fatalf("first calls verify=%d prepare=%d apply=%d ref_transactions=%d", adapter.verifyCalls, adapter.prepareCalls, adapter.applyCalls, adapter.refTransactions)
	}
	if adapter.prepared.CommitOID == "" || adapter.prepared.Parents != [2]string{oldCandidate.Snapshot.HeadSHA, newBase} {
		t.Fatalf("prepared=%+v", adapter.prepared)
	}
	assertBaseRefreshIntegrationPhysicalState(t, oldWorktree, adapter.prepared, newBase)
	if retained, err := f.db.Worktree(ctx, f.ref); err != nil || !reflect.DeepEqual(retained, oldWorktree) {
		t.Fatalf("Store worktree advanced before completion=%+v err=%v", retained, err)
	}
	if retained, err := f.db.HistoricalCandidate(ctx, f.ref); err != nil || !reflect.DeepEqual(retained, oldCandidate) {
		t.Fatalf("old candidate changed after lost response=%+v err=%v", retained, err)
	}
	assertBaseRefreshIntegrationNoPublicationMutation(t, f, oldWorktree, newBase)

	currentWorktree := recoverBaseRefreshLostApplyResponse(t, ctx, f, oldTicket, oldCandidate, newBase, adapter, pending, refreshEffect, f.github)
	assertBaseRefreshIntegrationNoPublicationMutation(t, f, oldWorktree, newBase)
	if currentWorktree.BaseSHA != newBase {
		t.Fatalf("recovered worktree base=%s want=%s", currentWorktree.BaseSHA, newBase)
	}
}

// TestPublishedBaseRefreshLostApplyResponseRecoversToBuilding repeats the
// exact mutation-response loss after an initial candidate is already bound to
// one waiting-CI pull request. Recovery must preserve that PR and must not push
// or edit publication while it completes the base refresh.
func TestPublishedBaseRefreshLostApplyResponseRecoversToBuilding(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	initial, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(ctx, f.ref, f.fence)
	if err != nil || initial.State != domain.StateWaitingCI || !initial.Transitioned {
		t.Fatalf("initial publication=%+v err=%v", initial, err)
	}
	published, err := f.db.LoadPublishedCandidate(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldTicket, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldWorktree, err := f.db.Worktree(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldCandidate, err := f.db.HistoricalCandidate(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, oldWorktree.Path, "update-ref", baseRefreshIntegrationWorktreeBaseRef(oldWorktree.Branch), oldWorktree.BaseSHA)

	newBase := advanceBaseRefreshIntegrationMain(t, f.bare)
	if err := f.github.SetBaseHeadOIDForTest(newBase); err != nil {
		t.Fatal(err)
	}
	if got := gitOutput(t, f.bare, "rev-parse", "refs/heads/"+oldWorktree.Branch+"^{commit}"); got != oldCandidate.Snapshot.HeadSHA {
		t.Fatalf("initial published branch=%s want=%s", got, oldCandidate.Snapshot.HeadSHA)
	}

	adapter := &baseRefreshIntegrationGit{store: f.db, runner: f.runner, loseNextApply: true}
	hosted := baseRefreshIntegrationGitHub{FakeGH: f.github}
	fence := domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: oldTicket.RunnerEpoch}
	first, err := baseRefreshIntegrationWorkerWithGitHub(f, adapter, hosted).Run(ctx, f.ref, fence)
	if !errors.Is(err, errBaseRefreshApplyResponseLost) {
		t.Fatalf("first published refresh result=%+v err=%v", first, err)
	}
	if first.State != domain.StateWaitingCI || first.Version != oldTicket.Version {
		t.Fatalf("lost response advanced published runtime result=%+v", first)
	}
	pending, found, err := f.db.PendingProtectedBaseRefresh(ctx, f.ref, oldTicket.Version, fence)
	if err != nil || !found {
		t.Fatalf("pending published refresh found=%v err=%v", found, err)
	}
	refreshEffect, err := f.db.Effect(ctx, pending.Mutation.SemanticKey)
	if err != nil || refreshEffect.State != store.EffectUncertain {
		t.Fatalf("lost published response effect=%+v err=%v", refreshEffect, err)
	}
	if adapter.verifyCalls != 1 || adapter.prepareCalls != 1 || adapter.applyCalls != 1 || adapter.refTransactions != 1 {
		t.Fatalf("published first calls verify=%d prepare=%d apply=%d ref_transactions=%d", adapter.verifyCalls, adapter.prepareCalls, adapter.applyCalls, adapter.refTransactions)
	}
	assertBaseRefreshIntegrationPhysicalState(t, oldWorktree, adapter.prepared, newBase)
	if retained, err := f.db.Worktree(ctx, f.ref); err != nil || !reflect.DeepEqual(retained, oldWorktree) {
		t.Fatalf("published Store worktree advanced before completion=%+v err=%v", retained, err)
	}
	assertBaseRefreshIntegrationPublicationPreserved(t, ctx, f, published)

	currentWorktree := recoverBaseRefreshLostApplyResponse(t, ctx, f, oldTicket, oldCandidate, newBase, adapter, pending, refreshEffect, hosted)
	if currentWorktree.BaseSHA != newBase {
		t.Fatalf("recovered published worktree base=%s want=%s", currentWorktree.BaseSHA, newBase)
	}
	assertBaseRefreshIntegrationPublicationPreserved(t, ctx, f, published)
	if got := gitOutput(t, f.bare, "rev-parse", "refs/heads/"+oldWorktree.Branch+"^{commit}"); got != oldCandidate.Snapshot.HeadSHA {
		t.Fatalf("refresh repushed published branch=%s want=%s", got, oldCandidate.Snapshot.HeadSHA)
	}
}

func recoverBaseRefreshLostApplyResponse(t *testing.T, ctx context.Context, f *publicationFixture, oldTicket store.Ticket, oldCandidate store.StoredCandidate, newBase string, adapter *baseRefreshIntegrationGit, pending store.ProtectedBaseRefresh, refreshEffect store.Effect, github contracts.GitHub) store.StoredWorktree {
	t.Helper()

	// Follow the daemon's real restart order. ReconcileEffects first revokes the
	// crashed claim, FenceRecoveredRunners establishes the signed runner bridge,
	// and publication rebind then runs before a fresh local runtime is admitted.
	databasePath := filepath.Join(filepath.Dir(f.bare), "state.sqlite")
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.db = nil
	restarted, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	newLeader, err := restarted.AcquireLeader(ctx, f.ref.Channel, "base-refresh-lost-response-restart")
	if err != nil {
		t.Fatal(err)
	}
	uncertain, err := restarted.ReconcileEffects(ctx, f.ref.Channel, newLeader)
	if err != nil || len(uncertain) != 1 || uncertain[0].SemanticKey != pending.Mutation.SemanticKey || uncertain[0].State != store.EffectUncertain || uncertain[0].ClaimEpoch <= refreshEffect.ClaimEpoch {
		t.Fatalf("startup effect reconciliation=%+v err=%v", uncertain, err)
	}
	if changed, err := restarted.FenceRecoveredRunners(ctx, f.ref.Channel, newLeader); err != nil || changed != 1 {
		t.Fatalf("startup runner fence changed=%d err=%v", changed, err)
	}
	if err := restarted.RebindRecoveredPublishedCandidates(ctx, f.ref.Channel, newLeader); err != nil {
		t.Fatalf("startup publication rebind: %v", err)
	}
	recoveredTicket, err := restarted.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	recoveredFence := domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: recoveredTicket.RunnerEpoch}
	if recoveredTicket.State != oldTicket.State || recoveredTicket.Version != oldTicket.Version+1 || recoveredTicket.RunnerEpoch != oldTicket.RunnerEpoch+1 {
		t.Fatalf("recovered ticket=%+v old=%+v", recoveredTicket, oldTicket)
	}

	f.runner.MutationAuthority = restarted
	adapter.store, adapter.runner = restarted, f.runner
	recovered, err := baseRefreshIntegrationWorkerWithGitHub(f, adapter, github).Run(ctx, f.ref, recoveredFence)
	if err != nil {
		t.Fatalf("recover lost apply response: %v", err)
	}
	if recovered.State != domain.StateBuilding || !recovered.Transitioned || recovered.Version != recoveredTicket.Version+1 {
		t.Fatalf("recovered result=%+v", recovered)
	}
	if adapter.verifyCalls != 1 || adapter.prepareCalls != 1 || adapter.applyCalls != 2 || adapter.refTransactions != 1 {
		t.Fatalf("recovery calls verify=%d prepare=%d apply=%d ref_transactions=%d", adapter.verifyCalls, adapter.prepareCalls, adapter.applyCalls, adapter.refTransactions)
	}

	building, err := restarted.Ticket(ctx, f.ref)
	if err != nil || building.State != domain.StateBuilding || building.Version != recovered.Version {
		t.Fatalf("building ticket=%+v err=%v", building, err)
	}
	completionContext, err := restarted.ProtectedBaseRefreshBuildContext(ctx, f.ref, building.Version, recoveredFence)
	if err != nil {
		t.Fatalf("load refresh build context: %v", err)
	}
	if completionContext.Completion.Preparation != adapter.prepared || completionContext.Completion.Worktree.BaseSHA != newBase || completionContext.Completion.Worktree.HeadSHA != adapter.prepared.CommitOID || !reflect.DeepEqual(completionContext.Predecessor, oldCandidate) {
		t.Fatalf("completion context=%+v old_candidate=%+v", completionContext, oldCandidate)
	}
	currentWorktree, err := restarted.Worktree(ctx, f.ref)
	if err != nil || currentWorktree.BaseSHA != newBase || currentWorktree.HeadSHA != adapter.prepared.CommitOID || currentWorktree.TicketVersion != building.Version || currentWorktree.Fence != recoveredFence {
		t.Fatalf("refreshed Store worktree=%+v err=%v", currentWorktree, err)
	}
	if retained, err := restarted.HistoricalCandidate(ctx, f.ref); err != nil || !reflect.DeepEqual(retained, oldCandidate) {
		t.Fatalf("immutable predecessor candidate=%+v err=%v", retained, err)
	}
	assertBaseRefreshIntegrationPhysicalState(t, currentWorktree, adapter.prepared, newBase)
	return currentWorktree
}

func assertBaseRefreshIntegrationPublicationPreserved(t *testing.T, ctx context.Context, f *publicationFixture, want store.PublishedCandidateEvidence) {
	t.Helper()
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 1 || f.github.MutationCount("pr_edit") != 0 {
		t.Fatalf("refresh mutated existing publication push=%d create=%d edit=%d", f.gitPushCount, f.github.MutationCount("pr_create"), f.github.MutationCount("pr_edit"))
	}
	retained, err := f.db.LoadHistoricalPublishedCandidate(ctx, f.ref)
	if err != nil || retained.Candidate.Snapshot != want.Candidate.Snapshot || !reflect.DeepEqual(retained.PullRequest, want.PullRequest) || retained.RemoteBranchOID != want.RemoteBranchOID {
		t.Fatalf("retained publication=%+v want=%+v err=%v", retained, want, err)
	}
	remote := f.github.Snapshot()
	if len(remote.PRs) != 1 || !reflect.DeepEqual(remote.PRs[0].Identity, want.PullRequest) {
		t.Fatalf("hosted pull requests=%+v want=%+v", remote.PRs, want.PullRequest)
	}
}

// TestBaseRefreshPublishesSecondGenerationToSamePullRequest exercises the
// complete generation boundary after a protected-base refresh. Git proof and
// preparation are native; only the paired-ref Apply remains the narrow typed
// fixture described above. The hosted fake models GitHub's automatic PR base
// and source-head observations without issuing a second PR creation.
func TestBaseRefreshPublishesSecondGenerationToSamePullRequest(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	initial, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(ctx, f.ref, f.fence)
	if err != nil || initial.State != domain.StateWaitingCI || !initial.Transitioned {
		t.Fatalf("initial publication=%+v err=%v", initial, err)
	}
	first, err := f.db.LoadPublishedCandidate(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldWorktree, err := f.db.Worktree(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, oldWorktree.Path, "update-ref", baseRefreshIntegrationWorktreeBaseRef(oldWorktree.Branch), oldWorktree.BaseSHA)

	newBase := advanceBaseRefreshIntegrationMain(t, f.bare)
	if err := f.github.SetBaseHeadOIDForTest(newBase); err != nil {
		t.Fatal(err)
	}
	waiting, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &baseRefreshIntegrationGit{store: f.db, runner: f.runner, loseNextProof: true}
	hosted := baseRefreshIntegrationGitHub{FakeGH: f.github}
	if _, err := baseRefreshIntegrationWorkerWithGitHub(f, adapter, hosted).Run(ctx, f.ref, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}); !errors.Is(err, errBaseRefreshProofResponseLost) {
		t.Fatalf("lost exact-tip proof response: %v", err)
	}
	if pending, found, err := f.db.PendingProtectedBaseRefresh(ctx, f.ref, waiting.Version, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}); err != nil || found || pending.ID != 0 || adapter.prepareCalls != 0 || adapter.refTransactions != 0 {
		t.Fatalf("proof uncertainty minted refresh: found=%v pending=%+v err=%v", found, pending, err)
	}
	if _, err := f.db.LoadHistoricalPublishedCandidate(ctx, f.ref); err != nil {
		t.Fatalf("historical PR after proof uncertainty: %v", err)
	}
	if _, err := f.db.ProtectedBaseRefreshProofIntent(ctx, f.ref, waiting.Version, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}, newBase); err != nil {
		t.Fatalf("same exact proof after uncertainty: %v", err)
	}
	refreshed, err := baseRefreshIntegrationWorkerWithGitHub(f, adapter, hosted).Run(ctx, f.ref, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch})
	if err != nil || refreshed.State != domain.StateBuilding || !refreshed.Transitioned {
		t.Fatalf("base refresh=%+v err=%v", refreshed, err)
	}
	if adapter.verifyCalls != 2 || adapter.prepareCalls != 1 || adapter.applyCalls != 1 || adapter.refTransactions != 1 {
		t.Fatalf("refresh calls verify=%d prepare=%d apply=%d ref_transactions=%d", adapter.verifyCalls, adapter.prepareCalls, adapter.applyCalls, adapter.refTransactions)
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 1 || f.github.MutationCount("pr_edit") != 0 {
		t.Fatalf("refresh mutated publication push=%d create=%d edit=%d", f.gitPushCount, f.github.MutationCount("pr_create"), f.github.MutationCount("pr_edit"))
	}
	if remote := gitOutput(t, f.bare, "rev-parse", "refs/heads/"+oldWorktree.Branch+"^{commit}"); remote != first.Candidate.Snapshot.HeadSHA {
		t.Fatalf("refresh pushed candidate branch=%s want=%s", remote, first.Candidate.Snapshot.HeadSHA)
	}

	building, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	refreshContext, err := f.db.ProtectedBaseRefreshBuildContext(ctx, f.ref, building.Version, fence)
	if err != nil || refreshContext.Predecessor.Snapshot != first.Candidate.Snapshot || refreshContext.Completion.Preparation != adapter.prepared {
		t.Fatalf("refresh context=%+v err=%v", refreshContext, err)
	}
	worktree, err := f.db.Worktree(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	originalBuilder, _, err := f.db.LoadHistoricalProviderAttemptResult(ctx, first.Candidate.BuilderResult)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"refreshed publication candidate","changed_files":["README.md"],"commands":[["go","test","./..."]]}`)
	builderClaim := completeProvider(t, f.db, signer, building, fence, worktree.IdentityJSON, building.ConfigDigest, domain.PhaseBuild, "builder", originalBuilder.Claim.Binding, builderRaw, phaseartifact.Validation{TicketType: building.Type}, f.ref, worktree.Path, newBase)
	builderKey := providerKey(f.ref, domain.PhaseBuild, builderClaim)
	_, parsed, err := f.db.LoadHistoricalProviderAttemptResult(ctx, builderKey)
	if err != nil || parsed.Builder == nil {
		t.Fatalf("refreshed builder=%+v err=%v", parsed, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	head := makeCommit(t, worktree.Path, "second publication generation", "second generation\n")
	tree := gitOutput(t, worktree.Path, "rev-parse", "HEAD^{tree}")
	if parent := gitOutput(t, worktree.Path, "show", "-s", "--format=%P", head); parent != adapter.prepared.CommitOID {
		t.Fatalf("second-generation parent=%s want=%s", parent, adapter.prepared.CommitOID)
	}
	policy := first.Candidate.Snapshot.CommandPolicyDigest
	command := completeCommand(t, f.db, f.ref, building.Version, fence, worktree.IdentityJSON, worktree.Path, worktree.Branch, newBase, builderKey, first.Candidate.Snapshot.VerificationIntentDigest, first.Candidate.Snapshot.ProofDigest, refreshContext.Verification.Revision.CheckpointID, "sha256:"+policy, 0)
	successor := domain.CandidateSnapshot{Generation: first.Candidate.Snapshot.Generation + 1, BaseSHA: newBase, HeadSHA: head, TreeSHA: tree, SourceDigest: first.Candidate.Snapshot.SourceDigest, VerificationIntentDigest: first.Candidate.Snapshot.VerificationIntentDigest, ProofDigest: first.Candidate.Snapshot.ProofDigest, CommandPolicyDigest: policy, BuilderEvidenceDigest: builderDigest}
	if _, err := f.db.RecordCandidate(ctx, store.CandidateEvidence{Ref: f.ref, ExpectedVersion: building.Version, Fence: fence, Snapshot: successor, BuilderResult: builderKey, Commit: store.CommitObservation{CommitOID: head, ParentOID: adapter.prepared.CommitOID, TreeOID: tree}, Reason: "protected base refresh", CommandResult: command}); err != nil {
		t.Fatalf("record second-generation candidate: %v", err)
	}
	if _, err := f.db.TransitionCandidate(ctx, store.Transition{Ref: f.ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, successor); err != nil {
		t.Fatalf("transition second-generation candidate: %v", err)
	}

	setBaseRefreshIntegrationFakePRBase(t, f, first.PullRequest.Number, newBase)
	trackBaseRefreshIntegrationFakePRHeadOnPush(t, f, first.PullRequest.Number, head)
	publishing, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	second, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(ctx, f.ref, domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: publishing.RunnerEpoch})
	if err != nil || second.State != domain.StateWaitingCI || !second.Transitioned {
		t.Fatalf("second publication=%+v err=%v", second, err)
	}
	published, err := f.db.LoadPublishedCandidate(ctx, f.ref)
	if err != nil || published.Candidate.Snapshot != successor || published.PullRequest.Number != first.PullRequest.Number || published.PullRequest.HeadOID != head || published.PullRequest.BaseOID != newBase {
		t.Fatalf("second publication witness=%+v err=%v", published, err)
	}
	remote := f.github.Snapshot()
	if f.gitPushCount != 2 || f.github.MutationCount("pr_create") != 1 || f.github.MutationCount("pr_edit") != 1 || len(remote.PRs) != 1 || remote.PRs[0].Identity.Number != first.PullRequest.Number {
		t.Fatalf("publication continuity push=%d create=%d edit=%d prs=%+v", f.gitPushCount, f.github.MutationCount("pr_create"), f.github.MutationCount("pr_edit"), remote.PRs)
	}
}

func baseRefreshIntegrationWorker(f *publicationFixture, git *baseRefreshIntegrationGit) localruntime.Worker {
	return baseRefreshIntegrationWorkerWithGitHub(f, git, f.github)
}

func baseRefreshIntegrationWorkerWithGitHub(f *publicationFixture, git *baseRefreshIntegrationGit, github contracts.GitHub) localruntime.Worker {
	return localruntime.Worker{
		Store:              f.db,
		Publication:        publication.Worker{Store: f.db, Git: f.runner, GitHub: github},
		PublicationEnabled: true,
		BaseRefreshEnabled: true,
		BaseRefresh:        baserefresh.Coordinator{Store: f.db, Git: git},
	}
}

type baseRefreshIntegrationGitHub struct{ *testkit.FakeGH }

func (g baseRefreshIntegrationGitHub) ObservePublishedPullRequest(ctx context.Context, want contracts.PullRequestIdentity) (contracts.PublishedPullRequestObservation, error) {
	observed, err := g.FakeGH.ObservePublishedPullRequest(ctx, want)
	if err != nil {
		return contracts.PublishedPullRequestObservation{}, err
	}
	observed.Identity.BaseOID = g.Snapshot().BaseHeadOID
	observed.BaseHeadOID = observed.Identity.BaseOID
	return observed, nil
}

func setBaseRefreshIntegrationFakePRBase(t *testing.T, f *publicationFixture, number int, base string) {
	t.Helper()
	state := f.github.Snapshot()
	found := false
	for index := range state.PRs {
		if state.PRs[index].Identity.Number == number {
			state.PRs[index].Identity.BaseOID = base
			found = true
		}
	}
	if !found || state.BaseHeadOID != base {
		t.Fatalf("fake PR/base not ready number=%d base=%s state=%+v", number, base, state)
	}
	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(f.bare), "github.json")
	tmp, err := os.CreateTemp(filepath.Dir(path), ".github-base-refresh-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.Write(append(payload, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		t.Fatal(err)
	}
}

func trackBaseRefreshIntegrationFakePRHeadOnPush(t *testing.T, f *publicationFixture, number int, head string) {
	t.Helper()
	delegate := f.runner.Run
	branch := f.dbWorktreeBranch(t)
	f.runner.Run = func(ctx context.Context, binary string, args, env []string) ([]byte, error) {
		target := false
		for index, arg := range args {
			if arg == "push" && index+2 < len(args) && args[index+2] == head+":refs/heads/"+branch {
				target = true
			}
		}
		output, err := delegate(ctx, binary, args, env)
		if err == nil && target {
			if updateErr := f.github.SetPullRequestHeadOIDForTest(number, head); updateErr != nil {
				return nil, updateErr
			}
		}
		return output, err
	}
}

func (f *publicationFixture) dbWorktreeBranch(t *testing.T) string {
	t.Helper()
	worktree, err := f.db.Worktree(context.Background(), f.ref)
	if err != nil {
		t.Fatal(err)
	}
	return worktree.Branch
}

func advanceBaseRefreshIntegrationMain(t *testing.T, bare string) string {
	t.Helper()
	root := t.TempDir()
	repository := filepath.Join(root, "base-advance")
	runGit(t, root, "clone", "--branch", "main", "--single-branch", bare, repository)
	runGit(t, repository, "config", "user.name", "base-refresh-fixture")
	runGit(t, repository, "config", "user.email", "base-refresh-fixture@example.test")
	if err := os.WriteFile(filepath.Join(repository, "BASE-REFRESH.md"), []byte("fresh protected base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "--", "BASE-REFRESH.md")
	runGit(t, repository, "commit", "-m", "advance protected base")
	base := gitOutput(t, repository, "rev-parse", "HEAD^{commit}")
	runGit(t, repository, "push", "origin", "HEAD:refs/heads/main")
	return base
}

func assertBaseRefreshIntegrationRemote(t *testing.T, f *publicationFixture, worktree store.StoredWorktree, base string) {
	t.Helper()
	if got := gitOutput(t, f.bare, "rev-parse", "--verify", "refs/heads/main^{commit}"); got != base {
		t.Fatalf("remote main=%s want=%s", got, base)
	}
	if got := gitOutput(t, f.bare, "for-each-ref", "--format=%(objectname)", "refs/heads/"+worktree.Branch); got != "" {
		t.Fatalf("remote candidate branch unexpectedly exists at %s", got)
	}
}

func assertBaseRefreshIntegrationNoPublicationMutation(t *testing.T, f *publicationFixture, worktree store.StoredWorktree, newBase string) {
	t.Helper()
	if f.gitPushCount != 0 || f.github.MutationCount("pr_create") != 0 || f.github.MutationCount("pr_update") != 0 {
		t.Fatalf("refresh crossed publication boundary push=%d pr_create=%d pr_update=%d", f.gitPushCount, f.github.MutationCount("pr_create"), f.github.MutationCount("pr_update"))
	}
	assertBaseRefreshIntegrationRemote(t, f, worktree, newBase)
}

func assertBaseRefreshIntegrationPhysicalState(t *testing.T, worktree store.StoredWorktree, prepared gitboundary.BaseRefreshPreparation, newBase string) {
	t.Helper()
	if head := gitOutput(t, worktree.Path, "rev-parse", "HEAD^{commit}"); head != prepared.CommitOID {
		t.Fatalf("physical head=%s want=%s", head, prepared.CommitOID)
	}
	if base := gitOutput(t, worktree.Path, "rev-parse", "--verify", baseRefreshIntegrationWorktreeBaseRef(worktree.Branch)+"^{commit}"); base != newBase {
		t.Fatalf("physical base=%s want=%s", base, newBase)
	}
	if parents := strings.Fields(gitOutput(t, worktree.Path, "show", "-s", "--format=%P", prepared.CommitOID)); !reflect.DeepEqual(parents, []string{prepared.Parents[0], prepared.Parents[1]}) {
		t.Fatalf("prepared parents=%v want=%v", parents, prepared.Parents)
	}
	if tree := gitOutput(t, worktree.Path, "rev-parse", "HEAD^{tree}"); tree != prepared.TreeOID {
		t.Fatalf("physical tree=%s want=%s", tree, prepared.TreeOID)
	}
	if status := gitOutput(t, worktree.Path, "status", "--porcelain"); status != "" {
		t.Fatalf("refreshed worktree dirty: %q", status)
	}
}

func baseRefreshIntegrationWorktreeBaseRef(branch string) string {
	sum := sha256.Sum256([]byte(branch))
	return fmt.Sprintf("refs/sf/worktree-base/%x", sum)
}

type baseRefreshIntegrationGit struct {
	store           *store.Store
	runner          gitboundary.Runner
	loseNextApply   bool
	loseNextProof   bool
	verifyCalls     int
	prepareCalls    int
	applyCalls      int
	refTransactions int
	prepared        gitboundary.BaseRefreshPreparation
}

func (g *baseRefreshIntegrationGit) VerifyExactProtectedBase(ctx context.Context, witness contracts.ProtectedBranchWitness) error {
	g.verifyCalls++
	if err := g.runner.VerifyExactProtectedBase(ctx, witness); err != nil {
		return err
	}
	if g.loseNextProof {
		g.loseNextProof = false
		return errBaseRefreshProofResponseLost
	}
	return nil
}

func (g *baseRefreshIntegrationGit) PrepareProtectedBaseRefresh(ctx context.Context, payload []byte, claim contracts.GitMutationClaim) (gitboundary.BaseRefreshPreparation, error) {
	g.prepareCalls++
	prepared, err := g.runner.PrepareProtectedBaseRefresh(ctx, payload, claim)
	if err == nil {
		g.prepared = prepared
	}
	return prepared, err
}

// ApplyProtectedBaseRefresh is a typed test-only stand-in for the production
// stdin-aware Git gate. It still acquires the real Store lease, reads the
// Store-recorded prepared object, validates exact old/new states, and performs
// one paired update-ref transaction. It is not a generic argv seam and is not
// evidence for the production process-launch supervisor, which has independent
// native Git tests.
func (g *baseRefreshIntegrationGit) ApplyProtectedBaseRefresh(ctx context.Context, payload []byte, claim contracts.GitMutationClaim) (applied gitboundary.BaseRefreshApplied, returnedErr error) {
	g.applyCalls++
	input, oldIdentity, err := decodeBaseRefreshIntegrationInput(payload, claim)
	if err != nil {
		return applied, err
	}
	lease, err := g.store.AcquireGitMutation(ctx, claim)
	if err != nil {
		return applied, err
	}
	defer func() {
		if releaseErr := lease.Release(); releaseErr != nil {
			returnedErr = errors.Join(returnedErr, releaseErr)
			applied = gitboundary.BaseRefreshApplied{}
		}
	}()
	reader, ok := lease.(contracts.GitBaseRefreshPreparedReader)
	if !ok {
		return applied, errors.New("fixture lease does not expose prepared base refresh")
	}
	prepared, found, err := reader.PreparedBaseRefresh(ctx)
	if err != nil || !found || prepared.Parents != [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID} || !baseRefreshIntegrationOID(prepared.CommitOID) || !baseRefreshIntegrationOID(prepared.TreeOID) {
		return applied, fmt.Errorf("fixture prepared refresh found=%v value=%+v: %w", found, prepared, errors.Join(err, gitboundary.ErrIdentityMismatch))
	}
	observed, err := g.gitOne(ctx, input.Worktree.Path, nil, "show", "-s", "--format=%T %P", prepared.CommitOID)
	if err != nil || observed != strings.Join([]string{prepared.TreeOID, prepared.Parents[0], prepared.Parents[1]}, " ") {
		return applied, fmt.Errorf("fixture prepared commit mismatch: %w", errors.Join(err, gitboundary.ErrIdentityMismatch))
	}

	isNew, err := g.refreshState(ctx, input, oldIdentity, prepared)
	if err != nil {
		return applied, err
	}
	if !isNew {
		if err := lease.Check(ctx); err != nil {
			return applied, err
		}
		if _, err := g.git(ctx, input.Worktree.Path, nil, "read-tree", "-u", "-m", claim.ExpectedHeadOID, prepared.CommitOID); err != nil {
			return applied, fmt.Errorf("fixture project prepared tree: %w", err)
		}
		if err := lease.Check(ctx); err != nil {
			return applied, err
		}
		transaction := []byte("start\nupdate refs/heads/" + input.Worktree.Branch + " " + prepared.CommitOID + " " + claim.ExpectedHeadOID + "\nupdate " + baseRefreshIntegrationWorktreeBaseRef(input.Worktree.Branch) + " " + claim.ExpectedBaseOID + " " + input.Worktree.BaseSHA + "\nprepare\ncommit\n")
		output, err := g.git(ctx, input.Worktree.Path, transaction, "update-ref", "--no-deref", "--stdin")
		if err != nil || string(output) != "start: ok\nprepare: ok\ncommit: ok\n" {
			return applied, fmt.Errorf("fixture paired ref transaction output=%q: %w", output, errors.Join(err, gitboundary.ErrIdentityMismatch))
		}
		g.refTransactions++
	}
	if err := lease.Check(ctx); err != nil {
		return applied, err
	}
	isNew, err = g.refreshState(ctx, input, oldIdentity, prepared)
	if err != nil || !isNew {
		return applied, fmt.Errorf("fixture refresh did not reach exact new state: %w", errors.Join(err, gitboundary.ErrUnsafeWorktree))
	}
	newIdentity := oldIdentity
	newIdentity.BaseHead = input.NewBaseSHA
	applied = gitboundary.BaseRefreshApplied{Preparation: prepared, Identity: newIdentity}
	if g.loseNextApply {
		g.loseNextApply = false
		return gitboundary.BaseRefreshApplied{}, errBaseRefreshApplyResponseLost
	}
	return applied, nil
}

type baseRefreshIntegrationInput struct {
	Format               string
	Ref                  domain.TicketRef
	TicketVersion        uint64
	Fence                domain.Fence
	SourceDigest         string
	ConfigGeneration     uint64
	ConfigDigest         string
	ConfigSnapshotDigest string
	Repository           string
	BaseRef              string
	Candidate            store.StoredCandidate
	Worktree             store.StoredWorktree
	NewBaseSHA           string
	BaseProofSemanticKey string
	ProtectedPaths       []string
}

func decodeBaseRefreshIntegrationInput(payload []byte, claim contracts.GitMutationClaim) (baseRefreshIntegrationInput, gitboundary.Identity, error) {
	var input baseRefreshIntegrationInput
	var identity gitboundary.Identity
	sum := sha256.Sum256(payload)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if len(payload) == 0 || len(payload) > 128<<10 || claim.RequestDigest != "sha256:"+hex.EncodeToString(sum[:]) || decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || json.Unmarshal(input.Worktree.IdentityJSON, &identity) != nil {
		return input, identity, gitboundary.ErrIdentityMismatch
	}
	if claim.Operation != "refresh-base" || claim.TicketRef != input.Ref || claim.Repository != input.Repository || claim.Worktree != input.Worktree.Path || claim.Branch != input.Worktree.Branch || claim.BaseRef != input.BaseRef || claim.ExpectedBaseOID != input.NewBaseSHA || claim.ExpectedHeadOID != input.Candidate.Snapshot.HeadSHA || input.Format != "sf.protected-base-refresh.v1" || input.TicketVersion == 0 || input.TicketVersion > claim.TicketVersion || input.Fence.LeaderEpoch == 0 || input.Fence.LeaderEpoch > claim.LeaderEpoch || input.Fence.RunnerEpoch == 0 || input.Fence.RunnerEpoch > claim.RunnerEpoch || input.Fence.ClaimEpoch != 0 || input.Worktree.State != "registered" || input.Worktree.BaseSHA != input.Candidate.Snapshot.BaseSHA || len(input.ProtectedPaths) == 0 || !baseRefreshIntegrationOID(input.Worktree.BaseSHA) || !baseRefreshIntegrationOID(input.NewBaseSHA) || !baseRefreshIntegrationOID(input.Candidate.Snapshot.HeadSHA) || !baseRefreshIntegrationOID(input.Candidate.Snapshot.TreeSHA) {
		return input, identity, gitboundary.ErrIdentityMismatch
	}
	if identity.Repository != input.Repository || identity.Worktree != input.Worktree.Path || identity.HeadRef != input.Worktree.Branch || identity.BaseRef != input.BaseRef || identity.BaseHead != input.Worktree.BaseSHA {
		return input, identity, gitboundary.ErrIdentityMismatch
	}
	return input, identity, nil
}

func (g *baseRefreshIntegrationGit) refreshState(ctx context.Context, input baseRefreshIntegrationInput, oldIdentity gitboundary.Identity, prepared gitboundary.BaseRefreshPreparation) (bool, error) {
	head, err := g.gitOne(ctx, input.Worktree.Path, nil, "rev-parse", "--verify", "refs/heads/"+input.Worktree.Branch+"^{commit}")
	if err != nil {
		return false, err
	}
	base, err := g.gitOne(ctx, input.Worktree.Path, nil, "rev-parse", "--verify", baseRefreshIntegrationWorktreeBaseRef(input.Worktree.Branch)+"^{commit}")
	if err != nil {
		return false, err
	}
	isOld := head == input.Candidate.Snapshot.HeadSHA && base == input.Worktree.BaseSHA
	isNew := head == prepared.CommitOID && base == input.NewBaseSHA
	if !isOld && !isNew {
		return false, gitboundary.ErrUnexpectedRemote
	}
	expected := oldIdentity
	expectedTree := input.Candidate.Snapshot.TreeSHA
	if isNew {
		expected.BaseHead = input.NewBaseSHA
		expectedTree = prepared.TreeOID
	}
	if err := g.runner.Reauthenticate(ctx, expected); err != nil {
		return false, err
	}
	indexTree, err := g.gitOne(ctx, input.Worktree.Path, nil, "write-tree")
	if err != nil || indexTree != expectedTree {
		return false, fmt.Errorf("fixture index tree=%s want=%s: %w", indexTree, expectedTree, errors.Join(err, gitboundary.ErrUnsafeWorktree))
	}
	if _, err := g.git(ctx, input.Worktree.Path, nil, "diff", "--quiet", "--no-ext-diff", "--no-textconv"); err != nil {
		return false, gitboundary.ErrUnsafeWorktree
	}
	if _, err := g.git(ctx, input.Worktree.Path, nil, "diff", "--cached", "--quiet", "--no-ext-diff", "--no-textconv"); err != nil {
		return false, gitboundary.ErrUnsafeWorktree
	}
	if extra, err := g.git(ctx, input.Worktree.Path, nil, "ls-files", "--others", "--exclude-standard", "-z"); err != nil || len(extra) != 0 {
		return false, fmt.Errorf("fixture untracked worktree data: %w", errors.Join(err, gitboundary.ErrUnsafeWorktree))
	}
	if ignored, err := g.git(ctx, input.Worktree.Path, nil, "ls-files", "--others", "--exclude-standard", "--ignored", "-z"); err != nil || len(ignored) != 0 {
		return false, fmt.Errorf("fixture ignored worktree data: %w", errors.Join(err, gitboundary.ErrUnsafeWorktree))
	}
	return isNew, nil
}

func (g *baseRefreshIntegrationGit) gitOne(ctx context.Context, directory string, stdin []byte, args ...string) (string, error) {
	output, err := g.git(ctx, directory, stdin, args...)
	value := strings.TrimSuffix(string(output), "\n")
	if err != nil || value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") {
		return "", errors.Join(err, gitboundary.ErrIdentityMismatch)
	}
	return value, nil
}

func (g *baseRefreshIntegrationGit) git(ctx context.Context, directory string, stdin []byte, args ...string) ([]byte, error) {
	commandCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	prefix := []string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "-c", "core.fsmonitor=false"}
	command := exec.CommandContext(commandCtx, "/usr/bin/git", append(prefix, args...)...)
	command.Env = []string{
		"PATH=/usr/bin:/bin",
		"LANG=C",
		"LC_ALL=C",
		"HOME=" + g.runner.Home,
		"TMPDIR=" + g.runner.Home,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_OPTIONAL_LOCKS=0",
	}
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	output, err := command.CombinedOutput()
	if len(output) > 64<<10 {
		return nil, errors.New("fixture Git output exceeded 64 KiB")
	}
	if err != nil {
		return nil, fmt.Errorf("fixture git %v: %w: %s", args, err, output)
	}
	return output, nil
}

func baseRefreshIntegrationOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return false
		}
	}
	return true
}
