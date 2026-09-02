package publication_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	gitboundary "github.com/nysa-company/sf/internal/git"
	githubboundary "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/localruntime"
	"github.com/nysa-company/sf/internal/mergeproof"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/publication"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

func TestWorkerPublishesRealCandidateExactlyOnce(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()

	got, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(f.ctx, f.ref, f.fence)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.StateWaitingCI || !got.Transitioned || got.Replayed {
		t.Fatalf("first publication result=%+v", got)
	}
	if f.github.MutationCount("pr_create") != 1 || f.gitPushCount != 1 {
		t.Fatalf("mutations push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
	published, err := f.db.PublishedCandidate(f.ctx, f.ref)
	if err != nil || published.RemoteBranchOID != f.candidate.Snapshot.HeadSHA || published.PullRequest.Number != 1 || !published.PullRequest.FactoryOwned || !published.PullRequestDraft {
		t.Fatalf("published candidate=%+v err=%v", published, err)
	}

	// A scheduler replay sees the terminal publication state and cannot issue
	// another push or PR creation.
	replay, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(f.ctx, f.ref, f.fence)
	if err != nil || replay.State != domain.StateWaitingCI || replay.Transitioned || replay.Replayed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if f.github.MutationCount("pr_create") != 1 || f.gitPushCount != 1 {
		t.Fatalf("replay mutated push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

// TestGuardedApprovalMergeRecoversLostProofResponse composes the production
// local-runtime merge path with the real Store, production GitHub client, and
// real protected-ref coordinator. The FakeGH supervisor advances only the
// fixture's private bare remote at the hosted-merge boundary, so the proof is
// an actual Git fetch/ancestry check rather than a fabricated Store witness.
func TestGuardedApprovalMergeRecoversLostProofResponse(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()

	// Publication is real: it records immutable candidate/PR evidence and
	// advances publishing -> waiting_ci before any approval exists.
	if _, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(f.ctx, f.ref, f.fence); err != nil {
		t.Fatalf("publish candidate: %v", err)
	}
	published, err := f.db.LoadPublishedCandidate(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.github.SetBaseHeadOIDForTest(published.PullRequest.BaseOID); err != nil {
		t.Fatal(err)
	}
	if err := f.github.SetRulesetsForTest(guardedFixtureRuleset()); err != nil {
		t.Fatal(err)
	}
	if err := f.github.SetChecks(published.PullRequest.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "fixture-unit", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}

	// Waiting CI and final review each consume their real Store evidence before
	// the operator's exact-head decision can enter merging.
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	workflowEngine := engine.New(f.db, spec)
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}
	ciResult, err := (localruntime.CIWorker{Store: f.db, Observer: f.github}).Run(f.ctx, f.ref, fence)
	if err != nil || !ciResult.Transitioned || ciResult.State != domain.StateReviewing {
		t.Fatalf("CI result=%+v err=%v", ciResult, err)
	}
	ticket, err = f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	completePublishedFinalReview(t, f, ticket, fence)
	if _, err := f.db.TransitionFinalReview(f.ctx, store.Transition{Ref: f.ref, ExpectedVersion: ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("final review transition: %v", err)
	}

	ticket, err = f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	approval, err := f.db.ApplyOperatorDecision(f.ctx, store.OperatorDecisionRequest{OperatorDecision: store.OperatorDecision{Ref: f.ref, ExpectedVersion: ticket.Version, Fence: fence, ReviewedHead: published.Candidate.Snapshot.HeadSHA, OperatorUID: 501, Decision: "approved"}})
	if err != nil || approval.Version != ticket.Version+1 {
		t.Fatalf("approval=%+v err=%v", approval, err)
	}
	// A daemon can also crash after the atomic approval transition but before
	// the merge worker has recorded any intent. That restart has no external
	// mutation to reconcile, yet must authenticate the exact review+approval
	// lineage before it installs the next runner fence.
	preMergeLeader, err := f.db.AcquireLeader(f.ctx, f.ref.Channel, "guarded-merge-before-intent")
	if err != nil {
		t.Fatal(err)
	}
	if effects, err := f.db.ReconcileEffects(f.ctx, f.ref.Channel, preMergeLeader); err != nil || len(effects) != 0 {
		t.Fatalf("pre-intent effect reconciliation=%+v err=%v", effects, err)
	}
	if changed, err := f.db.FenceRecoveredRunners(f.ctx, f.ref.Channel, preMergeLeader); err != nil || changed != 1 {
		t.Fatalf("pre-intent restart fence changed=%d err=%v", changed, err)
	}

	// Crash after the local merge effect is claimed but before MergeExactHead
	// persists its immutable merge intent. Durable intent absence proves the
	// external handoff never began, so recovery may retire this revoked claim
	// and authorize exactly one fresh attempt under the new runner fence.
	preIntentTicket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	preIntentFence := domain.Fence{LeaderEpoch: preMergeLeader, RunnerEpoch: preIntentTicket.RunnerEpoch}
	mergeKey := "merge/" + string(f.ref.Channel) + "/" + string(f.ref.Project) + "/" + string(f.ref.Ticket) + "/" + published.Candidate.Snapshot.HeadSHA
	mergeAuthorization := domain.MergeAuthorization{ReviewedHead: published.Candidate.Snapshot.HeadSHA, CurrentHead: published.Candidate.Snapshot.HeadSHA, ReviewedBaseSHA: published.Candidate.Snapshot.BaseSHA, CurrentBaseSHA: published.Candidate.Snapshot.BaseSHA, ReviewedBaseHeadOID: published.PullRequest.BaseOID, CurrentBaseHeadOID: published.PullRequest.BaseOID, Approved: true, GatesGreen: true}
	mergeDigest := githubboundary.CanonicalMergeRequestDigest(published.PullRequest, published.Candidate.Snapshot.HeadSHA, "squash", mergeAuthorization)
	if _, err := f.db.PlanEffect(f.ctx, store.EffectPlan{SemanticKey: mergeKey, Ref: f.ref, Kind: "merge", TicketVersion: preIntentTicket.Version, Fence: preIntentFence, RequestDigest: mergeDigest}); err != nil {
		t.Fatalf("plan pre-intent merge effect: %v", err)
	}
	claimed, err := f.db.ClaimEffect(f.ctx, store.EffectFence{SemanticKey: mergeKey, Ref: f.ref, TicketVersion: preIntentTicket.Version, Fence: preIntentFence})
	if err != nil || !claimed.Claimed {
		t.Fatalf("claim pre-intent merge effect=%+v err=%v", claimed, err)
	}
	if _, found, err := f.db.MergeIntent(f.ctx, mergeKey); err != nil || found {
		t.Fatalf("pre-intent claim unexpectedly has intent found=%v err=%v", found, err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	preIntentDB, err := store.Open(f.ctx, filepath.Join(filepath.Dir(f.bare), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	f.db = preIntentDB
	preMergeLeader, err = preIntentDB.AcquireLeader(f.ctx, f.ref.Channel, "guarded-merge-after-claim-before-intent")
	if err != nil {
		t.Fatal(err)
	}
	if effects, err := preIntentDB.ReconcileEffects(f.ctx, f.ref.Channel, preMergeLeader); err != nil || len(effects) != 1 || effects[0].SemanticKey != mergeKey || effects[0].State != store.EffectUncertain {
		t.Fatalf("pre-intent claimed effect reconciliation=%+v err=%v", effects, err)
	}
	if changed, err := preIntentDB.FenceRecoveredRunners(f.ctx, f.ref.Channel, preMergeLeader); err != nil || changed != 1 {
		t.Fatalf("pre-intent claimed effect fence changed=%d err=%v", changed, err)
	}
	f.runner.MutationAuthority = preIntentDB
	workflowEngine = engine.New(preIntentDB, spec)

	// Materialize the exact hosted-merge result locally but do not advance the
	// bare protected ref until FakeGH receives its one guarded merge command.
	project, err := f.db.Project(f.ctx, f.ref.Channel, f.ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, project.Path, "merge", "--squash", worktree.Branch)
	runGit(t, project.Path, "-c", "user.name=fixture", "-c", "user.email=fixture@example.test", "commit", "-m", "guarded hosted merge")
	mergeCommit := gitOutput(t, project.Path, "rev-parse", "HEAD")
	if err := f.github.SetMergeCommitForTest(mergeCommit); err != nil {
		t.Fatal(err)
	}

	proofFetches := 0
	priorRun := f.runner.Run
	f.runner.Run = func(ctx context.Context, binary string, args []string, env []string) ([]byte, error) {
		for _, arg := range args {
			if arg == "fetch" {
				proofFetches++
				break
			}
		}
		return priorRun(ctx, binary, args, env)
	}
	advanceFixtureMain := func() error {
		command := exec.Command("/usr/bin/git", "-C", project.Path, "push", f.bare, mergeCommit+":refs/heads/main")
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("advance fixture protected ref: %w: %s", err, output)
		}
		return nil
	}

	// The first verifier call commits the protected-ref proof, then simulates
	// a response lost before github.Client can settle the parent merge effect.
	// Its restart call must reuse that immutable proof without a second merge or
	// protected-ref fetch.
	lostProof := &lostProofResponseVerifier{Coordinator: mergeproof.Coordinator{Store: f.db, Git: f.runner}, loseNext: true}
	client := newStoreBackedFakeGHClient(t, f, lostProof, advanceFixtureMain)
	merging, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	mergeFence := domain.Fence{LeaderEpoch: preMergeLeader, RunnerEpoch: merging.RunnerEpoch}
	worker := localruntime.NewWorker(f.db, workflowworker.Worker{}, publication.Worker{Store: f.db, Git: f.runner, GitHub: client})
	worker.Engine = workflowEngine
	if _, err := worker.Run(f.ctx, f.ref, mergeFence); err == nil {
		t.Fatal("lost protected-proof response unexpectedly settled merge")
	}
	uncertain, err := f.db.Effect(f.ctx, mergeKey)
	if err != nil || uncertain.State != store.EffectUncertain || f.github.MutationCount("pr_merge") != 1 || proofFetches != 1 {
		t.Fatalf("lost response effect=%+v err=%v merge_calls=%d proof_fetches=%d", uncertain, err, f.github.MutationCount("pr_merge"), proofFetches)
	}

	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(f.ctx, filepath.Join(filepath.Dir(f.bare), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	newLeader, err := restarted.AcquireLeader(f.ctx, f.ref.Channel, "guarded-merge-restart")
	if err != nil {
		t.Fatal(err)
	}
	firstReconcile, err := restarted.ReconcileEffects(f.ctx, f.ref.Channel, newLeader)
	if err != nil || len(firstReconcile) != 1 || firstReconcile[0].SemanticKey != mergeKey || firstReconcile[0].State != store.EffectUncertain {
		t.Fatalf("restart effect reconciliation=%+v err=%v", firstReconcile, err)
	}
	if replay, err := restarted.ReconcileEffects(f.ctx, f.ref.Channel, newLeader); err != nil || len(replay) != 1 || replay[0].ClaimEpoch != firstReconcile[0].ClaimEpoch {
		t.Fatalf("same-leader reconcile was not idempotent first=%+v replay=%+v err=%v", firstReconcile, replay, err)
	}
	// Crash after ReconcileEffects but before FenceRecoveredRunners. The next
	// daemon leader may advance the uncertain claim once more, but the unchanged
	// ticket endpoint must still be fenceable without replaying the merge.
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err = store.Open(f.ctx, filepath.Join(filepath.Dir(f.bare), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	fenceLeader, err := restarted.AcquireLeader(f.ctx, f.ref.Channel, "guarded-merge-after-reconcile-crash")
	if err != nil {
		t.Fatal(err)
	}
	if effects, err := restarted.ReconcileEffects(f.ctx, f.ref.Channel, fenceLeader); err != nil || len(effects) != 1 || effects[0].SemanticKey != mergeKey || effects[0].ClaimEpoch <= firstReconcile[0].ClaimEpoch {
		t.Fatalf("restart effect reconciliation=%+v err=%v", effects, err)
	}
	if changed, err := restarted.FenceRecoveredRunners(f.ctx, f.ref.Channel, fenceLeader); err != nil || changed != 1 {
		t.Fatalf("restart fence changed=%d err=%v", changed, err)
	}
	recovered, err := restarted.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	// Crash once more after the first runner fence but before the resumed
	// worker observes GitHub. Production recovery must again reconcile the
	// stranded effect before advancing the ticket fence, without authorizing a
	// second hosted merge or protected-ref fetch.
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err = store.Open(f.ctx, filepath.Join(filepath.Dir(f.bare), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	secondLeader, err := restarted.AcquireLeader(f.ctx, f.ref.Channel, "guarded-merge-second-restart")
	if err != nil {
		t.Fatal(err)
	}
	if effects, err := restarted.ReconcileEffects(f.ctx, f.ref.Channel, secondLeader); err != nil || len(effects) != 1 || effects[0].SemanticKey != mergeKey || effects[0].State != store.EffectUncertain {
		t.Fatalf("second restart effect reconciliation=%+v err=%v", effects, err)
	}
	if changed, err := restarted.FenceRecoveredRunners(f.ctx, f.ref.Channel, secondLeader); err != nil || changed != 1 {
		t.Fatalf("second restart fence changed=%d err=%v", changed, err)
	}
	recovered, err = restarted.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.github.MutationCount("pr_merge"); got != 1 || proofFetches != 1 {
		t.Fatalf("second restart replayed external mutation merge_calls=%d proof_fetches=%d", got, proofFetches)
	}
	f.runner.MutationAuthority = restarted
	restartedClient := newStoreBackedFakeGHClient(t, f, mergeproof.Coordinator{Store: restarted, Git: f.runner}, advanceFixtureMain)
	restartedWorker := localruntime.NewWorker(restarted, workflowworker.Worker{}, publication.Worker{Store: restarted, Git: f.runner, GitHub: restartedClient})
	restartedWorker.Engine = &failMergeObservedOnceEngine{StateMachine: engine.New(restarted, spec)}
	if _, err := restartedWorker.Run(f.ctx, f.ref, domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: recovered.RunnerEpoch}); err == nil {
		t.Fatal("post-merge observation crash was not injected")
	}
	stranded, err := restarted.Ticket(f.ctx, f.ref)
	if err != nil || stranded.State != domain.StateMerging {
		t.Fatalf("post-proof stranded ticket=%+v err=%v", stranded, err)
	}
	confirmedBeforePause, err := restarted.Effect(f.ctx, mergeKey)
	if err != nil || confirmedBeforePause.State != store.EffectConfirmed || confirmedBeforePause.TicketVersion != stranded.Version || confirmedBeforePause.LeaderEpoch != secondLeader || confirmedBeforePause.RunnerEpoch != stranded.RunnerEpoch {
		t.Fatalf("confirmed pre-pause merge effect=%+v err=%v", confirmedBeforePause, err)
	}
	if got := f.github.MutationCount("pr_merge"); got != 1 || proofFetches != 1 {
		t.Fatalf("post-proof crash replayed external mutation merge_calls=%d proof_fetches=%d", got, proofFetches)
	}

	// Pause and rearm after the merge and child protected-ref proof are already
	// confirmed. The next worker must re-observe the immutable merge intent,
	// reuse the child proof, and promote the parent effect to the resumed fence
	// without a second hosted merge or Git fetch.
	controlProof, err := restarted.ControlProof(f.ctx, f.ref)
	if err != nil || !controlProof.Drained() {
		t.Fatalf("pre-pause control proof=%+v err=%v", controlProof, err)
	}
	stopping, err := restarted.TransitionAndInvalidateRunner(f.ctx, store.Transition{
		Ref: f.ref, ExpectedVersion: stranded.Version, From: domain.StateMerging, To: domain.StateStopping,
		ResumeState: domain.StateMerging, Trigger: "operator_pause_or_take",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: stranded.RunnerEpoch}, EventPayload: `{"intent":"pause"}`,
	})
	if err != nil {
		t.Fatalf("pause confirmed merge: %v", err)
	}
	pausedResult, err := restarted.CompleteControlTransition(f.ctx, store.Transition{
		Ref: f.ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: domain.StateMerging, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: stranded.RunnerEpoch + 1}, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("complete confirmed merge pause: %v", err)
	}
	resumeResult, err := engine.New(restarted, spec).Signal(f.ctx, contracts.SignalRequest{
		Ticket: f.ref, TicketVersion: pausedResult.Version, From: domain.StatePaused, Trigger: "operator_resume",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: stranded.RunnerEpoch + 1},
		Attributes: map[string]string{
			"operator_identity_authenticated": "true", "takeover_diff_none": "true",
			"branch_remote_identity_exact": "true", "prerequisites_green": "true",
		},
		EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("resume confirmed merge: %v", err)
	}
	resumed, err := restarted.Ticket(f.ctx, f.ref)
	if err != nil || resumeResult.To != domain.StateMerging || resumeResult.TicketVersion != pausedResult.Version+1 || resumeResult.EventID == "" || resumed.State != domain.StateMerging || resumed.Version != resumeResult.TicketVersion || resumed.ResumeState != "" || resumed.RunnerEpoch != stranded.RunnerEpoch+1 {
		t.Fatalf("resume confirmed merge result=%+v ticket=%+v err=%v", resumeResult, resumed, err)
	}
	capability, err := restarted.PostPublicationRearmProof(f.ctx, f.ref, controlProof.Ticket)
	if err != nil {
		t.Fatalf("prove confirmed merge rearm: %v", err)
	}
	var admission *store.RuntimeAdmissionCapability
	if err := restarted.ActivateRearm(f.ctx, capability, func(value *store.RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("fixture: runtime admission was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate confirmed merge rearm admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(f.ctx); err != nil {
		t.Fatalf("open confirmed merge rearm: %v", err)
	}

	mergeRetryPausedResult, err := restarted.Transition(f.ctx, store.Transition{
		Ref: f.ref, ExpectedVersion: resumed.Version, From: domain.StateMerging, To: domain.StatePaused,
		ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: resumed.RunnerEpoch}, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatalf("pause confirmed merge for retry: %v", err)
	}
	mergeRetryPaused, err := restarted.Ticket(f.ctx, f.ref)
	if err != nil || mergeRetryPaused.State != domain.StatePaused || mergeRetryPaused.Version != mergeRetryPausedResult.Version || mergeRetryPaused.ResumeState != domain.StateMerging || mergeRetryPaused.RunnerEpoch != resumed.RunnerEpoch {
		t.Fatalf("paused confirmed merge for retry ticket=%+v result=%+v err=%v", mergeRetryPaused, mergeRetryPausedResult, err)
	}
	mergeRetryResult, err := engine.New(restarted, spec).Signal(f.ctx, contracts.SignalRequest{
		Ticket: f.ref, TicketVersion: mergeRetryPaused.Version, From: domain.StatePaused, Trigger: "operator_retry",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: mergeRetryPaused.RunnerEpoch},
		Attributes: map[string]string{
			"operator_identity_authenticated": "true", "pause_reason_retryable": "true",
			"typed_prerequisites_satisfied": "true", "runner_epoch_current": "true",
		},
		EventPayload: `{"intent":"retry"}`,
	})
	if err != nil {
		t.Fatalf("retry confirmed merge: %v", err)
	}
	resumed, err = restarted.Ticket(f.ctx, f.ref)
	if err != nil || mergeRetryResult.To != domain.StateMerging || mergeRetryResult.TicketVersion != mergeRetryPaused.Version+1 || mergeRetryResult.EventID == "" || resumed.State != domain.StateMerging || resumed.Version != mergeRetryResult.TicketVersion || resumed.ResumeState != "" || resumed.RunnerEpoch != mergeRetryPaused.RunnerEpoch {
		t.Fatalf("retry confirmed merge result=%+v ticket=%+v err=%v", mergeRetryResult, resumed, err)
	}
	if needed, err := restarted.RuntimeRearmNeeded(f.ctx, f.ref); err != nil || !needed {
		t.Fatalf("retry confirmed merge rearm needed=%v err=%v", needed, err)
	}
	mergeRetryStopped, err := restarted.StoppedRuntimeTicket(f.ctx, f.ref)
	if err != nil {
		t.Fatalf("load confirmed merge retry stop: %v", err)
	}
	mergeRetryCapability, err := restarted.PostPublicationRearmProof(f.ctx, f.ref, mergeRetryStopped)
	if err != nil {
		t.Fatalf("prove confirmed merge retry rearm: %v", err)
	}
	var mergeRetryAdmission *store.RuntimeAdmissionCapability
	if err := restarted.ActivateRearm(f.ctx, mergeRetryCapability, func(value *store.RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("fixture: merge retry runtime admission was not consumable")
		}
		mergeRetryAdmission = value
		return nil
	}); err != nil || mergeRetryAdmission == nil {
		t.Fatalf("activate confirmed merge retry admission=%v err=%v", mergeRetryAdmission, err)
	}
	if err := mergeRetryAdmission.OpenStoreAdmission(f.ctx); err != nil {
		t.Fatalf("open confirmed merge retry rearm: %v", err)
	}

	restartedWorker.Engine = &failMergeObservedOnceEngine{StateMachine: engine.New(restarted, spec)}
	resumedFence := domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: resumed.RunnerEpoch}
	if _, err := restartedWorker.Run(f.ctx, f.ref, resumedFence); err == nil {
		t.Fatal("post-rearm merge observation crash was not injected")
	} else if err.Error() != "fixture: crash before merge_observed" {
		t.Fatalf("post-rearm merge recovery failed before observation injection: %v", err)
	}
	promoted, err := restarted.Effect(f.ctx, mergeKey)
	if err != nil || promoted.State != store.EffectConfirmed || promoted.TicketVersion != resumed.Version || promoted.LeaderEpoch != secondLeader || promoted.RunnerEpoch != resumed.RunnerEpoch || promoted.ClaimEpoch != confirmedBeforePause.ClaimEpoch+1 {
		t.Fatalf("promoted post-rearm merge effect=%+v prior=%+v err=%v", promoted, confirmedBeforePause, err)
	}
	promotedIntent, found, err := restarted.MergeIntent(f.ctx, mergeKey)
	if err != nil || !found {
		t.Fatalf("promoted merge intent found=%v err=%v", found, err)
	}
	_, mergeOID, found := strings.Cut(promoted.ObservedIdentity, "@")
	if !found || mergeOID == "" {
		t.Fatalf("promoted merge observation identity=%q", promoted.ObservedIdentity)
	}
	if _, err := restarted.MergeIntentForProof(f.ctx, promotedIntent.RepositoryHost, promotedIntent.RepositoryOwner, promotedIntent.RepositoryName, promotedIntent.BaseRef, promotedIntent.OriginalBaseOID, mergeOID); err != nil {
		t.Fatalf("promoted merge proof authority: %v", err)
	}
	if got := f.github.MutationCount("pr_merge"); got != 1 || proofFetches != 1 {
		t.Fatalf("post-rearm promotion replayed external mutation merge_calls=%d proof_fetches=%d", got, proofFetches)
	}

	restartedWorker.Engine = &failReconcileOnceEngine{StateMachine: engine.New(restarted, spec)}
	_, reconcileCrashErr := restartedWorker.Run(f.ctx, f.ref, resumedFence)
	if reconcileCrashErr == nil {
		t.Fatal("post-merge reconcile crash was not injected")
	}
	stranded, err = restarted.Ticket(f.ctx, f.ref)
	if err != nil || stranded.State != domain.StateReconciling {
		t.Fatalf("post-merge stranded ticket=%+v load_err=%v run_err=%v", stranded, err, reconcileCrashErr)
	}
	if got := f.github.MutationCount("pr_merge"); got != 1 || proofFetches != 1 {
		t.Fatalf("post-merge crash replayed external mutation merge_calls=%d proof_fetches=%d", got, proofFetches)
	}

	for _, tc := range []struct {
		name    string
		trigger string
		control bool
	}{
		{name: "operator_resume", trigger: "operator_resume", control: true},
		{name: "operator_retry", trigger: "operator_retry"},
	} {
		t.Run("engine reconciling "+tc.name, func(t *testing.T) {
			current, err := restarted.Ticket(f.ctx, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			fence := domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: current.RunnerEpoch}
			var pausedResult store.TransitionResult
			var stopped store.Ticket
			if tc.control {
				controlProof, err := restarted.ControlProof(f.ctx, f.ref)
				if err != nil || !controlProof.Drained() {
					t.Fatalf("pre-reconcile control proof=%+v err=%v", controlProof, err)
				}
				stopping, err := restarted.TransitionAndInvalidateRunner(f.ctx, store.Transition{
					Ref: f.ref, ExpectedVersion: current.Version, From: domain.StateReconciling, To: domain.StateStopping,
					ResumeState: domain.StateReconciling, Trigger: "operator_pause_or_take",
					Fence: fence, EventPayload: `{"intent":"pause"}`,
				})
				if err != nil {
					t.Fatalf("pause reconciling: %v", err)
				}
				pausedResult, err = restarted.CompleteControlTransition(f.ctx, store.Transition{
					Ref: f.ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused,
					ResumeState: domain.StateReconciling, Trigger: "process_and_effects_drained",
					Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: current.RunnerEpoch + 1}, EventPayload: `{}`,
				})
				if err != nil {
					t.Fatalf("complete reconciling pause: %v", err)
				}
				stopped = controlProof.Ticket
			} else {
				pausedResult, err = restarted.Transition(f.ctx, store.Transition{
					Ref: f.ref, ExpectedVersion: current.Version, From: domain.StateReconciling, To: domain.StatePaused,
					ResumeState: domain.StateReconciling, Trigger: "retry_or_correction_exhausted",
					Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
				})
				if err != nil {
					t.Fatalf("pause reconciling for retry: %v", err)
				}
			}
			paused, err := restarted.Ticket(f.ctx, f.ref)
			if err != nil || paused.State != domain.StatePaused || paused.Version != pausedResult.Version || paused.ResumeState != domain.StateReconciling {
				t.Fatalf("paused reconciling ticket=%+v result=%+v err=%v", paused, pausedResult, err)
			}

			attributes := map[string]string{
				"operator_identity_authenticated": "true",
			}
			eventPayload := `{}`
			if tc.control {
				attributes["takeover_diff_none"] = "true"
				attributes["branch_remote_identity_exact"] = "true"
				attributes["prerequisites_green"] = "true"
			} else {
				attributes["pause_reason_retryable"] = "true"
				attributes["typed_prerequisites_satisfied"] = "true"
				attributes["runner_epoch_current"] = "true"
				eventPayload = `{"intent":"retry"}`
			}
			result, err := engine.New(restarted, spec).Signal(f.ctx, contracts.SignalRequest{
				Ticket: f.ref, TicketVersion: paused.Version, From: domain.StatePaused, Trigger: tc.trigger,
				Fence:      domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: paused.RunnerEpoch},
				Attributes: attributes, EventPayload: eventPayload,
			})
			if err != nil {
				t.Fatalf("reconcile %s: %v", tc.trigger, err)
			}
			resumed, err := restarted.Ticket(f.ctx, f.ref)
			if err != nil || result.To != domain.StateReconciling || result.TicketVersion != paused.Version+1 || result.EventID == "" || resumed.State != domain.StateReconciling || resumed.Version != result.TicketVersion || resumed.ResumeState != "" || resumed.RunnerEpoch != paused.RunnerEpoch {
				t.Fatalf("reconcile %s result=%+v ticket=%+v err=%v", tc.trigger, result, resumed, err)
			}
			if !tc.control {
				if needed, err := restarted.RuntimeRearmNeeded(f.ctx, f.ref); err != nil || !needed {
					t.Fatalf("reconcile retry rearm needed=%v err=%v", needed, err)
				}
				stopped, err = restarted.StoppedRuntimeTicket(f.ctx, f.ref)
				if err != nil {
					t.Fatalf("load reconciling retry stop: %v", err)
				}
			}
			capability, err := restarted.PostPublicationRearmProof(f.ctx, f.ref, stopped)
			if err != nil {
				t.Fatalf("prove reconciling %s rearm: %v", tc.trigger, err)
			}
			var admission *store.RuntimeAdmissionCapability
			if err := restarted.ActivateRearm(f.ctx, capability, func(value *store.RuntimeAdmissionCapability) error {
				if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
					return errors.New("fixture: reconciling runtime admission was not consumable")
				}
				admission = value
				return nil
			}); err != nil || admission == nil {
				t.Fatalf("activate reconciling %s admission=%v err=%v", tc.trigger, admission, err)
			}
			if err := admission.OpenStoreAdmission(f.ctx); err != nil {
				t.Fatalf("open reconciling %s rearm: %v", tc.trigger, err)
			}
		})
	}

	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err = store.Open(f.ctx, filepath.Join(filepath.Dir(f.bare), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	postMergeLeader, err := restarted.AcquireLeader(f.ctx, f.ref.Channel, "guarded-merge-post-observation-restart")
	if err != nil {
		t.Fatal(err)
	}
	if effects, err := restarted.ReconcileEffects(f.ctx, f.ref.Channel, postMergeLeader); err != nil || len(effects) != 0 {
		t.Fatalf("post-merge effect reconciliation=%+v err=%v", effects, err)
	}
	if changed, err := restarted.FenceRecoveredRunners(f.ctx, f.ref.Channel, postMergeLeader); err != nil || changed != 1 {
		t.Fatalf("post-merge restart fence changed=%d err=%v", changed, err)
	}
	recovered, err = restarted.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	recoveredStopped, err := restarted.StoppedRuntimeTicket(f.ctx, f.ref)
	if err != nil {
		t.Fatalf("load post-merge recovered stop: %v", err)
	}
	recoveredCapability, err := restarted.PostPublicationRearmProof(f.ctx, f.ref, recoveredStopped)
	if err != nil {
		t.Fatalf("prove post-merge recovered rearm: %v", err)
	}
	var recoveredAdmission *store.RuntimeAdmissionCapability
	if err := restarted.ActivateRearm(f.ctx, recoveredCapability, func(value *store.RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("fixture: recovered runtime admission was not consumable")
		}
		recoveredAdmission = value
		return nil
	}); err != nil || recoveredAdmission == nil {
		t.Fatalf("activate post-merge recovered admission=%v err=%v", recoveredAdmission, err)
	}
	if err := recoveredAdmission.OpenStoreAdmission(f.ctx); err != nil {
		t.Fatalf("open post-merge recovered admission: %v", err)
	}
	f.runner.MutationAuthority = restarted
	restartedClient = newStoreBackedFakeGHClient(t, f, mergeproof.Coordinator{Store: restarted, Git: f.runner}, advanceFixtureMain)
	restartedWorker = localruntime.NewWorker(restarted, workflowworker.Worker{}, publication.Worker{Store: restarted, Git: f.runner, GitHub: restartedClient})
	restartedWorker.Engine = engine.New(restarted, spec)
	result, err := restartedWorker.Run(f.ctx, f.ref, domain.Fence{LeaderEpoch: postMergeLeader, RunnerEpoch: recovered.RunnerEpoch})
	if err != nil || !result.Transitioned || result.State != domain.StateDone {
		t.Fatalf("post-merge restart reconciliation result=%+v err=%v", result, err)
	}
	if got := f.github.MutationCount("pr_merge"); got != 1 || proofFetches != 1 {
		t.Fatalf("post-merge restart replayed external mutation merge_calls=%d proof_fetches=%d", got, proofFetches)
	}
	confirmed, err := restarted.Effect(f.ctx, mergeKey)
	if err != nil || confirmed.State != store.EffectConfirmed {
		t.Fatalf("recovered merge effect=%+v err=%v", confirmed, err)
	}
}

type failReconcileOnceEngine struct {
	workflowworker.StateMachine
	failed bool
}

type failMergeObservedOnceEngine struct {
	workflowworker.StateMachine
	failed bool
}

func (e *failMergeObservedOnceEngine) Signal(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From == domain.StateMerging && request.Trigger == "merge_observed" && !e.failed {
		e.failed = true
		return contracts.TransitionResult{}, errors.New("fixture: crash before merge_observed")
	}
	return e.StateMachine.Signal(ctx, request)
}

func (e *failReconcileOnceEngine) Signal(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From == domain.StateReconciling && !e.failed {
		e.failed = true
		return contracts.TransitionResult{}, errors.New("fixture: crash before reconcile_pass")
	}
	return e.StateMachine.Signal(ctx, request)
}

func TestManualMergeObservationRecoversCrashBeforeReconcilePass(t *testing.T) {
	f := newPublicationFixtureMode(t, domain.MergeManual)
	defer f.close()
	if _, err := (publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}).Run(f.ctx, f.ref, f.fence); err != nil {
		t.Fatalf("publish candidate: %v", err)
	}
	published, err := f.db.LoadPublishedCandidate(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.github.SetBaseHeadOIDForTest(published.PullRequest.BaseOID); err != nil {
		t.Fatal(err)
	}
	if err := f.github.SetChecks(published.PullRequest.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "fixture-unit", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	baseEngine := engine.New(f.db, spec)
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}
	if result, err := (localruntime.CIWorker{Store: f.db, Observer: f.github}).Run(f.ctx, f.ref, fence); err != nil || result.State != domain.StateReviewing {
		t.Fatalf("CI result=%+v err=%v", result, err)
	}
	ticket, err = f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	completePublishedFinalReview(t, f, ticket, fence)
	if _, err := f.db.TransitionFinalReview(f.ctx, store.Transition{Ref: f.ref, ExpectedVersion: ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingManualMerge, Trigger: "review_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	if err := f.github.SetPullRequestMergedForTest(published.PullRequest.Number, strings.Repeat("d", 40)); err != nil {
		t.Fatal(err)
	}
	waiting, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fault := &failReconcileOnceEngine{StateMachine: baseEngine}
	worker := localruntime.NewWorker(f.db, workflowworker.Worker{}, publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github})
	worker.Engine = fault
	_, firstErr := worker.Run(f.ctx, f.ref, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch})
	if firstErr == nil {
		t.Fatal("manual observation unexpectedly crossed injected reconcile crash")
	}
	stranded, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil || stranded.State != domain.StateReconciling {
		t.Fatalf("stranded ticket=%+v err=%v first_run_err=%v", stranded, err, firstErr)
	}
	if _, err := f.db.ManualMergeObservation(f.ctx, f.ref); err != nil {
		t.Fatalf("manual observation was not durable: %v", err)
	}
	if got := f.github.MutationCount("pr_merge"); got != 0 {
		t.Fatalf("manual observation invoked factory merge %d times", got)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(f.ctx, filepath.Join(filepath.Dir(f.bare), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	leader, err := restarted.AcquireLeader(f.ctx, f.ref.Channel, "manual-reconcile-restart")
	if err != nil {
		t.Fatal(err)
	}
	if effects, err := restarted.ReconcileEffects(f.ctx, f.ref.Channel, leader); err != nil || len(effects) != 0 {
		t.Fatalf("manual restart effects=%+v err=%v", effects, err)
	}
	if changed, err := restarted.FenceRecoveredRunners(f.ctx, f.ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("manual restart fence changed=%d err=%v", changed, err)
	}
	recovered, err := restarted.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	restartedWorker := localruntime.NewWorker(restarted, workflowworker.Worker{}, publication.Worker{Store: restarted, Git: f.runner, GitHub: f.github})
	restartedWorker.Engine = engine.New(restarted, spec)
	result, err := restartedWorker.Run(f.ctx, f.ref, domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch})
	if err != nil || !result.Transitioned || result.State != domain.StateDone {
		t.Fatalf("manual recovery result=%+v err=%v", result, err)
	}
	if got := f.github.MutationCount("pr_merge"); got != 0 {
		t.Fatalf("manual recovery invoked factory merge %d times", got)
	}
}

type fakeGHSupervisor struct {
	github     *testkit.FakeGH
	afterMerge func() error
}

func (r fakeGHSupervisor) Run(_ context.Context, _ string, args, _ []string) ([]byte, error) {
	output, err := r.github.Run(args)
	if len(args) >= 2 && args[0] == "pr" && args[1] == "merge" && r.afterMerge != nil {
		if advanceErr := r.afterMerge(); advanceErr != nil {
			return output, advanceErr
		}
	}
	return output, err
}

func (fakeGHSupervisor) Cleanup(context.Context) (githubboundary.CleanupProof, error) {
	return githubboundary.CleanupProof{Drained: true}, nil
}

type lostProofResponseVerifier struct {
	mergeproof.Coordinator
	loseNext bool
}

func (v *lostProofResponseVerifier) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
	proof, err := v.Coordinator.VerifyProtectedBranch(ctx, repository, baseRef, mergeCommit, originalBaseOID)
	if err == nil && v.loseNext {
		v.loseNext = false
		return contracts.ProtectedBranchObservation{}, errors.New("fixture: protected proof response lost")
	}
	return proof, err
}

func newStoreBackedFakeGHClient(t *testing.T, f *publicationFixture, verifier contracts.MergeBranchVerifier, afterMerge func() error) *githubboundary.Client {
	t.Helper()
	client, err := githubboundary.NewStoreClient("/usr/bin/fake-gh", f.runner.Home, f.runner.Home, fakeGHSupervisor{github: f.github, afterMerge: afterMerge}, f.db, verifier)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func completePublishedFinalReview(t *testing.T, f *publicationFixture, ticket store.Ticket, fence domain.Fence) {
	t.Helper()
	pair, err := f.db.ProviderPair(f.ctx, f.ref.Channel)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	request := store.ProviderAttemptRequest{Ref: f.ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseReview, Role: "reviewer", Binding: runtimeBinding(pair.Reviewer), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(), ExpectedHead: candidate.Snapshot.HeadSHA, ExpectedProof: candidate.Snapshot.ProofDigest, Repository: mustProjectPath(t, f.db, f.ref), Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, SupervisorKey: signer.PublicKey()}
	request.Input = contracts.PhaseInput{Ticket: request.Ref, Phase: request.Phase, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Prompt: "final review", Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, AllowedPaths: []string{"."}, Provider: request.Binding.Identity, AuthMode: request.Binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	claim, err := f.db.BeginProviderAttempt(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.RecordProviderLaunch(f.ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "guarded-merge", ProcessStartIdentity: fmt.Sprintf("guarded-merge-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	drain := contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest}
	proof, err := signer.ProveDrained(drain)
	if err != nil {
		t.Fatal(err)
	}
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(fmt.Sprintf(`{"schema":"sf.reviewer/v1","decision":"pass","repair_owner":"","findings":[],"reviewed_head":"%s","proof_digest":"%s"}`, candidate.Snapshot.HeadSHA, candidate.Snapshot.ProofDigest)), UsageTrusted: true, UsageUnits: 1}
	if _, err := f.db.CompleteProviderAttemptSuccess(f.ctx, claim, proof, ticket.Version, fence, raw, phaseartifact.Validation{TicketType: ticket.Type, ExpectedReviewedHead: candidate.Snapshot.HeadSHA, ExpectedProofDigest: candidate.Snapshot.ProofDigest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func guardedFixtureRuleset() testkit.FakeRuleset {
	return testkit.FakeRuleset{
		ID: 42, Target: "branch", Source: "acme/app", SourceType: "Repository", Enforcement: "active",
		Conditions: &testkit.FakeRulesetConditions{RefName: &testkit.FakeRulesetRefCondition{Include: []string{"refs/heads/main"}, Exclude: []string{}}},
		Rules: []testkit.FakeRulesetRule{
			{Type: "pull_request", Parameters: map[string]any{"allowed_merge_methods": []any{"squash"}}},
			{Type: "required_status_checks", Parameters: map[string]any{"strict_required_status_checks_policy": true, "required_status_checks": []any{map[string]any{"context": "unit"}}}},
			{Type: "non_fast_forward"},
			{Type: "deletion"},
		},
		BypassActors: []any{},
	}
}

func TestRecoveredPublishingRuntimePublishesAfterPrePublicationCrash(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()

	// Simulate the process dying immediately after the durable
	// building->publishing transition, before either public effect or witness
	// exists. Reopen the backed-up Store through daemon.Start so AcquireLeader
	// and the complete daemon.Recover path run before the runtime tick.
	home, err := os.MkdirTemp("/tmp", "sf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Backup(f.ctx, paths.Database); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err := daemon.Start(f.ctx, daemon.Config{Channel: domain.ChannelDev, Paths: paths, DaemonIdentity: "publication-crash-restart"})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	restarted, err := store.Open(f.ctx, paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	f.db = restarted
	f.runner.MutationAuthority = restarted
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
	if err != nil {
		t.Fatalf("recoverable candidate: %v", err)
	}
	if err := f.db.AuthenticatePublishingRecovery(f.ctx, f.ref, candidate, ticket.Version, domain.Fence{LeaderEpoch: owner.Epoch(), RunnerEpoch: ticket.RunnerEpoch}); err != nil {
		t.Fatalf("publishing recovery chain: %v candidate=%+v ticket=%+v", err, candidate, ticket)
	}
	ledgerDB, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer ledgerDB.Close()
	var priorVersion, priorRunner, priorLeader, recoveredVersion, recoveredRunner, recoveredLeader uint64
	var recoveryDigest, createdAt string
	if err := ledgerDB.QueryRowContext(f.ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, f.ref.Channel, f.ref.Project, f.ref.Ticket).Scan(&priorVersion, &priorRunner, &priorLeader, &recoveredVersion, &recoveredRunner, &recoveredLeader, &recoveryDigest, &createdAt); err != nil {
		t.Fatalf("recovery ledger: %v", err)
	}
	if priorVersion != candidate.TicketVersion+1 || priorRunner != candidate.Fence.RunnerEpoch || priorLeader != candidate.Fence.LeaderEpoch || recoveredVersion != ticket.Version || recoveredRunner != ticket.RunnerEpoch || recoveredLeader != owner.Epoch() || recoveryDigest == "" || createdAt == "" {
		t.Fatalf("recovery ledger tuple=(%d,%d,%d)->(%d,%d,%d) digest=%q created=%q candidate=%+v ticket=%+v leader=%d", priorVersion, priorRunner, priorLeader, recoveredVersion, recoveredRunner, recoveredLeader, recoveryDigest, createdAt, candidate, ticket, owner.Epoch())
	}
	runtimeWorker := localruntime.Worker{
		Store:              f.db,
		Publication:        publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github},
		PublicationEnabled: true,
	}
	captured := &capturingRuntimeWorker{worker: runtimeWorker}
	scheduler := workflowruntime.NewScheduler(
		f.ref.Channel,
		workflowruntime.StoreTicketSource{Store: f.db},
		staticWorktreeEnsurer{worktree: worktree},
		captured,
	)
	scheduler.AdmitPublishing = true
	tick := scheduler.Tick(f.ctx, domain.Fence{LeaderEpoch: owner.Epoch(), RunnerEpoch: ticket.RunnerEpoch})
	if tick.Outcome != workflowruntime.OutcomeInvoked || tick.Worker.State != domain.StateWaitingCI || !tick.Worker.Transitioned {
		t.Fatalf("recovered publication outcome=%s worker=%+v worker_err=%v tick_err=%v", tick.Outcome, tick.Worker, captured.err, tick.Err)
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 1 {
		t.Fatalf("recovered publication mutations push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

type capturingRuntimeWorker struct {
	worker localruntime.Worker
	err    error
}

func (w *capturingRuntimeWorker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	result, err := w.worker.Run(ctx, ref, fence)
	w.err = err
	return result, err
}

type staticWorktreeEnsurer struct {
	worktree store.StoredWorktree
}

func (e staticWorktreeEnsurer) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	return e.worktree, nil
}

func TestWorkerKeepsUnprovenPushUncertainAfterLostCommandResult(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	f.failPushBefore = 1
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, gitboundary.ErrPushUncertain) {
		t.Fatalf("first push failure=%v", err)
	}
	if got := f.github.MutationCount("pr_create"); got != 0 {
		t.Fatalf("PR created after pre-push crash: %d", got)
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err == nil {
		t.Fatal("uncertain push was blindly replayed")
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 0 {
		t.Fatalf("uncertain recovery mutations push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

func TestWorkerRetriesProvenBeforeStartPushExactlyOnce(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	// provePushHead runs before PushWithRequest can hand the push command to
	// the runner. The injected rev-parse failure is therefore a real adapter
	// pre-start result, not a simulated Store transition.
	f.failPushProofBefore = 1
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, gitboundary.ErrPushBeforeStart) {
		t.Fatalf("first pre-start push=%v", err)
	}
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	project, err := f.db.Project(f.ctx, f.ref.Channel, f.ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	request := "sha256:" + digestHex([]byte("sf.publication.push.v2\x00"+string(f.ref.Channel)+"\x00"+string(f.ref.Project)+"\x00"+string(f.ref.Ticket)+"\x00"+project.Path+"\x00"+worktree.Path+"\x00"+worktree.Branch+"\x00"+f.candidate.Snapshot.BaseSHA+"\x00"+f.candidate.Snapshot.HeadSHA+"\x00"))
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: f.ref, TicketVersion: ticket.Version, Fence: f.fence}, RequestDigest: request, Repository: project.Path, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "push", BaseRef: project.BaseRef, ExpectedBaseOID: f.candidate.Snapshot.BaseSHA, ExpectedHeadOID: f.candidate.Snapshot.HeadSHA}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	effect, err := f.db.Effect(f.ctx, intent.SemanticKey)
	if err != nil || effect.State != store.EffectFailed {
		t.Fatalf("pre-start push effect=%+v err=%v", effect, err)
	}
	if f.gitPushCount != 0 || f.github.MutationCount("pr_create") != 0 {
		t.Fatalf("pre-start unexpectedly mutated push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err != nil {
		t.Fatalf("safe push retry=%v", err)
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 1 {
		t.Fatalf("safe retry mutations push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

func TestWorkerReplansProvenAbsentDraftAfterCrash(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	if err := f.github.SetResponse("pr_create", testkit.ResponseErrorBefore); err != nil {
		t.Fatal(err)
	}
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err == nil {
		t.Fatal("first PR failure unexpectedly succeeded")
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 0 {
		t.Fatalf("first crash mutations push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
	// This is a real Worker path, not a manually altered effect: FakeGH proved
	// the create callback failed before launch, so the Worker must durably fail
	// the claimed row and leave precisely one safe retry available.
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, HeadOwner: "acme", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: f.candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: f.candidate.Snapshot.BaseSHA, FactoryOwned: true}
	title := "sf: " + string(ticket.Ref.Ticket)
	body := "<!-- sf:publication/v1 -->\n\nticket: " + string(ticket.Ref.Ticket) + "\ncandidate: " + f.candidate.Snapshot.HeadSHA + "\nsource: " + ticket.SourceDigest
	key := "github/draft-pr/v1/" + githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body)
	effect, err := f.db.Effect(f.ctx, key)
	if err != nil || effect.State != store.EffectFailed {
		t.Fatalf("pre-start draft effect=%+v err=%v", effect, err)
	}
	if err := f.github.SetResponse("pr_create"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err != nil {
		t.Fatal(err)
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 1 {
		t.Fatalf("recovery mutations push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

func TestWorkerFailsFinalCreateIdentityConflictBeforeStart(t *testing.T) {
	for _, test := range []struct {
		name  string
		owned bool
	}{
		{name: "human-owned", owned: false},
		{name: "factory-owned", owned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newPublicationFixture(t)
			defer f.close()
			github := &createIdentityConflictAtHandoff{FakeGH: f.github, owned: test.owned}
			w := publication.Worker{Store: f.db, Git: f.runner, GitHub: github}
			if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, contracts.ErrDraftCreateBeforeStart) {
				t.Fatalf("final create conflict err=%v", err)
			}
			if got := f.github.MutationCount("pr_create"); got != 0 {
				t.Fatalf("final create conflict launched create %d times", got)
			}

			ticket, err := f.db.Ticket(f.ctx, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			worktree, err := f.db.Worktree(f.ctx, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, HeadOwner: "acme", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: f.candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: f.candidate.Snapshot.BaseSHA, FactoryOwned: true}
			title := "sf: " + string(ticket.Ref.Ticket)
			body := "<!-- sf:publication/v1 -->\n\nticket: " + string(ticket.Ref.Ticket) + "\ncandidate: " + f.candidate.Snapshot.HeadSHA + "\nsource: " + ticket.SourceDigest
			key := "github/draft-pr/v1/" + githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body)
			effect, err := f.db.Effect(f.ctx, key)
			if err != nil || effect.State != store.EffectFailed {
				t.Fatalf("final create conflict effect=%+v err=%v", effect, err)
			}
		})
	}
}

func TestWorkerReconcilesLostCreateWithoutBlindReplay(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	if err := f.github.SetResponse("pr_create", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, githubboundary.ErrCreateUncertain) {
		t.Fatalf("lost create err=%v", err)
	}
	if got := f.github.MutationCount("pr_create"); got != 1 {
		t.Fatalf("lost create mutations=%d", got)
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err != nil {
		t.Fatalf("exact-present create reconciliation=%v", err)
	}
	if got := f.github.MutationCount("pr_create"); got != 1 {
		t.Fatalf("exact-present reconciliation replayed create %d times", got)
	}
}

func TestWorkerRefusesRetryWhileQuarantinedPushLeaseRemains(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	f.failPushBefore = 1
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err == nil {
		t.Fatal("first push failure unexpectedly succeeded")
	}
	facts, err := f.db.PublicationPushIntent(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ObserveEffect(f.ctx, store.EffectObservation{EffectFence: store.EffectFence{SemanticKey: facts.Effect.SemanticKey, Ref: f.ref, TicketVersion: facts.Effect.TicketVersion, Fence: domain.Fence{LeaderEpoch: facts.Effect.LeaderEpoch, RunnerEpoch: facts.Effect.RunnerEpoch, ClaimEpoch: facts.Effect.ClaimEpoch}}, Present: false}); err != nil {
		t.Fatal(err)
	}
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{SemanticKey: facts.Claim.SemanticKey, Ref: facts.Claim.TicketRef, TicketVersion: facts.Claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: facts.Claim.LeaderEpoch, RunnerEpoch: facts.Claim.RunnerEpoch}}, RequestDigest: facts.Claim.RequestDigest, Repository: facts.Claim.Repository, Worktree: facts.Claim.Worktree, Branch: facts.Claim.Branch, Operation: facts.Claim.Operation, BaseRef: facts.Claim.BaseRef, ExpectedBaseOID: facts.Claim.ExpectedBaseOID, ExpectedHeadOID: facts.Claim.ExpectedHeadOID}
	if _, err := f.db.PlanEffect(f.ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: f.ref, Kind: "git/push", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.db.IssueGitMutationClaim(f.ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.AcquireGitMutation(f.ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := f.db.RecoverGitMutationLeases(f.ctx, f.ref.Channel, f.fence.LeaderEpoch+1, nil); !errors.Is(err, store.ErrGitMutationLease) {
		t.Fatalf("quarantine result=%v", err)
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); err == nil {
		t.Fatal("retry succeeded with quarantined lease")
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 0 {
		t.Fatalf("quarantined retry mutated push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

func TestWorkerRejectsRemoteDriftAfterPublishedWitnessRecord(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	wt, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	alt := makeTreeCommit(t, wt.Path, f.candidate.Snapshot.TreeSHA, f.candidate.Snapshot.HeadSHA)
	runGit(t, wt.Path, "push", f.bare, alt+":refs/heads/fixture-alt")
	f.driftRemoteOID = alt
	// Branch observations occur once before the push, once after it, and once
	// for each immediate pre-transition validation. Move the bare ref between
	// those last two observations so the durable witness cannot advance state.
	f.driftRemoteAt = 4
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, publication.ErrRemoteCandidate) {
		t.Fatalf("drifted publication err=%v", err)
	}
	if _, err := f.db.PublishedCandidate(f.ctx, f.ref); err != nil {
		t.Fatalf("witness was not durable before drift rejection: %v", err)
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, publication.ErrPublicationDrift) {
		t.Fatalf("replay drift err=%v", err)
	}
	if f.gitPushCount != 1 || f.github.MutationCount("pr_create") != 1 {
		t.Fatalf("drift replay mutated push=%d pr=%d", f.gitPushCount, f.github.MutationCount("pr_create"))
	}
}

func TestWorkerRefusesExactPRTextDriftBeforeTransitionAndOnReplay(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	github := &driftAfterOutputWitness{FakeGH: f.github}
	w := publication.Worker{Store: f.db, Git: f.runner, GitHub: github}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, publication.ErrPullRequest) {
		t.Fatalf("post-confirm text drift err=%v", err)
	}
	if _, err := f.db.PublishedCandidate(f.ctx, f.ref); err != nil {
		t.Fatalf("witness was not durable before text-drift rejection: %v", err)
	}
	if _, err := w.Run(f.ctx, f.ref, f.fence); !errors.Is(err, publication.ErrPublicationDrift) {
		t.Fatalf("text-drift replay err=%v", err)
	}
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil || ticket.State != domain.StatePublishing {
		t.Fatalf("ticket after text drift=%+v err=%v", ticket, err)
	}
}

// driftAfterOutputWitness changes title after the first exact-output witness.
// That places the edit strictly between the RecordPublishedCandidate validation
// and the immediate pre-transition validation.
type driftAfterOutputWitness struct {
	*testkit.FakeGH
	calls int
}

func (g *driftAfterOutputWitness) ObserveFactoryPullRequestOutput(ctx context.Context, want contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, string, bool, bool, error) {
	identity, state, draft, applied, err := g.FakeGH.ObserveFactoryPullRequestOutput(ctx, want, title, body)
	g.calls++
	if err == nil && applied && g.calls == 1 {
		for _, pr := range g.FakeGH.Snapshot().PRs {
			if pr.Identity.Number == identity.Number {
				if err := g.FakeGH.SetPullRequestTextForTest(identity.Number, "foreign title", pr.Body); err != nil {
					return contracts.PullRequestIdentity{}, "", false, false, err
				}
				break
			}
		}
	}
	return identity, state, draft, applied, err
}

// createIdentityConflictAtHandoff adds a conflicting PR after the Worker's
// absence observation and immediately before FakeGH's final create boundary.
type createIdentityConflictAtHandoff struct {
	*testkit.FakeGH
	owned    bool
	injected bool
}

func (g *createIdentityConflictAtHandoff) CreateDraftPullRequest(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, error) {
	if !g.injected {
		conflict := identity
		conflict.Number, conflict.FactoryOwned = 7, g.owned
		if err := g.InjectPullRequestForTest(testkit.PullRequest{Identity: conflict, Draft: true}); err != nil {
			return contracts.PullRequestIdentity{}, err
		}
		g.injected = true
	}
	return g.FakeGH.CreateDraftPullRequest(ctx, claim, identity, title, body)
}

type publicationFixture struct {
	ctx                 context.Context
	db                  *store.Store
	ref                 domain.TicketRef
	fence               domain.Fence
	runner              gitboundary.Runner
	github              *testkit.FakeGH
	candidate           store.StoredCandidate
	gitPushCount        int
	failPushBefore      int
	failPushProofBefore int
	driftRemoteAt       int
	driftRemoteOID      string
	remoteObsCount      int
	drifted             bool
	bare                string
}

func newPublicationFixture(t *testing.T) *publicationFixture {
	return newPublicationFixtureMode(t, domain.MergeGuarded)
}

func newPublicationFixtureMode(t *testing.T, mergeMode domain.MergeMode) *publicationFixture {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	repository := filepath.Join(root, "repo")
	bare := filepath.Join(root, "origin.git")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "init", "-b", "main")
	runGit(t, repository, "config", "user.name", "fixture")
	runGit(t, repository, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "README.md")
	runGit(t, repository, "commit", "-m", "base")
	base := gitOutput(t, repository, "rev-parse", "HEAD")
	runGit(t, root, "init", "--bare", bare)
	runGit(t, repository, "remote", "add", "origin", "https://example.test/nysa/app.git")
	runGit(t, repository, "remote", "set-url", "--push", "origin", "https://example.test/nysa/app.git")
	// Setup uses the local path before the authenticated fixture transport is
	// installed. The Runner later rewrites only this exact URL to the bare repo.
	runGit(t, repository, "remote", "set-url", "origin", bare)
	runGit(t, repository, "remote", "set-url", "--push", "origin", bare)
	runGit(t, repository, "push", "origin", "main")
	runGit(t, repository, "remote", "set-url", "origin", "https://github.com/acme/app.git")
	runGit(t, repository, "remote", "set-url", "--push", "origin", "https://github.com/acme/app.git")
	canonicalRepository, err := filepath.EvalSymlinks(repository)
	if err != nil {
		t.Fatal(err)
	}
	repository = canonicalRepository

	db, err := store.Open(ctx, filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("app", repository), config.TicketOverride{})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "app", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	github, err := testkit.NewFakeGH(filepath.Join(root, "github.json"), contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := github.SetAuthenticated(true); err != nil {
		db.Close()
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-publication-e2e"}
	source := []byte("publication source")
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: digestHex(source), Source: source, Type: domain.TicketFeature, MergeMode: mergeMode, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "publication-e2e")
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, 1, "e2e", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	allocator := gitboundary.Allocator{Authority: db}
	branch, err := allocator.AllocateUnderFence(ctx, ref.Channel, ref.Project, ref.Ticket, started.Version, fence)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	worktree, err := db.TicketWorktreePath(ref)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(worktree), 0o700); err != nil {
		db.Close()
		t.Fatal(err)
	}
	runGit(t, repository, "worktree", "add", "-b", branch, worktree, "main")
	runner := newFixtureRunner(t, bare)
	runner.CredentialHelper = filepath.Join(runner.Home, "sf-git-credential")
	runner.GHBinary = filepath.Join(runner.Home, "gh")
	ghBytes := []byte("fixture gh")
	if err := os.WriteFile(runner.GHBinary, ghBytes, 0o700); err != nil {
		db.Close()
		t.Fatal(err)
	}
	ghDigest := sha256.Sum256(ghBytes)
	runner.GHBinaryDigest = "sha256:" + hex.EncodeToString(ghDigest[:])
	runner.GHConfigDir = filepath.Join(runner.Home, "gh-config")
	runner.MutationAuthority = db
	identity, err := runner.Snapshot(ctx, worktree, "main")
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: branch, IdentityJSON: identityJSON, BaseSHA: base, HeadSHA: base}); err != nil {
		db.Close()
		t.Fatal(err)
	}

	bq, _, err := db.RecordProviderQualification(ctx, qualification("11111111111111111111111111111111", "cursor", "cursor-family"))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	rq, _, err := db.RecordProviderQualification(ctx, qualification("22222222222222222222222222222222", "claude", "claude-family"))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, _, err := db.SelectProviderPair(ctx, domain.ChannelDev, bq.ID, rq.ID, time.Now().UTC()); err != nil {
		db.Close()
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	bindingBuilder := runtimeBinding(bq)
	bindingReviewer := runtimeBinding(rq)

	// Planner result and transition.
	planRaw := []byte(`{"schema":"sf.planner/v1","acceptance":["published branch exists"],"proof":{"kind":"acceptance","command":["go","test","./..."],"details":"fixture"},"paths":["README.md"],"commands":[["go","test","./..."]],"risks":["remote"]}`)
	planner := completeProvider(t, db, signer, started, fence, identityJSON, configDigest, domain.PhasePlanning, "planner", bindingBuilder, planRaw, phaseartifact.Validation{TicketType: refType()}, ref, worktree, base)
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, providerKey(ref, domain.PhasePlanning, planner))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	plan := parsed.Planner
	planKey := providerKey(ref, domain.PhasePlanning, planner)
	if _, err := db.RecordPlan(ctx, store.PlanArtifact{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Document: store.PlanDocument{Planner: plan, ProviderResult: &planKey, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	trans, err := db.TransitionPlan(ctx, store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	verifying := started
	verifying.Version = trans.Version

	planID, err := workflowprompt.NewPlanIdentity(*plan)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	verify := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planID.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"README.md"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: digestHex([]byte("verification"))}
	verifyRaw, _ := json.Marshal(verify)
	verifyClaim := completeProvider(t, db, signer, verifying, fence, identityJSON, configDigest, domain.PhaseVerification, "reviewer", bindingReviewer, verifyRaw, phaseartifact.Validation{TicketType: refType(), AcceptanceDigest: planID.Digest}, ref, worktree, base)
	verifyKey := providerKey(ref, domain.PhaseVerification, verifyClaim)
	intent, _ := workflowprompt.CanonicalVerificationIntentBytes(verify)
	proof, _ := workflowprompt.CanonicalVerificationProofBytes(verify)
	checkpoint := makeCommit(t, worktree, "checkpoint", "checkpoint\n")
	checkpointTree := gitOutput(t, worktree, "rev-parse", "HEAD^{tree}")
	verificationCommand := completeCommand(t, db, ref, verifying.Version, fence, identityJSON, worktree, branch, base, verifyKey, digestHex(intent), digestHex(proof), "", "sha256:"+digestHex([]byte("policy")), 1)
	if _, err := db.RecordVerification(ctx, store.VerificationArtifact{Ref: ref, ExpectedVersion: verifying.Version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: verify.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &verifyKey, Checkpoint: store.CommitObservation{CommitOID: checkpoint, ParentOID: base, TreeOID: checkpointTree}, CommandResult: verificationCommand}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	trans, err = db.TransitionVerification(ctx, store.Transition{Ref: ref, ExpectedVersion: verifying.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	building := verifying
	building.Version = trans.Version

	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"publication candidate","changed_files":["README.md"],"commands":[["go","test","./..."]]}`)
	builderClaim := completeProvider(t, db, signer, building, fence, identityJSON, configDigest, domain.PhaseBuild, "builder", bindingBuilder, builderRaw, phaseartifact.Validation{TicketType: refType()}, ref, worktree, base)
	builderKey := providerKey(ref, domain.PhaseBuild, builderClaim)
	_, parsed, err = db.LoadHistoricalProviderAttemptResult(ctx, builderKey)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	head := makeCommit(t, worktree, "candidate", "candidate\n")
	tree := gitOutput(t, worktree, "rev-parse", "HEAD^{tree}")
	policy := digestHex([]byte("policy"))
	candidateCommand := completeCommand(t, db, ref, building.Version, fence, identityJSON, worktree, branch, base, builderKey, digestHex(intent), digestHex(proof), checkpoint, "sha256:"+policy, 0)
	candidate := domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: head, TreeSHA: tree, SourceDigest: digestHex(source), VerificationIntentDigest: digestHex(intent), ProofDigest: digestHex(proof), CommandPolicyDigest: policy, BuilderEvidenceDigest: builderDigest}
	if _, err := db.RecordCandidate(ctx, store.CandidateEvidence{Ref: ref, ExpectedVersion: building.Version, Fence: fence, Snapshot: candidate, BuilderResult: builderKey, Commit: store.CommitObservation{CommitOID: head, ParentOID: checkpoint, TreeOID: tree}, Reason: "publication", CommandResult: candidateCommand}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	storedCandidate, err := db.LatestCandidate(ctx, ref)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	trans, err = db.TransitionCandidate(ctx, store.Transition{Ref: ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, storedCandidate.Snapshot)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	publishing := fence
	if trans.Version == 0 {
		db.Close()
		t.Fatal("candidate transition returned no version")
	}
	ticket, err := db.Ticket(ctx, ref)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if ticket.State != domain.StatePublishing {
		db.Close()
		t.Fatalf("state=%s", ticket.State)
	}
	f := &publicationFixture{ctx: ctx, db: db, ref: ref, fence: publishing, runner: runner, github: github, candidate: storedCandidate, bare: bare}
	runner.Run = func(ctx context.Context, binary string, args []string, env []string) ([]byte, error) {
		isBranchObservation := false
		for _, arg := range args {
			isBranchObservation = isBranchObservation || arg == "ls-remote"
		}
		joined := strings.Join(args, "\x00")
		if f.failPushProofBefore > 0 && strings.Contains(joined, "rev-parse") {
			f.failPushProofBefore--
			return nil, errors.New("fixture candidate proof unavailable before push launch")
		}
		isBranchObservation = isBranchObservation && strings.Contains(joined, branch)
		if isBranchObservation {
			f.remoteObsCount++
			if !f.drifted && f.driftRemoteAt != 0 && f.remoteObsCount == f.driftRemoteAt {
				cmd := exec.Command("/usr/bin/git", "--git-dir", bare, "update-ref", "refs/heads/"+branch, f.driftRemoteOID)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("drift remote: %v\n%s", err, out)
				}
				f.drifted = true
			}
		}
		for i := range args {
			if args[i] == "https://github.com/acme/app.git" {
				args[i] = bare
			}
		}
		isPush := false
		for _, arg := range args {
			isPush = isPush || arg == "push"
		}
		if isPush && strings.Contains(strings.Join(args, "\x00"), "refs/heads/"+branch) {
			f.gitPushCount++
			if f.failPushBefore > 0 {
				f.failPushBefore--
				return nil, errors.New("fixture crash before push")
			}
		}
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return out, err
	}
	f.runner = runner
	return f
}

func (f *publicationFixture) close() {
	if f.db != nil {
		_ = f.db.Close()
	}
}

func newFixtureRunner(t *testing.T, bare string) gitboundary.Runner {
	t.Helper()
	home := filepath.Join(t.TempDir(), "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	r := gitboundary.Runner{Binary: "/usr/bin/git", Home: home, TestLocalTransport: true}
	r.Run = func(ctx context.Context, binary string, args []string, env []string) ([]byte, error) {
		for i := range args {
			if args[i] == "https://github.com/acme/app.git" {
				args[i] = bare
			}
		}
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = env
		return cmd.CombinedOutput()
	}
	return r
}

func completeProvider(t *testing.T, db *store.Store, signer *contracts.DrainSigner, ticket store.Ticket, fence domain.Fence, identity []byte, configDigest string, phase domain.Phase, role string, binding contracts.RuntimeBinding, raw []byte, validation phaseartifact.Validation, ref domain.TicketRef, worktree, base string) store.ProviderAttemptClaim {
	t.Helper()
	input := contracts.PhaseInput{Ticket: ref, Phase: phase, Prompt: "fixture", Repository: mustProjectPath(t, db, ref), Worktree: worktree, WorktreeIdentity: string(identity), BaseSHA: base, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	input.LeaderEpoch, input.RunnerEpoch, input.ExpectedVersion = fence.LeaderEpoch, fence.RunnerEpoch, ticket.Version
	claim, err := db.BeginProviderAttempt(context.Background(), store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: configDigest, Capacity: 1, At: time.Now().UTC(), Repository: mustProjectPath(t, db, ref), Worktree: worktree, WorktreeIdentity: string(identity), BaseSHA: base, SupervisorKey: signer.PublicKey(), Input: input})
	if err != nil {
		t.Fatalf("begin %s/%s: %v binding=%+v input=%+v key=%d", phase, role, err, binding, input, len(signer.PublicKey()))
	}
	if err := db.RecordProviderLaunch(context.Background(), claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "fixture", ProcessStartIdentity: "fixture-" + string(rune(claim.ID)), Worktree: worktree}); err != nil {
		t.Fatal(err)
	}
	drain := contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest}
	proof, err := signer.ProveDrained(drain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := phaseartifact.Parse(phase, contracts.PhaseResult{Provider: binding.Identity, Artifact: raw}, validation); err != nil {
		t.Fatalf("parse %s/%s: %v", phase, role, err)
	}
	if _, err := db.CompleteProviderAttemptSuccess(context.Background(), claim, proof, ticket.Version, fence, contracts.PhaseResult{Provider: binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
		t.Fatalf("complete %s/%s: %v", phase, role, err)
	}
	return claim
}

func completeCommand(t *testing.T, db *store.Store, ref domain.TicketRef, version uint64, fence domain.Fence, identity []byte, worktree, branch, base string, provider store.ProviderAttemptResultKey, intent, proof, checkpoint, policy string, exit int) contracts.RepositoryCommandResultKey {
	t.Helper()
	commandDigest := "sha256:" + digestHex([]byte(`["go","test","./..."]`))
	spec := "sha256:" + digestHex([]byte("spec"))
	execDigest := "sha256:" + digestHex([]byte("go"))
	request := store.RepositoryCommandEvidenceRequest{Purpose: store.RepositoryCommandPurposePrebuildVerification, Ref: ref, TicketVersion: version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ProviderResult: provider, VerificationIntentDigest: intent, ProofDigest: proof, CheckpointID: checkpoint, ConfigCommandDigest: commandDigest, Worktree: worktree, WorktreeIdentity: string(identity), BaseSHA: base, PolicyDigest: policy, SpecDigest: spec, ExecutablePath: "/usr/bin/go", ExecutableDigest: execDigest}
	if checkpoint != "" {
		request.Purpose = store.RepositoryCommandPurposePostbuildCandidate
	}
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(request)
	if err != nil {
		t.Fatal(err)
	}
	intentRow := store.RepositoryCommandIntent{EffectFence: store.EffectFence{SemanticKey: semantic, Ref: ref, TicketVersion: version, Fence: fence}, RequestDigest: requestDigest, Repository: mustProjectPath(t, db, ref), Worktree: worktree, WorktreeIdentity: string(identity), Branch: branch, BaseRef: "main", BaseSHA: base, CommandDigest: commandDigest, SpecDigest: spec, PolicyDigest: policy, ExecutablePath: "/usr/bin/go", ExecutableDigest: execDigest}
	if _, err := db.PlanEffect(context.Background(), store.EffectPlan{SemanticKey: semantic, Ref: ref, Kind: "repository_command", TicketVersion: version, Fence: fence, RequestDigest: requestDigest}); err != nil {
		t.Fatalf("plan repository command: %v", err)
	}
	if effect, err := db.Effect(context.Background(), semantic); err != nil {
		t.Fatalf("read repository command effect: %v", err)
	} else if effect.Ref != ref || effect.Kind != "repository_command" || effect.RequestDigest != requestDigest || effect.TicketVersion != version || effect.LeaderEpoch != fence.LeaderEpoch || effect.RunnerEpoch != fence.RunnerEpoch {
		t.Fatalf("repository command effect mismatch=%+v want ref=%+v version=%d fence=%+v digest=%s", effect, ref, version, fence, requestDigest)
	}
	claim, err := db.IssueRepositoryCommandClaim(context.Background(), intentRow)
	if err != nil {
		t.Fatalf("issue repository command: %v", err)
	}
	lease, err := db.AcquireRepositoryCommand(context.Background(), claim)
	if err != nil {
		t.Fatal(err)
	}
	launch := contracts.RepositoryCommandLaunch{PID: int(claim.ClaimEpoch + 100), PGID: int(claim.ClaimEpoch + 100), BootIdentity: "fixture", ProcessStartIdentity: "command-fixture"}
	if err := lease.RecordRepositoryCommandLaunch(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	if err := lease.FinishRepositoryCommandLaunch(context.Background(), launch); err != nil {
		t.Fatal(err)
	}
	observed := time.Now().UTC()
	if err := db.CompleteRepositoryCommand(context.Background(), claim, contracts.CommandResult{ExitCode: exit, Stdout: []byte("fixture"), Observed: true, ObservedAt: observed}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	return contracts.RepositoryCommandResultKey{SemanticKey: semantic, ClaimEpoch: claim.ClaimEpoch}
}

func mustProjectPath(t *testing.T, db *store.Store, ref domain.TicketRef) string {
	t.Helper()
	p, err := db.Project(context.Background(), ref.Channel, ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	return p.Path
}
func providerKey(ref domain.TicketRef, phase domain.Phase, claim store.ProviderAttemptClaim) store.ProviderAttemptResultKey {
	return store.ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ref, Phase: phase, Attempt: claim.Attempt}
}
func qualification(run, provider, family string) store.ProviderQualification {
	return store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: domain.ProviderIdentity{Provider: provider, Model: provider + "-model", Family: family, Version: "1.0"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()}
}
func runtimeBinding(q store.ProviderQualification) contracts.RuntimeBinding {
	return contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: strings.Repeat("d", 64), AuthMode: "subscription"}
}
func refType() domain.TicketType   { return domain.TicketFeature }
func digestHex(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func gitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	cmd := exec.Command("/usr/bin/git", append([]string{"-C", directory}, args...)...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}
func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	cmd := exec.Command("/usr/bin/git", append([]string{"-C", directory}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}
func makeCommit(t *testing.T, worktree, message, content string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(worktree, "README.md"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worktree, "add", "README.md")
	cmd := exec.Command("/usr/bin/git", "-C", worktree, "-c", "user.name=fixture", "-c", "user.email=fixture@example.test", "commit", "-m", message)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("commit: %v\n%s", err, out)
	}
	return gitOutput(t, worktree, "rev-parse", "HEAD")
}

func makeTreeCommit(t *testing.T, worktree, tree, parent string) string {
	t.Helper()
	cmd := exec.Command("/usr/bin/git", "-C", worktree, "-c", "user.name=fixture", "-c", "user.email=fixture@example.test", "commit-tree", tree, "-p", parent, "-m", "remote drift")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}
