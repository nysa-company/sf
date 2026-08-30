package workflowworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// This deliberately uses a real Store and Engine. Repository-command
// composition is intentionally not wired into workflowruntime yet, so once a
// reviewer result exists the Store must fail closed instead of accepting the
// historical provider artifact alone.
func TestWorkerRealStoreFailsClosedWithoutCommandResultWiring(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "workflow.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const projectPath = "/tmp/workflow-worker-real"
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("real", projectPath), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "real", Path: projectPath, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "real", Ticket: "SF-real-worker"}
	sourceSum := sha256.Sum256([]byte("real source"))
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: hex.EncodeToString(sourceSum[:]), Source: []byte("real source"), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "real-worker-test")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := db.StartOrAdopt(ctx, ref, 1, "dev/real/SF-real-worker", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: initial.RunnerEpoch}
	identity := `{"repository":"/tmp/workflow-worker-real"}`
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: initial.Version, Fence: fence, Path: projectPath + "/SF-real-worker", Branch: "dev/real/SF-real-worker", IdentityJSON: []byte(identity), BaseSHA: realOID("a"), HeadSHA: realOID("b")}); err != nil {
		t.Fatal(err)
	}

	planner := realQualification(t, db, "11111111111111111111111111111111", "planner")
	builder := realQualification(t, db, "22222222222222222222222222222222", "builder")
	reviewer := realQualification(t, db, "33333333333333333333333333333333", "reviewer")
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, planner.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	runner := &realRunner{db: db, configDigest: configDigest, bindings: map[domain.Phase]contracts.RuntimeBinding{domain.PhasePlanning: realBinding(planner), domain.PhaseVerification: realBinding(reviewer), domain.PhaseBuild: realBinding(builder)}, signer: signer, failAfter: map[domain.Phase]bool{domain.PhasePlanning: true}}
	evidence := &realFaultEvidence{Evidence: db, failVerification: true, failCandidate: true}
	runtime := &realFaultEngine{StateMachine: engine.New(db, spec), failVerificationSignal: true, failCandidateSignal: true}
	checkpoint := &realCheckpoint{}
	candidateBoundary := &realCandidate{}
	worker := Worker{Evidence: evidence, Engine: runtime, Runner: runner, Checkpoint: checkpoint, Candidate: candidateBoundary, CheckpointMaterializer: checkpoint, CandidateMaterializer: candidateBoundary}
	if _, err := worker.Run(ctx, ref, fence); err == nil {
		t.Fatal("expected injected crash after durable planner result")
	}
	for i := 0; i < 3; i++ {
		if i == 3 || i == 6 {
			// Recover once after reviewer evidence/before signal and again after
			// candidate evidence/before signal. Each old result must be selected
			// by LatestReusable under the live fence, never re-run.
			leader, err = db.AcquireLeader(ctx, domain.ChannelDev, "real-worker-recovery")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
				t.Fatal(err)
			}
			fence.LeaderEpoch = leader
		}
		ticket, err := db.Ticket(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		fence.RunnerEpoch = ticket.RunnerEpoch
		_, err = worker.Run(ctx, ref, fence)
		if i == 1 {
			if err == nil {
				t.Fatalf("run %d expected injected post-result crash", i)
			}
			continue
		}
		if i == 2 {
			if !errors.Is(err, ErrCommandResultRequired) {
				t.Fatalf("run %d accepted unbound reviewer evidence: %v", i, err)
			}
			current, readErr := db.Ticket(ctx, ref)
			if readErr != nil || current.State != domain.StateVerifying || runner.calls[domain.PhasePlanning] != 1 || runner.calls[domain.PhaseVerification] != 1 {
				t.Fatalf("unwired command boundary ticket=%+v calls=%v err=%v", current, runner.calls, readErr)
			}
			return
		}
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	final, err := db.Ticket(ctx, ref)
	if err != nil || final.State != domain.StatePublishing {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	for _, phase := range []domain.Phase{domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild} {
		if runner.calls[phase] != 1 {
			t.Fatalf("%s calls=%d", phase, runner.calls[phase])
		}
	}
	if checkpoint.materializations < 2 || checkpoint.authentications < 3 || candidateBoundary.materializations < 3 || candidateBoundary.authentications < 3 {
		t.Fatalf("recovery boundaries checkpoint=%+v candidate=%+v", checkpoint, candidateBoundary)
	}
	verification, err := db.CurrentVerification(ctx, ref)
	if err != nil || verification.ProviderResult.AttemptID <= 0 {
		t.Fatalf("verification binding=%+v err=%v", verification, err)
	}
	candidate, err := db.LatestCandidate(ctx, ref)
	if err != nil || candidate.BuilderResult.AttemptID <= 0 || candidate.Commit.ParentOID != realOID("c") || candidate.Fence != fence {
		t.Fatalf("candidate binding=%+v err=%v", candidate, err)
	}
	// Questions are a durable operator boundary. A later scheduler pass must
	// leave the ticket paused and must not call Planner again.
	questionRef := domain.TicketRef{Channel: domain.ChannelDev, Project: "real", Ticket: "SF-real-question"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: questionRef, SourceDigest: realDigest("question source"), Source: []byte("question source"), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	questionTicket, err := db.StartOrAdopt(ctx, questionRef, 1, "dev/real/SF-real-question", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	questionFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: questionTicket.RunnerEpoch}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: questionRef, ExpectedVersion: questionTicket.Version, Fence: questionFence, Path: projectPath + "/SF-real-question", Branch: "dev/real/SF-real-question", IdentityJSON: []byte(identity), BaseSHA: realOID("a"), HeadSHA: realOID("b")}); err != nil {
		t.Fatal(err)
	}
	runner.questions = true
	plannerCalls := runner.calls[domain.PhasePlanning]
	if _, err := worker.Run(ctx, questionRef, questionFence); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Run(ctx, questionRef, questionFence); err != nil {
		t.Fatal(err)
	}
	paused, err := db.Ticket(ctx, questionRef)
	if err != nil || paused.State != domain.StatePaused || runner.calls[domain.PhasePlanning] != plannerCalls+1 {
		t.Fatalf("question pause=%+v calls=%d err=%v", paused, runner.calls[domain.PhasePlanning], err)
	}
}

func TestWorkerPlanningRecoveryRebindsOnlyExactRecoveredPlanResult(t *testing.T) {
	t.Run("rebinds an exact plan after RecordPlan before SignalPlan", func(t *testing.T) {
		fixture := newRealPlanningRecoveryFixture(t)
		defer fixture.db.Close()

		if _, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err == nil {
			t.Fatal("expected injected SignalPlan response loss")
		} else if !strings.Contains(err.Error(), "injected crash after plan evidence") {
			t.Fatalf("first run failed before durable plan: %v", err)
		}
		oldPlan, err := fixture.db.Plan(fixture.ctx, fixture.ref)
		if err != nil || oldPlan.Document.ProviderResult == nil || fixture.runner.calls[domain.PhasePlanning] != 1 {
			t.Fatalf("old plan=%+v calls=%d err=%v", oldPlan, fixture.runner.calls[domain.PhasePlanning], err)
		}
		fixture.recover(t)

		result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence)
		if err != nil || !result.Transitioned || !result.Replayed || result.State != domain.StateVerifying {
			t.Fatalf("recovery result=%+v err=%v", result, err)
		}
		plan, err := fixture.db.Plan(fixture.ctx, fixture.ref)
		if err != nil || plan.TicketVersion != result.Version-1 || plan.Fence != fixture.fence || plan.Document.ProviderResult == nil || *plan.Document.ProviderResult != *oldPlan.Document.ProviderResult {
			t.Fatalf("rebound plan=%+v old=%+v err=%v", plan, oldPlan, err)
		}
		if fixture.runner.calls[domain.PhasePlanning] != 1 {
			t.Fatalf("Planner reran after recovery: calls=%d", fixture.runner.calls[domain.PhasePlanning])
		}
	})

	t.Run("rejects a newer recovered planner result that does not match the stored plan", func(t *testing.T) {
		fixture := newRealPlanningRecoveryFixture(t)
		defer fixture.db.Close()

		if _, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err == nil {
			t.Fatal("expected injected SignalPlan response loss")
		} else if !strings.Contains(err.Error(), "injected crash after plan evidence") {
			t.Fatalf("first run failed before durable plan: %v", err)
		}
		oldPlan, err := fixture.db.Plan(fixture.ctx, fixture.ref)
		if err != nil || oldPlan.Document.ProviderResult == nil {
			t.Fatalf("old plan=%+v err=%v", oldPlan, err)
		}
		before, err := fixture.db.Ticket(fixture.ctx, fixture.ref)
		if err != nil {
			t.Fatal(err)
		}
		request, err := fixture.worker.request(fixture.ctx, before, fixture.fence, domain.PhasePlanning, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.runner.Run(fixture.ctx, request); !errors.Is(err, store.ErrProviderAttemptReusable) {
			t.Fatalf("new planner completion escaped atomic reuse admission: %v", err)
		}
		if fixture.runner.calls[domain.PhasePlanning] != 2 {
			t.Fatalf("planner call was not attempted: calls=%d", fixture.runner.calls[domain.PhasePlanning])
		}
	})
}

// A reviewer completion can be returned by providercoord's Begin-time reuse
// path after another runner already completed it.  Worker consumes only the
// immutable key, so model the lost response here: the first runner has
// completed the exact Store result but returns an error before Worker sees it.
func TestWorkerPersistsAndTransitionsReusedReviewerResult(t *testing.T) {
	fixture := newRealPlanningRecoveryFixture(t)
	defer fixture.db.Close()

	if _, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err == nil {
		t.Fatal("expected injected planner response loss")
	}
	fixture.recover(t)
	plan, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence)
	if err != nil || !plan.Transitioned || !plan.Replayed || plan.State != domain.StateVerifying {
		t.Fatalf("plan replay=%+v err=%v", plan, err)
	}
	fixture.runner.failAfter = map[domain.Phase]bool{domain.PhaseVerification: true}
	if _, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err == nil || !strings.Contains(err.Error(), "injected crash after durable verification result") {
		t.Fatalf("reviewer did not leave reusable completion: %v", err)
	}
	ticket, err := fixture.db.Ticket(fixture.ctx, fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	if ticket.State != domain.StateVerifying {
		t.Fatalf("ticket after lost reviewer response=%+v", ticket)
	}
	reusable, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, store.LatestReusableProviderAttemptRequest{Ref: fixture.ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: ticket.Version, Fence: fixture.fence})
	if err != nil || reusable.Key.AttemptID == 0 || reusable.Result.Claim.Role != "reviewer" {
		t.Fatalf("reviewer reusable=%+v err=%v", reusable, err)
	}
	result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence)
	if err != nil || !result.Transitioned || !result.Replayed || result.State != domain.StateBuilding {
		t.Fatalf("reviewer reuse transition=%+v err=%v", result, err)
	}
	verification, err := fixture.db.CurrentVerification(fixture.ctx, fixture.ref)
	if err != nil || verification.ProviderResult != reusable.Key || verification.TicketVersion != ticket.Version || verification.Fence != fixture.fence {
		t.Fatalf("verification=%+v reusable=%+v err=%v", verification, reusable, err)
	}
	if fixture.runner.calls[domain.PhaseVerification] != 1 {
		t.Fatalf("reviewer reran after reusable result: calls=%d", fixture.runner.calls[domain.PhaseVerification])
	}
}

type realPlanningRecoveryFixture struct {
	ctx     context.Context
	db      *store.Store
	ref     domain.TicketRef
	fence   domain.Fence
	worker  Worker
	runner  *realRunner
	machine *engine.Engine
}

func newRealPlanningRecoveryFixture(t *testing.T) *realPlanningRecoveryFixture {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "planning-recovery.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	const projectPath = "/tmp/workflow-worker-plan-recovery"
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("plan-recovery", projectPath), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "plan-recovery", Path: projectPath, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "plan-recovery", Ticket: "SF-plan-recovery"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: realDigest("plan recovery source"), Source: []byte("plan recovery source"), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "plan-recovery")
	if err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, 1, "dev/plan-recovery/SF-plan-recovery", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	worktreePath, branch := projectPath+"/SF-plan-recovery", "dev/plan-recovery/SF-plan-recovery"
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktreePath, Branch: branch, IdentityJSON: realWorktreeIdentity(t, projectPath, worktreePath, branch), BaseSHA: realOID("a"), HeadSHA: realOID("b")}); err != nil {
		t.Fatal(err)
	}
	planner := realQualification(t, db, "44444444444444444444444444444444", "planner")
	builder := realQualification(t, db, "55555555555555555555555555555555", "builder")
	reviewer := realQualification(t, db, "66666666666666666666666666666666", "reviewer")
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, planner.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	runner := &realRunner{db: db, repository: projectPath, configDigest: configDigest, bindings: map[domain.Phase]contracts.RuntimeBinding{domain.PhasePlanning: realBinding(planner), domain.PhaseVerification: realBinding(reviewer), domain.PhaseBuild: realBinding(builder)}, signer: signer}
	machine := engine.New(db, spec)
	runtime := &realFaultEngine{StateMachine: machine, failPlanSignal: true}
	checkpoint := &realCheckpoint{db: db, withCommand: true}
	candidate := &realCandidate{}
	return &realPlanningRecoveryFixture{ctx: ctx, db: db, ref: ref, fence: fence, worker: Worker{Evidence: db, Engine: runtime, Runner: runner, Checkpoint: checkpoint, Candidate: candidate, CheckpointMaterializer: checkpoint, CandidateMaterializer: candidate}, runner: runner, machine: machine}
}

func (f *realPlanningRecoveryFixture) recover(t *testing.T) {
	t.Helper()
	leader, err := f.db.AcquireLeader(f.ctx, domain.ChannelDev, "plan-recovery-restarted")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ReconcileEffects(f.ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.FenceRecoveredRunners(f.ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	if err := f.machine.RecoverChannel(f.ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	f.fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
}

type realFaultEvidence struct {
	Evidence
	failVerification, failCandidate bool
}

func (e *realFaultEvidence) RecordVerification(ctx context.Context, value store.VerificationArtifact) (store.VerificationRevision, error) {
	if e.failVerification {
		e.failVerification = false
		return store.VerificationRevision{}, fmt.Errorf("injected crash after verification materialization")
	}
	return e.Evidence.RecordVerification(ctx, value)
}
func (e *realFaultEvidence) RecordCandidate(ctx context.Context, value store.CandidateEvidence) ([]store.InvalidationReceipt, error) {
	if e.failCandidate {
		e.failCandidate = false
		return nil, fmt.Errorf("injected crash after candidate materialization")
	}
	return e.Evidence.RecordCandidate(ctx, value)
}

type realFaultEngine struct {
	StateMachine
	failPlanSignal, failVerificationSignal, failCandidateSignal bool
}

func (e *realFaultEngine) Signal(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From == domain.StateVerifying && e.failVerificationSignal {
		e.failVerificationSignal = false
		return contracts.TransitionResult{}, fmt.Errorf("injected crash after verification evidence")
	}
	return e.StateMachine.Signal(ctx, request)
}
func (e *realFaultEngine) SignalPlan(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if e.failPlanSignal {
		e.failPlanSignal = false
		return contracts.TransitionResult{}, fmt.Errorf("injected crash after plan evidence")
	}
	return e.StateMachine.SignalPlan(ctx, request)
}
func (e *realFaultEngine) SignalVerification(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if e.failVerificationSignal {
		e.failVerificationSignal = false
		return contracts.TransitionResult{}, fmt.Errorf("injected crash after verification evidence")
	}
	return e.StateMachine.SignalVerification(ctx, request)
}
func (e *realFaultEngine) SignalCandidate(ctx context.Context, request contracts.SignalRequest, candidate domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	if e.failCandidateSignal {
		e.failCandidateSignal = false
		return contracts.TransitionResult{}, fmt.Errorf("injected crash after candidate evidence")
	}
	return e.StateMachine.SignalCandidate(ctx, request, candidate)
}

type realRunner struct {
	db           *store.Store
	repository   string
	configDigest string
	bindings     map[domain.Phase]contracts.RuntimeBinding
	signer       *contracts.DrainSigner
	calls        map[domain.Phase]int
	failAfter    map[domain.Phase]bool
	questions    bool
}

func (r *realRunner) Run(ctx context.Context, req PhaseRequest) (PhaseResult, error) {
	if r.calls == nil {
		r.calls = map[domain.Phase]int{}
	}
	r.calls[req.Phase]++
	role := map[domain.Phase]string{domain.PhasePlanning: "planner", domain.PhaseVerification: "reviewer", domain.PhaseBuild: "builder"}[req.Phase]
	binding := r.bindings[req.Phase]
	repository := r.repository
	if repository == "" {
		repository = "/tmp/workflow-worker-real"
	}
	claim, err := r.db.BeginProviderAttempt(ctx, store.ProviderAttemptRequest{Ref: req.Ticket.Ref, ExpectedVersion: req.Ticket.Version, Fence: req.Fence, Phase: req.Phase, Role: role, Binding: binding, ConfigDigest: r.configDigest, Capacity: 1, At: time.Now().UTC(), Repository: repository, Worktree: req.Worktree.Path, WorktreeIdentity: string(req.Worktree.IdentityJSON), BaseSHA: req.Worktree.BaseSHA, SupervisorKey: r.signer.PublicKey(), Input: contracts.PhaseInput{Ticket: req.Ticket.Ref, Phase: req.Phase, LeaderEpoch: req.Fence.LeaderEpoch, RunnerEpoch: req.Fence.RunnerEpoch, ExpectedVersion: req.Ticket.Version, Prompt: "real integration", Repository: repository, Worktree: req.Worktree.Path, WorktreeIdentity: string(req.Worktree.IdentityJSON), BaseSHA: req.Worktree.BaseSHA, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}})
	if err != nil {
		return PhaseResult{}, fmt.Errorf("begin %s v%d fence=%+v: %w", req.Phase, req.Ticket.Version, req.Fence, err)
	}
	if err := r.db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: 99, PGID: 99, BootIdentity: "test", ProcessStartIdentity: "real-worker", Worktree: claim.Worktree}); err != nil {
		return PhaseResult{}, fmt.Errorf("record launch: %w", err)
	}
	artifact, changed := r.artifact(req)
	validation := phaseartifact.Validation{TicketType: req.Ticket.Type}
	if req.Phase == domain.PhaseVerification {
		id, _ := workflowprompt.NewPlanIdentity(*req.Plan.Document.Planner)
		validation.AcceptanceDigest = id.Digest
	}
	if _, err := phaseartifact.Parse(req.Phase, contracts.PhaseResult{Provider: binding.Identity, Artifact: artifact, ChangedFiles: changed}, validation); err != nil {
		return PhaseResult{}, fmt.Errorf("test artifact %s: %w", req.Phase, err)
	}
	proof, err := r.signer.ProveDrained(realDrainRequest(claim))
	if err != nil {
		return PhaseResult{}, fmt.Errorf("complete result: %w", err)
	}
	_, err = r.db.CompleteProviderAttemptSuccess(ctx, claim, proof, req.Ticket.Version, req.Fence, contracts.PhaseResult{Provider: binding.Identity, Artifact: artifact, ChangedFiles: changed, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC())
	if err != nil {
		return PhaseResult{}, fmt.Errorf("complete result: %w", err)
	}
	if r.failAfter[req.Phase] {
		delete(r.failAfter, req.Phase)
		return PhaseResult{}, fmt.Errorf("injected crash after durable %s result", req.Phase)
	}
	return PhaseResult{ProviderResult: store.ProviderAttemptResultKey{AttemptID: claim.ID, Ref: claim.Ref, Phase: claim.Phase, Attempt: claim.Attempt}}, nil
}
func (r *realRunner) artifact(req PhaseRequest) ([]byte, []string) {
	switch req.Phase {
	case domain.PhasePlanning:
		planner := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"real integration"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "real"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"none"}}
		if r.questions {
			planner.Questions = []phaseartifact.Question{{Prompt: "which target?", Options: []string{"one", "two"}}}
		}
		v, _ := json.Marshal(planner)
		return v, nil
	case domain.PhaseVerification:
		id, _ := workflowprompt.NewPlanIdentity(*req.Plan.Document.Planner)
		v, _ := json.Marshal(phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: id.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: realDigest("verification")})
		return v, nil
	default:
		v, _ := json.Marshal(phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "real", ChangedFiles: []string{"internal/real.go"}, Commands: [][]string{{"go", "test", "./..."}}})
		return v, []string{"internal/real.go"}
	}
}

// realVerificationCommandEvidence exercises the same public Store command
// lifecycle that production repository materialization must supply.  The
// worker only receives its immutable result key through VerificationCheckpoint.
func realVerificationCommandEvidence(ctx context.Context, db *store.Store, request PhaseRequest, artifact phaseartifact.Verification, provider store.ProviderAttemptResultKey) (contracts.RepositoryCommandResultKey, error) {
	if db == nil {
		return contracts.RepositoryCommandResultKey{}, errors.New("Store required for command evidence")
	}
	project, err := db.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(artifact)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	proof, err := workflowprompt.CanonicalVerificationProofBytes(artifact)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	argv, err := json.Marshal([]string{"go", "test", "./..."})
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	commandDigest := "sha256:" + realDigest(string(argv))
	policy, spec, executable := "sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), "sha256:"+strings.Repeat("3", 64)
	evidenceRequest := store.RepositoryCommandEvidenceRequest{Purpose: store.RepositoryCommandPurposePrebuildVerification, Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch, ProviderResult: provider, VerificationIntentDigest: realDigest(string(intent)), ProofDigest: realDigest(string(proof)), ConfigCommandDigest: commandDigest, Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), BaseSHA: request.Worktree.BaseSHA, PolicyDigest: policy, SpecDigest: spec, ExecutablePath: "/usr/bin/true", ExecutableDigest: executable}
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(evidenceRequest)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(evidenceRequest)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	command := store.RepositoryCommandIntent{EffectFence: store.EffectFence{SemanticKey: semantic, Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, Fence: request.Fence}, RequestDigest: requestDigest, Repository: project.Path, Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), Branch: request.Worktree.Branch, BaseRef: project.BaseRef, BaseSHA: request.Worktree.BaseSHA, CommandDigest: commandDigest, SpecDigest: spec, PolicyDigest: policy, ExecutablePath: "/usr/bin/true", ExecutableDigest: executable}
	if _, err := db.PlanEffect(ctx, store.EffectPlan{SemanticKey: semantic, Ref: request.Ticket.Ref, Kind: "repository_command", TicketVersion: request.Ticket.Version, Fence: request.Fence, RequestDigest: requestDigest}); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	claim, err := db.IssueRepositoryCommandClaim(ctx, command)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	launch := contracts.RepositoryCommandLaunch{PID: 654, PGID: 654, BootIdentity: "worker", ProcessStartIdentity: "worker-verification"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 1, Duration: time.Millisecond, Observed: true, ObservedAt: time.Now().UTC()}); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if err := lease.Release(); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	return contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}, nil
}

type realCheckpoint struct {
	db                                *store.Store
	withCommand                       bool
	materializations, authentications int
}

func (r *realCheckpoint) MaterializeVerificationCheckpoint(ctx context.Context, request PhaseRequest, artifact phaseartifact.Verification, key store.ProviderAttemptResultKey) (VerificationCheckpoint, error) {
	r.materializations++
	checkpoint := VerificationCheckpoint{ID: realOID("c"), Commit: store.CommitObservation{CommitOID: realOID("c"), ParentOID: realOID("a"), TreeOID: realOID("d")}}
	if r.withCommand {
		command, err := realVerificationCommandEvidence(ctx, r.db, request, artifact, key)
		if err != nil {
			return VerificationCheckpoint{}, err
		}
		checkpoint.CommandResult = command
	}
	return checkpoint, nil
}
func (r *realCheckpoint) AuthenticateVerificationCheckpoint(context.Context, PhaseRequest, phaseartifact.Verification, VerificationCheckpoint) error {
	r.authentications++
	return nil
}

type realCandidate struct{ materializations, authentications int }

func (r *realCandidate) MaterializeCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, store.ProviderAttemptResultKey) (CandidateWitness, error) {
	r.materializations++
	return CandidateWitness{Commit: store.CommitObservation{CommitOID: realOID("e"), ParentOID: realOID("c"), TreeOID: realOID("f")}, CommandPolicyDigest: realDigest("policy"), Reason: "real"}, nil
}
func (r *realCandidate) AuthenticateCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, CandidateWitness) error {
	r.authentications++
	return nil
}
func realOID(v string) string    { return strings.Repeat(v, 40) }
func realDigest(v string) string { s := sha256.Sum256([]byte(v)); return hex.EncodeToString(s[:]) }
func realWorktreeIdentity(t *testing.T, repository, worktree, branch string) []byte {
	t.Helper()
	identity, err := workflowprompt.MarshalCanonicalWorktreeIdentity(gitboundary.Identity{Repository: repository, RepositoryDev: 1, RepositoryIno: 2, Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4, GitFile: "gitdir: " + repository + "/.git/worktrees/real\n", GitFileDev: 5, GitFileIno: 6, CommonDir: repository + "/.git", CommonDirDev: 7, CommonDirIno: 8, Origin: "https://example.invalid/real", PushOrigin: "https://example.invalid/real", BaseRef: "main", BaseHead: realOID("a"), HeadRef: branch, ConfigHash: "sha256:" + strings.Repeat("b", 64), HooksHash: "sha256:" + strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	return identity
}
func realQualification(t *testing.T, db *store.Store, run, name string) store.ProviderQualification {
	t.Helper()
	q, _, err := db.RecordProviderQualification(context.Background(), store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: name + "-family", Version: "1.0.0"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Date(2026, 8, 29, 18, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return q
}
func realBinding(q store.ProviderQualification) contracts.RuntimeBinding {
	return contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: realDigest("auth:" + q.Provider.Model)}
}
func realDrainRequest(c store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: c.ID, Identity: c.Binding.Identity, Ref: c.Ref, Phase: c.Phase, Role: c.Role, Attempt: c.Attempt, LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch, ExpectedVersion: c.ExpectedVersion, LeaseKey: c.LeaseKey, BindingDigest: c.BindingDigest, BinaryDigest: c.Binding.BinaryDigest, PolicyDigest: c.Binding.PolicyDigest, AuthDigest: c.Binding.AuthDigest, AuthMode: c.Binding.AuthMode, Repository: c.Repository, Worktree: c.Worktree, WorktreeIdentity: c.WorktreeIdentity, BaseSHA: c.BaseSHA, RequestDigest: c.RequestDigest}
}
