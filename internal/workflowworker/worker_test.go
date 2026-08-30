package workflowworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

var testRef = domain.TicketRef{Channel: domain.ChannelDev, Project: "sf", Ticket: "SF-worker"}
var testFence = domain.Fence{LeaderEpoch: 7, RunnerEpoch: 1}

type fakeEvidence struct {
	ticket          store.Ticket
	plan            store.StoredPlan
	verification    store.StoredVerification
	candidate       store.StoredCandidate
	hasPlan         bool
	hasVerify       bool
	hasCandidate    bool
	budget          int
	plans           int
	verifications   int
	candidates      int
	providerResults map[int64]phaseartifact.Parsed
}

func (f *fakeEvidence) Ticket(context.Context, domain.TicketRef) (store.Ticket, error) {
	return f.ticket, nil
}
func (f *fakeEvidence) Plan(context.Context, domain.TicketRef) (store.StoredPlan, error) {
	if !f.hasPlan {
		return store.StoredPlan{}, store.ErrNotFound
	}
	return f.plan, nil
}
func (f *fakeEvidence) CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error) {
	if !f.hasVerify {
		return store.StoredVerification{}, store.ErrNotFound
	}
	return f.verification, nil
}
func (f *fakeEvidence) ValidateCurrentCandidateForBuildTransition(_ context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.StoredCandidate, error) {
	if !f.hasCandidate {
		return store.StoredCandidate{}, store.ErrNotFound
	}
	if f.candidate.TicketVersion != version || f.candidate.Fence != fence {
		return store.StoredCandidate{}, ErrStaleEvidence
	}
	return f.candidate, nil
}
func (f *fakeEvidence) Worktree(context.Context, domain.TicketRef) (store.StoredWorktree, error) {
	return store.StoredWorktree{Path: "/tmp/worktree", Branch: "refs/heads/sf", BaseSHA: oid}, nil
}
func (f *fakeEvidence) AssertTicketFence(_ context.Context, _ domain.TicketRef, version uint64, fence domain.Fence) error {
	if version != f.ticket.Version || fence != testFence {
		return store.ErrStaleFence
	}
	return nil
}
func (f *fakeEvidence) LoadCurrentProviderAttemptResult(_ context.Context, key store.ProviderAttemptResultKey, _ uint64, _ domain.Fence) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	p, ok := f.providerResults[key.AttemptID]
	if !ok {
		return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, store.ErrNotFound
	}
	if p.Verify != nil && f.plan.Document.Planner != nil {
		identity, err := workflowprompt.NewPlanIdentity(*f.plan.Document.Planner)
		if err != nil {
			return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, err
		}
		copy := *p.Verify
		copy.AcceptanceDigest = identity.Digest
		p.Verify = &copy
	}
	return store.ProviderAttemptResult{Claim: store.ProviderAttemptClaim{Ref: testRef, Phase: key.Phase, ExpectedVersion: f.ticket.Version, LeaderEpoch: testFence.LeaderEpoch, RunnerEpoch: testFence.RunnerEpoch, Role: map[domain.Phase]string{domain.PhasePlanning: "planner", domain.PhaseVerification: "verification", domain.PhaseBuild: "builder"}[key.Phase]}}, p, nil
}
func (f *fakeEvidence) LatestReusableProviderAttempt(_ context.Context, request store.LatestReusableProviderAttemptRequest) (store.LatestReusableProviderAttemptResult, error) {
	for id, p := range f.providerResults {
		if p.Phase == request.Phase {
			if p.Verify != nil && f.plan.Document.Planner != nil {
				identity, err := workflowprompt.NewPlanIdentity(*f.plan.Document.Planner)
				if err != nil {
					return store.LatestReusableProviderAttemptResult{}, err
				}
				copy := *p.Verify
				copy.AcceptanceDigest = identity.Digest
				p.Verify = &copy
			}
			return store.LatestReusableProviderAttemptResult{Key: store.ProviderAttemptResultKey{AttemptID: id, Ref: testRef, Phase: request.Phase, Attempt: 1}, Parsed: p}, nil
		}
	}
	return store.LatestReusableProviderAttemptResult{}, store.ErrNotFound
}
func (f *fakeEvidence) RecordPlan(_ context.Context, a store.PlanArtifact) (string, error) {
	f.hasPlan, f.plans = true, f.plans+1
	f.plan = store.StoredPlan{Digest: "plan", Document: a.Document, TicketVersion: a.ExpectedVersion, Fence: a.Fence}
	return "plan", nil
}
func (f *fakeEvidence) RecordVerification(_ context.Context, a store.VerificationArtifact) (store.VerificationRevision, error) {
	f.hasVerify, f.verifications = true, f.verifications+1
	ih, ph := sha256.Sum256(a.Intent), sha256.Sum256(a.Proof)
	f.verification = store.StoredVerification{Revision: store.VerificationRevision{Revision: 1, IntentDigest: hex.EncodeToString(ih[:]), ProofDigest: hex.EncodeToString(ph[:]), OwnedFiles: a.OwnedFiles, CheckpointID: a.CheckpointID}, TicketVersion: a.ExpectedVersion, Fence: a.Fence, Intent: a.Intent, Proof: a.Proof}
	return f.verification.Revision, nil
}
func (f *fakeEvidence) RecordCandidate(_ context.Context, a store.CandidateEvidence) ([]store.InvalidationReceipt, error) {
	f.hasCandidate, f.candidates = true, f.candidates+1
	f.candidate = store.StoredCandidate{Snapshot: a.Snapshot, TicketVersion: a.ExpectedVersion, Fence: a.Fence}
	return nil, nil
}
func (f *fakeEvidence) ConsumeBudget(context.Context, store.BudgetUse) (int, error) {
	if f.budget >= 2 {
		return f.budget, store.ErrBudgetExhausted
	}
	f.budget++
	return f.budget, nil
}

type fakeEngine struct {
	state   *store.Ticket
	last    contracts.SignalRequest
	stale   bool
	signals int
}

func (e *fakeEngine) SignalCandidate(ctx context.Context, req contracts.SignalRequest, _ domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	return e.Signal(ctx, req)
}

func (e *fakeEngine) Signal(_ context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	e.signals++
	e.last = req
	if e.stale {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	if req.Fence != testFence {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	switch req.Trigger {
	case "phase_pass":
		switch req.From {
		case domain.StatePlanning:
			e.state.State = domain.StateVerifying
		case domain.StateVerifying:
			e.state.State = domain.StateBuilding
		case domain.StateBuilding:
			e.state.State = domain.StatePublishing
		}
	case "needs_operator_input":
		e.state.State = domain.StatePaused
	case "verification_amendment_requested":
		e.state.State = domain.StateVerifying
	}
	e.state.Version++
	return contracts.TransitionResult{To: e.state.State, TicketVersion: e.state.Version}, nil
}

type fakeRunner struct {
	outputs  []PhaseResult
	requests []PhaseRequest
	evidence *fakeEvidence
}

type fakeCheckpoint struct{}

func (fakeCheckpoint) AuthenticateVerificationCheckpoint(context.Context, PhaseRequest, phaseartifact.Verification, VerificationCheckpoint) error {
	return nil
}

func (r *fakeRunner) Run(_ context.Context, req PhaseRequest) (PhaseResult, error) {
	r.requests = append(r.requests, req)
	if len(r.outputs) == 0 {
		return PhaseResult{}, errors.New("no scripted output")
	}
	out := r.outputs[0]
	r.outputs = r.outputs[1:]
	if r.evidence != nil {
		if r.evidence.providerResults == nil {
			r.evidence.providerResults = map[int64]phaseartifact.Parsed{}
		}
		r.evidence.providerResults[out.ProviderResult.AttemptID] = out.Parsed
	}
	return out, nil
}

const oid = "0123456789abcdef0123456789abcdef01234567"
const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func ticket(state domain.State) store.Ticket {
	return store.Ticket{Ref: testRef, State: state, Version: 1, RunnerEpoch: 1, Type: domain.TicketBug, SourceDigest: digest}
}
func provider() domain.ProviderIdentity {
	return domain.ProviderIdentity{Provider: "codex", Model: "test", Family: "test", Version: "1"}
}
func plannerOutput(questions bool) PhaseResult {
	p := &phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"accept"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofRegression, Command: []string{"go", "test", "./..."}, Details: "proof"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"risk"}}
	if questions {
		p.Questions = []phaseartifact.Question{{Prompt: "scope?", Options: []string{"a", "b"}}}
	}
	return PhaseResult{Parsed: phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: provider(), Planner: p}, ProviderResult: store.ProviderAttemptResultKey{AttemptID: 1, Ref: testRef, Phase: domain.PhasePlanning, Attempt: 1}}
}
func verificationOutput() PhaseResult {
	v := &phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: digest, ProofKind: phaseartifact.ProofRegression, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: digest}
	return PhaseResult{Parsed: phaseartifact.Parsed{Phase: domain.PhaseVerification, Provider: provider(), Verify: v}, ProviderResult: store.ProviderAttemptResultKey{AttemptID: 2, Ref: testRef, Phase: domain.PhaseVerification, Attempt: 1}, Checkpoint: &VerificationCheckpoint{ID: oid, Commit: store.CommitObservation{CommitOID: oid, ParentOID: oid, TreeOID: oid}}}
}
func builderOutput(amend bool) PhaseResult {
	b := &phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "implemented", ChangedFiles: []string{"internal/foo.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	if amend {
		b.AmendmentRequest = &phaseartifact.AmendmentRequest{OldProofDigest: digest, ProposedDigest: digest, Reason: "proof needs clarification"}
	}
	return PhaseResult{Parsed: phaseartifact.Parsed{Phase: domain.PhaseBuild, Provider: provider(), Builder: b}, ProviderResult: store.ProviderAttemptResultKey{AttemptID: 3, Ref: testRef, Phase: domain.PhaseBuild, Attempt: 1}}
}

func newWorker(state domain.State, runner *fakeRunner, evidence *fakeEvidence, engine *fakeEngine) Worker {
	evidence.ticket = ticket(state)
	engine.state = &evidence.ticket
	if evidence.providerResults == nil {
		evidence.providerResults = map[int64]phaseartifact.Parsed{}
	}
	if evidence.hasPlan && evidence.plan.Document.Planner == nil {
		out := plannerOutput(false)
		evidence.providerResults[out.ProviderResult.AttemptID] = out.Parsed
		evidence.plan.Document = store.PlanDocument{Planner: out.Parsed.Planner, ProviderResult: &out.ProviderResult, Acceptance: out.Parsed.Planner.Acceptance, ProofKind: string(out.Parsed.Planner.Proof.Kind), Paths: out.Parsed.Planner.Paths, Commands: out.Parsed.Planner.Commands, Risks: out.Parsed.Planner.Risks}
		evidence.plan.Digest, evidence.plan.TicketVersion, evidence.plan.Fence = "plan", evidence.ticket.Version, testFence
	}
	if runner != nil {
		runner.evidence = evidence
	}
	return Worker{Evidence: evidence, Engine: engine, Runner: runner, Candidate: fakeCandidate{}}
}

type fakeCandidate struct{}

func (fakeCandidate) AuthenticateCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, CandidateWitness) error {
	return nil
}

func TestPlannerPassAndQuestions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		questions bool
		want      domain.State
	}{{"pass", false, domain.StateVerifying}, {"questions", true, domain.StatePaused}} {
		t.Run(tc.name, func(t *testing.T) {
			e := &fakeEvidence{}
			eng := &fakeEngine{}
			run := &fakeRunner{outputs: []PhaseResult{plannerOutput(tc.questions)}}
			w := newWorker(domain.StatePlanning, run, e, eng)
			got, err := w.Run(context.Background(), testRef, testFence)
			if err != nil || eng.state.State != tc.want {
				t.Fatalf("run=%+v err=%v state=%s", got, err, eng.state.State)
			}
			if tc.questions && e.plans != 0 {
				t.Fatal("questions must not record an executable plan")
			}
			if !tc.questions && e.plans != 1 {
				t.Fatalf("plans=%d", e.plans)
			}
		})
	}
}

func TestVerificationPassAndBuilderAmendmentAndPass(t *testing.T) {
	e := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}}
	eng := &fakeEngine{}
	verification := &fakeRunner{outputs: []PhaseResult{verificationOutput()}}
	w := newWorker(domain.StateVerifying, verification, e, eng)
	w.Checkpoint = fakeCheckpoint{}
	if _, err := w.Run(context.Background(), testRef, testFence); err != nil {
		t.Fatal(err)
	}
	if eng.state.State != domain.StateBuilding || e.verifications != 1 {
		t.Fatalf("state=%s verifications=%d", eng.state.State, e.verifications)
	}
	builder := &fakeRunner{outputs: []PhaseResult{builderOutput(true)}}
	builder.evidence = e
	w.Runner = builder
	if _, err := w.Run(context.Background(), testRef, testFence); !errors.Is(err, ErrAmendmentUnsupported) {
		t.Fatalf("amend err=%v", err)
	}
	if eng.state.State != domain.StateBuilding || e.budget != 0 {
		t.Fatalf("amend state=%s budget=%d", eng.state.State, e.budget)
	}
	builder.outputs = []PhaseResult{builderOutput(false)}
	w.Runner = builder
	builder.outputs[0].Candidate = &CandidateWitness{Commit: store.CommitObservation{CommitOID: oid, ParentOID: oid, TreeOID: oid}, CommandPolicyDigest: digest, Reason: "candidate"}
	if _, err := w.Run(context.Background(), testRef, testFence); err != nil {
		t.Fatal(err)
	}
	if eng.state.State != domain.StatePublishing || e.candidates != 1 {
		t.Fatalf("pass state=%s candidates=%d", eng.state.State, e.candidates)
	}
}

func TestStaleFenceAndCancellation(t *testing.T) {
	e := &fakeEvidence{}
	eng := &fakeEngine{}
	run := &fakeRunner{outputs: []PhaseResult{plannerOutput(false)}}
	w := newWorker(domain.StatePlanning, run, e, eng)
	if _, err := w.Run(context.Background(), testRef, domain.Fence{LeaderEpoch: 7, RunnerEpoch: 99}); !errors.Is(err, store.ErrStaleFence) {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.Run(ctx, testRef, testFence); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancel err=%v", err)
	}
	if len(run.requests) != 0 {
		t.Fatal("canceled worker invoked a phase")
	}
}

func TestVerificationRejectsCheckpointIDThatDiffersFromCommit(t *testing.T) {
	e := &fakeEvidence{hasPlan: true}
	eng := &fakeEngine{}
	out := verificationOutput()
	out.Checkpoint.ID = "abcdefabcdefabcdefabcdefabcdefabcdefabcd"
	run := &fakeRunner{outputs: []PhaseResult{out}}
	w := newWorker(domain.StateVerifying, run, e, eng)
	w.Checkpoint = fakeCheckpoint{}
	if _, err := w.Run(context.Background(), testRef, testFence); !errors.Is(err, ErrCheckpointRequired) {
		t.Fatalf("checkpoint mismatch err=%v", err)
	}
	if eng.signals != 0 {
		t.Fatal("mismatched checkpoint advanced workflow")
	}
}

func TestEvidenceBeforeTransitionIsReplayed(t *testing.T) {
	e := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Digest: "plan", TicketVersion: 1, Fence: testFence, Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}}
	eng := &fakeEngine{}
	w := newWorker(domain.StatePlanning, nil, e, eng)
	got, err := w.Run(context.Background(), testRef, testFence)
	if err != nil || !got.Replayed || !got.Transitioned {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if eng.signals != 1 {
		t.Fatalf("signals=%d", eng.signals)
	}
}

func TestBuilderStopsWithoutAuthenticatedCandidate(t *testing.T) {
	e := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}, hasVerify: true, verification: store.StoredVerification{Revision: store.VerificationRevision{IntentDigest: digest, ProofDigest: digest, OwnedFiles: []string{"internal"}, CheckpointID: oid}}}
	eng := &fakeEngine{}
	run := &fakeRunner{outputs: []PhaseResult{builderOutput(false)}}
	w := newWorker(domain.StateBuilding, run, e, eng)
	if _, err := w.Run(context.Background(), testRef, testFence); !errors.Is(err, ErrStaleEvidence) {
		t.Fatalf("err=%v", err)
	}
	if eng.signals != 0 {
		t.Fatal("builder advanced without a candidate")
	}
}

func TestCurrentCandidateReplayAndStaleCandidateRefusal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		version    uint64
		fence      domain.Fence
		wantReplay bool
	}{
		// Builder result identity is intentionally non-reusable.  A recovered
		// candidate must block rather than adopt a generation it cannot bind to
		// an exact Builder result under the current fence.
		{name: "current", version: 1, fence: testFence},
		{name: "stale-version", version: 2, fence: testFence},
		{name: "stale-runner", version: 1, fence: domain.Fence{LeaderEpoch: 7, RunnerEpoch: 9}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &fakeEvidence{hasPlan: true, hasVerify: true, verification: store.StoredVerification{Revision: store.VerificationRevision{IntentDigest: digest, ProofDigest: digest, OwnedFiles: []string{"internal"}, CheckpointID: oid}}}
			e.ticket = ticket(domain.StateBuilding)
			e.candidate = store.StoredCandidate{TicketVersion: tc.version, Fence: tc.fence}
			e.hasCandidate = true
			eng := &fakeEngine{state: &e.ticket}
			w := Worker{Evidence: e, Engine: eng}
			got, err := w.Run(context.Background(), testRef, testFence)
			if tc.wantReplay {
				if err != nil || !got.Replayed || eng.signals != 1 {
					t.Fatalf("got=%+v err=%v signals=%d", got, err, eng.signals)
				}
			} else if !errors.Is(err, ErrStaleEvidence) || eng.signals != 0 {
				t.Fatalf("got=%+v err=%v signals=%d", got, err, eng.signals)
			}
		})
	}
}

func TestStalePlanAndVerificationNeverReplay(t *testing.T) {
	e := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Digest: "plan", TicketVersion: 99, Fence: testFence}, hasVerify: true, verification: store.StoredVerification{TicketVersion: 99, Fence: testFence}}
	e.ticket = ticket(domain.StatePlanning)
	eng := &fakeEngine{}
	if _, err := (Worker{Evidence: e, Engine: eng}).Run(context.Background(), testRef, testFence); !errors.Is(err, ErrStaleEvidence) {
		t.Fatalf("stale plan err=%v", err)
	}
	e.hasPlan = true
	e.plan.TicketVersion = 1
	e.plan.Fence = testFence
	e.ticket = ticket(domain.StateVerifying)
	eng.state = &e.ticket
	if _, err := (Worker{Evidence: e, Engine: eng}).Run(context.Background(), testRef, testFence); !errors.Is(err, ErrStaleEvidence) {
		t.Fatalf("stale verification err=%v", err)
	}
}

func TestWorkerRequiresValidReference(t *testing.T) {
	w := Worker{}
	if _, err := w.Run(context.Background(), domain.TicketRef{}, testFence); err == nil {
		t.Fatal("expected invalid reference")
	}
	_ = fmt.Sprint(w)
}
