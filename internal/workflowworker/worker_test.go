package workflowworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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
	ticket             store.Ticket
	plan               store.StoredPlan
	verification       store.StoredVerification
	candidate          store.StoredCandidate
	hasPlan            bool
	hasVerify          bool
	hasCandidate       bool
	budget             int
	plans              int
	verifications      int
	amendment          *store.VerificationAmendment
	amendmentDecision  store.VerificationAmendmentDecision
	amendmentErr       error
	reusableHistorical bool
	reusableRecovered  bool
	recoveredVersion   bool
	candidates         int
	providerResults    map[int64]phaseartifact.Parsed
}

// sourceResumeEvidence keeps the retained reviewer projection available for
// source-commit authentication while exposing the exact resume proof that
// lets Worker distinguish it from a freshly completed reviewer result.
type sourceResumeEvidence struct {
	*fakeEvidence
	proof store.OperatorSourceResumeProof
}

func (f *sourceResumeEvidence) OperatorSourceResumeRequiresFreshVerification(context.Context, domain.TicketRef, uint64) (bool, error) {
	return true, nil
}

func (f *sourceResumeEvidence) OperatorSourceResumeProof(context.Context, domain.TicketRef, uint64, domain.Fence) (store.OperatorSourceResumeProof, bool, error) {
	return f.proof, true, nil
}

// Plan supplies the same immutable Planner/provider binding that Store's
// source-resume projection retains.  These focused Worker tests exercise the
// source-specific Reviewer selection, so their prior hand-written plan shell
// must not bypass the production storedPlanIdentity authentication.
func (f *sourceResumeEvidence) Plan(ctx context.Context, ref domain.TicketRef) (store.StoredPlan, error) {
	plan, err := f.fakeEvidence.Plan(ctx, ref)
	if err != nil || (plan.Document.Planner != nil && plan.Document.ProviderResult != nil && plan.Digest != "") {
		return plan, err
	}
	out := plannerOutput(false)
	if f.providerResults == nil {
		f.providerResults = map[int64]phaseartifact.Parsed{}
	}
	f.providerResults[out.result.ProviderResult.AttemptID] = out.parsed
	plan.Document = store.PlanDocument{
		Planner:        out.parsed.Planner,
		ProviderResult: &out.result.ProviderResult,
		Acceptance:     out.parsed.Planner.Acceptance,
		ProofKind:      string(out.parsed.Planner.Proof.Kind),
		Paths:          out.parsed.Planner.Paths,
		Commands:       out.parsed.Planner.Commands,
		Risks:          out.parsed.Planner.Risks,
	}
	plan.Digest = "source-resume-plan"
	plan.TicketVersion = f.ticket.Version
	plan.Fence = testFence
	f.plan = plan
	return plan, nil
}

func (f *sourceResumeEvidence) RecordVerification(ctx context.Context, artifact store.VerificationArtifact) (store.VerificationRevision, error) {
	revision, err := f.fakeEvidence.RecordVerification(ctx, artifact)
	if err != nil {
		return store.VerificationRevision{}, err
	}
	if revision.Revision <= f.proof.Verification.Revision.Revision {
		revision.Revision = f.proof.Verification.Revision.Revision + 1
		f.verification.Revision.Revision = revision.Revision
	}
	return revision, nil
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
func (f *fakeEvidence) RecoverableVerification(ctx context.Context, ref domain.TicketRef) (store.StoredVerification, error) {
	return f.CurrentVerification(ctx, ref)
}
func (f *fakeEvidence) LatestCandidate(context.Context, domain.TicketRef) (store.StoredCandidate, error) {
	if !f.hasCandidate {
		return store.StoredCandidate{}, store.ErrNotFound
	}
	return f.candidate, nil
}
func (f *fakeEvidence) RecoverableCandidate(ctx context.Context, ref domain.TicketRef) (store.StoredCandidate, error) {
	return f.LatestCandidate(ctx, ref)
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
	return store.ProviderAttemptResult{Claim: store.ProviderAttemptClaim{ID: key.AttemptID, Attempt: key.Attempt, Ref: testRef, Phase: key.Phase, ExpectedVersion: f.ticket.Version, LeaderEpoch: testFence.LeaderEpoch, RunnerEpoch: testFence.RunnerEpoch, Role: map[domain.Phase]string{domain.PhasePlanning: "planner", domain.PhaseVerification: "reviewer", domain.PhaseBuild: "builder", domain.PhaseReview: "reviewer"}[key.Phase]}}, p, nil
}
func (f *fakeEvidence) LoadHistoricalProviderAttemptResult(ctx context.Context, key store.ProviderAttemptResultKey) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	return f.LoadCurrentProviderAttemptResult(ctx, key, f.ticket.Version, testFence)
}
func (f *fakeEvidence) ProviderResultReachesFence(context.Context, store.ProviderAttemptResultKey, uint64, domain.Fence) error {
	return nil
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
			key := store.ProviderAttemptResultKey{AttemptID: id, Ref: testRef, Phase: request.Phase, Attempt: 1}
			result, _, err := f.LoadCurrentProviderAttemptResult(context.Background(), key, request.ExpectedVersion, request.Fence)
			if err != nil {
				return store.LatestReusableProviderAttemptResult{}, err
			}
			if f.reusableHistorical {
				result.Claim.ExpectedVersion = request.ExpectedVersion - 1
			}
			if f.reusableRecovered {
				result.Claim.LeaderEpoch--
			}
			if f.recoveredVersion {
				result.Claim.ExpectedVersion--
			}
			return store.LatestReusableProviderAttemptResult{Key: key, Result: result, Parsed: p, Recovered: f.reusableRecovered}, nil
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
	f.verification = store.StoredVerification{Revision: store.VerificationRevision{Revision: 1, IntentDigest: hex.EncodeToString(ih[:]), ProofDigest: hex.EncodeToString(ph[:]), OwnedFiles: a.OwnedFiles, CheckpointID: a.CheckpointID}, TicketVersion: a.ExpectedVersion, Fence: a.Fence, Intent: a.Intent, Proof: a.Proof, Checkpoint: a.Checkpoint}
	if a.ProviderResult != nil {
		f.verification.ProviderResult = *a.ProviderResult
	}
	return f.verification.Revision, nil
}
func (f *fakeEvidence) RecordCandidate(_ context.Context, a store.CandidateEvidence) ([]store.InvalidationReceipt, error) {
	f.hasCandidate, f.candidates = true, f.candidates+1
	f.candidate = store.StoredCandidate{Snapshot: a.Snapshot, TicketVersion: a.ExpectedVersion, Fence: a.Fence}
	return nil, nil
}
func (f *fakeEvidence) PendingVerificationAmendment(_ context.Context, _ domain.TicketRef, _ uint64, _ domain.Fence) (store.VerificationAmendment, error) {
	if f.amendment == nil {
		return store.VerificationAmendment{}, store.ErrNotFound
	}
	return *f.amendment, nil
}
func (f *fakeEvidence) VerificationAmendmentDecision(_ context.Context, _ domain.TicketRef, _ uint64, _ domain.Fence, _ store.ProviderAttemptResultKey) (store.VerificationAmendmentDecision, error) {
	if f.amendmentErr != nil {
		return "", f.amendmentErr
	}
	if f.amendment == nil {
		return "", store.ErrNotFound
	}
	if f.amendmentDecision != "" {
		return f.amendmentDecision, nil
	}
	return store.VerificationAmendmentAccepted, nil
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
	err     error
	signals int
}

func (e *fakeEngine) SignalProviderExhausted(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	return e.Signal(ctx, req)
}

func (e *fakeEngine) SignalCandidate(ctx context.Context, req contracts.SignalRequest, _ domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalFinalReview(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	if e.state.Type == domain.TicketSpike {
		req.Attributes = map[string]string{"ticket_type_spike": "true", "report_present": "true", "no_merge_effect": "true"}
	} else if e.state.MergeMode == domain.MergeGuarded {
		req.Attributes = map[string]string{"ticket_type_not_spike": "true", "merge_mode_guarded": "true", "all_nonapproval_gates_green": "true"}
	} else {
		req.Attributes = map[string]string{"ticket_type_not_spike": "true", "merge_mode_manual": "true", "all_nonapproval_gates_green": "true"}
	}
	return e.Signal(ctx, req)
}

func (e *fakeEngine) SignalFinalReviewRepair(ctx context.Context, req contracts.SignalRequest, owner string) (contracts.TransitionResult, error) {
	req.Attributes = map[string]string{"correction_available": "true"}
	if owner == "builder" {
		req.Attributes["repair_owner_builder"] = "true"
	} else {
		req.Attributes["repair_owner_verification"] = "true"
	}
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalFinalReviewNeedsOperator(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	req.Trigger = "typed_blocker"
	req.EventPayload = `{"code":"review_needs_operator"}`
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalPlan(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalVerification(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalVerificationAmendment(ctx context.Context, req contracts.SignalRequest, decision store.VerificationAmendmentDecision, _ store.ProviderAttemptResultKey) (contracts.TransitionResult, error) {
	if decision == store.VerificationAmendmentAccepted {
		req.Trigger = "amendment_accepted"
	} else {
		req.Trigger = "amendment_rejected"
	}
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalVerificationAmendmentRequest(ctx context.Context, req contracts.SignalRequest, _ store.ProviderAttemptResultKey) (contracts.TransitionResult, error) {
	req.Trigger = "verification_amendment_requested"
	return e.Signal(ctx, req)
}
func (e *fakeEngine) SignalVerificationAmendmentBlocked(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	req.Trigger = "typed_blocker"
	return e.Signal(ctx, req)
}

func (e *fakeEngine) Signal(_ context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	e.signals++
	e.last = req
	if e.stale {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	if e.err != nil {
		return contracts.TransitionResult{}, e.err
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
	case "review_pass":
		if req.From == domain.StateReviewing {
			if req.Attributes["ticket_type_spike"] == "true" {
				e.state.State = domain.StateDone
			} else if req.Attributes["merge_mode_guarded"] == "true" {
				e.state.State = domain.StateWaitingApproval
			} else {
				e.state.State = domain.StateWaitingManualMerge
			}
		}
	case "review_repair":
		if req.Attributes["repair_owner_builder"] == "true" {
			e.state.State = domain.StateBuilding
		} else {
			e.state.State = domain.StateVerifying
		}
	case "typed_blocker":
		e.state.State = domain.StateBlocked
	case "needs_operator_input":
		e.state.State = domain.StatePaused
	case "verification_amendment_requested":
		e.state.State = domain.StateVerifying
	case "amendment_accepted", "amendment_rejected":
		e.state.State = domain.StateBuilding
	case "retry_or_correction_exhausted":
		e.state.State = domain.StatePaused
	}
	e.state.Version++
	return contracts.TransitionResult{To: e.state.State, TicketVersion: e.state.Version}, nil
}

type fakeRunner struct {
	outputs  []fakePhase
	requests []PhaseRequest
	evidence *fakeEvidence
	err      error
}
type fakePhase struct {
	result PhaseResult
	parsed phaseartifact.Parsed
}

type fakeCheckpoint struct{}

func (fakeCheckpoint) AuthenticateVerificationCheckpoint(context.Context, PhaseRequest, phaseartifact.Verification, VerificationCheckpoint) error {
	return nil
}

type fakeCheckpointMaterializer struct{}

func (fakeCheckpointMaterializer) MaterializeVerificationCheckpoint(_ context.Context, _ PhaseRequest, _ phaseartifact.Verification, _ store.ProviderAttemptResultKey) (VerificationCheckpoint, error) {
	return VerificationCheckpoint{ID: oid, Commit: store.CommitObservation{CommitOID: oid, ParentOID: oid, TreeOID: oid}, CommandResult: contracts.RepositoryCommandResultKey{SemanticKey: "command", ClaimEpoch: 1}}, nil
}

func (r *fakeRunner) Run(_ context.Context, req PhaseRequest) (PhaseResult, error) {
	r.requests = append(r.requests, req)
	if r.err != nil {
		return PhaseResult{}, r.err
	}
	if len(r.outputs) == 0 {
		return PhaseResult{}, errors.New("no scripted output")
	}
	out := r.outputs[0]
	r.outputs = r.outputs[1:]
	if r.evidence != nil {
		if r.evidence.providerResults == nil {
			r.evidence.providerResults = map[int64]phaseartifact.Parsed{}
		}
		r.evidence.providerResults[out.result.ProviderResult.AttemptID] = out.parsed
	}
	return out.result, nil
}

const oid = "0123456789abcdef0123456789abcdef01234567"
const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func ticket(state domain.State) store.Ticket {
	return store.Ticket{Ref: testRef, State: state, Version: 1, RunnerEpoch: 1, Type: domain.TicketBug, SourceDigest: digest}
}
func provider() domain.ProviderIdentity {
	return domain.ProviderIdentity{Provider: "codex", Model: "test", Family: "test", Version: "1"}
}
func plannerOutput(questions bool) fakePhase {
	p := &phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"accept"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofRegression, Command: []string{"go", "test", "./..."}, Details: "proof"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"risk"}}
	if questions {
		p.Questions = []phaseartifact.Question{{Prompt: "scope?", Options: []string{"a", "b"}}}
	}
	return fakePhase{parsed: phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: provider(), Planner: p}, result: PhaseResult{ProviderResult: store.ProviderAttemptResultKey{AttemptID: 1, Ref: testRef, Phase: domain.PhasePlanning, Attempt: 1}}}
}
func verificationOutput() fakePhase {
	v := &phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: digest, ProofKind: phaseartifact.ProofRegression, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: digest}
	return fakePhase{parsed: phaseartifact.Parsed{Phase: domain.PhaseVerification, Provider: provider(), Verify: v}, result: PhaseResult{ProviderResult: store.ProviderAttemptResultKey{AttemptID: 2, Ref: testRef, Phase: domain.PhaseVerification, Attempt: 1}}}
}
func builderOutput(amend bool) fakePhase {
	b := &phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "implemented", ChangedFiles: []string{"internal/foo.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	if amend {
		b.AmendmentRequest = &phaseartifact.AmendmentRequest{OldProofDigest: digest, ProposedDigest: strings.Repeat("b", 64), ProposedCommand: []string{"go", "test", "./..."}, Reason: "proof needs clarification"}
	}
	return fakePhase{parsed: phaseartifact.Parsed{Phase: domain.PhaseBuild, Provider: provider(), Builder: b}, result: PhaseResult{ProviderResult: store.ProviderAttemptResultKey{AttemptID: 3, Ref: testRef, Phase: domain.PhaseBuild, Attempt: 1}}}
}
func finalReviewOutput() fakePhase {
	return finalReviewOutcome(phaseartifact.ReviewPass, "")
}
func finalReviewOutcome(decision phaseartifact.ReviewDecision, owner string) fakePhase {
	r := &phaseartifact.Reviewer{Schema: "sf.reviewer/v1", Decision: decision, RepairOwner: owner, ReviewedHead: oid, ProofDigest: digest}
	if decision != phaseartifact.ReviewPass {
		r.Findings = []string{"fix exact finding"}
	}
	return fakePhase{parsed: phaseartifact.Parsed{Phase: domain.PhaseReview, Provider: provider(), Reviewer: r}, result: PhaseResult{ProviderResult: store.ProviderAttemptResultKey{AttemptID: 4, Ref: testRef, Phase: domain.PhaseReview, Attempt: 1}}}
}

func newWorker(state domain.State, runner *fakeRunner, evidence *fakeEvidence, engine *fakeEngine) Worker {
	evidence.ticket = ticket(state)
	engine.state = &evidence.ticket
	if evidence.providerResults == nil {
		evidence.providerResults = map[int64]phaseartifact.Parsed{}
	}
	if evidence.hasPlan && evidence.plan.Document.Planner == nil {
		out := plannerOutput(false)
		evidence.providerResults[out.result.ProviderResult.AttemptID] = out.parsed
		evidence.plan.Document = store.PlanDocument{Planner: out.parsed.Planner, ProviderResult: &out.result.ProviderResult, Acceptance: out.parsed.Planner.Acceptance, ProofKind: string(out.parsed.Planner.Proof.Kind), Paths: out.parsed.Planner.Paths, Commands: out.parsed.Planner.Commands, Risks: out.parsed.Planner.Risks}
		evidence.plan.Digest, evidence.plan.TicketVersion, evidence.plan.Fence = "plan", evidence.ticket.Version, testFence
	}
	if runner != nil {
		runner.evidence = evidence
	}
	return Worker{Evidence: evidence, Engine: engine, Runner: runner, Candidate: fakeCandidate{}, CheckpointMaterializer: fakeCheckpointMaterializer{}, CandidateMaterializer: fakeCandidateMaterializer{}}
}

type fakeCandidate struct{}

func (fakeCandidate) AuthenticateCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, CandidateWitness) error {
	return nil
}

type fakeCandidateMaterializer struct{}

func (fakeCandidateMaterializer) MaterializeCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, store.ProviderAttemptResultKey) (CandidateWitness, error) {
	return CandidateWitness{Commit: store.CommitObservation{CommitOID: oid, ParentOID: oid, TreeOID: oid}, CommandPolicyDigest: digest, Reason: "candidate", CommandResult: contracts.RepositoryCommandResultKey{SemanticKey: "command", ClaimEpoch: 1}}, nil
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
			run := &fakeRunner{outputs: []fakePhase{plannerOutput(tc.questions)}}
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

func TestProviderAttemptWindowExhaustionPausesThroughTypedEngineBoundary(t *testing.T) {
	evidence := &fakeEvidence{}
	engine := &fakeEngine{}
	worker := newWorker(domain.StatePlanning, &fakeRunner{err: ErrProviderAttemptExhausted}, evidence, engine)
	got, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !got.Transitioned || got.State != domain.StatePaused || engine.last.Trigger != "retry_or_correction_exhausted" || engine.signals != 1 {
		t.Fatalf("run=%+v err=%v signal=%+v calls=%d", got, err, engine.last, engine.signals)
	}
}

func TestTicketBudgetExhaustionBlocksOnceAndCannotBecomeProviderRetry(t *testing.T) {
	evidence := &fakeEvidence{}
	engine := &fakeEngine{}
	worker := newWorker(domain.StatePlanning, &fakeRunner{err: ErrTicketBudgetExhausted}, evidence, engine)
	got, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !got.Transitioned || got.State != domain.StateBlocked || engine.last.Trigger != "typed_blocker" || !strings.Contains(engine.last.EventPayload, `"code":"ticket_budget_exhausted"`) || engine.signals != 1 {
		t.Fatalf("run=%+v err=%v signal=%+v calls=%d", got, err, engine.last, engine.signals)
	}
	if replay, replayErr := worker.Run(context.Background(), testRef, testFence); replayErr != nil || replay.Transitioned || replay.State != domain.StateBlocked || engine.signals != 1 {
		t.Fatalf("blocked replay=%+v err=%v signals=%d", replay, replayErr, engine.signals)
	}
}

func TestFinalReviewPassTransitionsOnlyToModeWaitingStateAndReplays(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode domain.MergeMode
		want domain.State
	}{{"guarded", domain.MergeGuarded, domain.StateWaitingApproval}, {"manual", domain.MergeManual, domain.StateWaitingManualMerge}} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := &fakeEvidence{hasCandidate: true, candidate: store.StoredCandidate{Snapshot: domain.CandidateSnapshot{Generation: 1, BaseSHA: oid, HeadSHA: oid, TreeSHA: oid, SourceDigest: digest, VerificationIntentDigest: digest, ProofDigest: digest, CommandPolicyDigest: digest, BuilderEvidenceDigest: digest}, TicketVersion: 1, Fence: testFence}}
			engine := &fakeEngine{}
			runner := &fakeRunner{outputs: []fakePhase{finalReviewOutput()}}
			worker := newWorker(domain.StateReviewing, runner, evidence, engine)
			evidence.ticket.MergeMode = tc.mode
			if got, err := worker.Run(context.Background(), testRef, testFence); err != nil || !got.Transitioned || got.Replayed || engine.state.State != tc.want {
				t.Fatalf("fresh run=%+v err=%v state=%s", got, err, engine.state.State)
			}

			// Recreate the reviewing snapshot as it would appear after a crash
			// before SignalFinalReview's response. The immutable attempt is
			// reused; no second provider call is allowed.
			evidence.ticket.State, evidence.ticket.Version = domain.StateReviewing, 1
			engine.state = &evidence.ticket
			if got, err := worker.Run(context.Background(), testRef, testFence); err != nil || !got.Transitioned || !got.Replayed || len(runner.requests) != 1 || engine.state.State != tc.want {
				t.Fatalf("replay=%+v err=%v calls=%d state=%s", got, err, len(runner.requests), engine.state.State)
			}
		})
	}
}

func TestFinalReviewRepairOperatorAndSpikeOutcomesAreTyped(t *testing.T) {
	for _, tc := range []struct {
		name       string
		decision   phaseartifact.ReviewDecision
		owner      string
		ticketType domain.TicketType
		mergeMode  domain.MergeMode
		want       domain.State
	}{
		{name: "builder repair", decision: phaseartifact.ReviewRepair, owner: "builder", ticketType: domain.TicketFeature, mergeMode: domain.MergeGuarded, want: domain.StateBuilding},
		{name: "reviewer repair", decision: phaseartifact.ReviewRepair, owner: "reviewer", ticketType: domain.TicketFeature, mergeMode: domain.MergeGuarded, want: domain.StateVerifying},
		{name: "operator escalation", decision: phaseartifact.ReviewNeedsOperator, owner: "operator", ticketType: domain.TicketFeature, mergeMode: domain.MergeGuarded, want: domain.StateBlocked},
		{name: "spike report only", decision: phaseartifact.ReviewPass, ticketType: domain.TicketSpike, mergeMode: domain.MergeManual, want: domain.StateDone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := &fakeEvidence{hasCandidate: true, candidate: store.StoredCandidate{Snapshot: domain.CandidateSnapshot{Generation: 1, BaseSHA: oid, HeadSHA: oid, TreeSHA: oid, SourceDigest: digest, VerificationIntentDigest: digest, ProofDigest: digest, CommandPolicyDigest: digest, BuilderEvidenceDigest: digest}, TicketVersion: 1, Fence: testFence}}
			engine := &fakeEngine{}
			worker := newWorker(domain.StateReviewing, &fakeRunner{outputs: []fakePhase{finalReviewOutcome(tc.decision, tc.owner)}}, evidence, engine)
			evidence.ticket.Type, evidence.ticket.MergeMode = tc.ticketType, tc.mergeMode
			if got, err := worker.Run(context.Background(), testRef, testFence); err != nil || !got.Transitioned || engine.state.State != tc.want {
				t.Fatalf("run=%+v err=%v state=%s", got, err, engine.state.State)
			}
		})
	}

	// Autonomous review cannot invoke or replay a provider result until the
	// separately qualified merge authority is composed.
	evidence := &fakeEvidence{hasCandidate: true, candidate: store.StoredCandidate{Snapshot: domain.CandidateSnapshot{Generation: 1, BaseSHA: oid, HeadSHA: oid, TreeSHA: oid, SourceDigest: digest, VerificationIntentDigest: digest, ProofDigest: digest, CommandPolicyDigest: digest, BuilderEvidenceDigest: digest}, TicketVersion: 1, Fence: testFence}}
	engine := &fakeEngine{}
	runner := &fakeRunner{outputs: []fakePhase{finalReviewOutput()}}
	worker := newWorker(domain.StateReviewing, runner, evidence, engine)
	evidence.ticket.MergeMode = domain.MergeAutonomous
	if _, err := worker.Run(context.Background(), testRef, testFence); !errors.Is(err, ErrUnsupportedState) || len(runner.requests) != 0 || engine.signals != 0 {
		t.Fatalf("autonomous review err=%v calls=%d signals=%d", err, len(runner.requests), engine.signals)
	}
}

func TestCurrentPlanForCandidateReplayReloadsExactDurablePlan(t *testing.T) {
	evidence := &fakeEvidence{hasPlan: true}
	engine := &fakeEngine{}
	worker := newWorker(domain.StateBuilding, nil, evidence, engine)
	plan, err := worker.storedPlanIdentity(context.Background(), evidence.ticket, evidence.plan, testFence)
	if err != nil {
		t.Fatalf("stored plan identity: %v", err)
	}
	got, err := worker.currentPlanForCandidateReplay(context.Background(), evidence.ticket, testFence, plan)
	if err != nil || got.Document.ProviderResult == nil || *got.Document.ProviderResult != *evidence.plan.Document.ProviderResult {
		t.Fatalf("reloaded plan=%+v err=%v", got, err)
	}

	// The passed identity is only a comparison witness. A replay must reject a
	// Store plan whose bound Planner artifact no longer matches it.
	tampered := *evidence.plan.Document.Planner
	tampered.Paths = []string{"other"}
	evidence.plan.Document.Planner = &tampered
	if _, err := worker.currentPlanForCandidateReplay(context.Background(), evidence.ticket, testFence, plan); !errors.Is(err, ErrStaleEvidence) {
		t.Fatalf("tampered durable plan err=%v, want stale evidence", err)
	}
	evidence.hasPlan = false
	if _, err := worker.currentPlanForCandidateReplay(context.Background(), evidence.ticket, testFence, plan); !errors.Is(err, ErrStaleEvidence) {
		t.Fatalf("missing durable plan err=%v, want stale evidence", err)
	}
}

func TestVerificationPassAndBuilderAmendmentAndPass(t *testing.T) {
	e := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}}
	eng := &fakeEngine{}
	verification := &fakeRunner{outputs: []fakePhase{verificationOutput()}}
	w := newWorker(domain.StateVerifying, verification, e, eng)
	w.Checkpoint = fakeCheckpoint{}
	if _, err := w.Run(context.Background(), testRef, testFence); err != nil {
		t.Fatal(err)
	}
	if eng.state.State != domain.StateBuilding || e.verifications != 1 {
		t.Fatalf("state=%s verifications=%d", eng.state.State, e.verifications)
	}
	builder := &fakeRunner{outputs: []fakePhase{builderOutput(true)}}
	builder.evidence = e
	w.Runner = builder
	if _, err := w.Run(context.Background(), testRef, testFence); err != nil {
		t.Fatalf("amend err=%v", err)
	}
	if eng.state.State != domain.StateVerifying || e.budget != 0 {
		t.Fatalf("amend state=%s budget=%d", eng.state.State, e.budget)
	}
}

func TestSourceResumeMaterializesFreshReusableReviewerAfterResultBeforeRecord(t *testing.T) {
	base := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}, hasVerify: true, providerResults: map[int64]phaseartifact.Parsed{2: verificationOutput().parsed}}
	base.ticket = ticket(domain.StateVerifying)
	base.ticket.Version = 2
	// This is the retained old revision.  It must not be replayed, but its
	// presence also must not hide the completed reviewer result below.
	base.verification = store.StoredVerification{Revision: store.VerificationRevision{Revision: 1}, TicketVersion: 1, Fence: testFence}
	evidence := &sourceResumeEvidence{fakeEvidence: base, proof: store.OperatorSourceResumeProof{Ref: testRef, Version: 2, Fence: testFence, Verification: base.verification}}
	engine := &fakeEngine{state: &base.ticket}
	worker := Worker{Evidence: evidence, Engine: engine, Checkpoint: fakeCheckpoint{}, CheckpointMaterializer: fakeCheckpointMaterializer{}}
	result, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !result.Transitioned || !result.Replayed || engine.state.State != domain.StateBuilding || base.verifications != 1 {
		t.Fatalf("result=%+v err=%v state=%s records=%d", result, err, engine.state.State, base.verifications)
	}
}

func TestSourceResumeLaunchesFreshReviewerAfterCrashBeforeReviewerClaim(t *testing.T) {
	retainedKey := store.ProviderAttemptResultKey{AttemptID: 1, Ref: testRef, Phase: domain.PhaseVerification, Attempt: 1}
	base := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}, providerResults: map[int64]phaseartifact.Parsed{retainedKey.AttemptID: verificationOutput().parsed}}
	base.ticket = ticket(domain.StateVerifying)
	base.ticket.Version = 2
	retained := store.StoredVerification{Revision: store.VerificationRevision{Revision: 1}, TicketVersion: 1, Fence: testFence, ProviderResult: retainedKey}
	evidence := &sourceResumeEvidence{fakeEvidence: base, proof: store.OperatorSourceResumeProof{Ref: testRef, Version: 2, Fence: testFence, Verification: retained}}
	engine := &fakeEngine{state: &base.ticket}
	runner := &fakeRunner{evidence: base}
	worker := Worker{Evidence: evidence, Engine: engine, Runner: runner, Checkpoint: fakeCheckpoint{}, CheckpointMaterializer: fakeCheckpointMaterializer{}}

	if _, err := worker.Run(context.Background(), testRef, testFence); err == nil || !strings.Contains(err.Error(), "no scripted output") {
		t.Fatalf("pre-claim crash=%v", err)
	}
	if len(runner.requests) != 1 || base.verifications != 0 {
		t.Fatalf("pre-claim crash recorded unexpected evidence: calls=%d records=%d", len(runner.requests), base.verifications)
	}

	runner.outputs = []fakePhase{verificationOutput()}
	result, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !result.Transitioned || result.Replayed || len(runner.requests) != 2 || base.verifications != 1 || engine.state.State != domain.StateBuilding {
		t.Fatalf("fresh reviewer after pre-claim crash=%+v err=%v calls=%d records=%d state=%s", result, err, len(runner.requests), base.verifications, engine.state.State)
	}
}

func TestSourceResumeDoesNotRerunFreshReviewerAfterRecordBeforeTransitionCrash(t *testing.T) {
	base := &fakeEvidence{hasPlan: true, plan: store.StoredPlan{Document: store.PlanDocument{Acceptance: []string{"accept"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}}}}
	base.ticket = ticket(domain.StateVerifying)
	base.ticket.Version = 2
	retained := store.StoredVerification{Revision: store.VerificationRevision{Revision: 1}, TicketVersion: 1, Fence: testFence}
	evidence := &sourceResumeEvidence{fakeEvidence: base, proof: store.OperatorSourceResumeProof{Ref: testRef, Version: 2, Fence: testFence, Verification: retained}}
	engine := &fakeEngine{state: &base.ticket, err: errors.New("crash after RecordVerification")}
	runner := &fakeRunner{outputs: []fakePhase{verificationOutput()}, evidence: base}
	worker := Worker{Evidence: evidence, Engine: engine, Runner: runner, Checkpoint: fakeCheckpoint{}, CheckpointMaterializer: fakeCheckpointMaterializer{}}

	if _, err := worker.Run(context.Background(), testRef, testFence); err == nil || !strings.Contains(err.Error(), "crash after RecordVerification") {
		t.Fatalf("first source-resume record/transition crash=%v", err)
	}
	if len(runner.requests) != 1 || base.verifications != 1 || !base.hasVerify || base.verification.Revision.Revision != 2 {
		t.Fatalf("fresh verification was not recorded once: calls=%d records=%d verification=%+v", len(runner.requests), base.verifications, base.verification)
	}

	engine.err = nil
	result, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !result.Transitioned || !result.Replayed || len(runner.requests) != 1 || base.verifications != 1 || engine.state.State != domain.StateBuilding {
		t.Fatalf("source-resume transition replay=%+v err=%v calls=%d records=%d state=%s", result, err, len(runner.requests), base.verifications, engine.state.State)
	}
}

func TestVerificationAmendmentReplaysExactCurrentReviewerResult(t *testing.T) {
	for _, tc := range []struct {
		name        string
		decision    store.VerificationAmendmentDecision
		decisionErr error
		wantVerify  int
		wantState   domain.State
	}{
		{name: "accepted", decision: store.VerificationAmendmentAccepted, wantVerify: 1, wantState: domain.StateBuilding},
		{name: "rejected", decision: store.VerificationAmendmentRejected, wantVerify: 0, wantState: domain.StateBuilding},
		{name: "third digest blocks", decisionErr: store.ErrEvidenceConflict, wantVerify: 0, wantState: domain.StateBlocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := &fakeEvidence{hasPlan: true, amendment: &store.VerificationAmendment{TransitionTicketVersion: 1, Prior: store.VerificationRevision{Revision: 1, ProofDigest: digest}, ProposedDigest: strings.Repeat("b", 64), ProposedCommand: []string{"go", "test", "./..."}, Reason: "replace the proof", Requester: "builder"}, amendmentDecision: tc.decision, amendmentErr: tc.decisionErr, providerResults: map[int64]phaseartifact.Parsed{2: verificationOutput().parsed}}
			engine := &fakeEngine{}
			runner := &fakeRunner{}
			worker := newWorker(domain.StateVerifying, runner, evidence, engine)
			worker.Checkpoint = fakeCheckpoint{}
			got, err := worker.Run(context.Background(), testRef, testFence)
			if err != nil || !got.Transitioned || !got.Replayed || len(runner.requests) != 0 || evidence.verifications != tc.wantVerify || engine.state.State != tc.wantState {
				t.Fatalf("run=%+v err=%v calls=%d verifications=%d state=%s", got, err, len(runner.requests), evidence.verifications, engine.state.State)
			}
		})
	}
}

func TestVerificationAmendmentReplaysRecoveredReviewerWithoutThirdInvocation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		decision   store.VerificationAmendmentDecision
		wantVerify int
	}{
		{name: "accepted before transition", decision: store.VerificationAmendmentAccepted, wantVerify: 1},
		{name: "rejected before transition", decision: store.VerificationAmendmentRejected, wantVerify: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := &fakeEvidence{
				hasPlan:           true,
				reusableRecovered: true,
				recoveredVersion:  true,
				amendment: &store.VerificationAmendment{
					TransitionTicketVersion: 1,
					ConsumedVersion:         0,
					Prior:                   store.VerificationRevision{Revision: 1, ProofDigest: digest},
					ProposedDigest:          strings.Repeat("b", 64),
					ProposedCommand:         []string{"go", "test", "./..."},
					Reason:                  "replace the proof",
					Requester:               "builder",
				},
				amendmentDecision: tc.decision,
				providerResults:   map[int64]phaseartifact.Parsed{2: verificationOutput().parsed},
			}
			engine := &fakeEngine{}
			runner := &fakeRunner{}
			worker := newWorker(domain.StateVerifying, runner, evidence, engine)
			// Simulate a recovery ledger hop after the amendment Reviewer completed:
			// the result belongs to the immutable request endpoint (v1), while the
			// live decision runs at v2. Replaying it must not launch a third Reviewer.
			evidence.ticket.Version = 2
			worker.Checkpoint = fakeCheckpoint{}

			got, err := worker.Run(context.Background(), testRef, testFence)
			if err != nil || !got.Transitioned || !got.Replayed || len(runner.requests) != 0 || evidence.verifications != tc.wantVerify || engine.state.State != domain.StateBuilding {
				t.Fatalf("recovered amendment replay=%+v err=%v calls=%d verifications=%d state=%s", got, err, len(runner.requests), evidence.verifications, engine.state.State)
			}
		})
	}
}

func TestVerificationAmendmentIgnoresHistoricalReviewerResultAndLaunchesFresh(t *testing.T) {
	// The pre-amendment Reviewer is still the newest completed result until the
	// independent amendment Reviewer starts. It must not be mistaken for the
	// amendment decision, even though Store can authenticate it across the
	// recovery/version bridge.
	evidence := &fakeEvidence{
		hasPlan: true,
		amendment: &store.VerificationAmendment{
			TransitionTicketVersion: 1,
			Prior:                   store.VerificationRevision{Revision: 1, ProofDigest: digest},
			ProposedDigest:          strings.Repeat("b", 64),
			ProposedCommand:         []string{"go", "test", "./..."},
			Reason:                  "replace the proof",
			Requester:               "builder",
		},
		reusableHistorical: true,
		providerResults: map[int64]phaseartifact.Parsed{
			8: verificationOutput().parsed,
		},
	}
	engine := &fakeEngine{}
	runner := &fakeRunner{outputs: []fakePhase{verificationOutput()}}
	worker := newWorker(domain.StateVerifying, runner, evidence, engine)
	worker.Checkpoint = fakeCheckpoint{}
	got, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !got.Transitioned || got.Replayed || len(runner.requests) != 1 || evidence.verifications != 1 || engine.state.State != domain.StateBuilding {
		t.Fatalf("historical amendment result was reused: run=%+v err=%v calls=%d verifications=%d state=%s", got, err, len(runner.requests), evidence.verifications, engine.state.State)
	}
}

func TestStaleFenceAndCancellation(t *testing.T) {
	e := &fakeEvidence{}
	eng := &fakeEngine{}
	run := &fakeRunner{outputs: []fakePhase{plannerOutput(false)}}
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
	run := &fakeRunner{outputs: []fakePhase{out}}
	w := newWorker(domain.StateVerifying, run, e, eng)
	w.Checkpoint = fakeCheckpoint{}
	w.CheckpointMaterializer = badCheckpointMaterializer{}
	if _, err := w.Run(context.Background(), testRef, testFence); !errors.Is(err, ErrCheckpointRequired) {
		t.Fatalf("checkpoint mismatch err=%v", err)
	}
	if eng.signals != 0 {
		t.Fatal("mismatched checkpoint advanced workflow")
	}
}

type badCheckpointMaterializer struct{}

func (badCheckpointMaterializer) MaterializeVerificationCheckpoint(context.Context, PhaseRequest, phaseartifact.Verification, store.ProviderAttemptResultKey) (VerificationCheckpoint, error) {
	return VerificationCheckpoint{ID: oid, Commit: store.CommitObservation{CommitOID: "abcdefabcdefabcdefabcdefabcdefabcdefabcd", ParentOID: oid, TreeOID: oid}}, nil
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
	run := &fakeRunner{outputs: []fakePhase{builderOutput(false)}}
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
