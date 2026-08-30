// Package workflowworker contains the production-neutral, single-ticket
// walking skeleton. It owns orchestration only: Store remains the evidence
// authority, Engine remains the state-machine authority, and the injected
// phase runner remains the provider/runtime boundary.
package workflowworker

import (
	"context"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

var (
	ErrCanceled             = errors.New("workflow worker canceled")
	ErrNoPhaseRunner        = errors.New("workflow phase runner is not configured")
	ErrCandidateRequired    = errors.New("builder did not provide a Store-authenticated candidate witness")
	ErrCheckpointRequired   = errors.New("verification did not provide an authenticated checkpoint witness")
	ErrAmendmentUnsupported = errors.New("verification amendment requires an authenticated Store amendment request")
	ErrStaleEvidence        = errors.New("phase evidence is not current for this ticket fence")
	ErrUnsupportedState     = errors.New("workflow worker cannot execute this ticket state")
)

// Evidence is the deliberately small Store surface needed by the walking
// skeleton. *store.Store implements it directly; tests and future daemon
// compositions can provide an adapter without giving the worker SQL access.
type Evidence interface {
	Ticket(context.Context, domain.TicketRef) (store.Ticket, error)
	Plan(context.Context, domain.TicketRef) (store.StoredPlan, error)
	CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error)
	ValidateCurrentCandidateForBuildTransition(context.Context, domain.TicketRef, uint64, domain.Fence) (store.StoredCandidate, error)
	Worktree(context.Context, domain.TicketRef) (store.StoredWorktree, error)
	AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error
	LoadCurrentProviderAttemptResult(context.Context, store.ProviderAttemptResultKey, uint64, domain.Fence) (store.ProviderAttemptResult, phaseartifact.Parsed, error)
	LatestReusableProviderAttempt(context.Context, store.LatestReusableProviderAttemptRequest) (store.LatestReusableProviderAttemptResult, error)
	RecordPlan(context.Context, store.PlanArtifact) (string, error)
	RecordVerification(context.Context, store.VerificationArtifact) (store.VerificationRevision, error)
	RecordCandidate(context.Context, store.CandidateEvidence) ([]store.InvalidationReceipt, error)
	ConsumeBudget(context.Context, store.BudgetUse) (int, error)
}

// StateMachine is the narrow Engine surface. Signals must be implemented by
// engine.Engine so guards and durable transition persistence are centralized.
type StateMachine interface {
	Signal(context.Context, contracts.SignalRequest) (contracts.TransitionResult, error)
	SignalCandidate(context.Context, contracts.SignalRequest, domain.CandidateSnapshot) (contracts.TransitionResult, error)
}

// PhaseRequest carries only authenticated Store facts and the current fence.
// The phase runner may use it to construct workflowprompt inputs, but it must
// not treat provider output as transition authority.
type PhaseRequest struct {
	Ticket       store.Ticket
	Worktree     store.StoredWorktree
	Phase        domain.Phase
	Fence        domain.Fence
	Plan         *store.StoredPlan
	Verification *store.StoredVerification
	Candidate    *store.StoredCandidate
}

// PhaseResult carries only an immutable Store key and independently observed
// Git boundary witnesses.  Parsed provider output is deliberately absent.
type PhaseResult struct {
	// Parsed is retained only for compatibility with in-process test adapters.
	// Worker code never reads it; transition authority is always reloaded from
	// ProviderResult through Evidence.
	Parsed         phaseartifact.Parsed
	ProviderResult store.ProviderAttemptResultKey
	Checkpoint     *VerificationCheckpoint
	Candidate      *CandidateWitness
}

// VerificationCheckpoint is a typed commit witness. The worker does not
// accept a provider-supplied string as a checkpoint; CheckpointAuthenticator
// must re-authenticate this witness against the current worktree before the
// evidence write.
type VerificationCheckpoint struct {
	ID     string
	Commit store.CommitObservation
}

type CheckpointAuthenticator interface {
	AuthenticateVerificationCheckpoint(context.Context, PhaseRequest, phaseartifact.Verification, VerificationCheckpoint) error
}

// CandidateAuthenticator re-proves current Git/worktree facts before a
// candidate is recorded or replayed.  Store validates durable bindings; this
// interface validates the live boundary the Store intentionally cannot read.
type CandidateAuthenticator interface {
	AuthenticateCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, CandidateWitness) error
}

// CandidateWitness contains only facts produced by the injected commit
// boundary. Ticket, fence, source, verification, and Builder identities are
// constructed by the worker from current durable state.
type CandidateWitness struct {
	Commit              store.CommitObservation
	CommandPolicyDigest string
	Reason              string
}

// PhaseRunner is intentionally narrower than providercoord.Coordinator. A
// later composition can adapt Coordinator.Run here while keeping this worker
// independent of process supervision and provider registration.
type PhaseRunner interface {
	Run(context.Context, PhaseRequest) (PhaseResult, error)
}

// Worker executes at most one currently runnable phase per Run call and then
// re-reads the ticket. This makes it safe for a daemon scheduler to call it
// repeatedly and keeps every provider call outside SQLite transactions.
type Worker struct {
	Evidence   Evidence
	Engine     StateMachine
	Runner     PhaseRunner
	Checkpoint CheckpointAuthenticator
	Candidate  CandidateAuthenticator
}

type RunResult struct {
	Ref          domain.TicketRef
	State        domain.State
	Version      uint64
	Phase        domain.Phase
	Transitioned bool
	Replayed     bool
}

func (w Worker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (RunResult, error) {
	if err := ref.Validate(); err != nil {
		return RunResult{}, err
	}
	if w.Evidence == nil || w.Engine == nil {
		return RunResult{}, errors.New("workflow worker requires evidence and state machine")
	}
	if fence.LeaderEpoch == 0 {
		return RunResult{}, errors.New("workflow worker requires a leader fence")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RunResult{Ref: ref}, fmt.Errorf("%w: %v", ErrCanceled, err)
	}
	ticket, err := w.Evidence.Ticket(ctx, ref)
	if err != nil {
		return RunResult{Ref: ref}, err
	}
	result := RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}
	switch ticket.State {
	case domain.StatePlanning:
		result.Phase = domain.PhasePlanning
		result.Transitioned, result.Replayed, err = w.planning(ctx, ticket, fence)
	case domain.StateVerifying:
		result.Phase = domain.PhaseVerification
		result.Transitioned, result.Replayed, err = w.verifying(ctx, ticket, fence)
	case domain.StateBuilding:
		result.Phase = domain.PhaseBuild
		result.Transitioned, result.Replayed, err = w.building(ctx, ticket, fence)
	default:
		// Waiting states and terminal/control states are scheduler or
		// operator boundaries; the worker never publishes or reviews them.
		return result, nil
	}
	if err != nil {
		return result, err
	}
	// Signal mutates version/state.  Never return the pre-transition snapshot.
	current, readErr := w.Evidence.Ticket(ctx, ref)
	if readErr != nil {
		return result, readErr
	}
	result.State, result.Version = current.State, current.Version
	return result, nil
}

func (w Worker) planning(ctx context.Context, ticket store.Ticket, fence domain.Fence) (bool, bool, error) {
	plan, err := w.Evidence.Plan(ctx, ticket.Ref)
	if err == nil {
		if plan.TicketVersion != ticket.Version || plan.Fence != fence {
			return false, false, ErrStaleEvidence
		}
		if _, err := w.storedPlanIdentity(ctx, ticket, plan, fence); err != nil {
			return false, false, err
		}
		// Evidence-first replay closes the response-loss gap between RecordPlan
		// and Engine.Signal. Engine still re-checks state/version/fence.
		if err := w.signal(ctx, ticket, fence, "phase_pass", map[string]string{"typed_plan_valid": "true"}); err != nil {
			return false, true, err
		}
		return true, true, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return false, false, err
	}
	// A completed Planner is reusable after runner recovery.  It is reloaded
	// from immutable Store evidence and rebound by RecordPlan under this fence;
	// never invoke the provider a second time just because the plan write was
	// interrupted.
	if reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence}); reuseErr == nil {
		planner, parseErr := canonicalPlanner(reusable.Parsed)
		if parseErr != nil {
			return false, false, parseErr
		}
		if _, parseErr = w.planIdentity(ticket, planner); parseErr != nil {
			return false, false, parseErr
		}
		if len(planner.Questions) > 0 {
			if err := w.signal(ctx, ticket, fence, "needs_operator_input", map[string]string{"questions_bounded": "true"}); err != nil {
				return false, true, err
			}
			return true, true, nil
		}
		if _, recordErr := w.Evidence.RecordPlan(ctx, store.PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: store.PlanDocument{Planner: &planner, ProviderResult: &reusable.Key, Acceptance: planner.Acceptance, ProofKind: string(planner.Proof.Kind), Paths: planner.Paths, Commands: planner.Commands, Risks: planner.Risks}}); recordErr != nil {
			return false, true, recordErr
		}
		if err := w.signal(ctx, ticket, fence, "phase_pass", map[string]string{"typed_plan_valid": "true"}); err != nil {
			return false, true, err
		}
		return true, true, nil
	} else if !errors.Is(reuseErr, store.ErrNotFound) {
		return false, false, reuseErr
	}
	if w.Runner == nil {
		return false, false, ErrNoPhaseRunner
	}
	request, err := w.request(ctx, ticket, fence, domain.PhasePlanning, nil, nil, nil)
	if err != nil {
		return false, false, err
	}
	if err := w.Evidence.AssertTicketFence(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		return false, false, err
	}
	out, err := w.Runner.Run(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return false, false, fmt.Errorf("%w: %v", ErrCanceled, ctx.Err())
		}
		return false, false, err
	}
	_, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, out.ProviderResult, ticket.Version, fence)
	planner, err := canonicalPlanner(parsed)
	if err != nil {
		return false, false, err
	}
	if len(planner.Questions) > 0 {
		if err := w.signal(ctx, ticket, fence, "needs_operator_input", map[string]string{"questions_bounded": "true"}); err != nil {
			return false, false, err
		}
		return true, false, nil
	}
	identity, err := w.planIdentity(ticket, planner)
	if err != nil {
		return false, false, err
	}
	if _, err := w.Evidence.RecordPlan(ctx, store.PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: request.Fence, Document: store.PlanDocument{Planner: &planner, ProviderResult: &out.ProviderResult, Acceptance: planner.Acceptance, ProofKind: string(planner.Proof.Kind), Paths: planner.Paths, Commands: planner.Commands, Risks: planner.Risks}}); err != nil {
		return false, false, err
	}
	_ = identity
	if err := w.signal(ctx, ticket, fence, "phase_pass", map[string]string{"typed_plan_valid": "true"}); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func (w Worker) verifying(ctx context.Context, ticket store.Ticket, fence domain.Fence) (bool, bool, error) {
	plan, err := w.Evidence.Plan(ctx, ticket.Ref)
	if err != nil {
		return false, false, err
	}
	planIdentity, err := w.storedPlanIdentity(ctx, ticket, plan, fence)
	if err != nil {
		return false, false, err
	}
	verification, err := w.Evidence.CurrentVerification(ctx, ticket.Ref)
	if err == nil {
		if verification.TicketVersion != ticket.Version || verification.Fence != fence {
			return false, false, ErrStaleEvidence
		}
		if _, err := w.storedVerificationIdentity(ctx, ticket, planIdentity, verification, fence); err != nil {
			return false, false, err
		}
		if err := w.signal(ctx, ticket, fence, "phase_pass", map[string]string{"independent_intent_valid": "true", "prebuild_proof_valid": "true", "verification_checkpoint_committed": "true"}); err != nil {
			return false, true, err
		}
		return true, true, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return false, false, err
	}
	// The Store can identify a reusable Verification result, but this skeleton
	// has no durable full checkpoint observation to hand to the live Git
	// authenticator.  Fail closed rather than rerunning the reviewer or
	// accepting an old-fence checkpoint.
	if _, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseVerification, Role: "verification", ExpectedVersion: ticket.Version, Fence: fence}); reuseErr == nil {
		return false, true, ErrCheckpointRequired
	} else if !errors.Is(reuseErr, store.ErrNotFound) {
		return false, false, reuseErr
	}
	if w.Runner == nil {
		return false, false, ErrNoPhaseRunner
	}
	request, err := w.request(ctx, ticket, fence, domain.PhaseVerification, &plan, nil, nil)
	if err != nil {
		return false, false, err
	}
	if err := w.Evidence.AssertTicketFence(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		return false, false, err
	}
	out, err := w.Runner.Run(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return false, false, fmt.Errorf("%w: %v", ErrCanceled, ctx.Err())
		}
		return false, false, err
	}
	_, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, out.ProviderResult, ticket.Version, fence)
	artifact, err := canonicalVerification(parsed)
	if err != nil {
		return false, false, err
	}
	intent, err := canonicalVerificationIntent(artifact)
	if err != nil {
		return false, false, err
	}
	proof, err := canonicalVerificationProof(artifact)
	if err != nil {
		return false, false, err
	}
	if out.Checkpoint == nil || w.Checkpoint == nil || out.Checkpoint.ID == "" || out.Checkpoint.ID != out.Checkpoint.Commit.CommitOID {
		return false, false, ErrCheckpointRequired
	}
	intentDigest, err := workflowprompt.VerificationIntentDigest(artifact)
	if err != nil {
		return false, false, err
	}
	proofDigest, err := workflowprompt.VerificationProofDigest(artifact)
	if err != nil {
		return false, false, err
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(artifact, intentDigest, proofDigest, out.Checkpoint.ID)
	if err != nil {
		return false, false, err
	}
	if _, err := workflowprompt.ValidateVerificationIdentity(workflowTicket(ticket), planIdentity, verificationIdentity); err != nil {
		return false, false, err
	}
	if err := w.Checkpoint.AuthenticateVerificationCheckpoint(ctx, request, artifact, *out.Checkpoint); err != nil {
		return false, false, err
	}
	if _, err := w.Evidence.RecordVerification(ctx, store.VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: request.Fence, Intent: intent, Proof: proof, OwnedFiles: artifact.OwnedFiles, CheckpointID: out.Checkpoint.ID, ProviderResult: &out.ProviderResult, Checkpoint: out.Checkpoint.Commit}); err != nil {
		return false, false, err
	}
	if err := w.signal(ctx, ticket, fence, "phase_pass", map[string]string{"independent_intent_valid": "true", "prebuild_proof_valid": "true", "verification_checkpoint_committed": "true"}); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func (w Worker) building(ctx context.Context, ticket store.Ticket, fence domain.Fence) (bool, bool, error) {
	plan, err := w.Evidence.Plan(ctx, ticket.Ref)
	if err != nil {
		return false, false, err
	}
	planIdentity, err := w.storedPlanIdentity(ctx, ticket, plan, fence)
	if err != nil {
		return false, false, err
	}
	verification, err := w.Evidence.CurrentVerification(ctx, ticket.Ref)
	if err != nil {
		return false, false, err
	}
	verificationIdentity, err := w.storedVerificationIdentity(ctx, ticket, planIdentity, verification, fence)
	if err != nil {
		return false, false, err
	}
	candidate, err := w.Evidence.ValidateCurrentCandidateForBuildTransition(ctx, ticket.Ref, ticket.Version, fence)
	if err == nil {
		if err := w.authenticateStoredCandidate(ctx, ticket, fence, planIdentity, verificationIdentity, candidate); err != nil {
			return false, true, err
		}
		if err := w.signalCandidate(ctx, ticket, fence, candidate.Snapshot); err != nil {
			return false, true, err
		}
		return true, true, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return false, false, err
	}
	if w.Runner == nil {
		return false, false, ErrNoPhaseRunner
	}
	request, err := w.request(ctx, ticket, fence, domain.PhaseBuild, &plan, &verification, nil)
	if err != nil {
		return false, false, err
	}
	if err := w.Evidence.AssertTicketFence(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		return false, false, err
	}
	out, err := w.Runner.Run(ctx, request)
	if err != nil {
		if ctx.Err() != nil {
			return false, false, fmt.Errorf("%w: %v", ErrCanceled, ctx.Err())
		}
		return false, false, err
	}
	_, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, out.ProviderResult, ticket.Version, fence)
	builder, err := canonicalBuilder(parsed)
	if err != nil {
		return false, false, err
	}
	if builder.AmendmentRequest != nil {
		return false, false, ErrAmendmentUnsupported
	}
	if out.Candidate == nil {
		return false, false, ErrCandidateRequired
	}
	if w.Candidate == nil {
		return false, false, ErrCandidateRequired
	}
	if err := w.Candidate.AuthenticateCandidate(ctx, request, planIdentity, verificationIdentity, builder, *out.Candidate); err != nil {
		return false, false, err
	}
	if out.ProviderResult.AttemptID == 0 || out.ProviderResult.Ref != ticket.Ref || out.ProviderResult.Phase != domain.PhaseBuild || out.ProviderResult.Attempt <= 0 {
		return false, false, ErrCandidateRequired
	}
	if out.Candidate.Reason == "" || out.Candidate.CommandPolicyDigest == "" {
		return false, false, ErrCandidateRequired
	}
	evidence := store.CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: request.Fence, BuilderResult: out.ProviderResult, Commit: out.Candidate.Commit, Reason: out.Candidate.Reason, Snapshot: domain.CandidateSnapshot{BaseSHA: request.Worktree.BaseSHA, HeadSHA: out.Candidate.Commit.CommitOID, TreeSHA: out.Candidate.Commit.TreeOID, SourceDigest: ticket.SourceDigest, VerificationIntentDigest: verification.Revision.IntentDigest, ProofDigest: verification.Revision.ProofDigest, CommandPolicyDigest: out.Candidate.CommandPolicyDigest}}
	builderEvidenceDigest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil {
		return false, false, err
	}
	evidence.Snapshot.BuilderEvidenceDigest = builderEvidenceDigest
	if _, err := w.Evidence.RecordCandidate(ctx, evidence); err != nil {
		return false, false, err
	}
	candidate, err = w.Evidence.ValidateCurrentCandidateForBuildTransition(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		return false, false, err
	}
	if err := w.signalCandidate(ctx, ticket, fence, candidate.Snapshot); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func (w Worker) request(ctx context.Context, ticket store.Ticket, fence domain.Fence, phase domain.Phase, plan *store.StoredPlan, verification *store.StoredVerification, candidate *store.StoredCandidate) (PhaseRequest, error) {
	worktree, err := w.Evidence.Worktree(ctx, ticket.Ref)
	if err != nil {
		return PhaseRequest{}, err
	}
	if fence.RunnerEpoch != ticket.RunnerEpoch {
		return PhaseRequest{}, fmt.Errorf("%w: runner epoch %d does not match ticket %d", store.ErrStaleFence, fence.RunnerEpoch, ticket.RunnerEpoch)
	}
	return PhaseRequest{Ticket: ticket, Worktree: worktree, Phase: phase, Fence: fence, Plan: plan, Verification: verification, Candidate: candidate}, nil
}

func (w Worker) signal(ctx context.Context, ticket store.Ticket, fence domain.Fence, trigger string, attributes map[string]string) error {
	if fence.RunnerEpoch != ticket.RunnerEpoch {
		return store.ErrStaleFence
	}
	_, err := w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: trigger, Fence: fence, Attributes: attributes, EventPayload: "{}"})
	return err
}

func (w Worker) signalCandidate(ctx context.Context, ticket store.Ticket, fence domain.Fence, candidate domain.CandidateSnapshot) error {
	if fence.RunnerEpoch != ticket.RunnerEpoch {
		return store.ErrStaleFence
	}
	_, err := w.Engine.SignalCandidate(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: "phase_pass", Fence: fence, Attributes: map[string]string{"proof_green": "true", "diff_valid": "true", "git_control_plane_valid": "true", "candidate_checkpoint_committed": "true"}, EventPayload: "{}"}, candidate)
	return err
}

func workflowTicket(ticket store.Ticket) workflowprompt.Ticket {
	return workflowprompt.Ticket{Channel: ticket.Ref.Channel, Project: ticket.Ref.Project, ID: ticket.Ref.Ticket, Type: ticket.Type, SourceDigest: ticket.SourceDigest, Body: ticket.Problem}
}

func (w Worker) planIdentity(ticket store.Ticket, planner phaseartifact.Planner) (workflowprompt.PlanIdentity, error) {
	identity, err := workflowprompt.NewPlanIdentity(planner)
	if err != nil {
		return workflowprompt.PlanIdentity{}, err
	}
	_, err = workflowprompt.ValidatePlanIdentity(workflowTicket(ticket), identity)
	return identity, err
}

func (w Worker) storedPlanIdentity(ctx context.Context, ticket store.Ticket, plan store.StoredPlan, fence domain.Fence) (workflowprompt.PlanIdentity, error) {
	if plan.Document.Planner == nil || plan.Document.ProviderResult == nil {
		return workflowprompt.PlanIdentity{}, ErrStaleEvidence
	}
	_, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, *plan.Document.ProviderResult, ticket.Version, fence)
	if err != nil || parsed.Planner == nil {
		reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence})
		if reuseErr != nil || reusable.Key != *plan.Document.ProviderResult || reusable.Parsed.Planner == nil {
			return workflowprompt.PlanIdentity{}, ErrStaleEvidence
		}
		parsed = reusable.Parsed
	}
	if parsed.Planner == nil {
		return workflowprompt.PlanIdentity{}, ErrStaleEvidence
	}
	identity, err := w.planIdentity(ticket, *parsed.Planner)
	if err != nil {
		return workflowprompt.PlanIdentity{}, err
	}
	if plan.Digest == "" {
		return workflowprompt.PlanIdentity{}, ErrStaleEvidence
	}
	return identity, nil
}

func (w Worker) storedVerificationIdentity(ctx context.Context, ticket store.Ticket, plan workflowprompt.PlanIdentity, verification store.StoredVerification, fence domain.Fence) (workflowprompt.VerificationIdentity, error) {
	reusable, err := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseVerification, Role: "verification", ExpectedVersion: ticket.Version, Fence: fence})
	if err != nil || reusable.Parsed.Verify == nil {
		return workflowprompt.VerificationIdentity{}, ErrStaleEvidence
	}
	artifact := *reusable.Parsed.Verify
	identity, err := workflowprompt.NewVerificationIdentity(artifact, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID)
	if err != nil {
		return workflowprompt.VerificationIdentity{}, err
	}
	if _, err := workflowprompt.ValidateVerificationIdentity(workflowTicket(ticket), plan, identity); err != nil {
		return workflowprompt.VerificationIdentity{}, err
	}
	if w.Checkpoint == nil {
		return workflowprompt.VerificationIdentity{}, ErrCheckpointRequired
	}
	request, err := w.request(ctx, ticket, fence, domain.PhaseVerification, nil, nil, nil)
	if err != nil {
		return workflowprompt.VerificationIdentity{}, err
	}
	if err := w.Checkpoint.AuthenticateVerificationCheckpoint(ctx, request, artifact, VerificationCheckpoint{ID: verification.Revision.CheckpointID, Commit: store.CommitObservation{CommitOID: verification.Revision.CheckpointID}}); err != nil {
		return workflowprompt.VerificationIdentity{}, err
	}
	return identity, nil
}

// Builder results are intentionally not reusable under the existing Store
// authority.  Without a persisted exact Builder result key, a restart cannot
// re-authenticate candidate evidence, so replay fails closed instead of
// running the Builder again or accepting an ambiguous candidate.
func (w Worker) authenticateStoredCandidate(context.Context, store.Ticket, domain.Fence, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, store.StoredCandidate) error {
	return ErrStaleEvidence
}

func canonicalPlanner(parsed phaseartifact.Parsed) (phaseartifact.Planner, error) {
	if parsed.Phase != domain.PhasePlanning || parsed.Planner == nil {
		return phaseartifact.Planner{}, errors.New("planning phase did not return a Planner artifact")
	}
	if _, _, err := phaseartifact.CanonicalTypedArtifact(parsed); err != nil {
		return phaseartifact.Planner{}, err
	}
	return *parsed.Planner, nil
}

func canonicalVerification(parsed phaseartifact.Parsed) (phaseartifact.Verification, error) {
	if parsed.Phase != domain.PhaseVerification || parsed.Verify == nil {
		return phaseartifact.Verification{}, errors.New("verification phase did not return a Verification artifact")
	}
	if _, _, err := phaseartifact.CanonicalTypedArtifact(parsed); err != nil {
		return phaseartifact.Verification{}, err
	}
	return *parsed.Verify, nil
}

func canonicalBuilder(parsed phaseartifact.Parsed) (phaseartifact.Builder, error) {
	if parsed.Phase != domain.PhaseBuild || parsed.Builder == nil {
		return phaseartifact.Builder{}, errors.New("build phase did not return a Builder artifact")
	}
	if _, _, err := phaseartifact.CanonicalTypedArtifact(parsed); err != nil {
		return phaseartifact.Builder{}, err
	}
	return *parsed.Builder, nil
}

func canonicalVerificationIntent(value phaseartifact.Verification) ([]byte, error) {
	return workflowprompt.CanonicalVerificationIntentBytes(value)
}

func canonicalVerificationProof(value phaseartifact.Verification) ([]byte, error) {
	return workflowprompt.CanonicalVerificationProofBytes(value)
}
