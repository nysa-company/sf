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
	LatestCandidate(context.Context, domain.TicketRef) (store.StoredCandidate, error)
	ValidateCurrentCandidateForBuildTransition(context.Context, domain.TicketRef, uint64, domain.Fence) (store.StoredCandidate, error)
	Worktree(context.Context, domain.TicketRef) (store.StoredWorktree, error)
	AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error
	LoadCurrentProviderAttemptResult(context.Context, store.ProviderAttemptResultKey, uint64, domain.Fence) (store.ProviderAttemptResult, phaseartifact.Parsed, error)
	LoadHistoricalProviderAttemptResult(context.Context, store.ProviderAttemptResultKey) (store.ProviderAttemptResult, phaseartifact.Parsed, error)
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
	SignalPlan(context.Context, contracts.SignalRequest) (contracts.TransitionResult, error)
	SignalVerification(context.Context, contracts.SignalRequest) (contracts.TransitionResult, error)
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
	ProviderResult store.ProviderAttemptResultKey
}

// VerificationCheckpoint is a typed commit witness. The worker does not
// accept a provider-supplied string as a checkpoint; CheckpointAuthenticator
// must re-authenticate this witness against the current worktree before the
// evidence write.
type VerificationCheckpoint struct {
	ID            string
	Commit        store.CommitObservation
	CommandResult contracts.RepositoryCommandResultKey
}

// VerificationCheckpointMaterializer reconstructs a checkpoint witness from
// the provider result and the Git boundary. It is used both after a fresh
// provider completion and after a crash; provider output never carries it.
type VerificationCheckpointMaterializer interface {
	MaterializeVerificationCheckpoint(context.Context, PhaseRequest, phaseartifact.Verification, store.ProviderAttemptResultKey) (VerificationCheckpoint, error)
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

// CandidateMaterializer reconstructs the candidate commit boundary for an
// exact Builder result. It prevents a restart from invoking Builder again.
type CandidateMaterializer interface {
	MaterializeCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, store.ProviderAttemptResultKey) (CandidateWitness, error)
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
	Evidence               Evidence
	Engine                 StateMachine
	Runner                 PhaseRunner
	Checkpoint             CheckpointAuthenticator
	Candidate              CandidateAuthenticator
	CheckpointMaterializer VerificationCheckpointMaterializer
	CandidateMaterializer  CandidateMaterializer
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
		if errors.Is(err, ErrAmendmentUnsupported) {
			if current, readErr := w.Evidence.Ticket(ctx, ref); readErr == nil {
				result.State, result.Version = current.State, current.Version
			}
		}
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
			// A durable plan can survive a lost SignalPlan response, but its
			// old fence is not transition authority.  Rebind only the exact
			// newest recovered Planner result already named by that plan; never
			// substitute a newer result into an older plan document.
			if plan.Document.Planner == nil || plan.Document.ProviderResult == nil {
				return false, false, ErrStaleEvidence
			}
			reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence})
			if reuseErr != nil || !reusable.Recovered || reusable.Key != *plan.Document.ProviderResult {
				return false, false, ErrStaleEvidence
			}
			if _, parseErr := canonicalPlanner(reusable.Parsed); parseErr != nil {
				return false, true, parseErr
			}
			// storedPlanIdentity canonically compares the immutable document to
			// its exact historical provider result before RecordPlan appends the
			// current-fence binding.
			if _, identityErr := w.storedPlanIdentity(ctx, ticket, plan, fence); identityErr != nil {
				return false, true, identityErr
			}
			if _, recordErr := w.Evidence.RecordPlan(ctx, store.PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: plan.Document}); recordErr != nil {
				return false, true, recordErr
			}
			plan, err = w.Evidence.Plan(ctx, ticket.Ref)
			if err != nil || plan.TicketVersion != ticket.Version || plan.Fence != fence {
				return false, true, ErrStaleEvidence
			}
		}
		if _, err := w.storedPlanIdentity(ctx, ticket, plan, fence); err != nil {
			return false, false, err
		}
		// Evidence-first replay closes the response-loss gap between RecordPlan
		// and Engine.Signal. Engine still re-checks state/version/fence.
		if err := w.signalPlan(ctx, ticket, fence); err != nil {
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
		if err := w.signalPlan(ctx, ticket, fence); err != nil {
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
	result, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, out.ProviderResult, ticket.Version, fence)
	if err != nil || out.ProviderResult.Ref != ticket.Ref || out.ProviderResult.Phase != domain.PhasePlanning || result.Claim.Ref != ticket.Ref || result.Claim.Phase != domain.PhasePlanning || result.Claim.Role != "planner" || result.Claim.ID != out.ProviderResult.AttemptID || result.Claim.Attempt != out.ProviderResult.Attempt {
		return false, false, ErrStaleEvidence
	}
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
	if err := w.signalPlan(ctx, ticket, fence); err != nil {
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
			// A completed reviewer result survives runner-fence recovery, but its
			// old binding is not itself transition authority. Re-select the exact
			// newest reusable result under the live fence, re-materialize its Git
			// checkpoint, and let RecordVerification append a live binding.
			reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: ticket.Version, Fence: fence})
			if reuseErr != nil || !reusable.Recovered || reusable.Key != verification.ProviderResult {
				return false, false, ErrStaleEvidence
			}
			artifact, parseErr := canonicalVerification(reusable.Parsed)
			if parseErr != nil {
				return false, true, parseErr
			}
			replayRequest, requestErr := w.request(ctx, ticket, fence, domain.PhaseVerification, &plan, nil, nil)
			if requestErr != nil {
				return false, true, requestErr
			}
			if err := w.persistVerification(ctx, ticket, fence, replayRequest, planIdentity, artifact, reusable.Key); err != nil {
				return false, true, err
			}
			verification, err = w.Evidence.CurrentVerification(ctx, ticket.Ref)
			if err != nil || verification.TicketVersion != ticket.Version || verification.Fence != fence {
				return false, true, ErrStaleEvidence
			}
		}
		if _, err := w.storedVerificationIdentity(ctx, ticket, planIdentity, verification, fence); err != nil {
			return false, false, err
		}
		if err := w.signalVerification(ctx, ticket, fence); err != nil {
			return false, true, err
		}
		return true, true, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return false, false, err
	}
	if reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: ticket.Version, Fence: fence}); reuseErr == nil {
		artifact, parseErr := canonicalVerification(reusable.Parsed)
		if parseErr != nil {
			return false, true, parseErr
		}
		replayRequest, requestErr := w.request(ctx, ticket, fence, domain.PhaseVerification, &plan, nil, nil)
		if requestErr != nil {
			return false, true, requestErr
		}
		if err := w.persistVerification(ctx, ticket, fence, replayRequest, planIdentity, artifact, reusable.Key); err != nil {
			return false, true, err
		}
		if err := w.signalVerification(ctx, ticket, fence); err != nil {
			return false, true, err
		}
		return true, true, nil
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
	result, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, out.ProviderResult, ticket.Version, fence)
	if err != nil || out.ProviderResult.Ref != ticket.Ref || out.ProviderResult.Phase != domain.PhaseVerification || result.Claim.Ref != ticket.Ref || result.Claim.Phase != domain.PhaseVerification || result.Claim.Role != "reviewer" || result.Claim.ID != out.ProviderResult.AttemptID || result.Claim.Attempt != out.ProviderResult.Attempt {
		return false, false, ErrStaleEvidence
	}
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
	_ = intent
	_ = proof
	if err := w.persistVerification(ctx, ticket, fence, request, planIdentity, artifact, out.ProviderResult); err != nil {
		return false, false, err
	}
	if err := w.signalVerification(ctx, ticket, fence); err != nil {
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
	if errors.Is(err, store.ErrStaleFence) {
		// A candidate snapshot is immutable provenance.  Rebind an old snapshot
		// only from the exact newest recovered Builder result, after the injected
		// materializer and authenticator have re-proved the live Git boundary.
		old, oldErr := w.Evidence.LatestCandidate(ctx, ticket.Ref)
		if oldErr != nil {
			return false, false, err
		}
		reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: ticket.Version, Fence: fence})
		if reuseErr != nil || !reusable.Recovered || reusable.Key != old.BuilderResult {
			return false, false, ErrStaleEvidence
		}
		builder, parseErr := canonicalBuilder(reusable.Parsed)
		if parseErr != nil {
			return false, true, parseErr
		}
		if err := w.persistCandidate(ctx, ticket, fence, PhaseRequest{}, planIdentity, verificationIdentity, verification, builder, reusable.Key); err != nil {
			return false, true, err
		}
		candidate, err = w.Evidence.ValidateCurrentCandidateForBuildTransition(ctx, ticket.Ref, ticket.Version, fence)
		if err != nil {
			return false, true, err
		}
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
	if reusable, reuseErr := w.Evidence.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: ticket.Version, Fence: fence}); reuseErr == nil {
		builder, parseErr := canonicalBuilder(reusable.Parsed)
		if parseErr != nil {
			return false, true, parseErr
		}
		if builder.AmendmentRequest != nil {
			if err := w.signalPayload(ctx, ticket, fence, "retry_or_correction_exhausted", nil, `{"reason":"amendment_unsupported"}`); err != nil {
				return false, true, err
			}
			return false, true, ErrAmendmentUnsupported
		}
		if err := w.persistCandidate(ctx, ticket, fence, PhaseRequest{}, planIdentity, verificationIdentity, verification, builder, reusable.Key); err != nil {
			return false, true, err
		}
		candidate, err := w.Evidence.ValidateCurrentCandidateForBuildTransition(ctx, ticket.Ref, ticket.Version, fence)
		if err != nil {
			return false, true, err
		}
		if err = w.signalCandidate(ctx, ticket, fence, candidate.Snapshot); err != nil {
			return false, true, err
		}
		return true, true, nil
	} else if !errors.Is(reuseErr, store.ErrNotFound) {
		return false, false, reuseErr
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
	result, parsed, err := w.Evidence.LoadCurrentProviderAttemptResult(ctx, out.ProviderResult, ticket.Version, fence)
	if err != nil || out.ProviderResult.Ref != ticket.Ref || out.ProviderResult.Phase != domain.PhaseBuild || result.Claim.Ref != ticket.Ref || result.Claim.Phase != domain.PhaseBuild || result.Claim.Role != "builder" || result.Claim.ID != out.ProviderResult.AttemptID || result.Claim.Attempt != out.ProviderResult.Attempt {
		return false, false, ErrStaleEvidence
	}
	builder, err := canonicalBuilder(parsed)
	if err != nil {
		return false, false, err
	}
	if builder.AmendmentRequest != nil {
		if err := w.signalPayload(ctx, ticket, fence, "retry_or_correction_exhausted", nil, `{"reason":"amendment_unsupported"}`); err != nil {
			return false, false, err
		}
		return false, false, ErrAmendmentUnsupported
	}
	if err := w.persistCandidate(ctx, ticket, fence, request, planIdentity, verificationIdentity, verification, builder, out.ProviderResult); err != nil {
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
	return w.signalPayload(ctx, ticket, fence, trigger, attributes, "{}")
}
func (w Worker) signalPlan(ctx context.Context, ticket store.Ticket, fence domain.Fence) error {
	if fence.RunnerEpoch != ticket.RunnerEpoch {
		return store.ErrStaleFence
	}
	_, err := w.Engine.SignalPlan(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: "phase_pass", Fence: fence, Attributes: map[string]string{"typed_plan_valid": "true"}, EventPayload: "{}"})
	return err
}
func (w Worker) signalVerification(ctx context.Context, ticket store.Ticket, fence domain.Fence) error {
	if fence.RunnerEpoch != ticket.RunnerEpoch {
		return store.ErrStaleFence
	}
	_, err := w.Engine.SignalVerification(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: "phase_pass", Fence: fence, Attributes: map[string]string{"independent_intent_valid": "true", "prebuild_proof_valid": "true", "verification_checkpoint_committed": "true"}, EventPayload: "{}"})
	return err
}
func (w Worker) signalPayload(ctx context.Context, ticket store.Ticket, fence domain.Fence, trigger string, attributes map[string]string, payload string) error {
	if fence.RunnerEpoch != ticket.RunnerEpoch {
		return store.ErrStaleFence
	}
	_, err := w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: trigger, Fence: fence, Attributes: attributes, EventPayload: payload})
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
	_, parsed, err := w.Evidence.LoadHistoricalProviderAttemptResult(ctx, *plan.Document.ProviderResult)
	if err != nil || parsed.Planner == nil || parsed.Phase != domain.PhasePlanning {
		return workflowprompt.PlanIdentity{}, ErrStaleEvidence
	}
	stored, _, storedErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: parsed.Provider, Planner: plan.Document.Planner})
	loaded, _, loadedErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: parsed.Provider, Planner: parsed.Planner})
	if storedErr != nil || loadedErr != nil || string(stored) != string(loaded) {
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
	if verification.ProviderResult.AttemptID == 0 || verification.ProviderResult.Phase != domain.PhaseVerification || verification.Checkpoint.CommitOID != verification.Revision.CheckpointID || verification.Checkpoint.ParentOID == "" || verification.Checkpoint.TreeOID == "" {
		return workflowprompt.VerificationIdentity{}, ErrStaleEvidence
	}
	result, parsed, err := w.Evidence.LoadHistoricalProviderAttemptResult(ctx, verification.ProviderResult)
	if err != nil || result.Claim.Role != "reviewer" || parsed.Verify == nil {
		return workflowprompt.VerificationIdentity{}, ErrStaleEvidence
	}
	artifact := *parsed.Verify
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
	if err := w.Checkpoint.AuthenticateVerificationCheckpoint(ctx, request, artifact, VerificationCheckpoint{ID: verification.Revision.CheckpointID, Commit: verification.Checkpoint}); err != nil {
		return workflowprompt.VerificationIdentity{}, err
	}
	return identity, nil
}

func (w Worker) persistVerification(ctx context.Context, ticket store.Ticket, fence domain.Fence, request PhaseRequest, plan workflowprompt.PlanIdentity, artifact phaseartifact.Verification, key store.ProviderAttemptResultKey) error {
	result, parsed, err := w.Evidence.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || key.Ref != ticket.Ref || key.Phase != domain.PhaseVerification || result.Claim.Ref != ticket.Ref || result.Claim.Phase != domain.PhaseVerification || result.Claim.Role != "reviewer" || result.Claim.Attempt != key.Attempt || result.Claim.ID != key.AttemptID || parsed.Verify == nil {
		return ErrStaleEvidence
	}
	loaded, _, loadErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhaseVerification, Provider: parsed.Provider, Verify: parsed.Verify})
	want, _, wantErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhaseVerification, Provider: parsed.Provider, Verify: &artifact})
	if loadErr != nil || wantErr != nil || string(loaded) != string(want) {
		return ErrStaleEvidence
	}
	if request.Phase != domain.PhaseVerification || request.Ticket.Ref != ticket.Ref || request.Ticket.Version != ticket.Version || request.Fence != fence || request.Plan == nil {
		return ErrStaleEvidence
	}
	if w.CheckpointMaterializer == nil || w.Checkpoint == nil {
		return ErrCheckpointRequired
	}
	checkpoint, err := w.CheckpointMaterializer.MaterializeVerificationCheckpoint(ctx, request, artifact, key)
	if err != nil {
		return err
	}
	if checkpoint.ID == "" || checkpoint.ID != checkpoint.Commit.CommitOID || checkpoint.Commit.ParentOID == "" || checkpoint.Commit.TreeOID == "" {
		return ErrCheckpointRequired
	}
	if checkpoint.CommandResult.SemanticKey == "" || checkpoint.CommandResult.ClaimEpoch == 0 {
		return ErrCheckpointRequired
	}
	intent, err := canonicalVerificationIntent(artifact)
	if err != nil {
		return err
	}
	proof, err := canonicalVerificationProof(artifact)
	if err != nil {
		return err
	}
	intentDigest, err := workflowprompt.VerificationIntentDigest(artifact)
	if err != nil {
		return err
	}
	proofDigest, err := workflowprompt.VerificationProofDigest(artifact)
	if err != nil {
		return err
	}
	identity, err := workflowprompt.NewVerificationIdentity(artifact, intentDigest, proofDigest, checkpoint.ID)
	if err != nil {
		return err
	}
	if _, err = workflowprompt.ValidateVerificationIdentity(workflowTicket(ticket), plan, identity); err != nil {
		return err
	}
	if err = w.Checkpoint.AuthenticateVerificationCheckpoint(ctx, request, artifact, checkpoint); err != nil {
		return err
	}
	_, err = w.Evidence.RecordVerification(ctx, store.VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: artifact.OwnedFiles, CheckpointID: checkpoint.ID, ProviderResult: &key, Checkpoint: checkpoint.Commit, CommandResult: checkpoint.CommandResult})
	return err
}

func (w Worker) persistCandidate(ctx context.Context, ticket store.Ticket, fence domain.Fence, request PhaseRequest, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, stored store.StoredVerification, builder phaseartifact.Builder, key store.ProviderAttemptResultKey) error {
	result, parsed, err := w.Evidence.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || key.Ref != ticket.Ref || key.Phase != domain.PhaseBuild || result.Claim.Ref != ticket.Ref || result.Claim.Phase != domain.PhaseBuild || result.Claim.Role != "builder" || result.Claim.ID != key.AttemptID || result.Claim.Attempt != key.Attempt || parsed.Builder == nil {
		return ErrStaleEvidence
	}
	if request.Phase == "" {
		request, err = w.request(ctx, ticket, fence, domain.PhaseBuild, nil, &stored, nil)
		if err != nil {
			return err
		}
	}
	if w.CandidateMaterializer == nil || w.Candidate == nil {
		return ErrCandidateRequired
	}
	witness, err := w.CandidateMaterializer.MaterializeCandidate(ctx, request, plan, verification, builder, key)
	if err != nil {
		return err
	}
	if witness.Reason == "" || witness.CommandPolicyDigest == "" || witness.Commit.CommitOID == "" || witness.Commit.TreeOID == "" {
		return ErrCandidateRequired
	}
	if err = w.Candidate.AuthenticateCandidate(ctx, request, plan, verification, builder, witness); err != nil {
		return err
	}
	digest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil {
		return err
	}
	_, err = w.Evidence.RecordCandidate(ctx, store.CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, BuilderResult: key, Commit: witness.Commit, Reason: witness.Reason, Snapshot: domain.CandidateSnapshot{BaseSHA: request.Worktree.BaseSHA, HeadSHA: witness.Commit.CommitOID, TreeSHA: witness.Commit.TreeOID, SourceDigest: ticket.SourceDigest, VerificationIntentDigest: stored.Revision.IntentDigest, ProofDigest: stored.Revision.ProofDigest, CommandPolicyDigest: witness.CommandPolicyDigest, BuilderEvidenceDigest: digest}})
	return err
}

// A candidate replay is authorized only by its persisted exact Builder result
// key. The worker re-authenticates that key and the materialized Git witness;
// an absent, stale, or mismatched key fails closed rather than launching
// Builder again or accepting an ambiguous candidate.
func (w Worker) authenticateStoredCandidate(ctx context.Context, ticket store.Ticket, fence domain.Fence, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, candidate store.StoredCandidate) error {
	result, parsed, err := w.Evidence.LoadHistoricalProviderAttemptResult(ctx, candidate.BuilderResult)
	if err != nil || result.Claim.Ref != ticket.Ref || result.Claim.Phase != domain.PhaseBuild || result.Claim.Role != "builder" || result.Claim.ID != candidate.BuilderResult.AttemptID || result.Claim.Attempt != candidate.BuilderResult.Attempt || parsed.Builder == nil {
		return ErrStaleEvidence
	}
	digest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil || digest != candidate.Snapshot.BuilderEvidenceDigest {
		return ErrStaleEvidence
	}
	request, err := w.request(ctx, ticket, fence, domain.PhaseBuild, nil, nil, &candidate)
	if err != nil {
		return err
	}
	if w.CandidateMaterializer == nil || w.Candidate == nil {
		return ErrCandidateRequired
	}
	witness, err := w.CandidateMaterializer.MaterializeCandidate(ctx, request, plan, verification, *parsed.Builder, candidate.BuilderResult)
	if err != nil {
		return err
	}
	if witness.Commit.CommitOID != candidate.Commit.CommitOID || witness.Commit.TreeOID != candidate.Commit.TreeOID || witness.Commit.ParentOID != candidate.Commit.ParentOID || witness.CommandPolicyDigest != candidate.Snapshot.CommandPolicyDigest {
		return ErrStaleEvidence
	}
	return w.Candidate.AuthenticateCandidate(ctx, request, plan, verification, *parsed.Builder, witness)
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
