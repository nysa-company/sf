package workflowruntime

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// PhaseEvidence is the read-only Store surface needed by the production
// provider bridge. It deliberately excludes all state-transition, Git, and
// evidence-writing authority.
type PhaseEvidence interface {
	PlannerEvidence
	Ticket(context.Context, domain.TicketRef) (store.Ticket, error)
	Worktree(context.Context, domain.TicketRef) (store.StoredWorktree, error)
	Plan(context.Context, domain.TicketRef) (store.StoredPlan, error)
	CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error)
	RecoverableVerification(context.Context, domain.TicketRef) (store.StoredVerification, error)
	LatestReusableProviderAttempt(context.Context, store.LatestReusableProviderAttemptRequest) (store.LatestReusableProviderAttemptResult, error)
	OperatorSourceResumeRequiresFreshVerification(context.Context, domain.TicketRef, uint64) (bool, error)
	OperatorSourceResumeProof(context.Context, domain.TicketRef, uint64, domain.Fence) (store.OperatorSourceResumeProof, bool, error)
	PendingVerificationAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (store.VerificationAmendment, error)
	FinalReviewAuthority(context.Context, domain.TicketRef, uint64, domain.Fence) (store.FinalReviewAuthority, error)
	AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error
	LoadCurrentProviderAttemptResult(context.Context, store.ProviderAttemptResultKey, uint64, domain.Fence) (store.ProviderAttemptResult, phaseartifact.Parsed, error)
}

// PhaseRunner adapts the qualified coordinator to workflowworker for every
// provider phase that has Store-authenticated evidence. Publication remains
// outside this boundary; review is admitted only from FinalReviewAuthority.
type PhaseRunner struct {
	Store       PhaseEvidence
	Coordinator PlannerCoordinator
}

func NewPhaseRunner(evidence PhaseEvidence, coordinator PlannerCoordinator) *PhaseRunner {
	return &PhaseRunner{Store: evidence, Coordinator: coordinator}
}

// Run returns only a Store-issued immutable result key. In particular it
// never exposes Coordinator.Parsed or any coordinator-owned raw bytes.
func (r PhaseRunner) Run(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	if request.Phase == domain.PhasePlanning {
		return PlannerRunner{Store: r.Store, Coordinator: r.Coordinator}.Run(ctx, request)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return workflowworker.PhaseResult{}, err
	}
	if r.Store == nil || r.Coordinator == nil {
		return workflowworker.PhaseResult{}, ErrPlannerNotReady
	}

	switch request.Phase {
	case domain.PhaseVerification:
		return r.verification(ctx, request)
	case domain.PhaseBuild:
		return r.build(ctx, request)
	case domain.PhaseReview:
		return r.finalReview(ctx, request)
	default:
		return workflowworker.PhaseResult{}, ErrPhaseBoundaryUnavailable
	}
}

func (r PhaseRunner) finalReview(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	if request.Candidate == nil {
		return workflowworker.PhaseResult{}, ErrIdentityMismatch
	}
	project, effective, worktree, err := r.admit(ctx, request, domain.StateReviewing, "reviewer")
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	authority, err := r.Store.FinalReviewAuthority(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil || authority.Candidate.Snapshot != request.Candidate.Snapshot {
		return workflowworker.PhaseResult{}, ErrProviderResultInvalid
	}
	_, plan, err := r.planIdentity(ctx, request, project, worktree)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	verification, err := r.finalVerificationIdentity(ctx, request, project, worktree, plan, authority.Verification)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	_, parsed, err := r.loadHistorical(ctx, authority.Candidate.BuilderResult, request.Ticket.Ref, project, worktree, domain.PhaseBuild, providercoord.RoleBuilder)
	if err != nil || parsed.Builder == nil {
		return workflowworker.PhaseResult{}, ErrProviderResultInvalid
	}
	candidate, err := workflowprompt.NewCandidateIdentity(authority.Candidate.Snapshot.BaseSHA, authority.Candidate.Snapshot.HeadSHA, authority.Candidate.Snapshot.TreeSHA, authority.Candidate.Snapshot.SourceDigest, authority.Candidate.Snapshot.VerificationIntentDigest, authority.Candidate.Snapshot.ProofDigest, authority.Candidate.Snapshot.CommandPolicyDigest, *parsed.Builder, authority.Candidate.Snapshot.BuilderEvidenceDigest)
	if err != nil {
		return workflowworker.PhaseResult{}, ErrProviderResultInvalid
	}
	input, err := workflowprompt.FinalReviewer(workflowprompt.FinalReviewerInput{Ticket: phaseTicket(request.Ticket), Workspace: phaseWorkspace(project, worktree, plan.Plan.Paths), Plan: plan, Verification: verification, Candidate: candidate, Checks: authority.Checks, Runtime: phaseRuntime(effective.PhaseTimeout)})
	if err != nil {
		return workflowworker.PhaseResult{}, ErrConfigSnapshotInvalid
	}
	return r.run(ctx, request, project, worktree, providercoord.RoleReviewer, input, phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: plan.Digest, ExpectedReviewedHead: candidate.HeadSHA, ExpectedProofDigest: candidate.ProofDigest})
}

func (r PhaseRunner) verification(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	if request.Plan == nil {
		return workflowworker.PhaseResult{}, ErrIdentityMismatch
	}
	project, effective, worktree, err := r.admit(ctx, request, domain.StateVerifying, "reviewer")
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	freshSource, err := r.Store.OperatorSourceResumeRequiresFreshVerification(ctx, request.Ticket.Ref, request.Ticket.Version)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	var sourceProof store.OperatorSourceResumeProof
	if freshSource {
		var found bool
		sourceProof, found, err = r.Store.OperatorSourceResumeProof(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
		if err != nil || !found || sourceProof.Verification.Revision.Revision == 0 || sourceProof.Verification.TicketVersion >= request.Ticket.Version {
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		}
	}
	if request.Amendment == nil {
		// The amendment is a Store-owned mode switch, not an optional prompt
		// decoration. A direct PhaseRunner caller must not bypass a pending
		// Builder request simply by omitting its projection from PhaseRequest.
		if _, amendmentErr := r.Store.PendingVerificationAmendment(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence); amendmentErr == nil {
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		} else if !errors.Is(amendmentErr, store.ErrNotFound) {
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		}
		verification, readErr := r.Store.CurrentVerification(ctx, request.Ticket.Ref)
		if freshSource && errors.Is(readErr, store.ErrEvidenceConflict) {
			verification, readErr = r.Store.RecoverableVerification(ctx, request.Ticket.Ref)
		}
		if readErr != nil && !errors.Is(readErr, store.ErrNotFound) {
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		}
		if readErr == nil && (!freshSource || verification.Revision.Revision != sourceProof.Verification.Revision.Revision) {
			// A verifier is only launched before a verification is recorded.  A
			// source resume retains one historical revision only to authenticate the
			// checkout.  Any distinct fresh durable revision must be rebound by
			// Worker, never re-run by this provider boundary.
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		}
		if freshSource {
			reusable, reusableErr := r.Store.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: request.Ticket.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: request.Ticket.Version, Fence: request.Fence})
			if reusableErr == nil && reusable.Key != sourceProof.Verification.ProviderResult {
				// The Reviewer has already returned, but checkpoint materialization
				// has not yet been durably recorded. Worker owns that one-time write.
				return workflowworker.PhaseResult{}, ErrProviderResultInvalid
			}
			if reusableErr != nil && !errors.Is(reusableErr, store.ErrNotFound) {
				return workflowworker.PhaseResult{}, ErrProviderResultInvalid
			}
		}
	}
	if request.Amendment != nil {
		stored, amendmentErr := r.Store.PendingVerificationAmendment(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
		if amendmentErr != nil || !reflect.DeepEqual(stored, *request.Amendment) {
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		}
		// A completed amendment Reviewer can survive a daemon recovery with its
		// original fence. Reuse only one that descends from this request's
		// Builder->verifying endpoint; LatestReusable proves the recovery ledger
		// to the live fence. In particular, an earlier ordinary verification result
		// is not amendment authority and does not suppress the required review.
		if reusable, reusableErr := r.Store.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{Ref: request.Ticket.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: request.Ticket.Version, Fence: request.Fence}); reusableErr == nil {
			if stored.TransitionTicketVersion != 0 && reusable.Result.Claim.ExpectedVersion >= stored.TransitionTicketVersion {
				return workflowworker.PhaseResult{ProviderResult: reusable.Key}, nil
			}
		} else if !errors.Is(reusableErr, store.ErrNotFound) {
			return workflowworker.PhaseResult{}, ErrProviderResultInvalid
		}
	}
	_, identity, err := r.planIdentity(ctx, request, project, worktree)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	input, err := workflowprompt.Verification(workflowprompt.VerificationInput{
		Ticket:    phaseTicket(request.Ticket),
		Workspace: phaseWorkspace(project, worktree, identity.Plan.Paths),
		Plan:      identity,
		Runtime:   phaseRuntime(effective.PhaseTimeout),
		Amendment: amendmentPrompt(request.Amendment),
	})
	if err != nil {
		return workflowworker.PhaseResult{}, ErrConfigSnapshotInvalid
	}
	return r.run(ctx, request, project, worktree, providercoord.RoleReviewer, input, phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: identity.Digest})
}

func amendmentPrompt(value *store.VerificationAmendment) *workflowprompt.AmendmentReview {
	if value == nil {
		return nil
	}
	return &workflowprompt.AmendmentReview{PriorProofDigest: value.Prior.ProofDigest, ProposedDigest: value.ProposedDigest, ProposedCommand: append([]string(nil), value.ProposedCommand...), Reason: value.Reason, Requester: value.Requester}
}

func (r PhaseRunner) build(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	if request.Plan == nil || request.Verification == nil {
		return workflowworker.PhaseResult{}, ErrIdentityMismatch
	}
	project, effective, worktree, err := r.admit(ctx, request, domain.StateBuilding, "builder")
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	_, plan, err := r.planIdentity(ctx, request, project, worktree)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	verification, err := r.verificationIdentity(ctx, request, project, worktree, plan)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	input, err := workflowprompt.Builder(workflowprompt.BuilderInput{
		Ticket:       phaseTicket(request.Ticket),
		Workspace:    phaseWorkspace(project, worktree, plan.Plan.Paths),
		Plan:         plan,
		Verification: verification,
		Runtime:      phaseRuntime(effective.PhaseTimeout),
	})
	if err != nil {
		return workflowworker.PhaseResult{}, ErrConfigSnapshotInvalid
	}
	// A protected verification change is admissible only as a bounded Builder
	// amendment request. The Store records and charges that request before a
	// fresh independent Reviewer may accept the exact proposed proof.
	return r.run(ctx, request, project, worktree, providercoord.RoleBuilder, input, phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: plan.Digest, ProtectedVerification: append([]string(nil), verification.OwnedFiles...)})
}

func (r PhaseRunner) admit(ctx context.Context, request workflowworker.PhaseRequest, state domain.State, route string) (store.Project, config.Effective, store.StoredWorktree, error) {
	if request.Ticket.Ref.Validate() != nil || request.Ticket.State != state || request.Ticket.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrIdentityMismatch
	}
	project, err := r.Store.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if err != nil {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrIdentityMismatch
	}
	current, err := r.Store.Ticket(ctx, request.Ticket.Ref)
	if err != nil || !reflect.DeepEqual(current, request.Ticket) {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrIdentityMismatch
	}
	effective, err := decodeTicketConfig(request.Ticket)
	if err != nil {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, err
	}
	if project.Channel != request.Ticket.Ref.Channel || project.ID != request.Ticket.Ref.Project || project.Path != effective.Repository || project.BaseRef != effective.BaseBranch {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrIdentityMismatch
	}
	worktree, err := r.Store.Worktree(ctx, request.Ticket.Ref)
	if err != nil || !reflect.DeepEqual(request.Worktree, worktree) || validateHistoricalWorktree(worktree, project) != nil {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrIdentityMismatch
	}
	if !permittedMode(request.Ticket.MergeMode) || !permittedMode(effective.MergeMode) {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrUnsupportedMode
	}
	var providers []string
	switch route {
	case "reviewer":
		providers = effective.Providers.Reviewer
	case "builder":
		providers = effective.Providers.Builder
	default:
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrPhaseBoundaryUnavailable
	}
	if len(providers) != 1 || providers[0] != "codex" {
		return store.Project{}, config.Effective{}, store.StoredWorktree{}, ErrProviderOrder
	}
	return project, effective, worktree, nil
}

func permittedMode(mode domain.MergeMode) bool {
	return mode == domain.MergeManual || mode == domain.MergeGuarded
}

func phaseTicket(ticket store.Ticket) workflowprompt.Ticket {
	return workflowprompt.Ticket{Channel: ticket.Ref.Channel, Project: ticket.Ref.Project, ID: ticket.Ref.Ticket, Type: ticket.Type, SourceDigest: ticket.SourceDigest, Body: plannerBody(ticket)}
}

func phaseWorkspace(project store.Project, worktree store.StoredWorktree, paths []string) workflowprompt.Workspace {
	return workflowprompt.Workspace{Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, AllowedPaths: append([]string(nil), paths...)}
}

func phaseRuntime(timeout time.Duration) workflowprompt.Runtime {
	if timeout > 10*time.Minute {
		timeout = 10 * time.Minute
	}
	return workflowprompt.Runtime{Timeout: timeout, Profile: contracts.ProfileGuarded}
}

func (r PhaseRunner) planIdentity(ctx context.Context, request workflowworker.PhaseRequest, project store.Project, worktree store.StoredWorktree) (store.StoredPlan, workflowprompt.PlanIdentity, error) {
	// Plan returns the Store-selected append-only binding for this immutable
	// document. The request pointer is only a scheduler snapshot; it must
	// exactly match that Store value and must never select a different plan.
	plan, err := r.Store.Plan(ctx, request.Ticket.Ref)
	if err != nil || request.Plan != nil && !reflect.DeepEqual(*request.Plan, plan) || plan.Document.Planner == nil || plan.Document.ProviderResult == nil {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	_, parsed, err := r.loadHistorical(ctx, *plan.Document.ProviderResult, request.Ticket.Ref, project, worktree, domain.PhasePlanning, providercoord.RolePlanner)
	if err != nil || parsed.Planner == nil {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	stored, _, storedErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: parsed.Provider, Planner: plan.Document.Planner})
	loaded, _, loadedErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: parsed.Provider, Planner: parsed.Planner})
	if storedErr != nil || loadedErr != nil || !bytes.Equal(stored, loaded) {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	identity, err := workflowprompt.NewPlanIdentity(*parsed.Planner)
	// Store.Plan.Digest authenticates the complete durable PlanDocument (and
	// therefore includes its provider-result binding); PlanIdentity.Digest
	// authenticates only the canonical Planner artifact. They intentionally
	// have different preimages.
	if err != nil || plan.Digest == "" {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	if _, err := workflowprompt.ValidatePlanIdentity(phaseTicket(request.Ticket), identity); err != nil {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	return plan, identity, nil
}

func (r PhaseRunner) verificationIdentity(ctx context.Context, request workflowworker.PhaseRequest, project store.Project, worktree store.StoredWorktree, plan workflowprompt.PlanIdentity) (workflowprompt.VerificationIdentity, error) {
	// CurrentVerification likewise resolves the Store's append-only binding
	// (including an authorized recovery rebind), rather than accepting a
	// caller-selected revision or provider key.
	verification, err := r.Store.CurrentVerification(ctx, request.Ticket.Ref)
	if err != nil || request.Verification != nil && !reflect.DeepEqual(*request.Verification, verification) || verification.ProviderResult.AttemptID <= 0 || verification.ProviderResult.Ref != request.Ticket.Ref || verification.ProviderResult.Phase != domain.PhaseVerification || verification.Checkpoint.CommitOID != verification.Revision.CheckpointID || verification.Checkpoint.ParentOID == "" || verification.Checkpoint.TreeOID == "" {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	_, parsed, err := r.loadHistorical(ctx, verification.ProviderResult, request.Ticket.Ref, project, worktree, domain.PhaseVerification, providercoord.RoleReviewer)
	if err != nil || parsed.Verify == nil {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	identity, err := workflowprompt.NewVerificationIdentity(*parsed.Verify, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID)
	if err != nil || !reflect.DeepEqual(identity.OwnedFiles, verification.Revision.OwnedFiles) {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	if _, err := workflowprompt.ValidateVerificationIdentity(phaseTicket(request.Ticket), plan, identity); err != nil {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	return identity, nil
}

func (r PhaseRunner) finalVerificationIdentity(ctx context.Context, request workflowworker.PhaseRequest, project store.Project, worktree store.StoredWorktree, plan workflowprompt.PlanIdentity, verification store.StoredVerification) (workflowprompt.VerificationIdentity, error) {
	if verification.ProviderResult.AttemptID <= 0 || verification.ProviderResult.Ref != request.Ticket.Ref || verification.ProviderResult.Phase != domain.PhaseVerification || verification.Checkpoint.CommitOID != verification.Revision.CheckpointID || verification.Checkpoint.ParentOID == "" || verification.Checkpoint.TreeOID == "" {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	_, parsed, err := r.loadHistorical(ctx, verification.ProviderResult, request.Ticket.Ref, project, worktree, domain.PhaseVerification, providercoord.RoleReviewer)
	if err != nil || parsed.Verify == nil {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	identity, err := workflowprompt.NewVerificationIdentity(*parsed.Verify, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID)
	if err != nil || !reflect.DeepEqual(identity.OwnedFiles, verification.Revision.OwnedFiles) {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	if _, err := workflowprompt.ValidateVerificationIdentity(phaseTicket(request.Ticket), plan, identity); err != nil {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	return identity, nil
}

func (r PhaseRunner) run(ctx context.Context, request workflowworker.PhaseRequest, project store.Project, worktree store.StoredWorktree, role providercoord.Role, input contracts.PhaseInput, validation phaseartifact.Validation) (workflowworker.PhaseResult, error) {
	// This is intentionally the last operation before entering the coordinator.
	// Historical predecessor evidence may have old fences; the newly launched
	// attempt never may.
	if err := r.Store.AssertTicketFence(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence); err != nil {
		return workflowworker.PhaseResult{}, ErrIdentityMismatch
	}
	result := r.Coordinator.Run(ctx, providercoord.Request{Role: role, Input: input, Validation: validation, ExpectedVersion: request.Ticket.Version, Fence: request.Fence, ConfigDigest: request.Ticket.ConfigDigest})
	if result.Code != providercoord.Completed || result.ProviderResult.AttemptID <= 0 {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || result.Code == providercoord.Canceled {
			return workflowworker.PhaseResult{}, ErrCanceled
		}
		if result.Code == providercoord.AttemptExhausted {
			return workflowworker.PhaseResult{}, workflowworker.ErrProviderAttemptExhausted
		}
		if result.Code == providercoord.BudgetExhausted {
			return workflowworker.PhaseResult{}, workflowworker.ErrTicketBudgetExhausted
		}
		switch result.Code {
		case providercoord.ResultIndeterminate:
			return workflowworker.PhaseResult{}, workflowworker.ErrProviderResultIndeterminate
		case providercoord.RepairUnavailable:
			return workflowworker.PhaseResult{}, workflowworker.ErrProviderRepairUnavailable
		}
		return workflowworker.PhaseResult{}, ErrPlannerNotReady
	}
	key := result.ProviderResult
	_, parsed, err := r.loadCurrent(ctx, key, request, project, worktree, role, input, validation)
	if err != nil || !parsedForPhase(parsed, request.Phase) {
		return workflowworker.PhaseResult{}, ErrProviderResultInvalid
	}
	return workflowworker.PhaseResult{ProviderResult: key}, nil
}

func (r PhaseRunner) loadHistorical(ctx context.Context, key store.ProviderAttemptResultKey, ref domain.TicketRef, project store.Project, worktree store.StoredWorktree, phase domain.Phase, role providercoord.Role) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	result, parsed, err := r.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || key.AttemptID <= 0 || key.Ref != ref || key.Phase != phase || key.Attempt <= 0 || result.AttemptID != key.AttemptID || result.Claim.ID != key.AttemptID || result.Claim.Attempt != key.Attempt || result.Claim.Ref != ref || result.Claim.Phase != phase || result.Claim.Role != string(role) || result.Claim.ExpectedVersion == 0 || result.Claim.LeaderEpoch == 0 || result.Claim.RunnerEpoch == 0 || result.Claim.Repository != project.Path || result.Claim.Worktree != worktree.Path || result.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || result.Claim.BaseSHA != worktree.BaseSHA || result.Claim.Binding.Identity.Provider != "codex" || parsed.Phase != phase || parsed.Provider != result.Claim.Binding.Identity || parsed.Provider.Provider != "codex" || len(result.RawArtifact) == 0 || len(result.RawArtifact) > phaseartifact.MaxBytes {
		return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderResultInvalid
	}
	return result, parsed, nil
}

func (r PhaseRunner) loadCurrent(ctx context.Context, key store.ProviderAttemptResultKey, request workflowworker.PhaseRequest, project store.Project, worktree store.StoredWorktree, role providercoord.Role, input contracts.PhaseInput, validation phaseartifact.Validation) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	result, parsed, err := r.Store.LoadCurrentProviderAttemptResult(ctx, key, request.Ticket.Version, request.Fence)
	if err != nil || key.AttemptID <= 0 || key.Ref != request.Ticket.Ref || key.Phase != request.Phase || key.Attempt <= 0 || result.AttemptID != key.AttemptID || result.Claim.ID != key.AttemptID || result.Claim.Attempt != key.Attempt || result.Claim.Ref != request.Ticket.Ref || result.Claim.Phase != request.Phase || result.Claim.Role != string(role) || result.Claim.ExpectedVersion != request.Ticket.Version || result.Claim.LeaderEpoch != request.Fence.LeaderEpoch || result.Claim.RunnerEpoch != request.Fence.RunnerEpoch || result.Claim.Repository != project.Path || result.Claim.Worktree != worktree.Path || result.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || result.Claim.BaseSHA != worktree.BaseSHA || result.Claim.Binding.Identity.Provider != "codex" || parsed.Phase != request.Phase || parsed.Provider != result.Claim.Binding.Identity || parsed.Provider.Provider != "codex" || len(result.RawArtifact) == 0 || len(result.RawArtifact) > phaseartifact.MaxBytes || !matchesLaunchInput(result.Claim, key, input) || !matchesValidation(result.Validation, validation) {
		return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderResultInvalid
	}
	return result, parsed, nil
}

func matchesValidation(actual []byte, expected phaseartifact.Validation) bool {
	canonical, _, err := phaseartifact.CanonicalValidation(expected)
	return err == nil && bytes.Equal(actual, canonical)
}

// matchesLaunchInput binds the returned immutable key to this exact prompt
// construction. Coordinator is allowed to choose the durable provider binding
// and attempt number, but not to substitute any other phase input.
func matchesLaunchInput(claim store.ProviderAttemptClaim, key store.ProviderAttemptResultKey, input contracts.PhaseInput) bool {
	input.Provider = claim.Binding.Identity
	input.AuthMode = claim.Binding.AuthMode
	input.Attempt = key.Attempt
	input.LeaderEpoch = claim.LeaderEpoch
	input.RunnerEpoch = claim.RunnerEpoch
	input.ExpectedVersion = claim.ExpectedVersion
	// Repair is Store-owned launch context. The caller submits the same logical
	// phase request, while the immutable claim carries the exact prior-attempt
	// binding that made this one bounded repair admissible.
	input.Repair = claim.Input.Repair
	// Coordinator may constrain a launch to the ticket's remaining duration.
	// That is the sole permitted post-prompt difference; all other launch
	// input fields remain byte-for-byte bound by the canonical request digest.
	if claim.Input.Timeout <= 0 || claim.Input.Timeout > input.Timeout {
		return false
	}
	input.Timeout = claim.Input.Timeout
	payload, digest, err := contracts.CanonicalPhaseInput(claim.Input)
	if err != nil {
		return false
	}
	return digest == claim.RequestDigest && bytes.Equal(payload, claim.RequestPayload) && contracts.PhaseInputMatchesAuthenticatedClaim(input, claim.Input, claim.RequestDigest)
}

func parsedForPhase(parsed phaseartifact.Parsed, phase domain.Phase) bool {
	switch phase {
	case domain.PhaseVerification:
		return parsed.Verify != nil
	case domain.PhaseBuild:
		return parsed.Builder != nil
	case domain.PhaseReview:
		return parsed.Reviewer != nil
	default:
		return false
	}
}

var _ workflowworker.PhaseRunner = PhaseRunner{}
