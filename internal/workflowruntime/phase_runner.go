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
	Plan(context.Context, domain.TicketRef) (store.StoredPlan, error)
	CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error)
}

// PhaseRunner adapts the qualified coordinator to workflowworker for the
// three pre-publishing phases. Final review is intentionally not admitted
// here: publishing owns its exact-head orchestration.
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
	default:
		return workflowworker.PhaseResult{}, ErrPhaseBoundaryUnavailable
	}
}

func (r PhaseRunner) verification(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	project, effective, err := r.admit(ctx, request, domain.StateVerifying, "reviewer")
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	_, identity, err := r.planIdentity(ctx, request, project)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	input, err := workflowprompt.Verification(workflowprompt.VerificationInput{
		Ticket:    phaseTicket(request.Ticket),
		Workspace: phaseWorkspace(project, request, identity.Plan.Paths),
		Plan:      identity,
		Runtime:   phaseRuntime(effective.PhaseTimeout),
	})
	if err != nil {
		return workflowworker.PhaseResult{}, ErrConfigSnapshotInvalid
	}
	return r.run(ctx, request, project, providercoord.RoleReviewer, input, phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: identity.Digest})
}

func (r PhaseRunner) build(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	project, effective, err := r.admit(ctx, request, domain.StateBuilding, "builder")
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	_, plan, err := r.planIdentity(ctx, request, project)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	verification, err := r.verificationIdentity(ctx, request, project, plan)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	input, err := workflowprompt.Builder(workflowprompt.BuilderInput{
		Ticket:       phaseTicket(request.Ticket),
		Workspace:    phaseWorkspace(project, request, plan.Plan.Paths),
		Plan:         plan,
		Verification: verification,
		Runtime:      phaseRuntime(effective.PhaseTimeout),
	})
	if err != nil {
		return workflowworker.PhaseResult{}, ErrConfigSnapshotInvalid
	}
	// ApprovedAmendmentDigest remains empty by design. Any attempt to modify a
	// protected verification path therefore fails in phaseartifact validation.
	return r.run(ctx, request, project, providercoord.RoleBuilder, input, phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: plan.Digest, ProtectedVerification: append([]string(nil), verification.OwnedFiles...)})
}

func (r PhaseRunner) admit(ctx context.Context, request workflowworker.PhaseRequest, state domain.State, route string) (store.Project, config.Effective, error) {
	if request.Ticket.Ref.Validate() != nil || request.Ticket.State != state || request.Ticket.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 {
		return store.Project{}, config.Effective{}, ErrIdentityMismatch
	}
	project, err := r.Store.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if err != nil {
		return store.Project{}, config.Effective{}, ErrIdentityMismatch
	}
	effective, err := decodeTicketConfig(request.Ticket)
	if err != nil {
		return store.Project{}, config.Effective{}, err
	}
	if project.Channel != request.Ticket.Ref.Channel || project.ID != request.Ticket.Ref.Project || project.Path != effective.Repository || project.BaseRef != effective.BaseBranch {
		return store.Project{}, config.Effective{}, ErrIdentityMismatch
	}
	if err := validateWorktree(request, project); err != nil {
		return store.Project{}, config.Effective{}, err
	}
	if !permittedMode(request.Ticket.MergeMode) || !permittedMode(effective.MergeMode) {
		return store.Project{}, config.Effective{}, ErrUnsupportedMode
	}
	var providers []string
	switch route {
	case "reviewer":
		providers = effective.Providers.Reviewer
	case "builder":
		providers = effective.Providers.Builder
	default:
		return store.Project{}, config.Effective{}, ErrPhaseBoundaryUnavailable
	}
	if len(providers) != 1 || providers[0] != "codex" {
		return store.Project{}, config.Effective{}, ErrProviderOrder
	}
	return project, effective, nil
}

func permittedMode(mode domain.MergeMode) bool {
	return mode == domain.MergeManual || mode == domain.MergeGuarded
}

func phaseTicket(ticket store.Ticket) workflowprompt.Ticket {
	return workflowprompt.Ticket{Channel: ticket.Ref.Channel, Project: ticket.Ref.Project, ID: ticket.Ref.Ticket, Type: ticket.Type, SourceDigest: ticket.SourceDigest, Body: plannerBody(ticket)}
}

func phaseWorkspace(project store.Project, request workflowworker.PhaseRequest, paths []string) workflowprompt.Workspace {
	return workflowprompt.Workspace{Repository: project.Path, Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), BaseSHA: request.Worktree.BaseSHA, AllowedPaths: append([]string(nil), paths...)}
}

func phaseRuntime(timeout time.Duration) workflowprompt.Runtime {
	if timeout > 10*time.Minute {
		timeout = 10 * time.Minute
	}
	return workflowprompt.Runtime{Timeout: timeout, Profile: contracts.ProfileGuarded}
}

func (r PhaseRunner) planIdentity(ctx context.Context, request workflowworker.PhaseRequest, project store.Project) (store.StoredPlan, workflowprompt.PlanIdentity, error) {
	plan, err := r.Store.Plan(ctx, request.Ticket.Ref)
	if err != nil || plan.TicketVersion != request.Ticket.Version || plan.Fence != request.Fence || plan.Document.Planner == nil || plan.Document.ProviderResult == nil {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	_, parsed, err := r.load(ctx, *plan.Document.ProviderResult, request, project, domain.PhasePlanning, providercoord.RolePlanner, nil, nil)
	if err != nil || parsed.Planner == nil || !reflect.DeepEqual(*plan.Document.Planner, *parsed.Planner) {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	identity, err := workflowprompt.NewPlanIdentity(*parsed.Planner)
	if err != nil || plan.Digest != identity.Digest {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	if _, err := workflowprompt.ValidatePlanIdentity(phaseTicket(request.Ticket), identity); err != nil {
		return store.StoredPlan{}, workflowprompt.PlanIdentity{}, ErrProviderResultInvalid
	}
	return plan, identity, nil
}

func (r PhaseRunner) verificationIdentity(ctx context.Context, request workflowworker.PhaseRequest, project store.Project, plan workflowprompt.PlanIdentity) (workflowprompt.VerificationIdentity, error) {
	verification, err := r.Store.CurrentVerification(ctx, request.Ticket.Ref)
	if err != nil || verification.TicketVersion != request.Ticket.Version || verification.Fence != request.Fence || verification.ProviderResult.AttemptID <= 0 || verification.ProviderResult.Ref != request.Ticket.Ref || verification.ProviderResult.Phase != domain.PhaseVerification || verification.Checkpoint.CommitOID != verification.Revision.CheckpointID || verification.Checkpoint.ParentOID == "" || verification.Checkpoint.TreeOID == "" {
		return workflowprompt.VerificationIdentity{}, ErrProviderResultInvalid
	}
	_, parsed, err := r.load(ctx, verification.ProviderResult, request, project, domain.PhaseVerification, providercoord.RoleReviewer, nil, nil)
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

func (r PhaseRunner) run(ctx context.Context, request workflowworker.PhaseRequest, project store.Project, role providercoord.Role, input contracts.PhaseInput, validation phaseartifact.Validation) (workflowworker.PhaseResult, error) {
	result := r.Coordinator.Run(ctx, providercoord.Request{Role: role, Input: input, Validation: validation, ExpectedVersion: request.Ticket.Version, Fence: request.Fence, ConfigDigest: request.Ticket.ConfigDigest})
	if result.Code != providercoord.Completed || result.ProviderResult.AttemptID <= 0 {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || result.Code == providercoord.Canceled {
			return workflowworker.PhaseResult{}, ErrCanceled
		}
		return workflowworker.PhaseResult{}, ErrPlannerNotReady
	}
	key := result.ProviderResult
	_, parsed, err := r.load(ctx, key, request, project, request.Phase, role, &input, &validation)
	if err != nil || !parsedForPhase(parsed, request.Phase) {
		return workflowworker.PhaseResult{}, ErrProviderResultInvalid
	}
	return workflowworker.PhaseResult{ProviderResult: key}, nil
}

func (r PhaseRunner) load(ctx context.Context, key store.ProviderAttemptResultKey, request workflowworker.PhaseRequest, project store.Project, phase domain.Phase, role providercoord.Role, expectedInput *contracts.PhaseInput, expectedValidation *phaseartifact.Validation) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	result, parsed, err := r.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || key.AttemptID <= 0 || key.Ref != request.Ticket.Ref || key.Phase != phase || key.Attempt <= 0 || result.AttemptID != key.AttemptID || result.Claim.ID != key.AttemptID || result.Claim.Attempt != key.Attempt || result.Claim.Ref != request.Ticket.Ref || result.Claim.Phase != phase || result.Claim.Role != string(role) || result.Claim.ExpectedVersion != request.Ticket.Version || result.Claim.LeaderEpoch != request.Fence.LeaderEpoch || result.Claim.RunnerEpoch != request.Fence.RunnerEpoch || result.Claim.Repository != project.Path || result.Claim.Worktree != request.Worktree.Path || result.Claim.WorktreeIdentity != string(request.Worktree.IdentityJSON) || result.Claim.BaseSHA != request.Worktree.BaseSHA || result.Claim.Binding.Identity.Provider != "codex" || parsed.Phase != phase || parsed.Provider != result.Claim.Binding.Identity || parsed.Provider.Provider != "codex" || len(result.RawArtifact) == 0 || len(result.RawArtifact) > phaseartifact.MaxBytes {
		return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderResultInvalid
	}
	if expectedInput != nil || expectedValidation != nil {
		if expectedInput == nil || expectedValidation == nil || !matchesLaunchInput(result.Claim, key, *expectedInput) {
			return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderResultInvalid
		}
		if !matchesValidation(result.Validation, *expectedValidation) {
			return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderResultInvalid
		}
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
	payload, digest, err := contracts.CanonicalPhaseInput(input)
	if err != nil {
		return false
	}
	input.RequestDigest = digest
	return digest == claim.RequestDigest && bytes.Equal(payload, claim.RequestPayload) && reflect.DeepEqual(input, claim.Input)
}

func parsedForPhase(parsed phaseartifact.Parsed, phase domain.Phase) bool {
	switch phase {
	case domain.PhaseVerification:
		return parsed.Verify != nil
	case domain.PhaseBuild:
		return parsed.Builder != nil
	default:
		return false
	}
}

var _ workflowworker.PhaseRunner = PhaseRunner{}
