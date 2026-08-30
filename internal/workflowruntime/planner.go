package workflowruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
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

var (
	// ErrPhaseBoundaryUnavailable is returned for phases whose command-result
	// and prepared-commit boundaries are not part of this planning-only lane.
	ErrPhaseBoundaryUnavailable = errors.New("workflow phase boundary is unavailable")
	ErrConfigSnapshotInvalid    = errors.New("ticket configuration snapshot is invalid")
	ErrConfigDigestMismatch     = errors.New("ticket configuration digest is invalid")
	ErrIdentityMismatch         = errors.New("durable ticket/worktree identity mismatch")
	ErrUnsupportedMode          = errors.New("workflow mode is not supported by this runtime")
	ErrProviderOrder            = errors.New("planner provider order is not the exact codex route")
	ErrPlannerNotReady          = errors.New("qualified planner provider is not ready")
	ErrProviderResultInvalid    = errors.New("provider result is not Store-authenticated")
)

// PlannerEvidence is the complete Store surface needed by the planning
// adapter. It intentionally has no write methods and cannot authorize a state
// transition or Git operation.
type PlannerEvidence interface {
	Project(context.Context, domain.Channel, domain.ProjectID) (store.Project, error)
	LoadHistoricalProviderAttemptResult(context.Context, store.ProviderAttemptResultKey) (store.ProviderAttemptResult, phaseartifact.Parsed, error)
}

// PlannerCoordinator is the provider admission boundary. A coordinator owns
// qualification, process supervision, budget, and durable result publication.
type PlannerCoordinator interface {
	Run(context.Context, providercoord.Request) providercoord.Result
}

// PlannerRunner is a production-shaped planning-only workflowworker adapter.
// Store and Coordinator are injected to keep provider/Git authority outside
// this package.
type PlannerRunner struct {
	Store       PlannerEvidence
	Coordinator PlannerCoordinator
}

// PlannerAdapter is a descriptive alias for callers that prefer adapter
// terminology.
type PlannerAdapter = PlannerRunner

// PlannerResult contains the only provider data this boundary can expose:
// the Store-issued immutable result key and its bounded raw artifact. The
// worker consumes only Key; RawArtifact is available to a future evidence
// materializer and is never interpreted for paths, commands, or state here.
type PlannerResult struct {
	Key         store.ProviderAttemptResultKey
	RawArtifact []byte
}

func NewPlannerRunner(evidence PlannerEvidence, coordinator PlannerCoordinator) *PlannerRunner {
	return &PlannerRunner{Store: evidence, Coordinator: coordinator}
}

// Run implements workflowworker.PhaseRunner. RawArtifact is deliberately
// discarded at this interface: workflowworker accepts only the authenticated
// immutable result key.
func (r PlannerRunner) Run(ctx context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	result, err := r.RunArtifact(ctx, request)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	return workflowworker.PhaseResult{ProviderResult: result.Key}, nil
}

// RunArtifact performs planning admission and returns the authenticated key
// plus bounded artifact. It is separate from Run so no future phase runner can
// accidentally make raw provider bytes part of worker transition authority.
func (r PlannerRunner) RunArtifact(ctx context.Context, request workflowworker.PhaseRequest) (PlannerResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if request.Phase != domain.PhasePlanning {
		return PlannerResult{}, ErrPhaseBoundaryUnavailable
	}
	if err := ctx.Err(); err != nil {
		return PlannerResult{}, err
	}
	if r.Store == nil || r.Coordinator == nil {
		return PlannerResult{}, ErrPlannerNotReady
	}
	if request.Ticket.Ref.Validate() != nil || request.Ticket.State != domain.StatePlanning || request.Ticket.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 {
		return PlannerResult{}, ErrIdentityMismatch
	}
	project, err := r.Store.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if err != nil {
		return PlannerResult{}, ErrIdentityMismatch
	}
	effective, err := decodeTicketConfig(request.Ticket)
	if err != nil {
		return PlannerResult{}, err
	}
	if project.Channel != request.Ticket.Ref.Channel || project.ID != request.Ticket.Ref.Project || project.Path != effective.Repository || project.BaseRef != effective.BaseBranch {
		return PlannerResult{}, ErrIdentityMismatch
	}
	if err := validateWorktree(request, project); err != nil {
		return PlannerResult{}, err
	}
	if request.Ticket.MergeMode == domain.MergeAutonomous || effective.MergeMode == domain.MergeAutonomous {
		return PlannerResult{}, ErrUnsupportedMode
	}
	if len(effective.Providers.Planner) != 1 || effective.Providers.Planner[0] != "codex" {
		return PlannerResult{}, ErrProviderOrder
	}

	// Planner is read-only and can inspect the repository root. Cap the frozen
	// project timeout to workflowprompt's hard provider input bound.
	timeout := effective.PhaseTimeout
	if timeout > 10*time.Minute {
		timeout = 10 * time.Minute
	}
	input, err := workflowprompt.Planner(workflowprompt.PlannerInput{
		Ticket:    workflowprompt.Ticket{Channel: request.Ticket.Ref.Channel, Project: request.Ticket.Ref.Project, ID: request.Ticket.Ref.Ticket, Type: request.Ticket.Type, SourceDigest: request.Ticket.SourceDigest, Body: plannerBody(request.Ticket)},
		Workspace: workflowprompt.Workspace{Repository: project.Path, Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), BaseSHA: request.Worktree.BaseSHA, AllowedPaths: []string{"."}},
		Runtime:   workflowprompt.Runtime{Timeout: timeout, Profile: contracts.ProfileGuarded},
	})
	if err != nil {
		return PlannerResult{}, ErrConfigSnapshotInvalid
	}
	coordResult := r.Coordinator.Run(ctx, providercoord.Request{Role: providercoord.RolePlanner, Input: input, Validation: phaseartifact.Validation{TicketType: request.Ticket.Type}, ExpectedVersion: request.Ticket.Version, Fence: request.Fence, ConfigDigest: request.Ticket.ConfigDigest})
	if coordResult.Code != providercoord.Completed || coordResult.ProviderResult.AttemptID <= 0 {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) || coordResult.Code == providercoord.Canceled {
			return PlannerResult{}, ErrCanceled
		}
		return PlannerResult{}, ErrPlannerNotReady
	}
	key := coordResult.ProviderResult
	result, parsed, err := r.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || key.Ref != request.Ticket.Ref || key.Phase != domain.PhasePlanning || key.Attempt <= 0 || result.AttemptID != key.AttemptID || result.Claim.Ref != request.Ticket.Ref || result.Claim.Phase != domain.PhasePlanning || result.Claim.Role != string(workflowprompt.RolePlanner) || result.Claim.ExpectedVersion != request.Ticket.Version || result.Claim.LeaderEpoch != request.Fence.LeaderEpoch || result.Claim.RunnerEpoch != request.Fence.RunnerEpoch || result.Claim.Repository != project.Path || result.Claim.Worktree != request.Worktree.Path || result.Claim.WorktreeIdentity != string(request.Worktree.IdentityJSON) || result.Claim.BaseSHA != request.Worktree.BaseSHA || parsed.Phase != domain.PhasePlanning || parsed.Planner == nil || len(result.RawArtifact) == 0 || len(result.RawArtifact) > phaseartifact.MaxBytes {
		return PlannerResult{}, ErrProviderResultInvalid
	}
	if parsed.Provider.Provider != "codex" {
		return PlannerResult{}, ErrProviderResultInvalid
	}
	return PlannerResult{Key: key, RawArtifact: append([]byte(nil), result.RawArtifact...)}, nil
}

// plannerBody is reconstructed solely from the immutable ticket snapshot. A
// parsed Problem alone can omit title, acceptance criteria, or source details
// that are material to planning; retaining all fields also makes this adapter
// independent of later repository-file changes.
func plannerBody(ticket store.Ticket) string {
	var body strings.Builder
	if len(ticket.Source) != 0 {
		body.Write(ticket.Source)
	}
	if ticket.Title != "" {
		body.WriteString("\nTitle: ")
		body.WriteString(ticket.Title)
	}
	if ticket.Problem != "" {
		body.WriteString("\nProblem: ")
		body.WriteString(ticket.Problem)
	}
	if len(ticket.Acceptance) != 0 {
		body.WriteString("\nAcceptance:\n")
		for _, item := range ticket.Acceptance {
			body.WriteString("- ")
			body.WriteString(item)
			body.WriteByte('\n')
		}
	}
	return body.String()
}

func decodeTicketConfig(ticket store.Ticket) (config.Effective, error) {
	if ticket.ConfigGeneration == 0 || len(ticket.ConfigSnapshot) == 0 || ticket.ConfigDigest == "" {
		return config.Effective{}, ErrConfigSnapshotInvalid
	}
	if !digestMatches(ticket.ConfigSnapshot, ticket.ConfigDigest) {
		return config.Effective{}, ErrConfigDigestMismatch
	}
	value, err := config.DecodeSnapshot(ticket.ConfigSnapshot, ticket.ConfigDigest)
	if err != nil {
		return config.Effective{}, ErrConfigSnapshotInvalid
	}
	return value, nil
}

func digestMatches(data []byte, want string) bool {
	sum := sha256.Sum256(data)
	return len(want) == sha256.Size*2 && hex.EncodeToString(sum[:]) == want
}

func validateWorktree(request workflowworker.PhaseRequest, project store.Project) error {
	worktree := request.Worktree
	if worktree.State != "registered" || worktree.Path == "" || filepath.IsAbs(worktree.Path) == false || filepath.Clean(worktree.Path) != worktree.Path || worktree.Path == "/" || worktree.Branch == "" || worktree.TicketVersion != request.Ticket.Version || worktree.Fence != request.Fence || worktree.BaseSHA == "" || len(worktree.IdentityJSON) == 0 {
		return ErrIdentityMismatch
	}
	identity, err := workflowprompt.ValidateCanonicalWorktreeIdentity(worktree.IdentityJSON)
	if err != nil || identity.Repository != project.Path || identity.Worktree != worktree.Path || identity.BaseRef != project.BaseRef || identity.BaseHead != worktree.BaseSHA || identity.HeadRef != worktree.Branch {
		return ErrIdentityMismatch
	}
	return nil
}

// Keep the interface assertion near the adapter: a concrete Store and Worker
// composition can be assembled without a package-level daemon dependency.
var _ workflowworker.PhaseRunner = PlannerRunner{}
