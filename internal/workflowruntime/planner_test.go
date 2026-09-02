package workflowruntime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

type fakePlannerStore struct {
	project store.Project
	result  store.ProviderAttemptResult
	parsed  phaseartifact.Parsed
}

func (f fakePlannerStore) Project(context.Context, domain.Channel, domain.ProjectID) (store.Project, error) {
	return f.project, nil
}
func (f fakePlannerStore) LoadHistoricalProviderAttemptResult(context.Context, store.ProviderAttemptResultKey) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	return f.result, f.parsed, nil
}

type fakePlannerCoordinator struct {
	request providercoord.Request
	result  providercoord.Result
	calls   int
	onRun   func(providercoord.Request)
}

func (f *fakePlannerCoordinator) Run(_ context.Context, request providercoord.Request) providercoord.Result {
	f.calls++
	f.request = request
	if f.onRun != nil {
		f.onRun(request)
	}
	return f.result
}

func plannerFixture(t *testing.T) (workflowworker.PhaseRequest, *fakePlannerStore, *fakePlannerCoordinator) {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-1"}
	projectConfig := config.DefaultProject("p", "/repo")
	effective, err := config.Resolve(config.DefaultMachineLimits(), projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	src := sha256.Sum256([]byte("ticket"))
	ticket := store.Ticket{Ref: ref, State: domain.StatePlanning, Version: 4, RunnerEpoch: 7, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, SourceDigest: fmtDigest(src), Source: []byte("ticket"), Title: "important title", Acceptance: []string{"must retain acceptance"}, Problem: "make it better", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}
	identity := git.Identity{Repository: "/repo", RepositoryDev: 1, RepositoryIno: 2, Worktree: "/worktree", WorktreeDev: 3, WorktreeIno: 4, GitFile: "gitdir: /repo/.git/worktrees/SF-1\n", GitFileDev: 5, GitFileIno: 6, CommonDir: "/repo/.git", CommonDirDev: 7, CommonDirIno: 8, Origin: "https://example.invalid/repo", PushOrigin: "https://example.invalid/repo", BaseRef: "main", BaseHead: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", HeadRef: "dev/p/SF-1", ConfigHash: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", HooksHash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	identityJSON, err := workflowprompt.MarshalCanonicalWorktreeIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: 9, RunnerEpoch: 7}
	key := store.ProviderAttemptResultKey{AttemptID: 11, Ref: ref, Phase: domain.PhasePlanning, Attempt: 1}
	planner := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"accept"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "proof"}, Paths: []string{"."}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{}, Questions: []phaseartifact.Question{}}
	raw, _ := json.Marshal(planner)
	result := store.ProviderAttemptResult{AttemptID: key.AttemptID, RawArtifact: raw, Claim: store.ProviderAttemptClaim{ID: key.AttemptID, Ref: ref, Phase: domain.PhasePlanning, Role: "planner", Attempt: key.Attempt, ExpectedVersion: ticket.Version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: string(identityJSON), BaseSHA: identity.BaseHead, Binding: contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "codex", Model: "m", Family: "f", Version: "v"}}}}
	parsed := phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: result.Claim.Binding.Identity, Planner: &planner}
	evidence := &fakePlannerStore{project: store.Project{Channel: ref.Channel, ID: ref.Project, Path: "/repo", BaseRef: "main"}, result: result, parsed: parsed}
	coordinator := &fakePlannerCoordinator{result: providercoord.Result{Code: providercoord.Completed, ProviderResult: key}}
	coordinator.onRun = func(request providercoord.Request) {
		input := request.Input
		input.Provider = result.Claim.Binding.Identity
		input.AuthMode = result.Claim.Binding.AuthMode
		input.Attempt = key.Attempt
		input.LeaderEpoch, input.RunnerEpoch, input.ExpectedVersion = fence.LeaderEpoch, fence.RunnerEpoch, ticket.Version
		payload, requestDigest, _ := contracts.CanonicalPhaseInput(input)
		input.RequestDigest = requestDigest
		validation, _, _ := phaseartifact.CanonicalValidation(request.Validation)
		evidence.result.Claim.Input, evidence.result.Claim.RequestDigest, evidence.result.Claim.RequestPayload, evidence.result.Validation = input, requestDigest, payload, validation
	}
	return workflowworker.PhaseRequest{Ticket: ticket, Worktree: store.StoredWorktree{Path: "/worktree", Branch: identity.HeadRef, State: "registered", IdentityJSON: identityJSON, BaseSHA: identity.BaseHead, TicketVersion: ticket.Version, Fence: fence}, Phase: domain.PhasePlanning, Fence: fence}, evidence, coordinator
}

func fmtDigest(sum [32]byte) string { return fmt.Sprintf("%x", sum[:]) }

func TestPlannerRunnerMapsCodexPlannerAndReturnsAuthenticatedKey(t *testing.T) {
	request, evidence, coordinator := plannerFixture(t)
	runner := PlannerRunner{Store: evidence, Coordinator: coordinator}
	result, err := runner.RunArtifact(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Key.AttemptID != 11 || string(result.RawArtifact) == "" || coordinator.calls != 1 {
		t.Fatalf("result=%+v calls=%d", result, coordinator.calls)
	}
	if coordinator.request.Role != providercoord.RolePlanner || coordinator.request.Input.Phase != domain.PhasePlanning || coordinator.request.ConfigDigest != request.Ticket.ConfigDigest || coordinator.request.Input.Provider != (domain.ProviderIdentity{}) || !strings.Contains(coordinator.request.Input.Prompt, "important title") || !strings.Contains(coordinator.request.Input.Prompt, "must retain acceptance") || !strings.Contains(coordinator.request.Input.Prompt, "ticket") {
		t.Fatalf("provider request=%+v", coordinator.request)
	}
}

func TestPlannerRunnerOnlyMapsAttemptWindowExhaustionToProviderPause(t *testing.T) {
	request, evidence, coordinator := plannerFixture(t)
	runner := PlannerRunner{Store: evidence, Coordinator: coordinator}
	coordinator.result = providercoord.Result{Code: providercoord.BudgetExhausted, NeedsOperator: true}
	if _, err := runner.RunArtifact(context.Background(), request); !errors.Is(err, ErrPlannerNotReady) {
		t.Fatalf("ticket time/cost budget mapped to provider exhaustion: %v", err)
	}
	coordinator.result = providercoord.Result{Code: providercoord.AttemptExhausted, NeedsOperator: true}
	if _, err := runner.RunArtifact(context.Background(), request); !errors.Is(err, workflowworker.ErrProviderAttemptExhausted) {
		t.Fatalf("attempt window did not retain typed exhaustion: %v", err)
	}
}

func TestPlannerRunnerAcceptsCoordinatorNarrowedTimeout(t *testing.T) {
	request, evidence, coordinator := plannerFixture(t)
	bind := coordinator.onRun
	coordinator.onRun = func(input providercoord.Request) {
		bind(input)
		stored := evidence.result
		stored.Claim.Input.Timeout = time.Minute
		payload, digest, err := contracts.CanonicalPhaseInput(stored.Claim.Input)
		if err != nil {
			t.Fatal(err)
		}
		stored.Claim.Input.RequestDigest, stored.Claim.RequestDigest, stored.Claim.RequestPayload = digest, digest, payload
		evidence.result = stored
	}
	if _, err := (PlannerRunner{Store: evidence, Coordinator: coordinator}).RunArtifact(context.Background(), request); err != nil {
		t.Fatalf("narrowed timeout rejected: %v", err)
	}
}

func TestPlannerRunnerRejectsNonPlanningBoundaryAndTamperedConfig(t *testing.T) {
	request, evidence, coordinator := plannerFixture(t)
	runner := PlannerRunner{Store: evidence, Coordinator: coordinator}
	request.Phase = domain.PhaseVerification
	if _, err := runner.Run(context.Background(), request); !errors.Is(err, ErrPhaseBoundaryUnavailable) {
		t.Fatalf("verification err=%v", err)
	}
	request, evidence, coordinator = plannerFixture(t)
	request.Ticket.ConfigSnapshot = append([]byte(nil), request.Ticket.ConfigSnapshot...)
	request.Ticket.ConfigSnapshot[0] ^= 1
	if _, err := (PlannerRunner{Store: evidence, Coordinator: coordinator}).RunArtifact(context.Background(), request); !errors.Is(err, ErrConfigDigestMismatch) {
		t.Fatalf("tamper err=%v", err)
	}
	if coordinator.calls != 0 {
		t.Fatal("tampered config reached provider")
	}
}

func TestPlannerRunnerRejectsAutonomousAndMismatchedIdentity(t *testing.T) {
	request, evidence, coordinator := plannerFixture(t)
	request.Ticket.MergeMode = domain.MergeAutonomous
	if _, err := (PlannerRunner{Store: evidence, Coordinator: coordinator}).RunArtifact(context.Background(), request); !errors.Is(err, ErrUnsupportedMode) {
		t.Fatalf("mode err=%v", err)
	}
	request, evidence, coordinator = plannerFixture(t)
	request.Worktree.Path = "/foreign"
	if _, err := (PlannerRunner{Store: evidence, Coordinator: coordinator}).RunArtifact(context.Background(), request); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("identity err=%v", err)
	}
}

func TestPlannerRunnerRequiresQualificationCoordinator(t *testing.T) {
	request, evidence, _ := plannerFixture(t)
	if _, err := (PlannerRunner{Store: evidence}).RunArtifact(context.Background(), request); !errors.Is(err, ErrPlannerNotReady) {
		t.Fatalf("not ready err=%v", err)
	}
	if _, err := (PlannerRunner{Store: evidence, Coordinator: canceledPlanner{}}).RunArtifact(context.Background(), request); !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancel err=%v", err)
	}
}

type canceledPlanner struct{}

func (canceledPlanner) Run(context.Context, providercoord.Request) providercoord.Result {
	return providercoord.Result{Code: providercoord.Canceled}
}
