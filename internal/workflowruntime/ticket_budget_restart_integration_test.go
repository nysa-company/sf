package workflowruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

type restartBudgetPhaseRunner struct {
	calls int
}

func (r *restartBudgetPhaseRunner) Run(context.Context, workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	r.calls++
	return workflowworker.PhaseResult{}, workflowworker.ErrTicketBudgetExhausted
}

func restartBudgetDrainRequest(claim store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{
		ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref,
		Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt,
		LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch,
		ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey,
		BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest,
		PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest,
		AuthMode: claim.Binding.AuthMode, Repository: claim.Repository,
		Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity,
		BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest,
	}
}

func restartBudgetAttemptRequest(ref domain.TicketRef, ticket store.Ticket, fence domain.Fence, binding contracts.RuntimeBinding, supervisorKey []byte, repository, worktree, identity, baseSHA, configDigest string) store.ProviderAttemptRequest {
	return store.ProviderAttemptRequest{
		Ref: ref, ExpectedVersion: ticket.Version, Fence: fence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: binding,
		ConfigDigest: configDigest, Capacity: 1, At: time.Now().UTC(),
		Repository: repository, Worktree: worktree, WorktreeIdentity: identity,
		BaseSHA: baseSHA, SupervisorKey: supervisorKey,
		Input: contracts.PhaseInput{
			Ticket: ref, Phase: domain.PhasePlanning, LeaderEpoch: fence.LeaderEpoch,
			RunnerEpoch: fence.RunnerEpoch, ExpectedVersion: ticket.Version,
			Prompt: "restart budget regression", Repository: repository, Worktree: worktree,
			WorktreeIdentity: identity, BaseSHA: baseSHA, AllowedPaths: []string{"."},
			Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute,
			Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`),
		},
	}
}

func TestTicketBudgetRejectionSurvivesStoreRestartAndBlocksExactlyOnce(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ticket-budget-restart.sqlite")
	database, err := store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}

	const repository = "/tmp/restart-budget-repository"
	const worktree = "/tmp/restart-budget-repository/worktree"
	projectConfig := config.DefaultProject("restart-budget", repository)
	effective, err := config.Resolve(config.DefaultMachineLimits(), projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "restart-budget", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}

	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "restart-budget", Ticket: "SF-budget-restart"}
	source := []byte("restart after a durable budget rejection")
	sourceSum := sha256.Sum256(source)
	created := time.Now().UTC()
	if err := database.CreateTicket(ctx, store.Ticket{
		Ref: ref, SourceDigest: hex.EncodeToString(sourceSum[:]), Source: source,
		Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: created,
		MaxDuration: time.Hour, MaxCostMicroUSD: 100,
	}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "budget-restart-first")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/restart-budget/SF-budget-restart", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}

	identity := git.Identity{
		Repository: repository, RepositoryDev: 1, RepositoryIno: 2,
		Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4,
		GitFile: "gitdir: /tmp/restart-budget-repository/.git/worktrees/SF-budget-restart\n", GitFileDev: 5, GitFileIno: 6,
		CommonDir: repository + "/.git", CommonDirDev: 7, CommonDirIno: 8,
		Origin: "https://example.invalid/restart-budget", PushOrigin: "https://example.invalid/restart-budget",
		BaseRef: "main", BaseHead: strings.Repeat("a", 40), HeadRef: "dev/restart-budget/SF-budget-restart",
		ConfigHash: "sha256:" + strings.Repeat("b", 64), HooksHash: "sha256:" + strings.Repeat("c", 64),
	}
	identityJSON, err := workflowprompt.MarshalCanonicalWorktreeIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: identity.HeadRef, IdentityJSON: identityJSON, BaseSHA: identity.BaseHead, HeadSHA: identity.BaseHead}); err != nil {
		t.Fatal(err)
	}

	provider := testkit.NewScriptedProvider(domain.ProviderIdentity{Provider: "cursor", Model: "restart-budget", Family: "restart-budget-family", Version: "1"})
	binding, err := provider.Binding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reviewer := testkit.NewScriptedProvider(domain.ProviderIdentity{Provider: "claude", Model: "restart-review", Family: "restart-review-family", Version: "1"})
	reviewerBinding, err := reviewer.Binding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	qualifiedAt := time.Now().UTC()
	builderQ, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: strings.Repeat("a", 32), Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, Profile: store.QualificationGuarded, CreatedAt: qualifiedAt})
	if err != nil {
		t.Fatal(err)
	}
	reviewerQ, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: strings.Repeat("b", 32), Provider: reviewerBinding.Identity, BinaryDigest: reviewerBinding.BinaryDigest, PolicyDigest: reviewerBinding.PolicyDigest, FixtureDigest: reviewerBinding.FixtureDigest, Profile: store.QualificationGuarded, CreatedAt: qualifiedAt.Add(time.Nanosecond)})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, builderQ.ID, reviewerQ.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	supervisor := testkit.NewSupervisor()
	request := restartBudgetAttemptRequest(ref, started, fence, binding, supervisor.PublicKey(), repository, worktree, string(identityJSON), identity.BaseHead, configDigest)
	claim, err := database.BeginProviderAttempt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: 4242, PGID: 4242, BootIdentity: "budget-boot", ProcessStartIdentity: "budget-start", Worktree: worktree}); err != nil {
		t.Fatal(err)
	}
	proof, err := supervisor.Drain(ctx, restartBudgetDrainRequest(claim))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FailProviderAttemptBudget(ctx, claim, proof, started.Version, fence, 101, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = store.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	secondLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "budget-restart-second")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil {
		t.Fatal(err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	currentFence := domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: current.RunnerEpoch}
	retryRequest := restartBudgetAttemptRequest(ref, current, currentFence, binding, supervisor.PublicKey(), repository, worktree, string(identityJSON), identity.BaseHead, configDigest)
	if _, err := database.BeginProviderAttempt(ctx, retryRequest); !errors.Is(err, store.ErrBudgetExhausted) {
		t.Fatalf("post-restart provider admission=%v", err)
	}
	attempts, err := database.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].Outcome != "budget_exhausted" || attempts[0].State != "failed" {
		t.Fatalf("durable rejected attempts=%+v err=%v", attempts, err)
	}

	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runner := &restartBudgetPhaseRunner{}
	worker := workflowworker.Worker{Evidence: database, Engine: engine.New(database, spec), Runner: runner}
	result, err := worker.Run(ctx, ref, currentFence)
	if err != nil || !result.Transitioned || result.State != domain.StateBlocked {
		t.Fatalf("restart budget worker result=%+v err=%v", result, err)
	}
	if runner.calls != 1 {
		t.Fatalf("budget worker phase calls=%d", runner.calls)
	}
	blocked, err := database.Ticket(ctx, ref)
	if err != nil || blocked.State != domain.StateBlocked || blocked.BlockedCode != "ticket_budget_exhausted" {
		t.Fatalf("blocked ticket=%+v err=%v", blocked, err)
	}
	events, err := database.Events(ctx, ref.Channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	blockers := 0
	for _, event := range events {
		if event.Ref == ref && event.Trigger == "typed_blocker" && event.To == domain.StateBlocked {
			blockers++
		}
	}
	if blockers != 1 {
		t.Fatalf("budget blocker events=%d", blockers)
	}
	if replay, replayErr := worker.Run(ctx, ref, currentFence); replayErr != nil || replay.Transitioned || replay.State != domain.StateBlocked || runner.calls != 1 {
		t.Fatalf("budget replay=%+v err=%v calls=%d", replay, replayErr, runner.calls)
	}
}
