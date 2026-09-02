package workflowruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func TestExpiredTicketFlowsCoordinatorThroughWorkerIntoOneDurableBudgetBlocker(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "ticket-budget.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const repository = "/tmp/workflow-ticket-budget"
	const worktree = repository + "/worktree"
	projectConfig := config.DefaultProject("budget", repository)
	effective, err := config.Resolve(config.DefaultMachineLimits(), projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "budget", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "budget", Ticket: "SF-expired-budget"}
	source := []byte("prove ticket budget exhaustion")
	sourceSum := sha256.Sum256(source)
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: hex.EncodeToString(sourceSum[:]), Source: source, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC().Add(-2 * time.Hour), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "ticket-budget-integration")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/budget/SF-expired-budget", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	identity := gitboundary.Identity{
		Repository: repository, RepositoryDev: 1, RepositoryIno: 2,
		Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4,
		GitFile: "gitdir: /tmp/workflow-ticket-budget/.git/worktrees/SF-expired-budget\n", GitFileDev: 5, GitFileIno: 6,
		CommonDir: repository + "/.git", CommonDirDev: 7, CommonDirIno: 8,
		Origin: "https://example.invalid/budget", PushOrigin: "https://example.invalid/budget",
		BaseRef: "main", BaseHead: strings.Repeat("a", 40), HeadRef: "dev/budget/SF-expired-budget",
		ConfigHash: "sha256:" + strings.Repeat("b", 64), HooksHash: "sha256:" + strings.Repeat("c", 64),
	}
	identityJSON, err := workflowprompt.MarshalCanonicalWorktreeIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: identity.HeadRef, IdentityJSON: identityJSON, BaseSHA: identity.BaseHead, HeadSHA: identity.BaseHead}); err != nil {
		t.Fatal(err)
	}

	provider := testkit.NewScriptedProvider(domain.ProviderIdentity{Provider: "cursor", Model: "budget", Family: "budget-family", Version: "1"})
	provider.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderHang})
	registry := providercoord.NewRegistry()
	if err := registry.Register(ctx, provider); err != nil {
		t.Fatal(err)
	}
	coordinator, err := providercoord.New(registry, map[providercoord.Role]providercoord.Route{providercoord.RolePlanner: {Primary: "cursor", Capacity: 1}}, database, nil, testkit.NewSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	worker := workflowworker.Worker{Evidence: database, Engine: engine.New(database, spec), Runner: NewPlannerRunner(database, coordinator)}
	result, err := worker.Run(ctx, ref, fence)
	if err != nil || !result.Transitioned || result.State != domain.StateBlocked || result.Version != started.Version+1 {
		t.Fatalf("budget worker result=%+v err=%v", result, err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateBlocked || current.ResumeState != domain.StatePlanning || current.BlockedCode != "ticket_budget_exhausted" {
		t.Fatalf("budget ticket=%+v err=%v", current, err)
	}
	if calls := provider.CallsSnapshot(); len(calls) != 0 {
		t.Fatalf("expired ticket invoked provider: %v", calls)
	}
	if attempts, err := database.ProviderAttempts(ctx, ref); err != nil || len(attempts) != 0 {
		t.Fatalf("expired ticket attempts=%+v err=%v", attempts, err)
	}

	events, err := database.Events(ctx, ref.Channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	blockers := 0
	for _, event := range events {
		if event.Ref != ref || event.Trigger != "typed_blocker" || event.To != domain.StateBlocked {
			continue
		}
		blockers++
		var payload struct {
			Schema string `json:"schema"`
			Code   string `json:"code"`
			Phase  string `json:"phase"`
		}
		if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil || payload.Schema != "sf.ticket-budget-exhausted/v1" || payload.Code != "ticket_budget_exhausted" || payload.Phase != string(domain.PhasePlanning) {
			t.Fatalf("budget event=%+v payload=%s err=%v", event, event.Payload, err)
		}
	}
	if blockers != 1 {
		t.Fatalf("budget blocker count=%d events=%+v", blockers, events)
	}
	if replay, replayErr := worker.Run(ctx, ref, fence); replayErr != nil || replay.Transitioned || replay.State != domain.StateBlocked || len(provider.CallsSnapshot()) != 0 {
		t.Fatalf("budget replay=%+v err=%v calls=%v", replay, replayErr, provider.CallsSnapshot())
	}
}
