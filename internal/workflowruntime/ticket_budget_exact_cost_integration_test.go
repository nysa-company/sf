package workflowruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func TestExactCostPlannerCompletesThenNextPhaseBlocksBeforeProviderLaunch(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "exact-cost.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	repository := filepath.Join(t.TempDir(), "repository")
	worktree := filepath.Join(repository, "worktree")
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	projectConfig := config.DefaultProject("exact-cost", repository)
	effective, err := config.Resolve(config.DefaultMachineLimits(), projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "exact-cost", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}

	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "exact-cost", Ticket: "SF-exact-cost"}
	source := []byte("finish the planner at the exact ticket cost ceiling")
	sourceSum := sha256.Sum256(source)
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: hex.EncodeToString(sourceSum[:]), Source: source, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "exact-cost-integration")
	if err != nil {
		t.Fatal(err)
	}
	qualificationSupervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, qualificationSupervisor.PublicKey()); err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/exact-cost/SF-exact-cost", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	baseSHA := strings.Repeat("a", 40)
	identity := gitboundary.Identity{
		Repository: repository, RepositoryDev: 1, RepositoryIno: 2,
		Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4,
		GitFile: "gitdir: " + filepath.Join(repository, ".git", "worktrees", "SF-exact-cost") + "\n", GitFileDev: 5, GitFileIno: 6,
		CommonDir: filepath.Join(repository, ".git"), CommonDirDev: 7, CommonDirIno: 8,
		Origin: "https://example.invalid/exact-cost", PushOrigin: "https://example.invalid/exact-cost",
		BaseRef: "main", BaseHead: baseSHA, HeadRef: "dev/exact-cost/SF-exact-cost",
		ConfigHash: "sha256:" + strings.Repeat("b", 64), HooksHash: "sha256:" + strings.Repeat("c", 64),
	}
	identityJSON, err := workflowprompt.MarshalCanonicalWorktreeIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: identity.HeadRef, IdentityJSON: identityJSON, BaseSHA: baseSHA, HeadSHA: baseSHA}); err != nil {
		t.Fatal(err)
	}

	planner := exactCostProvider("planner-route", "planner-family")
	reviewer := exactCostProvider("reviewer-route", "reviewer-family")
	planner.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: exactCostPlannerArtifact(), UsageUnits: 100})
	reviewer.Add(domain.PhaseVerification, testkit.ProviderStep{Behavior: testkit.ProviderMalformed})
	plannerQualification := exactCostQualification(t, database, qualificationSupervisor, leader, planner, "11111111111111111111111111111111")
	reviewerQualification := exactCostQualification(t, database, qualificationSupervisor, leader, reviewer, "22222222222222222222222222222222")
	if _, _, err := database.SelectProviderSet(ctx, domain.ChannelDev, plannerQualification.ID, plannerQualification.ID, reviewerQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := providercoord.NewRegistry()
	if err := registry.Register(ctx, planner); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, reviewer); err != nil {
		t.Fatal(err)
	}
	coordinatorSupervisor := testkit.NewSupervisor()
	coordinator, err := providercoord.New(registry, map[providercoord.Role]providercoord.Route{
		providercoord.RolePlanner:  {Primary: planner.Name(), Capacity: 1},
		providercoord.RoleBuilder:  {Primary: planner.Name(), Capacity: 1},
		providercoord.RoleReviewer: {Primary: reviewer.Name(), Capacity: 1},
	}, database, nil, coordinatorSupervisor)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	worker := workflowworker.Worker{Evidence: database, Engine: engine.New(database, spec), Runner: NewPhaseRunner(database, coordinator)}

	planned, err := worker.Run(ctx, ref, fence)
	if err != nil || !planned.Transitioned || planned.State != domain.StateVerifying {
		t.Fatalf("exact-cost planner result=%+v err=%v", planned, err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateVerifying || current.MaxCostMicroUSD != 100 {
		t.Fatalf("post-planner ticket=%+v err=%v", current, err)
	}
	attempts, err := database.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].Phase != domain.PhasePlanning || attempts[0].State != "completed" || attempts[0].Outcome != "completed" || attempts[0].UsageUnits != current.MaxCostMicroUSD {
		t.Fatalf("exact-cost planner attempts=%+v err=%v", attempts, err)
	}
	if calls := planner.CallsSnapshot(); len(calls) != 1 || calls[0] != domain.PhasePlanning {
		t.Fatalf("planner calls=%v", calls)
	}

	blocked, err := worker.Run(ctx, ref, fence)
	if err != nil || !blocked.Transitioned || blocked.State != domain.StateBlocked || blocked.Version != current.Version+1 {
		t.Fatalf("next-phase budget result=%+v err=%v", blocked, err)
	}
	current, err = database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateBlocked || current.ResumeState != domain.StateVerifying || current.BlockedCode != "ticket_budget_exhausted" {
		t.Fatalf("blocked ticket=%+v err=%v", current, err)
	}
	if calls := reviewer.CallsSnapshot(); len(calls) != 0 {
		t.Fatalf("Reviewer launched after exact ticket cost was consumed: %v", calls)
	}
	attempts, err = database.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("next phase persisted an attempt after budget exhaustion: attempts=%+v err=%v", attempts, err)
	}

	events, err := database.Events(ctx, ref.Channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	blockers := 0
	for _, event := range events {
		if event.Ref != ref || event.Trigger != "typed_blocker" || event.From != domain.StateVerifying || event.To != domain.StateBlocked {
			continue
		}
		blockers++
		var payload struct {
			Schema        string `json:"schema"`
			Code          string `json:"code"`
			Phase         string `json:"phase"`
			SpentMicroUSD int64  `json:"spent_micro_usd"`
			MaxMicroUSD   int64  `json:"max_cost_micro_usd"`
		}
		if err := json.Unmarshal([]byte(event.Payload), &payload); err != nil || payload.Schema != "sf.ticket-budget-exhausted/v1" || payload.Code != "ticket_budget_exhausted" || payload.Phase != string(domain.PhaseVerification) || payload.SpentMicroUSD != 100 || payload.MaxMicroUSD != 100 {
			t.Fatalf("budget blocker payload=%s decoded=%+v err=%v", event.Payload, payload, err)
		}
	}
	if blockers != 1 {
		t.Fatalf("budget blocker count=%d events=%+v", blockers, events)
	}
	if replay, replayErr := worker.Run(ctx, ref, fence); replayErr != nil || replay.Transitioned || replay.State != domain.StateBlocked || len(reviewer.CallsSnapshot()) != 0 {
		t.Fatalf("budget blocker replay=%+v err=%v Reviewer calls=%v", replay, replayErr, reviewer.CallsSnapshot())
	}
	events, err = database.Events(ctx, ref.Channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	replayedBlockers := 0
	for _, event := range events {
		if event.Ref == ref && event.Trigger == "typed_blocker" && event.To == domain.StateBlocked {
			replayedBlockers++
		}
	}
	if replayedBlockers != 1 {
		t.Fatalf("replay duplicated durable blocker: count=%d events=%+v", replayedBlockers, events)
	}
}

type exactCostRoutedProvider struct {
	*testkit.ScriptedProvider
	route string
}

func exactCostProvider(route, family string) *exactCostRoutedProvider {
	return &exactCostRoutedProvider{
		ScriptedProvider: testkit.NewScriptedProvider(domain.ProviderIdentity{Provider: "codex", Model: route + "-model", Family: family, Version: "1"}),
		route:            route,
	}
}

func (p *exactCostRoutedProvider) Name() string { return p.route }

func (p *exactCostRoutedProvider) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	binding, err := p.ScriptedProvider.Binding(ctx)
	if err != nil {
		return contracts.RuntimeBinding{}, err
	}
	binding.AuthMode = "chatgpt_subscription"
	return binding, nil
}

func exactCostQualification(t *testing.T, database *store.Store, supervisor *processsupervisor.Supervisor, leader uint64, provider *exactCostRoutedProvider, runID string) store.ProviderQualification {
	t.Helper()
	binding, err := provider.Binding(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC()
	probeSum := sha256.Sum256([]byte("probe:" + runID))
	input := store.ProviderQualification{
		Channel: domain.ChannelDev, RunID: runID, Provider: binding.Identity,
		BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest,
		AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: hex.EncodeToString(probeSum[:]),
		Profile: store.QualificationGuarded, CreatedAt: created,
	}
	attestation, err := supervisor.AttestQualification(contracts.QualificationAttestation{
		Channel: input.Channel, RunID: input.RunID, Identity: input.Provider,
		BinaryDigest: input.BinaryDigest, PolicyDigest: input.PolicyDigest, FixtureDigest: input.FixtureDigest,
		AuthDigest: input.AuthDigest, AuthMode: input.AuthMode, ProbeDigest: input.ProbeDigest,
		Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	qualification, _, err := database.RecordAttestedProviderQualification(context.Background(), input, attestation)
	if err != nil {
		t.Fatal(err)
	}
	return qualification
}

func exactCostPlannerArtifact() []byte {
	return []byte(`{"schema":"sf.planner/v1","acceptance":["exact budget"],"proof":{"kind":"acceptance","command":["go","test","./..."],"details":"exact budget"},"paths":["internal"],"commands":[["go","test","./..."]],"risks":["none"],"questions":[]}`)
}
