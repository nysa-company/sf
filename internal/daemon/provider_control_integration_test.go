package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type providerControlSupervisor struct {
	*testkit.Supervisor
	mu       sync.Mutex
	recorder func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error
	entered  chan contracts.DrainRequest
}

func (s *providerControlSupervisor) SetLaunchRecorder(recorder func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error) {
	s.mu.Lock()
	s.recorder = recorder
	s.mu.Unlock()
}

func (s *providerControlSupervisor) Run(ctx context.Context, request contracts.DrainRequest, invocation contracts.Invocation, input contracts.PhaseInput) (contracts.CommandResult, error) {
	s.mu.Lock()
	recorder := s.recorder
	s.mu.Unlock()
	if recorder == nil {
		return contracts.CommandResult{}, errors.New("provider launch recorder is unavailable")
	}
	pid := int(request.ClaimID) + 50_000
	if err := recorder(ctx, request, contracts.ProviderLaunch{PID: pid, PGID: pid, BootIdentity: "provider-control-boot", ProcessStartIdentity: fmt.Sprintf("provider-control-%d", request.ClaimID), Worktree: request.Worktree}); err != nil {
		return contracts.CommandResult{}, err
	}
	select {
	case s.entered <- request:
	default:
	}
	return s.Supervisor.Run(ctx, request, invocation, input)
}

type providerControlEnsurer struct{ worktree store.StoredWorktree }

func (e providerControlEnsurer) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	return e.worktree, nil
}

type providerControlWorker struct {
	database    *store.Store
	coordinator *providercoord.Coordinator
	results     chan providercoord.Result
}

func (w *providerControlWorker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	ticket, err := w.database.Ticket(ctx, ref)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	project, err := w.database.Project(ctx, ref.Channel, ref.Project)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	worktree, err := w.database.Worktree(ctx, ref)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	result := w.coordinator.Run(ctx, providercoord.Request{
		Role: providercoord.RolePlanner, ExpectedVersion: ticket.Version, Fence: fence, ConfigDigest: ticket.ConfigDigest,
		Validation: phaseartifact.Validation{TicketType: ticket.Type},
		Input: contracts.PhaseInput{
			Ticket: ref, Phase: domain.PhasePlanning, Prompt: "plan the provider control regression",
			Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA,
			AllowedPaths: []string{"."}, Timeout: 10 * time.Second, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`),
		},
	})
	w.results <- result
	if result.Code == providercoord.Completed {
		return workflowworker.RunResult{Ref: ref}, nil
	}
	if result.Code == providercoord.Canceled && ctx.Err() != nil {
		return workflowworker.RunResult{Ref: ref}, ctx.Err()
	}
	return workflowworker.RunResult{Ref: ref}, fmt.Errorf("provider result %s", result.Code)
}

type providerControlTakeoverController struct {
	*runtimecontrol.Controller
	inspection contracts.TakeoverInspection
}

func (c *providerControlTakeoverController) InspectTakeover(context.Context, domain.TicketRef) (contracts.TakeoverInspection, error) {
	return c.inspection, nil
}

func recordProviderControlQualification(t *testing.T, database *store.Store, channel domain.Channel, runID string, provider *testkit.ScriptedProvider) store.ProviderQualification {
	t.Helper()
	binding, err := provider.Binding(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	qualification, _, err := database.RecordProviderQualification(t.Context(), store.ProviderQualification{
		Channel: channel, RunID: runID, Provider: binding.Identity,
		BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest,
		Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return qualification
}

func TestDaemonTakeDrainsActiveProviderAndResumeAdmitsSecondAttempt(t *testing.T) {
	primary := testkit.NewScriptedProvider(domain.ProviderIdentity{Provider: "cursor", Model: "cursor-model", Family: "cursor-family", Version: "v1"})
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderHang})
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"provider control"},"paths":["main.go"],"commands":[["go","test"]],"risks":["none"]}`), UsageUnits: 1})
	fallback := testkit.NewScriptedProvider(domain.ProviderIdentity{Provider: "claude", Model: "claude-model", Family: "claude-family", Version: "v1"})
	supervisor := &providerControlSupervisor{Supervisor: testkit.NewSupervisor(), entered: make(chan contracts.DrainRequest, 2)}
	registry := providercoord.NewRegistry()
	if err := registry.Register(t.Context(), primary); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(t.Context(), fallback); err != nil {
		t.Fatal(err)
	}

	var scheduler *workflowruntime.Scheduler
	var worker *providerControlWorker
	var registered store.StoredWorktree
	cfg, paths := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		worker = &providerControlWorker{database: deps.Store, coordinator: deps.ProviderCoordinator, results: make(chan providercoord.Result, 2)}
		scheduler = workflowruntime.NewScheduler(domain.ChannelStable, workflowruntime.StoreTicketSource{Store: deps.Store}, providerControlEnsurer{worktree: registered}, worker)
		runtime, err := workflowruntime.NewRuntimeWithConfig(scheduler, workflowruntime.RuntimeConfig{Interval: time.Hour, Workers: 1})
		if err != nil {
			return WorkflowRuntimeComponents{}, err
		}
		controller, err := runtimecontrol.New(deps.Store, runtime.ControlBundle(), runtimecontrol.MergeObserverFunc(func(context.Context, domain.TicketRef) (bool, error) { return false, nil }))
		if err != nil {
			_ = runtime.Close()
			return WorkflowRuntimeComponents{}, err
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: &providerControlTakeoverController{Controller: controller}}, nil
	})
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("demo", cfg.Projects[0].Path), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Projects[0].ConfigGeneration, cfg.Projects[0].ConfigDigest, cfg.Projects[0].ConfigSnapshot = 1, digest, snapshot
	cfg.ProviderSupervisor = supervisor
	cfg.ProviderCoordinatorFactory = func(database *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return providercoord.New(registry, map[providercoord.Role]providercoord.Route{
			providercoord.RolePlanner: {Primary: primary.Name(), Fallback: fallback.Name(), Capacity: 1},
		}, database, nil, process)
	}

	d, err := Start(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if scheduler == nil || worker == nil {
		t.Fatal("provider runtime was not composed")
	}
	primaryQualification := recordProviderControlQualification(t, d.store, d.channel, strings.Repeat("1", 32), primary)
	fallbackQualification := recordProviderControlQualification(t, d.store, d.channel, strings.Repeat("2", 32), fallback)
	if _, _, err := d.store.SelectProviderSet(t.Context(), d.channel, primaryQualification.ID, primaryQualification.ID, fallbackQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-active-provider-take"}
	if err := d.store.CreateTicket(t.Context(), store.Ticket{
		Ref: ref, SourceDigest: "active-provider-take", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded,
		CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100,
	}); err != nil {
		t.Fatal(err)
	}
	queued, err := d.store.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := d.store.StartOrAdopt(t.Context(), ref, queued.Version, "stable/demo/SF-active-provider-take/planning", domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	project, err := d.store.Project(t.Context(), d.channel, started.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	worktreePath := filepath.Join(paths.Worktrees, string(started.Ref.Project), string(started.Ref.Ticket))
	if err := os.MkdirAll(worktreePath, 0o755); err != nil {
		t.Fatal(err)
	}
	branch := "sf/stable/active-provider-take"
	base := strings.Repeat("a", 40)
	identity := daemonRecoveryWorktreeIdentity(project.Path, worktreePath, branch, project.BaseRef, base)
	if err := d.store.RegisterWorktree(t.Context(), store.WorktreeRegistration{
		Ref: started.Ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch},
		Path: worktreePath, Branch: branch, IdentityJSON: identity, BaseSHA: base, HeadSHA: base,
	}); err != nil {
		t.Fatal(err)
	}
	registered, err = d.store.Worktree(t.Context(), started.Ref)
	if err != nil {
		t.Fatal(err)
	}
	controller, ok := d.control.(*providerControlTakeoverController)
	if !ok {
		t.Fatalf("controller=%T", d.control)
	}
	controller.inspection = contracts.TakeoverInspection{Registered: true, Path: registered.Path, Branch: registered.Branch, Repository: project.Path, BaseSHA: base, HeadSHA: base, Clean: true, ChangeKind: "none", RemoteBaseSHA: base, RemoteIdentityExact: true}
	// The scheduler captured the zero registration during daemon composition;
	// replace only its deterministic, non-authoritative worktree adapter now
	// that Store has the exact durable registration.
	scheduler.Worktrees = providerControlEnsurer{worktree: registered}

	firstTick := make(chan workflowruntime.TickResult, 1)
	go func() { firstTick <- scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch}) }()
	var firstRequest contracts.DrainRequest
	select {
	case firstRequest = <-supervisor.entered:
	case result := <-worker.results:
		t.Fatalf("first provider returned before supervisor entry: %+v", result)
	case tick := <-firstTick:
		t.Fatalf("first scheduler tick returned before supervisor entry: %+v", tick)
	case <-time.After(2 * time.Second):
		t.Fatal("first provider attempt did not reach the supervisor")
	}
	if firstRequest.Attempt != 1 || firstRequest.ExpectedVersion != started.Version || firstRequest.RunnerEpoch != started.RunnerEpoch {
		t.Fatalf("first provider request=%+v started=%+v", firstRequest, started)
	}
	taken := daemonControl(d, started.Ref.Ticket, "take")
	if !taken.OK || taken.Mutation.Kind != "ticket_take" {
		t.Fatalf("take response=%+v", taken)
	}
	select {
	case tick := <-firstTick:
		if tick.Outcome != workflowruntime.OutcomeCanceled {
			t.Fatalf("first tick=%+v", tick)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("take did not join the active provider tick")
	}
	select {
	case result := <-worker.results:
		if result.Code != providercoord.Canceled || result.PersistenceFailure || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
			t.Fatalf("first provider result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("first provider result was not observed")
	}
	paused, err := d.store.Ticket(t.Context(), started.Ref)
	if err != nil || paused.State != domain.StatePaused || paused.ResumeState != domain.StatePlanning {
		t.Fatalf("taken ticket=%+v err=%v", paused, err)
	}
	attempts, err := d.store.ProviderAttempts(t.Context(), started.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].Attempt != 1 || attempts[0].State != "cancelled" || attempts[0].Outcome != "cancelled" || attempts[0].UsageUnits != 0 {
		t.Fatalf("first attempts=%+v err=%v", attempts, err)
	}
	active, err := d.store.ActiveProviderAttempts(t.Context(), d.channel)
	if err != nil || len(active) != 0 {
		t.Fatalf("active provider attempts=%+v err=%v", active, err)
	}
	leases, err := d.store.Leases(t.Context(), d.channel)
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases {
		if lease.Scope == "provider" {
			t.Fatalf("provider lease survived take: %+v", lease)
		}
	}
	lateRaw := contracts.PhaseResult{Provider: attempts[0].Binding.Identity, Artifact: []byte(`{"schema":"sf.planner/v1","acceptance":["late"],"proof":{"kind":"acceptance","command":["go","test"],"details":"late"},"paths":["main.go"],"commands":[["go","test"]],"risks":["late"]}`), UsageTrusted: true, UsageUnits: 1}
	lateProof, err := supervisor.Signer.ProveDrained(firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.store.CompleteProviderAttemptSuccess(t.Context(), attempts[0].ProviderAttemptClaim, lateProof, started.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}, lateRaw, phaseartifact.Validation{TicketType: started.Type}, time.Now().UTC()); !errors.Is(err, store.ErrStaleFence) && !errors.Is(err, store.ErrProviderAttempt) {
		t.Fatalf("late old completion was admitted: %v", err)
	}

	resumed := daemonResume(d, started.Ref.Ticket)
	if !resumed.OK {
		t.Fatalf("resume response=%+v", resumed)
	}
	secondTick := scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch})
	var secondResult providercoord.Result
	select {
	case secondResult = <-worker.results:
	case <-time.After(time.Second):
		t.Fatal("second provider result was not observed")
	}
	if secondResult.Code != providercoord.Completed || secondResult.ProviderResult.Attempt != 2 {
		t.Fatalf("second provider result=%+v tick=%+v", secondResult, secondTick)
	}
	if secondTick.Outcome != workflowruntime.OutcomeInvoked {
		t.Fatalf("second tick=%+v result=%+v", secondTick, secondResult)
	}
	attempts, err = d.store.ProviderAttempts(t.Context(), started.Ref)
	if err != nil || len(attempts) != 2 || attempts[1].Attempt != 2 || attempts[1].State != "completed" || attempts[1].Outcome != "completed" {
		t.Fatalf("resumed attempts=%+v err=%v", attempts, err)
	}
}
