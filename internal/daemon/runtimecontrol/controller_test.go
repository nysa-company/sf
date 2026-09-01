package runtimecontrol

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type controlTickets struct{}

func (controlTickets) ListTickets(context.Context, domain.Channel) ([]store.Ticket, error) {
	return nil, nil
}
func (controlTickets) Ticket(context.Context, domain.TicketRef) (store.Ticket, error) {
	return store.Ticket{}, store.ErrNotFound
}
func (controlTickets) RuntimeAdmissionReady(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error) {
	return true, nil
}

type controlEnsure struct{}

func (controlEnsure) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	return store.StoredWorktree{}, nil
}

type controlWorker struct{}

func (controlWorker) Run(context.Context, domain.TicketRef, domain.Fence) (workflowworker.RunResult, error) {
	return workflowworker.RunResult{}, nil
}

type drainingControlWorker struct{ entered chan struct{} }

func (w *drainingControlWorker) Run(ctx context.Context, _ domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	close(w.entered)
	<-ctx.Done()
	return workflowworker.RunResult{}, nil
}

func controllerBundle(t *testing.T) *workflowruntime.ControlBundle {
	t.Helper()
	runtime, err := workflowruntime.NewRuntime(workflowruntime.NewScheduler(domain.ChannelDev, controlTickets{}, controlEnsure{}, controlWorker{}), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return runtime.ControlBundle()
}

func controllerFixture(t *testing.T) (*store.Store, domain.TicketRef, uint64, store.Ticket) {
	t.Helper()
	return controllerFixtureAt(t, filepath.Join(t.TempDir(), "sf.sqlite"))
}

func controllerFixtureAt(t *testing.T, path string) (*store.Store, domain.TicketRef, uint64, store.Ticket) {
	t.Helper()
	ctx := t.Context()
	database, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("nysa", "/tmp/nysa"), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-controller"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "controller", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "runtime-control-test")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/SF-controller", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return database, ref, leader, started
}

func TestDrainRequiresExactRuntimeJoinAndStoreProof(t *testing.T) {
	database, ref, leader, started := controllerFixture(t)
	controller, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.PlanEffect(t.Context(), store.EffectPlan{SemanticKey: "controller/uncertain", Ref: ref, Kind: "repository_command", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "controller-uncertain"}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.ClaimEffect(t.Context(), store.EffectFence{SemanticKey: "controller/uncertain", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	drained, err := controller.Drain(t.Context(), ref)
	if err != nil || drained {
		t.Fatalf("uncertain effect passed drain: drained=%v err=%v", drained, err)
	}
}

func TestControllerRequiresSealedRuntimeBundle(t *testing.T) {
	database, _, _, _ := controllerFixture(t)
	if _, err := New(database, nil, nil); err == nil {
		t.Fatal("controller accepted an absent runtime bundle")
	}
	if _, err := New(database, &workflowruntime.ControlBundle{}, nil); err == nil {
		t.Fatal("controller accepted a literal bundle detached from its scheduler admission")
	}
}

func TestMergeObservedNeedsPrePublicationProofWithoutObserver(t *testing.T) {
	database, ref, leader, started := controllerFixture(t)
	controller, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if observed, err := controller.MergeObserved(t.Context(), ref); observed || err != nil {
		t.Fatalf("prepublication merge observation=%v err=%v", observed, err)
	}
	queued := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-controller-queued"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: queued, SourceDigest: "controller-queued", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	if observed, err := controller.MergeObserved(t.Context(), queued); observed || err != nil {
		t.Fatalf("queued prepublication merge observation=%v err=%v", observed, err)
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "test_pause", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if observed, err := controller.MergeObserved(t.Context(), ref); observed || err != nil {
		t.Fatalf("paused prepublication merge observation=%v err=%v", observed, err)
	}
	paused, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateBlocked, ResumeState: domain.StatePlanning, Trigger: "test_block", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if observed, err := controller.MergeObserved(t.Context(), ref); observed || err != nil {
		t.Fatalf("blocked prepublication merge observation=%v err=%v", observed, err)
	}
	// Merge observation itself is a control proof and therefore latches this
	// ticket. Use a fresh active ticket for the publication-only branch.
	database, ref, leader, started = controllerFixture(t)
	controller, err = New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.PlanEffect(t.Context(), store.EffectPlan{SemanticKey: "controller/publish", Ref: ref, Kind: "branch_push", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "controller-publish"}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.MergeObserved(t.Context(), ref); !errors.Is(err, ErrMergeProofRequired) {
		t.Fatalf("publishing ticket did not fail closed: %v", err)
	}
	called := false
	observer := MergeObserverFunc(func(context.Context, domain.TicketRef) (bool, error) {
		called = true
		return true, nil
	})
	controller, err = New(database, controllerBundle(t), observer)
	if err != nil {
		t.Fatal(err)
	}
	if observed, err := controller.MergeObserved(t.Context(), ref); err != nil || !observed || !called {
		t.Fatalf("injected observer observed=%v called=%v err=%v", observed, called, err)
	}
}

func TestRearmRequiresNewStoreProvenActiveIdentity(t *testing.T) {
	database, ref, leader, started := controllerFixture(t)
	controller, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if drained, err := controller.Drain(t.Context(), ref); err != nil || !drained {
		t.Fatalf("drain=%v err=%v", drained, err)
	}
	if err := controller.Rearm(t.Context(), ref); err == nil {
		t.Fatal("same durable identity rearmed")
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_next_prepublication_phase", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if needed, err := controller.RuntimeRearmNeeded(t.Context(), ref); err != nil || !needed {
		t.Fatalf("sealed resume retry needed=%v err=%v", needed, err)
	}
	if err := controller.Rearm(t.Context(), ref); err != nil {
		t.Fatalf("new active durable identity was not rearmed: %v", err)
	}
	if needed, err := controller.RuntimeRearmNeeded(t.Context(), ref); err != nil || needed {
		t.Fatalf("armed replay needed=%v err=%v", needed, err)
	}
	controller.mu.Lock()
	entry := controller.tickets[ref]
	controller.mu.Unlock()
	retained := entry != nil && entry.hasStop
	if !retained {
		t.Fatal("controller discarded the stop proof before exact Begin or terminal retirement")
	}
	if err := controller.Rearm(t.Context(), ref); err == nil {
		t.Fatal("rearm replay succeeded without a newer durable identity")
	}
}

func TestDrainAfterRearmedActiveBeginAcceptsControllerStoreSeal(t *testing.T) {
	database, ref, leader, started := controllerFixture(t)
	worker := &drainingControlWorker{entered: make(chan struct{})}
	scheduler := workflowruntime.NewScheduler(domain.ChannelDev, workflowruntime.StoreTicketSource{Store: database}, controlEnsure{}, worker)
	runtime, err := workflowruntime.NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := New(database, runtime.ControlBundle(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if drained, err := controller.Drain(t.Context(), ref); err != nil || !drained {
		t.Fatalf("initial drain=%v err=%v", drained, err)
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_rearm_active", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Rearm(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	tickDone := make(chan workflowruntime.TickResult, 1)
	go func() { tickDone <- scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: leader}) }()
	select {
	case <-worker.entered:
	case <-time.After(time.Second):
		t.Fatal("rearmed activity did not begin")
	}
	if drained, err := controller.Drain(t.Context(), ref); err != nil || !drained {
		t.Fatalf("drain after active rearm=%v err=%v", drained, err)
	}
	select {
	case <-tickDone:
	case <-time.After(time.Second):
		t.Fatal("drain did not cancel and join rearmed worker")
	}
	if _, err := database.StoppedRuntimeTicket(t.Context(), ref); err != nil {
		t.Fatalf("drained activity did not remain durably sealed: %v", err)
	}
}

func TestEngineInvalidationThenDrainJoinsRearmedActivity(t *testing.T) {
	for _, control := range []struct {
		name, trigger string
		to            domain.State
		attributes    map[string]string
	}{
		{name: "pause", trigger: "operator_pause_or_take", to: domain.StateStopping, attributes: map[string]string{"operator_identity_authenticated": "true"}},
		{name: "cancel", trigger: "operator_cancel", to: domain.StateCancelling, attributes: map[string]string{"operator_identity_authenticated": "true", "merge_not_observed": "true"}},
	} {
		t.Run(control.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sf.sqlite")
			database, ref, leader, started := controllerFixtureAt(t, path)
			worker := &drainingControlWorker{entered: make(chan struct{})}
			scheduler := workflowruntime.NewScheduler(domain.ChannelDev, workflowruntime.StoreTicketSource{Store: database}, controlEnsure{}, worker)
			runtime, err := workflowruntime.NewRuntime(scheduler, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			controller, err := New(database, runtime.ControlBundle(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if drained, err := controller.Drain(t.Context(), ref); err != nil || !drained {
				t.Fatalf("initial drain=%v err=%v", drained, err)
			}
			if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_rearm_active", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
				t.Fatal(err)
			}
			if err := controller.Rearm(t.Context(), ref); err != nil {
				t.Fatal(err)
			}
			tickDone := make(chan workflowruntime.TickResult, 1)
			go func() { tickDone <- scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: leader}) }()
			select {
			case <-worker.entered:
			case <-time.After(time.Second):
				t.Fatal("rearmed activity did not begin")
			}
			before, err := database.Ticket(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := statemachine.LoadEmbeddedApproved()
			if err != nil {
				t.Fatal(err)
			}
			transition, err := engine.New(database, spec).Signal(t.Context(), contracts.SignalRequest{Ticket: ref, TicketVersion: before.Version, From: before.State, Trigger: control.trigger, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: before.RunnerEpoch}, Attributes: control.attributes, EventPayload: `{"intent":"` + control.name + `"}`})
			if err != nil || transition.To != control.to {
				t.Fatalf("engine transition=%+v err=%v", transition, err)
			}
			invalidated, err := database.Ticket(t.Context(), ref)
			if err != nil || invalidated.Version != before.Version+1 || invalidated.RunnerEpoch != before.RunnerEpoch+1 {
				t.Fatalf("invalidated=%+v err=%v", invalidated, err)
			}
			if drained, err := controller.Drain(t.Context(), ref); err != nil || !drained {
				t.Fatalf("drain after engine invalidation=%v err=%v", drained, err)
			}
			select {
			case <-tickDone:
			case <-time.After(time.Second):
				t.Fatal("drain did not cancel and join the invalidated activity")
			}
			sealed, err := database.StoppedRuntimeTicket(t.Context(), ref)
			if err != nil || sealed.Version != invalidated.Version || sealed.RunnerEpoch != invalidated.RunnerEpoch {
				t.Fatalf("durable seal=%+v err=%v invalidated=%+v", sealed, err, invalidated)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := store.Open(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			recovered, err := reopened.StoppedRuntimeTicket(t.Context(), ref)
			if err != nil || recovered.Version != invalidated.Version || recovered.RunnerEpoch != invalidated.RunnerEpoch {
				t.Fatalf("reopened seal=%+v err=%v invalidated=%+v", recovered, err, invalidated)
			}
		})
	}
}

func TestRetireReclaimsTerminalControllerRecordOnlyAfterStoreProof(t *testing.T) {
	database, ref, leader, started := controllerFixture(t)
	controller, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if drained, err := controller.Drain(t.Context(), ref); err != nil || !drained {
		t.Fatalf("drain=%v err=%v", drained, err)
	}
	if err := controller.Retire(t.Context(), ref); err == nil {
		t.Fatal("nonterminal ticket retired its stop record")
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateCancelled, Trigger: "test_terminal", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Retire(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	_, retained := controller.tickets[ref]
	controller.mu.Unlock()
	if retained {
		t.Fatal("terminal retirement leaked controller record")
	}
	if _, err := database.StoppedRuntimeTicket(t.Context(), ref); !errors.Is(err, store.ErrStaleFence) {
		t.Fatalf("terminal retirement retained durable runtime control: %v", err)
	}
	// A fresh controller models a daemon restart. Repeating terminal cleanup
	// must not resurrect admission or retain an in-memory stop map.
	restarted, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Retire(t.Context(), ref); err != nil {
		t.Fatalf("restart terminal cleanup=%v", err)
	}
	restarted.mu.Lock()
	_, retained = restarted.tickets[ref]
	restarted.mu.Unlock()
	if retained {
		t.Fatal("restart retirement recreated a controller record")
	}
}

func TestControllerSerializesOneTicketWithoutBlockingOtherTicketObserverOrRetirement(t *testing.T) {
	database, first, leader, firstTicket := controllerFixture(t)
	second := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-controller-second"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: second, SourceDigest: "controller-second", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.StartOrAdopt(t.Context(), second, queued.Version, "dev/nysa/SF-controller-second", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	controller, err := New(database, controllerBundle(t), MergeObserverFunc(func(context.Context, domain.TicketRef) (bool, error) {
		close(entered)
		<-release
		return false, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	mergeDone := make(chan error, 1)
	go func() { _, err := controller.MergeObserved(context.Background(), first); mergeDone <- err }()
	<-entered
	secondDone := make(chan error, 1)
	go func() { _, err := controller.Drain(context.Background(), second); secondDone <- err }()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("unrelated ticket drain=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow observer on ticket A blocked ticket B control")
	}
	sameDone := make(chan error, 1)
	go func() { _, err := controller.Drain(context.Background(), first); sameDone <- err }()
	select {
	case err := <-sameDone:
		t.Fatalf("same-ticket drain bypassed observer serialization: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-mergeDone; err != nil {
		t.Fatalf("merge observation=%v", err)
	}
	if err := <-sameDone; err != nil {
		t.Fatalf("serialized drain=%v", err)
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: first, ExpectedVersion: firstTicket.Version, From: domain.StatePlanning, To: domain.StateCancelled, Trigger: "test_terminal", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: firstTicket.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Retire(t.Context(), first); err != nil {
		t.Fatalf("terminal retirement=%v", err)
	}
	controller.mu.Lock()
	_, retained := controller.tickets[first]
	controller.mu.Unlock()
	if retained {
		t.Fatal("terminal ticket retained controller entry")
	}
}
