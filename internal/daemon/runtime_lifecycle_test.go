package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/operator"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
)

type fakeWorkflowRuntime struct {
	mu       sync.Mutex
	startErr error
	started  chan struct{}
	closed   chan struct{}
	closeFn  func() error
	ctx      context.Context
	fence    domain.Fence
}

type foregroundDaemonFunc struct {
	serve func(context.Context) error
	close func() error
}

func (d foregroundDaemonFunc) Serve(ctx context.Context) error { return d.serve(ctx) }
func (d foregroundDaemonFunc) Close() error                    { return d.close() }

func (r *fakeWorkflowRuntime) Start(ctx context.Context, fence domain.Fence) error {
	r.mu.Lock()
	r.ctx, r.fence = ctx, fence
	r.mu.Unlock()
	if r.started != nil {
		close(r.started)
	}
	return r.startErr
}

func (r *fakeWorkflowRuntime) Close() error {
	if r.closed != nil {
		select {
		case <-r.closed:
		default:
			close(r.closed)
		}
	}
	if r.closeFn != nil {
		return r.closeFn()
	}
	return nil
}

func (r *fakeWorkflowRuntime) Drain(context.Context, domain.TicketRef) (bool, error) {
	return true, nil
}
func (r *fakeWorkflowRuntime) MergeObserved(context.Context, domain.TicketRef) (bool, error) {
	return false, nil
}
func (r *fakeWorkflowRuntime) Rearm(context.Context, domain.TicketRef) error  { return nil }
func (r *fakeWorkflowRuntime) Retire(context.Context, domain.TicketRef) error { return nil }

func lifecycleConfig(t *testing.T, factory WorkflowRuntimeFactory) (Config, config.ChannelPaths) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "sf-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	paths := config.ChannelPaths{
		Root: root, Database: filepath.Join(root, "sf.sqlite"), Socket: filepath.Join(root, "run", "sf.sock"),
		Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "backups"),
	}
	uid := uint32(os.Getuid())
	return Config{
		Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "runtime-lifecycle-" + strconv.Itoa(os.Getpid()),
		Operator: operator.Authenticator{ExpectedUID: uid, Lookup: func(string) (operator.Account, error) {
			return operator.Account{Username: "operator", UID: strconv.FormatUint(uint64(uid), 10)}, nil
		}},
		Projects:               []store.Project{{Channel: domain.ChannelStable, ID: "demo", Path: filepath.Join(root, "repo"), BaseRef: "main"}},
		WorkflowRuntimeFactory: factory,
	}, paths
}

func TestRuntimeFactoryRunsAfterRecoveryAndBeforeSocket(t *testing.T) {
	started := make(chan struct{})
	factoryCalled := false
	var gotFence domain.Fence
	var paths config.ChannelPaths
	var cfg Config
	cfg, paths = lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		factoryCalled = true
		if deps.Store == nil || deps.Engine == nil {
			t.Fatal("runtime dependencies were not composed")
		}
		if _, err := os.Stat(filepath.Join(paths.Events, "events.ndjson")); err != nil {
			t.Fatalf("runtime factory ran before event projection: %v", err)
		}
		runtime := &fakeWorkflowRuntime{started: started}
		runtime.startErr = nil
		runtime.closeFn = func() error { return nil }
		// Capture the exact fence supplied by daemon Start rather than deriving
		// one from the runtime dependencies.
		runtime.mu.Lock()
		runtime.fence = domain.Fence{}
		runtime.mu.Unlock()
		return WorkflowRuntimeComponents{Runtime: runtimeWithFence(runtime, &gotFence), Controller: runtime}, nil
	})
	// runtimeWithFence only adapts observation; the returned runtime remains
	// the exact fake implementation used by this test.
	daemon, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if !factoryCalled {
		t.Fatal("runtime factory was not called")
	}
	select {
	case <-started:
	default:
		t.Fatal("runtime was not started")
	}
	if _, err := os.Stat(paths.Socket); err != nil {
		t.Fatalf("socket was not exposed after runtime start: %v", err)
	}
	if gotFence.LeaderEpoch != daemon.Epoch() || gotFence.LeaderEpoch == 0 {
		t.Fatalf("runtime fence=%+v daemon epoch=%d", gotFence, daemon.Epoch())
	}

	serveCtx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(serveCtx) }()
	if _, err := transport.Call(context.Background(), paths.Socket, api.Request{Version: api.Version, RequestID: "runtime-status", Method: "daemon.status", Parameters: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop")
	}
}

// runtimeWithFence records the fence without introducing a second runtime
// abstraction into the test fixture.
func runtimeWithFence(runtime *fakeWorkflowRuntime, gotFence *domain.Fence) WorkflowRuntime {
	return &fencedFakeRuntime{fakeWorkflowRuntime: runtime, gotFence: gotFence}
}

type fencedFakeRuntime struct {
	*fakeWorkflowRuntime
	gotFence *domain.Fence
}

func (r *fencedFakeRuntime) Start(ctx context.Context, fence domain.Fence) error {
	*r.gotFence = fence
	return r.fakeWorkflowRuntime.Start(ctx, fence)
}

func TestRuntimeFactoryFailureLeavesNoSocketAndReleasesAuthority(t *testing.T) {
	factoryErr := errors.New("runtime factory failed")
	partial := &fakeWorkflowRuntime{closed: make(chan struct{})}
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		return WorkflowRuntimeComponents{Runtime: partial}, factoryErr
	})
	if _, err := Start(context.Background(), cfg); !errors.Is(err, factoryErr) {
		t.Fatalf("startup error=%v, want factory error", err)
	}
	select {
	case <-partial.closed:
	case <-time.After(time.Second):
		t.Fatal("factory-error partial runtime was not closed")
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket exposed after factory failure: %v", err)
	}
	// Reacquiring the same channel proves the failed startup closed Store and
	// leader resources rather than leaving a hidden owner behind.
	cfg.WorkflowRuntimeFactory = nil
	restarted, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeFactoryRejectsAmbiguousOrPartialControlBundles(t *testing.T) {
	t.Run("configured controller conflicts with factory", func(t *testing.T) {
		called := false
		cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
			called = true
			return WorkflowRuntimeComponents{}, nil
		})
		cfg.Controller = &fakeWorkflowRuntime{}
		if _, err := Start(context.Background(), cfg); err == nil {
			t.Fatal("factory accepted a separately configured controller")
		}
		if called {
			t.Fatal("ambiguous runtime composition reached the factory")
		}
		if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
			t.Fatalf("socket exposed for ambiguous composition: %v", err)
		}
	})

	t.Run("runtime without controller closes partial runtime", func(t *testing.T) {
		runtime := &fakeWorkflowRuntime{closed: make(chan struct{})}
		cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
			return WorkflowRuntimeComponents{Runtime: runtime}, nil
		})
		if _, err := Start(context.Background(), cfg); err == nil {
			t.Fatal("factory accepted a runtime without its controller")
		}
		select {
		case <-runtime.closed:
		case <-time.After(time.Second):
			t.Fatal("partial runtime was not closed")
		}
		if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
			t.Fatalf("socket exposed for partial composition: %v", err)
		}
	})

	t.Run("controller without runtime", func(t *testing.T) {
		cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
			return WorkflowRuntimeComponents{Controller: &fakeWorkflowRuntime{}}, nil
		})
		if _, err := Start(context.Background(), cfg); err == nil {
			t.Fatal("factory accepted a controller without its runtime")
		}
	})

	t.Run("nil pair leaves execution unavailable", func(t *testing.T) {
		cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
			return WorkflowRuntimeComponents{}, nil
		})
		d, err := Start(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if d.runtime != nil {
			t.Fatal("nil runtime/control pair installed a runtime")
		}
		if _, idle := d.control.(idleRuntimeController); !idle {
			t.Fatalf("nil runtime/control pair control=%T, want idle controller", d.control)
		}
	})
}

func qualificationRequest() api.Request {
	return api.Request{Version: api.Version, RequestID: "qualify-runtime", Method: "provider.qualify", Parameters: []byte(`{"builder":"codex","reviewer":"codex"}`)}
}

func TestQualificationActivatesIdleRuntimeOnlyAfterQualifierCommits(t *testing.T) {
	var qualified bool
	var factoryCalls int
	initialCoordinator := &providercoord.Coordinator{}
	qualifiedCoordinator := &providercoord.Coordinator{}
	started := make(chan struct{})
	runtime := &fakeWorkflowRuntime{started: started}
	cfg, _ := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		factoryCalls++
		if !qualified {
			if deps.ProviderCoordinator != initialCoordinator {
				t.Fatalf("initial coordinator=%p, want %p", deps.ProviderCoordinator, initialCoordinator)
			}
			return WorkflowRuntimeComponents{}, nil
		}
		if deps.ProviderCoordinator != qualifiedCoordinator {
			t.Fatalf("qualified coordinator=%p, want %p", deps.ProviderCoordinator, qualifiedCoordinator)
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	coordinatorCalls := 0
	cfg.ProviderCoordinatorFactory = func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		coordinatorCalls++
		if coordinatorCalls == 1 {
			return initialCoordinator, nil
		}
		return qualifiedCoordinator, nil
	}
	cfg.ProviderQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
		// This assignment models the qualifier's final durable commit. The
		// runtime factory must not run before this point.
		qualified = true
		return map[string]string{"durable": "qualified"}, nil
	}
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.runtime != nil || factoryCalls != 1 || coordinatorCalls != 1 {
		t.Fatalf("idle daemon runtime=%T factory=%d coordinators=%d", d.runtime, factoryCalls, coordinatorCalls)
	}
	response := d.qualifyProvider(context.Background(), qualificationRequest())
	if !response.OK {
		t.Fatalf("qualification response=%+v", response)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("qualified runtime did not start")
	}
	if d.runtime != runtime || d.control != runtime || d.providerCoordinator != qualifiedCoordinator {
		t.Fatalf("installed runtime/control/coordinator=%T/%T/%p", d.runtime, d.control, d.providerCoordinator)
	}
	if factoryCalls != 2 || coordinatorCalls != 2 {
		t.Fatalf("post-qualification factory=%d coordinators=%d", factoryCalls, coordinatorCalls)
	}
}

func TestQualificationOverTransportStartsRuntimeWithDaemonLifetime(t *testing.T) {
	processCtx, cancelProcess := context.WithCancel(context.Background())
	defer cancelProcess()
	var qualified bool
	started := make(chan struct{})
	runtime := &fakeWorkflowRuntime{started: started}
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		if !qualified {
			return WorkflowRuntimeComponents{}, nil
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	cfg.ProviderCoordinatorFactory = func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return &providercoord.Coordinator{}, nil
	}
	cfg.ProviderQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
		qualified = true
		return map[string]string{"durable": "qualified"}, nil
	}
	d, err := Start(processCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	serveCtx, stopServe := context.WithCancel(context.Background())
	defer stopServe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- d.Serve(serveCtx) }()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	response, err := transport.Call(requestCtx, paths.Socket, qualificationRequest())
	if err != nil || !response.OK {
		t.Fatalf("qualification response=%+v err=%v", response, err)
	}
	cancelRequest()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("qualified runtime did not start")
	}
	runtime.mu.Lock()
	runtimeCtx := runtime.ctx
	runtime.mu.Unlock()
	if runtimeCtx == nil {
		t.Fatal("qualified runtime did not receive a context")
	}
	select {
	case <-runtimeCtx.Done():
		t.Fatal("qualification request cancellation stopped the runtime")
	case <-time.After(25 * time.Millisecond):
	}
	cancelProcess()
	select {
	case <-runtimeCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("daemon process context did not stop qualified runtime")
	}
	stopServe()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop")
	}
}

func TestQualificationReportsCommittedActivationFailure(t *testing.T) {
	var qualified bool
	runtime := &fakeWorkflowRuntime{startErr: errors.New("start refused"), closed: make(chan struct{})}
	cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		if !qualified {
			return WorkflowRuntimeComponents{}, nil
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	cfg.ProviderCoordinatorFactory = func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return &providercoord.Coordinator{}, nil
	}
	cfg.ProviderQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
		qualified = true
		return map[string]string{"durable": "qualified"}, nil
	}
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	response := d.qualifyProvider(context.Background(), qualificationRequest())
	if response.OK || response.Error == nil || response.Error.Code != "runtime_activation_failed" {
		t.Fatalf("qualification response=%+v", response)
	}
	if !response.Mutation.Attempted || response.Mutation.Kind != "provider.qualify" || !strings.Contains(response.Error.Message, "qualification was recorded") {
		t.Fatalf("activation failure did not report committed qualification: %+v", response)
	}
	select {
	case <-runtime.closed:
	case <-time.After(time.Second):
		t.Fatal("failed runtime was not closed")
	}
	if d.runtime != nil {
		t.Fatalf("failed activation installed runtime %T", d.runtime)
	}
}

func TestRequalificationDoesNotReplaceAnActiveRuntime(t *testing.T) {
	var qualified bool
	var factoryCalls int
	runtime := &fakeWorkflowRuntime{closed: make(chan struct{})}
	cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		factoryCalls++
		if !qualified {
			return WorkflowRuntimeComponents{}, nil
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	cfg.ProviderCoordinatorFactory = func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return &providercoord.Coordinator{}, nil
	}
	cfg.ProviderQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
		qualified = true
		return map[string]string{"durable": "qualified"}, nil
	}
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if response := d.qualifyProvider(context.Background(), qualificationRequest()); !response.OK {
		t.Fatalf("first qualification=%+v", response)
	}
	if response := d.qualifyProvider(context.Background(), qualificationRequest()); response.OK || response.Error == nil || response.Error.Code != "runtime_already_active" {
		t.Fatalf("requalification=%+v", response)
	}
	if factoryCalls != 2 || d.runtime != runtime || d.control != runtime {
		t.Fatalf("requalification factory=%d runtime=%T control=%T", factoryCalls, d.runtime, d.control)
	}
	select {
	case <-runtime.closed:
		t.Fatal("requalification closed active runtime")
	default:
	}
}

func TestActiveRuntimeRefusesRequalificationBeforePairMutation(t *testing.T) {
	var qualified bool
	qualifierCalls := 0
	runtime := &fakeWorkflowRuntime{}
	cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		if !qualified {
			return WorkflowRuntimeComponents{}, nil
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	cfg.ProviderCoordinatorFactory = func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return &providercoord.Coordinator{}, nil
	}
	cfg.ProviderQualifier = func(ctx context.Context, database *store.Store, channel domain.Channel, _ string, _ string) (any, error) {
		qualifierCalls++
		if qualifierCalls > 1 {
			// If this runs, it deliberately changes the selected pair. The second
			// request must be rejected before the qualifier can reach this code.
			_, _, err := selectFixtureProviderPair(ctx, database, channel, "c")
			return nil, err
		}
		qualified = true
		pair, _, err := selectFixtureProviderPair(ctx, database, channel, "a")
		return pair, err
	}
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if response := d.qualifyProvider(context.Background(), qualificationRequest()); !response.OK {
		t.Fatalf("first qualification=%+v", response)
	}
	before, err := d.store.ProviderPair(context.Background(), d.channel)
	if err != nil {
		t.Fatal(err)
	}
	response := d.qualifyProvider(context.Background(), qualificationRequest())
	if response.OK || response.Error == nil || response.Error.Code != "runtime_already_active" || response.Mutation.Attempted {
		t.Fatalf("active requalification=%+v", response)
	}
	if response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join([]string{"sf", "daemon", "run"}, "\x00") {
		t.Fatalf("active requalification action=%+v", response.NextAction)
	}
	if qualifierCalls != 1 {
		t.Fatalf("active runtime invoked qualifier %d times", qualifierCalls)
	}
	after, err := d.store.ProviderPair(context.Background(), d.channel)
	if err != nil {
		t.Fatal(err)
	}
	if after.Builder.ID != before.Builder.ID || after.Reviewer.ID != before.Reviewer.ID || !after.SelectedAt.Equal(before.SelectedAt) {
		t.Fatalf("provider pair changed before=%+v after=%+v", before, after)
	}
}

func selectFixtureProviderPair(ctx context.Context, database *store.Store, channel domain.Channel, suffix string) (store.ProviderPair, bool, error) {
	digest := strings.Repeat("a", 64)
	builder, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{
		Channel: channel, RunID: strings.Repeat("1", 31) + suffix, Provider: domain.ProviderIdentity{Provider: "fixture-builder-" + suffix, Model: "builder", Family: "builder-family-" + suffix, Version: "1"},
		BinaryDigest: digest, PolicyDigest: digest, FixtureDigest: digest, Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return store.ProviderPair{}, false, err
	}
	reviewer, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{
		Channel: channel, RunID: strings.Repeat("2", 31) + suffix, Provider: domain.ProviderIdentity{Provider: "fixture-reviewer-" + suffix, Model: "reviewer", Family: "reviewer-family-" + suffix, Version: "1"},
		BinaryDigest: digest, PolicyDigest: digest, FixtureDigest: digest, Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		return store.ProviderPair{}, false, err
	}
	return database.SelectProviderPair(ctx, channel, builder.ID, reviewer.ID, time.Now().UTC())
}

func TestCloseWaitsForQualificationActivation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var qualified bool
	runtime := &fakeWorkflowRuntime{closed: make(chan struct{})}
	cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		if !qualified {
			return WorkflowRuntimeComponents{}, nil
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	cfg.ProviderCoordinatorFactory = func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return &providercoord.Coordinator{}, nil
	}
	cfg.ProviderQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
		close(entered)
		<-release
		qualified = true
		return map[string]string{"durable": "qualified"}, nil
	}
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	qualifyDone := make(chan api.Response, 1)
	go func() { qualifyDone <- d.qualifyProvider(context.Background(), qualificationRequest()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("qualification did not begin")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before qualification release: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if response := <-qualifyDone; response.OK || response.Error == nil || response.Error.Code != "runtime_activation_failed" || !response.Mutation.Attempted {
		t.Fatalf("qualification response=%+v", response)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if d.runtime != nil {
		t.Fatalf("Close allowed runtime activation after sealing shutdown: %T", d.runtime)
	}
}

func TestCloseFirstRejectsWaitingLifecycleHandlers(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*Daemon) api.Response
	}{
		{name: "qualification", call: func(d *Daemon) api.Response { return d.qualifyProvider(context.Background(), qualificationRequest()) }},
		{name: "control", call: func(d *Daemon) api.Response {
			return d.controlTicket(context.Background(), api.Request{Version: api.Version, RequestID: "control"}, domain.OperatorIdentity{}, "pause")
		}},
		{name: "resume", call: func(d *Daemon) api.Response {
			return d.resumeTicket(context.Background(), api.Request{Version: api.Version, RequestID: "resume"}, domain.OperatorIdentity{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, _ := lifecycleConfig(t, nil)
			cfg.ProviderQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
				t.Fatal("Close-first qualification reached durable qualifier")
				return nil, nil
			}
			d, err := Start(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			d.runtimeMu.Lock()
			entered := make(chan struct{})
			responseDone := make(chan api.Response, 1)
			go func() {
				close(entered)
				responseDone <- test.call(d)
			}()
			<-entered
			closeDone := make(chan error, 1)
			go func() { closeDone <- d.Close() }()
			deadline := time.Now().Add(time.Second)
			for !d.isClosed() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !d.isClosed() {
				t.Fatal("Close did not seal handler admission")
			}
			d.runtimeMu.Unlock()
			response := <-responseDone
			if response.OK || response.Error == nil || response.Error.Code != "daemon_stopping" {
				t.Fatalf("response=%+v", response)
			}
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close deadlocked with waiting lifecycle handler")
			}
		})
	}
}

func TestStartupCleanupJoinsStoreAndLeaderCloseErrors(t *testing.T) {
	cause := errors.New("factory failure")
	storeErr := errors.New("store close failure")
	leaderErr := errors.New("leader close failure")
	got := joinCloseError(cause, "close store", func() error { return storeErr })
	got = joinCloseError(got, "close leader lease", func() error { return leaderErr })
	if !errors.Is(got, cause) || !errors.Is(got, storeErr) || !errors.Is(got, leaderErr) {
		t.Fatalf("cleanup error=%v, want cause and both cleanup errors", got)
	}
}

func TestRuntimeStartFailureClosesRuntimeBeforeAuthority(t *testing.T) {
	runtime := &fakeWorkflowRuntime{startErr: errors.New("runtime start failed"), closed: make(chan struct{})}
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	_, err := Start(context.Background(), cfg)
	if !errors.Is(err, runtime.startErr) {
		t.Fatalf("startup error=%v, want runtime start error", err)
	}
	select {
	case <-runtime.closed:
	default:
		t.Fatal("failed runtime was not closed")
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket exposed after runtime start failure: %v", err)
	}
}

func TestRuntimeUsesProcessContextAfterStartupAndStopsOnCancellation(t *testing.T) {
	processCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &fakeWorkflowRuntime{}
	cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	daemon, err := Start(processCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	runtime.mu.Lock()
	runtimeCtx := runtime.ctx
	runtime.mu.Unlock()
	if runtimeCtx == nil {
		t.Fatal("runtime context was not supplied")
	}
	if _, hasDeadline := runtimeCtx.Deadline(); hasDeadline {
		t.Fatal("runtime inherited the startup timeout context")
	}
	cancel()
	select {
	case <-runtimeCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("runtime context did not observe process cancellation")
	}
}

func TestRunForegroundReturnsNormalContextShutdownAndJoinedErrors(t *testing.T) {
	t.Run("context cancellation with clean close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := runForeground(ctx, foregroundDaemonFunc{
			serve: func(got context.Context) error {
				if !errors.Is(got.Err(), context.Canceled) {
					t.Fatalf("serve context=%v", got.Err())
				}
				return nil
			},
			close: func() error { return nil },
		})
		if err != nil {
			t.Fatalf("clean context shutdown=%v", err)
		}
	})

	t.Run("serve and close errors", func(t *testing.T) {
		serveErr := errors.New("serve failed")
		closeErr := errors.New("close failed")
		err := runForeground(context.Background(), foregroundDaemonFunc{
			serve: func(context.Context) error { return serveErr },
			close: func() error { return closeErr },
		})
		if !errors.Is(err, serveErr) || !errors.Is(err, closeErr) {
			t.Fatalf("joined foreground error=%v", err)
		}
	})
}

func TestDaemonServeRejectsConcurrentLifecycleWithoutStoppingRuntime(t *testing.T) {
	runtime := &fakeWorkflowRuntime{closed: make(chan struct{})}
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	daemon, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- daemon.Serve(serveCtx) }()
	// A successful socket request proves the first Serve owns the listener
	// before the second call attempts to enter the lifecycle.
	if _, err := transport.Call(context.Background(), paths.Socket, api.Request{Version: api.Version, RequestID: "serve-owner", Method: "daemon.status", Parameters: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := daemon.Serve(context.Background()); !errors.Is(err, ErrAlreadyServing) {
		t.Fatalf("second Serve error=%v", err)
	}
	select {
	case <-runtime.closed:
		t.Fatal("rejected Serve stopped the runtime")
	default:
	}
	cancel()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Serve error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Serve did not stop")
	}
	select {
	case <-runtime.closed:
	case <-time.After(time.Second):
		t.Fatal("first Serve did not join runtime")
	}
}

func TestServeReturnsRuntimeCloseFailureOnCancellation(t *testing.T) {
	runtimeErr := errors.New("runtime close failed")
	runtime := &fakeWorkflowRuntime{closeFn: func() error { return runtimeErr }}
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	daemon, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- daemon.Serve(serveCtx) }()
	cancel()
	select {
	case err := <-serveDone:
		if !errors.Is(err, runtimeErr) {
			t.Fatalf("Serve error=%v, want runtime close error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not stop")
	}
	// Serve has already joined and detached the runtime; Close can still close
	// the remaining listener/authorities without invoking it a second time.
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket remained after Serve/Close: %v", err)
	}
}

func TestDaemonCloseJoinsRuntimeBeforeStoreAndIsIdempotent(t *testing.T) {
	release := make(chan struct{})
	closeEntered := make(chan struct{})
	var runtimeStore *store.Store
	runtime := &fakeWorkflowRuntime{closeFn: func() error {
		close(closeEntered)
		<-release
		if runtimeStore == nil {
			return errors.New("runtime store was not supplied")
		}
		if _, err := runtimeStore.Project(context.Background(), domain.ChannelStable, "demo"); err != nil {
			return fmt.Errorf("store closed before runtime: %w", err)
		}
		return nil
	}}
	cfg, _ := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		runtimeStore = deps.Store
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: runtime}, nil
	})
	daemon, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- daemon.Close() }()
	select {
	case <-closeEntered:
	case <-time.After(time.Second):
		t.Fatal("daemon did not enter runtime close")
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before runtime joined: %v", err)
	default:
	}
	close(release)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := runtimeStore.Project(context.Background(), domain.ChannelStable, "demo"); err == nil {
		t.Fatal("store remained open after daemon close")
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
}
