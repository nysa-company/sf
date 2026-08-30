package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/operator"
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
	cfg, paths = lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntime, error) {
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
		return runtimeWithFence(runtime, &gotFence), nil
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
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntime, error) { return nil, factoryErr })
	if _, err := Start(context.Background(), cfg); !errors.Is(err, factoryErr) {
		t.Fatalf("startup error=%v, want factory error", err)
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
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntime, error) { return runtime, nil })
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
	cfg, _ := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntime, error) { return runtime, nil })
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

func TestServeReturnsRuntimeCloseFailureOnCancellation(t *testing.T) {
	runtimeErr := errors.New("runtime close failed")
	runtime := &fakeWorkflowRuntime{closeFn: func() error { return runtimeErr }}
	cfg, paths := lifecycleConfig(t, func(RuntimeDependencies) (WorkflowRuntime, error) { return runtime, nil })
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
	cfg, _ := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntime, error) {
		runtimeStore = deps.Store
		return runtime, nil
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
