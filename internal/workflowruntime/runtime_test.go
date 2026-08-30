package workflowruntime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type blockingWorker struct {
	mu    sync.Mutex
	calls int
}

// poolBlockingWorker is deliberately concurrency-safe: the runtime pool tests
// use it under the race detector to observe only externally visible worker
// concurrency, not test-fixture data races.
type poolBlockingWorker struct {
	mu        sync.Mutex
	calls     map[domain.TicketRef]int
	active    map[domain.TicketRef]struct{}
	maxActive int
	entered   chan domain.TicketRef
	exited    chan domain.TicketRef
	release   chan struct{}
}

func newPoolBlockingWorker() *poolBlockingWorker {
	return &poolBlockingWorker{
		calls:   make(map[domain.TicketRef]int),
		active:  make(map[domain.TicketRef]struct{}),
		entered: make(chan domain.TicketRef, 4),
		exited:  make(chan domain.TicketRef, 4),
		release: make(chan struct{}),
	}
}

func (w *poolBlockingWorker) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	w.mu.Lock()
	w.calls[ref]++
	w.active[ref] = struct{}{}
	if len(w.active) > w.maxActive {
		w.maxActive = len(w.active)
	}
	w.mu.Unlock()
	w.entered <- ref
	select {
	case <-ctx.Done():
		w.mu.Lock()
		delete(w.active, ref)
		w.mu.Unlock()
		w.exited <- ref
		return workflowworker.RunResult{Ref: ref}, ctx.Err()
	case <-w.release:
		w.mu.Lock()
		delete(w.active, ref)
		w.mu.Unlock()
		w.exited <- ref
		return workflowworker.RunResult{Ref: ref}, nil
	}
}

func (w *poolBlockingWorker) snapshot() (map[domain.TicketRef]int, map[domain.TicketRef]struct{}, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	calls := make(map[domain.TicketRef]int, len(w.calls))
	for ref, count := range w.calls {
		calls[ref] = count
	}
	active := make(map[domain.TicketRef]struct{}, len(w.active))
	for ref := range w.active {
		active[ref] = struct{}{}
	}
	return calls, active, w.maxActive
}

func requireWorkerRefs(t *testing.T, entered <-chan domain.TicketRef, want ...domain.TicketRef) {
	t.Helper()
	got := make(map[domain.TicketRef]int, len(want))
	for range want {
		select {
		case ref := <-entered:
			got[ref]++
		case <-time.After(time.Second):
			t.Fatalf("workers entered=%v, want=%v", got, want)
		}
	}
	for _, ref := range want {
		if got[ref] != 1 {
			t.Fatalf("worker entries=%v, want exactly one %v", got, ref)
		}
	}
}

type poolEnsure struct{}

func (poolEnsure) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	return store.StoredWorktree{Path: "/tmp/pool-worktree", State: "registered"}, nil
}

func runtimeControlStore(t *testing.T) (*store.Store, domain.TicketRef, uint64, store.Ticket) {
	t.Helper()
	database, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "sf.sqlite"))
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
	if err := database.CreateProject(t.Context(), store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-runtime-retry"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: ref, SourceDigest: "runtime-retry", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(t.Context(), domain.ChannelDev, "runtime-retry-test")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(t.Context(), ref, queued.Version, "dev/nysa/SF-runtime-retry", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return database, ref, leader, started
}

type blockingEnsure struct {
	entered chan struct{}
}

func (e *blockingEnsure) Ensure(ctx context.Context, _ worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	select {
	case e.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return store.StoredWorktree{}, ctx.Err()
}

func (w *blockingWorker) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	w.mu.Lock()
	w.calls++
	w.mu.Unlock()
	<-ctx.Done()
	return workflowworker.RunResult{Ref: ref}, ctx.Err()
}

func TestRuntimeDrainRegistersBeforeEnsureAndJoinsExactTicket(t *testing.T) {
	target := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-drain-target"}
	ensure := &blockingEnsure{entered: make(chan struct{}, 1)}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(target, domain.StatePlanning)}}, ensure, &fakeWorker{})
	runtime, err := NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ensure.entered:
	case <-time.After(time.Second):
		t.Fatal("Ensure was not entered")
	}
	if err := runtime.ControlBundle().Drain(context.Background(), target); err != nil {
		t.Fatalf("Drain did not join the active Ensure: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeDrainDoesNotCancelUnrelatedTicketAndStopsReadmission(t *testing.T) {
	target := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-aaa-target"}
	other := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-zzz-other"}
	worker := &recordingBlockingWorker{entered: make(chan domain.TicketRef, 2), released: make(chan struct{})}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(target, domain.StatePlanning), ticket(other, domain.StatePlanning)}}, &fakeEnsure{}, worker)
	runtime, err := NewRuntime(scheduler, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 8}); err != nil {
		t.Fatal(err)
	}
	if got := <-worker.entered; got != target {
		t.Fatalf("first worker ref=%v, want target", got)
	}
	// Stable scheduler ordering makes target the first activity.
	if err := runtime.ControlBundle().Drain(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-worker.entered:
		if got != other {
			t.Fatalf("stopped ticket was readmitted: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("unrelated ticket did not continue after target drain")
	}
	if scheduler.admission.Stopped(target) == false {
		t.Fatal("target admission was not latched after drain")
	}
	close(worker.released)
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePoolTwoWorkersShareAdmissionAndDrainOnlyTarget(t *testing.T) {
	first := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-first"}
	second := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-second"}
	worker := newPoolBlockingWorker()
	runtime, err := NewRuntimeWithConfig(NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{
		ticket(second, domain.StatePlanning), ticket(first, domain.StatePlanning),
	}}, poolEnsure{}, worker), RuntimeConfig{Interval: time.Hour, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 17}); err != nil {
		t.Fatal(err)
	}
	requireWorkerRefs(t, worker.entered, first, second)
	calls, active, maxActive := worker.snapshot()
	if calls[first] != 1 || calls[second] != 1 || maxActive != 2 || len(active) != 2 {
		t.Fatalf("before drain calls=%v active=%v max=%d", calls, active, maxActive)
	}
	if err := runtime.ControlBundle().Drain(context.Background(), first); err != nil {
		t.Fatalf("target Drain=%v", err)
	}
	select {
	case exited := <-worker.exited:
		if exited != first {
			t.Fatalf("Drain exited sibling %v, want %v", exited, first)
		}
	case <-time.After(time.Second):
		t.Fatal("target Drain did not join its worker")
	}
	calls, active, maxActive = worker.snapshot()
	if calls[first] != 1 || calls[second] != 1 || maxActive != 2 {
		t.Fatalf("Drain changed pool concurrency: calls=%v max=%d", calls, maxActive)
	}
	if _, live := active[second]; !live || len(active) != 1 {
		t.Fatalf("Drain did not leave sibling live: active=%v", active)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case exited := <-worker.exited:
		if exited != second {
			t.Fatalf("Close exited %v, want sibling %v", exited, second)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join sibling worker")
	}
	_, active, _ = worker.snapshot()
	if len(active) != 0 {
		t.Fatalf("worker survived Close: active=%v", active)
	}
}

func TestRuntimePoolOneWorkerLimitsConcurrentTickets(t *testing.T) {
	first := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-one-first"}
	second := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-one-second"}
	worker := newPoolBlockingWorker()
	runtime, err := NewRuntimeWithConfig(NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{
		ticket(second, domain.StatePlanning), ticket(first, domain.StatePlanning),
	}}, poolEnsure{}, worker), RuntimeConfig{Interval: time.Hour, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 18}); err != nil {
		t.Fatal(err)
	}
	requireWorkerRefs(t, worker.entered, first)
	select {
	case ref := <-worker.entered:
		t.Fatalf("one-worker pool started a second ticket: %v", ref)
	case <-time.After(30 * time.Millisecond):
	}
	calls, active, maxActive := worker.snapshot()
	if calls[first] != 1 || calls[second] != 0 || len(active) != 1 || maxActive != 1 {
		t.Fatalf("one-worker pool calls=%v active=%v max=%d", calls, active, maxActive)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePoolNeverDoubleAdmitsSameTicket(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-duplicate"}
	worker := newPoolBlockingWorker()
	runtime, err := NewRuntimeWithConfig(NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{
		ticket(ref, domain.StatePlanning), ticket(ref, domain.StatePlanning),
	}}, poolEnsure{}, worker), RuntimeConfig{Interval: time.Hour, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 19}); err != nil {
		t.Fatal(err)
	}
	requireWorkerRefs(t, worker.entered, ref)
	select {
	case duplicate := <-worker.entered:
		t.Fatalf("duplicate ticket was admitted twice: %v", duplicate)
	case <-time.After(30 * time.Millisecond):
	}
	calls, active, maxActive := worker.snapshot()
	if calls[ref] != 1 || len(active) != 1 || maxActive != 1 {
		t.Fatalf("duplicate admission calls=%v active=%v max=%d", calls, active, maxActive)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePoolCloseJoinsEveryLoop(t *testing.T) {
	first := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-close-first"}
	second := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pool-close-second"}
	worker := newPoolBlockingWorker()
	runtime, err := NewRuntimeWithConfig(NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{
		ticket(first, domain.StatePlanning), ticket(second, domain.StatePlanning),
	}}, poolEnsure{}, worker), RuntimeConfig{Interval: time.Hour, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 20}); err != nil {
		t.Fatal(err)
	}
	requireWorkerRefs(t, worker.entered, first, second)
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	requireWorkerRefs(t, worker.exited, first, second)
	_, active, maxActive := worker.snapshot()
	if len(active) != 0 || maxActive != 2 {
		t.Fatalf("Close left workers active=%v max=%d", active, maxActive)
	}
	if err := runtime.Close(); err != nil {
		t.Fatalf("second Close=%v", err)
	}
}

func TestRuntimePoolRejectsInvalidWorkerCounts(t *testing.T) {
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{}, &fakeEnsure{}, &fakeWorker{})
	for _, workers := range []int{0, 3} {
		if runtime, err := NewRuntimeWithConfig(scheduler, RuntimeConfig{Interval: time.Second, Workers: workers}); !errors.Is(err, ErrRuntimeWorkers) || runtime != nil {
			t.Fatalf("workers=%d runtime=%v err=%v", workers, runtime, err)
		}
	}
	if runtime, err := NewRuntime(scheduler, time.Second); err != nil || runtime.workers != 1 {
		t.Fatalf("compatibility runtime=%+v err=%v", runtime, err)
	}
	runtime, err := NewRuntimeWithConfig(scheduler, RuntimeConfig{Interval: time.Hour, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 21}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 21}); !errors.Is(err, ErrRuntimeStarted) {
		t.Fatalf("replayed Start=%v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

type recordingBlockingWorker struct {
	entered  chan domain.TicketRef
	released chan struct{}
}

func (w *recordingBlockingWorker) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	w.entered <- ref
	select {
	case <-ctx.Done():
		return workflowworker.RunResult{Ref: ref}, ctx.Err()
	case <-w.released:
		return workflowworker.RunResult{Ref: ref}, nil
	}
}

func TestRuntimeDrainDeadlineDoesNotDetachJoin(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-drain-deadline"}
	worker := &ignoringCancellationWorker{entered: make(chan struct{}), release: make(chan struct{})}
	runtime, err := NewRuntime(NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, &fakeEnsure{}, worker), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 9}); err != nil {
		t.Fatal(err)
	}
	<-worker.entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := runtime.ControlBundle().Drain(ctx, ref); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain error=%v, want deadline", err)
	}
	close(worker.release)
	if err := runtime.ControlBundle().Drain(context.Background(), ref); err != nil {
		t.Fatalf("second Drain did not join retained activity: %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionRearmRequiresNewDrainedIdentity(t *testing.T) {
	admission := newAdmission()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-rearm"}
	run, end, admitted := admission.Begin(context.Background(), ref, 4, 1, 7)
	if !admitted {
		t.Fatal("initial admission refused")
	}
	go func() {
		<-run.Done()
		end()
	}()
	if err := admission.Stop(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := admission.Rearm(ref, 4, 1, 7, nil, nil, nil); !errors.Is(err, ErrRuntimeRearm) {
		t.Fatalf("same identity rearmed: %v", err)
	}
	if err := admission.Rearm(ref, 5, 1, 7, nil, nil, nil); err != nil {
		t.Fatalf("new durable identity did not rearm: %v", err)
	}
	if _, _, admitted := admission.Begin(context.Background(), ref, 6, 1, 7); admitted {
		t.Fatal("stale rearm token admitted a newer identity")
	}
	if _, _, admitted := admission.Begin(context.Background(), ref, 5, 2, 7); admitted {
		t.Fatal("stale leader fence admitted the rearm token")
	}
	run, end, admitted = admission.Begin(context.Background(), ref, 5, 1, 7)
	if !admitted {
		t.Fatal("exact rearm token was not consumed")
	}
	if admission.Stopped(ref) {
		t.Fatal("matching exact admission did not clear the stop latch")
	}
	if _, _, admitted := admission.Begin(context.Background(), ref, 5, 1, 7); admitted {
		t.Fatal("active exact admission was duplicated")
	}
	end()
	select {
	case <-run.Done():
	case <-time.After(time.Second):
		t.Fatal("admission end did not cancel child context")
	}
	if _, successorEnd, admitted := admission.Begin(context.Background(), ref, 6, 1, 7); !admitted {
		t.Fatal("normal resumed workflow successor was blocked")
	} else {
		successorEnd()
	}
}

func TestControlBundleRejectsBareCapabilities(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-sealed-control"}
	runtime, err := NewRuntime(NewScheduler(domain.ChannelDev, fakeTickets{}, &fakeEnsure{}, &fakeWorker{}), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bundle := runtime.ControlBundle()
	if err := bundle.ApplyRearm(&store.RuntimeAdmissionCapability{}); !errors.Is(err, ErrRuntimeRearm) {
		t.Fatalf("bare rearm capability=%v", err)
	}
	if err := bundle.ApplyRetirement(context.Background(), &store.RuntimeRetirementCapability{}); !errors.Is(err, ErrRuntimeRearm) {
		t.Fatalf("bare retirement capability=%v", err)
	}
	if err := bundle.Drain(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionCancellationAfterStoreOpenCompensatesAndRetries(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-open-cancel"}
	admission := newAdmission()
	if err := admission.Stop(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	var opens, closes int
	if err := admission.Rearm(ref, 5, 2, 7, func(context.Context) error { opens++; return nil }, func(context.Context) (bool, error) { closes++; return true, nil }, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	admission.afterOpen = cancel
	if _, _, admitted := admission.Begin(ctx, ref, 5, 2, 7); admitted {
		t.Fatal("cancelled Begin committed after Store open")
	}
	admission.afterOpen = nil
	if opens != 1 || closes != 1 || !admission.Stopped(ref) {
		t.Fatalf("open=%d close=%d stopped=%v", opens, closes, admission.Stopped(ref))
	}
	run, end, admitted := admission.Begin(context.Background(), ref, 5, 2, 7)
	if !admitted || run == nil || opens != 2 {
		t.Fatalf("compensated exact retry admitted=%v opens=%d", admitted, opens)
	}
	end()
}

func TestStoreBackedPostOpenCancellationRestoresExactRetryOnly(t *testing.T) {
	database, ref, leader, started := runtimeControlStore(t)
	if err := database.SealRuntimeControl(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	stopped, err := database.StoppedRuntimeTicket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_rearm", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	current, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := database.RearmProof(t.Context(), ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := NewScheduler(domain.ChannelDev, StoreTicketSource{Store: database}, &fakeEnsure{}, &fakeWorker{})
	runtime, err := NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.ControlBundle().Drain(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateRearm(t.Context(), capability, runtime.ControlBundle().ApplyRearm); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler.admission.afterOpen = cancel
	if _, _, admitted := scheduler.admission.Begin(ctx, ref, current.Version, leader, current.RunnerEpoch); admitted {
		t.Fatal("cancelled Store-backed Begin committed")
	}
	scheduler.admission.afterOpen = nil
	if _, _, admitted := scheduler.admission.Begin(context.Background(), ref, current.Version+1, leader, current.RunnerEpoch); admitted {
		t.Fatal("newer identity inherited exact pending admission")
	}
	if _, _, admitted := scheduler.admission.Begin(context.Background(), ref, current.Version, leader+1, current.RunnerEpoch); admitted {
		t.Fatal("wrong leader inherited exact pending admission")
	}
	run, end, admitted := scheduler.admission.Begin(context.Background(), ref, current.Version, leader, current.RunnerEpoch)
	if !admitted || run == nil {
		t.Fatal("exact pending admission did not retry after Store suspension")
	}
	end()
}

func TestAdmissionStopAfterStoreOpenCompensatesWithoutStoreOnlyGap(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-open-stop"}
	admission := newAdmission()
	if err := admission.Stop(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	var closes int
	if err := admission.Rearm(ref, 5, 2, 7, func(context.Context) error { return nil }, func(context.Context) (bool, error) { closes++; return false, nil }, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	opened := make(chan struct{})
	release := make(chan struct{})
	admission.afterOpen = func() { close(opened); <-release }
	beginDone := make(chan bool, 1)
	go func() { _, _, admitted := admission.Begin(context.Background(), ref, 5, 2, 7); beginDone <- admitted }()
	<-opened
	stopDone := make(chan error, 1)
	stopped := make(chan struct{})
	admission.afterStop = func() { close(stopped) }
	go func() { stopDone <- admission.Stop(context.Background(), ref) }()
	<-stopped
	close(release)
	if admitted := <-beginDone; admitted {
		t.Fatal("Stop lost to a Begin which had not committed")
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if closes != 1 || !admission.Stopped(ref) {
		t.Fatalf("close=%d stopped=%v", closes, admission.Stopped(ref))
	}
	if err := admission.Rearm(ref, 6, 2, 7, nil, nil, nil); err != nil {
		t.Fatalf("permanent post-open seal left stale pending permission: %v", err)
	}
	if err := admission.Retire(ref); err != nil {
		t.Fatalf("terminal-style retirement after compensated stop=%v", err)
	}
}

func TestAdmissionRearmRefusesToReplacePendingExactTuple(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-pending-one-use"}
	admission := newAdmission()
	if err := admission.Stop(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := admission.Rearm(ref, 5, 2, 7, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := admission.Rearm(ref, 6, 2, 7, nil, nil, nil); !errors.Is(err, ErrRuntimeRearm) {
		t.Fatalf("replaced pending exact tuple: %v", err)
	}
	_, end, admitted := admission.Begin(context.Background(), ref, 5, 2, 7)
	if !admitted {
		t.Fatal("original pending exact tuple was lost")
	}
	end()
}

type ignoringCancellationWorker struct {
	entered chan struct{}
	release chan struct{}
}

func (w *ignoringCancellationWorker) Run(context.Context, domain.TicketRef, domain.Fence) (workflowworker.RunResult, error) {
	close(w.entered)
	<-w.release
	return workflowworker.RunResult{}, nil
}

func (w *blockingWorker) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func TestRuntimeCancelWaitsForInFlightTickAndStartsNoSecondTick(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-1"}
	worker := &blockingWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, &fakeEnsure{}, worker)
	runtime, err := NewRuntime(scheduler, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 3}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for worker.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("runtime did not enter its first tick")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	runtime.Cancel()
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := worker.count(); got != 1 {
		t.Fatalf("worker calls=%d, cancellation started another tick", got)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWaitCanBeBoundedWithoutDetachingLoop(t *testing.T) {
	worker := &blockingWorker{}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-2"}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, &fakeEnsure{}, worker)
	runtime, err := NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background(), domain.Fence{LeaderEpoch: 4}); err != nil {
		t.Fatal(err)
	}
	for worker.count() == 0 {
		time.Sleep(time.Millisecond)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := runtime.Wait(waitCtx); err == nil {
		t.Fatal("bounded Wait returned before cancellation")
	}
	// Wait's context does not abandon the goroutine; Close still joins it.
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if worker.count() != 1 {
		t.Fatal("loop survived Close with an extra tick")
	}
}
