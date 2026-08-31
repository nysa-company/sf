package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/events"
	"github.com/nysa-company/sf/internal/leader"
	"github.com/nysa-company/sf/internal/operator"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type testIDs struct{ next int }

func (g *testIDs) NewTicketID(domain.Channel) (domain.TicketID, error) {
	g.next++
	return domain.TicketID(fmt.Sprintf("SF-test-%d", g.next)), nil
}

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type testRuntimeController struct {
	drain func(context.Context, domain.TicketRef) (bool, error)
	merge func(context.Context, domain.TicketRef) (bool, error)
}

type testRuntimeRetirementController struct {
	testRuntimeController
	retire func(context.Context, domain.TicketRef) error
}

type daemonRuntimeEnsure struct{}

func (daemonRuntimeEnsure) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	return store.StoredWorktree{Path: "/tmp/daemon-runtime-worktree", State: "registered"}, nil
}

type daemonRuntimeWorker struct {
	mu      sync.Mutex
	calls   map[domain.TicketRef]int
	active  map[domain.TicketRef]bool
	entered chan domain.TicketRef
	exited  chan domain.TicketRef
}

func newDaemonRuntimeWorker() *daemonRuntimeWorker {
	return &daemonRuntimeWorker{calls: make(map[domain.TicketRef]int), active: make(map[domain.TicketRef]bool), entered: make(chan domain.TicketRef, 8), exited: make(chan domain.TicketRef, 8)}
}

func (worker *daemonRuntimeWorker) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	worker.mu.Lock()
	worker.calls[ref]++
	worker.active[ref] = true
	worker.mu.Unlock()
	worker.entered <- ref
	<-ctx.Done()
	worker.mu.Lock()
	delete(worker.active, ref)
	worker.mu.Unlock()
	worker.exited <- ref
	return workflowworker.RunResult{Ref: ref}, ctx.Err()
}

func (worker *daemonRuntimeWorker) snapshot(ref domain.TicketRef) (calls int, active bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.calls[ref], worker.active[ref]
}

func (controller testRuntimeController) Drain(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if controller.drain == nil {
		return true, nil
	}
	return controller.drain(ctx, ref)
}

func (controller testRuntimeController) MergeObserved(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if controller.merge == nil {
		return false, nil
	}
	return controller.merge(ctx, ref)
}

func (controller testRuntimeRetirementController) Retire(ctx context.Context, ref domain.TicketRef) error {
	if controller.retire == nil {
		return nil
	}
	return controller.retire(ctx, ref)
}

func testDaemon(t *testing.T) (*Daemon, config.ChannelPaths, context.CancelFunc) {
	return testDaemonForChannel(t, domain.ChannelStable)
}

func testDaemonForChannel(t *testing.T, channel domain.Channel) (*Daemon, config.ChannelPaths, context.CancelFunc) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "sfv2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	paths := config.ChannelPaths{
		Root: root, Database: filepath.Join(root, "sf.sqlite"), Socket: filepath.Join(root, "run", "sf.sock"),
		Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "backups"),
	}
	_, thisFile, _, _ := runtime.Caller(0)
	stateMachine := filepath.Join(filepath.Dir(thisFile), "..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	uid := uint32(os.Getuid())
	auth := operator.Authenticator{ExpectedUID: uid, Lookup: func(got string) (operator.Account, error) {
		return operator.Account{Username: "operator", UID: strconv.FormatUint(uint64(uid), 10)}, nil
	}}
	d, err := Start(context.Background(), Config{
		Channel: channel, Paths: paths, StateMachinePath: stateMachine, DaemonIdentity: "integration-test-" + string(channel),
		Projects:  []store.Project{{Channel: channel, ID: "demo", Path: filepath.Join(root, "repo"), BaseRef: "main"}},
		TicketIDs: &testIDs{}, Clock: testClock{now: time.Unix(100, 0).UTC()}, Operator: auth,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	go func() { _ = d.Serve(serveCtx) }()
	t.Cleanup(func() { cancel(); _ = d.Close() })
	return d, paths, cancel
}

func TestDaemonFailureActionsUseTheDaemonChannelExecutable(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		t.Run(string(channel), func(t *testing.T) {
			d := &Daemon{channel: channel}
			binary := "sf"
			if channel == domain.ChannelDev {
				binary = "sf-dev"
			}
			for code, want := range map[string][]string{
				"autonomous_unavailable":       {binary, "providers", "qualify", "--help"},
				"terminal_replay_requires_new": {binary, "submit", "--help"},
				"unknown_project":              {binary, "init", "--help"},
				"invalid_submit":               {binary, "submit", "--help"},
				"invalid_logs":                 {binary, "logs", "--help"},
				"not_ready":                    {binary, "--help"},
				"other":                        {binary, "doctor"},
			} {
				response := d.failure(api.Request{Version: api.Version, RequestID: "failure-actions", Method: "ticket.submit"}, code, "failed", false)
				if response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join(want, "\x00") {
					t.Fatalf("%s action=%+v want=%+v", code, response.NextAction, want)
				}
				if strings.Contains(strings.Join(response.NextAction.Argv, " "), "<") {
					t.Fatalf("%s action includes a placeholder: %+v", code, response.NextAction.Argv)
				}
				if err := response.Validate(); err != nil {
					t.Fatalf("%s response failed validation: %v", code, err)
				}
			}
			for code, want := range map[string][]string{
				"invalid_control":            {binary, "pause", "--help"},
				"invalid_ticket_reference":   {binary, "pause", "--help"},
				"invalid_transition":         {binary, "status", "SF-action"},
				"external_merge_observed":    {binary, "status", "SF-action"},
				"external_state_unavailable": {binary, "status", "SF-action"},
				"blocked_process":            {binary, "status", "SF-action"},
				"uncertain_effect":           {binary, "status", "SF-action"},
			} {
				request := api.Request{Version: api.Version, RequestID: "control-actions", Method: "ticket.pause", Ticket: "SF-action"}
				response := d.failure(request, code, "failed", false)
				if response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join(want, "\x00") {
					t.Fatalf("%s control action=%+v want=%+v", code, response.NextAction, want)
				}
			}
		})
	}
}

func TestLeaseCapacitiesUseAuthenticatedProjectSnapshot(t *testing.T) {
	machine := config.DefaultMachineLimits()
	machine.MaxConcurrentTickets = 4
	projectConfig := config.DefaultProject("demo", "/tmp/demo")
	projectConfig.MaxConcurrentTickets = 3
	effective, err := config.Resolve(machine, projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	project := store.Project{Channel: domain.ChannelDev, ID: "demo", Path: "/tmp/demo", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}
	global, perProject, err := leaseCapacities(project)
	if err != nil || global != 4 || perProject != 3 {
		t.Fatalf("global=%d project=%d err=%v", global, perProject, err)
	}
	project.Path = "/tmp/other"
	if _, _, err := leaseCapacities(project); err == nil {
		t.Fatal("configuration snapshot was accepted for another repository")
	}
}

func TestDaemonAdmissionUsesConfiguredTwoTicketDefault(t *testing.T) {
	d, _, _ := testDaemon(t)
	for index := 1; index <= 3; index++ {
		ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "demo", Ticket: domain.TicketID(fmt.Sprintf("SF-capacity-%d", index))}
		if err := d.store.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: fmt.Sprintf("capacity-%d", index), Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
			t.Fatal(err)
		}
		request := api.Request{Version: api.Version, RequestID: fmt.Sprintf("capacity-%d", index), Method: "ticket.start", Ticket: string(ref.Ticket), Parameters: []byte(`{"channel":"stable","project":"demo"}`)}
		response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
		if index <= 2 && !response.OK {
			t.Fatalf("ticket %d was not admitted: %+v", index, response)
		}
		if index == 3 && (response.OK || response.Error == nil || response.Error.Code != "capacity_unavailable") {
			t.Fatalf("third ticket did not hit capacity: %+v", response)
		}
	}
}

func TestDaemonLogsReturnBoundedRedactedDurableEvents(t *testing.T) {
	d, paths, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-logs")
	_ = createAndStartControlTicket(t, d, "SF-other-logs")
	request := api.Request{Version: api.Version, RequestID: "logs", Method: "ticket.logs", Ticket: string(started.Ref.Ticket), Parameters: json.RawMessage(`{"channel":"stable","phase":"planning","follow":false,"after":0}`)}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if !response.OK {
		t.Fatalf("logs response=%+v", response)
	}
	var page struct {
		Ticket    domain.TicketID `json:"ticket"`
		NextAfter uint64          `json:"next_after"`
		Events    []events.Record `json:"events"`
	}
	if err := json.Unmarshal(response.Data, &page); err != nil {
		t.Fatal(err)
	}
	if page.Ticket != started.Ref.Ticket || page.NextAfter == 0 || len(page.Events) == 0 || len(page.Events) > maxLogItems {
		t.Fatalf("page=%+v", page)
	}
	for _, event := range page.Events {
		if event.Ticket != started.Ref.Ticket || !eventMatchesPhase(store.Event{From: event.From, To: event.To, Payload: string(event.Payload)}, "planning") {
			t.Fatalf("cross-ticket or cross-phase event=%+v", event)
		}
		if strings.Contains(string(event.Payload), paths.Root) {
			t.Fatalf("unredacted channel path in payload=%s", event.Payload)
		}
	}
	request.RequestID = "logs-replay"
	request.Parameters = json.RawMessage(fmt.Sprintf(`{"channel":"stable","phase":"planning","follow":false,"after":%d}`, page.NextAfter))
	replay := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if !replay.OK {
		t.Fatalf("replay=%+v", replay)
	}
	var replayPage struct {
		Events []events.Record `json:"events"`
	}
	if err := json.Unmarshal(replay.Data, &replayPage); err != nil || replayPage.Events == nil || len(replayPage.Events) != 0 {
		t.Fatalf("replay events=%+v err=%v", replayPage.Events, err)
	}
	request.RequestID = "logs-invalid"
	request.Parameters = json.RawMessage(`{"channel":"stable","phase":"unknown"}`)
	invalid := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if invalid.OK || invalid.Error == nil || invalid.Error.Code != "invalid_logs" {
		t.Fatalf("invalid phase response=%+v", invalid)
	}
}

func createAndStartControlTicket(t *testing.T, daemon *Daemon, id domain.TicketID) store.Ticket {
	t.Helper()
	ref := domain.TicketRef{Channel: daemon.channel, Project: "demo", Ticket: id}
	if err := daemon.store.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: "control-" + string(id), Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	request := api.Request{Version: api.Version, RequestID: "start-" + string(id), Method: "ticket.start", Ticket: string(id), Parameters: json.RawMessage(`{"channel":"` + string(daemon.channel) + `","project":"demo"}`)}
	response := daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if !response.OK {
		t.Fatalf("start response=%+v", response)
	}
	started, err := daemon.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func daemonControl(daemon *Daemon, id domain.TicketID, intent string) api.Response {
	return daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: intent + "-" + string(id), Method: "ticket." + intent,
		Ticket: string(id), OperatorLabel: "operator",
		Parameters: json.RawMessage(`{"channel":"` + string(daemon.channel) + `","operator":"operator"}`),
	})
}

func daemonResume(daemon *Daemon, id domain.TicketID) api.Response {
	return daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "resume-" + string(id), Method: "ticket.resume",
		Ticket: string(id), OperatorLabel: "operator",
		Parameters: json.RawMessage(`{"channel":"` + string(daemon.channel) + `","operator":"operator"}`),
	})
}

func TestDaemonFactoryTwoWorkerPauseDrainsOnlyTargetAndResumeRearms(t *testing.T) {
	worker := newDaemonRuntimeWorker()
	cfg, _ := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		runtime, err := workflowruntime.NewRuntimeWithConfig(workflowruntime.NewScheduler(
			domain.ChannelStable,
			workflowruntime.StoreTicketSource{Store: deps.Store},
			daemonRuntimeEnsure{},
			worker,
		), workflowruntime.RuntimeConfig{Interval: time.Millisecond, Workers: 2})
		if err != nil {
			return WorkflowRuntimeComponents{}, err
		}
		controller, err := runtimecontrol.New(deps.Store, runtime.ControlBundle(), runtimecontrol.MergeObserverFunc(func(context.Context, domain.TicketRef) (bool, error) {
			return false, nil
		}))
		if err != nil {
			_ = runtime.Close()
			return WorkflowRuntimeComponents{}, err
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: controller}, nil
	})
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	target := createAndStartControlTicket(t, d, "SF-runtime-target")
	sibling := createAndStartControlTicket(t, d, "SF-runtime-sibling")
	seen := map[domain.TicketRef]bool{}
	for len(seen) != 2 {
		select {
		case ref := <-worker.entered:
			seen[ref] = true
		case <-time.After(time.Second):
			t.Fatalf("runtime did not start both tickets: %v", seen)
		}
	}
	if !seen[target.Ref] || !seen[sibling.Ref] {
		t.Fatalf("worker entries=%v target=%v sibling=%v", seen, target.Ref, sibling.Ref)
	}
	if response := daemonControl(d, target.Ref.Ticket, "pause"); !response.OK {
		t.Fatalf("pause response=%+v", response)
	}
	select {
	case exited := <-worker.exited:
		if exited != target.Ref {
			t.Fatalf("pause stopped sibling %v, want %v", exited, target.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("pause did not join the target worker")
	}
	if calls, active := worker.snapshot(sibling.Ref); calls != 1 || !active {
		t.Fatalf("sibling did not continue through target pause: calls=%d active=%v", calls, active)
	}
	if response := daemonResume(d, target.Ref.Ticket); !response.OK {
		t.Fatalf("resume response=%+v", response)
	}
	select {
	case ref := <-worker.entered:
		if ref != target.Ref {
			t.Fatalf("resume ran %v, want target %v", ref, target.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not rearm and run the target")
	}
	if calls, active := worker.snapshot(target.Ref); calls != 2 || !active {
		t.Fatalf("target did not restart after resume: calls=%d active=%v", calls, active)
	}
	if response := daemonControl(d, target.Ref.Ticket, "cancel"); !response.OK {
		t.Fatalf("terminal cancel response=%+v error=%+v", response, response.Error)
	}
	select {
	case exited := <-worker.exited:
		if exited != target.Ref {
			t.Fatalf("cancel stopped sibling %v, want target %v", exited, target.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal cancel did not join the rearmed target")
	}
	if _, err := d.store.StoppedRuntimeTicket(context.Background(), target.Ref); !errors.Is(err, store.ErrStaleFence) {
		t.Fatalf("terminal cancel retained durable runtime control: %v", err)
	}
	if response := daemonControl(d, target.Ref.Ticket, "cancel"); !response.OK || !response.Mutation.Observed {
		t.Fatalf("terminal cancel retry=%+v", response)
	}
}

func TestDaemonPauseDrainsThenCommitsAndReplays(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-pause")
	var drains int
	d.control = testRuntimeController{drain: func(_ context.Context, ref domain.TicketRef) (bool, error) {
		drains++
		if ref != started.Ref {
			t.Fatalf("drain ref=%+v want=%+v", ref, started.Ref)
		}
		return true, nil
	}}
	response := daemonControl(d, started.Ref.Ticket, "pause")
	if !response.OK || !response.Mutation.Attempted || response.Mutation.Observed {
		t.Fatalf("pause response=%+v", response)
	}
	paused, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != domain.StatePaused || paused.ResumeState != domain.StatePlanning || paused.RunnerEpoch != started.RunnerEpoch+1 || paused.Version != started.Version+2 {
		t.Fatalf("paused=%+v started=%+v", paused, started)
	}
	leases, err := d.store.Leases(context.Background(), d.channel)
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
	events, err := d.store.Events(context.Background(), d.channel, 0, 20)
	if err != nil || len(events) < 3 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	intentEvent := events[len(events)-2]
	if intentEvent.Trigger != "operator_pause_or_take" || !strings.Contains(intentEvent.Payload, `"intent":"pause"`) || !strings.Contains(intentEvent.Payload, `"operator":"operator"`) || !strings.Contains(intentEvent.Payload, `"operator_uid":`) {
		t.Fatalf("control intent event=%+v", intentEvent)
	}
	if drainedEvent := events[len(events)-1]; drainedEvent.Trigger != "process_and_effects_drained" || drainedEvent.Payload != `{"drained":true,"intent":"pause"}` {
		t.Fatalf("drained event=%+v", drainedEvent)
	}
	replay := daemonControl(d, started.Ref.Ticket, "pause")
	if !replay.OK || !replay.Mutation.Observed || drains != 1 {
		t.Fatalf("replay=%+v drains=%d", replay, drains)
	}
}

func TestDaemonPauseRemainsStoppingUntilRuntimeDrains(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-blocked-pause")
	drained := false
	d.control = testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) { return drained, nil }}
	response := daemonControl(d, started.Ref.Ticket, "pause")
	if response.OK || response.Error == nil || response.Error.Code != "blocked_process" || !response.Mutation.Attempted {
		t.Fatalf("undrained response=%+v", response)
	}
	stopping, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if stopping.State != domain.StateStopping || stopping.RunnerEpoch != started.RunnerEpoch+1 {
		t.Fatalf("stopping=%+v started=%+v", stopping, started)
	}
	if leases, err := d.store.Leases(context.Background(), d.channel); err != nil || len(leases) != 2 {
		t.Fatalf("undrained leases=%+v err=%v", leases, err)
	}
	drained = true
	response = daemonControl(d, started.Ref.Ticket, "pause")
	if !response.OK {
		t.Fatalf("completed response=%+v", response)
	}
	paused, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil || paused.State != domain.StatePaused {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
}

func TestDaemonCancelChecksMergeBeforeAndAfterDrain(t *testing.T) {
	t.Run("merge already observed does not mutate", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-cancel-premerge")
		var drains int
		d.control = testRuntimeController{
			drain: func(context.Context, domain.TicketRef) (bool, error) { drains++; return true, nil },
			merge: func(context.Context, domain.TicketRef) (bool, error) { return true, nil },
		}
		response := daemonControl(d, started.Ref.Ticket, "cancel")
		if response.OK || response.Error == nil || response.Error.Code != "external_merge_observed" || response.Mutation.Attempted || drains != 0 {
			t.Fatalf("response=%+v drains=%d", response, drains)
		}
		stored, err := d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StatePlanning || stored.Version != started.Version || stored.RunnerEpoch != started.RunnerEpoch {
			t.Fatalf("stored=%+v err=%v started=%+v", stored, err, started)
		}
	})

	t.Run("merge racing drain leaves cancellation incomplete", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-cancel-race")
		observations := 0
		d.control = testRuntimeController{
			drain: func(context.Context, domain.TicketRef) (bool, error) { return true, nil },
			merge: func(context.Context, domain.TicketRef) (bool, error) {
				observations++
				return observations >= 2, nil
			},
		}
		response := daemonControl(d, started.Ref.Ticket, "cancel")
		if response.OK || response.Error == nil || response.Error.Code != "external_merge_observed" || !response.Mutation.Attempted || observations != 2 {
			t.Fatalf("response=%+v observations=%d", response, observations)
		}
		stored, err := d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StateCancelling || stored.RunnerEpoch != started.RunnerEpoch+1 {
			t.Fatalf("stored=%+v err=%v started=%+v", stored, err, started)
		}
		response = daemonControl(d, started.Ref.Ticket, "cancel")
		if response.OK || response.Error == nil || response.Error.Code != "external_merge_observed" {
			t.Fatalf("repeat after observed merge=%+v", response)
		}
		stored, err = d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StateCancelling {
			t.Fatalf("repeat stored=%+v err=%v", stored, err)
		}
	})

	t.Run("absence through drain completes cancellation", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-cancel")
		var observations int
		d.control = testRuntimeController{
			drain: func(context.Context, domain.TicketRef) (bool, error) { return true, nil },
			merge: func(context.Context, domain.TicketRef) (bool, error) { observations++; return false, nil },
		}
		response := daemonControl(d, started.Ref.Ticket, "cancel")
		if !response.OK || observations != 2 {
			t.Fatalf("response=%+v observations=%d", response, observations)
		}
		stored, err := d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StateCancelled || stored.RunnerEpoch != started.RunnerEpoch+1 {
			t.Fatalf("stored=%+v err=%v started=%+v", stored, err, started)
		}
	})
}

func TestDaemonCancellationRetriesTerminalRuntimeRetirementWithoutReopeningAdmission(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-cancel-retirement")
	retireErr := errors.New("runtime retirement unavailable")
	attempts := 0
	d.control = testRuntimeRetirementController{retire: func(context.Context, domain.TicketRef) error {
		attempts++
		if attempts == 1 {
			return retireErr
		}
		return nil
	}}
	response := daemonControl(d, started.Ref.Ticket, "cancel")
	if response.OK || response.Error == nil || response.Error.Code != "runtime_retirement_failed" || !response.Error.Retryable {
		t.Fatalf("first cancellation response=%+v", response)
	}
	terminal, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil || terminal.State != domain.StateCancelled {
		t.Fatalf("terminal cancellation was not persisted: ticket=%+v err=%v", terminal, err)
	}
	response = daemonControl(d, started.Ref.Ticket, "cancel")
	if !response.OK || !response.Mutation.Observed || attempts != 2 {
		t.Fatalf("retry response=%+v attempts=%d", response, attempts)
	}
	terminal, err = d.store.Ticket(context.Background(), started.Ref)
	if err != nil || terminal.State != domain.StateCancelled || terminal.Version != started.Version+2 {
		t.Fatalf("retirement retry changed terminal admission: ticket=%+v err=%v", terminal, err)
	}
}

func TestDaemonControlPreconditionFailureReportsNoMutation(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-control-read")
	d.control = testRuntimeController{merge: func(context.Context, domain.TicketRef) (bool, error) { return false, errors.New("unavailable") }}
	response := daemonControl(d, started.Ref.Ticket, "cancel")
	if response.OK || response.Error == nil || response.Error.Code != "external_state_unavailable" || response.Mutation.Attempted {
		t.Fatalf("response=%+v", response)
	}
}

func TestDaemonShowAndStatusExposeBoundedAuthenticatedEvidence(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-evidence-view")
	digest, err := d.store.RecordPlan(context.Background(), store.PlanArtifact{
		Ref: started.Ref, ExpectedVersion: started.Version,
		Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch},
		Document: store.PlanDocument{
			Acceptance: []string{"a durable result exists"}, ProofKind: "regression",
			Paths: []string{"internal/example.go"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"stale state"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	show := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "show-evidence", Method: "ticket.show", Ticket: string(started.Ref.Ticket),
		OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"demo","ticket":"SF-evidence-view"}`),
	})
	if !show.OK || !strings.Contains(string(show.Data), `"digest":"`+digest+`"`) || !strings.Contains(string(show.Data), `"proof_kind":"regression"`) || !strings.Contains(string(show.Data), `"operator":{"label":"operator"`) || strings.Contains(string(show.Data), "identity_json") {
		t.Fatalf("show=%+v data=%s", show, show.Data)
	}
	status := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "status-evidence", Method: "ticket.status", Ticket: string(started.Ref.Ticket),
		OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"","watch":false}`),
	})
	if !status.OK || !strings.Contains(string(status.Data), `"runner_epoch":`) || !strings.Contains(string(status.Data), `"phase_attempts":[]`) || !strings.Contains(string(status.Data), `"merge_mode":"guarded"`) {
		t.Fatalf("status=%+v data=%s", status, status.Data)
	}
}

func TestDaemonRefusesToHideCorruptDurableEvidence(t *testing.T) {
	d, paths, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-evidence-conflict")
	if _, err := d.store.RecordPlan(context.Background(), store.PlanArtifact{
		Ref: started.Ref, ExpectedVersion: started.Version,
		Fence:    domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch},
		Document: store.PlanDocument{Acceptance: []string{"one"}, ProofKind: "focused", Paths: []string{"x.go"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"one"}},
	}); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(context.Background(), `UPDATE plans SET artifact_bytes='{}' WHERE channel='stable' AND project_id='demo' AND ticket_id='SF-evidence-conflict'`); err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "show-conflict", Method: "ticket.show", Ticket: string(started.Ref.Ticket),
		OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"demo","ticket":"SF-evidence-conflict"}`),
	})
	if response.OK || response.Error == nil || response.Error.Code != "evidence_conflict" || response.NextAction == nil || strings.Join(response.NextAction.Argv, " ") != "sf doctor" {
		t.Fatalf("corrupt evidence response=%+v", response)
	}
}

func TestSubmitRejectsUnregisteredProjectBeforeTicketPersistence(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		t.Run(string(channel), func(t *testing.T) {
			d, _, _ := testDaemonForChannel(t, channel)
			request := api.Request{
				Version: api.Version, RequestID: "missing-project", Method: "ticket.submit",
				Parameters: []byte(`{"channel":"` + string(channel) + `","project":"missing","source":"# Missing project\n\nKeep the behavior deterministic.\n"}`),
			}
			response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
			binary := "sf"
			if channel == domain.ChannelDev {
				binary = "sf-dev"
			}
			if response.OK || response.Error == nil || response.Error.Code != "unknown_project" || response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join([]string{binary, "init", "--help"}, "\x00") {
				t.Fatalf("missing-project response=%+v", response)
			}
			tickets, err := d.store.Tickets(context.Background(), channel, "missing", 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(tickets) != 0 {
				t.Fatalf("unregistered project persisted tickets: %+v", tickets)
			}
		})
	}
}

func writeTicket(t *testing.T, dir, title string) string {
	t.Helper()
	path := filepath.Join(dir, strings.ToLower(strings.ReplaceAll(title, " ", "-"))+".md")
	contents := "# " + title + "\n\nKeep the behavior deterministic.\n\n## Acceptance\n- A durable result exists\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func executeCLI(t *testing.T, ctx context.Context, paths config.ChannelPaths, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut strings.Builder
	code := cli.Execute(ctx, args, &out, &errOut, cli.SocketClient{Path: paths.Socket, Timeout: 5 * time.Second})
	return code, out.String(), errOut.String()
}

func TestForegroundCLIListShowStartAndReplay(t *testing.T) {
	d, paths, _ := testDaemon(t)
	dir := t.TempDir()
	first, second := writeTicket(t, dir, "First ticket"), writeTicket(t, dir, "Second ticket")
	if code, _, errOut := executeCLI(t, context.Background(), paths, "submit", first, "--project", "demo"); code != 0 || errOut != "" {
		t.Fatalf("first submit code=%d stderr=%q", code, errOut)
	}
	if code, _, errOut := executeCLI(t, context.Background(), paths, "submit", second, "--project", "demo"); code != 0 || errOut != "" {
		t.Fatalf("second submit code=%d stderr=%q", code, errOut)
	}
	code, list, errOut := executeCLI(t, context.Background(), paths, "status", "--json")
	if code != 0 || errOut != "" || !strings.Contains(list, "SF-test-1") || !strings.Contains(list, "SF-test-2") {
		t.Fatalf("status code=%d output=%q stderr=%q", code, list, errOut)
	}
	code, shown, errOut := executeCLI(t, context.Background(), paths, "show", "SF-test-1", "--json")
	if code != 0 || errOut != "" || !strings.Contains(shown, "First ticket") || !strings.Contains(shown, "source_digest") {
		t.Fatalf("show code=%d output=%q stderr=%q", code, shown, errOut)
	}
	code, started, errOut := executeCLI(t, context.Background(), paths, "start", "SF-test-1", "--json")
	if code != 0 || errOut != "" || !strings.Contains(started, "planning") {
		t.Fatalf("start code=%d output=%q stderr=%q", code, started, errOut)
	}
	code, replay, errOut := executeCLI(t, context.Background(), paths, "start", "SF-test-1", "--json")
	if code != 0 || errOut != "" || !strings.Contains(replay, "\"observed\":true") {
		t.Fatalf("replay code=%d output=%q stderr=%q", code, replay, errOut)
	}
	events, err := d.store.Events(context.Background(), domain.ChannelStable, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	var submit, start int
	for _, event := range events {
		switch event.Trigger {
		case "submit_valid":
			submit++
		case "operator_start":
			start++
		}
	}
	if submit != 2 || start != 1 {
		t.Fatalf("normative event counts submit=%d start=%d events=%+v", submit, start, events)
	}
}

func TestDaemonRebuildsRedactedEventProjectionAfterMutations(t *testing.T) {
	_, paths, _ := testDaemon(t)
	ticketPath := writeTicket(t, t.TempDir(), "Projected ticket")
	if code, _, errOut := executeCLI(t, context.Background(), paths, "submit", ticketPath, "--project", "demo"); code != 0 || errOut != "" {
		t.Fatalf("submit code=%d stderr=%q", code, errOut)
	}
	if code, _, errOut := executeCLI(t, context.Background(), paths, "start", "SF-test-1"); code != 0 || errOut != "" {
		t.Fatalf("start code=%d stderr=%q", code, errOut)
	}
	projectionPath := filepath.Join(paths.Events, "events.ndjson")
	info, err := os.Lstat(projectionPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("projection identity info=%v err=%v", info, err)
	}
	data, err := os.ReadFile(projectionPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("projection lines=%d data=%s", len(lines), data)
	}
	for index, line := range lines {
		var record events.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("line %d record=%+v err=%v", index, record, err)
		}
		if err := record.Validate(); err != nil {
			t.Fatalf("line %d record=%+v err=%v", index, record, err)
		}
	}
	if !strings.Contains(string(data), `"trigger":"submit_valid"`) || !strings.Contains(string(data), `"trigger":"operator_start"`) {
		t.Fatalf("projection lacks normative events: %s", data)
	}
}

func TestProjectionFailureReportsCommittedMutationAndReadRepairsIt(t *testing.T) {
	d, paths, _ := testDaemon(t)
	original := d.projectionPath
	d.projectionMu.Lock()
	d.projectionPath = filepath.Join(paths.Root, "missing-parent", "events.ndjson")
	d.projectionMu.Unlock()
	ticketPath := writeTicket(t, t.TempDir(), "Projection repair")
	code, output, errOut := executeCLI(t, context.Background(), paths, "submit", ticketPath, "--project", "demo")
	if code != int(cli.ExitWait) || errOut != "" || !strings.Contains(output, "projection_unavailable") || !strings.Contains(output, "Mutation: attempted") || !strings.Contains(output, "sf status SF-test-1") {
		t.Fatalf("submit code=%d output=%q stderr=%q", code, output, errOut)
	}
	if _, err := d.store.TicketByID(context.Background(), domain.ChannelStable, "SF-test-1"); err != nil {
		t.Fatalf("committed ticket was lost: %v", err)
	}
	d.projectionMu.Lock()
	d.projectionPath = original
	d.projectionMu.Unlock()
	request := api.Request{Version: api.Version, RequestID: "repair-projection", Method: "daemon.status", Parameters: []byte(`{}`)}
	response, err := transport.Call(context.Background(), paths.Socket, request)
	if err != nil || !response.OK || !strings.Contains(string(response.Data), `"event_projection_ready":true`) {
		t.Fatalf("status response=%+v err=%v", response, err)
	}
	data, err := os.ReadFile(original)
	if err != nil || !strings.Contains(string(data), "SF-test-1") {
		t.Fatalf("repaired projection=%q err=%v", data, err)
	}
}

func TestForegroundDaemonStatusAndNormativeEvents(t *testing.T) {
	d, paths, _ := testDaemon(t)
	request := api.Request{Version: api.Version, RequestID: "status-test", Method: "daemon.status", Parameters: []byte(`{}`)}
	response, err := transport.Call(context.Background(), paths.Socket, request)
	if err != nil || !response.OK {
		t.Fatalf("daemon status response=%+v err=%v", response, err)
	}
	events, err := d.store.Events(context.Background(), domain.ChannelStable, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("unexpected events before submit: %+v", events)
	}
}

func TestDaemonSocketFallbackRetainsTheChannelExecutable(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sfv2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	server, err := transport.ListenWithExecutable(filepath.Join(root, "sf.sock"), uint32(os.Getuid()), transport.HandlerFunc(func(context.Context, transport.Peer, api.Request) api.Response {
		return api.Response{OK: false}
	}), "sf-dev")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})
	response, err := transport.Call(context.Background(), filepath.Join(root, "sf.sock"), api.Request{Version: api.Version, RequestID: "invalid-response", Method: "ticket.submit"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "internal_response_invalid" || response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join([]string{"sf-dev", "doctor"}, "\x00") {
		t.Fatalf("invalid response fallback=%+v", response)
	}
}

func TestForegroundRejectsWrongOperatorAndSecondLeader(t *testing.T) {
	_, paths, _ := testDaemon(t)
	request := api.Request{Version: api.Version, RequestID: "wrong-operator", Method: "daemon.status", OperatorLabel: "intruder", Parameters: []byte(`{}`)}
	response, err := transport.Call(context.Background(), paths.Socket, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.OK || response.Error == nil || response.Error.Code != "operator_identity_required" || response.NextAction == nil {
		t.Fatalf("wrong operator response=%+v", response)
	}
	second, err := leader.Acquire(filepath.Join(paths.Root, "run", "leader.lock"), domain.ChannelStable, "second")
	if err == nil {
		_ = second.Close()
		t.Fatal("second leader unexpectedly acquired")
	}
	if !errors.Is(err, leader.ErrLeaderExists) {
		t.Fatalf("second leader error=%v", err)
	}
}

func TestRestartFencesPlanningRunnerWithoutDroppingItsLease(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	dir := t.TempDir()
	ticket := writeTicket(t, dir, "Restart fence")
	if code, _, stderr := executeCLI(t, context.Background(), paths, "submit", ticket, "--project", "demo"); code != 0 || stderr != "" {
		t.Fatalf("submit code=%d stderr=%q", code, stderr)
	}
	if code, _, stderr := executeCLI(t, context.Background(), paths, "start", "SF-test-1"); code != 0 || stderr != "" {
		t.Fatalf("start code=%d stderr=%q", code, stderr)
	}
	ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "demo", Ticket: "SF-test-1"}
	before, err := d.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeLeases, err := d.store.Leases(context.Background(), domain.ChannelStable)
	if err != nil || len(beforeLeases) == 0 {
		t.Fatalf("before restart leases=%+v err=%v", beforeLeases, err)
	}
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// The production path uses the embedded reviewed artifact and may start with
	// no configured projects when the durable registration already exists.
	restarted, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "restart-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	after, err := restarted.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.RunnerEpoch != before.RunnerEpoch+1 || after.Version != before.Version+1 {
		t.Fatalf("recovery did not fence runner: before=%+v after=%+v", before, after)
	}
	leases, err := restarted.store.Leases(context.Background(), domain.ChannelStable)
	if err != nil || len(leases) != len(beforeLeases) {
		t.Fatalf("recovery changed capacity occupancy: leases=%+v err=%v", leases, err)
	}
	for index, lease := range leases {
		if lease.Ref != beforeLeases[index].Ref || lease.Scope != beforeLeases[index].Scope || lease.ScopeKey != beforeLeases[index].ScopeKey || !lease.AcquiredAt.Equal(beforeLeases[index].AcquiredAt) || lease.RunnerEpoch != after.RunnerEpoch {
			t.Fatalf("recovery did not transfer exact lease: before=%+v after=%+v", beforeLeases[index], lease)
		}
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	secondRestart, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "restart-test-second"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondRestart.Close() })
	afterSecondRestart, err := secondRestart.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if afterSecondRestart.RunnerEpoch != after.RunnerEpoch+1 || afterSecondRestart.Version != after.Version+1 {
		t.Fatalf("second recovery did not fence runner: first=%+v second=%+v", after, afterSecondRestart)
	}
	secondLeases, err := secondRestart.store.Leases(context.Background(), domain.ChannelStable)
	if err != nil || len(secondLeases) != len(leases) {
		t.Fatalf("second recovery changed capacity occupancy: leases=%+v err=%v", secondLeases, err)
	}
	for index, lease := range secondLeases {
		if lease.Scope != leases[index].Scope || lease.ScopeKey != leases[index].ScopeKey || !lease.AcquiredAt.Equal(leases[index].AcquiredAt) || lease.RunnerEpoch != afterSecondRestart.RunnerEpoch {
			t.Fatalf("second recovery was not idempotent transfer: first=%+v second=%+v", leases[index], lease)
		}
	}
}

func TestRestartRefusesAmbiguousLeaseAdoptionBeforeSocketExposure(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-restart-ambiguous-lease")
	if _, err := d.store.AcquireLeases(context.Background(), started.Ref, started.Version, domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: started.RunnerEpoch}, []store.LeaseRequest{{Scope: "provider", Resource: "recovery-provider", Capacity: 1}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "restart-ambiguous-lease"})
	if !errors.Is(err, store.ErrLeaseAdoption) {
		t.Fatalf("startup error=%v, want lease adoption refusal", err)
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket was exposed despite ambiguous adoption: %v", err)
	}
}

func TestRecoverDrainRequestPreservesChatGPTSubscriptionAuthMode(t *testing.T) {
	claim := store.ProviderAttemptClaim{ID: 41, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-auth-recovery"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2, Binding: contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "codex", Model: "gpt-5.6", Family: "openai", Version: "1"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), AuthDigest: strings.Repeat("c", 64), AuthMode: "chatgpt_subscription"}, LeaseKey: "provider/codex", BindingDigest: strings.Repeat("d", 64), LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: strings.Repeat("e", 40), RequestDigest: strings.Repeat("f", 64)}
	req := drainRequestForProviderClaim(claim)
	if req.AuthMode != "chatgpt_subscription" || req.RequestDigest != claim.RequestDigest {
		t.Fatalf("recovery request lost authenticated launch fields: %+v", req)
	}
}

func TestStartupBusyHonorsConfiguredDeadline(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	locked, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = locked.Close() })
	connection, err := locked.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer connection.ExecContext(context.Background(), "ROLLBACK")
	started := time.Now()
	_, err = Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "busy-test", StartupTimeout: 40 * time.Millisecond})
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("startup error=%v, want typed busy", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("startup outlived deadline: %s", elapsed)
	}
}

func TestStartRejectsSymlinkedExactSocketParent(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(root, "socket-link")); err != nil {
		t.Fatal(err)
	}
	paths := config.ChannelPaths{
		Root: root, Database: filepath.Join(root, "sf.sqlite"), Socket: filepath.Join(root, "socket-link", "sf.sock"),
		Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "backups"),
	}
	if _, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "symlink-test"}); err == nil {
		t.Fatal("daemon accepted a symlinked socket parent")
	}
}
