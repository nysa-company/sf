package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/leader"
	"github.com/nysa-company/sf/internal/operator"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
)

type testIDs struct{ next int }

func (g *testIDs) NewTicketID(domain.Channel) (domain.TicketID, error) {
	g.next++
	return domain.TicketID(fmt.Sprintf("SF-test-%d", g.next)), nil
}

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

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
	if err != nil || len(leases) == 0 || leases[0].RunnerEpoch != before.RunnerEpoch {
		t.Fatalf("recovery incorrectly released or rewrote stale leases: leases=%+v err=%v", leases, err)
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
