package daemon

import (
	"context"
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
		Channel: domain.ChannelStable, Paths: paths, StateMachinePath: stateMachine, DaemonIdentity: "integration-test",
		Projects:  []store.Project{{Channel: domain.ChannelStable, ID: "demo", Path: filepath.Join(root, "repo"), BaseRef: "main"}},
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
