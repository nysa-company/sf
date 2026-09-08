package daemon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/transport"
)

func TestCLIExternalCleanupLostCheckpointResponseIsReplayable(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("production host checkpoint is macOS-only")
	}
	d, paths, _ := testDaemon(t)
	ctx := context.Background()
	if err := d.store.QuarantineExternalMutations(ctx); err != nil {
		t.Fatal(err)
	}
	client := &lostRunResponseClient{client: cli.SocketClient{Path: paths.Socket, Timeout: 5 * time.Second}, method: "daemon.cleanup.prepare"}
	var out, stderr strings.Builder
	if code := cli.Execute(ctx, []string{"daemon", "cleanup", "prepare", "--json"}, &out, &stderr, client); code == 0 || !client.dropped {
		t.Fatal("fixture did not lose committed checkpoint response")
	}
	code, raw, _ := executeCLI(t, ctx, paths, "daemon", "cleanup", "prepare", "--json")
	var response api.Response
	if code != 0 || json.Unmarshal([]byte(raw), &response) != nil || !response.OK || response.Mutation.Attempted || !response.Mutation.Observed {
		t.Fatalf("checkpoint replay=%s", raw)
	}
	if quarantined, err := d.store.ExternalMutationsQuarantined(ctx); err != nil || !quarantined {
		t.Fatal("lost-response replay cleared quarantine")
	}
}

func TestCLIExternalCleanupCheckpointDoesNotClearBeforeReboot(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("production host checkpoint is macOS-only")
	}
	d, paths, _ := testDaemon(t)
	ctx := context.Background()
	if err := d.store.QuarantineExternalMutations(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		code, out, stderr := executeCLI(t, ctx, paths, "daemon", "cleanup", "prepare", "--json")
		var response api.Response
		if code != 0 || stderr != "" || json.Unmarshal([]byte(out), &response) != nil || !response.OK {
			t.Fatalf("prepare code=%d output=%s stderr=%s", code, out, stderr)
		}
		var data struct{ State string }
		if json.Unmarshal(response.Data, &data) != nil || data.State != "reboot_required" || response.Mutation.Attempted != (i == 0) || response.Mutation.Observed != (i != 0) {
			t.Fatalf("prepare result=%+v data=%s", response, response.Data)
		}
		if response.NextAction == nil || response.NextAction.Code != "host_reboot_required" {
			t.Fatal("checkpoint omitted reboot prerequisite")
		}
	}
	code, out, _ := executeCLI(t, ctx, paths, "daemon", "cleanup", "recover", "--json")
	var response api.Response
	if code != int(cli.ExitAction) || json.Unmarshal([]byte(out), &response) != nil || response.Error == nil || response.Error.Code != "host_reboot_required" {
		t.Fatalf("same-boot recovery=%s", out)
	}
	if quarantined, err := d.store.ExternalMutationsQuarantined(ctx); err != nil || !quarantined {
		t.Fatal("CLI cleared same-boot quarantine")
	}
}

func TestDaemonExternalCleanupRejectsCallerEvidenceAndWrongChannel(t *testing.T) {
	d, _, _ := testDaemon(t)
	for _, parameters := range []string{`{"channel":"dev"}`, `{"channel":"stable","boot_id":"forged"}`, `{"channel":"stable","machine_digest":"forged"}`} {
		response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "invalid-cleanup", Method: "daemon.cleanup.prepare", Parameters: json.RawMessage(parameters)})
		if response.OK || response.Error == nil || response.Error.Code != "invalid_argument" {
			t.Fatalf("untrusted parameters accepted: %+v", response)
		}
	}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid()) + 1}, api.Request{Version: api.Version, RequestID: "wrong-owner", Method: "daemon.cleanup.prepare", Parameters: json.RawMessage(`{"channel":"stable"}`)})
	if response.OK || response.Error == nil || response.Error.Code != "operator_identity_required" {
		t.Fatal("cleanup bypassed owner authentication")
	}
}

func TestDaemonExternalCleanupCheckpointSurvivesRestartWithoutReboot(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("production host checkpoint is macOS-only")
	}
	d, paths, cancel := testDaemon(t)
	ctx := context.Background()
	if err := d.store.QuarantineExternalMutations(ctx); err != nil {
		t.Fatal(err)
	}
	request := api.Request{Version: api.Version, RequestID: "checkpoint", Method: "daemon.cleanup.prepare", Parameters: json.RawMessage(`{"channel":"stable"}`)}
	peer := transport.Peer{UID: uint32(os.Getuid())}
	first := d.Handle(ctx, peer, request)
	if !first.OK || !first.Mutation.Attempted {
		t.Fatalf("checkpoint=%+v", first)
	}
	operator := d.auth
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	stateMachine := filepath.Join(filepath.Dir(file), "..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	reopened, err := Start(ctx, Config{Channel: domain.ChannelStable, Paths: paths, StateMachinePath: stateMachine, DaemonIdentity: "cleanup-restart", Operator: operator})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	replay := reopened.Handle(ctx, peer, request)
	if !replay.OK || replay.Mutation.Attempted || !replay.Mutation.Observed {
		t.Fatalf("restart replay=%+v", replay)
	}
	request.Method = "daemon.cleanup.recover"
	refused := reopened.Handle(ctx, peer, request)
	if refused.OK || refused.Error == nil || refused.Error.Code != "host_reboot_required" {
		t.Fatalf("restart was treated as reboot: %+v", refused)
	}
}
