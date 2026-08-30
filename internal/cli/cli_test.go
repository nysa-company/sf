package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func responseOK() api.Response {
	return api.Response{Version: api.Version, RequestID: "request", OK: true, Mutation: api.Mutation{}, Data: json.RawMessage(`{"state":"queued"}`)}
}

func TestPrimaryCommandsAndSecondarySetupCommandsExist(t *testing.T) {
	command := NewCommand(nil, &bytes.Buffer{}, &bytes.Buffer{})
	want := []string{"submit", "start", "status", "show", "logs", "pause", "resume", "recover", "cancel", "retry", "take", "approve", "reject", "doctor", "auth", "init", "providers", "daemon", "config", "update", "rollback", "version"}
	seen := make(map[string]bool)
	for _, child := range command.Commands() {
		seen[child.Name()] = true
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("missing command %q", name)
		}
	}
}

func TestLifecycleCommandUsesSocketClientAndChannelIdentity(t *testing.T) {
	var got api.Request
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		got = request
		return responseOK(), nil
	})
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"status", "SF-1"}, &output, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
	if got.Method != "ticket.status" || got.Ticket != "SF-1" {
		t.Fatalf("request=%+v", got)
	}
	var params map[string]any
	if err := json.Unmarshal(got.Parameters, &params); err != nil {
		t.Fatal(err)
	}
	if params["channel"] != string(domain.ChannelStable) {
		t.Fatalf("channel=%v", params["channel"])
	}
}

func TestStatusWatchPollsUntilContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var requests int
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		requests++
		if request.Method != "ticket.status" {
			t.Fatalf("method=%q", request.Method)
		}
		var parameters map[string]any
		if err := json.Unmarshal(request.Parameters, &parameters); err != nil || parameters["watch"] != false {
			t.Fatalf("parameters=%s err=%v", request.Parameters, err)
		}
		return responseOK(), nil
	})
	if code := Execute(ctx, []string{"status", "SF-1", "--watch"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit=%d", code)
	}
	if requests != 1 {
		t.Fatalf("watch requests=%d, want one bounded initial poll", requests)
	}
}

func TestStatusWatchStopsWhenTicketIsTerminal(t *testing.T) {
	var requests int
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		requests++
		response := responseOK()
		response.Data = json.RawMessage(`{"ticket":{"state":"done"}}`)
		return response, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if code := Execute(ctx, []string{"status", "SF-1", "--watch"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit=%d", code)
	}
	if requests != 1 {
		t.Fatalf("terminal watch requests=%d, want 1", requests)
	}
}

func TestLogsFollowUsesDurableCursorAndStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var got api.Request
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		got = request
		return api.Response{Version: api.Version, RequestID: request.RequestID, OK: true, Mutation: api.Mutation{}, Data: json.RawMessage(`{"next_after":7,"events":[]}`)}, nil
	})
	var output bytes.Buffer
	if code := Execute(ctx, []string{"logs", "SF-1", "--follow", "--phase", "build"}, &output, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
	if got.Method != "ticket.logs" || got.Ticket != "SF-1" {
		t.Fatalf("request=%+v", got)
	}
	var parameters struct {
		Follow bool   `json:"follow"`
		Phase  string `json:"phase"`
		After  uint64 `json:"after"`
	}
	if err := json.Unmarshal(got.Parameters, &parameters); err != nil || !parameters.Follow || parameters.Phase != "build" || parameters.After != 0 {
		t.Fatalf("parameters=%s decoded=%+v err=%v", got.Parameters, parameters, err)
	}
}

func TestMutatingOperatorLabelIsForwardedToDaemon(t *testing.T) {
	var got api.Request
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		got = request
		return responseOK(), nil
	})
	if code := Execute(context.Background(), []string{"pause", "SF-1", "--operator", "sofia"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit=%d", code)
	}
	if got.OperatorLabel != "sofia" {
		t.Fatalf("operator label=%q", got.OperatorLabel)
	}
}

func TestOperatorDefaultsAndSubmitNewAreForwarded(t *testing.T) {
	var requests []api.Request
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		requests = append(requests, request)
		return responseOK(), nil
	})
	if code := Execute(context.Background(), []string{"pause", "SF-1"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("pause exit=%d", code)
	}
	if requests[0].OperatorLabel == "" {
		t.Fatal("default operator label was not forwarded")
	}
	path := filepath.Join(t.TempDir(), "ticket.md")
	if err := os.WriteFile(path, []byte("# Test ticket\n\nProblem to solve.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := Execute(context.Background(), []string{"submit", path, "--project", "nysa", "--new"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("submit exit=%d", code)
	}
	var parameters struct {
		New    bool   `json:"new"`
		Source string `json:"source"`
	}
	if err := json.Unmarshal(requests[1].Parameters, &parameters); err != nil || !parameters.New || parameters.Source != "# Test ticket\n\nProblem to solve.\n" {
		t.Fatalf("parameters=%s err=%v", requests[1].Parameters, err)
	}
}

func TestErrorResponseHasOneExecutableNextActionAndMappedExit(t *testing.T) {
	response := api.Response{Version: api.Version, RequestID: "request", Mutation: api.Mutation{}, Error: &api.Error{Code: "provider_auth_missing", Message: "provider login is unavailable"}, NextAction: &domain.NextAction{Code: "auth", Argv: []string{"sf-dev", "auth", "login", "cursor"}}}
	client := fakeClient(func(_ context.Context, _ api.Request) (api.Response, error) { return response, nil })
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"start", "SF-1"}, &output, &bytes.Buffer{}, client); code != int(ExitAction) {
		t.Fatalf("exit=%d", code)
	}
	if got := output.String(); strings.Count(got, "Next:") != 1 || !strings.Contains(got, "sf-dev auth login cursor") {
		t.Fatalf("output=%q", got)
	}
}

func TestJSONRenderingPreservesResponseEnvelope(t *testing.T) {
	var output bytes.Buffer
	if err := Render(&output, responseOK(), true); err != nil {
		t.Fatal(err)
	}
	var decoded api.Response
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.OK || decoded.Version != api.Version || string(decoded.Data) != `{"state":"queued"}` {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestNextActionJSONUsesNormativeLowercaseFields(t *testing.T) {
	response := failure("blocked_process", "blocked", []string{"sf", "recover", "SF-1"})
	var output bytes.Buffer
	if err := Render(&output, response, true); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, `"next_action":{"code":"blocked_process","argv":[`) || strings.Contains(text, `"Code"`) || strings.Contains(text, `"Argv"`) {
		t.Fatalf("non-normative response=%s", text)
	}
}

func TestSocketRequestReceivesExecuteContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var received context.Context
	client := fakeClient(func(got context.Context, _ api.Request) (api.Response, error) {
		received = got
		return api.Response{}, errors.New("canceled")
	})
	if code := Execute(ctx, []string{"status", "SF-1"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != int(ExitWait) {
		t.Fatalf("exit=%d", code)
	}
	if received == nil || !errors.Is(received.Err(), context.Canceled) {
		t.Fatalf("request context was not propagated: %v", received)
	}
}

func TestInvalidDaemonFailureGetsSafeInternalJSONResponse(t *testing.T) {
	client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
		return api.Response{Version: api.Version, RequestID: "request", Mutation: api.Mutation{}, Error: &api.Error{Code: "blocked_process", Message: "blocked"}}, nil
	})
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"start", "SF-1", "--json"}, &output, &bytes.Buffer{}, client); code != int(ExitInternal) {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
	var response api.Response
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "internal_error" || response.NextAction == nil || len(response.NextAction.Argv) == 0 {
		t.Fatalf("response=%+v", response)
	}
}

func TestIncompatibleDaemonEnvelopeGetsCompatibilityExit(t *testing.T) {
	client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
		return api.Response{Version: "sf.local/v0", RequestID: "request", Mutation: api.Mutation{}, OK: true}, nil
	})
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"status", "SF-1", "--json"}, &output, &bytes.Buffer{}, client); code != int(ExitCompatibility) {
		t.Fatalf("exit=%d output=%s", code, output.String())
	}
	var response api.Response
	if err := json.Unmarshal(output.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "protocol_incompatible" {
		t.Fatalf("response=%+v", response)
	}
}

func TestRequestIDsAreNotTimestampOnly(t *testing.T) {
	first, second := requestID(), requestID()
	if first == second || !strings.HasPrefix(first, "cli-") {
		t.Fatalf("request IDs are not unique: %q %q", first, second)
	}
}

func TestSetupFlagsAreRequired(t *testing.T) {
	for _, args := range [][]string{{"init"}, {"providers", "qualify"}} {
		if code := Execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, nil); code != int(ExitInput) {
			t.Errorf("args=%v exit=%d", args, code)
		}
	}
}
