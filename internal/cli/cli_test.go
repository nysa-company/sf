package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

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
	if code := Execute(context.Background(), []string{"submit", "ticket.md", "--project", "nysa", "--new"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("submit exit=%d", code)
	}
	var parameters struct {
		New bool `json:"new"`
	}
	if err := json.Unmarshal(requests[1].Parameters, &parameters); err != nil || !parameters.New {
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
