package cli

import (
	"bytes"
	"context"
	"encoding/json"
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
