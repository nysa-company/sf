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

func TestConfigApplyCommandExists(t *testing.T) {
	command := NewCommand(nil, &bytes.Buffer{}, &bytes.Buffer{})
	var configCommandFound bool
	for _, child := range command.Commands() {
		if child.Name() != "config" {
			continue
		}
		configCommandFound = true
		var applyFound bool
		for _, nested := range child.Commands() {
			if nested.Name() == "apply" {
				applyFound = true
			}
		}
		if !applyFound {
			t.Fatal("config apply command is missing")
		}
	}
	if !configCommandFound {
		t.Fatal("config command is missing")
	}
}

func TestRootHelpUsesTheSelectedChannelBinary(t *testing.T) {
	for _, test := range []struct {
		channel domain.Channel
		want    string
	}{{domain.ChannelStable, "sf"}, {domain.ChannelDev, "sf-dev"}} {
		command := (&app{out: &bytes.Buffer{}, errOut: &bytes.Buffer{}, channel: test.channel, ctx: context.Background()}).command()
		if command.Use != test.want {
			t.Fatalf("channel=%s use=%q, want %q", test.channel, command.Use, test.want)
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

func TestLifecycleVerbsForwardTheirMethodsChannelAndOperator(t *testing.T) {
	ticketPath := filepath.Join(t.TempDir(), "ticket.md")
	if err := os.WriteFile(ticketPath, []byte("# Test ticket\n\nProblem text.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		args   []string
		method string
		ticket string
	}{
		{name: "submit", args: []string{"submit", ticketPath, "--project", "demo"}, method: "ticket.submit"},
		{name: "start", args: []string{"start", "SF-1"}, method: "ticket.start", ticket: "SF-1"},
		{name: "status", args: []string{"status", "SF-1"}, method: "ticket.status", ticket: "SF-1"},
		{name: "show", args: []string{"show", "SF-1"}, method: "ticket.show", ticket: "SF-1"},
		{name: "logs", args: []string{"logs", "SF-1", "--phase", "build"}, method: "ticket.logs", ticket: "SF-1"},
		{name: "pause", args: []string{"pause", "SF-1", "--operator", "sofia"}, method: "ticket.pause", ticket: "SF-1"},
		{name: "resume", args: []string{"resume", "SF-1", "--operator", "sofia"}, method: "ticket.resume", ticket: "SF-1"},
		{name: "recover", args: []string{"recover", "SF-1", "--mode", "guarded", "--operator", "sofia"}, method: "ticket.recover", ticket: "SF-1"},
		{name: "cancel", args: []string{"cancel", "SF-1", "--operator", "sofia"}, method: "ticket.cancel", ticket: "SF-1"},
		{name: "retry", args: []string{"retry", "SF-1", "--operator", "sofia"}, method: "ticket.retry", ticket: "SF-1"},
		{name: "take", args: []string{"take", "SF-1", "--operator", "sofia"}, method: "ticket.take", ticket: "SF-1"},
		{name: "approve", args: []string{"approve", "SF-1", "--operator", "sofia"}, method: "ticket.approve", ticket: "SF-1"},
		{name: "reject", args: []string{"reject", "SF-1", "--operator", "sofia", "--reason", "needs tests"}, method: "ticket.reject", ticket: "SF-1"},
		{name: "providers qualify", args: []string{"providers", "qualify", "--builder", "cursor", "--reviewer", "claude"}, method: "provider.qualify"},
		{name: "daemon status", args: []string{"daemon", "status"}, method: "daemon.status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got api.Request
			client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
				got = request
				return responseOK(), nil
			})
			if code := Execute(context.Background(), test.args, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
				t.Fatalf("exit=%d", code)
			}
			if got.Method != test.method || got.Ticket != test.ticket {
				t.Fatalf("request=%+v", got)
			}
			var parameters map[string]any
			if err := json.Unmarshal(got.Parameters, &parameters); err != nil {
				t.Fatal(err)
			}
			if parameters["channel"] != string(domain.ChannelStable) {
				t.Fatalf("channel=%v parameters=%s", parameters["channel"], got.Parameters)
			}
			operatorCommands := map[string]bool{"pause": true, "resume": true, "recover": true, "cancel": true, "retry": true, "take": true, "approve": true, "reject": true}
			if operatorCommands[test.name] && parameters["operator"] != "sofia" {
				t.Fatalf("operator=%v parameters=%s", parameters["operator"], got.Parameters)
			}
			if test.name == "reject" && parameters["reason"] != "needs tests" {
				t.Fatalf("reason=%v", parameters["reason"])
			}
			if test.name == "providers qualify" && (parameters["builder"] != "cursor" || parameters["reviewer"] != "claude") {
				t.Fatalf("provider parameters=%v", parameters)
			}
		})
	}
}

func TestRetryTooLongReasonNeverSuggestsPlaceholder(t *testing.T) {
	// Keep a long value on reject's local validation path to ensure its action
	// remains an executable command rather than a substitution template.
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"reject", "SF-1", "--reason", strings.Repeat("x", 4097)}, &output, &bytes.Buffer{}, nil); code != int(ExitInput) {
		t.Fatalf("exit=%d output=%q", code, output.String())
	}
	if strings.Contains(output.String(), "<short-reason>") || !strings.Contains(output.String(), "Next: "+binaryName()+" reject --help") {
		t.Fatalf("output=%q", output.String())
	}
}

func TestHumanRendererProjectsKnownTicketAndLogShapes(t *testing.T) {
	tests := []struct {
		name string
		data string
		want []string
	}{
		{name: "ticket", data: `{"channel":"dev","project":"nysa","ticket":"SF-1","state":"waiting_approval","merge_mode":"guarded"}`, want: []string{"SF-1", "Channel: dev", "State: waiting_approval", "Merge mode: guarded"}},
		{name: "detail", data: `{"channel":"stable","project":"nysa","ticket":"SF-2","state":"done","title":"Fix reminders","problem":"A bounded problem.","acceptance":["one"]}`, want: []string{"SF-2  Fix reminders", "Problem: A bounded problem.", "Acceptance: 1 item(s)"}},
		{name: "takeover", data: `{"channel":"dev","project":"nysa","ticket":"SF-4","state":"paused","resume_state":"building","takeover":{"registered":true,"path":"/private/tmp/SF-4","branch":"sf/SF-4","repository":"/private/tmp/repo","base_sha":"base","head_sha":"head","clean":true,"change_kind":"none","changed_files":[],"source_resumable":false}}`, want: []string{"SF-4", "State: paused", "Resume state: building", "Takeover worktree: /private/tmp/SF-4", "Branch: sf/SF-4", "Repository: /private/tmp/repo", "Local base: base", "Local head: head", "Change kind: none"}},
		{name: "early_takeover", data: `{"channel":"dev","project":"nysa","ticket":"SF-5","state":"paused","resume_state":"planning","takeover":{"registered":false,"path":"","clean":true,"change_kind":"no_worktree","changed_files":[],"source_resumable":false},"next_action":{"code":"resume","argv":["sf-dev","resume","SF-5"]}}`, want: []string{"SF-5", "State: paused", "Resume state: planning", "Takeover worktree: not created yet", "Next: sf-dev resume SF-5"}},
		{name: "ticket_budget", data: `{"channel":"dev","ticket":{"channel":"dev","project":"relay","ticket":"SF-6","state":"blocked","resume_state":"planning","blocked_code":"ticket_budget_exhausted"},"next_action":{"code":"ticket_budget_exhausted","argv":["sf-dev","cancel","SF-6"]}}`, want: []string{"SF-6", "State: blocked", "Resume state: planning", "Blocker: ticket_budget_exhausted", "Next: sf-dev cancel SF-6"}},
		{name: "ticket_budget_list", data: `{"channel":"dev","tickets":[{"channel":"dev","project":"relay","ticket":"SF-6","state":"blocked","blocked_code":"ticket_budget_exhausted","next_action":{"code":"ticket_budget_exhausted","argv":["sf-dev","cancel","SF-6"]}}]}`, want: []string{"Tickets (dev)", "- SF-6  blocked", "Next: sf-dev cancel SF-6"}},
		{name: "verification_amendment_invalid", data: `{"channel":"dev","ticket":{"channel":"dev","project":"relay","ticket":"SF-7","state":"blocked","resume_state":"verifying","blocked_code":"verification_amendment_invalid"},"next_action":{"code":"verification_amendment_invalid","argv":["sf-dev","cancel","SF-7"]}}`, want: []string{"SF-7", "State: blocked", "Resume state: verifying", "Blocker: verification_amendment_invalid", "Next: sf-dev cancel SF-7"}},
		{name: "verification_amendment_invalid_list", data: `{"channel":"dev","tickets":[{"channel":"dev","project":"relay","ticket":"SF-7","state":"blocked","blocked_code":"verification_amendment_invalid","next_action":{"code":"verification_amendment_invalid","argv":["sf-dev","cancel","SF-7"]}}]}`, want: []string{"Tickets (dev)", "- SF-7  blocked", "Next: sf-dev cancel SF-7"}},
		{name: "logs", data: `{"channel":"dev","ticket":"SF-3","events":[{"id":4,"from":"planning","to":"verifying"}]}`, want: []string{"Logs: SF-3", "#4 planning -> verifying"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := api.Response{Version: api.Version, RequestID: "human", OK: true, Data: json.RawMessage(test.data)}
			var output bytes.Buffer
			if err := Render(&output, response, false); err != nil {
				t.Fatal(err)
			}
			for _, want := range test.want {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("output=%q missing %q", output.String(), want)
				}
			}
			if test.name == "early_takeover" && strings.Count(output.String(), "Next:") != 1 {
				t.Fatalf("early takeover output has duplicate next actions: %q", output.String())
			}
		})
	}
}

func TestHumanRendererProjectsStatusEvidenceAndOperatorWithoutPaths(t *testing.T) {
	response := api.Response{Version: api.Version, RequestID: "status-detail", OK: true, Data: json.RawMessage(`{
		"channel":"dev","operator":{"uid":501,"username":"sofia","label":"sofia"},
		"ticket":{"ticket":"SF-9","state":"blocked","blocked_code":"blocked_process","merge_mode":"manual"},
		"evidence":{
			"plan":{"digest":"plan-digest","proof_kind":"focused","acceptance_count":2},
			"verification":{"revision":3,"checkpoint_id":"checkpoint-3","intent_digest":"intent","amends_revision":2},
			"candidate":{"generation":4,"head_sha":"head-4"},
			"worktree":{"path":"/private/tmp/secret-worktree","branch":"sf/SF-9","state":"ready"},
			"phase_attempts":[{"phase":"build","attempt":2,"state":"failed","outcome":"blocked_process"}],
			"operator_decisions":[{"id":7,"decision":"reject","reviewed_head":"head-3","invalidated":true}]
		}
	}`)}
	var output bytes.Buffer
	if err := Render(&output, response, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Operator: label=sofia, uid=501", "Blocker: blocked_process", "Plan: digest=plan-digest", "Verification: revision=3, checkpoint=checkpoint-3, intent=intent, amends=2", "Candidate: generation=4, head=head-4", "Worktree: branch=sf/SF-9", "Phase attempts:", "outcome=blocked_process", "Operator decisions:", "invalidated=true"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("status output=%q missing %q", output.String(), want)
		}
	}
	if strings.Contains(output.String(), "/private/tmp/secret-worktree") {
		t.Fatalf("human status leaked absolute worktree path: %q", output.String())
	}
}

func TestJSONRenderingIsUnchangedForAdditiveData(t *testing.T) {
	response := api.Response{Version: api.Version, RequestID: "json", OK: true, Data: json.RawMessage(`{"channel":"dev","ticket":"SF-1","new_field":{"opaque":true}}`)}
	var output bytes.Buffer
	if err := Render(&output, response, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"new_field":{"opaque":true}`) {
		t.Fatalf("json=%q", output.String())
	}
}

func TestUnknownCommandAndDaemonUnavailableHaveStableActions(t *testing.T) {
	var output, errOutput bytes.Buffer
	if code := Execute(context.Background(), []string{"wat"}, &output, &errOutput, nil); code != int(ExitInput) || !strings.Contains(errOutput.String(), "Error: invalid_command:") || !strings.Contains(errOutput.String(), "Next: "+binaryName()+" --help") {
		t.Fatalf("unknown code=%d stdout=%q stderr=%q", code, output.String(), errOutput.String())
	}
	output.Reset()
	if code := Execute(context.Background(), []string{"status", "SF-1"}, &output, &bytes.Buffer{}, nil); code != int(ExitWait) || !strings.Contains(output.String(), "Next: "+binaryName()+" daemon run") {
		t.Fatalf("daemon code=%d output=%q", code, output.String())
	}
}

func TestHelpAndUnknownCommandUseStableOutputContracts(t *testing.T) {
	var help, helpErr bytes.Buffer
	if code := Execute(context.Background(), []string{"--help"}, &help, &helpErr, nil); code != int(ExitOK) {
		t.Fatalf("help exit=%d stdout=%q stderr=%q", code, help.String(), helpErr.String())
	}
	for _, verb := range []string{"submit", "status", "show", "logs", "retry", "doctor"} {
		if !strings.Contains(help.String(), verb) {
			t.Errorf("help missing %q: %s", verb, help.String())
		}
	}
	var unknownJSON, unknownErr bytes.Buffer
	if code := Execute(context.Background(), []string{"unknown", "--json"}, &unknownJSON, &unknownErr, nil); code != int(ExitInput) {
		t.Fatalf("unknown exit=%d stdout=%q stderr=%q", code, unknownJSON.String(), unknownErr.String())
	}
	var response api.Response
	if err := json.Unmarshal(unknownErr.Bytes(), &response); err != nil {
		t.Fatalf("unknown response=%q err=%v", unknownErr.String(), err)
	}
	if response.Error == nil || response.Error.Code != "invalid_command" || response.NextAction == nil || len(response.NextAction.Argv) == 0 {
		t.Fatalf("unknown response=%+v", response)
	}
}
