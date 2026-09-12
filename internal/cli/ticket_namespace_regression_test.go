package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
)

func TestTicketNamespaceLegacyAndCanonicalRoutesForwardEquivalentRequests(t *testing.T) {
	tests := []struct {
		name   string
		legacy []string
		canon  []string
	}{
		{"list", []string{"tickets", "--project", "app"}, []string{"ticket", "list", "--project", "app"}},
		{"view", []string{"show", "SF-1"}, []string{"ticket", "view", "SF-1"}},
		{"logs", []string{"logs", "SF-1", "--phase", "build"}, []string{"ticket", "logs", "SF-1", "--phase", "build"}},
		{"pause", []string{"pause", "SF-1", "--operator", "sofia"}, []string{"ticket", "pause", "SF-1", "--operator", "sofia"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := func(args []string) api.Request {
				var got api.Request
				client := fakeClient(func(_ context.Context, value api.Request) (api.Response, error) {
					got = value
					return responseOK(), nil
				})
				if code := Execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 {
					t.Fatalf("args=%v exit=%d", args, code)
				}
				return got
			}
			legacy, canonical := request(tc.legacy), request(tc.canon)
			if tc.name == "view" {
				var parameters map[string]any
				if json.Unmarshal(canonical.Parameters, &parameters) != nil || parameters["section"] != "all" {
					t.Fatalf("canonical view must opt into all artifact sections: %s", canonical.Parameters)
				}
				delete(parameters, "section")
				canonical.Parameters, _ = json.Marshal(parameters)
			}
			if legacy.Method != canonical.Method || legacy.Ticket != canonical.Ticket || !reflect.DeepEqual(legacy.Parameters, canonical.Parameters) {
				t.Fatalf("legacy=%+v canonical=%+v", legacy, canonical)
			}
		})
	}
}

func TestNestedTicketArgumentErrorsPointToNestedHelpWithoutDaemon(t *testing.T) {
	var output, errors bytes.Buffer
	calls := 0
	client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
		calls++
		return responseOK(), nil
	})
	if code := Execute(context.Background(), []string{"ticket", "start", "--json"}, &output, &errors, client); code != int(ExitInput) {
		t.Fatalf("exit=%d stderr=%s", code, errors.String())
	}
	var response api.Response
	if err := json.Unmarshal(errors.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || response.NextAction == nil || strings.Join(response.NextAction.Argv, " ") != binaryName()+" ticket start --help" {
		t.Fatalf("calls=%d response=%+v stderr=%s", calls, response, errors.String())
	}
}

func TestTicketStartRejectsIDAndFileTogetherBeforeMutation(t *testing.T) {
	calls := 0
	client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
		calls++
		return responseOK(), nil
	})
	var errors bytes.Buffer
	if code := Execute(context.Background(), []string{"ticket", "start", "SF-1", "--file", "draft.md", "--project", "app", "--json"}, &bytes.Buffer{}, &errors, client); code != int(ExitInput) {
		t.Fatalf("exit=%d stderr=%s", code, errors.String())
	}
	if calls != 0 {
		t.Fatalf("start submitted despite mutually exclusive ID/file: %d calls", calls)
	}
}

func TestFactoryRunAndStatusMatchDaemonCompatibilityRoutes(t *testing.T) {
	var runCalls, statusCalls int
	run := func(args []string) (api.Request, string) {
		var got api.Request
		var output bytes.Buffer
		a := newApp(fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
			got = request
			return responseOK(), nil
		}), &output, &bytes.Buffer{})
		a.runDaemon = func(context.Context) error { runCalls++; return nil }
		command := a.command()
		command.SetArgs(args)
		if err := command.ExecuteContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		return got, output.String()
	}
	legacyRun, _ := run([]string{"daemon", "run"})
	canonicalRun, _ := run([]string{"factory", "run"})
	if runCalls != 2 || legacyRun.Method != "" || canonicalRun.Method != "" {
		t.Fatalf("run compatibility mismatch: calls=%d legacy=%+v canonical=%+v", runCalls, legacyRun, canonicalRun)
	}
	status := func(args []string) api.Request {
		var got api.Request
		a := newApp(fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
			got = request
			return responseOK(), nil
		}), &bytes.Buffer{}, &bytes.Buffer{})
		command := a.command()
		command.SetArgs(args)
		if err := command.ExecuteContext(context.Background()); err != nil {
			t.Fatal(err)
		}
		statusCalls++
		return got
	}
	legacyStatus, canonicalStatus := status([]string{"daemon", "status"}), status([]string{"factory", "status"})
	if statusCalls != 2 || legacyStatus.Method != canonicalStatus.Method || legacyStatus.Ticket != canonicalStatus.Ticket || !reflect.DeepEqual(legacyStatus.Parameters, canonicalStatus.Parameters) {
		t.Fatalf("status compatibility mismatch: legacy=%+v canonical=%+v", legacyStatus, canonicalStatus)
	}
	root := NewCommand(nil, &bytes.Buffer{}, &bytes.Buffer{})
	factory, _, err := root.Find([]string{"factory"})
	if err != nil || factory == nil {
		t.Fatal("factory command missing")
	}
	for _, child := range factory.Commands() {
		if child.Name() == "start" || child.Name() == "stop" {
			t.Fatalf("deferred factory lifecycle command advertised: %s", child.Name())
		}
	}
}
