package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/spf13/cobra"
)

func TestEveryPublicCommandExplainsItsPurpose(t *testing.T) {
	root := NewCommand(nil, &bytes.Buffer{}, &bytes.Buffer{})
	var visit func(*cobra.Command)
	visit = func(parent *cobra.Command) {
		for _, command := range parent.Commands() {
			if strings.TrimSpace(command.Short) == "" {
				t.Errorf("missing description: %s", command.CommandPath())
			}
			visit(command)
		}
	}
	visit(root)
}

func TestInputErrorsPointToSpecificCommandHelpWithoutCallingDaemon(t *testing.T) {
	for _, test := range []struct {
		args []string
		path string
	}{
		{[]string{"submit"}, "submit"},
		{[]string{"start"}, "start"},
		{[]string{"init"}, "init"},
		{[]string{"providers", "qualify"}, "providers qualify"},
		{[]string{"status", "--invalid-option"}, "status"},
	} {
		t.Run(test.path+strings.Join(test.args, "_"), func(t *testing.T) {
			var output, errorOutput bytes.Buffer
			client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
				t.Fatal("input error contacted daemon")
				return api.Response{}, nil
			})
			args := append(append([]string{}, test.args...), "--json")
			if code := Execute(context.Background(), args, &output, &errorOutput, client); code != int(ExitInput) {
				t.Fatalf("exit %d: %s", code, errorOutput.String())
			}
			var response api.Response
			if err := json.Unmarshal(errorOutput.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.Mutation.Attempted || response.NextAction == nil || strings.Join(response.NextAction.Argv, " ") != binaryName()+" "+test.path+" --help" {
				t.Fatalf("response: %+v", response)
			}
		})
	}
}

func TestHelpExamplesUseSelectedChannel(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		root := (&app{out: &bytes.Buffer{}, errOut: &bytes.Buffer{}, channel: channel}).command()
		command, _, err := root.Find([]string{"submit"})
		if err != nil || !strings.Contains(command.Example, root.Name()+" submit ticket.md --project my-app") {
			t.Fatalf("channel %s: %v", channel, err)
		}
	}
}

func TestTicketsListsTitlesThroughReadOnlyProjectScopedRequest(t *testing.T) {
	var output bytes.Buffer
	calls := 0
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		var parameters struct {
			Project string `json:"project"`
			Watch   bool   `json:"watch"`
		}
		if err := json.Unmarshal(request.Parameters, &parameters); err != nil {
			t.Fatal(err)
		}
		if request.Method != "ticket.status" || request.Ticket != "" || parameters.Project != "my-app" || parameters.Watch {
			t.Fatalf("unexpected request: %+v", request)
		}
		response := responseOK()
		response.Data = json.RawMessage(`{"channel":"stable","tickets":[{"ticket":"SF-example","title":"Count items","state":"queued"}]}`)
		return response, nil
	})
	if code := Execute(context.Background(), []string{"tickets", "--project", "my-app"}, &output, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit %d: %s", code, output.String())
	}
	if calls != 1 || !strings.Contains(output.String(), "Count items") || !strings.Contains(output.String(), "SF-example") {
		t.Fatalf("calls %d: %s", calls, output.String())
	}
}
