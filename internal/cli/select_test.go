package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

const selectedID = "SF-abcdef12345678901234567890123456"

func selectionInventory(items ...selectableTicket) api.Response {
	if items == nil {
		items = []selectableTicket{}
	}
	data, _ := json.Marshal(struct {
		Channel domain.Channel     `json:"channel"`
		Tickets []selectableTicket `json:"tickets"`
	}{domain.ChannelStable, items})
	response := responseOK()
	response.Data = data
	return response
}

func selectionItem(id, project string) selectableTicket {
	return selectableTicket{ID: id, Project: project, Channel: domain.ChannelStable, State: domain.StateQueued, Title: "Same title"}
}

func TestTicketSelectionOnlyDispatchesChosenFullIdentity(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		tty     bool
		input   string
		want    string
		project string
	}{
		{"prefix", []string{"start", "abcdef1"}, false, "", selectedID, ""},
		{"prefixed", []string{"start", "SF-abcdef1"}, false, "", selectedID, ""},
		{"picker", []string{"start"}, true, "2\n", "SF-fedcba12345678901234567890123456", ""},
		{"scope", []string{"start", "abcdef1", "--project", "app"}, false, "", selectedID, "app"},
		{"status picker", []string{"status", "--select"}, true, "1\n", selectedID, ""},
		{"nested prefix", []string{"ticket", "start", "abcdef1"}, false, "", selectedID, ""},
		{"nested picker", []string{"ticket", "start"}, true, "2\n", "SF-fedcba12345678901234567890123456", ""},
		{"nested scope", []string{"ticket", "start", "abcdef1", "--project", "app"}, false, "", selectedID, "app"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output, prompt bytes.Buffer
			var requests []api.Request
			client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
				requests = append(requests, request)
				if len(requests) == 1 {
					if request.Method != "ticket.status" || request.Ticket != "" {
						t.Fatalf("not read-only: %+v", request)
					}
					var values map[string]any
					_ = json.Unmarshal(request.Parameters, &values)
					if values["project"] != test.project {
						t.Fatalf("scope: %s", request.Parameters)
					}
					return selectionInventory(selectionItem(selectedID, "app"), selectionItem("SF-fedcba12345678901234567890123456", "app")), nil
				}
				if request.Ticket != test.want {
					t.Fatalf("wrong target: %+v", request)
				}
				return responseOK(), nil
			})
			a := newApp(client, &output, &prompt)
			a.input = strings.NewReader(test.input)
			a.interactive = func() bool { return test.tty }
			command := a.command()
			command.SetArgs(test.args)
			if err := command.ExecuteContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(requests) != 2 || a.last == nil || !a.last.OK {
				t.Fatalf("requests %d output %s", len(requests), output.String())
			}
			if test.tty && !strings.Contains(prompt.String(), "Same title") {
				t.Fatalf("missing title: %s", prompt.String())
			}
		})
	}
}

func TestTicketSelectionRefusesAmbiguityAndInvalidInventories(t *testing.T) {
	wrongChannel := selectionItem(selectedID, "app")
	wrongChannel.Channel = domain.ChannelDev
	badID := selectionItem("SF-abcdef\x1b[2J", "app")
	full := make([]selectableTicket, 1000)
	for _, test := range []struct {
		name, input string
		tty, json   bool
		args        []string
		items       []selectableTicket
	}{
		{"ambiguous", "", false, false, []string{"start", "abcdef"}, []selectableTicket{selectionItem(selectedID, "app"), selectionItem("SF-abcdef99945678901234567890123456", "other")}},
		{"cancel", "q\n", true, false, []string{"start"}, []selectableTicket{selectionItem(selectedID, "app")}},
		{"EOF", "", true, false, []string{"start"}, []selectableTicket{selectionItem(selectedID, "app")}},
		{"invalid number", "99\n", true, false, []string{"start"}, []selectableTicket{selectionItem(selectedID, "app")}},
		{"empty", "", false, false, []string{"start", "abcdef"}, nil},
		{"channel", "", false, false, []string{"start", "abcdef"}, []selectableTicket{wrongChannel}},
		{"project", "", false, false, []string{"start", "abcdef", "--project", "wrong"}, []selectableTicket{selectionItem(selectedID, "app")}},
		{"duplicate", "", false, false, []string{"start", "abcdef"}, []selectableTicket{selectionItem(selectedID, "app"), selectionItem(selectedID, "app")}},
		{"control ID", "", false, false, []string{"start", "abcdef"}, []selectableTicket{badID}},
		{"incomplete", "", false, false, []string{"start", "abcdef"}, full},
		{"json ambiguity", "1\n", true, true, []string{"start", "abcdef", "--json"}, []selectableTicket{selectionItem(selectedID, "app"), selectionItem("SF-abcdef99945678901234567890123456", "app")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output, prompt bytes.Buffer
			calls := 0
			client := fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
				calls++
				if r.Method != "ticket.status" || r.Ticket != "" {
					t.Fatalf("mutation: %+v", r)
				}
				return selectionInventory(test.items...), nil
			})
			a := newApp(client, &output, &prompt)
			a.input = strings.NewReader(test.input)
			a.interactive = func() bool { return test.tty }
			cmd := a.command()
			cmd.SetArgs(test.args)
			if err := cmd.ExecuteContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls != 1 || a.last == nil || a.last.OK || a.last.Mutation.Attempted {
				t.Fatalf("calls %d: %s", calls, output.String())
			}
			if test.json && prompt.Len() != 0 {
				t.Fatalf("JSON prompted: %s", prompt.String())
			}
		})
	}
}

func TestTicketSelectionDoesNotOverrideDaemonStateRefusal(t *testing.T) {
	var output bytes.Buffer
	calls := 0
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls++
		if calls == 1 {
			return selectionInventory(selectionItem(selectedID, "app")), nil
		}
		return failure("invalid_transition", "ticket changed while selection was open", []string{binaryName(), "status", r.Ticket}), nil
	}), &output, &bytes.Buffer{})
	a.input = strings.NewReader("1\n")
	a.interactive = func() bool { return true }
	cmd := a.command()
	cmd.SetArgs([]string{"start"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || a.last.OK || a.last.Error.Code != "invalid_transition" {
		t.Fatalf("%s", output.String())
	}
}

func TestProjectStatusWatchKeepsScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls++
		var p map[string]any
		_ = json.Unmarshal(r.Parameters, &p)
		if p["project"] != "app" {
			t.Fatalf("%s", r.Parameters)
		}
		cancel()
		return selectionInventory(), nil
	}), &bytes.Buffer{}, &bytes.Buffer{})
	a.interactive = func() bool { return false }
	cmd := a.command()
	cmd.SetArgs([]string{"status", "--project", "app", "--watch"})
	if err := cmd.ExecuteContext(ctx); err != nil || calls != 1 {
		t.Fatalf("%v calls %d", err, calls)
	}
}

func TestSelectionLabelsCannotInjectTerminalControl(t *testing.T) {
	label := safeSelectionLabel("hello\x1b[2J\n\u202e" + strings.Repeat("x", 200))
	if strings.ContainsAny(label, "\x1b\n\u202e") || len([]rune(label)) > 160 {
		t.Fatalf("unsafe %q", label)
	}
}
