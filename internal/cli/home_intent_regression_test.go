package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func homeIntentInventoryClient(t *testing.T, tickets []selectableTicket, mutations *int) Client {
	t.Helper()
	return fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		if request.Method != "ticket.status" {
			(*mutations)++
			t.Fatalf("unexpected mutation dispatch: %s", request.Method)
		}
		raw, _ := json.Marshal(map[string]any{"channel": "stable", "tickets": tickets})
		return api.Response{Version: api.Version, RequestID: request.RequestID, OK: true, Data: raw}, nil
	})
}

func TestHomeIntentRejectsForeignOrInconsistentInventory(t *testing.T) {
	tests := []struct {
		name    string
		tickets []selectableTicket
	}{
		{"foreign channel", []selectableTicket{{ID: "SF-one", Title: "Count", Project: "app", Channel: domain.ChannelDev, State: domain.StatePlanning}}},
		{"foreign project", []selectableTicket{{ID: "SF-one", Title: "Count", Project: "other", Channel: domain.ChannelStable, State: domain.StatePlanning}}},
		{"duplicate ID", []selectableTicket{{ID: "SF-one", Title: "One", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}, {ID: "SF-one", Title: "Two", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}}},
		{"invalid state", []selectableTicket{{ID: "SF-one", Title: "Count", Project: "app", Channel: domain.ChannelStable, State: domain.State("not-a-state")}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutations := 0
			a := newApp(homeIntentInventoryClient(t, tc.tickets, &mutations), &bytes.Buffer{}, &bytes.Buffer{})
			a.interactive = func() bool { return true }
			cmd := a.homeCommand()
			cmd.SetContext(context.Background())
			if err := a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "pause", Selector: "Count"}, "app", "claude-opus-4-6"); err != nil {
				t.Fatal(err)
			}
			if mutations != 0 {
				t.Fatalf("mutations=%d", mutations)
			}
		})
	}
}

func TestHomeIntentRejectsUnsupportedActionsBeforeInventory(t *testing.T) {
	calls := 0
	a := newApp(fakeClient(func(context.Context, api.Request) (api.Response, error) {
		calls++
		t.Fatal("unsupported home intent reached client")
		return api.Response{}, nil
	}), &bytes.Buffer{}, &bytes.Buffer{})
	a.interactive = func() bool { return true }
	cmd := a.homeCommand()
	cmd.SetContext(context.Background())
	for _, action := range []string{"approve", "shell", "restart"} {
		if err := a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: action, Selector: "SF-one"}, "app", "claude-opus-4-6"); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 0 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestHomeDoesNotInferWhenNonTTYOrJSON(t *testing.T) {
	for _, tc := range []struct {
		name string
		json bool
	}{
		{"non-tty", false},
		{"json", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			a := newApp(fakeClient(func(context.Context, api.Request) (api.Response, error) {
				calls++
				t.Fatal("noninteractive home contacted daemon")
				return api.Response{}, nil
			}), &bytes.Buffer{}, &bytes.Buffer{})
			a.json = tc.json
			a.interactive = func() bool { return false }
			cmd := a.homeCommand()
			cmd.SetContext(context.Background())
			_ = cmd.Execute()
			if calls != 0 {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}

func TestHomeIntentCancellationDoesNotMutate(t *testing.T) {
	mutations := 0
	var prompts bytes.Buffer
	a := newApp(homeIntentInventoryClient(t, []selectableTicket{{ID: "SF-one", Title: "Count", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}}, &mutations), &bytes.Buffer{}, &prompts)
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("cancel\n")
	cmd := a.homeCommand()
	cmd.SetContext(context.Background())
	if err := a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "pause", Selector: "Count"}, "app", "claude-opus-4-6"); err != nil {
		t.Fatal(err)
	}
	if mutations != 0 || !strings.Contains(prompts.String(), "Actual ticket: SF-one") {
		t.Fatalf("mutations=%d prompts=%q", mutations, prompts.String())
	}
}
