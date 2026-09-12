package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"strings"
	"testing"
)

func TestHomeIntentRequiresExactResolvedMutationConfirmation(t *testing.T) {
	for _, confirmation := range []string{"yes", "cancel SF-actual", "pause SF-actual"} {
		t.Run(confirmation, func(t *testing.T) {
			var output, prompts bytes.Buffer
			mutations := 0
			reads := 0
			a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
				if r.Method == "ticket.status" {
					reads++
					raw, _ := json.Marshal(map[string]any{"channel": "stable", "tickets": []selectableTicket{{ID: "SF-actual", Title: "Count jobs", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}}})
					return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
				}
				if r.Method != "ticket.pause" || r.Ticket != "SF-actual" {
					t.Fatalf("arbitrary dispatch %s %s", r.Method, r.Ticket)
				}
				mutations++
				return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true}, nil
			}), &output, &prompts)
			a.interactive = func() bool { return true }
			a.input = strings.NewReader(confirmation + "\n")
			cmd := a.homeCommand()
			cmd.SetContext(context.Background())
			if err := a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "pause", Selector: "Count jobs"}, "app", "claude-opus-4-6"); err != nil {
				t.Fatal(err)
			}
			want := 0
			if confirmation == "pause SF-actual" {
				want = 1
			}
			if mutations != want || reads < 1 {
				t.Fatalf("mutations=%d reads=%d", mutations, reads)
			}
			if !strings.Contains(prompts.String(), "Actual ticket: SF-actual") || !strings.Contains(prompts.String(), "Current state: planning") {
				t.Fatal("actual identity recap missing")
			}
		})
	}
}
func TestHomeIntentAmbiguousTitleUsesPickerAndNeverApproves(t *testing.T) {
	var prompts bytes.Buffer
	calls := 0
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls++
		if r.Method != "ticket.status" {
			t.Fatal("mutation after cancelled picker")
		}
		raw, _ := json.Marshal(map[string]any{"channel": "stable", "tickets": []selectableTicket{{ID: "SF-one", Title: "Count jobs", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}, {ID: "SF-two", Title: "Count jobs", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}}})
		return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
	}), &bytes.Buffer{}, &prompts)
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("q\n")
	cmd := a.homeCommand()
	cmd.SetContext(context.Background())
	_ = a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "cancel", Selector: "Count"}, "app", "claude-opus-4-6")
	if calls != 1 || !strings.Contains(prompts.String(), "Ticket number") {
		t.Fatal("ambiguity was guessed")
	}
	_ = a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "approve", Selector: "SF-one"}, "app", "claude-opus-4-6")
	if calls != 1 {
		t.Fatal("approval intent reached inventory/authority")
	}
}

func TestHomeIntentRevalidatesSelectionAfterConfirmation(t *testing.T) {
	reads := 0
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		if r.Method != "ticket.status" {
			t.Fatal("ticket mutated after disappearing")
		}
		reads++
		tickets := []selectableTicket{}
		if reads == 1 {
			tickets = append(tickets, selectableTicket{ID: "SF-actual", Title: "Count jobs", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning})
		}
		raw, _ := json.Marshal(map[string]any{"channel": "stable", "tickets": tickets})
		return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
	}), &bytes.Buffer{}, &bytes.Buffer{})
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("cancel SF-actual\n")
	cmd := a.homeCommand()
	cmd.SetContext(context.Background())
	_ = a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "cancel", Selector: "Count"}, "app", "claude-opus-4-6")
	if reads != 2 || a.last == nil || a.last.OK {
		t.Fatal("canonical handler skipped fresh revalidation")
	}
}

func TestHomeIntentRejectsProjectDrift(t *testing.T) {
	for _, drift := range []string{"project"} {
		t.Run(drift, func(t *testing.T) {
			reads := 0
			a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
				if r.Method != "ticket.status" {
					t.Fatal("mutation after recap drift")
				}
				reads++
				item := selectableTicket{ID: "SF-actual", Title: "Count jobs", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}
				if reads > 1 {
					item.Project = "other"
				}
				raw, _ := json.Marshal(map[string]any{"channel": "stable", "tickets": []selectableTicket{item}})
				return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
			}), &bytes.Buffer{}, &bytes.Buffer{})
			a.interactive = func() bool { return true }
			a.input = strings.NewReader("pause SF-actual\n")
			cmd := a.homeCommand()
			cmd.SetContext(context.Background())
			_ = a.dispatchHomeIntent(cmd, &contracts.AuthoringIntent{Action: "pause", Selector: "Count"}, "app", "claude-opus-4-6")
			if reads != 2 || a.last == nil || a.last.OK {
				t.Fatal("recap drift did not fail closed")
			}
		})
	}
}

func TestHomeIntentAPIConsentIsSeparateFromTicketMutation(t *testing.T) {
	var prompts bytes.Buffer
	calls := []string{}
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls = append(calls, r.Method)
		var values map[string]any
		_ = json.Unmarshal(r.Parameters, &values)
		var data any
		switch r.Method {
		case "authoring.create":
			if values["purpose"] != "home_intent" {
				t.Fatal("wrong purpose")
			}
			data = map[string]any{"authoring_session": authoringSessionView{ID: "intent-session", Channel: "stable", Project: "app", Purpose: "home_intent", ContextDigest: contracts.AuthoringDigest(nil), Capability: contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-opus-4-6"}}}}
		case "authoring.turn":
			if !strings.Contains(prompts.String(), "ticket new --status intent-session --turn "+values["key"].(string)) {
				t.Fatal("intent key not displayed before inference")
			}
			if !strings.Contains(prompts.String(), "Type send") {
				t.Fatal("intent inferred before consent")
			}
			data = map[string]any{"authoring_turn": authoringTurnView{Channel: "stable", Project: "app", Purpose: "home_intent", Session: "intent-session", Key: values["key"].(string), State: "completed", Outcome: "success", Result: &contracts.AuthoringResult{Kind: "home_intent", Intent: &contracts.AuthoringIntent{Action: "pause", Selector: "Count"}}}}
		case "ticket.status":
			data = map[string]any{"channel": "stable", "tickets": []selectableTicket{{ID: "SF-actual", Title: "Count jobs", Project: "app", Channel: domain.ChannelStable, State: domain.StatePlanning}}}
		default:
			t.Fatal("intent consent became mutation consent", r.Method)
		}
		raw, _ := json.Marshal(data)
		return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
	}), &bytes.Buffer{}, &prompts)
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("pause Count\nsend\nyes\n")
	cmd := a.command()
	cmd.SetArgs([]string{"home", "--project", "app", "--model", "claude-opus-4-6"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || a.last == nil || a.last.OK {
		t.Fatalf("calls=%v response=%+v", calls, a.last)
	}
}
