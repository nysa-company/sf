package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"strings"
	"testing"
)

func TestAuthoringStatusScriptReadsExactDraftOrIntentWithoutDispatch(t *testing.T) {
	for _, purpose := range []string{"ticket_draft", "home_intent"} {
		for _, jsonMode := range []bool{false, true} {
			t.Run(purpose, func(t *testing.T) {
				calls := 0
				var output bytes.Buffer
				result := contracts.AuthoringResult{Kind: "draft", Title: "Count", Problem: "Count jobs", Acceptance: []string{"Empty returns zero"}}
				if purpose == "home_intent" {
					result = contracts.AuthoringResult{Kind: "home_intent", Intent: &contracts.AuthoringIntent{Action: "cancel", Selector: "SF-actual"}}
				}
				a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
					calls++
					if r.Method != "authoring.status" {
						t.Fatal("recovery inferred or mutated")
					}
					raw, _ := json.Marshal(map[string]any{"authoring_turn": authoringTurnView{Channel: "stable", Project: "app", Purpose: purpose, Session: "session", Key: "turn", State: "completed", Outcome: "success", Result: &result}})
					return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
				}), &output, &bytes.Buffer{})
				a.interactive = func() bool { return false }
				args := []string{"ticket", "new", "--status", "session", "--turn", "turn"}
				if jsonMode {
					args = append(args, "--json")
				}
				cmd := a.command()
				cmd.SetArgs(args)
				if err := cmd.ExecuteContext(context.Background()); err != nil {
					t.Fatal(err)
				}
				if calls != 1 || a.last == nil || !a.last.OK {
					t.Fatalf("calls=%d response=%+v", calls, a.last)
				}
				if !jsonMode && !strings.Contains(output.String(), "nothing saved, submitted, dispatched or resent") {
					t.Fatal("read-only semantics missing")
				}
			})
		}
	}
}
func TestAuthoringStatusRejectsIdentityMismatchAndCreationFlags(t *testing.T) {
	for _, args := range [][]string{{"ticket", "new", "--status", "session", "--turn", "turn"}, {"ticket", "new", "--status", "session", "--turn", "turn", "--model", "model"}} {
		calls := 0
		a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
			calls++
			raw, _ := json.Marshal(map[string]any{"authoring_turn": authoringTurnView{Channel: "stable", Project: "app", Purpose: "ticket_draft", Session: "other", Key: "turn", State: "reserved"}})
			return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
		}), &bytes.Buffer{}, &bytes.Buffer{})
		a.interactive = func() bool { return false }
		cmd := a.command()
		cmd.SetArgs(args)
		_ = cmd.ExecuteContext(context.Background())
		if a.last == nil || a.last.OK {
			t.Fatal("unsafe status accepted")
		}
		if len(args) > 6 && calls != 0 {
			t.Fatal("creation flags reached daemon")
		}
	}
}
func TestAuthoringRecoveryActionRetainsKnownSessionAndTurn(t *testing.T) {
	a := newApp(nil, &bytes.Buffer{}, &bytes.Buffer{})
	response := a.authoringRecoveryResponse(failure("authoring_unavailable", "unknown", nil), "session", "turn")
	if got := strings.Join(response.NextAction.Argv, " "); !strings.Contains(got, "ticket new --status session --turn turn") {
		t.Fatal(got)
	}
	if !response.Mutation.Attempted || response.Mutation.Identity != "session/turn" {
		t.Fatal("unknown turn identity lost")
	}
}

type rejectAuthoringRecoveryWriter struct{}

func (rejectAuthoringRecoveryWriter) Write(value []byte) (int, error) {
	if strings.Contains(string(value), "Turn recovery") {
		return 0, errors.New("display unavailable")
	}
	return len(value), nil
}

func TestAuthoringRecoveryDisplayFailurePreventsInference(t *testing.T) {
	calls := 0
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls++
		if r.Method != "authoring.create" {
			t.Fatal("turn sent without durable user-visible key")
		}
		raw, _ := json.Marshal(map[string]any{"authoring_session": authoringSessionView{Channel: "stable", Project: "app", Purpose: "home_intent", ID: "session", ContextDigest: contracts.AuthoringDigest(nil), Capability: contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "model"}}}})
		return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
	}), &bytes.Buffer{}, rejectAuthoringRecoveryWriter{})
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("list tickets\nsend\n")
	cmd := a.command()
	cmd.SetArgs([]string{"home", "--project", "app", "--model", "model"})
	_ = cmd.ExecuteContext(context.Background())
	if calls != 1 || a.last == nil || a.last.OK {
		t.Fatal("display failure did not prevent inference")
	}
}

func TestAuthoringStatusRedactsCustomProviderResultBeforeHumanDisplay(t *testing.T) {
	var output bytes.Buffer
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		result := contracts.AuthoringResult{Kind: "question", Question: "token: hidden-authoring-value"}
		raw, _ := json.Marshal(map[string]any{"authoring_turn": authoringTurnView{Channel: "stable", Project: "app", Purpose: "ticket_draft", Session: "session", Key: "turn", State: "completed", Outcome: "success", Result: &result}})
		return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
	}), &output, &bytes.Buffer{})
	a.interactive = func() bool { return false }
	cmd := a.command()
	cmd.SetArgs([]string{"ticket", "new", "--status", "session", "--turn", "turn"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "hidden-authoring-value") || !strings.Contains(output.String(), "[REDACTED]") {
		t.Fatal("custom provider result leaked", output.String())
	}
}
