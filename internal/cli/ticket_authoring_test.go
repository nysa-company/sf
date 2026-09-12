package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAuthoringDefaultScriptsNeverCreateSession(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		a := newApp(fakeClient(func(context.Context, api.Request) (api.Response, error) {
			t.Fatal("script contacted authoring")
			return api.Response{}, nil
		}), &bytes.Buffer{}, &bytes.Buffer{})
		a.interactive = func() bool { return false }
		args := []string{"ticket", "new"}
		if jsonMode {
			args = append(args, "--json")
		}
		cmd := a.command()
		cmd.SetArgs(args)
		_ = cmd.ExecuteContext(context.Background())
		if a.last == nil || a.last.OK {
			t.Fatal("script did not refuse")
		}
	}
}
func TestAuthoringConsentPreviewSaveNeverImplicitlyStarts(t *testing.T) {
	result := contracts.AuthoringResult{Kind: "draft", Title: "Count jobs", Problem: "Return count; token: hidden-authoring-value", Scope: []string{}, Acceptance: []string{"Empty returns zero"}, Assumptions: []string{}}
	clean, err := authoring.SanitizePurpose("ticket_draft", result, redact.NewPolicy("", nil))
	if err != nil {
		t.Fatal(err)
	}
	source, err := authoring.Markdown(clean)
	if err != nil {
		t.Fatal(err)
	}
	var prompts, output bytes.Buffer
	calls := []string{}
	a := newApp(fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls = append(calls, r.Method)
		var values map[string]any
		_ = json.Unmarshal(r.Parameters, &values)
		var data any
		switch r.Method {
		case "authoring.create":
			if strings.Contains(prompts.String(), "Send this authoring turn") {
				t.Fatal("session not established first")
			}
			data = map[string]any{"authoring_session": authoringSessionView{ID: "draft-session", Channel: "stable", Project: "nysa", Purpose: "ticket_draft", Capability: contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "opus"}}, ContextDigest: contracts.AuthoringDigest(nil)}}
		case "authoring.turn":
			if !strings.Contains(prompts.String(), "ticket new --status draft-session --turn "+values["key"].(string)) {
				t.Fatal("turn key not displayed before inference")
			}
			if !strings.Contains(prompts.String(), "Send this authoring turn") {
				t.Fatal("inference without displayed consent")
			}
			data = map[string]any{"authoring_turn": authoringTurnView{Channel: "stable", Project: "nysa", Purpose: "ticket_draft", Session: "draft-session", Key: values["key"].(string), State: "completed", Outcome: "success", Result: &result}}
		default:
			t.Fatalf("unexpected lifecycle method %s", r.Method)
		}
		raw, _ := json.Marshal(data)
		return api.Response{Version: api.Version, RequestID: r.RequestID, OK: true, Data: raw}, nil
	}), &output, &prompts)
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("Count jobs\nsend\nsave\nleave\n")
	path := filepath.Join(t.TempDir(), "draft.md")
	cmd := a.command()
	cmd.SetArgs([]string{"ticket", "new", path, "--project", "nysa", "--model", "opus"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != source {
		t.Fatalf("reviewed bytes differed: %v", err)
	}
	if len(calls) != 2 || !strings.Contains(prompts.String(), source) {
		t.Fatalf("calls=%v preview missing", calls)
	}
	if strings.Contains(prompts.String(), "hidden-authoring-value") || strings.Contains(string(data), "hidden-authoring-value") {
		t.Fatal("provider secret reached preview or saved draft")
	}
}
