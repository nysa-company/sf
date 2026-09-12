package daemon

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
)

func TestArtifactSanitizationRedactsBeforeTruncationAndNestedActions(t *testing.T) {
	policy := redact.NewPolicy("/private/operator", nil)
	text, truncated := safeArtifactText("password=secret-value\n\x1b[2J\u202e"+strings.Repeat("x", 100), policy, 40)
	if !truncated || strings.Contains(text, "secret-value") || strings.ContainsAny(text, "\x1b\u202e") || len([]rune(text)) != 40 {
		t.Fatalf("unsafe text=%q truncated=%t", text, truncated)
	}
	action := domain.NextAction{Code: "inspect", Argv: []string{"sf", "show", "SF-1"}}
	value := map[string]any{"next_action": action, "artifacts": map[string]any{"next_action": map[string]any{"argv": []string{"password=hidden-secret", "\x1b\u202e"}}}}
	got := sanitizeArtifactObject(value, policy)
	if !reflect.DeepEqual(got["next_action"], action) {
		t.Fatal("typed daemon action changed")
	}
	data, _ := json.Marshal(got)
	if strings.Contains(string(data), "hidden-secret") || strings.Contains(string(data), `\u001b`) || strings.Contains(string(data), "\u202e") {
		t.Fatalf("nested next_action escaped sanitization: %s", data)
	}
}

func TestCanonicalArtifactReadIsOptInReadOnlyAndExplicitlyUnavailable(t *testing.T) {
	d, paths, _ := testDaemon(t)
	ctx := context.Background()
	path := writeTicket(t, t.TempDir(), "Artifact view")
	if code, output, _ := executeCLI(t, ctx, paths, "submit", path, "--project", "demo", "--json"); code != 0 {
		t.Fatalf("submit=%s", output)
	}
	before, err := d.store.TicketByID(ctx, domain.ChannelStable, "SF-test-1")
	if err != nil {
		t.Fatal(err)
	}
	read := func(args ...string) map[string]any {
		t.Helper()
		code, output, stderr := executeCLI(t, ctx, paths, args...)
		var response api.Response
		var data map[string]any
		if code != 0 || json.Unmarshal([]byte(output), &response) != nil || json.Unmarshal(response.Data, &data) != nil {
			t.Fatalf("read=%d %s %s", code, output, stderr)
		}
		return data
	}
	legacy := read("show", "SF-test-1", "--json")
	if legacy["artifacts"] != nil || legacy["source"] == nil {
		t.Fatal("legacy show shape changed")
	}
	view := read("ticket", "view", "SF-test-1", "--json")
	artifacts, ok := view["artifacts"].(map[string]any)
	if !ok || len(artifacts) != 6 || view["source"] != nil {
		t.Fatalf("canonical shape=%v", view)
	}
	for _, name := range []string{"plan", "proof", "changes", "pr"} {
		if artifacts[name].(map[string]any)["available"] != false {
			t.Fatalf("invented %s artifact: %v", name, artifacts[name])
		}
	}
	plan := read("ticket", "view", "SF-test-1", "--section", "plan", "--json")
	if len(plan["artifacts"].(map[string]any)) != 1 {
		t.Fatal("section selected unrelated source content")
	}
	after, err := d.store.TicketByID(ctx, domain.ChannelStable, "SF-test-1")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("artifact inspection changed durable ticket")
	}
}
