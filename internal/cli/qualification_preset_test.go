package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
)

func TestQualificationPresetDispatchesExactlySelectedPair(t *testing.T) {
	for _, tc := range []struct{ preset, builder, reviewer string }{
		{"claude-codex", "claude", "codex"}, {"codex-claude", "codex", "claude"}, {"codex-codex", "codex", "codex"},
	} {
		calls := 0
		client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
			calls++
			var values map[string]any
			if err := json.Unmarshal(request.Parameters, &values); err != nil {
				t.Fatal(err)
			}
			if request.Method != "provider.qualify" || values["builder"] != tc.builder || values["reviewer"] != tc.reviewer {
				t.Fatal("wrong qualification pair")
			}
			return responseOK(), nil
		})
		var out bytes.Buffer
		if code := Execute(context.Background(), []string{"providers", "qualify", "--preset", tc.preset, "--json"}, &out, &bytes.Buffer{}, client); code != 0 || calls != 1 {
			t.Fatalf("preset=%s exit=%d calls=%d", tc.preset, code, calls)
		}
	}
}

func TestQualificationPresetPreservesExplicitModels(t *testing.T) {
	calls := 0
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		var values map[string]any
		if err := json.Unmarshal(request.Parameters, &values); err != nil {
			t.Fatal(err)
		}
		if values["builder_model"] != "claude-sonnet-5" || values["reviewer_model"] != "gpt-5.6-luna" {
			t.Fatal("model selection lost")
		}
		return responseOK(), nil
	})
	args := []string{"providers", "qualify", "--preset", "claude-codex", "--builder-model", "claude-sonnet-5", "--reviewer-model", "gpt-5.6-luna", "--json"}
	if code := Execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, client); code != 0 || calls != 1 {
		t.Fatal("explicit model dispatch failed")
	}
}

func TestQualificationPresetInvalidSelectionNeverCallsDaemon(t *testing.T) {
	for _, flags := range [][]string{
		{}, {"--builder", "claude"}, {"--preset", ""}, {"--preset", "unknown-codex"},
		{"--preset", "claude-codex", "--builder", "claude"},
		{"--preset", "claude-codex", "--reviewer", "codex"}, {"--preset", "select", "--json"},
	} {
		client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
			t.Fatal("invalid selection invoked daemon")
			return api.Response{}, nil
		})
		args := append([]string{"providers", "qualify"}, flags...)
		if code := Execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, client); code == 0 {
			t.Fatalf("invalid flags accepted: %v", flags)
		}
	}
}

func TestQualificationInteractivePresetAndCancellation(t *testing.T) {
	for _, answer := range []string{"2\n", "q\n", "", "invalid\n"} {
		calls := 0
		client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
			calls++
			var values map[string]any
			if err := json.Unmarshal(request.Parameters, &values); err != nil {
				t.Fatal(err)
			}
			if values["builder"] != "claude" || values["reviewer"] != "codex" {
				t.Fatal("picker changed pair")
			}
			return responseOK(), nil
		})
		a := newApp(client, &bytes.Buffer{}, &bytes.Buffer{})
		a.input, a.interactive = strings.NewReader(answer), func() bool { return true }
		command := a.command()
		command.SetArgs([]string{"providers", "qualify", "--preset", "select"})
		_ = command.ExecuteContext(context.Background())
		want := 0
		if answer == "2\n" {
			want = 1
		}
		if calls != want {
			t.Fatalf("answer=%q calls=%d want=%d", answer, calls, want)
		}
	}
}
