package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
)

func TestModelPickerRequiresCompleteIndependentSelection(t *testing.T) {
	for _, tc := range []struct{ preset, answer, builder, reviewer string }{
		{"claude-codex", "1\n1\n", "claude-sonnet-5", "gpt-5.6-luna"},
		{"codex-claude", "1\n1\n", "gpt-5.6-luna", "claude-sonnet-5"},
		{"codex-codex", "1\n1\n", "gpt-5.6-luna", "gpt-5.5"},
		{"codex-codex", "2\n1\n", "gpt-5.5", "gpt-5.6-luna"},
		{"cursor-claude", "1\n1\n", "gpt-5.6-luna-low", "claude-sonnet-5"},
		{"claude-cursor", "1\n1\n", "claude-sonnet-5", "gpt-5.6-luna-low"},
		{"cursor-codex", "1\n1\n", "gpt-5.6-luna-low", "gpt-5.5"},
		{"cursor-cursor", "1\n1\n", "gpt-5.6-luna-low", "claude-sonnet-5-low"},
		{"cursor-claude", "1\nq\n", "", ""},
		{"cursor-claude", "4\n", "", ""},
		{"claude-codex", "q\n", "", ""},
		{"claude-codex", "1\nq\n", "", ""},
		{"claude-codex", "1\n", "", ""},
		{"claude-codex", "1\ninvalid\n", "", ""},
	} {
		calls := 0
		client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
			calls++
			var values map[string]any
			if err := json.Unmarshal(request.Parameters, &values); err != nil {
				t.Fatal(err)
			}
			if values["builder_model"] != tc.builder || values["reviewer_model"] != tc.reviewer {
				t.Fatal("model selection changed")
			}
			return responseOK(), nil
		})
		var prompt bytes.Buffer
		a := newApp(client, &bytes.Buffer{}, &prompt)
		a.input, a.interactive = strings.NewReader(tc.answer), func() bool { return true }
		command := a.command()
		command.SetArgs([]string{"providers", "qualify", "--preset", tc.preset, "--models", "select"})
		err := command.ExecuteContext(context.Background())
		if tc.builder == "" {
			if err == nil || calls != 0 {
				t.Fatal("incomplete selection invoked qualifier")
			}
		} else if err != nil || calls != 1 {
			t.Fatalf("selection error=%v calls=%d", err, calls)
		}
		if !strings.Contains(prompt.String(), "may invoke paid models") {
			t.Fatal("missing paid-call disclosure")
		}
	}
}

func TestModelPickerRejectsScriptAndConflictingFlags(t *testing.T) {
	for _, flags := range [][]string{
		{"--models", "select", "--json"},
		{"--models", "select", "--builder-model", "claude-sonnet-5"},
		{"--models", "select", "--reviewer-model", "gpt-5.6-luna"},
		{"--models", "auto"}, {"--models", ""},
		{"--builder-model", "sonnet"}, {"--reviewer-model", "auto"},
		{"--builder-model", "gpt-5.6-luna"}, {"--reviewer-model", "claude-sonnet-5"},
	} {
		client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
			t.Fatal("invalid model selection invoked daemon")
			return api.Response{}, nil
		})
		args := append([]string{"providers", "qualify", "--preset", "claude-codex"}, flags...)
		if code := Execute(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}, client); code == 0 {
			t.Fatal("invalid flags accepted")
		}
	}
	a := newApp(nil, &bytes.Buffer{}, &bytes.Buffer{})
	a.interactive = func() bool { return false }
	if _, _, err := a.selectProviderModels(context.Background(), "claude", "codex"); err == nil {
		t.Fatal("nonterminal prompted")
	}
}

func TestModelPickerCatalogDoesNotClaimSameFamilyIndependence(t *testing.T) {
	if len(modelChoices("cursor", "")) != 10 {
		t.Fatal("Cursor exact catalog changed")
	}
	for _, family := range []string{"anthropic-claude", "openai-gpt-5.6", "xai-grok"} {
		for _, choice := range modelChoices("cursor", family) {
			if choice.family == family {
				t.Fatal("same family exposed across transport")
			}
		}
	}
	for _, model := range []string{"auto", "sonnet", "gpt-5.6-luna", "--force", "cursor-grok-4.6-low;true"} {
		if validateExplicitModel("cursor", model) == nil {
			t.Fatal("alias or malformed model accepted")
		}
	}
	if len(modelChoices("claude", "anthropic-claude")) != 0 {
		t.Fatal("same Claude family exposed")
	}
	choices := modelChoices("codex", "openai-gpt-5.6")
	if len(choices) != 1 || choices[0].model != "gpt-5.5" {
		t.Fatal("Codex family exclusion changed")
	}
}
