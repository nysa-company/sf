package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestProviderPresetPickerHasNoAuthoritySideEffects(t *testing.T) {
	for _, tc := range []struct{ answer, want string }{{"1\n", "codex-codex"}, {"2\n", "claude-codex"}, {"3\n", "codex-claude"}, {"4\n", "cursor-codex"}, {"5\n", "codex-cursor"}, {"6\n", "cursor-claude"}, {"7\n", "claude-cursor"}, {"8\n", "cursor-cursor"}, {"q\n", ""}, {"\n", ""}, {"9\n", ""}, {"", ""}, {strings.Repeat("x", 2000), ""}} {
		var out, prompt bytes.Buffer
		a := newApp(nil, &out, &prompt)
		a.input = strings.NewReader(tc.answer)
		a.interactive = func() bool { return true }
		got, err := a.selectInitialProviderPreset(context.Background())
		if got != tc.want || (err != nil) != (tc.want == "") {
			t.Fatalf("selection=%q err=%v want=%q", got, err, tc.want)
		}
		if a.last != nil || out.Len() != 0 {
			t.Fatal("picker issued an action/JSON response")
		}
		if !strings.Contains(prompt.String(), "Qualification") || !strings.Contains(prompt.String(), "estimated-cost") {
			t.Fatal("missing authority/billing disclosure")
		}
	}
}

func TestProviderPresetPickerRefusesJSONAndNonTerminal(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		var out, prompt bytes.Buffer
		a := newApp(nil, &out, &prompt)
		a.json = jsonMode
		a.interactive = func() bool { return jsonMode }
		a.input = strings.NewReader("2\n")
		if _, err := a.selectInitialProviderPreset(context.Background()); err == nil || prompt.Len() != 0 {
			t.Fatal("noninteractive selection was prompted")
		}
	}
}
