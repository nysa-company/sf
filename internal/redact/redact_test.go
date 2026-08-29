package redact

import (
	"strings"
	"testing"
)

func TestRedactsSecretFormsAndKnownPaths(t *testing.T) {
	policy := NewPolicy("/Users/alice", map[string]string{"/tmp/worktree": "$WORKTREE"})
	input := `token=abc password:xyz Authorization: Bearer abc https://user:pass@example.test/x?a=1&access_token=abc /Users/alice/project /tmp/worktree ordinary`
	output := policy.String(input)
	for _, secret := range []string{"abc", "xyz", "user:pass", "/Users/alice", "/tmp/worktree"} {
		if strings.Contains(output, secret) {
			t.Fatalf("%q leaked in %q", secret, output)
		}
	}
	if !strings.Contains(output, "$HOME") || !strings.Contains(output, "$WORKTREE") {
		t.Fatalf("known path labels missing: %q", output)
	}
}

func TestJSONRedactsSecretKeysWithoutChangingShape(t *testing.T) {
	output := JSON([]byte(`{"message":"keep","credentials":{"api_key":"abc"},"items":[{"secret":"xyz"}]}`))
	text := string(output)
	if strings.Contains(text, "abc") || strings.Contains(text, "xyz") || !strings.Contains(text, `"message":"keep"`) {
		t.Fatalf("redacted JSON=%s", text)
	}
}
