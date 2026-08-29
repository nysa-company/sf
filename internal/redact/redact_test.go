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

func TestRedactsQuotedMultilineAndProviderSessionSecrets(t *testing.T) {
	policy := NewPolicy("/Users/alice", map[string]string{"/Users/alice/project": "$PROJECT"})
	input := "TOKEN=\"quoted env secret\"\nSF_PROVIDER_SESSION='session secret'\nprovider-session: 'session secret 2'\nAuthorization:\n Bearer multiline-secret\n/Users/alice/project"
	output := policy.String(input)
	for _, secret := range []string{"quoted env secret", "session secret", "session secret 2", "multiline-secret"} {
		if strings.Contains(output, secret) {
			t.Fatalf("secret %q leaked in %q", secret, output)
		}
	}
	if !strings.Contains(output, "$PROJECT") {
		t.Fatalf("overlapping root was not replaced deterministically: %q", output)
	}
}

func TestJSONPreservesLargeNumbersWhileRedactingSecrets(t *testing.T) {
	input := []byte(`{"count":9007199254740993123456789,"provider_session":"keep-out"}`)
	output := JSON(input)
	if !strings.Contains(string(output), "9007199254740993123456789") || strings.Contains(string(output), "keep-out") {
		t.Fatalf("JSON redaction changed number or leaked secret: %s", output)
	}
}
