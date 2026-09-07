package processsupervisor

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestClaudeObserverMeasuresWithoutPrompt(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin observer")
	}
	dir := credentialHome(t)
	path := filepath.Join(dir, "claude")
	script := `#!/bin/sh
case "$*" in
  --version) echo '2.1.263 (Claude Code)' ;;
  --help) echo '--restricted --safe-mode --no-session-persistence --strict-mcp-config --json-schema' ;;
  '--safe-mode --restricted auth status')
    test -n "$CLAUDE_CODE_OAUTH_TOKEN" || exit 1
    test -z "$ANTHROPIC_API_KEY" || exit 2
    test "$PWD" = "$HOME" || exit 3
    echo '{"loggedIn":true,"authMethod":"oauth_token"}' ;;
  *) exit 4 ;;
esac
`
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ANTHROPIC_API_KEY", "must-not-inherit")
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lookup := func(context.Context, string, string) ([]byte, error) {
		return json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": "fixture-only", "expiresAt": time.Now().Add(time.Hour).UnixMilli()}})
	}
	bound, err := s.observeClaudeRuntime(context.Background(), path, "claude-sonnet-5", lookup)
	if err != nil || bound.Identity.Version != "2.1.263" || len(bound.AuthDigest) != 64 {
		t.Fatal("observer failed", err)
	}
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(script, "2.1.263", "2.1.264")), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := s.observeClaudeRuntime(context.Background(), path, "claude-sonnet-5", lookup); err == nil {
		t.Fatal("untested version accepted")
	}
}

func TestInstalledClaudeRuntimeObservation(t *testing.T) {
	if os.Getenv("SF_TEST_CLAUDE_OBSERVER") != "1" {
		t.Skip("explicit status-only native credential test")
	}
	path, err := exec.LookPath("claude")
	if err != nil {
		t.Fatal("CLI missing")
	}
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	bound, err := s.ObserveClaudeRuntime(context.Background(), path, "claude-sonnet-5")
	if err != nil || bound.Identity.Model != "claude-sonnet-5" {
		t.Fatal("installed observer failed", err)
	}
}

func TestInstalledClaudeObservationWithIsolatedDaemonHome(t *testing.T) {
	if os.Getenv("SF_TEST_CLAUDE_OBSERVER") != "1" {
		t.Skip("explicit status-only credential test")
	}
	path, err := exec.LookPath("claude")
	if err != nil {
		t.Fatal("CLI missing")
	}
	t.Setenv("HOME", credentialHome(t))
	if raw, err := lookupCLISecret(context.Background(), "Claude Code-credentials", ""); err != nil || len(raw) == 0 {
		t.Fatal("host Keychain lookup failed with isolated daemon HOME")
	}
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.ObserveClaudeRuntime(context.Background(), path, "claude-sonnet-5"); err != nil {
		t.Fatal("isolated daemon home observation failed", err)
	}
}

func TestObservedClaudeAuthRejectsAmbiguousOrAPIMode(t *testing.T) {
	for _, raw := range []string{`{}`, `{"loggedIn":false,"authMethod":"oauth_token"}`, `{"loggedIn":true,"authMethod":"api_key"}`, `{"loggedIn":false,"loggedIn":true,"authMethod":"oauth_token"}`, `{"loggedIn":true,"authMethod":"oauth_token","authMethod":"api_key"}`, `{"loggedIn":true,"authMethod":"oauth_token"} {}`, strings.Repeat("x", 16385)} {
		if validObservedClaudeAuth([]byte(raw)) {
			t.Fatal("invalid status admitted")
		}
	}
	if !validObservedClaudeAuth([]byte(`{"loggedIn":true,"authMethod":"oauth_token","future":true}`)) {
		t.Fatal("valid additive status rejected")
	}
}
