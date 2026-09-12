package processsupervisor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestAuthoringObserverRequiresPinnedBoundedStatus(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin observer")
	}
	flags := []string{"--restricted", "--safe-mode", "--no-session-persistence", "--strict-mcp-config", "--json-schema", "--bare", "--tools", "--permission-mode", "--allowedTools", "--disallowedTools"}
	const script = `#!/bin/sh
case "$*" in
  --version) echo 'VERSION (Claude Code)' ;;
  --help) echo 'FLAGS' ;;
  '--max-turns 3 --safe-mode --restricted auth status')
    test -n "$CLAUDE_CODE_OAUTH_TOKEN" || exit 1
    test -z "$ANTHROPIC_API_KEY" || exit 2
    test "$PWD" = "$HOME" || exit 3
    STATUS ;;
  *) exit 4 ;;
esac
`
	tests := []struct {
		name, version, help, status string
		wantOK                      bool
	}{
		{"hidden bound authenticated", "2.1.263", strings.Join(flags, " "), `echo '{"loggedIn":true,"authMethod":"oauth_token"}'`, true},
		{"bound parser rejection", "2.1.263", strings.Join(flags, " "), "exit 5", false},
		{"unauthenticated", "2.1.263", strings.Join(flags, " "), `echo '{"loggedIn":false,"authMethod":"oauth_token"}'`, false},
		{"ambiguous status", "2.1.263", strings.Join(flags, " "), `echo '{"loggedIn":false,"loggedIn":true,"authMethod":"oauth_token"}'`, false},
		{"unsupported version", "2.1.264", strings.Join(flags, " "), `echo '{"loggedIn":true,"authMethod":"oauth_token"}'`, false},
	}
	for _, flag := range flags {
		tests = append(tests, struct {
			name, version, help, status string
			wantOK                      bool
		}{"missing " + flag, "2.1.263", strings.ReplaceAll(strings.Join(flags, " "), flag, ""), `echo '{"loggedIn":true,"authMethod":"oauth_token"}'`, false})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := credentialHome(t)
			path := filepath.Join(dir, "claude")
			body := strings.NewReplacer("VERSION", test.version, "FLAGS", test.help, "STATUS", test.status).Replace(script)
			if err := os.WriteFile(path, []byte(body), 0700); err != nil {
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
			binding, err := s.observeClaudeOperation(context.Background(), path, "claude-sonnet-4-6", lookup, true)
			if (err == nil) != test.wantOK {
				t.Fatal("unexpected authoring observation outcome")
			}
			if !test.wantOK {
				wantStage := "authstatus"
				if test.name == "unsupported version" {
					wantStage = "version"
				}
				if strings.HasPrefix(test.name, "missing ") {
					wantStage = "help"
				}
				if AuthoringPreparationCategory(err) != wantStage {
					t.Fatal("incorrect first failing preparation stage")
				}
			}
			if test.wantOK && (binding.Identity.Version != "2.1.263" || len(binding.AuthDigest) != 64) {
				t.Fatal("missing authenticated pinned binding")
			}
		})
	}
}
