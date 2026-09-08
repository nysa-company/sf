package processsupervisor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/cursorprovider"
)

func credentialHome(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestCLICredentialsAreScopedAndDoNotCopySettings(t *testing.T) {
	for _, provider := range []string{"claude", "cursor"} {
		t.Run(provider, func(t *testing.T) {
			home := credentialHome(t)
			lookup := func(_ context.Context, service, account string) ([]byte, error) {
				if provider == "claude" {
					if service != "Claude Code-credentials" || account != "" {
						t.Fatal("wrong service")
					}
					return json.Marshal(map[string]any{"claudeAiOauth": map[string]any{"accessToken": "fixture-only-token", "expiresAt": time.Now().Add(time.Hour).UnixMilli()}, "unrelated": "must-not-copy"})
				}
				if account != "cursor-user" || service != "cursor-access-token" && service != "cursor-refresh-token" {
					t.Fatal("wrong service")
				}
				return []byte("fixture-only-token"), nil
			}
			env, digest, err := prepareCLICredentials(context.Background(), provider, home, lookup)
			if err != nil || len(digest) != 64 || len(env) == 0 {
				t.Fatal("credential preparation failed")
			}
			if strings.Contains(strings.Join(env, " "), "must-not-copy") {
				t.Fatal("unrelated credentials copied")
			}
			if provider == "cursor" {
				path := filepath.Join(home, ".cursor", "auth.json")
				info, err := os.Stat(path)
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("auth file not private")
				}
				var data map[string]string
				raw, err := os.ReadFile(path)
				if err != nil || json.Unmarshal(raw, &data) != nil || len(data) != 2 || data["accessToken"] == "" || data["refreshToken"] == "" {
					t.Fatal("wrong credential shape")
				}
				if _, err := os.Stat(filepath.Join(home, ".cursor", "cli-config.json")); !os.IsNotExist(err) {
					t.Fatal("user settings copied")
				}
			}
		})
	}
}

func TestCLICredentialsRejectUnsafeSources(t *testing.T) {
	for _, provider := range []string{"claude", "cursor"} {
		for _, raw := range []string{"", "bad\nsecret", strings.Repeat("x", 17<<10), `{"claudeAiOauth":{"accessToken":"expired","expiresAt":1}}`} {
			lookup := func(context.Context, string, string) ([]byte, error) {
				if provider == "cursor" && strings.HasPrefix(raw, "{") {
					return nil, errors.New("fixture failure")
				}
				return []byte(raw), nil
			}
			if _, _, err := prepareCLICredentials(context.Background(), provider, credentialHome(t), lookup); err == nil {
				t.Fatal("unsafe source accepted")
			}
		}
	}
	home := credentialHome(t)
	if err := os.Chmod(home, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := prepareCLICredentials(context.Background(), "cursor", home, func(context.Context, string, string) ([]byte, error) {
		t.Fatal("read before home validation")
		return nil, nil
	}); err == nil {
		t.Fatal("nonprivate home accepted")
	}
}

func TestCLIEnvironmentRejectsCredentialReplacement(t *testing.T) {
	lookup := func(context.Context, string, string) ([]byte, error) { return []byte("fixture-token"), nil }
	_, digest, err := prepareCLICredentials(context.Background(), "cursor", credentialHome(t), lookup)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CURSOR_API_KEY", "must-not-inherit")
	t.Setenv("NODE_OPTIONS", "must-not-inherit")
	env, _, cleanup, err := vettedCLIEnvironment(context.Background(), "cursor", digest, lookup)
	if err != nil {
		t.Fatal("exact credential binding refused")
	}
	defer cleanup()
	for _, value := range env {
		if strings.Contains(value, "must-not-inherit") {
			t.Fatal("inherited configuration reached runtime")
		}
	}
	replaced := func(context.Context, string, string) ([]byte, error) { return []byte("different-fixture-token"), nil }
	if _, _, cleanup, err := vettedCLIEnvironment(context.Background(), "cursor", digest, replaced); err == nil {
		cleanup()
		t.Fatal("changed credential binding accepted")
	}
}

// Opt-in host compatibility check. Credentials are read only by the fixed
// lookup and passed only to an installed CLI status command. Never logs the
// environment, payload, stdout, stderr or account details. No model call.
func TestInstalledCLIPrivateHomeAuthentication(t *testing.T) {
	if os.Getenv("SF_TEST_CLI_AUTH") != "1" {
		t.Skip("explicit host credential test only")
	}
	for _, provider := range []string{"claude", "cursor"} {
		t.Run(provider, func(t *testing.T) {
			home := credentialHome(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			env, _, err := prepareCLICredentials(ctx, provider, home, lookupCLISecret)
			if err != nil {
				t.Fatal("credential handoff unavailable")
			}
			binary := "claude"
			args := []string{"auth", "status"}
			if provider == "cursor" {
				binary = "agent"
				args = []string{"status", "--format", "json"}
			}
			path, err := exec.LookPath(binary)
			if err != nil {
				t.Fatal("installed CLI missing")
			}
			cmd := exec.CommandContext(ctx, path, args...)
			cmd.Dir = home
			cmd.Env = append([]string{"HOME=" + home, "TMPDIR=" + home, "PATH=/usr/bin:/bin", "LANG=C"}, env...)
			var out limitedBuffer
			out.limit = 16 << 10
			cmd.Stdout = &out
			if cmd.Run() != nil || out.truncated {
				t.Fatal("isolated CLI status failed")
			}
			if provider == "cursor" {
				if !cursorprovider.BrowserAuthenticated(out.Bytes()) {
					t.Fatal("isolated Cursor login unavailable")
				}
			} else {
				var status struct {
					LoggedIn bool   `json:"loggedIn"`
					Method   string `json:"authMethod"`
				}
				if json.Unmarshal(out.Bytes(), &status) != nil || !status.LoggedIn || status.Method != "oauth_token" {
					t.Fatal("isolated Claude OAuth status unavailable")
				}
			}
		})
	}
}
