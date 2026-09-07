package processsupervisor

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/cursorprovider"
)

func TestCursorObserverPinsCatalogWithoutModelPrompt(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native macOS observer")
	}
	dir := credentialHome(t)
	path := filepath.Join(dir, "cursor-agent")
	script := `#!/bin/sh
test -z "$CURSOR_API_KEY" || exit 8
test "$PWD" = "$HOME" || exit 9
test "${#HOME}" -lt 60 || exit 10
case "$*" in
  --version) echo '2026.09.02-c22c1a3' ;;
  'status --format json')
    test -f "$HOME/.cursor/auth.json" || exit 11
    echo '{"status":"authenticated","isAuthenticated":true,"hasAccessToken":true,"hasRefreshToken":true}' ;;
  models) echo 'gpt-5.6-luna-low - GPT-5.6 Luna 1M Low (default)' ;;
  *) exit 12 ;;
esac
`
	for name, data := range map[string]string{"cursor-agent": script, "node": "fixture", "index.js": "fixture"} {
		if os.WriteFile(filepath.Join(dir, name), []byte(data), 0700) != nil {
			t.Fatal("fixture unavailable")
		}
	}
	t.Setenv("CURSOR_API_KEY", "must-not-inherit")
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	lookup := func(context.Context, string, string) ([]byte, error) { return []byte("fixture-token"), nil }
	b, display, err := s.observeCursorRuntime(t.Context(), path, "gpt-5.6-luna-low", lookup)
	if err != nil || display != "GPT-5.6 Luna 272K Low" || b.FixtureDigest != cursorprovider.FixtureDigest(b.Identity.Model, display) || len(b.AuthDigest) != 64 {
		t.Fatal("observer failed", err)
	}
	if len(s.trusted) != 0 {
		t.Fatal("observation registered a runtime")
	}
	if os.WriteFile(path, []byte(strings.Replace(script, "2026.09.02-c22c1a3", "other-version", 1)), 0700) != nil {
		t.Fatal("fixture unavailable")
	}
	if _, _, err := s.observeCursorRuntime(t.Context(), path, "gpt-5.6-luna-low", lookup); err == nil {
		t.Fatal("untested version accepted")
	}
}

func TestInstalledCursorRuntimeObservation(t *testing.T) {
	if os.Getenv("SF_TEST_CURSOR_OBSERVER") != "1" {
		t.Skip("explicit status/catalog-only native test")
	}
	path, err := exec.LookPath("cursor-agent")
	if err != nil {
		t.Fatal("CLI unavailable")
	}
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b, display, err := s.ObserveCursorRuntime(t.Context(), path, "gpt-5.6-luna-low")
	if err != nil || b.FixtureDigest != cursorprovider.FixtureDigest("gpt-5.6-luna-low", display) || display == "" {
		t.Fatal("native metadata observation failed", err)
	}
	t.Log("exact catalog ID/display and browser status observed; no model prompt; not qualification")
}
