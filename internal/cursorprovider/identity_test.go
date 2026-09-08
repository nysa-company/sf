package cursorprovider

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/claudeprovider"
)

func TestCursorFamilyUsesUnderlyingModel(t *testing.T) {
	claude, _ := claudeprovider.ModelFamily("claude-sonnet-5")
	for model, want := range map[string]string{
		"claude-sonnet-5-low": claude,
		"gpt-5.6-luna-low":    "openai-gpt-5.6",
		"cursor-grok-4.6-low": "xai-grok",
	} {
		if got, ok := ModelFamily(model); !ok || got != want {
			t.Fatalf("%s: got %q, %v", model, got, ok)
		}
	}
	for _, model := range []string{"auto", "sonnet", "luna", "grok", "gpt-5.6-luna-low-fast", "unknown"} {
		if _, ok := ModelFamily(model); ok {
			t.Fatalf("unqualified model admitted: %s", model)
		}
	}
}

func TestCursorBrowserStatusFailsClosed(t *testing.T) {
	valid := `{"status":"authenticated","isAuthenticated":true,"hasAccessToken":true,"hasRefreshToken":true,"userInfo":{"email":"fixture@example.invalid"}}`
	if !BrowserAuthenticated([]byte(valid)) {
		t.Fatal("observed browser status shape refused")
	}
	for name, data := range map[string]string{
		"empty": "", "array": "[]", "partial": valid[:len(valid)-1],
		"trailing": valid + "}", "second": valid + valid,
		"oversize":   strings.Repeat(" ", 16<<10) + valid,
		"logged out": strings.Replace(valid, `"authenticated"`, `"unauthenticated"`, 1),
		"no refresh": strings.Replace(valid, `"hasRefreshToken":true`, `"hasRefreshToken":false`, 1),
		"missing":    strings.Replace(valid, `"hasAccessToken":true,`, "", 1),
		"null":       strings.Replace(valid, `"isAuthenticated":true`, `"isAuthenticated":null`, 1),
		"string":     strings.Replace(valid, `"isAuthenticated":true`, `"isAuthenticated":"true"`, 1),
		"duplicate":  strings.Replace(valid, `"status":`, `"status":"unauthenticated","status":`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if BrowserAuthenticated([]byte(data)) {
				t.Fatal("malformed/unauthenticated response admitted")
			}
		})
	}
}
