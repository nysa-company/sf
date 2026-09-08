// Package cursorprovider defines Cursor protocol facts, not launch authority.
// A model catalog entry and login observation do not qualify a runtime or
// establish monetary usage. Process isolation remains supervisor-owned.
package cursorprovider

import (
	"bytes"
	"encoding/json"
	"io"
)

const AuthModeBrowser = "cursor_browser"

// SupportedModels is a selection catalog, not a qualification or entitlement.
// Every selected model still needs its own current native attestation.
func SupportedModels() []string {
	return []string{
		"gpt-5.6-luna-low", "gpt-5.6-luna-medium", "gpt-5.6-luna-high",
		"claude-sonnet-5-low", "claude-sonnet-5-medium", "claude-sonnet-5-high", "claude-sonnet-5-thinking-high",
		"cursor-grok-4.6-low", "cursor-grok-4.6-medium", "cursor-grok-4.6-high",
	}
}

// ModelFamily binds the exact IDs observed from the authenticated CLI catalog.
// Cursor is a transport, not a model family: Claude through Cursor is not an
// independent reviewer of Claude through Claude Code. Unknown/auto models fail
// closed instead of acquiring an invented family or silently falling back.
func ModelFamily(model string) (string, bool) {
	switch model {
	case "claude-sonnet-5-low", "claude-sonnet-5-medium", "claude-sonnet-5-high", "claude-sonnet-5-thinking-high":
		return "anthropic-claude", true
	case "gpt-5.6-luna-low", "gpt-5.6-luna-medium", "gpt-5.6-luna-high":
		return "openai-gpt-5.6", true
	case "cursor-grok-4.6-low", "cursor-grok-4.6-medium", "cursor-grok-4.6-high":
		return "xai-grok", true
	default:
		return "", false
	}
}

// BrowserAuthenticated consumes only the bounded machine status observation.
// It must be called after checking probe exit status, truncation and executable
// identity in a scrubbed environment with no API-key/endpoint override. It
// neither reads credentials nor proves subscription pricing or credit balance.
func BrowserAuthenticated(output []byte) bool {
	if len(output) == 0 || len(output) > 16<<10 {
		return false
	}
	// Refuse duplicate top-level fields, including conflicting auth indicators.
	d := json.NewDecoder(bytes.NewReader(output))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return false
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || len(fields) >= 32 {
			return false
		}
		if _, exists := fields[key]; exists {
			return false
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return false
		}
		fields[key] = raw
	}
	end, err := d.Token()
	if err != nil || end != json.Delim('}') {
		return false
	}
	if _, err := d.Token(); err != io.EOF {
		return false
	}
	var status string
	if json.Unmarshal(fields["status"], &status) != nil || status != "authenticated" {
		return false
	}
	for _, key := range []string{"isAuthenticated", "hasAccessToken", "hasRefreshToken"} {
		var value bool
		if json.Unmarshal(fields[key], &value) != nil || !value {
			return false
		}
	}
	return true
}
