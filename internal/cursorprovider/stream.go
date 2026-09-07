package cursorprovider

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"

	"github.com/nysa-company/sf/internal/providerjson"
)

// StreamArtifact validates one bounded print-mode session. expectedModel is
// the display label observed during runtime qualification, not a guessed
// conversion from a catalog ID. Cursor reports a display label here: this
// check supplements (and cannot replace) pinned argv/runtime model identity.
// Callers must separately authenticate successful exit, no truncation, drain,
// invocation, and usage. No transcript or credential-bearing error is returned.
func StreamArtifact(output []byte, worktree, expectedModel string) ([]byte, error) {
	terminal, err := streamTerminal(output, worktree, expectedModel)
	if err != nil {
		return nil, err
	}
	return providerjson.Artifact(terminal, false)
}

func streamTerminal(output []byte, worktree, expectedModel string) ([]byte, error) {
	if len(output) == 0 || len(output) > providerjson.MaxResultBytes || expectedModel == "" ||
		!filepath.IsAbs(worktree) || filepath.Clean(worktree) != worktree {
		return nil, providerjson.ErrProtocol
	}
	lines := bytes.Split(bytes.TrimSuffix(output, []byte("\n")), []byte("\n"))
	if len(lines) < 2 || len(lines) > 4096 {
		return nil, providerjson.ErrProtocol
	}
	var session string
	for i, line := range lines {
		fields, ok := streamFields(line)
		if !ok {
			return nil, providerjson.ErrProtocol
		}
		value := func(key string) string {
			var s string
			_ = json.Unmarshal(fields[key], &s)
			return s
		}
		kind := value("type")
		if i == 0 {
			session = value("session_id")
			if kind != "system" || value("subtype") != "init" || session == "" || len(session) > 256 ||
				value("apiKeySource") != "login" || value("cwd") != worktree || value("model") != expectedModel {
				return nil, providerjson.ErrProtocol
			}
			continue
		}
		if value("session_id") != session {
			return nil, providerjson.ErrProtocol
		}
		if kind == "result" {
			if i != len(lines)-1 {
				return nil, providerjson.ErrProtocol
			}
			return bytes.Clone(line), nil
		}
		switch kind {
		case "thinking":
			// The pinned CLI emits delta/completed notifications in ask mode.
			// They never supply an artifact and are not exposed to callers.
			if value("subtype") != "delta" && value("subtype") != "completed" {
				return nil, providerjson.ErrProtocol
			}
		case "user", "assistant", "tool_call":
			// Only the terminal result supplies an artifact, never tool output.
		default:
			return nil, providerjson.ErrProtocol
		}
	}
	return nil, providerjson.ErrProtocol
}

func streamFields(line []byte) (map[string]json.RawMessage, bool) {
	d := json.NewDecoder(bytes.NewReader(line))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return nil, false
	}
	fields := make(map[string]json.RawMessage)
	for d.More() {
		t, err = d.Token()
		key, ok := t.(string)
		if err != nil || !ok || len(fields) >= 128 {
			return nil, false
		}
		if _, exists := fields[key]; exists {
			return nil, false
		}
		var raw json.RawMessage
		if d.Decode(&raw) != nil {
			return nil, false
		}
		fields[key] = raw
	}
	if t, err = d.Token(); err != nil || t != json.Delim('}') {
		return nil, false
	}
	_, err = d.Token()
	return fields, err == io.EOF
}
