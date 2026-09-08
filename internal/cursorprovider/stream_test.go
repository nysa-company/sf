package cursorprovider

import (
	"strings"
	"testing"
)

func TestStreamArtifactBindsSessionAndLogin(t *testing.T) {
	init := `{"type":"system","subtype":"init","apiKeySource":"login","cwd":"/private/worktree","session_id":"session","model":"qualified display"}`
	result := `{"type":"result","subtype":"success","is_error":false,"session_id":"session","result":"{\"ok\":true}"}`
	valid := init + "\n" + result + "\n"
	for name, output := range map[string]string{
		"minimal":  valid,
		"messages": init + "\n" + `{"type":"assistant","session_id":"session","message":{"content":"not an artifact"}}` + "\n" + result,
		"thinking": init + "\n" + `{"type":"thinking","subtype":"delta","session_id":"session","text":"opaque fixture"}` + "\n" + `{"type":"thinking","subtype":"completed","session_id":"session"}` + "\n" + result,
	} {
		t.Run(name, func(t *testing.T) {
			artifact, err := StreamArtifact([]byte(output), "/private/worktree", "qualified display")
			if err != nil || string(artifact) != `{"ok":true}` {
				t.Fatal("valid session rejected", err)
			}
		})
	}
	for name, output := range map[string]string{
		"api auth":                 strings.Replace(valid, `"login"`, `"env"`, 1),
		"model drift":              strings.Replace(valid, "qualified display", "other", 1),
		"directory drift":          strings.Replace(valid, "/private/worktree", "/other", 1),
		"session drift":            init + "\n" + strings.Replace(result, `"session"`, `"other"`, 1),
		"missing init":             result,
		"missing result":           init + "\n",
		"second result":            valid + result,
		"second init":              init + "\n" + valid,
		"blank line":               init + "\n\n" + result,
		"truncated":                valid[:len(valid)-3],
		"terminal error":           strings.Replace(valid, `"is_error":false`, `"is_error":true`, 1),
		"duplicate model":          strings.Replace(valid, `"model":`, `"model":"other","model":`, 1),
		"tool as result":           init + "\n" + `{"type":"tool_call","session_id":"session","result":"{}"}`,
		"thinking as result":       init + "\n" + `{"type":"thinking","subtype":"completed","session_id":"session","result":"{}"}`,
		"thinking unknown subtype": init + "\n" + `{"type":"thinking","subtype":"success","session_id":"session"}` + "\n" + result,
		"thinking session drift":   init + "\n" + `{"type":"thinking","subtype":"delta","session_id":"other"}` + "\n" + result,
		"null session":             init + "\n" + strings.Replace(result, `"session_id":"session"`, `"session_id":null`, 1),
		"oversize":                 strings.Repeat("x", (1<<20)+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := StreamArtifact([]byte(output), "/private/worktree", "qualified display"); err == nil {
				t.Fatal("invalid session accepted")
			}
		})
	}
}
