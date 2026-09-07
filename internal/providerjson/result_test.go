package providerjson

import (
	"errors"
	"strings"
	"testing"
)

func TestTerminalArtifact(t *testing.T) {
	for _, tc := range []struct {
		name, output string
		schema       bool
		want         error
	}{
		{"claude", `{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true},"future_metadata":1}`, true, nil},
		{"cursor", `{"type":"result","subtype":"success","is_error":false,"result":"{\"ok\":true}"}`, false, nil},
		{"empty", ``, false, ErrProtocol},
		{"partial", `{"type":"result"`, false, ErrProtocol},
		{"missing_success", `{"type":"result","subtype":"success","result":"{}"}`, false, ErrProtocol},
		{"null_success", `{"type":"result","subtype":"success","is_error":null}`, false, ErrProtocol},
		{"typed_error", `{"type":"result","subtype":"success","is_error":"false"}`, false, ErrProtocol},
		{"terminal_error", `{"type":"result","subtype":"error_during_execution","is_error":true}`, false, ErrTerminal},
		{"conflicting_status", `{"type":"result","subtype":"error","is_error":false}`, false, ErrTerminal},
		{"duplicate", `{"type":"result","subtype":"success","is_error":true,"is_error":false,"result":"{}"}`, false, ErrProtocol},
		{"two_envelopes", `{"type":"result"}{"type":"result"}`, false, ErrProtocol},
		{"array", `[]`, false, ErrProtocol},
		{"missing_schema", `{"type":"result","subtype":"success","is_error":false,"result":"{}"}`, true, ErrArtifact},
		{"null_schema", `{"type":"result","subtype":"success","is_error":false,"structured_output":null}`, true, ErrArtifact},
		{"prose", `{"type":"result","subtype":"success","is_error":false,"result":"done"}`, false, ErrArtifact},
		{"oversized", strings.Repeat(" ", MaxResultBytes+1), false, ErrProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			artifact, err := Artifact([]byte(tc.output), tc.schema)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			if tc.want == nil && string(artifact) != `{"ok":true}` {
				t.Fatalf("unexpected artifact: %q", artifact)
			}
			if tc.want != nil && len(artifact) != 0 {
				t.Fatal("failure returned an artifact")
			}
		})
	}
}
