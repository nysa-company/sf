package claudeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func successStreamFixture(input contracts.PhaseInput, terminal string) []byte {
	session := "11111111-1111-4111-8111-111111111111"
	values := []map[string]any{
		{"type": "system", "subtype": "init", "model": input.Provider.Model},
		{"type": "assistant", "parent_tool_use_id": nil, "message": map[string]any{"role": "assistant", "model": input.Provider.Model, "content": []any{map[string]any{"type": "tool_use", "id": "tool_read", "name": "Read", "input": map[string]any{"file_path": "x"}}}}},
		{"type": "user", "parent_tool_use_id": nil, "message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "tool_read", "content": "fixture text"}}}},
		{"type": "assistant", "parent_tool_use_id": nil, "message": map[string]any{"role": "assistant", "model": input.Provider.Model, "content": []any{map[string]any{"type": "text", "text": "done"}}}},
		{},
	}
	_ = json.Unmarshal([]byte(terminal), &values[4])
	var lines [][]byte
	for i, value := range values {
		value["session_id"] = session
		value["uuid"] = fmt.Sprintf("22222222-2222-4222-8222-%012d", i+1)
		raw, _ := json.Marshal(value)
		lines = append(lines, raw)
	}
	return bytes.Join(lines, []byte{'\n'})
}

func TestClaudeSuccessStreamRequiresCompleteExactIdentity(t *testing.T) {
	input := claudeInput()
	input.Provider.Version = "2.1.263"
	stream := successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true}}`)
	if _, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: stream}); err != nil {
		t.Fatal("complete stream refused")
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"missing-init": func(raw []byte) []byte { return bytes.Join(bytes.Split(raw, []byte{'\n'})[1:], []byte{'\n'}) },
		"missing-final": func(raw []byte) []byte {
			lines := bytes.Split(raw, []byte{'\n'})
			return bytes.Join(lines[:len(lines)-1], []byte{'\n'})
		},
		"trailing-result": func(raw []byte) []byte {
			lines := bytes.Split(raw, []byte{'\n'})
			return append(append(raw, '\n'), lines[len(lines)-1]...)
		},
		"model": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(input.Provider.Model), []byte("other-model"), 1)
		},
		"session": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("11111111-1111"), []byte("33333333-3333"), 1)
		},
		"duplicate-id": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("8222-000000000002"), []byte("8222-000000000001"), 1)
		},
		"duplicate-field": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"type":"system"`), []byte(`"type":"system","type":"system"`), 1)
		},
		"missing-tool-result": func(raw []byte) []byte {
			lines := bytes.Split(raw, []byte{'\n'})
			return bytes.Join(append(lines[:2], lines[3:]...), []byte{'\n'})
		},
		"foreign-tool-result": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"tool_use_id":"tool_read"`), []byte(`"tool_use_id":"other"`), 1)
		},
		"shell": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"name":"Read"`), []byte(`"name":"Bash"`), 1)
		},
		"write-in-planner": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"name":"Read"`), []byte(`"name":"Write"`), 1)
		},
		"subagent": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"parent_tool_use_id":null`), []byte(`"parent_tool_use_id":"agent"`), 1)
		},
		"partial": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"type":"assistant"`), []byte(`"type":"stream_event"`), 1)
		},
		"terminal-error": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"is_error":false`), []byte(`"is_error":true`), 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw := mutate(append([]byte(nil), stream...))
			if _, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: raw}); err == nil {
				t.Fatal("inconsistent stream accepted")
			}
		})
	}
	for _, command := range []contracts.CommandResult{{ExitCode: 1, Stdout: stream}, {Stdout: stream, StdoutTruncated: true}, {Stdout: stream, StderrTruncated: true}} {
		if _, err := TerminalStreamResult(context.Background(), input, command); err == nil {
			t.Fatal("failed or truncated command accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := TerminalStreamResult(ctx, input, contracts.CommandResult{Stdout: stream}); err == nil {
		t.Fatal("cancelled stream accepted")
	}
	input.Phase = domain.PhaseBuild
	if _, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: bytes.Replace(stream, []byte(`"name":"Read"`), []byte(`"name":"Write"`), 1)}); err != nil {
		t.Fatal("Builder write stream refused")
	}
}

func TestClaudeSuccessStreamThinkingMetadata(t *testing.T) {
	input := claudeInput()
	base := successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true}}`)
	lines := bytes.Split(base, []byte{'\n'})
	for _, tc := range []struct {
		name, counters string
		valid          bool
	}{
		{"valid", `"estimated_tokens":12,"estimated_tokens_delta":4`, true},
		{"zero", `"estimated_tokens":0,"estimated_tokens_delta":0`, true},
		{"negative", `"estimated_tokens":-1,"estimated_tokens_delta":0`, false},
		{"delta_exceeds_total", `"estimated_tokens":1,"estimated_tokens_delta":2`, false},
		{"fractional", `"estimated_tokens":1.5,"estimated_tokens_delta":1`, false},
		{"missing", `"estimated_tokens":1`, false},
		{"null", `"estimated_tokens":null,"estimated_tokens_delta":0`, false},
		{"oversized", `"estimated_tokens":1048577,"estimated_tokens_delta":1`, false},
		{"extra", `"estimated_tokens":12,"estimated_tokens_delta":4,"model":"other"`, false},
		{"duplicate", `"estimated_tokens":12,"estimated_tokens":12,"estimated_tokens_delta":4`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := []byte(`{"type":"system","subtype":"thinking_tokens","uuid":"33333333-3333-4333-8333-333333333333","session_id":"11111111-1111-4111-8111-111111111111",` + tc.counters + `}`)
			stream := bytes.Join(append(append([][]byte{lines[0]}, metadata), lines[1:]...), []byte{'\n'})
			terminal, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: stream})
			if (err == nil) != tc.valid || tc.valid && !bytes.Equal(terminal, lines[len(lines)-1]) {
				t.Fatal("thinking metadata classification or terminal changed")
			}
		})
	}
}

func TestClaudeSuccessStreamPermissionDenial(t *testing.T) {
	input := claudeInput()
	base := successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true}}`)
	base = bytes.Replace(base, []byte(`"tool_use_id":"tool_read"`), []byte(`"tool_use_id":"tool_read","is_error":true`), 1)
	lines := bytes.Split(base, []byte{'\n'})
	denial := []byte(`{"type":"system","subtype":"permission_denied","uuid":"33333333-3333-4333-8333-333333333333","session_id":"11111111-1111-4111-8111-111111111111","tool_name":"Read","tool_use_id":"tool_read","decision_reason_type":"mode","message":"fixture denied"}`)
	stream := bytes.Join(append(append([][]byte{}, lines[:2]...), append([][]byte{denial}, lines[2:]...)...), []byte{'\n'})
	if _, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: stream}); err != nil {
		t.Fatal("matched native denial refused")
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"unknown_tool": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"tool_name":"Read"`), []byte(`"tool_name":"Bash"`), 1)
		},
		"foreign_call": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"tool_use_id":"tool_read"`), []byte(`"tool_use_id":"foreign"`), 1)
		},
		"successful_result": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"is_error":true`), []byte(`"is_error":false`), 1)
		},
		"subagent": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"tool_name":"Read"`), []byte(`"agent_id":"foreign","tool_name":"Read"`), 1)
		},
		"hook": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"decision_reason_type":"mode"`), []byte(`"decision_reason_type":"hook"`), 1)
		},
		"extra": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"tool_name":"Read"`), []byte(`"other":"fixture","tool_name":"Read"`), 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: mutate(append([]byte(nil), stream...))}); err == nil {
				t.Fatal("unbound denial accepted")
			}
		})
	}
}

func TestClaudeSuccessStreamRateMetadata(t *testing.T) {
	input := claudeInput()
	base := successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true}}`)
	lines := bytes.Split(base, []byte{'\n'})
	for _, tc := range []struct {
		name, info string
		valid      bool
	}{
		{"allowed", `{"status":"allowed"}`, true},
		{"warning", `{"status":"allowed_warning","utilization":0.9}`, true},
		{"advisory_only", `{"status":"allowed","untrusted_metadata":{"anything":"not artifact or billing evidence"}}`, true},
		{"rejected", `{"status":"rejected"}`, false},
		{"unknown", `{"status":"unknown"}`, false},
		{"missing", `{}`, false},
		{"null", `null`, false},
		{"mistyped", `{"status":true}`, false},
		{"duplicate", `{"status":"rejected","status":"allowed"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := []byte(`{"type":"rate_limit_event","uuid":"33333333-3333-4333-8333-333333333333","session_id":"11111111-1111-4111-8111-111111111111","rate_limit_info":` + tc.info + `}`)
			stream := bytes.Join(append(append([][]byte{lines[0]}, metadata), lines[1:]...), []byte{'\n'})
			terminal, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: stream})
			if (err == nil) != tc.valid || tc.valid && !bytes.Equal(terminal, lines[len(lines)-1]) {
				t.Fatal("rate metadata classification or terminal changed")
			}
		})
	}
}

func TestClaudeReadOnlyUnavailableToolIsNotExecution(t *testing.T) {
	input := claudeInput()
	input.Phase = domain.PhaseReview
	for _, tc := range []struct {
		name, tool, content string
		isError, valid      bool
	}{
		{"bare", "Edit", "No such tool available: Edit", true, true},
		{"error_prefix", "Write", "Error: No such tool available: Write", true, true},
		{"wrapped", "Edit", "<tool_use_error>No such tool available: Edit</tool_use_error>", true, true},
		{"wrapped_error", "Write", "<tool_use_error>Error: No such tool available: Write</tool_use_error>", true, true},
		{"native_disabled", "Edit", "<tool_use_error>Error: No such tool available: Edit. Edit is disabled for this session, in subagents as well as here.</tool_use_error>", true, true},
		{"foreign_disabled", "Edit", "<tool_use_error>Error: No such tool available: Edit. Write is disabled for this session, in subagents as well as here.</tool_use_error>", true, false},
		{"ordinary_failure", "Edit", "disk full after partial write", true, false},
		{"success", "Edit", "No such tool available: Edit", false, false},
		{"foreign_tool", "Edit", "No such tool available: Write", true, false},
		{"shell", "Bash", "No such tool available: Bash", true, false},
		{"additional_text", "Edit", "No such tool available: Edit; already wrote", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := successStreamFixture(input, `{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true}}`)
			stream = bytes.Replace(stream, []byte(`"name":"Read"`), []byte(`"name":"`+tc.tool+`"`), 1)
			content, _ := json.Marshal(tc.content)
			stream = bytes.Replace(stream, []byte(`"fixture text"`), content, 1)
			stream = bytes.Replace(stream, []byte(`"tool_use_id":"tool_read"`), []byte(fmt.Sprintf(`"tool_use_id":"tool_read","is_error":%t`, tc.isError)), 1)
			if _, err := TerminalStreamResult(context.Background(), input, contracts.CommandResult{Stdout: stream}); (err == nil) != tc.valid {
				t.Fatal("unavailable tool classification mismatch")
			}
		})
	}
}
