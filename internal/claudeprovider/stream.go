package claudeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providerjson"
)

var ErrSuccessStream = errors.New("Claude success stream is incomplete or inconsistent")

// TerminalStreamResult authenticates bounded protocol framing, not execution
// or billing. Only a complete same-session stream may expose its final JSON
// envelope to the existing artifact/accounting parser. No intermediate text
// is a result; tool output cannot override model/session or final authority.
func TerminalStreamResult(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult) (terminal []byte, failure error) {
	stage, event := "input", 0
	defer func() {
		if errors.Is(failure, ErrSuccessStream) {
			// Fixed parser stages and an index only, never provider-controlled text.
			failure = fmt.Errorf("%w: %s event %d", ErrSuccessStream, stage, event)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	family, supported := ModelFamily(input.Provider.Model)
	if !streamToolAllowed(input.Phase, "Read") {
		return nil, ErrSuccessStream
	}
	if !supported || input.Provider.Provider != "claude" || input.Provider.Family != family || input.Provider.Version != "2.1.263" || input.AuthMode != AuthModeSubscription || input.Profile != contracts.ProfileGuarded || command.ExitCode != 0 || command.StdoutTruncated || command.StderrTruncated || len(command.Stdout) == 0 || len(command.Stdout) > providerjson.MaxResultBytes || len(command.Stderr) > providerjson.MaxResultBytes {
		return nil, ErrSuccessStream
	}
	lines := bytes.Split(bytes.TrimSpace(command.Stdout), []byte{'\n'})
	stage = "framing"
	if len(lines) < 2 || len(lines) > 4096 {
		return nil, ErrSuccessStream
	}
	seen, tools := map[string]bool{}, map[string]string{}
	denied := map[string]bool{}
	unavailableOnly := map[string]bool{}
	allTools := map[string]bool{}
	var session string
	assistants, retryCount, retryMaximum := 0, 0, 0
	for index, line := range lines {
		event, stage = index, "object"
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields, err := providerjson.Object(line)
		if err != nil {
			return nil, ErrSuccessStream
		}
		kind, id, current := rejectionString(fields, "type"), rejectionString(fields, "uuid"), rejectionString(fields, "session_id")
		stage = "identity"
		if !rejectionID(id) || seen[id] || !rejectionID(current) {
			return nil, ErrSuccessStream
		}
		seen[id] = true
		if index == 0 {
			stage = "initialization"
			if kind != "system" || rejectionString(fields, "subtype") != "init" || rejectionString(fields, "model") != input.Provider.Model {
				return nil, ErrSuccessStream
			}
			session = current
			continue
		}
		stage = "session"
		if current != session {
			return nil, ErrSuccessStream
		}
		if index == len(lines)-1 {
			stage = "terminal"
			var isError *bool
			if kind != "result" || rejectionString(fields, "subtype") != "success" || json.Unmarshal(fields["is_error"], &isError) != nil || isError == nil || *isError || len(tools) != 0 || assistants == 0 || retryCount != 0 {
				return nil, ErrSuccessStream
			}
			return append([]byte(nil), line...), nil
		}
		stage = "event_kind"
		switch kind {
		case "system":
			stage = "system_unknown"
			if rejectionString(fields, "subtype") == "permission_denied" {
				stage = "permission_denial"
				id := rejectionString(fields, "tool_use_id")
				if tools[id] == "" || denied[id] || rejectionString(fields, "tool_name") != tools[id] || !validPermissionDenial(fields) {
					return nil, ErrSuccessStream
				}
				denied[id] = true
				continue
			}
			if rejectionString(fields, "subtype") == "thinking_tokens" {
				stage = "thinking_metadata"
				var total, delta *int
				if len(fields) != 6 || json.Unmarshal(fields["estimated_tokens"], &total) != nil || json.Unmarshal(fields["estimated_tokens_delta"], &delta) != nil || total == nil || delta == nil || *total < 0 || *total > 1<<20 || *delta < 0 || *delta > *total {
					return nil, ErrSuccessStream
				}
				// Native display estimates only: never usage, result or retry proof.
				continue
			}
			switch rejectionString(fields, "subtype") {
			case "api_retry":
				stage = "retry_notice"
			case "permission_denied":
				stage = "system_permission_denied"
			case "turn_starting", "init_milestone", "informational", "notification":
				stage = "system_notice"
			case "model_fallback", "model_consent_fallback", "model_refusal_fallback", "model_refusal_no_fallback":
				stage = "system_model_switch"
			case "turn_duration":
				stage = "system_turn_duration"
			case "stop_hook_summary":
				stage = "system_stop_hook_summary"
			case "hook_response", "hook_started", "hook_progress":
				stage = "system_hook"
			case "task_started", "task_notification", "task_progress":
				stage = "system_task"
			case "compact_boundary", "status":
				stage = "system_compaction_status"
			}
			var attempt, maximum, delay, status *int
			category := rejectionString(fields, "error")
			if rejectionString(fields, "subtype") != "api_retry" || !rejectionCategory(category) || json.Unmarshal(fields["attempt"], &attempt) != nil || json.Unmarshal(fields["max_retries"], &maximum) != nil || json.Unmarshal(fields["retry_delay_ms"], &delay) != nil || json.Unmarshal(fields["error_status"], &status) != nil || attempt == nil || maximum == nil || delay == nil || status == nil || *attempt != retryCount+1 || *maximum < 1 || *maximum > 15 || *attempt > *maximum || *delay < 0 || *delay > 60000 || !rejectionHTTPMatches(category, *status) || (retryMaximum != 0 && retryMaximum != *maximum) {
				return nil, ErrSuccessStream
			}
			retryCount, retryMaximum = *attempt, *maximum
		case "assistant", "user":
			stage = "message_parent"
			if !bytes.Equal(bytes.TrimSpace(fields["parent_tool_use_id"]), []byte("null")) {
				return nil, ErrSuccessStream
			}
			if raw, exists := fields["error"]; exists && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return nil, ErrSuccessStream
			}
			stage = "message_role"
			message, err := providerjson.Object(fields["message"])
			if err != nil || rejectionString(message, "role") != kind {
				return nil, ErrSuccessStream
			}
			stage = "message_model"
			if kind == "assistant" && rejectionString(message, "model") != input.Provider.Model {
				return nil, ErrSuccessStream
			}
			if kind == "assistant" {
				assistants++
				retryCount, retryMaximum = 0, 0
			} else if retryCount != 0 {
				return nil, ErrSuccessStream
			}
			stage = "content"
			var content []json.RawMessage
			if json.Unmarshal(message["content"], &content) != nil || len(content) == 0 || len(content) > 128 {
				return nil, ErrSuccessStream
			}
			for _, raw := range content {
				stage = "content_block"
				block, err := providerjson.Object(raw)
				if err != nil {
					return nil, ErrSuccessStream
				}
				typeName := rejectionString(block, "type")
				if kind == "user" {
					stage = "tool_result"
					toolID := rejectionString(block, "tool_use_id")
					if typeName != "tool_result" || tools[toolID] == "" || !validToolResultContent(block["content"]) {
						return nil, ErrSuccessStream
					}
					var toolError bool
					if raw, exists := block["is_error"]; exists {
						if json.Unmarshal(raw, &toolError) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
							return nil, ErrSuccessStream
						}
					}
					if denied[toolID] && !toolError {
						return nil, ErrSuccessStream
					}
					if unavailableOnly[toolID] && (!toolError || !nativeUnavailableTool(block["content"], tools[toolID])) {
						return nil, ErrSuccessStream
					}
					delete(tools, toolID)
					delete(denied, toolID)
					delete(unavailableOnly, toolID)
					continue
				}
				switch typeName {
				case "text":
					if !streamString(block["text"]) {
						return nil, ErrSuccessStream
					}
				case "thinking":
					if !streamString(block["thinking"]) || !streamString(block["signature"]) {
						return nil, ErrSuccessStream
					}
				case "redacted_thinking":
					if !streamString(block["data"]) {
						return nil, ErrSuccessStream
					}
				case "tool_use":
					stage = "tool_identity"
					toolID := rejectionString(block, "id")
					if toolID == "" || len(toolID) > 256 || allTools[toolID] {
						return nil, ErrSuccessStream
					}
					stage = "tool_policy"
					if !streamToolAllowed(input.Phase, rejectionString(block, "name")) {
						name := rejectionString(block, "name")
						if (input.Phase != domain.PhasePlanning && input.Phase != domain.PhaseReview) || (name != "Write" && name != "Edit") {
							return nil, ErrSuccessStream
						}
						// These tools are absent from the read-only CLI registry.
						// Only its exact NO_SUCH_TOOL result can settle this attempt;
						// an ordinary error may follow a partial write and is refused.
						unavailableOnly[toolID] = true
					}
					stage = "tool_input"
					if _, err := providerjson.Object(block["input"]); err != nil {
						return nil, ErrSuccessStream
					}
					tools[toolID], allTools[toolID] = rejectionString(block, "name"), true
				default:
					return nil, ErrSuccessStream
				}
			}
		case "rate_limit_event":
			stage = "rate_limit_metadata"
			info, err := providerjson.Object(fields["rate_limit_info"])
			if len(fields) != 4 || err != nil || len(info) > 32 {
				return nil, ErrSuccessStream
			}
			switch rejectionString(info, "status") {
			case "allowed", "allowed_warning":
				// Other bounded advisory fields are not consumed or returned.
				// This neither authenticates charges nor permits a retry.
			default:
				return nil, ErrSuccessStream
			}
		case "tool_use_summary":
			stage = "tool_summary"
			return nil, ErrSuccessStream
		default:
			return nil, ErrSuccessStream
		}
	}
	return nil, ErrSuccessStream
}

func nativeUnavailableTool(raw json.RawMessage, name string) bool {
	var content string
	if (name != "Write" && name != "Edit") || json.Unmarshal(raw, &content) != nil {
		return false
	}
	want := "No such tool available: " + name
	for _, suffix := range []string{"", ". " + name + " is disabled for this session, in subagents as well as here."} {
		message := want + suffix
		if content == message || content == "Error: "+message || content == "<tool_use_error>"+message+"</tool_use_error>" || content == "<tool_use_error>Error: "+message+"</tool_use_error>" {
			return true
		}
	}
	return false
}

func validPermissionDenial(fields map[string]json.RawMessage) bool {
	for key, raw := range fields {
		switch key {
		case "type", "subtype", "uuid", "session_id", "tool_name", "tool_use_id":
		case "agent_id":
			if !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				return false
			}
		case "decision_reason_type":
			switch rejectionString(fields, key) {
			case "rule", "mode", "safetyCheck", "workingDir", "other":
			default:
				return false
			}
		case "decision_reason", "message":
			if !streamString(raw) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func streamString(raw json.RawMessage) bool {
	var value *string
	return json.Unmarshal(raw, &value) == nil && value != nil
}
func validToolResultContent(raw json.RawMessage) bool {
	if streamString(raw) {
		return true
	}
	var blocks []json.RawMessage
	if json.Unmarshal(raw, &blocks) != nil || len(blocks) > 128 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	for _, raw := range blocks {
		block, err := providerjson.Object(raw)
		if err != nil || rejectionString(block, "type") != "text" || !streamString(block["text"]) {
			return false
		}
	}
	return true
}
func streamToolAllowed(phase domain.Phase, name string) bool {
	switch phase {
	case domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview:
	default:
		return false
	}
	switch name {
	case "Read", "Glob", "Grep", "StructuredOutput":
		return true
	case "Write", "Edit":
		return phase == domain.PhaseBuild || phase == domain.PhaseVerification
	}
	return false
}
