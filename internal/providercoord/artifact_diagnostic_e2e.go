//go:build sf_e2e

package providercoord

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
)

func reportProviderRunFailure(result contracts.CommandResult, commandFailed bool, err error) {
	fmt.Fprintf(os.Stderr, "SF_TEST_PROVIDER_RESULT command_failed=%t exit=%d stdout_bytes=%d stderr_bytes=%d stdout_truncated=%t stderr_truncated=%t stream_stage=%s\n", commandFailed, result.ExitCode, len(result.Stdout), len(result.Stderr), result.StdoutTruncated, result.StderrTruncated, successStreamCategory(err))
}

func successStreamCategory(err error) string {
	if err == nil {
		return "none"
	}
	parts := strings.Fields(strings.TrimPrefix(err.Error(), "Claude success stream is incomplete or inconsistent: "))
	if len(parts) != 3 || parts[1] != "event" {
		return "other"
	}
	index, e := strconv.Atoi(parts[2])
	if e != nil || index < 0 || index > 4096 {
		return "other"
	}
	switch parts[0] {
	case "input", "framing", "object", "identity", "initialization", "session", "terminal", "event_kind", "system_unknown", "permission_denial", "thinking_metadata", "retry_notice", "system_permission_denied", "system_notice", "system_model_switch", "system_turn_duration", "system_stop_hook_summary", "system_hook", "system_task", "system_compaction_status", "message_parent", "message_role", "message_model", "content", "content_block", "tool_result", "tool_identity", "tool_policy", "tool_input", "rate_limit_metadata", "tool_summary":
		return parts[0] + "/" + strconv.Itoa(index)
	}
	return "other"
}

// Never log the error itself: JSON decoding and path errors can contain
// provider-controlled output. Emit only a fixed category in acceptance builds.
func reportBuilderValidationFailure(err error) {
	fmt.Fprintln(os.Stderr, "SF_TEST_BUILDER_VALIDATION="+builderValidationCategory(err))
}

func reportPlannerValidationFailure(err error) {
	fmt.Fprintln(os.Stderr, "SF_TEST_PLANNER_VALIDATION="+plannerValidationCategory(err))
}

// Classify only code-owned error prefixes; never emit an error or field value.
func plannerValidationCategory(err error) string {
	if err == nil {
		return "unknown"
	}
	message := strings.TrimPrefix(err.Error(), "validate planning artifact: ")
	for _, match := range []struct{ prefix, category string }{
		{"unsupported schema ", "schema_version"},
		{"json: ", "json_shape"},
		{"invalid character ", "json_syntax"},
		{"proof kind ", "proof_kind"},
		{"proof command ", "proof_command"},
		{"proof details ", "proof_details"},
		{"acceptance ", "acceptance"},
		{"planner paths: ", "paths"},
		{"planner must name at least one affected path", "paths_empty"},
		{"planner must name 1 to 20 commands", "commands_count"},
		{"planner command ", "command"},
		{"risks ", "risks"},
		{"planner may ask ", "questions_count"},
		{"each question ", "question_shape"},
		{"question options ", "question_options"},
	} {
		if strings.HasPrefix(message, match.prefix) {
			return match.category
		}
	}
	return "other_validation"
}

func builderValidationCategory(err error) string {
	if err == nil {
		return "unknown"
	}
	message := strings.TrimPrefix(err.Error(), "validate build artifact: ")
	switch message {
	case "builder summary is required":
		return "summary_missing"
	case "builder changed files are required":
		return "changed_files_missing"
	case "builder command evidence is required":
		return "commands_missing"
	case "builder changed protected verification without an amendment request":
		return "protected_verification_changed"
	case "verification amendment request requires a protected verification change":
		return "amendment_without_protected_change"
	case "verification amendment request is incomplete", "verification amendment command does not match the configuration snapshot", "verification amendment is not freshly approved":
		return "amendment_invalid"
	}
	for _, match := range []struct{ prefix, category string }{
		{"builder changed files: ", "changed_files_invalid"},
		{"builder command ", "command_invalid"},
		{"protected verification files: ", "protected_inventory_invalid"},
		{"unsupported schema ", "schema_version"},
		{"json: ", "json_shape"},
		{"invalid character ", "json_syntax"},
	} {
		if strings.HasPrefix(message, match.prefix) {
			return match.category
		}
	}
	return "other_validation"
}
