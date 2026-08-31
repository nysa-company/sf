package cli

import (
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestShellWordsQuotesEveryShellMetacharacter(t *testing.T) {
	got := shellWords([]string{"sf", "doctor", "a;$(touch /tmp/pwned)", "safe/path", "line\nbreak"})
	want := "sf doctor 'a;$(touch /tmp/pwned)' safe/path 'line\nbreak'"
	if got != want {
		t.Fatalf("shellWords=%q, want %q", got, want)
	}
}

func TestEveryCurrentErrorCodeHasAnExplicitStableExitCategory(t *testing.T) {
	// This inventory mirrors codes emitted by the current direct CLI, daemon,
	// and transport boundaries. Keep it table-driven so adding a code requires
	// choosing its operator-visible category rather than silently becoming 7.
	codes := map[string]ExitCode{
		"invalid_command": ExitInput, "invalid_request": ExitInput, "invalid_ticket": ExitInput,
		"invalid_argument": ExitInput, "invalid_repository": ExitInput, "invalid_configuration": ExitInput,
		"invalid_control": ExitInput, "invalid_ticket_reference": ExitInput, "invalid_submit": ExitInput,
		"invalid_logs": ExitInput, "wrong_channel": ExitInput, "ticket_not_found": ExitInput,
		"operator_action_required": ExitAction, "operator_identity_required": ExitAction,
		"provider_auth_missing": ExitAction, "provider_unavailable": ExitAction, "auth_login_failed": ExitAction,
		"blocked_process": ExitAction, "uncertain_effect": ExitAction, "external_merge_observed": ExitAction,
		"invalid_transition": ExitAction, "control_drain_failed": ExitAction, "control_completion_failed": ExitAction,
		"not_configured": ExitAction, "doctor_not_configured": ExitAction, "doctor_failed": ExitAction,
		"project_conflict": ExitAction, "init_failed": ExitAction, "unknown_project": ExitAction,
		"doctor_required": ExitAction, "start_refused": ExitAction, "submit_refused": ExitAction,
		"terminal_replay_requires_new": ExitAction, "not_ready": ExitAction, "runtime_activation_failed": ExitAction,
		"runtime_already_active": ExitAction,
		"daemon_unavailable":     ExitWait, "provider_waiting": ExitWait, "checks_pending": ExitWait,
		"store_busy": ExitWait, "projection_unavailable": ExitWait, "external_state_unavailable": ExitWait,
		"control_state_unavailable": ExitWait, "evidence_unavailable": ExitWait, "logs_unavailable": ExitWait,
		"status_unavailable": ExitWait, "capacity_unavailable": ExitWait, "leader_lost": ExitWait,
		"ticket_id_unavailable": ExitWait,
		"policy_refusal":        ExitPolicy, "safety_blocked": ExitPolicy, "unqualified_provider": ExitPolicy,
		"evidence_conflict": ExitPolicy, "autonomous_unavailable": ExitPolicy,
		"protocol_incompatible": ExitCompatibility, "version_mismatch": ExitCompatibility, "schema_mismatch": ExitCompatibility,
		"internal_error": ExitInternal, "internal_encoding": ExitInternal, "invalid_response": ExitInternal,
		"internal_response_invalid": ExitInternal, "daemon_start_failed": ExitInternal,
	}
	for code, want := range codes {
		for _, retryable := range []bool{false, true} {
			response := api.Response{Version: api.Version, RequestID: code, Error: &api.Error{Code: code, Retryable: retryable}, NextAction: &domain.NextAction{Code: code, Argv: []string{"sf", "doctor"}}}
			if got := exitCode(response); got != want {
				t.Errorf("%s retryable=%t: got exit %d, want %d", code, retryable, got, want)
			}
		}
	}
}

func TestRuntimeActivationResponsesRemainActionable(t *testing.T) {
	for _, test := range []struct {
		code string
		argv []string
	}{
		{code: "runtime_activation_failed", argv: []string{"sf-dev", "providers", "qualify", "--builder", "codex", "--reviewer", "codex"}},
		{code: "runtime_already_active", argv: []string{"sf-dev", "daemon", "status"}},
	} {
		t.Run(test.code, func(t *testing.T) {
			response := api.Response{Version: api.Version, RequestID: test.code, Error: &api.Error{Code: test.code}, NextAction: &domain.NextAction{Code: test.code, Argv: test.argv}}
			if err := validateCLIResponse(response); err != nil {
				t.Fatalf("response validation: %v", err)
			}
			if exitCode(response) != ExitAction {
				t.Fatalf("exit=%d, want action", exitCode(response))
			}
		})
	}
}
