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
		"invalid_resume": ExitInput, "invalid_retry": ExitInput, "invalid_recover": ExitInput,
		"invalid_decision":         ExitInput,
		"operator_action_required": ExitAction, "operator_identity_required": ExitAction,
		"provider_auth_missing": ExitAction, "provider_unavailable": ExitAction, "auth_login_failed": ExitAction,
		"blocked_process": ExitAction, "uncertain_effect": ExitAction, "external_merge_observed": ExitAction,
		"invalid_transition": ExitAction, "control_drain_failed": ExitAction, "control_completion_failed": ExitAction,
		"not_configured": ExitAction, "doctor_not_configured": ExitAction, "doctor_failed": ExitAction,
		"project_conflict": ExitAction, "init_failed": ExitAction, "config_apply_failed": ExitAction, "unknown_project": ExitAction,
		"doctor_required": ExitAction, "start_refused": ExitAction, "submit_refused": ExitAction,
		"terminal_replay_requires_new": ExitAction, "not_ready": ExitAction, "runtime_activation_failed": ExitAction,
		"runtime_already_active": ExitAction, "runtime_rearm_unavailable": ExitAction,
		"retry_required": ExitAction, "retry_not_available": ExitAction, "retry_transition_refused": ExitAction,
		"provider_retry_exhausted":         ExitAction,
		"provider_retry_resubmit_required": ExitAction,
		"provider_retry_worktree_unready":  ExitAction,
		"provider_retry_rearm_blocked":     ExitAction,
		"recover_mode_refused":             ExitAction, "recover_transition_refused": ExitAction, "resume_transition_refused": ExitAction,
		"decision_refused": ExitAction, "approval_head_changed": ExitAction,
		"runtime_retirement_failed": ExitAction, "source_commit_required": ExitAction,
		"ticket_budget_exhausted": ExitAction, "provider_result_indeterminate": ExitAction,
		"provider_repair_unavailable": ExitAction,
		"daemon_unavailable":          ExitWait, "daemon_stopping": ExitWait, "provider_waiting": ExitWait, "checks_pending": ExitWait,
		"store_busy": ExitWait, "projection_unavailable": ExitWait, "external_state_unavailable": ExitWait,
		"control_state_unavailable": ExitWait, "evidence_unavailable": ExitWait, "logs_unavailable": ExitWait,
		"status_unavailable": ExitWait, "capacity_unavailable": ExitWait, "leader_lost": ExitWait,
		"ticket_id_unavailable": ExitWait, "runtime_rearm_failed": ExitWait, "resume_state_unavailable": ExitWait,
		"retry_state_unavailable": ExitWait, "provider_retry_worktree_unavailable": ExitWait, "approval_evidence_unavailable": ExitWait,
		"decision_unavailable": ExitWait, "decision_state_unavailable": ExitWait,
		"policy_refusal": ExitPolicy, "safety_blocked": ExitPolicy, "unqualified_provider": ExitPolicy,
		"evidence_conflict": ExitPolicy, "autonomous_unavailable": ExitPolicy,
		"takeover_inspection_failed": ExitPolicy, "takeover_changes_unadopted": ExitPolicy,
		"takeover_verification_changes_unadopted": ExitPolicy, "takeover_source_out_of_scope": ExitPolicy,
		"ticket_policy_refused": ExitPolicy, "takeover_remote_drift": ExitPolicy,
		"takeover_remote_evidence_unavailable": ExitPolicy,
		"protocol_incompatible":                ExitCompatibility, "version_mismatch": ExitCompatibility, "schema_mismatch": ExitCompatibility,
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

func TestProviderSafetyBlockersRemainActionableWithDaemonCancelArgv(t *testing.T) {
	for _, test := range []struct {
		code string
		argv []string
	}{
		{code: "provider_result_indeterminate", argv: []string{"sf-dev", "cancel", "SF-dev-provider-blocker"}},
		{code: "provider_repair_unavailable", argv: []string{"sf", "cancel", "SF-stable-provider-blocker"}},
		{code: "provider_retry_resubmit_required", argv: []string{"sf-dev", "cancel", "SF-dev-provider-retry"}},
	} {
		t.Run(test.code, func(t *testing.T) {
			response := api.Response{
				Version:   api.Version,
				RequestID: test.code,
				Error:     &api.Error{Code: test.code},
				NextAction: &domain.NextAction{
					Code: test.code,
					Argv: test.argv,
				},
			}
			if err := validateCLIResponse(response); err != nil {
				t.Fatalf("response validation: %v", err)
			}
			if got := exitCode(response); got != ExitAction {
				t.Fatalf("exit=%d, want %d", got, ExitAction)
			}
			action := nextAction(response)
			if action == nil {
				t.Fatal("next action is nil")
			}
			if len(action.Argv) != len(test.argv) {
				t.Fatalf("next action argv=%v, want %v", action.Argv, test.argv)
			}
			for i := range test.argv {
				if action.Argv[i] != test.argv[i] {
					t.Fatalf("next action argv=%v, want %v", action.Argv, test.argv)
				}
			}
		})
	}
}

func TestRuntimeActivationResponsesRemainActionable(t *testing.T) {
	for _, test := range []struct {
		code string
		argv []string
	}{
		{code: "runtime_activation_failed", argv: []string{"sf-dev", "providers", "qualify", "--builder", "codex", "--reviewer", "codex"}},
		{code: "runtime_already_active", argv: []string{"sf-dev", "daemon", "status"}},
		{code: "daemon_stopping", argv: []string{"sf-dev", "daemon", "status"}},
	} {
		t.Run(test.code, func(t *testing.T) {
			response := api.Response{Version: api.Version, RequestID: test.code, Error: &api.Error{Code: test.code}, NextAction: &domain.NextAction{Code: test.code, Argv: test.argv}}
			if err := validateCLIResponse(response); err != nil {
				t.Fatalf("response validation: %v", err)
			}
			want := ExitAction
			if test.code == "daemon_stopping" {
				want = ExitWait
			}
			if exitCode(response) != want {
				t.Fatalf("exit=%d, want %d", exitCode(response), want)
			}
		})
	}
}
