package providerjson

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestCommandObservationDoesNotInventBillingAuthority(t *testing.T) {
	for _, schema := range []bool{false, true} {
		input := contracts.PhaseInput{Provider: domain.ProviderIdentity{Provider: "fixture", Model: "exact"}}
		output := []byte(`{"type":"result","subtype":"success","is_error":false,"result":"{\"ok\":true}","structured_output":{"ok":true},"total_cost_usd":0,"usage":{"inputTokens":4}}`)
		result, err := Command(context.Background(), input, contracts.CommandResult{Stdout: output}, schema)
		if err != nil || result.Outcome != contracts.PhaseResultCompleted || string(result.Artifact) != `{"ok":true}` || result.Provider != input.Provider {
			t.Fatalf("schema=%v result=%+v err=%v", schema, result, err)
		}
		if result.UsageTrusted || result.TokenUsageTrusted || result.UsageUnits != 0 || result.Transcript != "" {
			t.Fatal("untrusted provider metadata acquired billing/logging authority")
		}
	}
}

func TestCommandObservationRequiresCleanBoundedTerminal(t *testing.T) {
	valid := []byte(`{"type":"result","subtype":"success","is_error":false,"structured_output":{"ok":true}}`)
	for name, tc := range map[string]struct {
		command contracts.CommandResult
		outcome string
		reason  contracts.ProviderFailureReason
	}{
		"exit":         {contracts.CommandResult{ExitCode: -1, Stdout: valid}, contracts.PhaseResultIndeterminate, contracts.ProviderFailureExit},
		"truncated":    {contracts.CommandResult{Stdout: valid, StdoutTruncated: true}, contracts.PhaseResultIndeterminate, contracts.ProviderFailureOutput},
		"stderr limit": {contracts.CommandResult{Stdout: valid, StderrTruncated: true}, contracts.PhaseResultIndeterminate, contracts.ProviderFailureOutput},
		"stderr only":  {contracts.CommandResult{Stderr: valid}, contracts.PhaseResultIndeterminate, contracts.ProviderFailureProtocol},
		"terminal":     {contracts.CommandResult{Stdout: []byte(`{"type":"result","subtype":"error","is_error":true}`)}, contracts.PhaseResultIndeterminate, contracts.ProviderFailureTerminal},
		"artifact":     {contracts.CommandResult{Stdout: []byte(`{"type":"result","subtype":"success","is_error":false}`)}, contracts.PhaseResultInvalidArtifact, ""},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := Command(context.Background(), contracts.PhaseInput{}, tc.command, true)
			if err == nil || result.Outcome != tc.outcome || result.FailureReason != tc.reason || result.UsageTrusted || len(result.Artifact) != 0 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Command(ctx, contracts.PhaseInput{}, contracts.CommandResult{Stdout: valid}, true); err != context.Canceled {
		t.Fatal("cancelled request accepted")
	}
}

func TestCommandKeepsEstimateSeparateFromCharge(t *testing.T) {
	for _, artifact := range []string{`,"structured_output":{"ok":true}`, ""} {
		output := []byte(`{"type":"result","subtype":"success","is_error":false,"total_cost_usd":0.0074268` + artifact + `}`)
		result, _ := Command(context.Background(), contracts.PhaseInput{}, contracts.CommandResult{Stdout: output}, true)
		if result.ReportedCostEstimateMicroUSD == nil || *result.ReportedCostEstimateMicroUSD != 7427 || result.UsageTrusted || result.UsageUnits != 0 {
			t.Fatal("estimate was lost or acquired charge authority")
		}
	}
	output := []byte(`{"type":"result","subtype":"success","is_error":false,"total_cost_usd":null,"structured_output":{"ok":true}}`)
	result, err := Command(context.Background(), contracts.PhaseInput{}, contracts.CommandResult{Stdout: output}, true)
	if err == nil || result.Outcome != contracts.PhaseResultIndeterminate || result.ReportedCostEstimateMicroUSD != nil || result.UsageTrusted {
		t.Fatal("malformed estimate accepted")
	}
}

func TestProviderRetryHintsDoNotProvePreExecution(t *testing.T) {
	// Provider metadata is not a supervisor-issued no-execution receipt.
	// Zero turns/tokens and Retry-After cannot authorize artifact repair or
	// another paid launch after an ambiguous terminal failure.
	for _, output := range []string{
		`{"type":"result","subtype":"error_during_execution","is_error":true,"num_turns":0,"total_cost_usd":0,"retry_after":1,"error":"rate_limit"}`,
		`{"type":"result","subtype":"error","is_error":true,"usage":{"input_tokens":0,"output_tokens":0},"retryable":true}`,
		`{"type":"result","subtype":"success","is_error":true,"structured_output":{},"pre_execution":true}`,
	} {
		for _, schema := range []bool{true, false} {
			result, err := Command(context.Background(), contracts.PhaseInput{}, contracts.CommandResult{Stdout: []byte(output)}, schema)
			if err == nil || result.Outcome != contracts.PhaseResultIndeterminate || result.FailureReason != contracts.ProviderFailureTerminal || len(result.Artifact) != 0 {
				t.Fatal("untrusted retry hint authorized a completed/repairable result")
			}
		}
	}
}
