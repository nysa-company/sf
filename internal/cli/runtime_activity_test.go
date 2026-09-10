package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestHumanRuntimeActivityIsHistoricalAndActionable(t *testing.T) {
	var output bytes.Buffer
	activity := map[string]any{"available": true, "observations": []any{map[string]any{"outcome": "readiness_failed", "observed_at": "2026-09-05T12:00:00Z", "observed_ticket_version": float64(7)}}}
	if err := renderRuntimeActivity(&output, activity); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "historical, not current state") || !strings.Contains(output.String(), "Inspect doctor") {
		t.Fatal(output.String())
	}
	output.Reset()
	if err := renderRuntimeActivity(&output, map[string]any{"available": false}); err != nil || output.Len() != 0 {
		t.Fatalf("unavailable invented history: %s err=%v", output.String(), err)
	}
}

func TestHumanRepositoryFailureHasSafeAction(t *testing.T) {
	var output bytes.Buffer
	activity := map[string]any{"available": true, "observations": []any{map[string]any{"outcome": "repository_preflight_failed", "observed_at": "2026-09-09T12:00:00Z", "observed_ticket_version": float64(7), "error": "untrusted-secret-value"}}}
	if err := renderRuntimeActivity(&output, activity); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "doctor --repo") || !strings.Contains(output.String(), "historical, not current state") || strings.Contains(output.String(), "untrusted-secret-value") {
		t.Fatal(output.String())
	}
}

func TestHumanRuntimeUnavailableAndAdmissionReason(t *testing.T) {
	var output bytes.Buffer
	if err := renderRuntimeActivity(&output, map[string]any{"available": false, "reason": "runtime_not_composed", "summary": "untrusted-secret-value"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "providers qualify --help") || strings.Contains(output.String(), "untrusted-secret-value") {
		t.Fatal(output.String())
	}
	output.Reset()
	activity := map[string]any{"available": true, "observations": []any{map[string]any{"outcome": "worker_failed", "reason": "provider_binding_unavailable", "summary": "untrusted-secret-value"}}}
	if err := renderRuntimeActivity(&output, activity); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "provider_binding_unavailable") || strings.Contains(output.String(), "untrusted-secret-value") {
		t.Fatal(output.String())
	}
}

func TestTicketListExplainsIdleRuntimeWithoutTickets(t *testing.T) {
	var output bytes.Buffer
	parent := map[string]any{"channel": "dev", "runtime_activity": map[string]any{"available": false, "reason": "runtime_not_composed"}}
	if err := renderTickets(&output, parent, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Workflow runtime: unavailable") || !strings.Contains(output.String(), "No tickets.") {
		t.Fatal(output.String())
	}
}
