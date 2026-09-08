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
