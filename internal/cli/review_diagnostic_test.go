package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestReviewDiagnosticRenderingIsHistoricalAndSafe(t *testing.T) {
	var output bytes.Buffer
	err := renderEvidence(&output, map[string]any{"review_diagnostic": map[string]any{"available": true, "decision": "needs_operator", "attempt": 2, "ticket_version": 12, "reviewed_head": strings.Repeat("a", 40), "findings": []any{"Inspect missing fixture\n\x1b[2J"}, "truncated": true}})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"Recorded review: needs_operator", "historical, not approval", "Reviewer finding: Inspect missing fixture", "Review diagnostic truncated"} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("missing %q: %s", expected, output.String())
		}
	}
	if strings.Contains(output.String(), "\x1b") {
		t.Fatal("terminal control leaked")
	}
	output.Reset()
	if err := renderReviewDiagnostic(&output, map[string]any{"available": false}); err != nil || !strings.Contains(output.String(), "no verdict inferred") {
		t.Fatalf("unavailable: %s %v", output.String(), err)
	}
}
