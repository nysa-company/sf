package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestHumanStatusLabelsSubmissionAgeAndDeadline(t *testing.T) {
	budget := map[string]any{"available": true, "age": "20m0s", "remaining": "40m0s", "deadline_at": "2026-09-05T13:00:00Z"}
	for _, data := range []map[string]any{
		{"ticket": map[string]any{"ticket": "SF-test", "state": "paused"}, "budget_clock": budget},
		{"channel": "dev", "tickets": []any{map[string]any{"ticket": "SF-test", "state": "queued", "budget_clock": budget}}},
	} {
		var output bytes.Buffer
		if err := renderHumanData(&output, data); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "Deadline remaining: 40m0s (includes queue/pause time)") {
			t.Fatalf("output=%s", output.String())
		}
		if _, ok := data["ticket"]; ok && !strings.Contains(output.String(), "Age since submission: 20m0s") {
			t.Fatalf("age missing: %s", output.String())
		}
	}
}
