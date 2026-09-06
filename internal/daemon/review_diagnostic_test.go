package daemon

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
)

func TestReviewDiagnosticIsBoundedRedactedHistorical(t *testing.T) {
	v := store.HistoricalReviewDiagnostic{Attempt: 2, TicketVersion: 12, Review: phaseartifact.Reviewer{Decision: phaseartifact.ReviewNeedsOperator, Findings: []string{"Read /private/operator/project; password=hide-me\n\x1b[2J\u202e", strings.Repeat("a", 600), "third", "fourth", "fifth", "excluded"}}}
	got := reviewDiagnosticView(v, redact.NewPolicy("/private/operator", nil))
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"hide-me", "/private/operator", "excluded", `\u001b`, "\u202e"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("unsafe diagnostic contains %q", forbidden)
		}
	}
	findings := got["findings"].([]string)
	if got["historical"] != true || got["truncated"] != true || len(findings) != 5 || len([]rune(findings[1])) != 512 || !strings.Contains(findings[0], "[REDACTED]") {
		t.Fatalf("projection: %+v", got)
	}
}
