package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestHumanEstimatedAccountingNeverClaimsZeroOrHardCap(t *testing.T) {
	var output bytes.Buffer
	data := map[string]any{"ticket": map[string]any{"ticket": "SF-test", "state": "planning"}, "evidence": map[string]any{
		"provider_accounting": map[string]any{"mode": "reported_estimate_v1", "sf_launch_limit": float64(16), "request_timeout": "45m0s", "actual_total_known": false, "hard_dollar_cap": false},
	}}
	if err := renderHumanData(&output, data); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"actual total unknown", "not a hard dollar cap", "16 SF launches", "45m0s", "CLI-internal calls"} {
		if !strings.Contains(output.String(), text) {
			t.Fatalf("missing %q: %s", text, output.String())
		}
	}
	if strings.Contains(output.String(), "$0") {
		t.Fatal("unknown spend displayed as zero")
	}
}
