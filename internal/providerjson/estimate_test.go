package providerjson

import "testing"

func TestReportedCostEstimateExactAndOptional(t *testing.T) {
	for _, tc := range []struct {
		number string
		want   int64
	}{
		{"0", 0}, {"0.0074268", 7427}, {"0.022203", 22203},
		{"0.0000001", 1}, {"1e-7", 1}, {"1.25e2", 125000000},
		{"9223372036854.775807", 9223372036854775807},
	} {
		t.Run(tc.number, func(t *testing.T) {
			got, err := ReportedCostEstimate([]byte(`{"total_cost_usd":` + tc.number + `}`))
			if err != nil || got == nil || *got != tc.want {
				t.Fatalf("got %v, err %v", got, err)
			}
		})
	}
	got, err := ReportedCostEstimate([]byte(`{"usage":{"tokens":123}}`))
	if err != nil || got != nil {
		t.Fatal("missing cost must remain unknown")
	}
}

func TestReportedCostEstimateRejectsMalformedOrOverflow(t *testing.T) {
	for _, number := range []string{`null`, `"0"`, `true`, `-1`, `-0`, `1e999`, `NaN`, `1/2`, `9223372036854.775808`, `1e99`} {
		if got, err := ReportedCostEstimate([]byte(`{"total_cost_usd":` + number + `}`)); err == nil || got != nil {
			t.Fatalf("accepted %s", number)
		}
	}
	if _, err := ReportedCostEstimate([]byte(`{"total_cost_usd":0,"total_cost_usd":0}`)); err == nil {
		t.Fatal("duplicate accepted")
	}
}
