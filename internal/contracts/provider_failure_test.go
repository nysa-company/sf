package contracts

import "testing"

func TestProviderFailureReasonsAreClosed(t *testing.T) {
	for _, reason := range []ProviderFailureReason{ProviderFailureCommand, ProviderFailureExit, ProviderFailureOutput, ProviderFailureProtocol, ProviderFailureTerminal, ProviderFailureBinding, ProviderFailureAdapter} {
		if !ValidProviderFailureReason(reason) {
			t.Fatalf("rejected closed reason %q", reason)
		}
	}
	for _, reason := range []ProviderFailureReason{"", "password=secret", "nonzero_exit\n", "NONZERO_EXIT", "rate_limit"} {
		if ValidProviderFailureReason(reason) {
			t.Fatalf("accepted unbounded reason %q", reason)
		}
	}
}
