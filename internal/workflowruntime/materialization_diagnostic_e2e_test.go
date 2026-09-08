//go:build sf_e2e

package workflowruntime

import "testing"

func TestVerificationCheckpointDiagnosticIsClosed(t *testing.T) {
	for _, stage := range []string{"command", "outcome", "parent", "policy", "evidence", "commit"} {
		if verificationCheckpointCategory(stage) != stage {
			t.Fatal("known stage not classified")
		}
	}
	for _, unknown := range []string{"", "secret-content", "command\nsecret", "/private/path"} {
		if verificationCheckpointCategory(unknown) != "other" {
			t.Fatal("unknown content leaked")
		}
	}
}
