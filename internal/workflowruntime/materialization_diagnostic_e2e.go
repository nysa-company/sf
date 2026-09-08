//go:build sf_e2e

package workflowruntime

import (
	"fmt"
	"os"
)

// Only controller-owned stage labels, never error text or provider content.
func reportVerificationCheckpointFailure(stage string) {
	fmt.Fprintln(os.Stderr, "SF_TEST_VERIFICATION_CHECKPOINT="+verificationCheckpointCategory(stage))
}

func verificationCheckpointCategory(stage string) string {
	switch stage {
	case "command", "outcome", "parent", "policy", "evidence", "commit":
		return stage
	default:
		return "other"
	}
}
