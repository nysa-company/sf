package store

import (
	"fmt"
	"strings"
	"testing"
)

func TestPostbuildRepairCandidateHandoffAndRecovery(t *testing.T) {
	for _, scenario := range []struct {
		refresh  bool
		negative string
	}{{}, {refresh: true}, {negative: "unrelated"}, {negative: "stale"}} {
		t.Run(fmt.Sprintf("refresh_%t_%s", scenario.refresh, scenario.negative), func(t *testing.T) {
			db, ctx, failure := postbuildFailureFixture(t, 1)
			entry, err := db.TransitionPostbuildRepair(ctx, PostbuildRepairRequest{PostbuildFailureRequest: failure, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)})
			if err != nil {
				t.Fatal(err)
			}
			assertPostbuildCandidateHandoff(t, db, ctx, failure.Ref, entry.Version, failure.Fence, scenario.refresh, true, scenario.negative)
		})
	}
}
