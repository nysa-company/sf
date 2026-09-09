package statemachine

import (
	"errors"
	"testing"
)

func TestPostbuildRepairRequiresEveryIndependentGuard(t *testing.T) {
	spec := loadApprovedSpec(t)
	required := []string{"failed_postbuild_evidence_exact", "prepublication_only", "retained_worktree_exact", "correction_available", "no_live_writer"}
	guards := map[string]bool{}
	for _, guard := range required {
		guards[guard] = true
	}
	selected, err := spec.Select("building", "postbuild_repair", guards)
	if err != nil || selected.ID != "postbuild_repair" || selected.To != "building" || len(selected.Invalidates) != 0 || len(selected.AllowedEffects) != 0 {
		t.Fatalf("repair transition=%+v err=%v", selected, err)
	}
	for _, guard := range required {
		guards[guard] = false
		if _, err := spec.Select("building", "postbuild_repair", guards); !errors.Is(err, ErrNoTransition) {
			t.Fatalf("missing %s admitted: %v", guard, err)
		}
		guards[guard] = true
	}
	for _, state := range []string{"paused", "blocked", "publishing", "waiting_ci", "done"} {
		if _, err := spec.Select(state, "postbuild_repair", guards); !errors.Is(err, ErrNoTransition) {
			t.Fatalf("repair admitted from %s: %v", state, err)
		}
	}
}
