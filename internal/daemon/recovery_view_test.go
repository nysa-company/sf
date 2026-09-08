package daemon

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestRecoveryViewDoesNotTurnRegistrationIntoEditPermission(t *testing.T) {
	for _, state := range []domain.State{domain.StateBlocked, domain.StatePaused, domain.StateStopping, domain.StateCancelling, domain.StateCancelled} {
		for _, registered := range []bool{false, true} {
			evidence := map[string]any{}
			if registered {
				evidence["worktree"] = map[string]any{"path": "/private/not-for-guidance"}
			}
			got := recoveryView(store.Ticket{State: state, BlockedCode: "postbuild_command_failed"}, evidence)
			if got == nil || got["writer_safety"] != "not_checked" || got["worktree_registered"] != registered {
				t.Fatalf("state=%s registered=%t: %#v", state, registered, got)
			}
			for _, value := range got {
				if text, ok := value.(string); ok && strings.Contains(text, "/private/not-for-guidance") {
					t.Fatal("guidance copied worktree path")
				}
			}
		}
	}
	if got := recoveryView(store.Ticket{State: domain.StateBuilding, BlockedCode: "postbuild_command_failed"}, nil); got != nil {
		t.Fatal("historical blocker presented as current failure")
	}
}

func TestRecoveryViewDoesNotBlameTestsForEveryFailure(t *testing.T) {
	got := recoveryView(store.Ticket{State: domain.StateBlocked, BlockedCode: "postbuild_command_failed"}, nil)
	if !strings.Contains(got["cause"].(string), "does not establish whether the implementation or the test is wrong") {
		t.Fatalf("cause=%v", got["cause"])
	}
	got = recoveryView(store.Ticket{State: domain.StateBlocked, BlockedCode: "untrusted\x1b[2Jpayload"}, nil)
	if strings.Contains(got["cause"].(string), "payload") {
		t.Fatal("untrusted blocker copied into explanation")
	}
}
