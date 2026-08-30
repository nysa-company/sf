package processsupervisor

import (
	"errors"
	"syscall"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestRapidPIDReuseWithSameSecondDisplayIdentityNeverMatches(t *testing.T) {
	// Both processes could render as the same ps lstart second. The durable
	// kernel identity includes microseconds (Darwin) or start ticks (Linux), so
	// the replacement cannot pass the no-signal verification gate.
	launch := contracts.ProviderLaunch{PID: 4242, PGID: 4242, BootIdentity: "darwin:boot", ProcessStartIdentity: "darwin:1724934896:101", Worktree: "/tmp/worktree"}
	if persistedIdentityMatches(launch, "darwin:1724934896:102", 4242) {
		t.Fatal("rapidly reused PID was accepted for signalling")
	}
	if persistedIdentityMatches(launch, launch.ProcessStartIdentity, 4243) {
		t.Fatal("foreign process group was accepted for signalling")
	}
}

func TestRecoveryLivenessRules(t *testing.T) {
	if !bootIdentityChanged("linux:old-boot", "linux:new-boot") {
		t.Fatal("reboot did not prove the old process namespace dead")
	}
	if !missingLeaderGroupGone(syscall.ESRCH, syscall.ESRCH) {
		t.Fatal("missing leader with no group members did not release")
	}
	if missingLeaderGroupGone(syscall.ESRCH, nil) || missingLeaderGroupGone(syscall.ESRCH, errors.New("foreign group")) {
		t.Fatal("surviving or foreign group was released")
	}
}
