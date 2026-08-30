package git

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

// MutationDrainer is the production restart verifier for sf-git-exec process
// groups. It only signals a group after the durable PID, PGID, boot, and start
// identity all still match; any ambiguity is a quarantine, not a best effort.
type MutationDrainer struct{ SoftDrain, HardDrain time.Duration }

func (d MutationDrainer) DrainGitMutation(ctx context.Context, launch contracts.GitMutationLaunch) error {
	if !validLaunch(launch) {
		return ErrIdentityMismatch
	}
	if d.SoftDrain <= 0 {
		d.SoftDrain = 2 * time.Second
	}
	if d.HardDrain <= 0 {
		d.HardDrain = 2 * time.Second
	}
	gone, exact, err := persistedGitLaunchStatus(launch)
	if err != nil || !exact {
		return ErrIdentityMismatch
	}
	if gone {
		return nil
	}
	_ = syscall.Kill(-launch.PGID, syscall.SIGTERM)
	if waitGitGroup(ctx, launch, d.SoftDrain) {
		return nil
	}
	_, exact, err = persistedGitLaunchStatus(launch)
	if err != nil || !exact {
		return ErrIdentityMismatch
	}
	_ = syscall.Kill(-launch.PGID, syscall.SIGKILL)
	if waitGitGroup(ctx, launch, d.HardDrain) {
		return nil
	}
	return errors.New("git process group drain is unclear")
}
func validLaunch(v contracts.GitMutationLaunch) bool {
	return v.PID > 0 && v.PID == v.PGID && v.BootIdentity != "" && v.ProcessStartIdentity != ""
}
func waitGitGroup(ctx context.Context, launch contracts.GitMutationLaunch, wait time.Duration) bool {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		gone, exact, err := persistedGitLaunchStatus(launch)
		if err != nil || !exact {
			return false
		}
		if gone {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-tick.C:
		}
	}
}

var _ contracts.GitMutationDrainer = MutationDrainer{}
