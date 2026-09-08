package processsupervisor

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/pythonclosure"
)

// waitPythonCommand owns no Wait goroutine or lease. Its caller starts exactly
// one cmd.Wait, supplies that channel, and must independently prove disappearance
// and durably Finish before setting CommandResult.Observed. An abort error is
// separate from the process exit: resource aborts cannot become red test evidence.
// Reaped is false when no Wait result arrived, even if the leader disappeared.
func (s RepositoryCommandSupervisor) waitPythonCommand(ctx context.Context, wait <-chan error, launch contracts.RepositoryCommandLaunch, resource <-chan error) (waitErr, abortErr error, reaped bool) {
	if ctx == nil || wait == nil || launch.PID <= 0 || launch.PGID != launch.PID || launch.ProcessStartIdentity == "" || launch.BootIdentity == "" || s.SoftDrain > 30*time.Second || s.HardDrain > 30*time.Second {
		return nil, ErrUnclear, false
	}
	select {
	case waitErr, reaped = <-wait:
		if !reaped {
			return nil, ErrUnclear, false
		}
		// A reported resource failure takes precedence even if exit wins the
		// select. The owner must also inspect scratch after normal completion.
		select {
		case err := <-resource:
			return waitErr, pythonScratchAbort(err), true
		default:
			return waitErr, ctx.Err(), true
		}
	case <-ctx.Done():
		abortErr = ctx.Err()
	case err := <-resource:
		abortErr = pythonScratchAbort(err)
	}
	if err := signalPythonLaunch(launch, syscall.SIGTERM); err != nil {
		return nil, errors.Join(abortErr, err), false
	}
	soft := time.NewTimer(s.drainSoft())
	defer soft.Stop()
	select {
	case waitErr, reaped = <-wait:
		if !reaped {
			return nil, errors.Join(abortErr, ErrUnclear), false
		}
		return waitErr, abortErr, true
	case <-soft.C:
	}
	if err := signalPythonLaunch(launch, syscall.SIGKILL); err != nil {
		return nil, errors.Join(abortErr, err), false
	}
	hard := time.NewTimer(s.drainHard() + 250*time.Millisecond)
	defer hard.Stop()
	select {
	case waitErr, reaped = <-wait:
		if !reaped {
			return nil, errors.Join(abortErr, ErrUnclear), false
		}
		return waitErr, abortErr, true
	case <-hard.C:
		return nil, errors.Join(abortErr, ErrUnclear), false
	}
}

func pythonScratchAbort(err error) error {
	if errors.Is(err, pythonclosure.ErrLimit) {
		return contracts.ErrRepositoryCommandResourceLimit
	}
	// Inspection failure is not evidence that a quota was exceeded.
	return ErrUnclear
}

// Only the exact recorded single-process group may be signaled. A vanished
// leader is not authority to signal a possibly recycled group; the wait owner
// still has to prove disappearance before any durable Finish or release.
func signalPythonLaunch(launch contracts.RepositoryCommandLaunch, signal syscall.Signal) error {
	if launch.PID <= 0 || launch.PGID != launch.PID || launch.ProcessStartIdentity == "" || launch.BootIdentity == "" || (signal != syscall.SIGTERM && signal != syscall.SIGKILL) {
		return ErrUnclear
	}
	boot, err := hostBootIdentity()
	if err != nil || boot != launch.BootIdentity {
		return ErrUnclear
	}
	start, err := processStartIdentity(launch.PID)
	if err != nil {
		if errors.Is(syscall.Kill(-launch.PGID, 0), syscall.ESRCH) {
			return nil
		}
		return ErrUnclear
	}
	pgid, err := syscall.Getpgid(launch.PID)
	if err != nil || start != launch.ProcessStartIdentity || pgid != launch.PGID {
		return ErrUnclear
	}
	return signalGroup(launch.PGID, signal)
}
