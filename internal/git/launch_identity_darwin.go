//go:build darwin

package git

import (
	"fmt"
	"syscall"

	"github.com/nysa-company/sf/internal/contracts"
	"golang.org/x/sys/unix"
)

func gitLaunchIdentity(pid int) (contracts.GitMutationLaunch, error) {
	p, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || p == nil || int(p.Proc.P_pid) != pid || p.Proc.P_starttime.Sec <= 0 {
		return contracts.GitMutationLaunch{}, ErrIdentityMismatch
	}
	boot, err := unix.SysctlTimeval("kern.boottime")
	if err != nil || boot.Sec <= 0 {
		return contracts.GitMutationLaunch{}, ErrIdentityMismatch
	}
	pgid, err := syscall.Getpgid(pid)
	if err != nil || pgid != pid {
		return contracts.GitMutationLaunch{}, ErrIdentityMismatch
	}
	return contracts.GitMutationLaunch{PID: pid, PGID: pgid, BootIdentity: fmt.Sprintf("darwin:%d:%d", boot.Sec, boot.Usec), ProcessStartIdentity: fmt.Sprintf("darwin:%d:%d", p.Proc.P_starttime.Sec, p.Proc.P_starttime.Usec)}, nil
}
