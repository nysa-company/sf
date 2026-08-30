//go:build darwin

package git

import (
	"fmt"
	"github.com/nysa-company/sf/internal/contracts"
	"golang.org/x/sys/unix"
	"syscall"
)

func persistedGitLaunchStatus(v contracts.GitMutationLaunch) (gone, exact bool, err error) {
	boot, e := unix.SysctlTimeval("kern.boottime")
	if e != nil || boot.Sec <= 0 {
		return false, false, ErrIdentityMismatch
	}
	if v.BootIdentity != fmt.Sprintf("darwin:%d:%d", boot.Sec, boot.Usec) {
		return true, true, nil
	}
	if e = syscall.Kill(v.PID, 0); e == syscall.ESRCH {
		if syscall.Kill(-v.PGID, 0) == syscall.ESRCH {
			return true, true, nil
		}
		return false, false, ErrIdentityMismatch
	}
	if e != nil {
		return false, false, e
	}
	p, e := unix.SysctlKinfoProc("kern.proc.pid", v.PID)
	if e != nil || p == nil {
		return false, false, ErrIdentityMismatch
	}
	pgid, e := syscall.Getpgid(v.PID)
	if e != nil || pgid != v.PGID {
		return false, false, ErrIdentityMismatch
	}
	return false, fmt.Sprintf("darwin:%d:%d", p.Proc.P_starttime.Sec, p.Proc.P_starttime.Usec) == v.ProcessStartIdentity, nil
}
