//go:build darwin

package processsupervisor

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// processStartIdentity uses the kernel's timeval, including microseconds.
// Unlike ps lstart, this distinguishes processes born in the same second.
func processStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", ErrUnclear
	}
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || process == nil || int(process.Proc.P_pid) != pid || process.Proc.P_starttime.Sec <= 0 {
		return "", ErrUnclear
	}
	return fmt.Sprintf("darwin:%d:%d", process.Proc.P_starttime.Sec, process.Proc.P_starttime.Usec), nil
}

func hostBootIdentity() (string, error) {
	boot, err := unix.SysctlTimeval("kern.boottime")
	if err != nil || boot.Sec <= 0 {
		return "", ErrUnclear
	}
	return fmt.Sprintf("darwin:%d:%d", boot.Sec, boot.Usec), nil
}
