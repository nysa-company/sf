//go:build darwin

package ghrunner

import (
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
)

func processStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", errors.New("invalid process")
	}
	p, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || p == nil || int(p.Proc.P_pid) != pid || p.Proc.P_starttime.Sec <= 0 {
		return "", errors.New("process identity unavailable")
	}
	return fmt.Sprintf("darwin:%d:%d", p.Proc.P_starttime.Sec, p.Proc.P_starttime.Usec), nil
}

func hostBootIdentity() (string, error) {
	p, err := unix.SysctlTimeval("kern.boottime")
	if err != nil || p.Sec <= 0 {
		return "", errors.New("boot identity unavailable")
	}
	return fmt.Sprintf("darwin:%d:%d", p.Sec, p.Usec), nil
}
