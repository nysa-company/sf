//go:build linux

package processsupervisor

import (
	"fmt"
	"os"
	"strings"
)

// processStartIdentity combines the kernel boot ID with /proc's start-tick
// field (22). PID reuse cannot retain this tuple across a process lifetime.
func processStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", ErrUnclear
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) == "" {
		return "", ErrUnclear
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", ErrUnclear
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return "", ErrUnclear
	}
	fields := strings.Fields(string(stat)[end+1:]) // field 3 begins at index 0
	if len(fields) <= 19 || fields[19] == "" {     // field 22: starttime
		return "", ErrUnclear
	}
	return "linux:" + strings.TrimSpace(string(boot)) + ":" + fields[19], nil
}

func hostBootIdentity() (string, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) == "" {
		return "", ErrUnclear
	}
	return "linux:" + strings.TrimSpace(string(boot)), nil
}
