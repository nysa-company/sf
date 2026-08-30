//go:build linux

package ghrunner

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func processStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", errors.New("invalid process")
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) == "" {
		return "", errors.New("boot identity unavailable")
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", errors.New("process identity unavailable")
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return "", errors.New("process identity unavailable")
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) <= 19 || fields[19] == "" {
		return "", errors.New("process identity unavailable")
	}
	return "linux:" + strings.TrimSpace(string(boot)) + ":" + fields[19], nil
}

func hostBootIdentity() (string, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(boot)) == "" {
		return "", errors.New("boot identity unavailable")
	}
	return "linux:" + strings.TrimSpace(string(boot)), nil
}
