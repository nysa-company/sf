package processsupervisor

import (
	"path/filepath"
	"runtime"
	"strings"
)

// Private runtime data only. No repository or operator home capability is
// granted. This is the existing trusted-provider baseline, not a claim about
// hostile same-UID process containment or provider billing/network isolation.
func authoringSandbox(stage, executable, home, temporary string) (string, error) {
	if runtime.GOOS != "darwin" {
		return "", ErrUnclear
	}
	for _, path := range []string{stage, executable, home, temporary} {
		if !cleanAbsolute(path) || path == "/" || strings.ContainsAny(path, "\x00\r\n") {
			return "", ErrUnclear
		}
	}
	if filepath.Dir(executable) != stage {
		return "", ErrUnclear
	}
	profile := "(version 1)\n(deny default)\n(allow sysctl-read)\n(allow mach-lookup)\n(allow process-info*)\n(allow signal (target self))\n(allow network*)\n(allow file-read-metadata)\n"
	for _, path := range []string{"/System", "/usr/lib", "/usr/share", "/Library/Apple", "/private/etc", "/dev", stage, home, temporary} {
		profile += "(allow file-read* (subpath " + seatbeltString(path) + "))\n"
	}
	for _, path := range []string{stage, home, temporary} {
		for _, ancestor := range seatbeltAncestors(path) {
			profile += "(allow file-read* (literal " + seatbeltString(ancestor) + "))\n"
		}
	}
	profile += "(allow process-exec (literal " + seatbeltString(executable) + "))\n"
	for _, path := range []string{home, temporary} {
		profile += "(allow file-write* (subpath " + seatbeltString(path) + "))\n"
	}
	return profile + "(allow file-write* (literal \"/dev/null\"))\n", nil
}
