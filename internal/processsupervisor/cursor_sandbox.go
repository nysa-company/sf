package processsupervisor

import (
	"path/filepath"
	"runtime"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// Cursor intentionally leaves its local-worker server alive after print mode.
// This code-owned wrapper retains the group's leader through scoped cleanup.
// Every caller argument is a quoted positional value, never shell source.
// Cleanup addresses only this launch's private HOME/worktree socket namespace.
const cursorOwnedLifecycleScript = `"$1" -p "$2" "$3" "${@:4}"
result=$?
"$1" -p "$2" "$3" local-worker kill >/dev/null 2>/dev/null
cleanup=$?
if [ "$cleanup" -ne 0 ]; then exit 125; fi
exit "$result"`

// cursorRoleSandboxProfile is a physical filesystem boundary, not a claim
// that trusted hooks are disabled or network/model calls are confined. Only
// supervisor-owned canonical private paths may be supplied. Qualification
// must measure this profile with the pinned CLI before production admission.
func cursorRoleSandboxProfile(input contracts.PhaseInput, stage, home string) (string, error) {
	if runtime.GOOS != "darwin" || input.Profile != contracts.ProfileGuarded {
		return "", ErrUnclear
	}
	paths := []string{input.Worktree, stage, home}
	for _, p := range paths {
		if !cleanAbsolute(p) || p == "/" || strings.ContainsAny(p, "\x00\r\n") {
			return "", ErrUnclear
		}
	}
	// Private writable runtime data must never enclose or overlap the checkout.
	for _, p := range []string{stage, home} {
		if p == input.Worktree || strings.HasPrefix(input.Worktree, p+"/") || strings.HasPrefix(p, input.Worktree+"/") {
			return "", ErrUnclear
		}
	}
	profile := "(version 1)\n(deny default)\n(allow sysctl-read)\n(allow mach-lookup)\n(allow process-info*)\n(allow process-fork)\n(allow signal (target self))\n(allow network*)\n(allow file-read-metadata)\n"
	// macOS dyld reads the root directory itself while locating its cache.
	// A literal grants no reads beneath it.
	profile += "(allow file-read* (literal \"/\"))\n"
	for _, p := range append(paths, "/bin/sh", "/bin/bash", "/usr/bin/env") {
		for _, ancestor := range seatbeltAncestors(p) {
			profile += "(allow file-read* (literal " + seatbeltString(ancestor) + "))\n"
		}
	}
	for _, p := range []string{"/System", "/usr/lib", "/usr/share", "/Library/Apple", "/private/etc", "/dev", stage, home, input.Worktree} {
		profile += "(allow file-read* (subpath " + seatbeltString(p) + "))\n"
	}
	// The pinned Cursor launcher resolves its sibling runtime using these
	// exact system helpers. No general /usr/bin execution is granted.
	for _, p := range []string{"/bin/sh", "/bin/bash", "/usr/bin/env", "/usr/bin/basename", "/usr/bin/dirname", "/bin/realpath"} {
		profile += "(allow file-read* process-exec (literal " + seatbeltString(p) + "))\n"
	}
	profile += "(allow process-exec (subpath " + seatbeltString(stage) + "))\n" +
		"(allow file-write* (subpath " + seatbeltString(home) + "))\n" +
		"(allow file-write* (literal \"/dev/null\"))\n"
	write := input.Phase == domain.PhaseBuild || input.Phase == domain.PhaseVerification
	if !write && input.Phase != domain.PhaseReview && input.Phase != domain.PhasePlanning {
		return "", ErrUnclear
	}
	if write {
		if len(input.AllowedPaths) == 0 || len(input.AllowedPaths) > 256 {
			return "", ErrUnclear
		}
		for _, p := range input.AllowedPaths {
			if p == "." || p == ".." || p == "" || filepath.IsAbs(p) || filepath.Clean(p) != p || strings.HasPrefix(p, "../") || strings.ContainsAny(p, "\x00\r\n\\") {
				return "", ErrUnclear
			}
			profile += "(allow file-write* (subpath " + seatbeltString(filepath.Join(input.Worktree, p)) + "))\n"
		}
	}
	// Even a broad approved source directory cannot authorize control files.
	for _, p := range []string{".git", ".sf", ".cursor", ".claude"} {
		profile += "(deny file-write* (subpath " + seatbeltString(filepath.Join(input.Worktree, p)) + "))\n"
	}
	return profile, nil
}
