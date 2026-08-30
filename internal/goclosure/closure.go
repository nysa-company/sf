// Package goclosure defines the bounded local dependency contract for the
// sole guarded repository-command recipe. It is deliberately parser-light:
// Go itself validates a vendored graph under -mod=vendor; this package only
// decides whether a repository may reach that local compiler boundary.
package goclosure

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

const MaxGoModBytes int64 = 1 << 20
const MaxVendorModulesBytes int64 = 8 << 20

var (
	ErrInvalid    = errors.New("invalid Go module marker")
	ErrUnvendored = errors.New("external Go dependencies require checked-in vendor closure")
)

// Validate returns useVendor for a dependency-free module or a checked-in,
// non-symlinked vendor/modules.txt closure. It never inspects an ambient Go
// cache and callers must still disable network/module mutation.
func Validate(worktree string) (useVendor bool, err error) {
	goMod := filepath.Join(worktree, "go.mod")
	info, err := os.Lstat(goMod)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() > MaxGoModBytes {
		return false, ErrInvalid
	}
	contents, err := os.ReadFile(goMod)
	if err != nil {
		return false, ErrInvalid
	}
	var module, goVersion, requires bool
	for _, raw := range strings.Split(string(contents), "\n") {
		line := strings.TrimSpace(strings.SplitN(raw, "//", 2)[0])
		switch {
		case strings.HasPrefix(line, "module ") && strings.TrimSpace(strings.TrimPrefix(line, "module ")) != "":
			module = true
		case strings.HasPrefix(line, "go ") && strings.TrimSpace(strings.TrimPrefix(line, "go ")) != "":
			goVersion = true
		case line == "require (" || strings.HasPrefix(line, "require "):
			requires = true
		case strings.HasPrefix(line, "replace ") || strings.HasPrefix(line, "exclude ") || strings.HasPrefix(line, "retract "):
			return false, ErrInvalid
		}
	}
	if !module || !goVersion {
		return false, ErrInvalid
	}
	if !requires {
		return false, nil
	}
	vendor := filepath.Join(worktree, "vendor")
	vendorInfo, err := os.Lstat(vendor)
	if err != nil || vendorInfo.Mode()&os.ModeSymlink != 0 || !vendorInfo.IsDir() {
		return false, ErrUnvendored
	}
	modules := filepath.Join(vendor, "modules.txt")
	entry, err := os.Lstat(modules)
	if err != nil || entry.Mode()&os.ModeSymlink != 0 || !entry.Mode().IsRegular() || entry.Size() == 0 || entry.Size() > MaxVendorModulesBytes {
		return false, ErrUnvendored
	}
	return true, nil
}
