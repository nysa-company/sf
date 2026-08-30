// Package runtimeassets resolves the private executables that form one sf
// channel's local runtime bundle. It never consults PATH: a running sf binary
// may compose only its exact sibling helper for the same stable/dev channel.
package runtimeassets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/nysa-company/sf/internal/domain"
)

var ErrUnsafeBundle = errors.New("runtime executable bundle is unsafe")

// Core is the minimum executable set needed before publication is enabled.
// Git credentials and SSH helpers deliberately remain outside this boundary;
// resolving the core does not make remote mutation reachable.
type Core struct {
	Executable string
	GitExec    string
}

// CurrentCore resolves the currently running executable and its exact
// channel-matched Git gate. The returned paths are canonical and absolute.
func CurrentCore(channel domain.Channel) (Core, error) {
	executable, err := os.Executable()
	if err != nil {
		return Core{}, fmt.Errorf("%w: locate primary executable", ErrUnsafeBundle)
	}
	return ResolveCore(channel, executable)
}

// ResolveCore is exported for composition tests and explicit bundle probes.
// It rejects a renamed primary, a cross-channel helper, symlink leaf, unsafe
// ownership/mode/link count, and any lookup outside the primary's directory.
func ResolveCore(channel domain.Channel, executable string) (Core, error) {
	primaryName, helperName, err := names(channel)
	if err != nil {
		return Core{}, err
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || filepath.Base(executable) != primaryName {
		return Core{}, fmt.Errorf("%w: primary executable does not match channel", ErrUnsafeBundle)
	}
	primary, err := authenticate(executable)
	if err != nil {
		return Core{}, fmt.Errorf("%w: primary executable", err)
	}
	helperCandidate := filepath.Join(filepath.Dir(primary), helperName)
	helper, err := authenticate(helperCandidate)
	if err != nil {
		return Core{}, fmt.Errorf("%w: Git execution helper", err)
	}
	if filepath.Dir(primary) != filepath.Dir(helper) || filepath.Base(helper) != helperName {
		return Core{}, fmt.Errorf("%w: helper escaped executable bundle", ErrUnsafeBundle)
	}
	return Core{Executable: primary, GitExec: helper}, nil
}

func names(channel domain.Channel) (string, string, error) {
	switch channel {
	case domain.ChannelStable:
		return "sf", "sf-git-exec", nil
	case domain.ChannelDev:
		return "sf-dev", "sf-git-exec-dev", nil
	default:
		return "", "", fmt.Errorf("%w: invalid channel", ErrUnsafeBundle)
	}
}

func authenticate(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: executable is missing or symlinked", ErrUnsafeBundle)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(canonical) || filepath.Clean(canonical) != canonical {
		return "", fmt.Errorf("%w: executable path is not canonical", ErrUnsafeBundle)
	}
	canonicalInfo, err := os.Lstat(canonical)
	if err != nil || !safeExecutable(canonicalInfo) || !secureParents(filepath.Dir(canonical)) {
		return "", fmt.Errorf("%w: executable metadata is unsafe", ErrUnsafeBundle)
	}
	file, err := os.Open(canonical)
	if err != nil {
		return "", fmt.Errorf("%w: open executable", ErrUnsafeBundle)
	}
	opened, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil || !os.SameFile(canonicalInfo, opened) {
		return "", fmt.Errorf("%w: executable changed while opening", ErrUnsafeBundle)
	}
	if closeErr != nil {
		return "", fmt.Errorf("%w: close executable", ErrUnsafeBundle)
	}
	return canonical, nil
}

func safeExecutable(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0 &&
		info.Mode().Perm()&0o022 == 0 && (int(stat.Uid) == os.Getuid() || stat.Uid == 0) && stat.Nlink == 1
}

func secureParents(directory string) bool {
	for {
		info, err := os.Lstat(directory)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
			return false
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || (int(stat.Uid) != os.Getuid() && stat.Uid != 0) {
			return false
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return true
		}
		directory = parent
	}
}
