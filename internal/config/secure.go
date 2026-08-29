package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// PrepareChannel creates only the selected channel's private local
// directories. Existing symlinked or foreign-owned targets fail closed.
func PrepareChannel(paths ChannelPaths) error {
	if !filepath.IsAbs(paths.Root) || filepath.Clean(paths.Root) != paths.Root {
		return errors.New("channel root must be an absolute clean path")
	}
	children := []string{
		filepath.Dir(paths.Database), filepath.Dir(paths.Machine), filepath.Dir(paths.Socket),
		paths.Logs, paths.Events, paths.Worktrees, paths.Backups,
	}
	for _, path := range children {
		if !pathWithin(paths.Root, path) {
			return errors.New("channel path escapes its channel root")
		}
	}
	for _, path := range append([]string{paths.Root}, children...) {
		if err := ensurePrivateDirectory(path); err != nil {
			return err
		}
	}
	return nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func ensurePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("directory path must be absolute")
	}
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	root := volume + string(filepath.Separator)
	trimmed := strings.TrimPrefix(strings.TrimPrefix(clean, root), volume)
	current := root
	for _, part := range strings.Split(trimmed, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("create channel directory: %w", err)
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return fmt.Errorf("inspect channel directory: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if allowedSystemDirectoryAlias(current) {
				continue
			}
			return errors.New("channel directory contains a symlink")
		}
		if !info.IsDir() {
			return errors.New("channel path component is not a directory")
		}
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return fmt.Errorf("inspect channel directory: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !ok || stat.Uid != uint32(os.Getuid()) {
		return errors.New("channel directory is not a current-user real directory")
	}
	if err := os.Chmod(clean, 0o700); err != nil {
		return fmt.Errorf("secure channel directory: %w", err)
	}
	return nil
}

func allowedSystemDirectoryAlias(path string) bool {
	return runtime.GOOS == "darwin" && (path == "/var" || path == "/tmp")
}
