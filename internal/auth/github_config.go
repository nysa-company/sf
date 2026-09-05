package auth

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"unicode"
	"unicode/utf8"
)

// GitHubConfigPath selects gh's macOS configuration path without filesystem
// writes or credential reads. Callers must pass this path explicitly, even
// when missing, to avoid selecting a different default account. Existing
// directories are separately authenticated by ExistingGitHubConfigDirectory.
func GitHubConfigPath(home, explicit, xdg string) (string, error) {
	path := explicit
	if path == "" {
		if xdg != "" {
			if !safeGitHubConfigPath(xdg) {
				return "", errors.New("XDG_CONFIG_HOME must be a clean absolute directory path")
			}
			path = filepath.Join(xdg, "gh")
		} else if home != "" {
			path = filepath.Join(home, ".config", "gh")
		} else {
			return "", nil
		}
	}
	if !safeGitHubConfigPath(path) {
		return "", errors.New("GitHub configuration directory must be a clean absolute directory path")
	}
	return path, nil
}

// ExistingGitHubConfigDirectory validates without creating configuration.
// Empty means missing, not permission to select a different account.
func ExistingGitHubConfigDirectory(home, explicit, xdg string) (string, error) {
	path, err := GitHubConfigPath(home, explicit, xdg)
	if err != nil || path == "" {
		return "", err
	}
	return existingAuthenticationDirectory(path)
}

func existingAuthenticationDirectory(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return "", errors.New("authentication directory must be a real owner-controlled directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() && stat.Uid != 0 {
		return "", errors.New("authentication directory ownership is unsafe")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !safeGitHubConfigPath(canonical) {
		return "", errors.New("authentication directory could not be authenticated")
	}
	current, err := os.Lstat(canonical)
	if err != nil || !os.SameFile(info, current) {
		return "", errors.New("authentication directory changed during validation")
	}
	return canonical, nil
}

func safeGitHubConfigPath(path string) bool {
	if len(path) > 4096 || !utf8.ValidString(path) || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return false
	}
	for _, r := range path {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
