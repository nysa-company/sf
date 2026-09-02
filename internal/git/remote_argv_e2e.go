//go:build sf_e2e

package git

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const e2eGitHubRemote = "https://github.com/acme/app.git"

// rewriteE2ERemoteArgv is the sole build-tagged fixture transport seam.  It
// only rewrites the complete, code-owned fixture URL; arbitrary remote text
// and all Run-hook invocations remain untouched.  The returned slice is a
// copy, so callers cannot accidentally mutate an argv owned by a test.
func rewriteE2ERemoteArgv(argv []string) ([]string, error) {
	bare := os.Getenv("SF_E2E_GIT_BARE")
	if bare == "" {
		return argv, nil
	}
	if err := validateE2EBare(bare); err != nil {
		return nil, err
	}
	result := append([]string(nil), argv...)
	remoteIndex := -1
	switch {
	case len(result) == 3 && result[0] == "push":
		remoteIndex = 1
	case len(result) == 4 && result[0] == "ls-remote" && result[1] == "--heads":
		remoteIndex = 2
	case len(result) == 5 && result[0] == "fetch" && result[1] == "--no-write-fetch-head" && result[2] == "--no-tags":
		remoteIndex = 3
	}
	if remoteIndex >= 0 && result[remoteIndex] == e2eGitHubRemote {
		result[remoteIndex] = bare
	}
	return result, nil
}

func validateE2EBare(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return errors.New("SF_E2E_GIT_BARE must be an absolute non-root path")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return errors.New("SF_E2E_GIT_BARE must be canonical and contain no symlink")
	}
	for parent := filepath.Dir(path); ; parent = filepath.Dir(parent) {
		parentInfo, parentErr := os.Lstat(parent)
		if parentErr != nil || parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() || parentInfo.Mode().Perm()&0o022 != 0 || !ownedByCurrentOrRoot(parentInfo) {
			return errors.New("SF_E2E_GIT_BARE has unsafe parent directory")
		}
		if parent == string(filepath.Separator) {
			break
		}
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || !ownedByCurrentOrRoot(info) {
		return errors.New("SF_E2E_GIT_BARE must be a private owner-controlled directory")
	}
	for _, name := range []string{"HEAD", "config"} {
		entry, entryErr := os.Lstat(filepath.Join(path, name))
		if entryErr != nil || entry.Mode()&os.ModeSymlink != 0 || !entry.Mode().IsRegular() || !ownedByCurrentOrRoot(entry) || linkCountIsNotOne(entry) {
			return fmt.Errorf("SF_E2E_GIT_BARE has unsafe %s", name)
		}
	}
	for _, name := range []string{"objects", "refs"} {
		entry, entryErr := os.Lstat(filepath.Join(path, name))
		if entryErr != nil || entry.Mode()&os.ModeSymlink != 0 || !entry.IsDir() || !ownedByCurrentOrRoot(entry) {
			return fmt.Errorf("SF_E2E_GIT_BARE has unsafe %s directory", name)
		}
	}
	return nil
}
