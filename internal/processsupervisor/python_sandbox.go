package processsupervisor

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

type PythonSandboxPaths struct {
	Worktree, Runtime, Dependencies, Bootstrap, Scratch, Executable string
}

// RepositoryPythonSandboxProfile is for the prepared single-process pytest
// recipe. The caller must authenticate the worktree and prepared manifests,
// hold the Store lease and set resource limits before releasing its gate.
// This does not admit Python commands by itself. Scratch is never a project or
// runtime subtree; file-size/aggregate scratch limits are a separate launch
// requirement, not a property claimed by Seatbelt's pathname rules.
func RepositoryPythonSandboxProfile(p PythonSandboxPaths) (string, error) {
	if !repositorySandboxAvailable(p.Worktree) {
		return "", ErrUnclear
	}
	for _, value := range []string{p.Worktree, p.Runtime, p.Dependencies, p.Bootstrap, p.Scratch, p.Executable} {
		if !cleanAbsolute(value) || value == string(filepath.Separator) {
			return "", ErrUnclear
		}
		resolved, err := filepath.EvalSymlinks(value)
		if err != nil || resolved != value {
			return "", ErrUnclear
		}
	}
	within := func(child, parent string) bool {
		return child == parent || strings.HasPrefix(child, parent+string(filepath.Separator))
	}
	if p.Executable == p.Runtime || !within(p.Executable, p.Runtime) {
		return "", ErrUnclear
	}
	for _, readRoot := range []string{p.Worktree, p.Runtime, p.Dependencies} {
		if within(p.Scratch, readRoot) || within(readRoot, p.Scratch) {
			return "", ErrUnclear
		}
	}
	if within(p.Bootstrap, p.Scratch) {
		return "", ErrUnclear
	}
	for _, directory := range []string{p.Worktree, p.Runtime, p.Dependencies, p.Scratch} {
		info, err := os.Lstat(directory)
		if err != nil || !info.IsDir() || !trustedOwner(info) {
			return "", ErrUnclear
		}
		if directory != p.Worktree && (info.Mode().Perm()&0077 != 0 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0) {
			return "", ErrUnclear
		}
	}
	if err := authenticateRepositorySourceExecutable(p.Executable); err != nil {
		return "", ErrUnclear
	}
	info, err := os.Lstat(p.Bootstrap)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || !trustedOwner(info) || info.Size() != int64(len(pythonclosure.BootstrapSource)) {
		return "", ErrUnclear
	}
	digest, err := executableFileDigest(p.Bootstrap)
	if err != nil || digest != pythonclosure.BootstrapDigest() {
		return "", ErrUnclear
	}
	profile := "(version 1)\n(deny default)\n" +
		"(allow file-read-metadata)\n" +
		"(allow file-read* (literal \"/\") (subpath \"/System\") (subpath \"/usr/lib\") (literal \"/dev/urandom\") (literal \"/dev/random\"))\n" +
		"(allow file-read* file-write* (literal \"/dev/null\"))\n" +
		"(allow file-read* file-write* (subpath " + seatbeltString(p.Scratch) + "))\n" +
		"(allow file-read* (subpath " + seatbeltString(p.Worktree) + ") (subpath " + seatbeltString(p.Runtime) + ") (subpath " + seatbeltString(p.Dependencies) + ") (literal " + seatbeltString(p.Bootstrap) + "))\n" +
		"(deny network*)\n(deny process-fork)\n(deny process-exec)\n" +
		"(allow process-exec (literal " + seatbeltString(p.Executable) + "))\n" +
		"(allow sysctl-read)\n(allow mach-lookup)\n"
	return profile, nil
}
