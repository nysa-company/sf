package processsupervisor

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const repositorySandboxExec = "/usr/bin/sandbox-exec"

type repositorySandboxPaths struct {
	Repository string
	Worktree   string
	GitFile    string
	CommonDir  string
	Home       string
	Temporary  string
	Executable string
	// Toolchain is a private, authenticated closure (for example staged Node
	// and npm).  It is intentionally distinct from Executable: scripts may
	// exec descendants, but their runtime files must still be a fixed staged
	// tree rather than an ambient package-manager installation.
	Toolchain        string
	DependencyPaths  []string
	AllowProcessTree bool
}

// repositoryStrictSandboxProfile applies to Git and each Go test binary. The
// executable starts through the one exact literal allowance, then cannot fork
// or exec. Go test wrappers first make themselves a process-group leader, so
// setsid is rejected by POSIX; every such group is durably acknowledged by
// the parent before untrusted test code begins.
func repositoryStrictSandboxProfile(repository, executable string) (string, error) {
	return repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: repository, Worktree: repository, GitFile: filepath.Join(repository, ".git"), CommonDir: filepath.Join(repository, ".git"), Executable: executable})
}

// repositoryStrictSandboxProfileFor is deliberately default-deny.  The
// guarded profile is not an autonomous claim (ADR 0002 remains negative), but
// a credential-free verification process still must not be handed ambient home
// access or a writable worktree.  The Go driver remains outside this profile
// only while compiling the one exact, CGO-disabled recipe; every test binary
// is entered through the durable gate below.
func repositoryStrictSandboxProfileFor(paths repositorySandboxPaths) (string, error) {
	if !repositorySandboxAvailable(paths.Repository) || !cleanAbsolute(paths.Repository) || !cleanAbsolute(paths.Worktree) || !cleanAbsolute(paths.Executable) {
		return "", ErrUnclear
	}
	for _, path := range []string{paths.GitFile, paths.CommonDir, paths.Home, paths.Temporary, paths.Toolchain} {
		if path != "" && !cleanAbsolute(path) {
			return "", ErrUnclear
		}
	}
	profile := "(version 1)\n" +
		"(deny default)\n" +
		// Dynamic loaders and Go's runtime need these code-owned system paths.
		// Seatbelt also requires metadata access to the filesystem root while
		// resolving a literal allowlist; this grants no descendant reads.
		"(allow file-read* (literal \"/\"))\n" +
		"(allow file-read* (subpath \"/System\"))\n" +
		"(allow file-read* (subpath \"/usr/lib\"))\n" +
		"(allow file-read* (subpath \"/usr/bin\"))\n" +
		"(allow file-read* (subpath \"/usr/share\"))\n" +
		"(allow file-read* (subpath \"/Library/Apple\"))\n" +
		"(allow file-read* (subpath \"/Library/Developer\"))\n" +
		"(allow file-read* (subpath \"/dev\"))\n" +
		"(allow file-read* (subpath \"/etc\"))\n" +
		"(allow mach-lookup)\n" +
		"(allow sysctl-read)\n" +
		"(allow file-read* (literal " + seatbeltString(paths.Repository) + "))\n" +
		"(allow file-read* (literal " + seatbeltString(paths.Worktree) + "))\n" +
		"(allow file-read* (subpath " + seatbeltString(paths.Repository) + "))\n" +
		"(allow file-read* (subpath " + seatbeltString(paths.Worktree) + "))\n" +
		"(allow file-read* (literal " + seatbeltString(paths.Executable) + "))\n" +
		"(allow file-write* (literal \"/dev/null\"))\n"
	if paths.Toolchain != "" {
		profile += "(allow file-read* (subpath " + seatbeltString(paths.Toolchain) + "))\n" +
			"(allow file-map-executable (subpath " + seatbeltString(paths.Toolchain) + "))\n"
	}
	for _, path := range paths.DependencyPaths {
		if !cleanAbsolute(path) {
			return "", ErrUnclear
		}
		profile += "(allow file-read* (literal " + seatbeltString(path) + "))\n" +
			"(allow file-map-executable (literal " + seatbeltString(path) + "))\n"
		// dyld resolves versioned dylib symlinks through their exact library
		// directory. The closure builder supplies only parent directories of
		// authenticated dependency files; no Homebrew prefix is allowed.
		profile += "(allow file-read* (subpath " + seatbeltString(filepath.Dir(path)) + "))\n" +
			"(allow file-map-executable (subpath " + seatbeltString(filepath.Dir(path)) + "))\n"
	}
	// A default-deny profile needs metadata permission for each ancestor while
	// resolving an otherwise exact path.  Do not replace this with a broad home
	// allowlist: only the named command paths receive ancestor traversal.
	for _, path := range []string{paths.Repository, paths.Worktree, paths.GitFile, paths.CommonDir, paths.Home, paths.Temporary, paths.Executable, paths.Toolchain} {
		for _, ancestor := range seatbeltAncestors(path) {
			profile += "(allow file-read* (literal " + seatbeltString(ancestor) + "))\n"
		}
	}
	for _, path := range paths.DependencyPaths {
		for _, ancestor := range seatbeltAncestors(path) {
			profile += "(allow file-read* (literal " + seatbeltString(ancestor) + "))\n"
		}
	}
	if paths.GitFile != "" {
		profile += "(allow file-read* (literal " + seatbeltString(paths.GitFile) + "))\n"
	}
	if paths.CommonDir != "" {
		profile += "(allow file-read* (subpath " + seatbeltString(paths.CommonDir) + "))\n"
	}
	for _, path := range []string{paths.Home, paths.Temporary} {
		if path != "" {
			profile += "(allow file-read* (subpath " + seatbeltString(path) + "))\n" +
				"(allow file-write* (subpath " + seatbeltString(path) + "))\n"
		}
	}
	// The source and worktree remain readable for tests, but their Git control
	// plane never becomes writable.  This matters because normal linked
	// worktrees place .git and common-dir outside the base repository.
	profile += "(deny file-write* (subpath " + seatbeltString(paths.Repository) + "))\n" +
		"(deny file-write* (subpath " + seatbeltString(paths.Worktree) + "))\n"
	if paths.GitFile != "" {
		profile += "(deny file-write* (literal " + seatbeltString(paths.GitFile) + "))\n"
	}
	if paths.CommonDir != "" {
		profile += "(deny file-write* (subpath " + seatbeltString(paths.CommonDir) + "))\n"
	}
	profile += "(deny network*)\n"
	if paths.AllowProcessTree {
		// npm scripts necessarily use Node, a shell, and (for many normal test
		// runners) further descendants.  Seatbelt restrictions are inherited by
		// each child, so this admits a process tree without admitting repository,
		// Git-control, credential, host-write, or network authority.  Lifecycle
		// ambiguity is still quarantined by the supervisor; this is guarded-only,
		// not an ADR 0002 autonomous-containment assertion.
		profile += "(allow process-fork)\n(allow process-exec)\n"
	} else {
		profile += "(deny process-fork)\n" +
			"(deny process-exec)\n" +
			"(allow process-exec (literal " + seatbeltString(paths.Executable) + "))\n"
	}
	return profile, nil
}

func cleanAbsolute(path string) bool { return filepath.IsAbs(path) && filepath.Clean(path) == path }

func seatbeltAncestors(path string) []string {
	if path == "" || !cleanAbsolute(path) {
		return nil
	}
	seen := map[string]bool{}
	var result []string
	for current := filepath.Dir(path); current != "/"; current = filepath.Dir(current) {
		if !seen[current] {
			seen[current] = true
			result = append(result, current)
		}
	}
	return result
}

func repositorySandboxAvailable(repository string) bool {
	if runtime.GOOS != "darwin" || !filepath.IsAbs(repository) || filepath.Clean(repository) != repository {
		return false
	}
	info, err := os.Stat(repositorySandboxExec)
	return err == nil && !info.IsDir()
}

func seatbeltString(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

// RepositoryTestSandboxProfile is called by the sf gate immediately before it
// execs the Go-generated test binary. The target path comes from the trusted
// Go driver; a test cannot alter this profile after the wrapper has become a
// process-group leader and before it is sandboxed.
func RepositoryTestSandboxProfile(repository, executable string) (string, error) {
	if strings.TrimSpace(repository) == "" {
		return "", errors.New("repository sandbox repository is required")
	}
	paths := repositorySandboxPaths{
		Repository: repository,
		Worktree:   os.Getenv("SF_REPOSITORY_SANDBOX_WORKTREE"),
		GitFile:    os.Getenv("SF_REPOSITORY_SANDBOX_GIT_FILE"),
		CommonDir:  os.Getenv("SF_REPOSITORY_SANDBOX_COMMON_DIR"),
		Home:       os.Getenv("SF_REPOSITORY_SANDBOX_HOME"),
		Temporary:  os.Getenv("SF_REPOSITORY_SANDBOX_TMP"),
		Executable: executable,
	}
	if paths.Worktree == "" {
		paths.Worktree = repository
	}
	return repositoryStrictSandboxProfileFor(paths)
}
