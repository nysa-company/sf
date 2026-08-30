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

// repositoryStrictSandboxProfile applies to Git and each Go test binary. The
// executable starts through the one exact literal allowance, then cannot fork
// or exec. Go test wrappers first make themselves a process-group leader, so
// setsid is rejected by POSIX; every such group is durably acknowledged by
// the parent before untrusted test code begins.
func repositoryStrictSandboxProfile(repository, executable string) (string, error) {
	if !repositorySandboxAvailable(repository) || !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return "", ErrUnclear
	}
	return "(version 1)\n" +
		"(allow default)\n" +
		"(deny network*)\n" +
		"(deny file-write* (subpath " + seatbeltString(repository) + "))\n" +
		"(deny process-fork)\n" +
		"(deny process-exec)\n" +
		"(allow process-exec (literal " + seatbeltString(executable) + "))\n", nil
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
	return repositoryStrictSandboxProfile(repository, executable)
}
