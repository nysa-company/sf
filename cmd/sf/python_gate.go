package main

import (
	"bytes"
	"encoding/json"
	"os"
	"syscall"

	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/pythonclosure"
)

// repositoryPythonGate runs only in the separately started launch child. The
// parent supplies FD3 after recording its Store launch and FD4 for the verified
// worktree. This internal mode is not repository-command policy admission.
func repositoryPythonGate(argv []string) (bool, int) {
	if len(argv) < 2 || argv[1] != "__repository_python_command_gate" {
		return false, 0
	}
	if len(argv) != 4 || len(argv[2]) > 32768 || !pythonclosure.ValidTestPath(argv[3]) {
		return true, 125
	}
	var paths processsupervisor.PythonSandboxPaths
	if json.Unmarshal([]byte(argv[2]), &paths) != nil {
		return true, 125
	}
	canonical, err := json.Marshal(paths)
	if err != nil || !bytes.Equal(canonical, []byte(argv[2])) {
		return true, 125
	}
	gate := os.NewFile(3, "python-launch-gate")
	var one [1]byte
	if _, err := gate.Read(one[:]); err != nil || one[0] != 1 {
		gate.Close()
		return true, 125
	}
	gate.Close()
	if syscall.Fchdir(4) != nil {
		return true, 125
	}
	cwd, err := os.Getwd()
	if err != nil || cwd != paths.Worktree {
		return true, 125
	}
	profile, err := processsupervisor.RepositoryPythonSandboxProfile(paths)
	if err != nil || processsupervisor.ApplyRepositoryPythonResourceLimits() != nil || processsupervisor.ApplyRepositoryTestSandbox(profile) != nil {
		return true, 125
	}
	syscall.CloseOnExec(4)
	args := []string{paths.Executable, "-I", "-S", "-B", paths.Bootstrap, paths.Dependencies, paths.Worktree, argv[3]}
	env := []string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + paths.Scratch, "TMPDIR=" + paths.Scratch}
	if syscall.Exec(paths.Executable, args, env) != nil {
		return true, 126
	}
	return true, 0
}
