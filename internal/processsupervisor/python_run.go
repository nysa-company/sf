package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/pythonclosure"
	"golang.org/x/sys/unix"
)

// pythonPreflight requires explicit composition; the production factory has no
// prepared root until setup/provisioning acceptance enables that capability.
func (s RepositoryCommandSupervisor) pythonPreflight(spec contracts.CommandSpec) error {
	if _, err := pythonclosure.ParseRecipe(spec.Argv); err != nil || !cleanAbsolute(s.PythonSnapshots) {
		return ErrUnclear
	}
	return nil
}

// runPython uses the typed prepared recipe; neither PATH Python nor arbitrary
// pytest options are execution authority. Empty prepared-root composition fails
// closed before Store acquire through Preflight.
func (s RepositoryCommandSupervisor) runPython(ctx context.Context, claim contracts.RepositoryCommandClaim, spec contracts.CommandSpec, policy executionpolicy.CommandSnapshot, lease contracts.RepositoryCommandLease) (contracts.CommandResult, error) {
	if !repositoryCommandPlatformAvailable(runtime.GOOS) {
		return contracts.CommandResult{}, ErrUnclear
	}
	recipe, err := pythonclosure.ParseRecipe(spec.Argv)
	if ctx == nil || err != nil || lease == nil || spec.Profile != contracts.ProfileGuarded || spec.Stdin != nil || spec.Directory != claim.Worktree || spec.Timeout <= 0 || spec.Timeout > 45*time.Minute || s.SoftDrain > 30*time.Second || s.HardDrain > 30*time.Second || policy.Authorize(spec.Argv) != nil || policy.Digest() != claim.PolicyDigest {
		return contracts.CommandResult{}, ErrUnclear
	}
	argv, err := json.Marshal(spec.Argv)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	sum := sha256.Sum256(argv)
	if claim.CommandDigest != "sha256:"+hex.EncodeToString(sum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	empty := sha256.Sum256(nil)
	binding, err := json.Marshal(struct {
		Argv        []string
		Directory   string
		Timeout     int64
		Profile     contracts.ExecutionProfile
		StdinDigest string
	}{spec.Argv, spec.Directory, spec.Timeout.Nanoseconds(), spec.Profile, "sha256:" + hex.EncodeToString(empty[:])})
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	sum = sha256.Sum256(binding)
	if claim.SpecDigest != "sha256:"+hex.EncodeToString(sum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()
	prepared, executable, digest, err := s.openPythonRuntime(runCtx, spec.Argv)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	artifacts := &stagedNodeArtifacts{runtime: func() { _ = prepared.Close() }}
	started, finished, recorded := false, false, false
	var launch contracts.RepositoryCommandLaunch
	finisher := &repositoryLaunchFinisher{run: func(context.Context) error {
		persist, stop := repositoryLeasePersistenceContext()
		defer stop()
		return lease.FinishRepositoryCommandLaunch(persist, launch)
	}}
	defer func() {
		if !started || finished {
			artifacts.Close()
			return
		}
		if recorded {
			s.drainStagedNodeLifecycle(launch, lease, artifacts, finisher.Finish)
		}
		// An unrecorded, unreaped launch retains private artifacts. Never infer
		// disappearance from failure to persist its process identity.
	}()
	if executable != claim.ExecutablePath || digest != claim.ExecutableDigest {
		return contracts.CommandResult{}, ErrUnclear
	}
	parsed, err := parseRepositoryIdentity(claim)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	worktree, err := s.openAuthenticatedWorktree(claim, parsed)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	defer worktree.Close()
	if s.GitRunner.ExecHelper == "" || s.GitRunner.Home == "" {
		return contracts.CommandResult{}, ErrUnclear
	}
	if err := s.GitRunner.Reauthenticate(runCtx, parsed); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	stage, err := os.MkdirTemp("", "sf-python-launch-")
	if err != nil {
		return contracts.CommandResult{}, err
	}
	artifacts.source = func() { _ = os.RemoveAll(stage) }
	canonicalStage, err := filepath.EvalSymlinks(stage)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	stage = canonicalStage
	scratch := filepath.Join(stage, "scratch")
	if err := os.Mkdir(scratch, 0700); err != nil {
		return contracts.CommandResult{}, err
	}
	bootstrap := filepath.Join(stage, "bootstrap.py")
	if err := os.WriteFile(bootstrap, []byte(pythonclosure.BootstrapSource), 0400); err != nil {
		return contracts.CommandResult{}, err
	}
	snapshot := filepath.Join(s.PythonSnapshots, recipe.EnvironmentDigest[7:])
	paths := PythonSandboxPaths{Worktree: claim.Worktree, Runtime: filepath.Join(snapshot, "runtime"), Dependencies: filepath.Join(snapshot, "dependencies"), Bootstrap: bootstrap, Scratch: scratch, Executable: executable}
	if !pythonPreparedPathsMatch(prepared, paths) {
		return contracts.CommandResult{}, ErrUnclear
	}
	if _, err := RepositoryPythonSandboxProfile(paths); err != nil {
		return contracts.CommandResult{}, err
	}
	scratchFD, err := os.Open(scratch)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer scratchFD.Close()
	monitor, err := startPythonScratchMonitor(runCtx, int(scratchFD.Fd()))
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer monitor.Stop()
	self := s.Executable
	if self == "" {
		self, err = os.Executable()
		if err != nil {
			return contracts.CommandResult{}, err
		}
	}
	self, err = stageRepositoryGate(self)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	priorCleanup := artifacts.source
	artifacts.source = func() { priorCleanup(); _ = os.RemoveAll(filepath.Dir(self)) }
	encoded, err := json.Marshal(paths)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer gateRead.Close()
	defer gateWrite.Close()
	cmd := exec.CommandContext(runCtx, self, "__repository_python_command_gate", string(encoded), recipe.TestPath)
	cmd.Cancel = func() error { return nil }
	cmd.WaitDelay = s.drainHard()
	cmd.Dir = "/"
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + scratch, "TMPDIR=" + scratch}
	cmd.ExtraFiles = []*os.File{gateRead, worktree}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr repositoryBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	begin := time.Now()
	if err := cmd.Start(); err != nil {
		return contracts.CommandResult{}, err
	}
	started = true
	_ = gateRead.Close()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	// Before gate release the only executable is trusted SF. Kill its exact
	// process handle on any launch-persistence failure, then bound the reap.
	refuseLaunch := func() (contracts.CommandResult, error) {
		_ = gateWrite.Close()
		_ = cmd.Process.Kill()
		select {
		case <-wait:
			finished = true
		case <-time.After(s.drainHard() + 250*time.Millisecond):
		}
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	startID, e1 := processStartIdentity(cmd.Process.Pid)
	bootID, e2 := hostBootIdentity()
	launch = contracts.RepositoryCommandLaunch{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: bootID, ProcessStartIdentity: startID}
	if e1 != nil || e2 != nil {
		return refuseLaunch()
	}
	persist, stop := repositoryLeasePersistenceContext()
	err = lease.RecordRepositoryCommandLaunch(persist, launch)
	stop()
	if err != nil {
		return refuseLaunch()
	}
	recorded = true
	if lease.Check(runCtx) != nil || prepared.Revalidate(runCtx) != nil || !pythonPreparedPathsMatch(prepared, paths) || s.GitRunner.Reauthenticate(runCtx, parsed) != nil {
		return refuseLaunch()
	}
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		return refuseLaunch()
	}
	_ = gateWrite.Close()
	waitErr, abortErr, reaped := s.waitPythonCommand(runCtx, wait, launch, monitor.failure)
	monitor.Stop()
	select {
	case err := <-monitor.failure:
		abortErr = errors.Join(abortErr, pythonScratchAbort(err))
	default:
	}
	if !reaped || s.ensureGone(launch, false) != nil {
		_ = lease.Quarantine()
		return contracts.CommandResult{}, errors.Join(abortErr, ErrUnclear)
	}
	if _, err := pythonclosure.InspectScratchDirectoryFD(context.Background(), int(scratchFD.Fd())); err != nil {
		abortErr = errors.Join(abortErr, pythonScratchAbort(err))
	}
	if err := finisher.Finish(runCtx); err != nil {
		_ = lease.Quarantine()
		return contracts.CommandResult{}, errors.Join(abortErr, ErrUnclear)
	}
	finished = true
	if errors.Is(abortErr, ErrUnclear) {
		_ = lease.Quarantine()
		return contracts.CommandResult{}, abortErr
	}
	if stdout.overflow || stderr.overflow {
		return contracts.CommandResult{}, errors.New("repository command output exceeds limit")
	}
	result := contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), Duration: time.Since(begin), Observed: true, ObservedAt: time.Now().UTC()}
	if waitErr != nil {
		result.ExitCode = -1
		var exit *exec.ExitError
		if errors.As(waitErr, &exit) {
			result.ExitCode = exit.ExitCode()
			if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() && status.Signal() == syscall.SIGXFSZ {
				abortErr = errors.Join(abortErr, contracts.ErrRepositoryCommandResourceLimit)
			}
		}
	}
	return result, errors.Join(abortErr, waitErr)
}

func pythonPreparedPathsMatch(prepared *pythonclosure.Prepared, paths PythonSandboxPaths) bool {
	if prepared == nil {
		return false
	}
	for fd, path := range map[int]string{prepared.RuntimeFD(): paths.Runtime, prepared.DependenciesFD(): paths.Dependencies} {
		var retained, named unix.Stat_t
		if unix.Fstat(fd, &retained) != nil || unix.Lstat(path, &named) != nil || named.Mode&unix.S_IFMT != unix.S_IFDIR || retained.Dev != named.Dev || retained.Ino != named.Ino {
			return false
		}
	}
	return true
}
