package processsupervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
)

const repositoryOutputLimit = 64 << 10
const repositoryInputLimit = 1 << 20

// RepositoryCommandSupervisor is the sole os/exec boundary for exact,
// credential-free repository commands. The child is held behind the sf gate
// until its Store lease has durably recorded the process identity.
type RepositoryCommandSupervisor struct {
	Executable           string
	SoftDrain, HardDrain time.Duration
}

func (s RepositoryCommandSupervisor) Run(ctx context.Context, claim contracts.RepositoryCommandClaim, spec contracts.CommandSpec, policy executionpolicy.CommandSnapshot, lease contracts.RepositoryCommandLease) (contracts.CommandResult, error) {
	if lease == nil || len(spec.Argv) == 0 || spec.Directory != claim.Worktree || spec.Timeout <= 0 || policy.Authorize(spec.Argv) != nil || policy.Digest() != claim.PolicyDigest {
		return contracts.CommandResult{}, ErrUnclear
	}
	argvBytes, err := json.Marshal(spec.Argv)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	argvSum := sha256.Sum256(argvBytes)
	if claim.CommandDigest != "sha256:"+hex.EncodeToString(argvSum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	if !filepath.IsAbs(spec.Directory) || filepath.Clean(spec.Directory) != spec.Directory {
		return contracts.CommandResult{}, ErrUnclear
	}
	resolved, err := resolveFixedExecutable(spec.Argv[0])
	if err != nil {
		return contracts.CommandResult{}, err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	if _, err = authenticateExecutable(resolved); err != nil {
		return contracts.CommandResult{}, err
	}
	if actual, err := filepath.EvalSymlinks(spec.Directory); err != nil || actual != claim.Worktree {
		return contracts.CommandResult{}, ErrUnclear
	}
	var input []byte
	if spec.Stdin != nil {
		input, err = io.ReadAll(io.LimitReader(spec.Stdin, repositoryInputLimit+1))
		if err != nil || len(input) > repositoryInputLimit {
			return contracts.CommandResult{}, errors.New("repository command stdin exceeds limit")
		}
	}
	stdinSum := sha256.Sum256(input)
	specBytes, err := json.Marshal(struct {
		Argv        []string
		Directory   string
		Timeout     int64
		Profile     contracts.ExecutionProfile
		StdinDigest string
	}{spec.Argv, spec.Directory, spec.Timeout.Nanoseconds(), spec.Profile, "sha256:" + hex.EncodeToString(stdinSum[:])})
	if err != nil {
		return contracts.CommandResult{}, err
	}
	specSum := sha256.Sum256(specBytes)
	if claim.SpecDigest != "sha256:"+hex.EncodeToString(specSum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()
	self := s.Executable
	if self == "" {
		self, err = os.Executable()
		if err != nil {
			return contracts.CommandResult{}, err
		}
	}
	home, err := os.MkdirTemp("", "sf-command-home-")
	if err != nil {
		return contracts.CommandResult{}, err
	}
	tmp, err := os.MkdirTemp("", "sf-command-tmp-")
	if err != nil {
		_ = os.RemoveAll(home)
		return contracts.CommandResult{}, err
	}
	defer os.RemoveAll(home)
	defer os.RemoveAll(tmp)
	env, err := executionpolicy.MinimalEnvironment(home, tmp)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer gateWrite.Close()
	worktreeFD, err := os.Open(spec.Directory)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer worktreeFD.Close()
	worktreeInfo, err := worktreeFD.Stat()
	if err != nil || !worktreeInfo.IsDir() {
		return contracts.CommandResult{}, ErrUnclear
	}
	argv := append([]string{"__repository_command_gate", resolved}, spec.Argv[1:]...)
	cmd := exec.Command(self, argv...)
	// Start from a fixed directory. The gate changes directory through the
	// inherited FD 4 only after the durable launch record is committed, so a
	// rename/replace of the path cannot redirect the command.
	cmd.Dir = string(filepath.Separator)
	cmd.Env = env
	cmd.Stdin = bytes.NewReader(input)
	cmd.ExtraFiles = []*os.File{gateRead}
	cmd.ExtraFiles = append(cmd.ExtraFiles, worktreeFD)
	// The opened directory remains inherited as FD 4 for the gate; the
	// process starts in the canonical path after the preflight identity check.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr repositoryBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	if err := cmd.Start(); err != nil {
		gateRead.Close()
		return contracts.CommandResult{}, err
	}
	gateRead.Close()
	startID, e1 := processStartIdentity(cmd.Process.Pid)
	bootID, e2 := hostBootIdentity()
	launch := contracts.RepositoryCommandLaunch{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: bootID, ProcessStartIdentity: startID}
	if e1 != nil || e2 != nil || lease.RecordRepositoryCommandLaunch(ctx, launch) != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = s.ensureGone(launch)
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = s.ensureGone(launch)
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	_ = gateWrite.Close()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	var waitErr error
	select {
	case waitErr = <-wait:
	case <-runCtx.Done():
		_ = signalGroup(launch.PGID, syscall.SIGTERM)
		select {
		case waitErr = <-wait:
		case <-time.After(s.drainSoft()):
			_ = signalGroup(launch.PGID, syscall.SIGKILL)
			waitErr = <-wait
		}
	}
	if err := s.ensureGone(launch); err != nil {
		return contracts.CommandResult{}, fmt.Errorf("repository ensure gone: %w", err)
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	if stdout.overflow || stderr.overflow {
		return contracts.CommandResult{}, errors.New("repository command output exceeds limit")
	}
	result := contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), Duration: time.Since(started), ExitCode: 0}
	if waitErr != nil {
		if ee, ok := waitErr.(*exec.ExitError); ok {
			result.ExitCode = ee.ExitCode()
		} else {
			result.ExitCode = -1
		}
		if runCtx.Err() != nil {
			return result, runCtx.Err()
		}
		return result, waitErr
	}
	return result, nil
}

func resolveFixedExecutable(name string) (string, error) {
	if filepath.IsAbs(name) {
		return name, nil
	}
	if name == "" || filepath.Base(name) != name {
		return "", exec.ErrNotFound
	}
	for _, dir := range []string{"/usr/bin", "/bin", "/usr/sbin", "/sbin"} {
		candidate := filepath.Join(dir, name)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}
	return "", exec.ErrNotFound
}
func (s RepositoryCommandSupervisor) drainSoft() time.Duration {
	if s.SoftDrain > 0 {
		return s.SoftDrain
	}
	return 2 * time.Second
}
func (s RepositoryCommandSupervisor) ensureGone(l contracts.RepositoryCommandLaunch) error {
	if l.PID <= 0 || l.PGID != l.PID || l.BootIdentity == "" || l.ProcessStartIdentity == "" {
		return ErrUnclear
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		if got, e := processStartIdentity(l.PID); e == nil && got == l.ProcessStartIdentity { /* still live */
		} else if e := syscall.Kill(-l.PGID, 0); e == syscall.ESRCH {
			return nil
		} else if e != nil {
			return ErrUnclear
		}
		if time.Now().After(deadline) {
			return ErrUnclear
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type repositoryBuffer struct {
	bytes.Buffer
	overflow bool
}

func (b *repositoryBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > repositoryOutputLimit {
		b.overflow = true
		n := repositoryOutputLimit - b.Len()
		if n > 0 {
			_, _ = b.Buffer.Write(p[:n])
		}
		return len(p), nil
	}
	return b.Buffer.Write(p)
}

// RepositoryCommandDrainer verifies the complete persisted launch identity
// before signalling. An absent or mismatched identity is never guessed.
type RepositoryCommandDrainer struct{ SoftDrain, HardDrain time.Duration }

func (d RepositoryCommandDrainer) DrainRepositoryCommand(ctx context.Context, l contracts.RepositoryCommandLaunch) error {
	if !validRepositoryLaunch(l) {
		return ErrUnclear
	}
	if boot, err := hostBootIdentity(); err != nil || boot != l.BootIdentity {
		return ErrUnclear
	}
	start, err := processStartIdentity(l.PID)
	if err != nil {
		if syscall.Kill(-l.PGID, 0) == syscall.ESRCH {
			return nil
		}
		return ErrUnclear
	}
	if start != l.ProcessStartIdentity {
		return ErrUnclear
	}
	if err := signalGroup(l.PGID, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		return err
	}
	soft := d.SoftDrain
	if soft <= 0 {
		soft = 2 * time.Second
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(soft):
	}
	if syscall.Kill(-l.PGID, 0) == nil {
		_ = signalGroup(l.PGID, syscall.SIGKILL)
	}
	hard := d.HardDrain
	if hard <= 0 {
		hard = 2 * time.Second
	}
	deadline := time.NewTimer(hard)
	defer deadline.Stop()
	for {
		if syscall.Kill(-l.PGID, 0) == syscall.ESRCH {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return ErrUnclear
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func validRepositoryLaunch(l contracts.RepositoryCommandLaunch) bool {
	return l.PID > 0 && l.PGID == l.PID && l.BootIdentity != "" && l.ProcessStartIdentity != ""
}
