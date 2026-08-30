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
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

const repositoryOutputLimit = 64 << 10
const repositoryInputLimit = 1 << 20

// RepositoryCommandSupervisor is the sole os/exec boundary for exact,
// credential-free repository commands. The child is held behind the sf gate
// until its Store lease has durably recorded the process identity.
type RepositoryCommandSupervisor struct {
	Executable           string
	SoftDrain, HardDrain time.Duration
	// beforeWorktreeOpen is test-only synchronization for the preflight/open
	// replacement race.  It is intentionally unexported so production callers
	// cannot turn it into a launch hook.
	beforeWorktreeOpen func()
}

func (s RepositoryCommandSupervisor) Run(ctx context.Context, claim contracts.RepositoryCommandClaim, spec contracts.CommandSpec, policy executionpolicy.CommandSnapshot, lease contracts.RepositoryCommandLease) (contracts.CommandResult, error) {
	if lease == nil || spec.Profile != contracts.ProfileGuarded || len(spec.Argv) == 0 || spec.Directory != claim.Worktree || spec.Timeout <= 0 || spec.Timeout > 45*time.Minute || s.SoftDrain > 30*time.Second || s.HardDrain > 30*time.Second || policy.Authorize(spec.Argv) != nil || policy.Digest() != claim.PolicyDigest {
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
	resolved, err = filepath.EvalSymlinks(resolved)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	if _, err = authenticateExecutable(resolved); err != nil {
		return contracts.CommandResult{}, err
	}
	if claim.ExecutablePath != resolved {
		return contracts.CommandResult{}, ErrUnclear
	}
	fileBytes, err := os.ReadFile(resolved)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	fileSum := sha256.Sum256(fileBytes)
	if claim.ExecutableDigest != "sha256:"+hex.EncodeToString(fileSum[:]) {
		return contracts.CommandResult{}, ErrUnclear
	}
	identity, err := parseRepositoryIdentity(claim)
	if err != nil {
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
	staged, err := stageExecutable(resolved, claim.ExecutableDigest)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	defer os.RemoveAll(filepath.Dir(staged))
	env, err := executionpolicy.MinimalEnvironment(home, tmp)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer gateWrite.Close()
	worktreeFD, err := s.openAuthenticatedWorktree(claim, identity)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	defer worktreeFD.Close()
	// Reauthenticate the rest of the persisted Git identity after the opened
	// directory FD has proven exactly which worktree was selected.  The gate
	// subsequently changes directory only through that FD.
	if err := (gitboundary.Runner{}).Reauthenticate(runCtx, identity); err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	argv := append([]string{"__repository_command_gate", staged}, spec.Argv[1:]...)
	cmd := exec.CommandContext(runCtx, self, argv...)
	// Context cancellation arms WaitDelay, which closes supervisor-owned pipe
	// endpoints even if an escaped child inherited stdout/stderr.
	cmd.Cancel = func() error { return nil }
	cmd.WaitDelay = s.drainHard()
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
		_ = s.ensureGone(launch, false)
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	// The launch record and gate are deliberately separate system calls. Check
	// the exact Store fence once more immediately before unblocking the child so
	// a pause/cancel/take that raced the record cannot start stale work.
	if err := lease.Check(ctx); err != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = s.ensureGone(launch, false)
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	_ = gateWrite.Close()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	monitorStop := make(chan struct{})
	escaped := make(chan bool, 1)
	go monitorRepositoryGroup(launch, monitorStop, escaped)
	monitorStopped := false
	defer func() {
		if !monitorStopped {
			close(monitorStop)
		}
	}()
	var waitErr error
	select {
	case waitErr = <-wait:
	case <-runCtx.Done():
		if pgid, err := syscall.Getpgid(launch.PID); err != nil || pgid != launch.PGID {
			_ = lease.Quarantine()
			return contracts.CommandResult{}, ErrUnclear
		}
		_ = signalGroup(launch.PGID, syscall.SIGTERM)
		select {
		case waitErr = <-wait:
		case <-time.After(s.drainSoft()):
			if pgid, err := syscall.Getpgid(launch.PID); err != nil || pgid != launch.PGID {
				_ = lease.Quarantine()
				return contracts.CommandResult{}, ErrUnclear
			}
			_ = signalGroup(launch.PGID, syscall.SIGKILL)
			select {
			case waitErr = <-wait:
			case <-time.After(s.drainHard() + 250*time.Millisecond):
				_ = lease.Quarantine()
				return contracts.CommandResult{}, ErrUnclear
			}
		}
	}
	close(monitorStop)
	monitorStopped = true
	groupEscaped := <-escaped
	if err := s.ensureGone(launch, groupEscaped); err != nil {
		return contracts.CommandResult{}, fmt.Errorf("repository ensure gone: %w", err)
	}
	finishCtx, finishCancel := repositoryLeasePersistenceContext()
	err = lease.FinishRepositoryCommandLaunch(finishCtx, launch)
	finishCancel()
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	if stdout.overflow || stderr.overflow {
		return contracts.CommandResult{}, errors.New("repository command output exceeds limit")
	}
	result := contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), Duration: time.Since(started), ExitCode: 0, Observed: true}
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
	if name == "" {
		return "", exec.ErrNotFound
	}
	tool := filepath.Base(name)
	if tool != "git" && tool != "go" {
		return "", exec.ErrNotFound
	}
	for _, candidate := range approvedRepositoryExecutables(tool) {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		if filepath.IsAbs(name) {
			provided, err := filepath.EvalSymlinks(name)
			if err != nil || provided != resolved {
				continue
			}
			return resolved, nil
		}
		if name == tool {
			return resolved, nil
		}
	}
	return "", exec.ErrNotFound
}

// approvedRepositoryExecutables is intentionally code-owned. A digest binds
// the selected binary to an intent; this list prevents a private executable
// merely named "git" or "go" from becoming eligible for that binding.
func approvedRepositoryExecutables(tool string) []string {
	switch tool {
	case "git":
		return []string{"/usr/bin/git", "/usr/local/bin/git", "/opt/homebrew/bin/git"}
	case "go":
		return []string{"/usr/local/go/bin/go", "/usr/local/bin/go", "/opt/homebrew/bin/go"}
	default:
		return nil
	}
}
func (s RepositoryCommandSupervisor) drainSoft() time.Duration {
	if s.SoftDrain > 0 {
		return s.SoftDrain
	}
	return 2 * time.Second
}
func (s RepositoryCommandSupervisor) drainHard() time.Duration {
	if s.HardDrain > 0 {
		return s.HardDrain
	}
	return 2 * time.Second
}

func stageExecutable(path, expectedDigest string) (string, error) {
	source, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer source.Close()
	dir, err := os.MkdirTemp("", "sf-command-exec-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	destination := filepath.Join(dir, "command")
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		_ = os.RemoveAll(dir)
		return "", err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, hash), io.LimitReader(source, 64<<20+1))
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n > 64<<20 || sourceSizeOver(path, 64<<20) || expectedDigest != "sha256:"+hex.EncodeToString(hash.Sum(nil)) {
		_ = os.RemoveAll(dir)
		return "", ErrUnclear
	}
	return destination, nil
}

func sourceSizeOver(path string, limit int64) bool {
	info, err := os.Stat(path)
	return err != nil || info.Size() > limit
}

func parseRepositoryIdentity(claim contracts.RepositoryCommandClaim) (gitboundary.Identity, error) {
	var identity gitboundary.Identity
	decoder := json.NewDecoder(strings.NewReader(claim.WorktreeIdentity))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return gitboundary.Identity{}, err
	}
	if identity.Repository != claim.Repository || identity.Worktree != claim.Worktree || identity.BaseRef != claim.BaseRef || identity.BaseHead != claim.BaseSHA || identity.HeadRef != claim.Branch {
		return gitboundary.Identity{}, ErrUnclear
	}
	for _, value := range []string{identity.Repository, identity.Worktree, identity.GitFile, identity.CommonDir, identity.Origin, identity.PushOrigin, identity.BaseRef, identity.BaseHead, identity.HeadRef, identity.ConfigHash, identity.HooksHash} {
		if strings.TrimSpace(value) == "" {
			return gitboundary.Identity{}, ErrUnclear
		}
	}
	for _, pair := range [][2]uint64{{identity.RepositoryDev, identity.RepositoryIno}, {identity.WorktreeDev, identity.WorktreeIno}, {identity.GitFileDev, identity.GitFileIno}, {identity.CommonDirDev, identity.CommonDirIno}, {identity.PushOriginDev, identity.PushOriginIno}} {
		if pair[0] == 0 || pair[1] == 0 {
			return gitboundary.Identity{}, ErrUnclear
		}
	}
	for _, path := range []string{identity.Repository, identity.Worktree, identity.GitFile, identity.CommonDir} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return gitboundary.Identity{}, ErrUnclear
		}
	}
	return identity, nil
}

func matchesDirectoryIdentity(info os.FileInfo, dev, ino uint64) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && uint64(stat.Dev) == dev && uint64(stat.Ino) == ino
}

func (s RepositoryCommandSupervisor) openAuthenticatedWorktree(claim contracts.RepositoryCommandClaim, identity gitboundary.Identity) (*os.File, error) {
	if s.beforeWorktreeOpen != nil {
		s.beforeWorktreeOpen()
	}
	opened, err := os.Open(claim.Worktree)
	if err != nil {
		return nil, err
	}
	info, err := opened.Stat()
	if err != nil || !info.IsDir() || !matchesDirectoryIdentity(info, identity.WorktreeDev, identity.WorktreeIno) {
		_ = opened.Close()
		return nil, ErrUnclear
	}
	return opened, nil
}
func repositoryLeasePersistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// monitorRepositoryGroup catches the recorded leader changing process group
// while it is still observable. It cannot, by itself, prove that a fast
// double-forked descendant did not escape after the leader exits; callers must
// not mistake this local check for hostile same-UID containment.
func monitorRepositoryGroup(l contracts.RepositoryCommandLaunch, stop <-chan struct{}, escaped chan<- bool) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if pgid, err := syscall.Getpgid(l.PID); err == nil && pgid != l.PGID {
			escaped <- true
			return
		}
		select {
		case <-stop:
			escaped <- false
			return
		case <-ticker.C:
		}
	}
}

func (s RepositoryCommandSupervisor) ensureGone(l contracts.RepositoryCommandLaunch, groupEscaped bool) error {
	if l.PID <= 0 || l.PGID != l.PID || l.BootIdentity == "" || l.ProcessStartIdentity == "" {
		return ErrUnclear
	}
	if groupEscaped {
		return ErrUnclear
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		if got, e := processStartIdentity(l.PID); e == nil {
			if got != l.ProcessStartIdentity {
				return ErrUnclear
			}
		} else if syscall.Kill(-l.PGID, 0) == syscall.ESRCH {
			return nil
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
	if d.SoftDrain > 30*time.Second || d.HardDrain > 30*time.Second {
		return ErrUnclear
	}
	if boot, err := hostBootIdentity(); err != nil || boot != l.BootIdentity {
		return ErrUnclear
	}
	start, err := processStartIdentity(l.PID)
	if err != nil {
		return ErrUnclear
	}
	if start != l.ProcessStartIdentity {
		return ErrUnclear
	}
	pgid, err := syscall.Getpgid(l.PID)
	if err != nil || pgid != l.PGID {
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
	if current, err := syscall.Getpgid(l.PID); err == nil && current == l.PGID && syscall.Kill(-l.PGID, 0) == nil {
		_ = signalGroup(l.PGID, syscall.SIGKILL)
	}
	hard := d.HardDrain
	if hard <= 0 {
		hard = 2 * time.Second
	}
	deadline := time.NewTimer(hard)
	defer deadline.Stop()
	for {
		start, startErr := processStartIdentity(l.PID)
		groupErr := syscall.Kill(-l.PGID, 0)
		if startErr != nil && groupErr == syscall.ESRCH {
			// The recorded leader was reaped and its original group is empty.
			// This is sufficient only for the documented non-hostile local
			// containment boundary; it is not an OS process-tree witness.
			return nil
		}
		if startErr == nil && (start != l.ProcessStartIdentity || func() bool { pgid, err := syscall.Getpgid(l.PID); return err != nil || pgid != l.PGID }()) {
			return ErrUnclear
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
