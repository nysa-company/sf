package processsupervisor

import (
	"bufio"
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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/goclosure"
)

const repositoryOutputLimit = 64 << 10
const repositoryInputLimit = 1 << 20
const repositoryToolchainFileLimit = 32768
const repositoryToolchainByteLimit = 512 << 20
const repositoryToolchainEntryLimit = 65536
const repositoryToolchainDepthLimit = 64
const repositoryToolchainStageLimit = 30 * time.Second
const repositoryExecutableByteLimit int64 = 64 << 20
const repositoryExecutableStageLimit = 10 * time.Second
const repositoryTestGroupByteLimit = 64 << 10

// repositoryTestGroupLimit matches the durable Store limit.  A wrapper is
// never acknowledged beyond this bound: an unrecorded test group could not be
// recovered safely after a crash.
const repositoryTestGroupLimit = 64

// RepositoryCommandSupervisor is the sole os/exec boundary for exact,
// credential-free repository commands. The child is held behind the sf gate
// until its Store lease has durably recorded the process identity.
type RepositoryCommandSupervisor struct {
	Executable string
	// GitRunner is the prequalified, credential-free Git observer used to
	// reauthenticate the persisted worktree identity immediately before
	// launch. It is injected by composition; a zero-value Runner is not a
	// safe fallback because it has no packaged execution helper/HOME.
	GitRunner            gitboundary.Runner
	SoftDrain, HardDrain time.Duration
	// beforeWorktreeOpen is test-only synchronization for the preflight/open
	// replacement race.  It is intentionally unexported so production callers
	// cannot turn it into a launch hook.
	beforeWorktreeOpen func()
}

// Preflight is side-effect-free. v1 repository commands are macOS-only: other
// hosts cannot reach any filesystem, toolchain, or process operation.
func repositoryCommandPlatformAvailable(goos string) bool { return goos == "darwin" }

func (s RepositoryCommandSupervisor) Preflight(spec contracts.CommandSpec) error {
	if !repositoryCommandPlatformAvailable(runtime.GOOS) {
		return ErrUnclear
	}
	if len(spec.Argv) > 0 && filepath.Base(spec.Argv[0]) == "npm" {
		return ErrSubprocessRecipeUnsupported
	}
	if len(spec.Argv) == 0 || filepath.Base(spec.Argv[0]) != "go" {
		return ErrUnclear
	}
	return nil
}

var ErrSubprocessRecipeUnsupported = errors.New("repository npm recipe requires operator or CI takeover")

func (s RepositoryCommandSupervisor) Run(ctx context.Context, claim contracts.RepositoryCommandClaim, spec contracts.CommandSpec, policy executionpolicy.CommandSnapshot, lease contracts.RepositoryCommandLease) (contracts.CommandResult, error) {
	if err := s.Preflight(spec); err != nil {
		return contracts.CommandResult{}, err
	}
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
	useVendor, err := goclosure.Validate(spec.Directory)
	if err != nil {
		if errors.Is(err, goclosure.ErrUnvendored) {
			return contracts.CommandResult{}, ErrSubprocessRecipeUnsupported
		}
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
	if err = authenticateRepositorySourceExecutable(resolved); err != nil {
		return contracts.CommandResult{}, err
	}
	if claim.ExecutablePath != resolved {
		return contracts.CommandResult{}, ErrUnclear
	}
	selectedDigest, err := executableFileDigest(resolved)
	if err != nil || claim.ExecutableDigest != selectedDigest {
		return contracts.CommandResult{}, ErrUnclear
	}
	identity, err := parseRepositoryIdentity(claim)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	if spec.Stdin != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	var input []byte
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
	// macOS exposes /var through /private/var. Seatbelt path matching follows
	// the canonical form, so normalize supervisor-owned directories before
	// placing them in the default-deny profile or environment.
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	tmp, err = filepath.EvalSymlinks(tmp)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer os.RemoveAll(home)
	defer os.RemoveAll(tmp)
	goRoot := filepath.Dir(filepath.Dir(resolved))
	rootBinary, rootErr := filepath.EvalSymlinks(filepath.Join(goRoot, "bin", "go"))
	if rootErr != nil || rootBinary != resolved {
		return contracts.CommandResult{}, ErrUnclear
	}
	stagedToolchain, err := stageGoToolchain(goRoot)
	if err != nil {
		return contracts.CommandResult{}, fmt.Errorf("stage Go toolchain: %w", err)
	}
	defer os.RemoveAll(filepath.Dir(stagedToolchain))
	staged := filepath.Join(stagedToolchain, "bin", "go")
	stagedDigest, digestErr := executableDigest(staged)
	sourceDigest, sourceDigestErr := executableFileDigest(resolved)
	if digestErr != nil || sourceDigestErr != nil || stagedDigest != sourceDigest {
		return contracts.CommandResult{}, ErrUnclear
	}
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
	if s.GitRunner.ExecHelper == "" || s.GitRunner.Home == "" {
		return contracts.CommandResult{}, ErrUnclear
	}
	// Staging is a local supervisor preparation step. Start the command timeout
	// only once the immutable gate/toolchain is ready, so a slow private copy
	// cannot consume the ticket's configured verification budget.
	runCtx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()
	if err := s.GitRunner.Reauthenticate(runCtx, identity); err != nil {
		return contracts.CommandResult{}, fmt.Errorf("reauthenticate repository command worktree: %w", err)
	}
	// The whole Go root, including compiler/linker tools, is copied into a
	// private owner-only stage before the driver sees repository input. CGO
	// and every module/tool-selection escape are disabled by the exact policy
	// recipe plus this environment.
	env = append(env, "GOROOT="+stagedToolchain, "CGO_ENABLED=0", "GOPROXY=off", "GOSUMDB=off", "GONOSUMDB=*", "GOTOOLCHAIN=local", "GOENV=off", "GOWORK=off", "GOTELEMETRY=off")
	if useVendor {
		env = append(env, "GOFLAGS=-mod=vendor")
	}
	self, err = stageRepositoryGate(self)
	if err != nil {
		return contracts.CommandResult{}, fmt.Errorf("stage repository gate: %w", err)
	}
	defer os.RemoveAll(filepath.Dir(self))
	// Package execution is serial so the shared durable group-report/
	// acknowledgement pipe cannot acknowledge a different test binary.
	// -count=1 makes a verification actually execute rather than trust a prior
	// Go cache result. Policy has already required the sole v1 recipe.
	launchArgs := append([]string{"test", "-p=1", "-count=1", "-exec=" + self + " __repository_command_test_gate"}, spec.Argv[2:]...)
	gitFile, err := repositoryGitFilePath(identity)
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	env = append(env,
		"SF_REPOSITORY_SANDBOX_REPOSITORY="+claim.Repository,
		"SF_REPOSITORY_SANDBOX_WORKTREE="+claim.Worktree,
		"SF_REPOSITORY_SANDBOX_GIT_FILE="+gitFile,
		"SF_REPOSITORY_SANDBOX_COMMON_DIR="+identity.CommonDir,
		"SF_REPOSITORY_SANDBOX_HOME="+home,
		"SF_REPOSITORY_SANDBOX_TMP="+tmp,
	)
	argv := append([]string{"__repository_command_gate", staged}, launchArgs...)
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
	cmd.ExtraFiles = []*os.File{gateRead, worktreeFD}
	var groupReportRead, groupReportWrite, groupAckRead, groupAckWrite *os.File
	groupReportRead, groupReportWrite, err = os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	groupAckRead, groupAckWrite, err = os.Pipe()
	if err != nil {
		_ = groupReportRead.Close()
		_ = groupReportWrite.Close()
		return contracts.CommandResult{}, err
	}
	defer groupReportRead.Close()
	defer groupAckRead.Close()
	defer groupAckWrite.Close()
	cmd.ExtraFiles = append(cmd.ExtraFiles, groupReportWrite, groupAckRead)
	// The opened directory remains inherited as FD 4 for the gate; the
	// process starts in the canonical path after the preflight identity check.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr repositoryBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	if err := cmd.Start(); err != nil {
		gateRead.Close()
		if groupReportWrite != nil {
			_ = groupReportWrite.Close()
		}
		return contracts.CommandResult{}, err
	}
	gateRead.Close()
	if groupReportWrite != nil {
		_ = groupReportWrite.Close()
		_ = groupAckRead.Close()
	}
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
	groups := &repositoryTestGroups{}
	var reportsDone <-chan struct{}
	recorder, ok := lease.(contracts.RepositoryCommandGroupRecorder)
	if !ok {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = lease.Quarantine()
		return contracts.CommandResult{}, ErrUnclear
	}
	done := make(chan struct{})
	reportsDone = done
	go func() {
		defer close(done)
		readRepositoryTestGroups(groupReportRead, groupAckWrite, recorder, groups)
	}()
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
		groups.stop()
		if pgid, err := syscall.Getpgid(launch.PID); err != nil || pgid != launch.PGID {
			terminateRepositoryGroups(groups.snapshot(), syscall.SIGKILL)
			_ = lease.Quarantine()
			return contracts.CommandResult{}, ErrUnclear
		}
		_ = signalGroup(launch.PGID, syscall.SIGTERM)
		terminateRepositoryGroups(groups.snapshot(), syscall.SIGTERM)
		select {
		case waitErr = <-wait:
		case <-time.After(s.drainSoft()):
			if pgid, err := syscall.Getpgid(launch.PID); err != nil || pgid != launch.PGID {
				terminateRepositoryGroups(groups.snapshot(), syscall.SIGKILL)
				_ = lease.Quarantine()
				return contracts.CommandResult{}, ErrUnclear
			}
			_ = signalGroup(launch.PGID, syscall.SIGKILL)
			terminateRepositoryGroups(groups.snapshot(), syscall.SIGKILL)
			select {
			case waitErr = <-wait:
			case <-time.After(s.drainHard() + 250*time.Millisecond):
				_ = lease.Quarantine()
				return contracts.CommandResult{}, ErrUnclear
			}
		}
	}
	if reportsDone != nil {
		select {
		case <-reportsDone:
		case <-time.After(s.drainHard() + 250*time.Millisecond):
			// A missing acknowledgement/report is process-lifecycle ambiguity,
			// not an excuse to infer that the primary driver's old process group
			// proves the separately grouped wrapper died. Keep the repository
			// quarantined for startup repair/recovery.
			terminateRepositoryGroups(groups.snapshot(), syscall.SIGKILL)
			_ = lease.Quarantine()
			return contracts.CommandResult{}, ErrUnclear
		}
		if groups.err() != nil {
			terminateRepositoryGroups(groups.snapshot(), syscall.SIGKILL)
			_ = lease.Quarantine()
			return contracts.CommandResult{}, fmt.Errorf("repository test-group handshake: %w", groups.err())
		}
	}
	close(monitorStop)
	monitorStopped = true
	groupEscaped := <-escaped
	if err := s.ensureGone(launch, groupEscaped); err != nil {
		return contracts.CommandResult{}, fmt.Errorf("repository ensure gone: %w", err)
	}
	if err := ensureRepositoryGroupsGone(groups.snapshot()); err != nil {
		_ = lease.Quarantine()
		return contracts.CommandResult{}, fmt.Errorf("repository test groups remain: %w", err)
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

// authenticateRepositorySourceExecutable intentionally does not require every
// ancestor directory to be private. Homebrew's code-owned Go installation is
// commonly group-writable at an ancestor even though its executable file is
// not. The source is never executed by path: stageExecutable opens it once,
// hashes that exact FD against the Store claim, and executes only the private
// staged copy. A directory-swap race therefore becomes a digest mismatch,
// not a broadened executable authority.
func authenticateRepositorySourceExecutable(path string) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Mode()&0o022 != 0 || !trustedOwner(info) {
		return ErrUnclear
	}
	return nil
}

func resolveFixedExecutable(name string) (string, error) {
	if name == "" {
		return "", exec.ErrNotFound
	}
	tool := filepath.Base(name)
	if tool != "go" {
		return "", exec.ErrNotFound
	}
	for _, candidate := range approvedRepositoryGoExecutables() {
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

// approvedRepositoryGoExecutables is intentionally code-owned. A digest binds
// the selected Go driver to an intent; this list prevents a private executable
// merely named "go" from becoming eligible for that binding.
func approvedRepositoryGoExecutables() []string {
	return []string{"/usr/local/go/bin/go", "/usr/local/bin/go", "/opt/homebrew/bin/go"}
}

// RepositoryExecutableDigest binds the approved executable file to the claim.
func RepositoryExecutableDigest(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return executableFileDigest(resolved)
}

func executableFileDigest(path string) (string, error) {
	digest, err := executableDigest(path)
	if err != nil {
		return "", err
	}
	return digest, nil
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
	n, copyErr := copyRepositoryBytes(io.MultiWriter(out, hash), source, repositoryExecutableByteLimit, time.Now().Add(repositoryExecutableStageLimit))
	closeErr := out.Close()
	if copyErr != nil || closeErr != nil || n > repositoryExecutableByteLimit || sourceSizeOver(path, repositoryExecutableByteLimit) || expectedDigest != "sha256:"+hex.EncodeToString(hash.Sum(nil)) {
		_ = os.RemoveAll(dir)
		return "", ErrUnclear
	}
	return destination, nil
}

func executableDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > repositoryExecutableByteLimit {
		return "", ErrUnclear
	}
	sum := sha256.New()
	n, err := copyRepositoryBytes(sum, file, repositoryExecutableByteLimit, time.Now().Add(repositoryExecutableStageLimit))
	if err != nil || n != info.Size() || n > repositoryExecutableByteLimit {
		return "", ErrUnclear
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
}

// copyRepositoryBytes has a fixed buffer and checks the staging deadline
// between every bounded read/write. It also reads one byte past the byte cap,
// so a file that grows after Stat is rejected rather than silently truncated.
func copyRepositoryBytes(destination io.Writer, source io.Reader, limit int64, deadline time.Time) (int64, error) {
	if limit < 0 {
		return 0, ErrUnclear
	}
	buffer := make([]byte, 32<<10)
	var copied int64
	for {
		if !time.Now().Before(deadline) {
			return 0, ErrUnclear
		}
		readSize := len(buffer)
		remaining := limit - copied
		if remaining < int64(readSize) {
			readSize = int(remaining + 1)
		}
		n, readErr := source.Read(buffer[:readSize])
		if n > 0 {
			if int64(n) > remaining {
				return 0, ErrUnclear
			}
			written, writeErr := destination.Write(buffer[:n])
			if writeErr != nil || written != n || !time.Now().Before(deadline) {
				return 0, ErrUnclear
			}
			copied += int64(n)
		}
		if readErr == io.EOF {
			return copied, nil
		}
		if readErr != nil {
			return 0, ErrUnclear
		}
	}
}

// stageRepositoryGate closes the otherwise dangerous self-reexec pathname
// window. The current daemon image is already trusted; the helper it spawns is
// an owner-only, digest-checked private copy and is never executed by its
// original pathname.
func stageRepositoryGate(path string) (string, error) {
	if path == "" {
		return "", ErrUnclear
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	if err := authenticateRepositorySourceExecutable(resolved); err != nil {
		return "", err
	}
	digest, err := executableDigest(resolved)
	if err != nil {
		return "", err
	}
	return stageExecutable(resolved, digest)
}

// stageGoToolchain copies the complete exact Go root into a private directory.
// The driver otherwise finds compiler/linker tools by mutable paths below
// GOROOT after its own executable was staged. CGO is disabled separately, but
// staging the whole root also prevents a replacement of Go's native tools from
// becoming pre-test authority.
func stageGoToolchain(root string) (string, error) {
	if !cleanAbsolute(root) {
		return "", ErrUnclear
	}
	rootInfo, err := os.Lstat(root)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() || rootInfo.Mode().Perm()&0o022 != 0 || !trustedOwner(rootInfo) {
		return "", ErrUnclear
	}
	base, err := os.MkdirTemp("", "sf-go-toolchain-")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(base, 0o700); err != nil {
		_ = os.RemoveAll(base)
		return "", err
	}
	destinationRoot := filepath.Join(base, "goroot")
	deadline := time.Now().Add(repositoryToolchainStageLimit)
	var entries, files int
	var copied int64
	copyFile := func(source, destination string, info os.FileInfo) error {
		if files >= repositoryToolchainFileLimit || info.Size() < 0 || info.Size() > repositoryToolchainByteLimit-copied {
			return ErrUnclear
		}
		sourceFile, err := os.Open(source)
		if err != nil {
			return err
		}
		destinationFile, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm()&0o555)
		if err != nil {
			_ = sourceFile.Close()
			return err
		}
		var written int64
		buffer := make([]byte, 32<<10)
		for {
			if !time.Now().Before(deadline) {
				_ = sourceFile.Close()
				_ = destinationFile.Close()
				return ErrUnclear
			}
			n, readErr := sourceFile.Read(buffer)
			if n > 0 {
				if int64(n) > repositoryToolchainByteLimit-copied-written {
					_ = sourceFile.Close()
					_ = destinationFile.Close()
					return ErrUnclear
				}
				m, writeErr := destinationFile.Write(buffer[:n])
				if writeErr != nil || m != n {
					_ = sourceFile.Close()
					_ = destinationFile.Close()
					return ErrUnclear
				}
				written += int64(n)
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = sourceFile.Close()
				_ = destinationFile.Close()
				return ErrUnclear
			}
		}
		sourceCloseErr := sourceFile.Close()
		closeErr := destinationFile.Close()
		if sourceCloseErr != nil || closeErr != nil || written != info.Size() || !time.Now().Before(deadline) {
			return ErrUnclear
		}
		files++
		copied += written
		return nil
	}
	var walk func(source, destination string, depth int) error
	walk = func(source, destination string, depth int) error {
		if !time.Now().Before(deadline) {
			return ErrUnclear
		}
		if depth > repositoryToolchainDepthLimit {
			return ErrUnclear
		}
		info, err := os.Lstat(source)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
			return ErrUnclear
		}
		entries++
		if entries > repositoryToolchainEntryLimit {
			return ErrUnclear
		}
		if info.IsDir() {
			// The stage is private because its top-level parent is 0700. Keep
			// nested directories owner-writable while the bounded walker copies
			// children. ReadDir's fixed batch avoids allocating an adversarial
			// directory's entire child list before the entry cap is checked.
			if err := os.Mkdir(destination, 0o700); err != nil {
				return err
			}
			dir, err := os.Open(source)
			if err != nil {
				return err
			}
			defer dir.Close()
			for {
				children, readErr := dir.ReadDir(128)
				for _, child := range children {
					if child.Name() == "." || child.Name() == ".." || strings.Contains(child.Name(), string(filepath.Separator)) {
						return ErrUnclear
					}
					if err := walk(filepath.Join(source, child.Name()), filepath.Join(destination, child.Name()), depth+1); err != nil {
						return err
					}
				}
				if readErr == io.EOF {
					return nil
				}
				if readErr != nil {
					return readErr
				}
			}
		}
		if !info.Mode().IsRegular() {
			return ErrUnclear
		}
		return copyFile(source, destination, info)
	}
	err = walk(root, destinationRoot, 0)
	if err != nil {
		_ = os.RemoveAll(base)
		return "", err
	}
	goBinary := filepath.Join(destinationRoot, "bin", "go")
	if err := authenticateRepositorySourceExecutable(goBinary); err != nil {
		_ = os.RemoveAll(base)
		return "", err
	}
	return destinationRoot, nil
}

func repositoryGitFilePath(identity gitboundary.Identity) (string, error) {
	if !cleanAbsolute(identity.Worktree) {
		return "", ErrUnclear
	}
	path := filepath.Join(identity.Worktree, ".git")
	if filepath.Clean(path) != path {
		return "", ErrUnclear
	}
	return path, nil
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
	for _, pair := range [][2]uint64{{identity.RepositoryDev, identity.RepositoryIno}, {identity.WorktreeDev, identity.WorktreeIno}, {identity.GitFileDev, identity.GitFileIno}, {identity.CommonDirDev, identity.CommonDirIno}} {
		if pair[0] == 0 || pair[1] == 0 {
			return gitboundary.Identity{}, ErrUnclear
		}
	}
	// A hermetic local push remote is a filesystem witness. SSH remotes have
	// no local inode by definition and Snapshot records their zero pair.
	if filepath.IsAbs(identity.PushOrigin) && (identity.PushOriginDev == 0 || identity.PushOriginIno == 0) {
		return gitboundary.Identity{}, ErrUnclear
	}
	gitFileTarget, ok := strings.CutPrefix(identity.GitFile, "gitdir: ")
	gitFileTarget = strings.TrimSpace(gitFileTarget)
	if !ok || !filepath.IsAbs(gitFileTarget) || filepath.Clean(gitFileTarget) != gitFileTarget {
		return gitboundary.Identity{}, ErrUnclear
	}
	for _, path := range []string{identity.Repository, identity.Worktree, identity.CommonDir} {
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

type repositoryTestGroups struct {
	mu       sync.Mutex
	groups   []contracts.RepositoryCommandLaunch
	stopping bool
	failure  error
}

func (g *repositoryTestGroups) stop() {
	g.mu.Lock()
	g.stopping = true
	g.mu.Unlock()
}
func (g *repositoryTestGroups) snapshot() []contracts.RepositoryCommandLaunch {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]contracts.RepositoryCommandLaunch(nil), g.groups...)
}
func (g *repositoryTestGroups) stoppingNow() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopping
}
func (g *repositoryTestGroups) add(v contracts.RepositoryCommandLaunch) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !boundedRepositoryTestGroups(append(g.groups, v)) {
		return false
	}
	g.groups = append(g.groups, v)
	return true
}

func repositoryTestGroupBytes(v contracts.RepositoryCommandLaunch) (int, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}
	return len(raw), nil
}

func boundedRepositoryTestGroups(groups []contracts.RepositoryCommandLaunch) bool {
	if len(groups) > repositoryTestGroupLimit {
		return false
	}
	var total int
	for _, group := range groups {
		if !validRepositoryLaunch(group) {
			return false
		}
		n, err := repositoryTestGroupBytes(group)
		if err != nil || n > repositoryTestGroupByteLimit || total > repositoryTestGroupByteLimit-n {
			return false
		}
		total += n
	}
	return true
}
func (g *repositoryTestGroups) fail(err error) {
	g.mu.Lock()
	if g.failure == nil {
		g.failure = err
	}
	g.mu.Unlock()
}
func (g *repositoryTestGroups) err() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failure
}

// readRepositoryTestGroups is the parent half of the test-wrapper handshake.
// The wrapper reports only after Setpgid(0,0), then waits for this function to
// durably record the exact PID/start witness before sandboxing and execing the
// test binary. Go is invoked with -p=1, making the shared acknowledgement
// byte unambiguous.
func readRepositoryTestGroups(report *os.File, ack *os.File, recorder contracts.RepositoryCommandGroupRecorder, groups *repositoryTestGroups) {
	if report == nil || ack == nil || recorder == nil {
		groups.fail(ErrUnclear)
		return
	}
	scanner := bufio.NewScanner(report)
	scanner.Buffer(make([]byte, 32), 128)
	for scanner.Scan() {
		pid, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
		if err != nil || pid <= 0 {
			groups.fail(ErrUnclear)
			return
		}
		start, e1 := processStartIdentity(pid)
		boot, e2 := hostBootIdentity()
		pgid, e3 := syscall.Getpgid(pid)
		v := contracts.RepositoryCommandLaunch{PID: pid, PGID: pgid, BootIdentity: boot, ProcessStartIdentity: start}
		if e1 != nil || e2 != nil || e3 != nil || !validRepositoryLaunch(v) || groups.stoppingNow() {
			if validRepositoryLaunch(v) {
				_ = groups.add(v)
				_ = signalGroup(v.PGID, syscall.SIGKILL)
			}
			groups.fail(ErrUnclear)
			return
		}
		if !groups.add(v) {
			// The wrapper is still behind the acknowledgement gate.  Kill its
			// just-created group rather than permit a 65th group whose identity
			// cannot be durably retained for recovery.
			_ = signalGroup(v.PGID, syscall.SIGKILL)
			groups.fail(ErrUnclear)
			return
		}
		persistCtx, cancel := repositoryLeasePersistenceContext()
		err = recorder.RecordRepositoryCommandProcessGroup(persistCtx, v)
		cancel()
		if err != nil {
			_ = signalGroup(v.PGID, syscall.SIGKILL)
			groups.fail(err)
			return
		}
		if _, err := ack.Write([]byte{1}); err != nil {
			groups.fail(err)
			return
		}
	}
	if err := scanner.Err(); err != nil {
		groups.fail(err)
	}
}

func terminateRepositoryGroups(groups []contracts.RepositoryCommandLaunch, signal syscall.Signal) {
	for _, group := range groups {
		if validRepositoryLaunch(group) {
			_ = signalGroup(group.PGID, signal)
		}
	}
}

func ensureRepositoryGroupsGone(groups []contracts.RepositoryCommandLaunch) error {
	for _, group := range groups {
		if !validRepositoryLaunch(group) {
			return ErrUnclear
		}
		deadline := time.Now().Add(250 * time.Millisecond)
		for {
			start, startErr := processStartIdentity(group.PID)
			groupErr := syscall.Kill(-group.PGID, 0)
			if startErr != nil && groupErr == syscall.ESRCH {
				break
			}
			if startErr == nil && (start != group.ProcessStartIdentity || func() bool { pgid, err := syscall.Getpgid(group.PID); return err != nil || pgid != group.PGID }()) {
				return ErrUnclear
			}
			if time.Now().After(deadline) {
				return ErrUnclear
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return nil
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
// before signalling. It is intentionally not a general process-tree
// containment primitive: v1 only admits Go test wrappers whose group leaders
// were durably recorded before untrusted test code was acknowledged.
type RepositoryCommandDrainer struct {
	SoftDrain, HardDrain time.Duration
	// bootIdentity is test-only injection. Production always obtains the
	// current host boot witness.
	bootIdentity func() (string, error)
}

func (d RepositoryCommandDrainer) DrainRepositoryCommand(ctx context.Context, l contracts.RepositoryCommandLaunch) error {
	return d.DrainRepositoryCommandTree(ctx, l, nil)
}

func (d RepositoryCommandDrainer) DrainRepositoryCommandTree(ctx context.Context, primary contracts.RepositoryCommandLaunch, groups []contracts.RepositoryCommandLaunch) error {
	if !validRepositoryLaunch(primary) || !boundedRepositoryTestGroups(groups) || !boundedRepositoryTestGroups([]contracts.RepositoryCommandLaunch{primary}) || d.SoftDrain > 30*time.Second || d.HardDrain > 30*time.Second {
		return ErrUnclear
	}
	soft, hard := d.SoftDrain, d.HardDrain
	if soft <= 0 {
		soft = 2 * time.Second
	}
	if hard <= 0 {
		hard = 2 * time.Second
	}
	// Recovery has one budget for the Store load, every persisted group, and
	// both drain phases.  Do not give each tracked group a fresh timeout.
	deadline := time.Now().Add(soft + hard)
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	if err := repositoryDrainContextErr(ctx, deadline); err != nil {
		return err
	}
	launches := append([]contracts.RepositoryCommandLaunch{primary}, groups...)
	for _, launch := range launches {
		if !validRepositoryLaunch(launch) || launch.BootIdentity != primary.BootIdentity {
			return ErrUnclear
		}
	}
	boot, err := d.currentBootIdentity()
	if err != nil {
		return ErrUnclear
	}
	if err := repositoryDrainContextErr(ctx, deadline); err != nil {
		return err
	}
	// A boot identifier is an OS-lifetime witness. A process from the prior
	// boot cannot still exist, so no old PID or PGID is ever signalled.
	if boot != primary.BootIdentity {
		return nil
	}
	live, err := inspectRepositoryGroups(ctx, deadline, launches)
	if err != nil {
		return err
	}
	for _, launch := range live {
		if err := repositoryDrainContextErr(ctx, deadline); err != nil {
			return err
		}
		if err := signalGroup(launch.PGID, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
			return ErrUnclear
		}
	}
	softDeadline := time.Now().Add(soft)
	if deadline.Before(softDeadline) {
		softDeadline = deadline
	}
	if err := waitRepositoryGroups(ctx, launches, softDeadline); err == nil {
		return nil
	} else if !errors.Is(err, errRepositoryGroupsStillLive) {
		return err
	}

	// Re-authenticate every leader immediately before KILL. If a leader has
	// vanished while its old group remains, the PGID could now name an
	// unrelated group; that is ambiguity, not permission to signal it.
	live, err = inspectRepositoryGroups(ctx, deadline, launches)
	if err != nil {
		return err
	}
	for _, launch := range live {
		if err := repositoryDrainContextErr(ctx, deadline); err != nil {
			return err
		}
		if err := signalGroup(launch.PGID, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			return ErrUnclear
		}
	}
	return waitRepositoryGroups(ctx, launches, deadline)
}

func (d RepositoryCommandDrainer) currentBootIdentity() (string, error) {
	if d.bootIdentity != nil {
		return d.bootIdentity()
	}
	return hostBootIdentity()
}

var errRepositoryGroupsStillLive = errors.New("repository command groups still live")

// inspectRepositoryGroups is deliberately stricter than Kill(-pgid, 0): a
// missing leader plus a live group is not authenticated and therefore cannot
// be signalled. A missing leader and ESRCH for its exact group is the narrow
// proof that this recorded wrapper group is gone.
func inspectRepositoryGroups(ctx context.Context, deadline time.Time, launches []contracts.RepositoryCommandLaunch) ([]contracts.RepositoryCommandLaunch, error) {
	live := make([]contracts.RepositoryCommandLaunch, 0, len(launches))
	for _, launch := range launches {
		if err := repositoryDrainContextErr(ctx, deadline); err != nil {
			return nil, err
		}
		start, startErr := processStartIdentity(launch.PID)
		groupErr := syscall.Kill(-launch.PGID, 0)
		if startErr != nil {
			if groupErr == syscall.ESRCH {
				continue
			}
			return nil, ErrUnclear
		}
		if start != launch.ProcessStartIdentity {
			return nil, ErrUnclear
		}
		pgid, err := syscall.Getpgid(launch.PID)
		// Darwin can retain the exact exited leader briefly as a zombie until
		// its parent reaps it. Its start witness still matches, while ESRCH for
		// the exact group proves no runnable member remains; this is safe gone,
		// not a permission to signal a possibly reused PGID.
		if groupErr == syscall.ESRCH {
			continue
		}
		if err != nil || pgid != launch.PGID || groupErr != nil {
			return nil, ErrUnclear
		}
		live = append(live, launch)
	}
	return live, nil
}

func waitRepositoryGroups(ctx context.Context, launches []contracts.RepositoryCommandLaunch, deadline time.Time) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		live, err := inspectRepositoryGroups(ctx, deadline, launches)
		if err != nil {
			return err
		}
		if len(live) == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return errRepositoryGroupsStillLive
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func repositoryDrainContextErr(ctx context.Context, deadline time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
func validRepositoryLaunch(l contracts.RepositoryCommandLaunch) bool {
	return l.PID > 0 && l.PGID == l.PID && l.BootIdentity != "" && l.ProcessStartIdentity != ""
}
