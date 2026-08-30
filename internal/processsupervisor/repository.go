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
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

// Preflight is a deliberately side-effect-free compatibility hook. Typed
// recipes are admitted only after Run stages the complete selected toolchain
// and applies the inherited default-deny profile.
func (s RepositoryCommandSupervisor) Preflight(contracts.CommandSpec) error { return nil }

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
	if err = authenticateRepositorySourceExecutable(resolved); err != nil {
		return contracts.CommandResult{}, err
	}
	if claim.ExecutablePath != resolved {
		return contracts.CommandResult{}, ErrUnclear
	}
	selectedDigest, err := repositoryExecutableDigest(resolved)
	if err != nil || claim.ExecutableDigest != selectedDigest {
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
	isGo := filepath.Base(resolved) == "go"
	_, npmRootErr := nodeToolchainRoot(resolved)
	isNPM := npmRootErr == nil
	var staged, stagedToolchain string
	var npmClosure nodeToolchainClosure
	if isGo {
		goRoot := filepath.Dir(filepath.Dir(resolved))
		rootBinary, rootErr := filepath.EvalSymlinks(filepath.Join(goRoot, "bin", "go"))
		if rootErr != nil || rootBinary != resolved {
			return contracts.CommandResult{}, ErrUnclear
		}
		stagedToolchain, err = stageGoToolchain(goRoot)
		if err != nil {
			return contracts.CommandResult{}, fmt.Errorf("stage Go toolchain: %w", err)
		}
		defer os.RemoveAll(filepath.Dir(stagedToolchain))
		staged = filepath.Join(stagedToolchain, "bin", "go")
		stagedDigest, digestErr := executableDigest(staged)
		sourceDigest, sourceDigestErr := executableFileDigest(resolved)
		if digestErr != nil || sourceDigestErr != nil || stagedDigest != sourceDigest {
			return contracts.CommandResult{}, ErrUnclear
		}
	} else if isNPM {
		npmClosure, err = resolveNodeToolchainClosure(resolved)
		if err != nil || npmClosure.Digest != claim.ExecutableDigest {
			return contracts.CommandResult{}, ErrUnclear
		}
		staged, stagedToolchain = resolved, npmClosure.Root
	} else {
		staged, err = stageExecutable(resolved, claim.ExecutableDigest)
		if err != nil {
			return contracts.CommandResult{}, fmt.Errorf("stage repository executable: %w", err)
		}
		defer os.RemoveAll(filepath.Dir(staged))
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
	isGoTest := isGo && len(spec.Argv) >= 2 && spec.Argv[1] == "test"
	if isGo {
		// The whole Go root, including compiler/linker tools, is copied into a
		// private owner-only stage before the driver sees repository input. CGO
		// and every module/tool-selection escape are disabled by the exact policy
		// recipe plus this environment.
		env = append(env, "GOROOT="+stagedToolchain, "CGO_ENABLED=0", "GOPROXY=off", "GOSUMDB=off", "GONOSUMDB=*", "GOTOOLCHAIN=local", "GOENV=off", "GOWORK=off", "GOTELEMETRY=off")
	} else if isNPM {
		// Do not let npm discover a host-global prefix/config/cache or fetch a
		// package. The default-deny Seatbelt profile independently denies
		// network and every non-private write path; these knobs make normal npm
		// behavior deterministic before it reaches that kernel boundary.
		env = append(env,
			"PATH="+filepath.Join(stagedToolchain, "bin")+":/usr/bin:/bin:/usr/sbin:/sbin",
			"NPM_CONFIG_AUDIT=false", "NPM_CONFIG_FUND=false", "NPM_CONFIG_UPDATE_NOTIFIER=false",
			"NPM_CONFIG_OFFLINE=true", "NPM_CONFIG_PREFER_OFFLINE=true",
			"NPM_CONFIG_CACHE="+filepath.Join(tmp, "npm-cache"),
			"NPM_CONFIG_USERCONFIG="+filepath.Join(home, "npmrc"), "NPM_CONFIG_GLOBALCONFIG=/dev/null",
			"OPENSSL_CONF=/dev/null",
		)
	}
	self, err = stageRepositoryGate(self)
	if err != nil {
		return contracts.CommandResult{}, fmt.Errorf("stage repository gate: %w", err)
	}
	defer os.RemoveAll(filepath.Dir(self))
	var sandboxProfile string
	launchArgs := append([]string(nil), spec.Argv[1:]...)
	if isGoTest {
		// Package execution is serial so the shared durable group-report/
		// acknowledgement pipe cannot acknowledge a different test binary.
		// -count=1 makes a verification actually execute rather than trust a
		// prior Go cache result.
		launchArgs = append([]string{"test", "-p=1", "-count=1", "-exec=" + self + " __repository_command_test_gate"}, spec.Argv[2:]...)
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
	} else if !isGo {
		gitFile, pathErr := repositoryGitFilePath(identity)
		if pathErr != nil {
			return contracts.CommandResult{}, ErrUnclear
		}
		sandboxProfile, err = repositoryStrictSandboxProfileFor(repositorySandboxPaths{Repository: claim.Repository, Worktree: claim.Worktree, GitFile: gitFile, CommonDir: identity.CommonDir, Home: home, Temporary: tmp, Executable: staged, Toolchain: stagedToolchain, DependencyPaths: npmClosure.DependencyPaths, AllowProcessTree: isNPM})
	}
	if err != nil {
		return contracts.CommandResult{}, ErrUnclear
	}
	argv := append([]string{"__repository_command_gate"}, launchArgs...)
	if isGo {
		argv = append([]string{"__repository_command_gate", staged}, launchArgs...)
	} else {
		argv = append([]string{"__repository_command_gate", repositorySandboxExec, "-p", sandboxProfile, staged}, launchArgs...)
	}
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
	if isGoTest {
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
	}
	// The opened directory remains inherited as FD 4 for the gate; the
	// process starts in the canonical path after the preflight identity check.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr repositoryBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if isNPM {
		// Recompute every Node/npm closure member after profile construction and
		// immediately before the gate starts. A mismatch is a pre-launch refusal.
		if !nodeClosureMatches(resolved, npmClosure, claim.ExecutableDigest) {
			return contracts.CommandResult{}, ErrUnclear
		}
	}
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
	if isGoTest {
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
	if filepath.IsAbs(name) {
		resolved, err := filepath.EvalSymlinks(name)
		if err != nil {
			return "", exec.ErrNotFound
		}
		if _, nodeErr := nodeToolchainRoot(resolved); nodeErr == nil {
			return resolved, nil
		}
	}
	tool := filepath.Base(name)
	if tool != "git" && tool != "go" && tool != "npm" {
		return "", exec.ErrNotFound
	}
	for _, candidate := range approvedRepositoryExecutables(tool) {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		if tool == "npm" {
			if _, err := nodeToolchainRoot(resolved); err != nil {
				continue
			}
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
		// /usr/bin/git is a macOS developer-tools trampoline. The actual
		// binary must be bound and staged; otherwise the trampoline performs a
		// second path-based exec after the digest check.
		return []string{"/Library/Developer/CommandLineTools/usr/bin/git", "/Applications/Xcode.app/Contents/Developer/usr/bin/git", "/usr/bin/git", "/usr/local/bin/git", "/opt/homebrew/bin/git"}
	case "go":
		return []string{"/usr/local/go/bin/go", "/usr/local/bin/go", "/opt/homebrew/bin/go"}
	case "npm":
		return []string{"/opt/homebrew/bin/npm", "/usr/local/bin/npm"}
	default:
		return nil
	}
}

// RepositoryExecutableDigest binds an npm claim to the complete immutable
// Node/npm closure rather than only npm-cli.js. Other approved commands are
// single staged files and retain their ordinary file digest.
func RepositoryExecutableDigest(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	return repositoryExecutableDigest(resolved)
}

func repositoryExecutableDigest(resolved string) (string, error) {
	if root, err := nodeToolchainRoot(resolved); err == nil {
		closure, err := resolveNodeToolchainClosureForRoot(root)
		if err != nil {
			return "", err
		}
		return closure.Digest, nil
	}
	return executableFileDigest(resolved)
}

type nodeToolchainClosure struct {
	Root            string
	Digest          string
	DependencyPaths []string
}

func resolveNodeToolchainClosure(entry string) (nodeToolchainClosure, error) {
	root, err := nodeToolchainRoot(entry)
	if err != nil {
		return nodeToolchainClosure{}, err
	}
	return resolveNodeToolchainClosureForRoot(root)
}

// resolveNodeToolchainClosure is bounded to a qualified Node 22 root and the
// transitive Mach-O dependencies of its node binary. It never treats an otool
// result as a broad filesystem grant: only checked absolute dependency files
// are later passed to Seatbelt as literals.
func resolveNodeToolchainClosureForRoot(root string) (nodeToolchainClosure, error) {
	rootDigest, err := nodeToolchainDigest(root)
	if err != nil {
		return nodeToolchainClosure{}, err
	}
	node := filepath.Join(root, "bin", "node")
	probeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	probe := exec.CommandContext(probeCtx, node, "--version")
	probe.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + root, "TMPDIR=" + root}
	version, err := probe.Output()
	if err != nil || !strings.HasPrefix(strings.TrimSpace(string(version)), "v22.") {
		return nodeToolchainClosure{}, ErrUnclear
	}
	paths, err := nodeMachOClosure(root, node)
	if err != nil {
		return nodeToolchainClosure{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("node-root\x00" + root + "\x00" + rootDigest + "\x00"))
	for _, path := range paths {
		resolved, err := filepath.EvalSymlinks(path)
		if err != nil || !trustedNodeDependency(path, resolved) || authenticateRepositorySourceDependency(resolved) != nil {
			return nodeToolchainClosure{}, ErrUnclear
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return nodeToolchainClosure{}, err
		}
		_, _ = hash.Write([]byte("dependency\x00" + path + "\x00" + resolved + "\x00" + info.Mode().String() + "\x00" + strconv.FormatInt(info.Size(), 10) + "\x00"))
		if err := hashFile(hash, resolved); err != nil {
			return nodeToolchainClosure{}, err
		}
	}
	// Seatbelt must grant each versioned dylib directory so dyld can resolve
	// internal symlinks. Bind the whole enumerated directory as well: an added
	// sibling cannot become a pre-launch loader input without changing this
	// closure digest.
	directories := map[string]bool{}
	for _, path := range paths {
		resolved, err := filepath.EvalSymlinks(filepath.Dir(path))
		if err != nil || !cleanAbsolute(resolved) {
			return nodeToolchainClosure{}, ErrUnclear
		}
		directories[resolved] = true
	}
	dirs := make([]string, 0, len(directories))
	for directory := range directories {
		dirs = append(dirs, directory)
	}
	sort.Strings(dirs)
	for _, directory := range dirs {
		digest, err := dependencyDirectoryDigest(directory)
		if err != nil {
			return nodeToolchainClosure{}, fmt.Errorf("authenticate Node dependency directory %s: %w", directory, err)
		}
		_, _ = hash.Write([]byte("dependency-directory\x00" + directory + "\x00" + digest + "\x00"))
	}
	return nodeToolchainClosure{Root: root, Digest: "sha256:" + hex.EncodeToString(hash.Sum(nil)), DependencyPaths: paths}, nil
}

func dependencyDirectoryDigest(root string) (string, error) {
	info, err := os.Lstat(root)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
		return "", ErrUnclear
	}
	hash := sha256.New()
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ErrUnclear
		}
		info, err := entry.Info()
		if err != nil || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
			return ErrUnclear
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := filepath.EvalSymlinks(path)
			if err != nil || !pathWithin(root, target) {
				return ErrUnclear
			}
			targetInfo, err := os.Stat(target)
			if err != nil || targetInfo.Mode().Perm()&0o022 != 0 || !trustedOwner(targetInfo) {
				return ErrUnclear
			}
			_, _ = hash.Write([]byte("L\x00" + rel + "\x00" + target + "\x00"))
			if targetInfo.IsDir() {
				// WalkDir reaches the real sibling directory separately; record the
				// link identity without traversing it twice.
				return nil
			}
			if !targetInfo.Mode().IsRegular() {
				return ErrUnclear
			}
			return hashFile(hash, target)
		}
		if entry.IsDir() {
			_, _ = hash.Write([]byte("D\x00" + rel + "\x00" + info.Mode().String() + "\x00"))
			return nil
		}
		if !info.Mode().IsRegular() {
			return ErrUnclear
		}
		_, _ = hash.Write([]byte("F\x00" + rel + "\x00" + info.Mode().String() + "\x00" + strconv.FormatInt(info.Size(), 10) + "\x00"))
		return hashFile(hash, path)
	})
	if err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func nodeMachOClosure(root, node string) ([]string, error) {
	queue := []string{node}
	seen := map[string]bool{}
	allowed := map[string]bool{}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		resolved, err := filepath.EvalSymlinks(current)
		if err != nil || seen[resolved] {
			continue
		}
		seen[resolved] = true
		dependencies, err := machODependencies(resolved)
		if err != nil {
			return nil, fmt.Errorf("inspect Node dependency %s: %w", resolved, err)
		}
		rpaths, err := machORpaths(resolved)
		if err != nil {
			return nil, fmt.Errorf("inspect Node rpaths %s: %w", resolved, err)
		}
		for _, dependency := range dependencies {
			candidate, err := resolveMachODependency(resolved, rpaths, dependency)
			if err != nil {
				return nil, fmt.Errorf("resolve Node dependency %s from %s: %w", dependency, resolved, err)
			}
			// System libraries live inside the code-owned macOS paths already
			// explicit in the profile. They are neither Homebrew grants nor
			// mutable Node closure members.
			if strings.HasPrefix(candidate, "/System/") || strings.HasPrefix(candidate, "/usr/lib/") {
				continue
			}
			resolvedCandidate, err := filepath.EvalSymlinks(candidate)
			if err != nil || !trustedNodeDependency(candidate, resolvedCandidate) {
				return nil, fmt.Errorf("untrusted Node dependency %s resolved %s: %w", candidate, resolvedCandidate, ErrUnclear)
			}
			allowed[candidate] = true
			allowed[resolvedCandidate] = true
			queue = append(queue, resolvedCandidate)
		}
	}
	paths := make([]string, 0, len(allowed))
	for path := range allowed {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func machORpaths(path string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/usr/bin/otool", "-l", path).Output()
	if err != nil || len(output) > 256<<10 {
		return nil, ErrUnclear
	}
	var result []string
	wantPath := false
	for _, line := range strings.Split(string(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "cmd LC_RPATH" {
			wantPath = true
			continue
		}
		if wantPath && strings.HasPrefix(line, "path ") {
			path, _, ok := strings.Cut(strings.TrimPrefix(line, "path "), " (offset ")
			if !ok || path == "" {
				return nil, ErrUnclear
			}
			result = append(result, path)
			wantPath = false
		}
	}
	return result, nil
}

func machODependencies(path string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "/usr/bin/otool", "-L", path).Output()
	if err != nil || len(output) > 128<<10 {
		return nil, ErrUnclear
	}
	var result []string
	for _, line := range strings.Split(string(output), "\n")[1:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		dependency, _, ok := strings.Cut(line, " (")
		if !ok || dependency == "" {
			return nil, ErrUnclear
		}
		result = append(result, dependency)
	}
	return result, nil
}

func resolveMachODependency(current string, rpaths []string, dependency string) (string, error) {
	switch {
	case strings.HasPrefix(dependency, "/"):
		return dependency, nil
	case strings.HasPrefix(dependency, "@loader_path/"):
		return filepath.Join(filepath.Dir(current), strings.TrimPrefix(dependency, "@loader_path/")), nil
	case strings.HasPrefix(dependency, "@rpath/"):
		name := strings.TrimPrefix(dependency, "@rpath/")
		for _, rpath := range append([]string{"@loader_path/../lib"}, rpaths...) {
			var directory string
			switch {
			case strings.HasPrefix(rpath, "@loader_path/"):
				directory = filepath.Join(filepath.Dir(current), strings.TrimPrefix(rpath, "@loader_path/"))
			case strings.HasPrefix(rpath, "/"):
				directory = rpath
			default:
				return "", ErrUnclear
			}
			candidate := filepath.Join(directory, name)
			if _, err := os.Stat(candidate); err == nil {
				return candidate, nil
			}
		}
		return "", ErrUnclear
	default:
		return "", ErrUnclear
	}
}

func trustedNodeDependency(path, resolved string) bool {
	if pathWithin("/opt/homebrew/Cellar", resolved) || pathWithin("/usr/local/Cellar", resolved) || pathWithin("/usr/local/lib/nodejs", resolved) {
		return strings.HasPrefix(path, "/opt/homebrew/opt/") || strings.HasPrefix(path, "/usr/local/opt/") || pathWithin("/opt/homebrew/Cellar", path) || pathWithin("/usr/local/Cellar", path) || pathWithin("/usr/local/lib/nodejs", path)
	}
	return false
}

func authenticateRepositorySourceDependency(path string) error {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
		return ErrUnclear
	}
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func nodeClosureMatches(entry string, expected nodeToolchainClosure, expectedDigest string) bool {
	fresh, err := resolveNodeToolchainClosure(entry)
	return err == nil && fresh.Digest == expectedDigest && fresh.Digest == expected.Digest && equalStringSlices(fresh.DependencyPaths, expected.DependencyPaths)
}

func executableFileDigest(path string) (string, error) {
	digest, err := executableDigest(path)
	if err != nil {
		return "", err
	}
	return digest, nil
}

func nodeToolchainRoot(entry string) (string, error) {
	const npmSuffix = "/lib/node_modules/npm/bin/npm-cli.js"
	if !strings.HasSuffix(entry, npmSuffix) || !cleanAbsolute(entry) {
		return "", ErrUnclear
	}
	root := strings.TrimSuffix(entry, npmSuffix)
	if !trustedNodeToolchainRoot(root) {
		return "", ErrUnclear
	}
	for _, required := range []string{filepath.Join(root, "bin", "node"), entry} {
		if err := authenticateRepositorySourceExecutable(required); err != nil {
			return "", err
		}
	}
	return root, nil
}

func trustedNodeToolchainRoot(root string) bool {
	for _, prefix := range []string{"/opt/homebrew/Cellar/node@22/", "/usr/local/Cellar/node@22/", "/usr/local/lib/nodejs/"} {
		if strings.HasPrefix(root, prefix) && cleanAbsolute(root) {
			return true
		}
	}
	return false
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

func executableDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(sum.Sum(nil)), nil
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
	err = filepath.WalkDir(root, func(source string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, source)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ErrUnclear
		}
		destination := destinationRoot
		if rel != "." {
			destination = filepath.Join(destinationRoot, rel)
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
			return ErrUnclear
		}
		if entry.IsDir() {
			// The stage is private because its top-level parent is 0700. Keep
			// nested directories owner-writable while WalkDir copies children.
			return os.MkdirAll(destination, 0o700)
		}
		if !info.Mode().IsRegular() {
			return ErrUnclear
		}
		sourceFile, err := os.Open(source)
		if err != nil {
			return err
		}
		defer sourceFile.Close()
		destinationFile, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm()&0o555)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(destinationFile, sourceFile)
		closeErr := destinationFile.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
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

// nodeToolchainDigest authenticates the whole Node/npm closure in a stable
// lexical order. npm-cli.js is only a launcher: binding just that file would
// leave Node and npm's JavaScript dependency graph mutable after planning.
func nodeToolchainDigest(root string) (string, error) {
	if !trustedNodeToolchainRoot(root) {
		return "", ErrUnclear
	}
	hash := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return ErrUnclear
		}
		info, err := entry.Info()
		if err != nil || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
			return ErrUnclear
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := filepath.EvalSymlinks(path)
			if err != nil || !pathWithin(root, target) {
				return ErrUnclear
			}
			targetInfo, err := os.Stat(target)
			if err != nil || !targetInfo.Mode().IsRegular() || targetInfo.Mode().Perm()&0o022 != 0 || !trustedOwner(targetInfo) {
				return ErrUnclear
			}
			_, _ = hash.Write([]byte("L\x00" + rel + "\x00" + strings.TrimPrefix(target, root+string(filepath.Separator)) + "\x00"))
			return hashFile(hash, target)
		}
		if entry.IsDir() {
			_, _ = hash.Write([]byte("D\x00" + rel + "\x00"))
			return nil
		}
		if !info.Mode().IsRegular() {
			return ErrUnclear
		}
		_, _ = hash.Write([]byte("F\x00" + rel + "\x00"))
		return hashFile(hash, path)
	})
	if err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func hashFile(hash io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(hash, f); err != nil {
		return err
	}
	_, err = hash.Write([]byte{0})
	return err
}

func pathWithin(root, path string) bool {
	if !cleanAbsolute(root) || !cleanAbsolute(path) {
		return false
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
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
func (g *repositoryTestGroups) add(v contracts.RepositoryCommandLaunch) {
	g.mu.Lock()
	g.groups = append(g.groups, v)
	g.mu.Unlock()
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
				groups.add(v)
				_ = signalGroup(v.PGID, syscall.SIGKILL)
			}
			groups.fail(ErrUnclear)
			return
		}
		groups.add(v)
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
// before signalling. An absent or mismatched identity is never guessed.
type RepositoryCommandDrainer struct{ SoftDrain, HardDrain time.Duration }

func (d RepositoryCommandDrainer) DrainRepositoryCommand(ctx context.Context, l contracts.RepositoryCommandLaunch) error {
	return d.DrainRepositoryCommandTree(ctx, l, nil)
}

func (d RepositoryCommandDrainer) DrainRepositoryCommandTree(ctx context.Context, primary contracts.RepositoryCommandLaunch, groups []contracts.RepositoryCommandLaunch) error {
	if err := d.drainRepositoryGroup(ctx, primary); err != nil {
		return err
	}
	for _, group := range groups {
		if err := d.drainRepositoryGroup(ctx, group); err != nil {
			return err
		}
	}
	return nil
}

func (d RepositoryCommandDrainer) drainRepositoryGroup(ctx context.Context, l contracts.RepositoryCommandLaunch) error {
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
