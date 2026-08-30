// Package processsupervisor owns guarded local provider processes. Adapters
// only provide argv; this package is the only os/exec boundary.
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
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

var ErrUnclear = errors.New("provider process drain is unclear")

// LaunchRecorder persists the exact child identity before the gate is opened.
// Implementations must fail closed; a supervisor never releases a child after
// recorder failure.
type LaunchRecorder interface {
	RecordLaunch(context.Context, contracts.DrainRequest, Identity, string) error
}

type Identity struct {
	PID, PGID            int
	BootIdentity         string
	ProcessStartIdentity string
}

type Supervisor struct {
	Signer   *contracts.DrainSigner
	Recorder LaunchRecorder
	trusted  map[domain.ProviderIdentity]trustedExecutable
	// Executable is the sf binary that implements __provider_gate. It is only
	// overridden by compiled-boundary tests; production uses os.Executable.
	Executable           string
	SoftDrain, HardDrain time.Duration
	mu                   sync.Mutex
	runs                 map[requestKey]*run
}
type trustedExecutable struct {
	path         string // immutable source spelling selected during qualification
	stagedPath   string // supervisor-owned executable snapshot
	stagedDir    string
	digest       string
	policyDigest string
	authDigest   string
	authHome     string
}
type run struct {
	identity Identity
	worktree string
	done     chan struct{}
	streams  chan struct{}
}

func New(recorder LaunchRecorder) (*Supervisor, error) {
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		return nil, err
	}
	return &Supervisor{Signer: signer, Recorder: recorder, trusted: map[domain.ProviderIdentity]trustedExecutable{}, SoftDrain: 2 * time.Second, HardDrain: 2 * time.Second, runs: map[requestKey]*run{}}, nil
}
func (s *Supervisor) PublicKey() []byte { return s.Signer.PublicKey() }
func (s *Supervisor) SetLaunchRecorder(recorder func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if recorder == nil {
		s.Recorder = nil
		return
	}
	s.Recorder = launchRecorderFunc(recorder)
}

// RegisterExecutable authenticates the exact provider executable selected by
// qualification. Production composition must register a path before a
// provider can run; invocation argv cannot select or replace it.
func (s *Supervisor) RegisterExecutable(identity domain.ProviderIdentity, path string) (string, error) {
	if s == nil || identity.Provider == "" || identity.Model == "" || identity.Family == "" || identity.Version == "" {
		return "", errors.New("complete provider identity is required")
	}
	trusted, err := authenticateExecutable(path)
	if err != nil {
		return "", err
	}
	// Legacy non-Codex fixtures use their directly registered executable. Real
	// Codex runtimes must use RegisterRuntime, which stages a pinned snapshot.
	trusted.stagedPath = trusted.path
	s.mu.Lock()
	trusted.policyDigest = environmentPolicyDigest()
	s.trusted[identity] = trusted
	s.mu.Unlock()
	return trusted.digest, nil
}

// RegisterRuntime installs the exact qualification binding and credential-home
// path used by an adapter. Credential bytes are never opened or copied. The
// executable is copied to a supervisor-owned immutable snapshot: on Darwin a
// same-UID attacker can still race the source before the copy, but cannot
// alter the staged bytes after their digest is checked against qualification.
func (s *Supervisor) RegisterRuntime(binding contracts.RuntimeBinding, executable, authHome string) (string, error) {
	if s == nil || binding.Identity.Provider != "codex" || binding.BinaryDigest == "" || binding.PolicyDigest != environmentPolicyDigest() || binding.AuthDigest == "" || authHome == "" {
		return "", errors.New("complete Codex runtime binding is required")
	}
	if err := privateExistingDirectory(authHome); err != nil {
		return "", errors.New("Codex authentication home is unsafe")
	}
	trusted, err := authenticateExecutable(executable)
	if err != nil {
		return "", err
	}
	if trusted.digest != binding.BinaryDigest {
		return "", errors.New("Codex executable digest does not match qualification")
	}
	if err := trusted.stage(); err != nil {
		return "", err
	}
	trusted.authDigest, trusted.authHome, trusted.policyDigest = binding.AuthDigest, authHome, binding.PolicyDigest
	s.mu.Lock()
	s.trusted[binding.Identity] = trusted
	s.mu.Unlock()
	return trusted.digest, nil
}

// PolicyDigest is the digest of the supervisor-owned environment policy. It
// lets the durable provider binding pin the policy without persisting any
// per-run secret or temporary directory name.
func (s *Supervisor) PolicyDigest() string { return environmentPolicyDigest() }

func environmentPolicyDigest() string {
	sum := sha256.Sum256([]byte("PATH=/usr/bin:/bin\x00LANG=C\x00LC_ALL=C\x00HOME=<private>\x00TMPDIR=<private>\x00CODEX_HOME=<private>"))
	return hex.EncodeToString(sum[:])
}

func authenticateExecutable(path string) (trustedExecutable, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return trustedExecutable{}, errors.New("trusted executable must be an absolute clean path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved {
		return trustedExecutable{}, errors.New("trusted executable could not be resolved")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Mode()&0o022 != 0 {
		return trustedExecutable{}, errors.New("trusted executable must be an executable private regular file")
	}
	if !trustedOwner(info) {
		return trustedExecutable{}, errors.New("trusted executable owner is not trusted")
	}
	for current := filepath.Dir(resolved); ; current = filepath.Dir(current) {
		entry, err := os.Lstat(current)
		if err != nil || entry.Mode()&os.ModeSymlink != 0 || !entry.IsDir() {
			return trustedExecutable{}, errors.New("trusted executable parent is not a real directory")
		}
		if !trustedOwner(entry) || entry.Mode()&0o022 != 0 {
			return trustedExecutable{}, errors.New("trusted executable parent is not private")
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	file, err := os.Open(resolved)
	if err != nil {
		return trustedExecutable{}, errors.New("trusted executable could not be opened")
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return trustedExecutable{}, errors.New("trusted executable digest could not be read")
	}
	return trustedExecutable{path: resolved, digest: hex.EncodeToString(hash.Sum(nil))}, nil
}

func (trusted *trustedExecutable) stage() error {
	if trusted == nil || trusted.path == "" || trusted.digest == "" {
		return errors.New("trusted executable is incomplete")
	}
	directory, err := os.MkdirTemp("", "sf-provider-exec-")
	if err != nil {
		return err
	}
	cleanup := func() { _ = os.RemoveAll(directory) }
	if err := os.Chmod(directory, 0o700); err != nil {
		cleanup()
		return err
	}
	source, err := os.Open(trusted.path)
	if err != nil {
		cleanup()
		return errors.New("trusted executable could not be opened for staging")
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() || !trustedOwner(info) || info.Mode().Perm()&0o022 != 0 {
		cleanup()
		return errors.New("trusted executable changed before staging")
	}
	targetPath := filepath.Join(directory, "provider")
	target, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		cleanup()
		return err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(target, hash), io.LimitReader(source, 128<<20))
	syncErr := target.Sync()
	closeErr := target.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != trusted.digest {
		cleanup()
		return errors.New("trusted executable changed while staging")
	}
	trusted.stagedPath, trusted.stagedDir = targetPath, directory
	return nil
}

func trustedOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	uid := uint32(os.Getuid())
	return stat.Uid == uid || stat.Uid == 0
}

type launchRecorderFunc func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error

func (f launchRecorderFunc) RecordLaunch(ctx context.Context, req contracts.DrainRequest, identity Identity, worktree string) error {
	return f(ctx, req, contracts.ProviderLaunch{PID: identity.PID, PGID: identity.PGID, BootIdentity: identity.BootIdentity, ProcessStartIdentity: identity.ProcessStartIdentity, Worktree: worktree})
}

type requestKey struct {
	ClaimID          int64
	Identity         domain.ProviderIdentity
	Ref              domain.TicketRef
	Phase            domain.Phase
	Role             string
	Attempt          int
	LeaderEpoch      uint64
	RunnerEpoch      uint64
	ExpectedVersion  uint64
	LeaseKey         string
	BindingDigest    string
	BinaryDigest     string
	PolicyDigest     string
	AuthDigest       string
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
}

func key(r contracts.DrainRequest) requestKey {
	return requestKey{ClaimID: r.ClaimID, Identity: r.Identity, Ref: r.Ref, Phase: r.Phase, Role: r.Role, Attempt: r.Attempt, LeaderEpoch: r.LeaderEpoch, RunnerEpoch: r.RunnerEpoch, ExpectedVersion: r.ExpectedVersion, LeaseKey: r.LeaseKey, BindingDigest: r.BindingDigest, BinaryDigest: r.BinaryDigest, PolicyDigest: r.PolicyDigest, AuthDigest: r.AuthDigest, Repository: r.Repository, Worktree: r.Worktree, WorktreeIdentity: r.WorktreeIdentity, BaseSHA: r.BaseSHA}
}

// validCodexInvocation is deliberately duplicated from the adapter's small
// code-owned shape. The supervisor—not a provider—decides which Codex flags
// are executable. In particular it excludes legacy sandbox, add-dir, config
// injection, hooks, plugin/MCP setup, and every free-form argv tail.
func validCodexInvocation(invocation contracts.Invocation, identity domain.ProviderIdentity, input contracts.PhaseInput) bool {
	parent, workspaceAccess := "", ""
	switch input.Phase {
	case domain.PhasePlanning, domain.PhaseReview:
		parent, workspaceAccess = "read-only", "read"
	case domain.PhaseVerification, domain.PhaseBuild:
		parent, workspaceAccess = "workspace", "write"
	default:
		return false
	}
	want := []string{invocation.Argv[0], "exec", "--ephemeral", "--json", "--ignore-user-config", "--ignore-rules", "--profile", "sf-guarded", "--config", `permissions.sf-guarded.extends=":` + parent + `"`, "--config", `permissions.sf-guarded.filesystem={":root"="deny",":minimal"="read",":workspace_roots"="` + workspaceAccess + `"}`, "--config", `permissions.sf-guarded.network.enabled=false`, "--model", identity.Model, "-C", input.Worktree, "--output-schema", contracts.OutputSchemaPlaceholder, "--output-last-message", contracts.OutputLastMessagePlaceholder, "-"}
	if len(invocation.Argv) != len(want) || !invocation.CaptureLastMessage || len(invocation.Stdin) == 0 || len(invocation.OutputSchema) == 0 {
		return false
	}
	for i := range want {
		if invocation.Argv[i] != want[i] {
			return false
		}
	}
	return true
}

func (s *Supervisor) Run(ctx context.Context, request contracts.DrainRequest, invocation contracts.Invocation, input contracts.PhaseInput) (contracts.CommandResult, error) {
	if len(invocation.Argv) == 0 || !filepath.IsAbs(invocation.Argv[0]) || input.Worktree == "" || filepath.Clean(input.Worktree) != input.Worktree {
		return contracts.CommandResult{}, errors.New("guarded argv and worktree required")
	}
	if request.ClaimID <= 0 || request.Ref.Validate() != nil || request.Phase == "" || (request.Role != "planner" && request.Role != "builder" && request.Role != "reviewer") || request.Attempt <= 0 || request.LeaderEpoch == 0 || request.RunnerEpoch == 0 || request.ExpectedVersion == 0 || request.LeaseKey == "" || request.BindingDigest == "" || request.BinaryDigest == "" || request.PolicyDigest == "" || request.Worktree == "" {
		return contracts.CommandResult{}, errors.New("complete provider claim identity is required")
	}
	s.mu.Lock()
	trusted, registered := s.trusted[request.Identity]
	s.mu.Unlock()
	if !registered {
		return contracts.CommandResult{}, errors.New("provider executable is not registered")
	}
	if invocation.Argv[0] != trusted.path {
		return contracts.CommandResult{}, errors.New("provider invocation executable does not match qualification")
	}
	if request.BinaryDigest != "" && request.BinaryDigest != trusted.digest {
		return contracts.CommandResult{}, errors.New("provider executable digest does not match claim")
	}
	if request.PolicyDigest != "" && request.PolicyDigest != trusted.policyDigest {
		return contracts.CommandResult{}, errors.New("provider environment policy does not match claim")
	}
	if request.Identity.Provider == "codex" {
		if input.Profile != contracts.ProfileGuarded || request.AuthDigest == "" || request.AuthDigest != trusted.authDigest || invocation.AuthHome != trusted.authHome || !validCodexInvocation(invocation, request.Identity, input) {
			return contracts.CommandResult{}, errors.New("Codex invocation does not match guarded registered runtime policy")
		}
	}
	providerPath := trusted.stagedPath
	if providerPath == "" {
		return contracts.CommandResult{}, errors.New("provider executable was not staged")
	}
	var executable *os.File
	if runtime.GOOS == "linux" {
		executable, err := os.Open(trusted.stagedPath)
		if err != nil {
			return contracts.CommandResult{}, errors.New("provider executable could not be pinned")
		}
		defer executable.Close()
		providerPath = fmt.Sprintf("/proc/self/fd/%d", 4) // FD 3 is the gate.
	}
	self := s.Executable
	if self == "" {
		var err error
		self, err = os.Executable()
		if err != nil {
			return contracts.CommandResult{}, err
		}
	}
	environment, temporary, cleanupEnvironment, err := vettedEnvironment(invocation.AuthHome)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer cleanupEnvironment()
	arguments, finalMessage, err := materializeInvocationFiles(invocation, temporary)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	gateRead, gateWrite, err := os.Pipe()
	if err != nil {
		return contracts.CommandResult{}, err
	}
	defer gateWrite.Close()
	argv := append([]string{"__provider_gate", providerPath}, arguments[1:]...)
	cmd := exec.Command(self, argv...)
	cmd.Dir = input.Worktree
	cmd.Env = environment
	cmd.Stdin = bytes.NewReader(invocation.Stdin)
	cmd.ExtraFiles = []*os.File{gateRead} // FD 3: wrapper exits on EOF before release.
	if executable != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, executable) // FD 4: pinned provider.
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = 64<<10, 64<<10
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		gateRead.Close()
		return contracts.CommandResult{}, err
	}
	gateRead.Close()
	startIdentity, identityErr := processStartIdentity(cmd.Process.Pid)
	if identityErr != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return contracts.CommandResult{}, ErrUnclear
	}
	bootIdentity, bootErr := hostBootIdentity()
	if bootErr != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return contracts.CommandResult{}, ErrUnclear
	}
	r := &run{identity: Identity{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: bootIdentity, ProcessStartIdentity: startIdentity}, worktree: input.Worktree, done: make(chan struct{}), streams: make(chan struct{})}
	s.mu.Lock()
	if _, exists := s.runs[key(request)]; exists {
		s.mu.Unlock()
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		return contracts.CommandResult{}, errors.New("provider claim is already running")
	}
	s.runs[key(request)] = r
	s.mu.Unlock()
	s.mu.Lock()
	recorder := s.Recorder
	s.mu.Unlock()
	if recorder == nil || recorder.RecordLaunch(ctx, request, r.identity, r.worktree) != nil {
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		close(r.done)
		close(r.streams)
		s.removeRun(request, r)
		return contracts.CommandResult{}, ErrUnclear
	}
	// Durable identity exists; the only release is closing the inherited gate.
	if _, err := gateWrite.Write([]byte{1}); err != nil {
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		s.removeRun(request, r)
		return contracts.CommandResult{}, ErrUnclear
	}
	if err := gateWrite.Close(); err != nil {
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		s.removeRun(request, r)
		return contracts.CommandResult{}, ErrUnclear
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait(); close(r.done); close(r.streams) }()
	var runErr error
	select {
	case runErr = <-wait:
	case <-ctx.Done():
		runErr = ctx.Err()
		_ = s.terminate(r)
		<-wait
	}
	if err := s.proveGone(r); err != nil {
		return contracts.CommandResult{}, err
	}
	lastMessage, lastTruncated, lastErr := readBoundedFile(finalMessage, 1<<20)
	result := contracts.CommandResult{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), OutputLastMessage: lastMessage, StdoutTruncated: stdout.exceeded(), StderrTruncated: stderr.exceeded(), OutputLastMessageTruncated: lastTruncated}
	if runErr != nil {
		result.ExitCode = -1
		return result, runErr
	}
	if lastErr != nil {
		return result, lastErr
	}
	result.ExitCode = 0
	return result, nil
}

func (s *Supervisor) removeRun(request contracts.DrainRequest, target *run) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.runs[key(request)]; current == target {
		delete(s.runs, key(request))
	}
}

func vettedEnvironment(authHome string) ([]string, string, func(), error) {
	home, err := os.MkdirTemp("", "sf-provider-home-")
	if err != nil {
		return nil, "", func() {}, err
	}
	tmp, err := os.MkdirTemp("", "sf-provider-tmp-")
	if err != nil {
		_ = os.RemoveAll(home)
		return nil, "", func() {}, err
	}
	if err := privateDirectory(home); err != nil {
		_ = os.RemoveAll(home)
		_ = os.RemoveAll(tmp)
		return nil, "", func() {}, err
	}
	if err := privateDirectory(tmp); err != nil {
		_ = os.RemoveAll(home)
		_ = os.RemoveAll(tmp)
		return nil, "", func() {}, err
	}
	environment := []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C", "HOME=" + home, "TMPDIR=" + tmp}
	if authHome != "" {
		if err := privateExistingDirectory(authHome); err != nil {
			_ = os.RemoveAll(home)
			_ = os.RemoveAll(tmp)
			return nil, "", func() {}, errors.New("provider authentication home is unsafe")
		}
		runtimeHome := filepath.Join(home, "codex")
		if err := copyCodexAuth(authHome, runtimeHome); err != nil {
			_ = os.RemoveAll(home)
			_ = os.RemoveAll(tmp)
			return nil, "", func() {}, errors.New("provider authentication runtime could not be prepared")
		}
		environment = append(environment, "CODEX_HOME="+runtimeHome)
	}
	return environment, tmp, func() {
		_ = os.RemoveAll(home)
		_ = os.RemoveAll(tmp)
	}, nil
}

func copyCodexAuth(sourceHome, destination string) error {
	if err := os.Mkdir(destination, 0o700); err != nil {
		return err
	}
	source := filepath.Join(sourceHome, "auth.json")
	info, err := os.Lstat(source)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || !trustedOwner(info) || info.Mode().Perm()&0o077 != 0 || info.Size() > 1<<20 {
		return errors.New("unsafe Codex auth file")
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(filepath.Join(destination, "auth.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, io.LimitReader(in, 1<<20+1))
	syncErr, closeErr := out.Sync(), out.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		return errors.New("could not copy Codex auth")
	}
	return os.Chmod(filepath.Join(destination, "auth.json"), 0o600)
}

func materializeInvocationFiles(invocation contracts.Invocation, temporary string) ([]string, string, error) {
	if len(invocation.Argv) == 0 {
		return nil, "", errors.New("provider invocation argv is required")
	}
	arguments := append([]string(nil), invocation.Argv...)
	schemaCount, outputCount := 0, 0
	outputPath := ""
	for index, argument := range arguments {
		if argument == contracts.OutputSchemaPlaceholder {
			schemaCount++
			if len(invocation.OutputSchema) == 0 || len(invocation.OutputSchema) > 1<<20 || !json.Valid(invocation.OutputSchema) {
				return nil, "", errors.New("provider output schema is invalid")
			}
			path := filepath.Join(temporary, "output-schema.json")
			if err := os.WriteFile(path, invocation.OutputSchema, 0o600); err != nil {
				return nil, "", err
			}
			arguments[index] = path
		}
		if argument == contracts.OutputLastMessagePlaceholder {
			outputCount++
			if !invocation.CaptureLastMessage {
				return nil, "", errors.New("provider final artifact output is not enabled")
			}
			outputPath = filepath.Join(temporary, "output-last-message.json")
			arguments[index] = outputPath
		}
	}
	if len(invocation.OutputSchema) == 0 && schemaCount != 0 || len(invocation.OutputSchema) != 0 && schemaCount != 1 {
		return nil, "", errors.New("provider output schema placeholder is invalid")
	}
	if invocation.CaptureLastMessage && outputCount != 1 || !invocation.CaptureLastMessage && outputCount != 0 {
		return nil, "", errors.New("provider final artifact placeholder is invalid")
	}
	if len(invocation.Stdin) > 64<<10 {
		return nil, "", errors.New("provider stdin exceeds limit")
	}
	return arguments, outputPath, nil
}

func readBoundedFile(path string, limit int64) ([]byte, bool, error) {
	if path == "" {
		return nil, false, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(contents)) > limit {
		return contents[:limit], true, nil
	}
	return contents, false, nil
}

func privateDirectory(path string) error {
	if err := os.Chmod(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !trustedOwner(info) {
		return errors.New("provider environment directory is not private")
	}
	return nil
}

// privateExistingDirectory is intentionally read-only: provider credential
// homes are operator-owned and the daemon must never repair their modes or
// otherwise mutate authentication state.
func privateExistingDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("provider authentication home must be an absolute clean directory")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return errors.New("provider authentication home contains a symlink")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 || !trustedOwner(info) {
		return errors.New("provider authentication home is not private")
	}
	return nil
}
func (s *Supervisor) Drain(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	s.mu.Lock()
	r := s.runs[key(request)]
	s.mu.Unlock()
	if r == nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	if err := s.terminate(r); err != nil {
		return contracts.DrainProof{}, err
	}
	if err := s.proveGone(r); err != nil {
		return contracts.DrainProof{}, err
	}
	s.removeRun(request, r)
	return s.Signer.ProveDrained(request)
}

// DrainPersisted is restart recovery: it accepts only the exact durable
// PID/PGID identity published through the launch gate.
func (s *Supervisor) DrainPersisted(ctx context.Context, request contracts.DrainRequest, launch contracts.ProviderLaunch) (contracts.DrainProof, error) {
	if launch.PID <= 0 || launch.PGID <= 0 || launch.PID != launch.PGID || launch.BootIdentity == "" || launch.ProcessStartIdentity == "" {
		return contracts.DrainProof{}, ErrUnclear
	}
	boot, err := hostBootIdentity()
	if err != nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	if bootIdentityChanged(launch.BootIdentity, boot) {
		return s.Signer.ProveDrained(request) // reboot proves every old group gone.
	}
	if leaderErr := syscall.Kill(launch.PID, 0); leaderErr == syscall.ESRCH {
		groupErr := syscall.Kill(-launch.PGID, 0)
		if missingLeaderGroupGone(leaderErr, groupErr) {
			return s.Signer.ProveDrained(request) // leader and every group member are gone.
		}
		return contracts.DrainProof{}, ErrUnclear
	} else if leaderErr != nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	pgid, err := syscall.Getpgid(launch.PID)
	if err != nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	actualStart, err := processStartIdentity(launch.PID)
	if err != nil || !persistedIdentityMatches(launch, actualStart, pgid) {
		return contracts.DrainProof{}, ErrUnclear
	}
	r := &run{identity: Identity{PID: launch.PID, PGID: launch.PGID, BootIdentity: boot, ProcessStartIdentity: actualStart}, done: make(chan struct{}), streams: make(chan struct{})}
	close(r.streams)
	go func() {
		for {
			if err := syscall.Kill(launch.PID, 0); err == syscall.ESRCH {
				close(r.done)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	if err := s.terminate(r); err != nil {
		return contracts.DrainProof{}, err
	}
	if err := s.proveGone(r); err != nil {
		return contracts.DrainProof{}, err
	}
	return s.Signer.ProveDrained(request)
}

func bootIdentityChanged(recorded, observed string) bool {
	return recorded != "" && observed != "" && recorded != observed
}
func missingLeaderGroupGone(leaderErr, groupErr error) bool {
	return leaderErr == syscall.ESRCH && groupErr == syscall.ESRCH
}

// persistedIdentityMatches is separated from signalling so a failed identity
// check has a mechanically obvious no-signal path. In particular, a rapid
// PID reuse with a matching human-readable lstart cannot reach terminate.
func persistedIdentityMatches(launch contracts.ProviderLaunch, observed string, observedPGID int) bool {
	return launch.PID > 0 && launch.PID == launch.PGID && launch.BootIdentity != "" && launch.ProcessStartIdentity != "" && observed == launch.ProcessStartIdentity && observedPGID == launch.PGID
}
func (s *Supervisor) terminate(r *run) error {
	_ = signalGroup(r.identity.PGID, syscall.SIGTERM)
	select {
	case <-r.done:
		return nil
	case <-time.After(s.SoftDrain):
	}
	_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
	select {
	case <-r.done:
		return nil
	case <-time.After(s.HardDrain):
		return ErrUnclear
	}
}
func (s *Supervisor) proveGone(r *run) error {
	select {
	case <-r.done:
	case <-time.After(s.HardDrain):
		return ErrUnclear
	}
	select {
	case <-r.streams:
	case <-time.After(s.HardDrain):
		return ErrUnclear
	}
	if err := syscall.Kill(-r.identity.PGID, 0); err == nil || err != syscall.ESRCH {
		return ErrUnclear
	}
	return nil
}
func signalGroup(pgid int, sig syscall.Signal) error {
	if pgid <= 0 {
		return ErrUnclear
	}
	err := syscall.Kill(-pgid, sig)
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

type limitedBuffer struct {
	bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	before := b.Len()
	if b.Len() < b.limit {
		n := b.limit - b.Len()
		if n > len(p) {
			n = len(p)
		}
		_, _ = b.Buffer.Write(p[:n])
	}
	if len(p) > b.limit-before {
		b.truncated = true
	}
	return len(p), nil
}

func (b *limitedBuffer) exceeded() bool { return b.truncated }

var _ io.Writer = (*limitedBuffer)(nil)
