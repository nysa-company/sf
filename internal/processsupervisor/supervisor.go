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
	"golang.org/x/sys/unix"
)

var ErrUnclear = errors.New("provider process drain is unclear")

const maxDrainDuration = 30 * time.Second

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
	retired              map[*stagedExecutable]struct{}
	changed              chan struct{}
	closing              bool
	closed               bool
	closeDone            chan struct{}
	closeErr             error
	// beforeStart is test-only synchronization for the selected-snapshot to
	// process-launch handoff. Production callers cannot install a hook.
	beforeStart func()
	// stageRuntime is test-only fault injection for the all-or-nothing staging
	// boundary. Nil uses trustedExecutable.stage in production.
	stageRuntime func(*trustedExecutable) error
}
type trustedExecutable struct {
	path         string // immutable source spelling selected during qualification
	stagedPath   string // supervisor-owned executable snapshot
	stagedDir    string
	snapshot     *stagedExecutable
	digest       string
	policyDigest string
	authDigest   string
	authMode     string
	authHome     string
}

// stagedExecutable is owned by the supervisor, unlike legacy registered
// executable paths. refs protects the interval from Run selecting a snapshot
// until that exact Run has completely returned, including the pre-launch
// preparation interval before a process is visible in runs.
type stagedExecutable struct {
	path, directory string
	refs            int
	cleaning        bool
}
type run struct {
	identity Identity
	worktree string
	done     chan struct{}
	streams  chan struct{}
	finished chan struct{}
	snapshot *stagedExecutable
}

func New(recorder LaunchRecorder) (*Supervisor, error) {
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		return nil, err
	}
	return &Supervisor{Signer: signer, Recorder: recorder, trusted: map[domain.ProviderIdentity]trustedExecutable{}, SoftDrain: 2 * time.Second, HardDrain: 2 * time.Second, runs: map[requestKey]*run{}, retired: map[*stagedExecutable]struct{}{}, changed: make(chan struct{})}, nil
}
func (s *Supervisor) PublicKey() []byte { return s.Signer.PublicKey() }

// AttestQualification is intentionally the only qualification-signing
// capability exposed by the process supervisor.  Adapters receive neither
// this method nor the private key.
func (s *Supervisor) AttestQualification(value contracts.QualificationAttestation) (contracts.QualificationAttestation, error) {
	if s == nil {
		return contracts.QualificationAttestation{}, errors.New("qualification supervisor is unavailable")
	}
	return s.Signer.SignQualification(value)
}
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
	if s.closing || s.closed {
		s.mu.Unlock()
		return "", errors.New("provider supervisor is closed")
	}
	previous := s.trusted[identity]
	trusted.policyDigest = environmentPolicyDigest()
	s.trusted[identity] = trusted
	s.retireLocked(previous)
	s.mu.Unlock()
	_ = s.reclaimRetired()
	return trusted.digest, nil
}

// RegisterRuntime installs the exact qualification binding and credential-home
// path used by an adapter. Credential bytes are never opened or copied. The
// executable is copied to a supervisor-owned immutable snapshot: on Darwin a
// same-UID attacker can still race the source before the copy, but cannot
// alter the staged bytes after their digest is checked against qualification.
func (s *Supervisor) RegisterRuntime(binding contracts.RuntimeBinding, executable, authHome string) (string, error) {
	if s == nil || binding.Identity.Provider != "codex" || binding.BinaryDigest == "" || binding.PolicyDigest != environmentPolicyDigest() || binding.AuthDigest == "" || binding.AuthMode != "chatgpt_subscription" || authHome == "" {
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
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		return "", errors.New("provider supervisor is closed")
	}
	if runtimeBindingMatches(s.trusted[binding.Identity], trusted, binding, authHome) {
		s.mu.Unlock()
		return trusted.digest, nil
	}
	s.mu.Unlock()
	// Stage before replacement. A failed stage therefore leaves the current
	// trusted binding untouched.
	s.mu.Lock()
	stage := s.stageRuntime
	s.mu.Unlock()
	if stage == nil {
		stage = func(value *trustedExecutable) error { return value.stage() }
	}
	if err := stage(&trusted); err != nil {
		return "", err
	}
	trusted.authDigest, trusted.authMode, trusted.authHome, trusted.policyDigest = binding.AuthDigest, binding.AuthMode, authHome, binding.PolicyDigest
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		_ = os.RemoveAll(trusted.stagedDir)
		return "", errors.New("provider supervisor is closed")
	}
	if runtimeBindingMatches(s.trusted[binding.Identity], trusted, binding, authHome) {
		s.mu.Unlock()
		_ = os.RemoveAll(trusted.stagedDir)
		return trusted.digest, nil
	}
	previous := s.trusted[binding.Identity]
	s.trusted[binding.Identity] = trusted
	s.retireLocked(previous)
	s.mu.Unlock()
	_ = s.reclaimRetired()
	return trusted.digest, nil
}

func runtimeBindingMatches(current, candidate trustedExecutable, binding contracts.RuntimeBinding, authHome string) bool {
	return current.snapshot != nil && stagedRuntimeMatches(current.snapshot, current.digest) && current.path == candidate.path && current.digest == candidate.digest &&
		current.policyDigest == binding.PolicyDigest && current.authDigest == binding.AuthDigest &&
		current.authMode == binding.AuthMode && current.authHome == authHome
}

// stagedRuntimeMatches rechecks a cached snapshot before a refresh reuses it.
// Supervisor-owned snapshots can still disappear after an operator cleanup or
// filesystem fault; treating that as a successful registration would defer a
// deterministic configuration failure until a later provider launch.
func stagedRuntimeMatches(snapshot *stagedExecutable, digest string) bool {
	if snapshot == nil || snapshot.path == "" || snapshot.directory == "" || filepath.Dir(snapshot.path) != snapshot.directory || digest == "" {
		return false
	}
	info, err := os.Lstat(snapshot.path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || info.Size() > 128<<20 || !trustedOwner(info) {
		return false
	}
	file, err := os.Open(snapshot.path)
	if err != nil {
		return false
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, io.LimitReader(file, 128<<20+1))
	closeErr := file.Close()
	return copyErr == nil && closeErr == nil && hex.EncodeToString(hash.Sum(nil)) == digest
}

// PolicyDigest is the digest of the supervisor-owned environment policy. It
// lets the durable provider binding pin the policy without persisting any
// per-run secret or temporary directory name.
func (s *Supervisor) PolicyDigest() string { return environmentPolicyDigest() }

// Close permanently closes the supervisor. It first stops new registration
// and launch, then drains every run that crossed the launch boundary. Staged
// snapshots are removed only after no Run can still reference them. An
// ambiguous drain leaves its snapshot on disk for operator recovery and the
// same error is returned by later Close calls.
func (s *Supervisor) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		s.mu.Lock()
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	s.closing, s.closeDone = true, make(chan struct{})
	runs := make(map[requestKey]*run, len(s.runs))
	for request, active := range s.runs {
		runs[request] = active
	}
	for identity, trusted := range s.trusted {
		delete(s.trusted, identity)
		s.retireLocked(trusted)
	}
	s.mu.Unlock()

	var result error
	for request, active := range runs {
		drainCtx, cancel, err := s.drainContext(context.Background())
		if err == nil {
			err = s.terminateContext(drainCtx, active)
			if err == nil {
				err = s.proveGoneContext(drainCtx, active)
			}
			cancel()
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("drain provider run %d: %w", request.ClaimID, err))
			continue
		}
		s.removeRunKey(request, active)
		if err := s.waitForRunReturn(active); err != nil {
			result = errors.Join(result, fmt.Errorf("join provider run %d: %w", request.ClaimID, err))
		}
	}

	if err := s.waitForSnapshotReferences(); err != nil {
		result = errors.Join(result, err)
	}
	if err := s.reclaimRetired(); err != nil {
		result = errors.Join(result, err)
	}
	s.mu.Lock()
	s.closeErr, s.closed = result, true
	close(s.closeDone)
	s.mu.Unlock()
	return result
}

func (s *Supervisor) retireLocked(trusted trustedExecutable) {
	if trusted.snapshot != nil {
		s.retired[trusted.snapshot] = struct{}{}
	}
}

func (s *Supervisor) acquireTrusted(identity domain.ProviderIdentity) (trustedExecutable, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.closed {
		return trustedExecutable{}, nil, errors.New("provider supervisor is closed")
	}
	trusted, found := s.trusted[identity]
	if !found {
		return trustedExecutable{}, nil, errors.New("provider executable is not registered")
	}
	if trusted.snapshot != nil {
		trusted.snapshot.refs++
	}
	return trusted, func() { s.releaseSnapshot(trusted.snapshot) }, nil
}

func (s *Supervisor) releaseSnapshot(snapshot *stagedExecutable) {
	if snapshot == nil {
		return
	}
	s.mu.Lock()
	if snapshot.refs > 0 {
		snapshot.refs--
	}
	s.signalChangedLocked()
	s.mu.Unlock()
	_ = s.reclaimRetired()
}

func (s *Supervisor) signalChangedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *Supervisor) waitForSnapshotReferences() error {
	deadline := s.SoftDrain + s.HardDrain
	if !validDrainDurations(s.SoftDrain, s.HardDrain) {
		return ErrUnclear
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	for {
		s.mu.Lock()
		busy := false
		for snapshot := range s.retired {
			if snapshot.refs != 0 {
				busy = true
				break
			}
		}
		changed := s.changed
		s.mu.Unlock()
		if !busy {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ErrUnclear
		}
	}
}

func (s *Supervisor) waitForRunReturn(active *run) error {
	if active == nil || active.finished == nil || !validDrainDurations(s.SoftDrain, s.HardDrain) {
		return ErrUnclear
	}
	timer := time.NewTimer(s.SoftDrain + s.HardDrain)
	defer timer.Stop()
	select {
	case <-active.finished:
		return nil
	case <-timer.C:
		return ErrUnclear
	}
}

func (s *Supervisor) reclaimRetired() error {
	for {
		s.mu.Lock()
		var target *stagedExecutable
		for snapshot := range s.retired {
			if snapshot.refs == 0 && !snapshot.cleaning {
				snapshot.cleaning = true
				target = snapshot
				break
			}
		}
		s.mu.Unlock()
		if target == nil {
			return nil
		}
		err := os.RemoveAll(target.directory)
		s.mu.Lock()
		if err == nil {
			delete(s.retired, target)
		} else {
			target.cleaning = false
		}
		s.signalChangedLocked()
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("remove retired provider executable: %w", err)
		}
	}
}

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
	trusted.snapshot = &stagedExecutable{path: targetPath, directory: directory}
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
	AuthMode         string
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
	RequestDigest    string
}

func key(r contracts.DrainRequest) requestKey {
	return requestKey{ClaimID: r.ClaimID, Identity: r.Identity, Ref: r.Ref, Phase: r.Phase, Role: r.Role, Attempt: r.Attempt, LeaderEpoch: r.LeaderEpoch, RunnerEpoch: r.RunnerEpoch, ExpectedVersion: r.ExpectedVersion, LeaseKey: r.LeaseKey, BindingDigest: r.BindingDigest, BinaryDigest: r.BinaryDigest, PolicyDigest: r.PolicyDigest, AuthDigest: r.AuthDigest, AuthMode: r.AuthMode, Repository: r.Repository, Worktree: r.Worktree, WorktreeIdentity: r.WorktreeIdentity, BaseSHA: r.BaseSHA, RequestDigest: r.RequestDigest}
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
	want := []string{invocation.Argv[0], "exec", "--ephemeral", "--json", "--ignore-user-config", "--ignore-rules", "--config", `default_permissions="sf-guarded"`, "--config", `permissions.sf-guarded.extends=":` + parent + `"`, "--config", `permissions.sf-guarded.filesystem={":root"="deny",":minimal"="read",":workspace_roots"="` + workspaceAccess + `"}`, "--config", `permissions.sf-guarded.network.enabled=false`, "--model", identity.Model, "-C", input.Worktree, "--output-schema", contracts.OutputSchemaPlaceholder, "--output-last-message", contracts.OutputLastMessagePlaceholder, "-"}
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
	if s == nil || !validDrainDurations(s.SoftDrain, s.HardDrain) {
		return contracts.CommandResult{}, errors.New("provider drain durations exceed the machine bound")
	}
	if len(invocation.Argv) == 0 || !filepath.IsAbs(invocation.Argv[0]) || input.Worktree == "" || filepath.Clean(input.Worktree) != input.Worktree {
		return contracts.CommandResult{}, errors.New("guarded argv and worktree required")
	}
	if request.ClaimID <= 0 || request.Ref.Validate() != nil || request.Phase == "" || (request.Role != "planner" && request.Role != "builder" && request.Role != "reviewer") || request.Attempt <= 0 || request.LeaderEpoch == 0 || request.RunnerEpoch == 0 || request.ExpectedVersion == 0 || request.LeaseKey == "" || request.BindingDigest == "" || request.BinaryDigest == "" || request.PolicyDigest == "" || request.Repository == "" || request.Worktree == "" || request.WorktreeIdentity == "" || request.BaseSHA == "" || !validRequestDigest(request.RequestDigest) {
		return contracts.CommandResult{}, errors.New("complete provider claim identity is required")
	}
	if !requestMatchesInput(request, input) || !contracts.PhaseInputDigestMatches(input, request.RequestDigest) {
		return contracts.CommandResult{}, errors.New("provider claim does not match phase input")
	}
	trusted, releaseSnapshot, trustedErr := s.acquireTrusted(request.Identity)
	if trustedErr != nil {
		return contracts.CommandResult{}, trustedErr
	}
	defer releaseSnapshot()
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
		if input.Profile != contracts.ProfileGuarded || input.AuthMode != "chatgpt_subscription" || request.AuthDigest == "" || request.AuthDigest != trusted.authDigest || request.AuthMode != trusted.authMode || invocation.AuthHome != trusted.authHome || !validCodexInvocation(invocation, request.Identity, input) {
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
	requestKey := key(request)
	if s.beforeStart != nil {
		s.beforeStart()
	}
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		gateRead.Close()
		return contracts.CommandResult{}, errors.New("provider supervisor is closed")
	}
	if _, exists := s.runs[requestKey]; exists {
		s.mu.Unlock()
		gateRead.Close()
		return contracts.CommandResult{}, errors.New("provider claim is already running")
	}
	// Hold the lifecycle lock from the final closed check through cmd.Start and
	// run registration. Close therefore cannot miss a process that was selected
	// before launch but had not yet reached the runs map.
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		gateRead.Close()
		return contracts.CommandResult{}, err
	}
	gateRead.Close()
	startIdentity, identityErr := processStartIdentity(cmd.Process.Pid)
	if identityErr != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		s.mu.Unlock()
		return contracts.CommandResult{}, ErrUnclear
	}
	bootIdentity, bootErr := hostBootIdentity()
	if bootErr != nil {
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		s.mu.Unlock()
		return contracts.CommandResult{}, ErrUnclear
	}
	r := &run{identity: Identity{PID: cmd.Process.Pid, PGID: cmd.Process.Pid, BootIdentity: bootIdentity, ProcessStartIdentity: startIdentity}, worktree: input.Worktree, done: make(chan struct{}), streams: make(chan struct{}), finished: make(chan struct{}), snapshot: trusted.snapshot}
	s.runs[requestKey] = r
	s.mu.Unlock()
	defer close(r.finished)
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
		return contracts.CommandResult{}, ErrUnclear
	}
	if err := gateWrite.Close(); err != nil {
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
		_ = cmd.Wait()
		return contracts.CommandResult{}, ErrUnclear
	}
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait(); close(r.done); close(r.streams) }()
	var runErr error
	select {
	case runErr = <-wait:
	case <-ctx.Done():
		runErr = ctx.Err()
		if terminateErr := s.terminate(r); terminateErr != nil {
			return contracts.CommandResult{}, terminateErr
		}
		select {
		case <-wait:
		case <-time.After(maxDrainDuration):
			return contracts.CommandResult{}, ErrUnclear
		}
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
	s.removeRunKey(key(request), target)
}

func (s *Supervisor) removeRunKey(request requestKey, target *run) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.runs[request]; current == target {
		delete(s.runs, request)
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
	opened, err := in.Stat()
	if err != nil || !sameFileIdentity(info, opened) {
		return errors.New("Codex auth file changed while opening")
	}
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

func sameFileIdentity(left, right os.FileInfo) bool {
	leftStat, leftOK := left.Sys().(*syscall.Stat_t)
	rightStat, rightOK := right.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && leftStat.Dev == rightStat.Dev && leftStat.Ino == rightStat.Ino && left.Mode() == right.Mode() && left.Size() == right.Size()
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
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || limit < 0 {
		return nil, false, errors.New("provider final artifact path is invalid")
	}
	// Open the parent and leaf by descriptor with no-follow flags. The final
	// artifact path is provider-controlled output; opening it by path would let
	// a symlink replacement turn a credential file into the returned artifact.
	parentFD, err := unix.Open(filepath.Dir(path), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, false, err
	}
	defer unix.Close(parentFD)
	name := filepath.Base(path)
	var before unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, false, err
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, false, errors.New("provider final artifact is not a regular file")
	}
	fd, err := unix.Openat(parentFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, false, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, false, errors.New("provider final artifact could not be opened")
	}
	defer file.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Dev != before.Dev || opened.Ino != before.Ino || opened.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, false, errors.New("provider final artifact changed while opening")
	}
	contents, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, false, err
	}
	var after unix.Stat_t
	if err := unix.Fstatat(parentFD, name, &after, unix.AT_SYMLINK_NOFOLLOW); err != nil || after.Dev != opened.Dev || after.Ino != opened.Ino || after.Mode&unix.S_IFMT != unix.S_IFREG || after.Size != opened.Size {
		return nil, false, errors.New("provider final artifact changed while reading")
	}
	if int64(len(contents)) > limit {
		return contents[:limit], true, nil
	}
	return contents, false, nil
}

func requestMatchesInput(request contracts.DrainRequest, input contracts.PhaseInput) bool {
	if request.Repository == "" || request.WorktreeIdentity == "" || request.BaseSHA == "" || request.Ref != input.Ticket || request.Phase != input.Phase || request.Identity != input.Provider || request.AuthMode != input.AuthMode || request.Repository != input.Repository || request.Worktree != input.Worktree || request.WorktreeIdentity != input.WorktreeIdentity || request.BaseSHA != input.BaseSHA || request.Attempt != input.Attempt || request.LeaderEpoch != input.LeaderEpoch || request.RunnerEpoch != input.RunnerEpoch || request.ExpectedVersion != input.ExpectedVersion || input.RequestDigest != request.RequestDigest {
		return false
	}
	switch request.Role {
	case "planner":
		return request.Phase == domain.PhasePlanning
	case "builder":
		return request.Phase == domain.PhaseBuild
	case "reviewer":
		return request.Phase == domain.PhaseVerification || request.Phase == domain.PhaseReview
	default:
		return false
	}
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
	if s == nil || !validRequestDigest(request.RequestDigest) {
		return contracts.DrainProof{}, ErrUnclear
	}
	drainCtx, cancel, err := s.drainContext(ctx)
	if err != nil {
		return contracts.DrainProof{}, err
	}
	defer cancel()
	s.mu.Lock()
	r := s.runs[key(request)]
	s.mu.Unlock()
	if r == nil {
		return contracts.DrainProof{}, ErrUnclear
	}
	if err := s.terminateContext(drainCtx, r); err != nil {
		return contracts.DrainProof{}, err
	}
	if err := s.proveGoneContext(drainCtx, r); err != nil {
		return contracts.DrainProof{}, err
	}
	s.removeRun(request, r)
	return s.Signer.ProveDrained(request)
}

// DrainPersisted is restart recovery for a qualified local provider. It
// proves only the recorded process group; v1 does not claim containment of
// hostile same-UID trees that escaped that group.
func (s *Supervisor) DrainPersisted(ctx context.Context, request contracts.DrainRequest, launch contracts.ProviderLaunch) (contracts.DrainProof, error) {
	if s == nil || !validRequestDigest(request.RequestDigest) {
		return contracts.DrainProof{}, ErrUnclear
	}
	drainCtx, cancel, err := s.drainContext(ctx)
	if err != nil {
		return contracts.DrainProof{}, err
	}
	defer cancel()
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
		// A missing leader plus an absent old group is not proof that a child
		// escaped with setsid/double-fork. Without an independent durable
		// descendant witness, retain quarantine rather than releasing it.
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
			case <-drainCtx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	if err := s.terminateContext(drainCtx, r); err != nil {
		return contracts.DrainProof{}, err
	}
	if err := s.proveGoneContext(drainCtx, r); err != nil {
		return contracts.DrainProof{}, err
	}
	return s.Signer.ProveDrained(request)
}

func bootIdentityChanged(recorded, observed string) bool {
	return recorded != "" && observed != "" && recorded != observed
}

// persistedIdentityMatches is separated from signalling so a failed identity
// check has a mechanically obvious no-signal path. In particular, a rapid
// PID reuse with a matching human-readable lstart cannot reach terminate.
func persistedIdentityMatches(launch contracts.ProviderLaunch, observed string, observedPGID int) bool {
	return launch.PID > 0 && launch.PID == launch.PGID && launch.BootIdentity != "" && launch.ProcessStartIdentity != "" && observed == launch.ProcessStartIdentity && observedPGID == launch.PGID
}

func validRequestDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// drainContext is the single wall-clock budget for TERM, KILL, stream drain,
// and final group observation. Caller cancellation always wins.
func (s *Supervisor) drainContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if !validDrainDurations(s.SoftDrain, s.HardDrain) || ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrUnclear
	}
	bounded, cancel := context.WithTimeout(ctx, s.SoftDrain+s.HardDrain)
	return bounded, cancel, nil
}

func validDrainDurations(soft, hard time.Duration) bool {
	return soft > 0 && hard > 0 && soft <= maxDrainDuration && hard <= maxDrainDuration && soft <= maxDrainDuration-hard
}

func (s *Supervisor) terminate(r *run) error {
	return s.terminateContext(context.Background(), r)
}
func (s *Supervisor) terminateContext(ctx context.Context, r *run) error {
	if r == nil || r.identity.PID <= 0 || r.identity.PGID != r.identity.PID {
		return ErrUnclear
	}
	select {
	case <-r.done:
		return nil
	default:
	}
	if err := validateLiveIdentity(r.identity); err != nil {
		return err
	}
	_ = signalGroup(r.identity.PGID, syscall.SIGTERM)
	select {
	case <-r.done:
		return nil
	case <-time.After(s.SoftDrain):
	case <-ctx.Done():
		return ErrUnclear
	}
	_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
	select {
	case <-r.done:
		return nil
	case <-time.After(s.HardDrain):
	case <-ctx.Done():
		return ErrUnclear
	}
	return ErrUnclear
}

func validateLiveIdentity(identity Identity) error {
	if identity.PID <= 0 || identity.PGID != identity.PID || identity.BootIdentity == "" || identity.ProcessStartIdentity == "" {
		return ErrUnclear
	}
	if err := syscall.Kill(identity.PID, 0); err != nil {
		return ErrUnclear
	}
	pgid, err := syscall.Getpgid(identity.PID)
	if err != nil || pgid != identity.PGID {
		return ErrUnclear
	}
	start, err := processStartIdentity(identity.PID)
	if err != nil || start != identity.ProcessStartIdentity {
		return ErrUnclear
	}
	return nil
}
func (s *Supervisor) proveGone(r *run) error {
	return s.proveGoneContext(context.Background(), r)
}
func (s *Supervisor) proveGoneContext(ctx context.Context, r *run) error {
	select {
	case <-r.done:
	case <-ctx.Done():
		return ErrUnclear
	}
	select {
	case <-r.streams:
	case <-ctx.Done():
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
