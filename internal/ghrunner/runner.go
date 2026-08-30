// Package ghrunner is the supervised process boundary for the GitHub CLI.
//
// A Runner is deliberately bound to one executable and one small environment
// contract. It never invokes a shell, retains command output, or logs command
// inputs. Cleanup is a one-shot proof: callers must obtain it before allowing
// another externally mutating operation. Process groups are a trusted local
// same-UID boundary; this package does not claim containment against a
// hostile same-UID process that escapes with setsid or a retained descriptor.
// In particular, a detached descendant that closes both output descriptors
// cannot be distinguished from a drained group using these local witnesses;
// callers must treat the proof as a profile-bound local handoff, not hostile
// process containment.
package ghrunner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/github"
)

const (
	// MaxOutput is the combined stdout+stderr bound.
	MaxOutput = 1 << 20
	// MaxDeadline is the maximum owned-child budget. Trusted local filesystem
	// preflight happens synchronously before this budget starts and may be
	// delayed by the OS; it creates no child and is not a mutating operation.
	MaxDeadline = 2 * time.Minute
	// MaxRunWallClock includes the owned-child budget and bounded termination,
	// fast-exit identity handoff, and pipe-drain overhead. It excludes trusted
	// local preflight before Start.
	MaxRunWallClock     = MaxDeadline + identitySampleGrace + termWait + killWait + pipeWait*2
	maxArg              = 64 << 10
	maxArgs             = 128
	maxInput            = 256 << 10
	maxWait             = MaxDeadline
	termWait            = 500 * time.Millisecond
	killWait            = 500 * time.Millisecond
	pipeWait            = 500 * time.Millisecond
	cleanupWait         = 2 * time.Second
	identitySampleGrace = 2 * time.Second
)

var (
	ErrInvalidExecutable        = errors.New("gh executable is invalid")
	ErrExecutableChanged        = errors.New("gh executable changed after authentication")
	ErrInvalidEnvironment       = errors.New("gh environment is outside the minimal contract")
	ErrInvalidCommand           = errors.New("gh command is invalid")
	ErrOutputTooLarge           = errors.New("gh output exceeded the bound")
	ErrConcurrentRun            = github.ErrRunnerBusy
	ErrCleanupBeforeRun         = errors.New("gh cleanup has no preceding run")
	ErrCleanupAlreadyUsed       = errors.New("gh cleanup proof was already consumed")
	ErrExternalCleanupUncertain = contracts.ErrExternalCleanupUncertain
	ErrRunnerClosed             = errors.New("gh runner is closed")
	ErrCleanupInProgress        = errors.New("gh cleanup is already in progress")
	hostBootIdentityFn          = hostBootIdentity
	processStartIdentityFn      = processStartIdentity
	validatedEnvironmentFn      = validatedEnvironment
)

// Runner is safe for one production GitHub client. It is not a pool: a
// second Run is refused until the preceding Run has a consumed Cleanup proof.
type Runner struct {
	mu               sync.Mutex
	requested        string
	canonical        string
	digest           string
	active           *run
	needsCleanup     bool
	cleanupUsed      bool
	prelaunchPending bool
	snapshotPath     string
	snapshotDir      string
	removeSnapshot   func(string) error
	closing          bool
	closed           bool
}

var _ github.SupervisedCommandRunner = (*Runner)(nil)

// New authenticates executable immediately and binds its canonical path and
// SHA-256 digest. The input may be a symlink, but the symlink target is pinned
// and must resolve to the same target before every start.
func New(executable string) (*Runner, error) {
	requested, canonical, digest, err := authenticate(executable)
	if err != nil {
		return nil, err
	}
	snapshot, directory, err := snapshotExecutable(canonical, digest)
	if err != nil {
		return nil, err
	}
	return &Runner{requested: requested, canonical: canonical, digest: digest, snapshotPath: snapshot, snapshotDir: directory, removeSnapshot: os.RemoveAll}, nil
}

// NewRunner is an explicit spelling of New for composition code.
func NewRunner(executable string) (*Runner, error) { return New(executable) }

// Digest returns the constructor-time executable digest. It contains no
// command or credential material and is useful for durable qualification.
func (r *Runner) Digest() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.digest
}

// Path returns the canonical executable path bound by the constructor.
func (r *Runner) Path() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.canonical
}

// Close removes the private executable snapshot after all runs have been
// drained. A live or pending run is never discarded.
func (r *Runner) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	if r.active != nil || r.needsCleanup || r.closing {
		r.mu.Unlock()
		return ErrConcurrentRun
	}
	directory := r.snapshotDir
	removeSnapshot := r.removeSnapshot
	if removeSnapshot == nil {
		removeSnapshot = os.RemoveAll
	}
	r.closing = true
	r.mu.Unlock()
	if directory == "" {
		r.mu.Lock()
		r.closing = false
		r.closed = true
		r.mu.Unlock()
		return nil
	}
	if err := removeSnapshot(directory); err != nil {
		// Retain both paths so a caller can retry after a transient removal
		// failure. Clearing them before RemoveAll would make the snapshot
		// permanently unreachable and would lose cleanup state.
		r.mu.Lock()
		r.closing = false
		r.mu.Unlock()
		return err
	}
	r.mu.Lock()
	if r.snapshotDir == directory {
		r.snapshotDir, r.snapshotPath = "", ""
	}
	r.closing = false
	r.closed = true
	r.mu.Unlock()
	return nil
}

// markPrelaunch records a failed launch attempt that never created an owned
// process. The next Cleanup can therefore return a definite drained proof;
// an invalid attempt cannot accidentally quarantine the whole client.
func (r *Runner) markPrelaunch() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markPrelaunchLocked()
}

func (r *Runner) markPrelaunchLocked() {
	if r.active == nil && !r.needsCleanup {
		if r.closing || r.closed {
			return
		}
		r.needsCleanup = true
		r.prelaunchPending = true
		r.cleanupUsed = false
	}
}

type run struct {
	cmd                    *exec.Cmd
	pid, pgid              int
	identityMu             sync.RWMutex
	boot, start            string
	stdout, stderr         *limitedBuffer
	stdoutRead, stderrRead *os.File
	waitDone               chan error
	ownerGone              chan struct{}
	streamsDone            chan struct{}
	ownerDone              bool
	waitErr                error
	streamsUncertain       bool
	identityUncertain      atomic.Bool
	finished               chan struct{}
	cleaned                bool // protected by Runner.mu; final proof consumed
	cleanupInProgress      bool // protected by Runner.mu; retryable claim
}

// Run starts exactly binary with args and env. The binary must equal the
// canonical path bound by New; args exclude argv[0].
func (r *Runner) Run(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	if r == nil {
		return nil, ErrInvalidCommand
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrRunnerClosed
	}
	if r.active != nil || r.needsCleanup || r.closing {
		r.mu.Unlock()
		return nil, ErrConcurrentRun
	}
	if ctx == nil {
		r.markPrelaunchLocked()
		r.mu.Unlock()
		return nil, ErrInvalidCommand
	}
	if err := validateCommand(binary, args); err != nil {
		r.markPrelaunchLocked()
		r.mu.Unlock()
		return nil, err
	}
	safeEnv, err := validatedEnvironmentFn(env)
	if err != nil {
		r.markPrelaunchLocked()
		r.mu.Unlock()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		r.markPrelaunchLocked()
		r.mu.Unlock()
		return nil, err
	}

	binaryCanonical, binaryErr := filepath.EvalSymlinks(binary)
	if binaryErr != nil || binaryCanonical != r.canonical {
		r.mu.Unlock()
		r.markPrelaunch()
		return nil, ErrInvalidExecutable
	}
	// Re-authenticate as the final pre-start operation. This catches target
	// replacement and symlink retargeting between construction and launch.
	_, current, digest, err := authenticate(r.requested)
	snapshotDigest, snapshotErr := authenticateSnapshot(r.snapshotPath)
	if err != nil || current != r.canonical || digest != r.digest || snapshotErr != nil || snapshotDigest != r.digest {
		r.mu.Unlock()
		r.markPrelaunch()
		return nil, ErrExecutableChanged
	}
	// Revalidate the canonical environment immediately before Start as well;
	// an attacker may chmod, replace, or retarget a directory after the first
	// validation performed before taking the runner lock.
	safeEnv, err = validatedEnvironmentFn(env)
	if err != nil {
		r.markPrelaunchLocked()
		r.mu.Unlock()
		return nil, err
	}
	command := exec.Command(r.snapshotPath, args...)
	command.Env = safeEnv
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = termWait + killWait + pipeWait
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		r.needsCleanup = true
		r.prelaunchPending = true
		r.mu.Unlock()
		return nil, err
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		r.needsCleanup = true
		r.prelaunchPending = true
		r.mu.Unlock()
		return nil, err
	}
	command.Stdout, command.Stderr = stdoutWrite, stderrWrite
	stdout := &limitedBuffer{limit: MaxOutput}
	// One shared sink bounds the combined stdout+stderr envelope to 1 MiB.
	// Separate pipes are retained so a noisy stream cannot deadlock the other.
	stderr := stdout
	// The owned-child budget starts immediately before Start. All preceding
	// work is trusted local preflight: it performs no child mutation, but may
	// be delayed synchronously by the OS and is therefore not described as a
	// hard context bound.
	bounded, cancel := boundedContext(ctx)
	defer cancel()
	runDeadline, _ := bounded.Deadline()
	if err := bounded.Err(); err != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		_ = stderrRead.Close()
		_ = stderrWrite.Close()
		r.needsCleanup = true
		r.prelaunchPending = true
		r.mu.Unlock()
		return nil, err
	}
	if err := command.Start(); err != nil {
		// Start failure is definite: no process owns either write end.
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		_ = stderrRead.Close()
		_ = stderrWrite.Close()
		r.needsCleanup = true
		r.prelaunchPending = true
		r.mu.Unlock()
		return nil, err
	}
	// Parent write ends must be closed immediately. An inherited parent end
	// would make a retained descendant indistinguishable from a live owner.
	_ = stdoutWrite.Close()
	_ = stderrWrite.Close()
	currentRun := &run{cmd: command, pid: command.Process.Pid, pgid: command.Process.Pid, stdout: stdout, stderr: stderr, stdoutRead: stdoutRead, stderrRead: stderrRead, waitDone: make(chan error, 1), ownerGone: make(chan struct{}), streamsDone: make(chan struct{}), finished: make(chan struct{})}
	r.active = currentRun
	r.cleanupUsed = false
	r.needsCleanup = true
	r.mu.Unlock()

	// Install Wait immediately after Start. A very short-lived owner may exit
	// before identity sampling; its completion is then a legitimate drained
	// launch, not an identity-fault quarantine.
	go func() {
		err := command.Wait()
		currentRun.waitDone <- err
		close(currentRun.ownerGone)
	}()

	var streams sync.WaitGroup
	streams.Add(2)
	go func() { defer streams.Done(); _, _ = io.Copy(stdout, stdoutRead) }()
	go func() { defer streams.Done(); _, _ = io.Copy(stderr, stderrRead) }()
	go monitorLeader(currentRun)
	go func() { streams.Wait(); close(currentRun.streamsDone) }()

	// Identity samplers are extension points for platform-specific process
	// identity. Run them asynchronously: an injected or OS-stalled sampler
	// must not defeat the owned-child deadline or strand Run in a synchronous
	// call. The buffered result permits the sampler to finish without needing
	// Run to remain alive.
	type identityResult struct {
		boot, start       string
		bootErr, startErr error
	}
	identityCh := make(chan identityResult, 1)
	go func() {
		boot, bootErr := hostBootIdentityFn()
		start, startErr := processStartIdentityFn(command.Process.Pid)
		identityCh <- identityResult{boot: boot, start: start, bootErr: bootErr, startErr: startErr}
	}()
	var identities identityResult
	identityTimedOut := false
	select {
	case identities = <-identityCh:
		currentRun.identityMu.Lock()
		currentRun.boot, currentRun.start = identities.boot, identities.start
		currentRun.identityMu.Unlock()
	case <-bounded.Done():
		identityTimedOut = true
	}
	identityFailed := identityTimedOut || identities.bootErr != nil || identities.startErr != nil
	ownerExited := false
	select {
	case <-currentRun.ownerGone:
		ownerExited = true
	default:
	}
	// An ordinary identity read can race a legitimate fast exit. Give the
	// already-installed Wait a bounded handoff before treating a sampler error
	// as an external identity ambiguity. A sampler timeout itself remains an
	// uncertainty even if the owner happened to exit while it was blocked.
	if identityFailed && !identityTimedOut && !ownerExited {
		select {
		case <-currentRun.ownerGone:
			ownerExited = true
		case <-time.After(identitySampleGrace):
		}
	}
	if identityFailed && !identityTimedOut && ownerExited {
		identityFailed = false
	}
	if identityFailed {
		// The process record exists before identity faults are acted upon. Kill
		// the newly-created group and wait only for the bounded handoff; Cleanup
		// will quarantine because identity could not be proven.
		currentRun.identityUncertain.Store(true)
		_ = signalGroup(currentRun.pgid, syscall.SIGKILL)
		remaining := time.Until(runDeadline)
		if remaining > 0 {
			select {
			case <-currentRun.ownerGone:
				currentRun.ownerDone = true
			case <-time.After(minDuration(termWait+killWait, remaining)):
				currentRun.ownerDone = false
			}
		}
	}

	// The owned-child context covers post-start execution and its bounded
	// termination handoff; trusted preflight occurred before this boundary.
	var waitErr error
	cancelled := false
	if identityFailed {
		waitErr = ErrExternalCleanupUncertain
	} else {
		select {
		case waitErr = <-currentRun.waitDone:
			currentRun.ownerDone = true
		case <-bounded.Done():
			cancelled = true
			terminated := r.terminate(currentRun, bounded, runDeadline)
			currentRun.identityUncertain.Store(!terminated)
			remaining := time.Until(runDeadline)
			if remaining > 0 {
				select {
				case waitErr = <-currentRun.waitDone:
					currentRun.ownerDone = true
				case <-time.After(minDuration(termWait+killWait, remaining)):
					currentRun.identityUncertain.Store(true)
				}
			} else {
				// terminate already issued the final group kill. Cleanup will wait
				// for ownerGone and independently prove the group and pipes gone.
			}
			if waitErr == nil {
				waitErr = bounded.Err()
			}
		}
	}
	if cancelled {
		waitErr = bounded.Err()
	}
	currentRun.waitErr = waitErr
	// A pipe holder can survive its owner. Never wait indefinitely; close our
	// read ends after the bounded handoff and carry uncertainty to Cleanup.
	remaining := time.Until(runDeadline)
	if remaining <= 0 {
		select {
		case <-currentRun.streamsDone:
		default:
			// RunMax deliberately includes this final bounded grace period so
			// normal group termination can close both readers without turning a
			// deadline into spurious quarantine.
			select {
			case <-currentRun.streamsDone:
			case <-time.After(pipeWait):
				currentRun.streamsUncertain = true
				_ = stdoutRead.Close()
				_ = stderrRead.Close()
			}
		}
	} else {
		select {
		case <-currentRun.streamsDone:
		case <-time.After(minDuration(pipeWait, remaining)):
			currentRun.streamsUncertain = true
			_ = stdoutRead.Close()
			_ = stderrRead.Close()
			remaining = time.Until(runDeadline)
			if remaining > 0 {
				select {
				case <-currentRun.streamsDone:
				case <-time.After(minDuration(pipeWait, remaining)):
					currentRun.streamsUncertain = true
				}
			}
		}
	}
	_ = stdoutRead.Close()
	_ = stderrRead.Close()
	r.mu.Lock()
	// Keep this run as the immediately preceding run until Cleanup consumes it.
	r.needsCleanup = true
	r.mu.Unlock()
	// Every non-atomic lifecycle field is now final. Cleanup waits for this
	// marker before it can inspect or publish a proof.
	close(currentRun.finished)
	output := stdout.Bytes()
	if identityFailed {
		return output, ErrExternalCleanupUncertain
	}
	if stdout.Truncated() && waitErr == nil {
		return output, ErrOutputTooLarge
	}
	return output, waitErr
}

// Cleanup consumes the one proof associated with the immediately preceding
// Run. Ambiguity always returns a quarantine proof and uncertainty error.
func (r *Runner) Cleanup(ctx context.Context) (github.CleanupProof, error) {
	if r == nil || ctx == nil {
		return github.CleanupProof{}, ErrCleanupBeforeRun
	}
	r.mu.Lock()
	current := r.active
	if current == nil && r.prelaunchPending {
		r.prelaunchPending = false
		r.needsCleanup = false
		r.cleanupUsed = true
		r.mu.Unlock()
		return github.CleanupProof{Drained: true}, nil
	}
	if current != nil && current.cleaned {
		r.mu.Unlock()
		return github.CleanupProof{}, ErrCleanupAlreadyUsed
	}
	if current != nil && current.cleanupInProgress {
		r.mu.Unlock()
		return github.CleanupProof{}, ErrCleanupInProgress
	}
	if !r.needsCleanup || current == nil {
		used := r.cleanupUsed
		r.mu.Unlock()
		if used {
			return github.CleanupProof{}, ErrCleanupAlreadyUsed
		}
		return github.CleanupProof{}, ErrCleanupBeforeRun
	}
	current.cleanupInProgress = true
	r.mu.Unlock()
	bounded, cancel := boundedCleanupContext(ctx)
	defer cancel()
	// Run owns all lifecycle and stream state until this marker. Claiming the
	// proof above prevents a second Cleanup, while this wait prevents a first
	// Cleanup from observing partially-written owner/stream/wait state.
	select {
	case <-current.finished:
	case <-bounded.Done():
		r.releaseCleanupClaim(current)
		return github.CleanupProof{}, cleanupContextError(bounded)
	}
	if !current.ownerDone {
		select {
		case <-current.ownerGone:
			// Run normally records this before closing finished. This fallback is
			// only for a completed owner whose channel notification raced the
			// final state write; no other goroutine reads this field now.
			current.ownerDone = true
		case <-bounded.Done():
			return r.consumeCleanupUncertain(current)
		}
	}
	if current.streamsUncertain || current.identityUncertain.Load() {
		return r.consumeCleanupUncertain(current)
	}
	if err := observeLeader(bounded, current); err != nil {
		return r.consumeCleanupUncertain(current)
	}
	if err := waitGroupGone(bounded, current.pgid); err != nil {
		return r.consumeCleanupUncertain(current)
	}
	select {
	case <-current.streamsDone:
	default:
		return r.consumeCleanupUncertain(current)
	}
	r.mu.Lock()
	r.active = nil
	r.needsCleanup = false
	current.cleaned = true
	current.cleanupInProgress = false
	r.cleanupUsed = true
	r.mu.Unlock()
	return github.CleanupProof{Drained: true}, nil
}

func (r *Runner) releaseCleanupClaim(current *run) {
	r.mu.Lock()
	if r.active == current && !current.cleaned {
		current.cleanupInProgress = false
	}
	r.mu.Unlock()
}

func (r *Runner) consumeCleanupUncertain(current *run) (github.CleanupProof, error) {
	r.mu.Lock()
	if r.active == current {
		current.cleaned = true
		current.cleanupInProgress = false
		r.cleanupUsed = true
	}
	r.mu.Unlock()
	return quarantine()
}

func cleanupContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return context.DeadlineExceeded
}

func quarantine() (github.CleanupProof, error) {
	return github.CleanupProof{Quarantined: true}, ErrExternalCleanupUncertain
}

func (r *Runner) terminate(current *run, bounded context.Context, deadline time.Time) bool {
	if current == nil || current.pid <= 0 || current.pgid != current.pid {
		return false
	}
	// Once the single wall-clock budget is exhausted there is no wait budget
	// left, so issue the group kill immediately and let Cleanup await the owner
	// asynchronously.
	if time.Until(deadline) <= 0 && !current.identityUncertain.Load() {
		err := signalGroup(current.pgid, syscall.SIGKILL)
		return err == nil || errors.Is(err, syscall.ESRCH)
	}
	// Parent cancellation can close bounded before its derived deadline. Give a
	// final identity observation only a small independent handoff window; if an
	// injected reader stalls, force-kill the known launch group rather than
	// letting the reader delay cancellation.
	if bounded.Err() != nil {
		observation, cancelObservation := context.WithTimeout(context.Background(), termWait)
		observeErr := observeLeader(observation, current)
		cancelObservation()
		if observeErr != nil {
			_ = signalGroup(current.pgid, syscall.SIGKILL)
			select {
			case <-current.ownerGone:
				return false
			case <-time.After(killWait):
				return false
			}
		}
		_ = signalGroup(current.pgid, syscall.SIGTERM)
		select {
		case <-current.ownerGone:
			return true
		case <-time.After(termWait):
		}
		_ = signalGroup(current.pgid, syscall.SIGKILL)
		select {
		case <-current.ownerGone:
			return true
		case <-time.After(killWait):
			return false
		}
	}
	if err := observeLeader(bounded, current); err != nil {
		_ = signalGroup(current.pgid, syscall.SIGKILL)
		return false
	}
	if err := signalGroup(current.pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false
	}
	wait := termWait
	if remaining := time.Until(deadline); remaining < wait {
		wait = remaining
	}
	if wait > 0 {
		select {
		case <-current.ownerGone:
			return true
		case <-time.After(wait):
		}
	}
	if err := observeLeader(bounded, current); err != nil {
		_ = signalGroup(current.pgid, syscall.SIGKILL)
		return false
	}
	if err := signalGroup(current.pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false
	}
	if remaining := time.Until(deadline); remaining > 0 {
		select {
		case <-current.ownerGone:
			return true
		case <-time.After(minDuration(killWait, remaining)):
		}
	}
	return false
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// monitorLeader records a changed process-group identity while the owner is
// alive. A later ESRCH for the original group is never treated as proof after
// such a change. This is an identity observation only; the supervisor makes
// no hostile same-UID containment claim.
func monitorLeader(current *run) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-current.ownerGone:
			return
		case <-ticker.C:
			if syscall.Kill(current.pid, 0) != nil {
				continue
			}
			pgid, err := syscall.Getpgid(current.pid)
			if err != nil {
				// A process can be observed in the brief unreaped-exit state;
				// failure to read its identity there is not a group change.
				continue
			}
			if pgid != current.pgid {
				current.identityUncertain.Store(true)
				return
			}
		}
	}
}

func observeLeader(ctx context.Context, current *run) error {
	// Re-sampling protects against PID reuse, but the reader is an extension
	// point that may stall. Keep it behind a bounded, buffered observation so
	// Cleanup and cancellation never synchronously wait on it.
	if current == nil || current.pid <= 0 {
		return ErrExternalCleanupUncertain
	}
	err := syscall.Kill(current.pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		return ErrExternalCleanupUncertain
	}
	pgid, err := syscall.Getpgid(current.pid)
	if err != nil || pgid != current.pgid {
		return ErrExternalCleanupUncertain
	}
	current.identityMu.RLock()
	knownStart := current.start
	current.identityMu.RUnlock()
	if knownStart == "" {
		return ErrExternalCleanupUncertain
	}
	start, sampleErr := boundedProcessStartIdentity(ctx, current.pid)
	if sampleErr != nil || start != knownStart {
		return ErrExternalCleanupUncertain
	}
	return nil
}

func boundedProcessStartIdentity(ctx context.Context, pid int) (string, error) {
	type result struct {
		value string
		err   error
	}
	results := make(chan result, 1)
	go func() {
		value, err := processStartIdentityFn(pid)
		results <- result{value: value, err: err}
	}()
	select {
	case result := <-results:
		return result.value, result.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func waitGroupGone(ctx context.Context, pgid int) error {
	if pgid <= 0 {
		return ErrExternalCleanupUncertain
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return ErrExternalCleanupUncertain
		}
		select {
		case <-ctx.Done():
			return ErrExternalCleanupUncertain
		case <-ticker.C:
		}
	}
}

func signalGroup(pgid int, signal syscall.Signal) error {
	if pgid <= 0 {
		return ErrExternalCleanupUncertain
	}
	return syscall.Kill(-pgid, signal)
}

type limitedBuffer struct {
	mu        sync.Mutex
	data      bytes.Buffer
	limit     int
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	room := b.limit - b.data.Len()
	if room > 0 {
		n := len(p)
		if n > room {
			n = room
		}
		_, _ = b.data.Write(p[:n])
	}
	if len(p) > room {
		b.truncated = true
	}
	return len(p), nil
}
func (b *limitedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data.Bytes()...)
}

func (b *limitedBuffer) Truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.truncated
}

func authenticate(input string) (string, string, string, error) {
	if input == "" || !filepath.IsAbs(input) || filepath.Clean(input) != input || strings.IndexByte(input, 0) >= 0 {
		return "", "", "", ErrInvalidExecutable
	}
	canonical, err := filepath.EvalSymlinks(input)
	if err != nil || !filepath.IsAbs(canonical) || filepath.Clean(canonical) != canonical {
		return "", "", "", ErrInvalidExecutable
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Mode()&0o022 != 0 || !trustedOwner(info) || !secureAncestors(canonical) {
		return "", "", "", ErrInvalidExecutable
	}
	file, err := os.Open(canonical)
	if err != nil {
		return "", "", "", ErrInvalidExecutable
	}
	defer file.Close()
	hash := sha256.New()
	const maxExecutableSize = 128 << 20
	count, err := io.Copy(hash, io.LimitReader(file, maxExecutableSize+1))
	if err != nil || count > maxExecutableSize {
		return "", "", "", ErrInvalidExecutable
	}
	return input, canonical, hex.EncodeToString(hash.Sum(nil)), nil
}

func snapshotExecutable(source, digest string) (string, string, error) {
	directory, err := os.MkdirTemp(filepath.Dir(source), ".sf-gh-exec-")
	if err != nil {
		// A trusted executable directory may be read-only (for example a
		// package-managed /usr/bin). Keep the snapshot in a private temp dir;
		// the source itself was already validated through its full chain.
		directory, err = os.MkdirTemp("", ".sf-gh-exec-")
	}
	if err != nil {
		return "", "", ErrInvalidExecutable
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		_ = os.RemoveAll(directory)
		return "", "", ErrInvalidExecutable
	}
	in, err := os.Open(source)
	if err != nil {
		_ = os.RemoveAll(directory)
		return "", "", ErrInvalidExecutable
	}
	defer in.Close()
	targetPath := filepath.Join(directory, "gh")
	out, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		_ = os.RemoveAll(directory)
		return "", "", ErrInvalidExecutable
	}
	hash := sha256.New()
	count, copyErr := io.Copy(io.MultiWriter(out, hash), io.LimitReader(in, maxExecutableSize+1))
	syncErr, closeErr := out.Sync(), out.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || count > maxExecutableSize || hex.EncodeToString(hash.Sum(nil)) != digest {
		_ = os.RemoveAll(directory)
		return "", "", ErrInvalidExecutable
	}
	return targetPath, directory, nil
}

func authenticateSnapshot(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", ErrInvalidExecutable
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Mode()&0o022 != 0 || !trustedOwner(info) || !secureAncestors(path) {
		return "", ErrInvalidExecutable
	}
	file, err := os.Open(path)
	if err != nil {
		return "", ErrInvalidExecutable
	}
	defer file.Close()
	hash := sha256.New()
	count, err := io.Copy(hash, io.LimitReader(file, maxExecutableSize+1))
	if err != nil || count > maxExecutableSize {
		return "", ErrInvalidExecutable
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

const maxExecutableSize = 128 << 20

func secureAncestors(path string) bool {
	for current := filepath.Dir(path); ; current = filepath.Dir(current) {
		entry, err := os.Lstat(current)
		if err != nil || entry.Mode()&os.ModeSymlink != 0 || !entry.IsDir() || (entry.Mode()&0o022 != 0 && entry.Mode()&os.ModeSticky == 0) || !trustedOwner(entry) {
			return false
		}
		parent := filepath.Dir(current)
		if parent == current {
			return true
		}
	}
}

func trustedOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	uid := uint32(os.Getuid())
	return stat.Uid == uid || stat.Uid == 0
}

func validateCommand(binary string, args []string) error {
	if binary == "" || strings.IndexByte(binary, 0) >= 0 || len(args) > maxArgs {
		return ErrInvalidCommand
	}
	total := len(binary)
	for _, value := range append([]string{binary}, args...) {
		if value == "" || len(value) > maxArg || strings.IndexByte(value, 0) >= 0 {
			return ErrInvalidCommand
		}
		total += len(value)
	}
	if total > maxInput {
		return ErrInvalidCommand
	}
	return nil
}

var fixedEnvironment = map[string]string{"GH_PROMPT_DISABLED": "1", "GIT_TERMINAL_PROMPT": "0", "NO_COLOR": "1", "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"}

func validatedEnvironment(env []string) ([]string, error) {
	if len(env) == 0 || len(env) > 8 {
		return nil, ErrInvalidEnvironment
	}
	seen := map[string]bool{}
	total := 0
	result := make([]string, 0, len(env))
	for _, entry := range env {
		if entry == "" || len(entry) > maxArg || strings.IndexByte(entry, 0) >= 0 {
			return nil, ErrInvalidEnvironment
		}
		total += len(entry)
		if total > maxInput {
			return nil, ErrInvalidEnvironment
		}
		key, value, ok := strings.Cut(entry, "=")
		if !ok || seen[key] {
			return nil, ErrInvalidEnvironment
		}
		seen[key] = true
		switch key {
		case "HOME", "GH_CONFIG_DIR":
			canonical, ok := canonicalDirectory(value)
			if !ok {
				return nil, ErrInvalidEnvironment
			}
			value = canonical
		case "SF_FAKE_GH_STATE":
			canonical, ok := canonicalStatePath(value)
			if !ok {
				return nil, ErrInvalidEnvironment
			}
			value = canonical
		default:
			if want, ok := fixedEnvironment[key]; !ok || value != want {
				return nil, ErrInvalidEnvironment
			}
		}
		result = append(result, key+"="+value)
	}
	for key := range fixedEnvironment {
		if !seen[key] {
			return nil, fmt.Errorf("%w: missing %s", ErrInvalidEnvironment, key)
		}
	}
	if !seen["HOME"] || !seen["GH_CONFIG_DIR"] {
		return nil, ErrInvalidEnvironment
	}
	return result, nil
}

func validateEnvironment(env []string) error { _, err := validatedEnvironment(env); return err }

func canonicalDirectory(path string) (string, bool) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return "", false
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !filepath.IsAbs(canonical) || filepath.Clean(canonical) != canonical {
		return "", false
	}
	info, err := os.Lstat(canonical)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !trustedOwner(info) || !secureAncestors(canonical) {
		return "", false
	}
	return canonical, true
}

func canonicalStatePath(path string) (string, bool) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", false
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil || !filepath.IsAbs(parent) || filepath.Clean(parent) != parent {
		return "", false
	}
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !trustedOwner(info) || !secureAncestors(parent) {
		return "", false
	}
	if existing, err := os.Lstat(path); err == nil && (existing.Mode()&os.ModeSymlink != 0 || !existing.Mode().IsRegular() || existing.Mode()&0o022 != 0 || !trustedOwner(existing)) {
		return "", false
	}
	return filepath.Join(parent, filepath.Base(path)), true
}

func boundedContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= maxWait {
		return parent, func() {}
	}
	return context.WithTimeout(parent, maxWait)
}

func boundedCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= cleanupWait {
		return parent, func() {}
	}
	return context.WithTimeout(parent, cleanupWait)
}
