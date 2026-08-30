package ghrunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/github"
)

// TestGHRunnerHelper is an executable helper process. It intentionally uses
// only argv so the runner's environment contract remains exact.
func TestGHRunnerHelper(t *testing.T) {
	if len(os.Args) == 0 || os.Args[len(os.Args)-1] != "gh-runner-helper" {
		return
	}
	mode := "ok"
	if len(os.Args) > 1 {
		mode = os.Args[len(os.Args)-2]
		if len(os.Args) > 4 {
			mode = os.Args[len(os.Args)-3]
		}
	}
	switch mode {
	case "ok":
		_, _ = os.Stdout.WriteString("stdout")
		_, _ = os.Stderr.WriteString("stderr")
	case "nonzero":
		os.Exit(7)
	case "fast":
		return
	case "hang":
		for {
			time.Sleep(time.Second)
		}
	case "setsid":
		// A process-group leader cannot call setsid. Leave a child behind in a
		// new session while the owner exits; its pipe is retained ambiguity.
		marker := ""
		if len(os.Args) > 2 {
			marker = os.Args[len(os.Args)-2]
		}
		_, _ = syscall.ForkExec(os.Args[0], []string{os.Args[0], "-test.run=TestGHRunnerHelper", "setsid-child", marker, "gh-runner-helper"}, &syscall.ProcAttr{Files: []uintptr{0, 1, 2}})
		time.Sleep(500 * time.Millisecond)
	case "setsid-child":
		_, _ = syscall.Setsid()
		if len(os.Args) > 2 && os.Args[len(os.Args)-2] != "" {
			_ = os.WriteFile(os.Args[len(os.Args)-2], []byte(fmt.Sprint(os.Getpid())), 0o600)
		}
		for {
			time.Sleep(10 * time.Second)
		}
	}
}

func runnerEnvironment(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	config := t.TempDir()
	return []string{"HOME=" + home, "GH_CONFIG_DIR=" + config, "GH_PROMPT_DISABLED=1", "GIT_TERMINAL_PROMPT=0", "NO_COLOR=1", "PATH=/usr/bin:/bin:/usr/sbin:/sbin"}
}

func helperArgs(mode string) []string {
	return []string{"-test.run=TestGHRunnerHelper", mode, "gh-runner-helper"}
}

func helperArgsMarker(mode, marker string) []string {
	return []string{"-test.run=TestGHRunnerHelper", mode, marker, "gh-runner-helper"}
}

func TestRunAndCleanup(t *testing.T) {
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	output, err := runner.Run(context.Background(), mustExecutable(t), helperArgs("ok"), runnerEnvironment(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "stdout") || !strings.Contains(string(output), "stderr") {
		t.Fatalf("output %q", output)
	}
	proof, err := runner.Cleanup(context.Background())
	if err != nil || !proof.Drained || proof.Quarantined {
		t.Fatalf("cleanup proof=%+v err=%v", proof, err)
	}
	if _, err := runner.Cleanup(context.Background()); !errors.Is(err, ErrCleanupBeforeRun) && !errors.Is(err, ErrCleanupAlreadyUsed) {
		t.Fatalf("double cleanup: %v", err)
	}
}

func TestRunNonzeroAndBoundedCancellation(t *testing.T) {
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Run(context.Background(), mustExecutable(t), helperArgs("nonzero"), runnerEnvironment(t))
	if err == nil {
		t.Fatal("expected exit error")
	}
	if _, err := runner.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}

	runner, err = New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = runner.Run(ctx, mustExecutable(t), helperArgs("hang"), runnerEnvironment(t))
	if time.Since(started) > 3*time.Second {
		t.Fatal("Run exceeded bounded cancellation")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout error %v", err)
	}
	if proof, cleanErr := runner.Cleanup(context.Background()); cleanErr != nil || !proof.Drained {
		t.Fatalf("cleanup=%+v err=%v", proof, cleanErr)
	}
}

func TestRefusalsAndConcurrent(t *testing.T) {
	path := mustExecutable(t)
	runner, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), path, []string{"bad\x00arg"}, runnerEnvironment(t)); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("NUL refusal %v", err)
	}
	if _, err := runner.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	badEnv := append(runnerEnvironment(t), "PATH=/tmp")
	if _, err := runner.Run(context.Background(), path, helperArgs("ok"), badEnv); !errors.Is(err, ErrInvalidEnvironment) {
		t.Fatalf("env refusal %v", err)
	}
	if _, err := runner.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), "/bin/sh", helperArgs("ok"), runnerEnvironment(t)); !errors.Is(err, ErrInvalidExecutable) {
		t.Fatalf("binary refusal %v", err)
	}
	if _, err := runner.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, runErr := runner.Run(ctx, path, helperArgs("hang"), runnerEnvironment(t)); result <- runErr }()
	time.Sleep(15 * time.Millisecond)
	if _, err := runner.Run(context.Background(), path, helperArgs("ok"), runnerEnvironment(t)); !errors.Is(err, ErrConcurrentRun) {
		t.Fatalf("concurrent refusal %v", err)
	}
	<-result
	if _, err := runner.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSecondEnvironmentValidationFailureHasDefiniteCleanup(t *testing.T) {
	old := validatedEnvironmentFn
	defer func() { validatedEnvironmentFn = old }()
	var calls int
	validatedEnvironmentFn = func(env []string) ([]string, error) {
		calls++
		if calls == 2 {
			return nil, ErrInvalidEnvironment
		}
		return validatedEnvironment(env)
	}
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := runner.Run(context.Background(), mustExecutable(t), helperArgs("ok"), runnerEnvironment(t))
	if !errors.Is(runErr, ErrInvalidEnvironment) {
		t.Fatalf("second environment validation error=%v", runErr)
	}
	if calls != 2 {
		t.Fatalf("environment validation calls=%d, want two", calls)
	}
	proof, cleanupErr := runner.Cleanup(context.Background())
	if cleanupErr != nil || !proof.Drained || proof.Quarantined {
		t.Fatalf("second validation cleanup=%+v err=%v", proof, cleanupErr)
	}
}

func TestCleanupWaitsForRunLifecycleAndNextRunWorks(t *testing.T) {
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, mustExecutable(t), helperArgs("hang"), runnerEnvironment(t))
		runDone <- runErr
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runner.mu.Lock()
		active := runner.active != nil
		runner.mu.Unlock()
		if active {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	runner.mu.Lock()
	active := runner.active != nil
	runner.mu.Unlock()
	if !active {
		t.Fatal("Run did not publish active lifecycle record")
	}
	cleanupDone := make(chan struct {
		proof github.CleanupProof
		err   error
	}, 1)
	go func() {
		proof, cleanupErr := runner.Cleanup(context.Background())
		cleanupDone <- struct {
			proof github.CleanupProof
			err   error
		}{proof: proof, err: cleanupErr}
	}()
	cleanupClaimedBy := time.Now().Add(time.Second)
	for {
		runner.mu.Lock()
		claimed := runner.active != nil && runner.active.cleanupInProgress
		runner.mu.Unlock()
		if claimed {
			break
		}
		if time.Now().After(cleanupClaimedBy) {
			t.Fatal("Cleanup did not claim the active proof")
		}
		time.Sleep(time.Millisecond)
	}
	runner.mu.Lock()
	finished := runner.active.finished
	runner.mu.Unlock()
	select {
	case <-finished:
		t.Fatal("Cleanup claim raced past Run lifecycle completion")
	default:
	}
	cancel()
	if runErr := <-runDone; !errors.Is(runErr, context.Canceled) {
		t.Fatalf("Run cancellation: %v", runErr)
	}
	result := <-cleanupDone
	if result.err != nil || !result.proof.Drained || result.proof.Quarantined {
		t.Fatalf("cleanup=%+v err=%v", result.proof, result.err)
	}
	runner.mu.Lock()
	active, needsCleanup := runner.active != nil, runner.needsCleanup
	runner.mu.Unlock()
	if active || needsCleanup {
		t.Fatalf("cleanup left runner active=%v needsCleanup=%v", active, needsCleanup)
	}
	if _, err := runner.Run(context.Background(), mustExecutable(t), helperArgs("fast"), runnerEnvironment(t)); err != nil {
		t.Fatalf("next Run after cleanup: %v", err)
	}
	if proof, err := runner.Cleanup(context.Background()); err != nil || !proof.Drained {
		t.Fatalf("next cleanup=%+v err=%v", proof, err)
	}
}

func TestCleanupTimeoutReleasesClaimForRetry(t *testing.T) {
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(runCtx, mustExecutable(t), helperArgs("hang"), runnerEnvironment(t))
		runDone <- runErr
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runner.mu.Lock()
		active := runner.active != nil
		runner.mu.Unlock()
		if active {
			break
		}
		time.Sleep(time.Millisecond)
	}
	runner.mu.Lock()
	active := runner.active != nil
	runner.mu.Unlock()
	if !active {
		t.Fatal("Run did not publish active lifecycle record")
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 20*time.Millisecond)
	proof, cleanupErr := runner.Cleanup(cleanupCtx)
	cancelCleanup()
	if cleanupErr == nil || proof.Drained || proof.Quarantined {
		t.Fatalf("timed-out cleanup=%+v err=%v", proof, cleanupErr)
	}
	if runErr := <-runDone; !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatalf("Run timeout: %v", runErr)
	}
	proof, cleanupErr = runner.Cleanup(context.Background())
	if cleanupErr != nil || !proof.Drained || proof.Quarantined {
		t.Fatalf("retry cleanup=%+v err=%v", proof, cleanupErr)
	}
	if _, err := runner.Run(context.Background(), mustExecutable(t), helperArgs("fast"), runnerEnvironment(t)); err != nil {
		t.Fatalf("next Run after retry: %v", err)
	}
	if proof, err := runner.Cleanup(context.Background()); err != nil || !proof.Drained {
		t.Fatalf("next cleanup=%+v err=%v", proof, err)
	}
}

func TestCloseSerializesSnapshotRemovalAndRun(t *testing.T) {
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	runner.removeSnapshot = func(path string) error {
		close(entered)
		<-release
		return os.RemoveAll(path)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- runner.Close() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Close did not enter removal seam")
	}
	if _, err := runner.Run(context.Background(), mustExecutable(t), helperArgs("fast"), runnerEnvironment(t)); !errors.Is(err, ErrConcurrentRun) {
		t.Fatalf("Run during Close: %v", err)
	}
	close(release)
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), "\x00", nil, nil); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("invalid Run after Close: %v", err)
	}
	if _, err := runner.Run(context.Background(), mustExecutable(t), helperArgs("fast"), runnerEnvironment(t)); !errors.Is(err, ErrRunnerClosed) {
		t.Fatalf("Run after Close: %v", err)
	}
}

func TestExecutableReplacementRefused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gh")
	source, err := os.ReadFile(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, source, 0o755); err != nil {
		t.Fatal(err)
	}
	runner, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(source, []byte("replacement")...), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), path, helperArgs("ok"), runnerEnvironment(t)); !errors.Is(err, ErrExecutableChanged) {
		t.Fatalf("replacement error %v", err)
	}
}

func TestStartFailureHasDefiniteDrain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gh")
	if err := os.WriteFile(path, []byte("#!/definitely/missing/interpreter\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	runner, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(context.Background(), path, []string{"noop"}, runnerEnvironment(t)); err == nil {
		t.Fatal("expected start failure")
	}
	proof, err := runner.Cleanup(context.Background())
	if err != nil || !proof.Drained || proof.Quarantined {
		t.Fatalf("start-failure cleanup=%+v err=%v", proof, err)
	}
}

func TestPostStartIdentityFaultIsBoundedAndQuarantined(t *testing.T) {
	oldBoot, oldStart := hostBootIdentityFn, processStartIdentityFn
	defer func() { hostBootIdentityFn, processStartIdentityFn = oldBoot, oldStart }()
	hostBootIdentityFn = func() (string, error) { return "", errors.New("injected boot identity fault") }
	start := time.Now()
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	_, runErr := runner.Run(context.Background(), mustExecutable(t), helperArgs("hang"), runnerEnvironment(t))
	if !errors.Is(runErr, ErrExternalCleanupUncertain) {
		t.Fatalf("identity fault error %v", runErr)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("identity fault exceeded bounded handoff")
	}
	proof, cleanErr := runner.Cleanup(context.Background())
	if !errors.Is(cleanErr, ErrExternalCleanupUncertain) || !proof.Quarantined {
		t.Fatalf("cleanup=%+v err=%v", proof, cleanErr)
	}
}

func TestPostStartProcessIdentityFaultIsBounded(t *testing.T) {
	old := processStartIdentityFn
	defer func() { processStartIdentityFn = old }()
	processStartIdentityFn = func(int) (string, error) { return "", errors.New("injected process identity fault") }
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, runErr := runner.Run(context.Background(), mustExecutable(t), helperArgs("hang"), runnerEnvironment(t))
	if !errors.Is(runErr, ErrExternalCleanupUncertain) || time.Since(started) > 3*time.Second {
		t.Fatalf("identity fault err=%v duration=%s", runErr, time.Since(started))
	}
	proof, cleanErr := runner.Cleanup(context.Background())
	if !errors.Is(cleanErr, ErrExternalCleanupUncertain) || !proof.Quarantined {
		t.Fatalf("cleanup=%+v err=%v", proof, cleanErr)
	}
}

func TestBlockingPostStartIdentityIsDeadlineBounded(t *testing.T) {
	oldBoot, oldStart := hostBootIdentityFn, processStartIdentityFn
	release := make(chan struct{})
	entered := make(chan struct{})
	identityDone := make(chan struct{})
	defer func() {
		close(release)
		<-identityDone
		hostBootIdentityFn = oldBoot
		processStartIdentityFn = oldStart
	}()
	hostBootIdentityFn = func() (string, error) {
		close(entered)
		<-release
		return "", errors.New("released identity fault")
	}
	processStartIdentityFn = func(pid int) (string, error) {
		defer close(identityDone)
		return oldStart(pid)
	}
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, runErr := runner.Run(ctx, mustExecutable(t), helperArgs("hang"), runnerEnvironment(t))
	if runErr == nil || !errors.Is(runErr, ErrExternalCleanupUncertain) || time.Since(started) > 2*time.Second {
		t.Fatalf("blocking identity err=%v duration=%s", runErr, time.Since(started))
	}
	select {
	case <-entered:
	default:
		t.Fatal("identity sampler was not started")
	}
	proof, cleanupErr := runner.Cleanup(context.Background())
	if !errors.Is(cleanupErr, ErrExternalCleanupUncertain) || !proof.Quarantined {
		t.Fatalf("cleanup=%+v err=%v", proof, cleanupErr)
	}
}

func TestCancellationAndSubsequentIdentitySamplingAreBounded(t *testing.T) {
	old := processStartIdentityFn
	defer func() { processStartIdentityFn = old }()
	var calls atomic.Int32
	var firstOnce sync.Once
	firstSampleDone := make(chan struct{})
	secondStarted := make(chan struct{})
	secondDone := make(chan struct{})
	release := make(chan struct{})
	processStartIdentityFn = func(pid int) (string, error) {
		if calls.Add(1) == 1 {
			value, err := old(pid)
			firstOnce.Do(func() { close(firstSampleDone) })
			return value, err
		}
		close(secondStarted)
		<-release
		close(secondDone)
		return old(pid)
	}
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	env := runnerEnvironment(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := time.Now()
	runDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(ctx, mustExecutable(t), helperArgs("hang"), env)
		runDone <- runErr
	}()
	select {
	case <-firstSampleDone:
	case <-time.After(time.Second):
		t.Fatal("initial process identity sample did not complete")
	}
	cancel()
	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run waited on a blocked subsequent identity sample")
	}
	if !errors.Is(runErr, context.Canceled) || time.Since(started) > 2*time.Second {
		t.Fatalf("blocked subsequent identity cancellation err=%v duration=%s", runErr, time.Since(started))
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking subsequent identity sample was not started")
	}
	close(release)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("blocked identity sampler did not finish after release")
	}
	if calls.Load() < 2 {
		t.Fatalf("identity reader calls=%d, want initial plus bounded subsequent sample", calls.Load())
	}
	proof, cleanupErr := runner.Cleanup(context.Background())
	if !errors.Is(cleanupErr, ErrExternalCleanupUncertain) || !proof.Quarantined || proof.Drained {
		t.Fatalf("blocked identity cleanup=%+v err=%v", proof, cleanupErr)
	}
}

func TestCloseRetainsSnapshotAfterRemovalFailure(t *testing.T) {
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	path, directory := runner.snapshotPath, runner.snapshotDir
	injected := errors.New("injected remove failure")
	runner.removeSnapshot = func(string) error { return injected }
	if err := runner.Close(); !errors.Is(err, injected) {
		t.Fatalf("Close error=%v", err)
	}
	if runner.snapshotPath != path || runner.snapshotDir != directory {
		t.Fatalf("snapshot paths were discarded after failed removal")
	}
	runner.removeSnapshot = os.RemoveAll
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if runner.snapshotPath != "" || runner.snapshotDir != "" {
		t.Fatal("snapshot paths retained after successful removal")
	}
}

func TestFastExitBeforeIdentitySampleIsNotQuarantined(t *testing.T) {
	old := processStartIdentityFn
	defer func() { processStartIdentityFn = old }()
	processStartIdentityFn = func(int) (string, error) { return "", errors.New("sample raced with exit") }
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		if _, runErr := runner.Run(context.Background(), mustExecutable(t), helperArgs("fast"), runnerEnvironment(t)); runErr != nil {
			t.Fatalf("fast run %d: %v", i, runErr)
		}
		proof, cleanErr := runner.Cleanup(context.Background())
		if cleanErr != nil || !proof.Drained {
			t.Fatalf("fast cleanup %d: %+v %v", i, proof, cleanErr)
		}
	}
}

func TestPostStartIdentityFaultRetainedPipeIsQuarantined(t *testing.T) {
	old := hostBootIdentityFn
	defer func() { hostBootIdentityFn = old }()
	marker := filepath.Join(t.TempDir(), "child-pid")
	t.Cleanup(func() { killMarkedProcess(t, marker) })
	hostBootIdentityFn = func() (string, error) {
		time.Sleep(2 * time.Second)
		return "", errors.New("injected boot identity fault")
	}
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, runErr := runner.Run(context.Background(), mustExecutable(t), helperArgsMarker("setsid", marker), runnerEnvironment(t))
	if (runErr != nil && !errors.Is(runErr, ErrExternalCleanupUncertain)) || time.Since(started) > 5*time.Second {
		t.Fatalf("identity retained fault err=%v duration=%s", runErr, time.Since(started))
	}
	proof, cleanErr := runner.Cleanup(context.Background())
	if !errors.Is(cleanErr, ErrExternalCleanupUncertain) || !proof.Quarantined {
		t.Fatalf("cleanup=%+v err=%v", proof, cleanErr)
	}
}

func TestRetainedPipeIsUncertain(t *testing.T) {
	// A setsid descendant retaining a pipe is observable and quarantined. A
	// setsid descendant that closes both descriptors is intentionally outside
	// this witness model; package documentation records that limitation.
	runner, err := New(mustExecutable(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "child-pid")
	t.Cleanup(func() { killMarkedProcess(t, marker) })
	_, _ = runner.Run(ctx, mustExecutable(t), helperArgsMarker("setsid", marker), runnerEnvironment(t))
	proof, cleanupErr := runner.Cleanup(context.Background())
	if !errors.Is(cleanupErr, ErrExternalCleanupUncertain) || !proof.Quarantined || proof.Drained {
		t.Fatalf("uncertain cleanup=%+v err=%v", proof, cleanupErr)
	}
}

func killMarkedProcess(t *testing.T, marker string) {
	t.Helper()
	data, _ := os.ReadFile(marker)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	if pid <= 0 {
		t.Errorf("retained child did not publish a pid")
		return
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && syscall.Kill(pid, 0) == nil {
		time.Sleep(10 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	if syscall.Kill(pid, 0) == nil {
		t.Errorf("retained child pid %d survived cleanup", pid)
	}
}

func mustExecutable(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil || strings.ContainsRune(path, '\x00') {
		t.Fatal(err)
	}
	return path
}
