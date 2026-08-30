package ghrunner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
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
