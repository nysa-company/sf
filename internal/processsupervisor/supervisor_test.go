package processsupervisor

import (
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
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type recordingLaunches func(context.Context, contracts.DrainRequest, Identity, string) error

func (f recordingLaunches) RecordLaunch(ctx context.Context, request contracts.DrainRequest, identity Identity, worktree string) error {
	return f(ctx, request, identity, worktree)
}

func runtimeRegistration(t *testing.T) (*Supervisor, contracts.RuntimeBinding, string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authHome := filepath.Join(root, "codex-home")
	if err := os.Mkdir(authHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), []byte(`{"tokens":{"access_token":"fixture"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(root, "codex")
	contents := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(executable, contents, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Executable = testProviderGate(t)
	binding := contracts.RuntimeBinding{
		Identity:     domain.ProviderIdentity{Provider: "codex", Model: "model", Family: "family", Version: "v1"},
		BinaryDigest: hex.EncodeToString(digest[:]), PolicyDigest: supervisor.PolicyDigest(),
		FixtureDigest: strings.Repeat("a", 64), AuthDigest: strings.Repeat("b", 64), AuthMode: "chatgpt_subscription",
	}
	return supervisor, binding, executable, authHome
}

func TestRegisterRuntimeRefreshReusesAndReclaimsStagedSnapshots(t *testing.T) {
	supervisor, binding, executable, authHome := runtimeRegistration(t)
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	first := supervisor.trusted[binding.Identity].stagedDir
	for range 3 {
		if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
			t.Fatal(err)
		}
		if got := supervisor.trusted[binding.Identity].stagedDir; got != first {
			t.Fatalf("unchanged binding restaged %q, want %q", got, first)
		}
	}
	for index := 0; index < 3; index++ {
		previous := supervisor.trusted[binding.Identity].stagedDir
		binding.AuthDigest = fmt.Sprintf("%064x", index+1)
		if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
			t.Fatal(err)
		}
		if got := supervisor.trusted[binding.Identity].stagedDir; got == previous {
			t.Fatal("changed binding reused the old staged executable")
		}
		if _, err := os.Stat(previous); !os.IsNotExist(err) {
			t.Fatalf("retired snapshot remained at %q: %v", previous, err)
		}
		if len(supervisor.retired) != 0 {
			t.Fatalf("retired snapshots leaked: %d", len(supervisor.retired))
		}
	}
}

func TestRegisterRuntimeStagesExecutableLargerThanLegacyLimit(t *testing.T) {
	supervisor, binding, executable, authHome := runtimeRegistration(t)
	file, err := os.OpenFile(executable, os.O_WRONLY|os.O_TRUNC, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("#!/bin/sh\nexit 0\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Truncate(129 << 20); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, source); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	binding.BinaryDigest = hex.EncodeToString(hash.Sum(nil))
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatalf("runtime above the legacy 128 MiB limit was rejected: %v", err)
	}
	trusted := supervisor.trusted[binding.Identity]
	if info, err := os.Stat(trusted.stagedPath); err != nil || info.Size() != 129<<20 || !stagedRuntimeMatches(trusted.snapshot, binding.BinaryDigest) {
		t.Fatalf("large runtime snapshot is incomplete: info=%v err=%v", info, err)
	}
}

func TestRegisterRuntimeFailureAndMissingCachePreserveOrReplaceExactly(t *testing.T) {
	supervisor, binding, executable, authHome := runtimeRegistration(t)
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	first := supervisor.trusted[binding.Identity]
	failed := binding
	failed.AuthDigest = strings.Repeat("c", 64)
	supervisor.stageRuntime = func(*trustedExecutable) error { return errors.New("stage failure") }
	if _, err := supervisor.RegisterRuntime(failed, executable, authHome); err == nil {
		t.Fatal("failed stage was accepted")
	}
	supervisor.stageRuntime = nil
	if got := supervisor.trusted[binding.Identity]; got.stagedDir != first.stagedDir || got.snapshot != first.snapshot {
		t.Fatal("failed staging replaced the usable runtime")
	}
	if err := os.Remove(first.stagedPath); err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	if got := supervisor.trusted[binding.Identity]; got.stagedDir == first.stagedDir || !stagedRuntimeMatches(got.snapshot, binding.BinaryDigest) {
		t.Fatal("missing cached snapshot was reused instead of atomically replaced")
	}
}

func TestStagedRuntimeMatchesRequiresPrivateExecutableAndDirectory(t *testing.T) {
	newSnapshot := func(t *testing.T) (*stagedExecutable, string) {
		t.Helper()
		directory, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(directory, "provider")
		contents := []byte("#!/bin/sh\nexit 0\n")
		if err := os.WriteFile(path, contents, 0o500); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(contents)
		return &stagedExecutable{path: path, directory: directory}, hex.EncodeToString(digest[:])
	}

	t.Run("valid private executable", func(t *testing.T) {
		snapshot, digest := newSnapshot(t)
		if !stagedRuntimeMatches(snapshot, digest) {
			t.Fatal("private executable snapshot was rejected")
		}
	})
	t.Run("missing execute bit", func(t *testing.T) {
		snapshot, digest := newSnapshot(t)
		if err := os.Chmod(snapshot.path, 0o400); err != nil {
			t.Fatal(err)
		}
		if stagedRuntimeMatches(snapshot, digest) {
			t.Fatal("non-executable staged runtime was accepted")
		}
	})
	t.Run("non-private parent mode", func(t *testing.T) {
		snapshot, digest := newSnapshot(t)
		if err := os.Chmod(snapshot.directory, 0o750); err != nil {
			t.Fatal(err)
		}
		if stagedRuntimeMatches(snapshot, digest) {
			t.Fatal("group-readable staged runtime directory was accepted")
		}
	})
	t.Run("symlinked parent", func(t *testing.T) {
		snapshot, digest := newSnapshot(t)
		moved := snapshot.directory + "-moved"
		if err := os.Rename(snapshot.directory, moved); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(moved, snapshot.directory); err != nil {
			t.Fatal(err)
		}
		if stagedRuntimeMatches(snapshot, digest) {
			t.Fatal("symlinked staged runtime directory was accepted")
		}
	})
}

func TestSupervisorCloseNeverDeletesLegacyExecutable(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	if _, err := supervisor.RegisterExecutable(identity, "/bin/sh"); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat("/bin/sh"); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("Close modified legacy executable: info=%v err=%v", info, err)
	}
}

func TestSupervisorCloseReclaimsStagedRuntimeAndIsIdempotent(t *testing.T) {
	supervisor, binding, executable, authHome := runtimeRegistration(t)
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	staged := supervisor.trusted[binding.Identity].stagedDir
	if err := supervisor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatalf("Close retained unused stage %q: %v", staged, err)
	}
	if err := supervisor.Close(); err != nil {
		t.Fatalf("second Close=%v", err)
	}
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err == nil {
		t.Fatal("closed supervisor accepted a new runtime")
	}
}

func TestSupervisorCompletedRuntimeRunDoesNotLeakSnapshotReference(t *testing.T) {
	supervisor, binding, executable, authHome := runtimeRegistration(t)
	supervisor.Recorder = recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error { return nil })
	contents := []byte("#!/bin/sh\nout=''\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = '--output-last-message' ]; then out=\"$2\"; shift 2; continue; fi\n  shift\ndone\nsleep 1\nprintf '{}' > \"$out\"\n")
	if err := os.WriteFile(executable, contents, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(contents)
	binding.BinaryDigest = hex.EncodeToString(digest[:])
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	staged := supervisor.trusted[binding.Identity].stagedDir
	request, invocation, input := codexRunFixture(t, executable, binding, authHome)
	if _, err := supervisor.Run(context.Background(), request, invocation, input); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatalf("completed runtime Run leaked staged snapshot %q: %v", staged, err)
	}
}

func TestSupervisorCloseRetainsStagedRuntimeUntilPipeHoldingEscapeeWaits(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(root, "ready")
	release := filepath.Join(root, "release")
	escapee := buildDetachedPipeHolder(t, root, ready, release)
	provider := filepath.Join(root, "codex")
	program := fmt.Sprintf("#!/bin/sh\n\"%s\" &\nwhile [ ! -f \"%s\" ]; do sleep 0.01; done\nexit 0\n", escapee, ready)
	if err := os.WriteFile(provider, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(program))
	authHome := filepath.Join(root, "codex-home")
	if err := os.Mkdir(authHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), []byte(`{"tokens":{"access_token":"fixture"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	supervisor, err := New(recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Executable = testProviderGate(t)
	supervisor.SoftDrain, supervisor.HardDrain = 100*time.Millisecond, 100*time.Millisecond
	binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "codex", Model: "model", Family: "family", Version: "v1"}, BinaryDigest: hex.EncodeToString(digest[:]), PolicyDigest: supervisor.PolicyDigest(), FixtureDigest: strings.Repeat("a", 64), AuthDigest: strings.Repeat("b", 64), AuthMode: "chatgpt_subscription"}
	if _, err := supervisor.RegisterRuntime(binding, provider, authHome); err != nil {
		t.Fatal(err)
	}
	staged := supervisor.trusted[binding.Identity].stagedDir
	request, invocation, input := codexRunFixture(t, provider, binding, authHome)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		_, err := supervisor.Run(runCtx, request, invocation, input)
		runDone <- err
	}()
	// Compiling and scheduling the detached fixture is not itself a product
	// timeout contract. Keep this setup bound generous enough for loaded CI,
	// while still reporting an early Run failure immediately below.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case err := <-runDone:
			t.Fatalf("provider Run returned before detached child became ready: %v", err)
		case <-deadline.C:
			t.Fatalf("timed out waiting for detached child readiness at %q", ready)
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer func() { _ = os.WriteFile(release, []byte("release"), 0o600) }()
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, ErrUnclear) {
			t.Fatalf("canceled run error=%v, want ErrUnclear", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Run did not return while escapee held its pipes")
	}
	if err := supervisor.Close(); !errors.Is(err, ErrUnclear) {
		t.Fatalf("Close=%v, want ErrUnclear", err)
	}
	if _, err := os.Stat(staged); err != nil {
		t.Fatalf("Close reclaimed staged evidence before wait completion: %v", err)
	}
	if err := os.WriteFile(release, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	awaitMissing(t, staged, 2*time.Second)
}

func buildDetachedPipeHolder(t *testing.T, directory, ready, release string) string {
	t.Helper()
	source := filepath.Join(directory, "escapee.go")
	binary := filepath.Join(directory, "escapee")
	program := fmt.Sprintf("package main\nimport (\"os\"; \"syscall\"; \"time\")\nfunc main() { if _, err := syscall.Setsid(); err != nil { os.Exit(2) }; if err := os.WriteFile(%q, []byte(\"ready\"), 0600); err != nil { os.Exit(3) }; for { if _, err := os.Stat(%q); err == nil { return }; time.Sleep(5 * time.Millisecond) } }\n", ready, release)
	if err := os.WriteFile(source, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "build", "-o", binary, source)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build pipe-holding escapee: %v\n%s", err, output)
	}
	return binary
}

func awaitFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func awaitMissing(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q to be reclaimed", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSupervisorRunRetainsCompletedRunUntilDrain(t *testing.T) {
	supervisor, request, invocation, input := legacyRunFixture(t, "")
	if _, err := supervisor.Run(context.Background(), request, invocation, input); err != nil {
		t.Fatal(err)
	}
	supervisor.mu.Lock()
	remaining := len(supervisor.runs)
	supervisor.mu.Unlock()
	if remaining != 1 {
		t.Fatalf("completed Run did not retain its proof-bearing entry: %d", remaining)
	}
	if _, err := supervisor.Drain(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	supervisor.mu.Lock()
	remaining = len(supervisor.runs)
	supervisor.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("Drain leaked %d completed entries", remaining)
	}
}

func TestSupervisorCloseDrainsActiveRunAndRejectsFutureRun(t *testing.T) {
	supervisor, request, invocation, input := legacyRunFixture(t, "sleep 30")
	entered := make(chan struct{})
	supervisor.Recorder = recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error {
		close(entered)
		return nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := supervisor.Run(context.Background(), request, invocation, input)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("provider run did not cross the durable launch gate")
	}
	if err := supervisor.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed supervisor let its active Run report success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not join the active Run")
	}
	if _, err := supervisor.Run(context.Background(), request, invocation, input); err == nil {
		t.Fatal("closed supervisor accepted a new Run")
	}
}

func TestSupervisorCloseJoinsRunWhenGateWriteRacesDrain(t *testing.T) {
	supervisor, request, invocation, input := legacyRunFixture(t, "sleep 30")
	supervisor.SoftDrain, supervisor.HardDrain = 250*time.Millisecond, 250*time.Millisecond
	recorded, release := make(chan struct{}), make(chan struct{})
	supervisor.Recorder = recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error {
		close(recorded)
		<-release
		return nil
	})
	runDone := make(chan error, 1)
	go func() {
		_, err := supervisor.Run(context.Background(), request, invocation, input)
		runDone <- err
	}()
	select {
	case <-recorded:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not reach the launch recorder")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- supervisor.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		supervisor.mu.Lock()
		closing := supervisor.closing
		supervisor.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not claim the run")
		}
		time.Sleep(time.Millisecond)
	}
	// Give Close's TERM a chance to make the gate write fail with EPIPE, then
	// let RecordLaunch return. The synchronous wait path must still complete
	// both channels that Close observes.
	time.Sleep(25 * time.Millisecond)
	close(release)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("racing Run did not return")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close=%v after gate-write race", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not join the gate-write race")
	}
}

func TestReplacementRetainsSnapshotUntilBlockedRunReturns(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authHome := filepath.Join(root, "codex-home")
	if err := os.Mkdir(authHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), []byte(`{"tokens":{"access_token":"fixture"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(root, "codex")
	program := "#!/bin/sh\nout=''\nwhile [ $# -gt 0 ]; do\n  if [ \"$1\" = '--output-last-message' ]; then out=\"$2\"; shift 2; continue; fi\n  shift\ndone\nsleep 1\nprintf '{}' > \"$out\"\n"
	if err := os.WriteFile(executable, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(program))
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Executable = testProviderGate(t)
	binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "codex", Model: "model", Family: "family", Version: "v1"}, BinaryDigest: hex.EncodeToString(sum[:]), PolicyDigest: supervisor.PolicyDigest(), FixtureDigest: strings.Repeat("a", 64), AuthDigest: strings.Repeat("b", 64), AuthMode: "chatgpt_subscription"}
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	oldStage := supervisor.trusted[binding.Identity].stagedDir
	entered := make(chan struct{})
	supervisor.Recorder = recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error {
		close(entered)
		return nil
	})
	request, invocation, input := codexRunFixture(t, executable, binding, authHome)
	done := make(chan error, 1)
	go func() {
		_, err := supervisor.Run(context.Background(), request, invocation, input)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Run did not register its launch")
	}
	binding.AuthDigest = strings.Repeat("c", 64)
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldStage); err != nil {
		t.Fatalf("replacement deleted a snapshot still held by Run: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked Run did not complete")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(oldStage); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retired snapshot was not reclaimed after final Run reference")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := supervisor.Drain(context.Background(), request); err != nil {
		t.Fatalf("completed replacement run was not drainable: %v", err)
	}
}

func TestCloseWinsPrelaunchSnapshotRaceWithoutDeletingInUseStage(t *testing.T) {
	supervisor, binding, executable, authHome := runtimeRegistration(t)
	if _, err := supervisor.RegisterRuntime(binding, executable, authHome); err != nil {
		t.Fatal(err)
	}
	staged := supervisor.trusted[binding.Identity].stagedDir
	supervisor.Executable = testProviderGate(t)
	entered, release := make(chan struct{}), make(chan struct{})
	supervisor.beforeStart = func() {
		close(entered)
		<-release
	}
	request, invocation, input := codexRunFixture(t, executable, binding, authHome)
	runDone := make(chan error, 1)
	go func() {
		_, err := supervisor.Run(context.Background(), request, invocation, input)
		runDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not retain its selected snapshot before launch")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- supervisor.Close() }()
	deadline := time.Now().Add(time.Second)
	for {
		supervisor.mu.Lock()
		closing := supervisor.closing
		supervisor.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not claim the prelaunch handoff")
		}
		time.Sleep(time.Millisecond)
	}
	// Close has marked the supervisor closed and is now waiting on the selected
	// snapshot. Releasing Run must make it refuse cmd.Start rather than insert a
	// process that Close did not snapshot.
	close(release)
	select {
	case err := <-runDone:
		if err == nil {
			t.Fatal("prelaunch Run started after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prelaunch Run did not return after Close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after prelaunch reference left")
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatalf("Close retained an unused prelaunch stage: %v", err)
	}
}

func legacyRunFixture(t *testing.T, command string) (*Supervisor, contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) {
	t.Helper()
	supervisor, err := New(recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Executable = testProviderGate(t)
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	digest, err := supervisor.RegisterExecutable(identity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	worktree := t.TempDir()
	input := contracts.PhaseInput{Ticket: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-run"}, Phase: domain.PhaseBuild, Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, Prompt: "fixture", Repository: worktree, Worktree: worktree, WorktreeIdentity: "identity", BaseSHA: "base", Provider: identity, Timeout: time.Minute}
	_, input.RequestDigest, err = contracts.CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: input.Ticket, Phase: input.Phase, Role: "builder", Attempt: input.Attempt, LeaderEpoch: input.LeaderEpoch, RunnerEpoch: input.RunnerEpoch, ExpectedVersion: input.ExpectedVersion, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: digest, PolicyDigest: supervisor.PolicyDigest(), Repository: input.Repository, Worktree: input.Worktree, WorktreeIdentity: input.WorktreeIdentity, BaseSHA: input.BaseSHA, RequestDigest: input.RequestDigest}
	argv := []string{"/bin/sh"}
	if command != "" {
		argv = append(argv, "-c", command)
	}
	return supervisor, request, contracts.Invocation{Argv: argv}, input
}

func testProviderGate(t *testing.T) string {
	t.Helper()
	gate := filepath.Join(t.TempDir(), "provider-gate")
	// The helper mirrors the production wrapper's argv shape. The supervisor's
	// own durable recorder remains the boundary under test here; the helper
	// intentionally does not consume the inherited release byte.
	if err := os.WriteFile(gate, []byte("#!/bin/sh\n[ \"$1\" = __provider_gate ] || exit 125\nshift\nprovider=$1\nshift\nexec \"$provider\" \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return gate
}

func codexRunFixture(t *testing.T, executable string, binding contracts.RuntimeBinding, authHome string) (contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) {
	t.Helper()
	worktree := t.TempDir()
	input := contracts.PhaseInput{Ticket: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-codex"}, Phase: domain.PhaseBuild, Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, Prompt: "fixture", Repository: worktree, Worktree: worktree, WorktreeIdentity: "identity", BaseSHA: "base", Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	_, input.RequestDigest = mustCanonicalPhaseInput(t, input)
	request := contracts.DrainRequest{ClaimID: 1, Identity: binding.Identity, Ref: input.Ticket, Phase: input.Phase, Role: "builder", Attempt: input.Attempt, LeaderEpoch: input.LeaderEpoch, RunnerEpoch: input.RunnerEpoch, ExpectedVersion: input.ExpectedVersion, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, Repository: input.Repository, Worktree: input.Worktree, WorktreeIdentity: input.WorktreeIdentity, BaseSHA: input.BaseSHA, RequestDigest: input.RequestDigest}
	argv := []string{executable, "exec", "--ephemeral", "--json", "--ignore-user-config", "--ignore-rules", "--config", `default_permissions="sf-guarded"`, "--config", `permissions.sf-guarded.extends=":workspace"`, "--config", `permissions.sf-guarded.filesystem={":root"="deny",":minimal"="read",":workspace_roots"="write"}`, "--config", `permissions.sf-guarded.network.enabled=false`, "--model", binding.Identity.Model, "-C", input.Worktree, "--output-schema", contracts.OutputSchemaPlaceholder, "--output-last-message", contracts.OutputLastMessagePlaceholder, "-"}
	return request, contracts.Invocation{Argv: argv, Stdin: []byte("fixture"), OutputSchema: input.Schema, CaptureLastMessage: true, AuthHome: authHome}, input
}

func mustCanonicalPhaseInput(t *testing.T, input contracts.PhaseInput) ([]byte, string) {
	t.Helper()
	payload, digest, err := contracts.CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	return payload, digest
}

func TestMaterializeOutputSchemaUsesPrivateSupervisorFileAndBoundsStdin(t *testing.T) {
	temporary := t.TempDir()
	invocation := contracts.Invocation{
		Argv:         []string{"/fixture/codex", "exec", "--output-schema", contracts.OutputSchemaPlaceholder, "-"},
		Stdin:        []byte("untrusted ticket prompt"),
		OutputSchema: []byte(`{"type":"object"}`),
	}
	arguments, _, err := materializeInvocationFiles(invocation, temporary)
	if err != nil || arguments[3] == contracts.OutputSchemaPlaceholder || filepath.Dir(arguments[3]) != temporary {
		t.Fatalf("arguments=%q err=%v", arguments, err)
	}
	info, err := os.Stat(arguments[3])
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("schema permissions=%v err=%v", info.Mode(), err)
	}
	contents, err := os.ReadFile(arguments[3])
	if err != nil || string(contents) != string(invocation.OutputSchema) {
		t.Fatalf("schema contents=%q err=%v", contents, err)
	}
	invocation.Stdin = make([]byte, 64<<10+1)
	if _, _, err := materializeInvocationFiles(invocation, temporary); err == nil {
		t.Fatal("oversized provider stdin was accepted")
	}
	invocation.Stdin = nil
	invocation.Argv = []string{"/fixture/codex", "exec", "-"}
	if _, _, err := materializeInvocationFiles(invocation, temporary); err == nil {
		t.Fatal("schema without a placeholder was accepted")
	}
}

func TestRunKeyRequiresExactClaimIdentity(t *testing.T) {
	base := contracts.DrainRequest{
		ClaimID: 7, Identity: domain.ProviderIdentity{Provider: "p", Model: "m", Family: "f", Version: "v"},
		Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-key"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2,
		LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: "binary",
		Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base", RequestDigest: strings.Repeat("e", 64),
	}
	for name, mutate := range map[string]func(*contracts.DrainRequest){
		"role":              func(request *contracts.DrainRequest) { request.Role = "reviewer" },
		"leader":            func(request *contracts.DrainRequest) { request.LeaderEpoch++ },
		"runner":            func(request *contracts.DrainRequest) { request.RunnerEpoch++ },
		"binding":           func(request *contracts.DrainRequest) { request.BindingDigest = "other" },
		"policy":            func(request *contracts.DrainRequest) { request.PolicyDigest = "other" },
		"worktree":          func(request *contracts.DrainRequest) { request.Worktree = "/other" },
		"worktree identity": func(request *contracts.DrainRequest) { request.WorktreeIdentity = "other" },
		"request digest":    func(request *contracts.DrainRequest) { request.RequestDigest = strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if key(base) == key(changed) {
				t.Fatal("mismatched claim identity reused the same supervisor run key")
			}
		})
	}
}

func TestDrainContextHonorsCallerDeadlineAndTotalBudget(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.SoftDrain, supervisor.HardDrain = 80*time.Millisecond, 80*time.Millisecond
	caller, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	bounded, boundedCancel, err := supervisor.drainContext(caller)
	if err != nil {
		t.Fatal(err)
	}
	defer boundedCancel()
	<-bounded.Done()
	if elapsed := time.Since(started); elapsed > 75*time.Millisecond {
		t.Fatalf("caller deadline was not honored: %s", elapsed)
	}
	started = time.Now()
	bounded, boundedCancel, err = supervisor.drainContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer boundedCancel()
	<-bounded.Done()
	if elapsed := time.Since(started); elapsed > 230*time.Millisecond {
		t.Fatalf("soft+hard drain budget was exceeded: %s", elapsed)
	}
}

func TestDrainContextRejectsDurationsAboveTotalMachineCap(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.SoftDrain, supervisor.HardDrain = 20*time.Second, 20*time.Second
	if _, _, err := supervisor.drainContext(context.Background()); !errors.Is(err, ErrUnclear) {
		t.Fatalf("20s+20s drain exceeded total cap but was accepted: %v", err)
	}
}

func TestDrainPersistedLeaderGoneAfterSetsidRemainsUnclear(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := hostBootIdentity()
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: 99, RequestDigest: strings.Repeat("a", 64)}
	// A setsid child can outlive a vanished leader. With no durable descendant
	// witness, v1 rejects recovery instead of claiming whole-tree containment.
	launch := contracts.ProviderLaunch{PID: 999999, PGID: 999999, BootIdentity: boot, ProcessStartIdentity: "old-leader", Worktree: "/worktree"}
	if _, err := supervisor.DrainPersisted(context.Background(), request, launch); !errors.Is(err, ErrUnclear) {
		t.Fatalf("leader-gone recovery was accepted: %v", err)
	}
}

func TestRunRequiresRegisteredMatchingExecutable(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	input := contracts.PhaseInput{Worktree: t.TempDir()}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-executable"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "lease", BindingDigest: "binding", Worktree: input.Worktree}
	if _, err := supervisor.Run(context.Background(), request, contracts.Invocation{Argv: []string{"/bin/sh"}}, input); err == nil {
		t.Fatal("unregistered executable was accepted")
	}
	binaryDigest, err := supervisor.RegisterExecutable(identity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	request.BinaryDigest = binaryDigest
	request.PolicyDigest = supervisor.PolicyDigest()
	for _, path := range []string{"/bin/echo"} {
		if _, err := supervisor.Run(context.Background(), request, contracts.Invocation{Argv: []string{path}}, input); err == nil {
			t.Fatalf("mismatched executable %q was accepted", path)
		}
	}
}

func TestRunRequiresBothDurableBindingDigests(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	binaryDigest, err := supervisor.RegisterExecutable(identity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-digest"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: binaryDigest, PolicyDigest: supervisor.PolicyDigest(), Worktree: t.TempDir()}
	for name, mutate := range map[string]func(*contracts.DrainRequest){
		"binary": func(r *contracts.DrainRequest) { r.BinaryDigest = "" },
		"policy": func(r *contracts.DrainRequest) { r.PolicyDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if _, err := supervisor.Run(context.Background(), changed, contracts.Invocation{Argv: []string{"/bin/sh"}}, contracts.PhaseInput{Worktree: changed.Worktree}); err == nil {
				t.Fatal("execution without complete binding digests was accepted")
			}
		})
	}
}

func TestRapidPIDReuseWithSameSecondDisplayIdentityNeverMatches(t *testing.T) {
	// Both processes could render as the same ps lstart second. The durable
	// kernel identity includes microseconds (Darwin) or start ticks (Linux), so
	// the replacement cannot pass the no-signal verification gate.
	launch := contracts.ProviderLaunch{PID: 4242, PGID: 4242, BootIdentity: "darwin:boot", ProcessStartIdentity: "darwin:1724934896:101", Worktree: "/tmp/worktree"}
	if persistedIdentityMatches(launch, "darwin:1724934896:102", 4242) {
		t.Fatal("rapidly reused PID was accepted for signalling")
	}
	if persistedIdentityMatches(launch, launch.ProcessStartIdentity, 4243) {
		t.Fatal("foreign process group was accepted for signalling")
	}
}

func TestRecoveryLivenessRules(t *testing.T) {
	if !bootIdentityChanged("linux:old-boot", "linux:new-boot") {
		t.Fatal("reboot did not prove the old process namespace dead")
	}
	// A missing leader is deliberately ambiguous: v1 has no durable witness
	// for a setsid/double-fork writer outside the recorded process group.
}

func TestReadBoundedFileDoesNotFollowCredentialSymlink(t *testing.T) {
	directory := t.TempDir()
	credential := filepath.Join(directory, "auth.json")
	artifact := filepath.Join(directory, "output-last-message.json")
	if err := os.WriteFile(credential, []byte(`{"access_token":"must-not-leak"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(credential, artifact); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readBoundedFile(artifact, 1<<20); err == nil {
		t.Fatal("final artifact reader followed a credential symlink")
	}
}

func TestReadBoundedFileAuthenticatesOpenedRegularFile(t *testing.T) {
	directory := t.TempDir()
	artifact := filepath.Join(directory, "output-last-message.json")
	if err := os.WriteFile(artifact, []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	contents, truncated, err := readBoundedFile(artifact, 1<<20)
	if err != nil || truncated || string(contents) != `{"ok":true}` {
		t.Fatalf("contents=%q truncated=%v err=%v", contents, truncated, err)
	}
}

func TestRequestMatchesInputRequiresEveryDurableBinding(t *testing.T) {
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-binding"}
	request := contracts.DrainRequest{ClaimID: 1, Identity: identity, Ref: ref, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2, LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, LeaseKey: "lease", BindingDigest: "binding", BinaryDigest: "binary", PolicyDigest: "policy", Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base"}
	input := contracts.PhaseInput{Ticket: ref, Phase: domain.PhaseBuild, Attempt: 2, LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, Provider: identity, Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base"}
	if !requestMatchesInput(request, input) {
		t.Fatal("matching durable request was rejected")
	}
	for name, mutate := range map[string]func(*contracts.PhaseInput){
		"ticket":            func(value *contracts.PhaseInput) { value.Ticket.Ticket = "SF-other" },
		"phase":             func(value *contracts.PhaseInput) { value.Phase = domain.PhasePlanning },
		"attempt":           func(value *contracts.PhaseInput) { value.Attempt++ },
		"leader":            func(value *contracts.PhaseInput) { value.LeaderEpoch++ },
		"runner":            func(value *contracts.PhaseInput) { value.RunnerEpoch++ },
		"ticket version":    func(value *contracts.PhaseInput) { value.ExpectedVersion++ },
		"provider":          func(value *contracts.PhaseInput) { value.Provider.Model = "other" },
		"repository":        func(value *contracts.PhaseInput) { value.Repository = "/other" },
		"worktree":          func(value *contracts.PhaseInput) { value.Worktree = "/other" },
		"worktree identity": func(value *contracts.PhaseInput) { value.WorktreeIdentity = "other" },
		"base":              func(value *contracts.PhaseInput) { value.BaseSHA = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := input
			mutate(&changed)
			if requestMatchesInput(request, changed) {
				t.Fatal("mismatched phase input was accepted")
			}
		})
	}
}
