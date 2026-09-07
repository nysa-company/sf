package processsupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/cursorprovider"
)

// Exercises the actual Claude branch of Supervisor.Run with native credentials
// and staged executable. The recorder/gate are test fixtures, not Store E2E.
// Cancellation may reach the provider, so opt-in is explicitly a paid probe.
func TestInstalledClaudeSupervisorCancellationDrains(t *testing.T) {
	runInstalledClaudeCancellation(t, false)
}

func TestInstalledClaudeCancellationAfterWritePreservesWorktree(t *testing.T) {
	runInstalledClaudeCancellation(t, true)
}

func TestInstalledCursorCancellationAfterWritePreservesWorktree(t *testing.T) {
	runInstalledCLICancellation(t, true, "cursor-agent", "SF_TEST_CURSOR_CANCEL")
}

func runInstalledClaudeCancellation(t *testing.T, afterWrite bool) {
	t.Helper()
	runInstalledCLICancellation(t, afterWrite, "claude", "SF_TEST_CLAUDE_CANCEL")
}

func runInstalledCLICancellation(t *testing.T, afterWrite bool, executableName, optIn string) {
	t.Helper()
	if os.Getenv(optIn) != "1" {
		t.Skip("explicit native paid CLI cancellation probe")
	}
	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()
	entered := make(chan struct{})
	s, err := New(recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error { close(entered); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error("supervisor close did not prove drain")
		}
	})
	s.Executable = testProviderGate(t)
	exe, err := exec.LookPath(executableName)
	if err != nil {
		t.Fatal("Claude unavailable")
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal("Claude path unavailable")
	}
	var binding contracts.RuntimeBinding
	if executableName == "cursor-agent" {
		binding, _, err = s.ObserveCursorRuntime(ctx, exe, "gpt-5.6-luna-low")
	} else {
		binding, err = s.ObserveClaudeRuntime(ctx, exe, "claude-sonnet-5")
	}
	if err != nil {
		t.Fatal("Claude observation failed")
	}
	authHome := credentialHome(t)
	if _, err := s.RegisterRuntime(binding, exe, authHome); err != nil {
		t.Fatal("Claude registration failed")
	}
	request, _, input := codexRunFixture(t, exe, binding, authHome)
	// Seatbelt uses physical paths. t.TempDir may return /var's symlink alias
	// on macOS; production and qualification inputs use the canonical path.
	canonical, err := filepath.EvalSymlinks(input.Worktree)
	if err != nil {
		t.Fatal("fixture worktree cannot be canonicalized")
	}
	input.Worktree, input.Repository = canonical, canonical
	request.Worktree, request.Repository = canonical, canonical
	_, input.RequestDigest = mustCanonicalPhaseInput(t, input)
	request.RequestDigest = input.RequestDigest
	marker := filepath.Join(input.Worktree, "ready.txt")
	if afterWrite {
		input.AllowedPaths = []string{"ready.txt"}
		for _, name := range []string{"sample-01.txt", "sample-02.txt", "sample-03.txt", "sample-04.txt", "sample-05.txt", "sample-06.txt", "sample-07.txt", "sample-08.txt", "sample-09.txt", "sample-10.txt", "sample-11.txt", "sample-12.txt", "sample-13.txt", "sample-14.txt", "sample-15.txt", "sample-16.txt", "sample-17.txt", "sample-18.txt", "sample-19.txt", "sample-20.txt"} {
			input.AllowedPaths = append(input.AllowedPaths, name)
		}
		input.Prompt = "Cancellation fixture in a disposable worktree. First use Write to create ready.txt containing exactly SF_CANCEL_READY. Then use separate Write tool calls to create twenty small files named sample-01.txt through sample-20.txt, each containing its number. Do not combine operations or use shell, network, or paths outside this directory. Return a JSON object only after all writes."
		_, input.RequestDigest = mustCanonicalPhaseInput(t, input)
		request.RequestDigest = input.RequestDigest
	}
	var invocation contracts.Invocation
	if executableName == "cursor-agent" {
		invocation, err = cursorprovider.Invocation(ctx, exe, authHome, input)
	} else {
		invocation, err = claudeprovider.Invocation(ctx, exe, authHome, input)
	}
	if err != nil {
		t.Fatal("Claude invocation failed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	var completed contracts.CommandResult
	go func() {
		var runErr error
		completed, runErr = s.Run(runCtx, request, invocation, input)
		done <- runErr
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("run refused before recorder: %v", err)
	case <-ctx.Done():
		t.Fatal("launch timeout")
	}
	var observedMarker []byte
	if afterWrite {
		// Observe a real tool effect independently of model prose. No fixed
		// startup sleep is evidence that a model was actually in flight.
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			contents, err := os.ReadFile(marker)
			if err == nil && string(bytes.TrimSpace(contents)) == "SF_CANCEL_READY" {
				observedMarker = contents
				break
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal("fixture marker could not be inspected")
			}
			select {
			case runErr := <-done:
				toolEvents, errorResults := 0, 0
				for _, line := range bytes.Split(completed.Stdout, []byte("\n")) {
					var event struct {
						Type    string `json:"type"`
						IsError bool   `json:"is_error"`
					}
					if json.Unmarshal(line, &event) == nil {
						if event.Type == "tool_call" {
							toolEvents++
						}
						if event.Type == "result" && event.IsError {
							errorResults++
						}
					}
				}
				t.Fatalf("provider ended before write: run_error=%t drain_unclear=%t exit=%d tool_events=%d error_results=%d stdout_bytes=%d stderr_bytes=%d", runErr != nil, errors.Is(runErr, ErrUnclear), completed.ExitCode, toolEvents, errorResults, len(completed.Stdout), len(completed.Stderr))
			case <-ctx.Done():
				t.Fatal("Claude did not reach the bounded write observation")
			case <-ticker.C:
			}
		}
	} else {
		// Give the staged process a bounded opportunity to enter CLI startup.
		time.Sleep(200 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancel returned success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancel did not join within bound")
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	proof, err := s.Drain(drainCtx, request)
	if err != nil || !contracts.VerifyDrainProof(s.PublicKey(), request, proof) {
		t.Fatal("no authenticated drain proof")
	}
	if afterWrite {
		contents, err := os.ReadFile(marker)
		if err != nil || !bytes.Equal(contents, observedMarker) {
			t.Fatal("cancellation discarded or changed the partial worktree effect")
		}
	}
}
