package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/store"
)

func TestCompiledDevGateRestartDrainsRecordedGroupBeforeCallerContinues(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "sf-dev")
	build := exec.Command("go", "build", "-ldflags", "-X github.com/nysa-company/sf/internal/version.Channel=dev", "-o", binary, ".")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sf-dev: %v\n%s", err, output)
	}
	owner, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	owner.Executable = binary
	launches := make(chan contracts.ProviderLaunch, 1)
	request := contracts.DrainRequest{ClaimID: 9, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-gate-restart"}, Phase: domain.PhaseBuild, Attempt: 1, Identity: domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "1"}, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "provider/fixture", BindingDigest: strings.Repeat("a", 64)}
	owner.SetLaunchRecorder(func(_ context.Context, got contracts.DrainRequest, launch contracts.ProviderLaunch) error {
		if got != request {
			return fmt.Errorf("unexpected launch request")
		}
		launches <- launch
		return nil
	})
	finished := make(chan error, 1)
	go func() {
		_, err := owner.Run(context.Background(), request, contracts.Invocation{Argv: []string{"/bin/sh", "-c", "sleep 10"}}, contracts.PhaseInput{Worktree: t.TempDir()})
		finished <- err
	}()
	var launch contracts.ProviderLaunch
	select {
	case launch = <-launches:
	case <-time.After(5 * time.Second):
		t.Fatal("gate did not durably publish launch")
	}
	// A new supervisor models the post-crash daemon. It can only signal after
	// reading and validating the durable identity emitted before gate release.
	restarted, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.DrainPersisted(context.Background(), request, launch); err != nil {
		t.Fatalf("restart drain: %v", err)
	}
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("owner run unexpectedly succeeded after recovery termination")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("provider group survived recovery drain")
	}
	if err := syscall.Kill(-launch.PGID, 0); err != syscall.ESRCH {
		t.Fatalf("provider group still exists: %v", err)
	}
}

func TestProductionForegroundDaemonServesAnotherCLIClient(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sfh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelStable)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	database, err := store.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateProject(context.Background(), store.Project{Channel: domain.ChannelStable, ID: "demo", Path: t.TempDir(), BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	binary := filepath.Join(t.TempDir(), "sf")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build production CLI: %v\n%s", err, output)
	}
	environment := append(os.Environ(), "HOME="+home)
	foreground := exec.Command(binary, "daemon", "run")
	foreground.Env = environment
	var foregroundOutput bytes.Buffer
	foreground.Stdout = &foregroundOutput
	foreground.Stderr = &foregroundOutput
	if err := foreground.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if foreground.ProcessState == nil || !foreground.ProcessState.Exited() {
			_ = foreground.Process.Signal(os.Interrupt)
			_, _ = foreground.Process.Wait()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Lstat(paths.Socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = foreground.Wait()
			t.Fatalf("production daemon did not create its socket: %s", foregroundOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	ticket := filepath.Join(t.TempDir(), "ticket.md")
	if err := os.WriteFile(ticket, []byte("# Production daemon\n\nSubmit through the compiled CLI.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	submit := exec.Command(binary, "submit", ticket, "--project", "demo", "--json")
	submit.Env = environment
	output, err := submit.CombinedOutput()
	if err != nil {
		t.Fatalf("submit through production CLI: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), `"ticket":"SF-`) || !strings.Contains(string(output), `"state":"queued"`) {
		t.Fatalf("unexpected submit output: %s", output)
	}
	if err := foreground.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	if err := foreground.Wait(); err != nil {
		t.Fatalf("foreground daemon exit: %v", err)
	}
}

func TestCompiledChannelBinaryReportsExecutableUnknownProjectRecovery(t *testing.T) {
	for _, test := range []struct {
		name    string
		channel domain.Channel
		ldflags string
	}{
		{name: "stable", channel: domain.ChannelStable},
		{name: "dev", channel: domain.ChannelDev, ldflags: "-X github.com/nysa-company/sf/internal/version.Channel=dev"},
	} {
		t.Run(test.name, func(t *testing.T) {
			home, err := os.MkdirTemp("/tmp", "sfh-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			paths, err := config.PathsFor(home, test.channel)
			if err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(t.TempDir(), "sf")
			arguments := []string{"build", "-o", binary, "."}
			if test.ldflags != "" {
				arguments = []string{"build", "-ldflags", test.ldflags, "-o", binary, "."}
			}
			build := exec.Command("go", arguments...)
			build.Dir = "."
			if output, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build %s CLI: %v\n%s", test.name, err, output)
			}
			environment := append(os.Environ(), "HOME="+home)
			foreground := exec.Command(binary, "daemon", "run")
			foreground.Env = environment
			var foregroundOutput bytes.Buffer
			foreground.Stdout = &foregroundOutput
			foreground.Stderr = &foregroundOutput
			if err := foreground.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if foreground.ProcessState == nil || !foreground.ProcessState.Exited() {
					_ = foreground.Process.Signal(os.Interrupt)
					_, _ = foreground.Process.Wait()
				}
			})
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Lstat(paths.Socket); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = foreground.Wait()
					t.Fatalf("%s daemon did not create its socket: %s", test.name, foregroundOutput.String())
				}
				time.Sleep(20 * time.Millisecond)
			}
			ticket := filepath.Join(t.TempDir(), "ticket.md")
			if err := os.WriteFile(ticket, []byte("# Unknown project\n\nSubmit through the compiled CLI.\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			submit := exec.Command(binary, "submit", ticket, "--project", "missing")
			submit.Env = environment
			output, err := submit.CombinedOutput()
			exitError, ok := err.(*exec.ExitError)
			if !ok || exitError.ExitCode() != 3 {
				t.Fatalf("%s submit error=%v output=%s", test.name, err, output)
			}
			binaryName := "sf"
			if test.channel == domain.ChannelDev {
				binaryName = "sf-dev"
			}
			if got := string(output); !strings.Contains(got, "Error: unknown_project:") || !strings.Contains(got, "Next: "+binaryName+" init --help") || strings.Contains(got, "Next: sf doctor") {
				t.Fatalf("%s unexpected submit output: %s", test.name, got)
			}
		})
	}
}
