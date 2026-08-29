package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

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
