package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon"
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

func TestCompiledDevDaemonRecoversDurableGatedProviderBeforeSocket(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sfh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	worktree := t.TempDir()
	raw := []byte(`{"providers":"integration"}`)
	digestBytes := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", digestBytes)
	database, err := store.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	project := store.Project{Channel: domain.ChannelDev, ID: "demo", Path: worktree, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}
	if err := database.CreateProject(context.Background(), project); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
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
	_, thisFile, _, _ := runtime.Caller(0)
	stateMachine := filepath.Join(filepath.Dir(thisFile), "..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	d, err := daemon.Start(context.Background(), daemon.Config{Channel: domain.ChannelDev, Paths: paths, StateMachinePath: stateMachine, DaemonIdentity: "provider-owner", Projects: []store.Project{project}, RecoveryAuthorityKey: owner.PublicKey(), ProviderSupervisor: owner, RecoveryDrainer: owner})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := store.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-gated-recovery"}
	if err := writer.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: "integration", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := writer.StartOrAdopt(context.Background(), ref, 1, "dev/demo/SF-gated-recovery", domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	identity := `{"repository":"integration"}`
	if err := writer.RegisterWorktree(context.Background(), store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, Path: worktree, Branch: "dev/demo/SF-gated-recovery", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	transition, err := writer.Transition(context.Background(), store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBuilding, Trigger: "integration", Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	ticket.Version = transition.Version
	ticket.State = domain.StateBuilding
	qual := func(run, name, family string) store.ProviderQualification {
		return store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: family, Version: "1"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()}
	}
	builder, _, err := writer.RecordProviderQualification(context.Background(), qual("11111111111111111111111111111111", "builder", "builder-family"))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := writer.RecordProviderQualification(context.Background(), qual("22222222222222222222222222222222", "reviewer", "reviewer-family"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.SelectProviderSet(context.Background(), domain.ChannelDev, builder.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	auth := sha256.Sum256([]byte("auth"))
	binding := contracts.RuntimeBinding{Identity: builder.Provider, BinaryDigest: builder.BinaryDigest, PolicyDigest: builder.PolicyDigest, FixtureDigest: builder.FixtureDigest, AuthDigest: fmt.Sprintf("%x", auth)}
	claim, err := writer.BeginProviderAttempt(context.Background(), store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(), Repository: worktree, Worktree: worktree, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), SupervisorKey: owner.PublicKey()})
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: claim.ID, Ref: ref, Phase: claim.Phase, Attempt: claim.Attempt, Identity: binding.Identity, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest}
	ownerDone := make(chan error, 1)
	go func() {
		_, err := owner.Run(context.Background(), request, contracts.Invocation{Argv: []string{"/bin/sh", "-c", "sleep 10"}}, contracts.PhaseInput{Worktree: worktree})
		ownerDone <- err
	}()
	var launch contracts.ProviderLaunch
	deadline := time.Now().Add(5 * time.Second)
	for {
		launch, err = writer.ProviderLaunchIdentity(context.Background(), claim)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("durable launch not recorded: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	} // owner crash: provider group deliberately remains live.
	foreground := exec.Command(binary, "daemon", "run")
	foreground.Env = append(os.Environ(), "HOME="+home)
	if err := foreground.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if foreground.ProcessState == nil || !foreground.ProcessState.Exited() {
			_ = foreground.Process.Signal(os.Interrupt)
			_, _ = foreground.Process.Wait()
		}
	}()
	deadline = time.Now().Add(5 * time.Second)
	for {
		attempts, e := writer.ProviderAttempts(context.Background(), ref)
		if e == nil && len(attempts) == 1 && attempts[0].State == "cancelled" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("claim not reconciled before socket: attempts=%+v err=%v", attempts, e)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := syscall.Kill(-launch.PGID, 0); err != syscall.ESRCH {
		t.Fatalf("provider group survived daemon recovery: %v", err)
	}
	if _, err := os.Lstat(paths.Socket); err != nil {
		t.Fatalf("socket was not exposed after recovery: %v", err)
	}
	status := exec.Command(binary, "status", "--json")
	status.Env = append(os.Environ(), "HOME="+home)
	if out, err := status.CombinedOutput(); err != nil {
		t.Fatalf("socket did not accept status: %v %s", err, out)
	}
	select {
	case <-ownerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("owner waiter did not finish")
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
