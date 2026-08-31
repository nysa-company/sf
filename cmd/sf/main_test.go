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
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

func bindSupervisorTestInput(t *testing.T, request *contracts.DrainRequest) contracts.PhaseInput {
	t.Helper()
	input := contracts.PhaseInput{Ticket: request.Ref, Phase: request.Phase, Attempt: request.Attempt, LeaderEpoch: request.LeaderEpoch, RunnerEpoch: request.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Prompt: "supervisor fixture", Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, AllowedPaths: []string{"."}, Provider: request.Identity, AuthMode: request.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}
	_, digest, err := contracts.CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	input.RequestDigest, request.RequestDigest = digest, digest
	return input
}

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
	request := contracts.DrainRequest{ClaimID: 9, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-gate-restart"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, Identity: domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "1"}, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "provider/fixture", BindingDigest: strings.Repeat("a", 64), Repository: "/repo", Worktree: t.TempDir(), WorktreeIdentity: "identity", BaseSHA: "base"}
	binaryDigest, err := owner.RegisterExecutable(request.Identity, "/bin/sh")
	if err != nil {
		t.Fatalf("register fixture executable: %v", err)
	}
	request.BinaryDigest = binaryDigest
	request.PolicyDigest = owner.PolicyDigest()
	input := bindSupervisorTestInput(t, &request)
	owner.SetLaunchRecorder(func(_ context.Context, got contracts.DrainRequest, launch contracts.ProviderLaunch) error {
		if got != request {
			return fmt.Errorf("unexpected launch request")
		}
		launches <- launch
		return nil
	})
	finished := make(chan error, 1)
	go func() {
		_, err := owner.Run(context.Background(), request, contracts.Invocation{Argv: []string{"/bin/sh", "-c", "sleep 10"}}, input)
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

func TestCompiledDevRunRetainsClaimUntilNormalDrain(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "sf-dev")
	build := exec.Command("go", "build", "-ldflags", "-X github.com/nysa-company/sf/internal/version.Channel=dev", "-o", binary, ".")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sf-dev: %v\n%s", err, output)
	}
	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Executable = binary
	identity := domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "1"}
	binaryDigest, err := supervisor.RegisterExecutable(identity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DrainRequest{ClaimID: 10, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-normal-drain"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, Identity: identity, LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "provider/fixture", BindingDigest: strings.Repeat("a", 64), BinaryDigest: binaryDigest, PolicyDigest: supervisor.PolicyDigest(), Repository: "/repo", Worktree: t.TempDir(), WorktreeIdentity: "identity", BaseSHA: "base"}
	input := bindSupervisorTestInput(t, &request)
	supervisor.SetLaunchRecorder(func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error { return nil })
	if _, err := supervisor.Run(context.Background(), request, contracts.Invocation{Argv: []string{"/bin/sh", "-c", "exit 0"}}, input); err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Drain(context.Background(), request); err != nil {
		t.Fatalf("completed run was not retained for normal drain: %v", err)
	}
	if _, err := supervisor.Drain(context.Background(), request); err == nil {
		t.Fatal("drain proof remained reusable after cleanup")
	}
}

func TestCompiledDevCoordinatorDrainsNormalRunBeforeFinishingAttempt(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	worktree := t.TempDir()
	raw := []byte(`{"providers":"compiled-normal"}`)
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "compiled", Path: worktree, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "compiled-coordinator")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "compiled", Ticket: "SF-compiled-normal"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "compiled", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Minute, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := database.StartOrAdopt(ctx, ref, 1, "dev/compiled/SF-compiled-normal", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	identityJSON := `{"repository":"compiled"}`
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: worktree, Branch: "dev/compiled/SF-compiled-normal", IdentityJSON: []byte(identityJSON), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	transition, err := database.Transition(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBuilding, Trigger: "compiled-normal", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "sf-dev")
	build := exec.Command("go", "build", "-ldflags", "-X github.com/nysa-company/sf/internal/version.Channel=dev", "-o", binary, ".")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sf-dev: %v\n%s", err, output)
	}
	owner.Executable = binary
	owner.SetLaunchRecorder(func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error { return nil })
	providerIdentity := domain.ProviderIdentity{Provider: "compiled-normal", Model: "compiled-model", Family: "compiled-family", Version: "1"}
	binaryDigest, err := owner.RegisterExecutable(providerIdentity, "/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	qual := func(run string, identity domain.ProviderIdentity, providerDigest, policyDigest string) store.ProviderQualification {
		return store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: identity, BinaryDigest: providerDigest, PolicyDigest: policyDigest, FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()}
	}
	builder, _, err := database.RecordProviderQualification(ctx, qual(strings.Repeat("1", 32), providerIdentity, binaryDigest, owner.PolicyDigest()))
	if err != nil {
		t.Fatal(err)
	}
	reviewerIdentity := domain.ProviderIdentity{Provider: "compiled-reviewer", Model: "reviewer-model", Family: "reviewer-family", Version: "1"}
	reviewer, _, err := database.RecordProviderQualification(ctx, qual(strings.Repeat("2", 32), reviewerIdentity, strings.Repeat("d", 64), strings.Repeat("e", 64)))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.SelectProviderSet(ctx, domain.ChannelDev, builder.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	provider := &compiledNormalProvider{binding: contracts.RuntimeBinding{Identity: providerIdentity, BinaryDigest: binaryDigest, PolicyDigest: owner.PolicyDigest(), FixtureDigest: strings.Repeat("c", 64), AuthDigest: strings.Repeat("d", 64)}}
	registry := providercoord.NewRegistry()
	if err := registry.Register(ctx, provider); err != nil {
		t.Fatal(err)
	}
	coordinator, err := providercoord.New(registry, map[providercoord.Role]providercoord.Route{providercoord.RoleBuilder: {Primary: provider.Name()}}, database, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(ctx, providercoord.Request{Role: providercoord.RoleBuilder, ExpectedVersion: transition.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhaseBuild, Prompt: "compiled normal", Repository: worktree, Worktree: worktree, WorktreeIdentity: identityJSON, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"."}, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}})
	if result.Code != providercoord.Completed {
		t.Fatalf("compiled coordinator result=%+v", result)
	}
	attempts, err := database.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "completed" {
		t.Fatalf("normal run was not finalized after drain: attempts=%+v err=%v", attempts, err)
	}
}

func TestCompiledDevDaemonRecoversDurableGatedProviderBeforeSocket(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sfh-")
	if err != nil {
		t.Fatal(err)
	}
	home, err = filepath.EvalSymlinks(home)
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
	fixtureIdentity := domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "1"}
	binaryDigest, err := owner.RegisterExecutable(fixtureIdentity, "/bin/sh")
	if err != nil {
		t.Fatalf("register fixture executable: %v", err)
	}
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
	builderIdentity := domain.ProviderIdentity{Provider: "builder", Model: "builder-model", Family: "builder-family", Version: "1"}
	binaryDigest, err = owner.RegisterExecutable(builderIdentity, "/bin/sh")
	if err != nil {
		t.Fatalf("register fixture executable: %v", err)
	}
	qual := func(run, name, family string) store.ProviderQualification {
		binary := strings.Repeat("a", 64)
		if name == builderIdentity.Provider {
			binary = binaryDigest
		}
		policy := strings.Repeat("b", 64)
		if name == builderIdentity.Provider {
			policy = owner.PolicyDigest()
		}
		return store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: family, Version: "1"}, BinaryDigest: binary, PolicyDigest: policy, FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()}
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
	registry := providercoord.NewRegistry()
	provider := &compiledIntegrationProvider{binding: binding}
	if err := registry.Register(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	coordinator, err := providercoord.New(registry, map[providercoord.Role]providercoord.Route{providercoord.RoleBuilder: {Primary: "builder"}}, writer, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	coordinatorDone := make(chan providercoord.Result, 1)
	go func() {
		coordinatorDone <- coordinator.Run(context.Background(), providercoord.Request{Role: providercoord.RoleBuilder, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhaseBuild, Prompt: "integration", Repository: worktree, Worktree: worktree, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"."}, Timeout: 5 * time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}})
	}()
	var launch contracts.ProviderLaunch
	deadline := time.Now().Add(5 * time.Second)
	var claim store.ProviderAttempt
	for {
		select {
		case result := <-coordinatorDone:
			t.Fatalf("coordinator returned before launch: %+v", result)
		default:
		}
		claims, claimsErr := writer.ActiveProviderAttempts(context.Background(), domain.ChannelDev)
		if claimsErr == nil && len(claims) == 1 {
			claim = claims[0]
			launch, err = writer.ProviderLaunchIdentity(context.Background(), claim.ProviderAttemptClaim)
			if err == nil {
				break
			}
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
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Lstat(paths.Socket); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket was not exposed after recovery")
		}
		time.Sleep(20 * time.Millisecond)
	}
	status := exec.Command(binary, "status", "--json")
	status.Env = append(os.Environ(), "HOME="+home)
	if out, err := status.CombinedOutput(); err != nil {
		t.Fatalf("socket did not accept status: %v %s", err, out)
	}
	select {
	case <-coordinatorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("owner waiter did not finish")
	}
}

func TestCompiledDevDaemonQuarantinesMismatchedForeignProviderBeforeSocket(t *testing.T) {
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

	// Capture the current boot identity through the public supervisor boundary;
	// the foreign process below gets a deliberately mismatched start identity.
	owner, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	owner.Executable = binary
	fixtureIdentity := domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "1"}
	binaryDigest, err := owner.RegisterExecutable(fixtureIdentity, "/bin/sh")
	if err != nil {
		t.Fatalf("register fixture executable: %v", err)
	}
	var bootIdentity string
	owner.SetLaunchRecorder(func(_ context.Context, _ contracts.DrainRequest, launch contracts.ProviderLaunch) error {
		bootIdentity = launch.BootIdentity
		return nil
	})
	request := contracts.DrainRequest{
		ClaimID: 1, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-identity-probe"},
		Phase: domain.PhaseBuild, Role: "builder", Attempt: 1, Identity: domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "1"},
		LeaderEpoch: 1, RunnerEpoch: 1, ExpectedVersion: 1, LeaseKey: "provider/identity-probe", BindingDigest: strings.Repeat("a", 64), BinaryDigest: binaryDigest, PolicyDigest: owner.PolicyDigest(), Repository: "/repo", Worktree: t.TempDir(), WorktreeIdentity: "identity", BaseSHA: "base",
	}
	input := bindSupervisorTestInput(t, &request)
	if _, err := owner.Run(context.Background(), request,
		contracts.Invocation{Argv: []string{"/bin/sh", "-c", "sleep 0.1"}}, input); err != nil {
		t.Fatalf("capture boot identity: %v", err)
	}
	if bootIdentity == "" {
		t.Fatal("supervisor did not publish boot identity")
	}

	foreign := exec.Command("/bin/sleep", "30")
	foreign.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := foreign.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if foreign.ProcessState != nil && foreign.ProcessState.Exited() {
			return
		}
		_ = syscall.Kill(-foreign.Process.Pid, syscall.SIGTERM)
		wait := make(chan struct{})
		go func() {
			_, _ = foreign.Process.Wait()
			close(wait)
		}()
		select {
		case <-wait:
		case <-time.After(time.Second):
			_ = syscall.Kill(-foreign.Process.Pid, syscall.SIGKILL)
			<-wait
		}
	})

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
	t.Cleanup(func() { _ = writer.Close() })
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-mismatched-foreign"}
	if err := writer.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: "integration", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := writer.StartOrAdopt(context.Background(), ref, 1, "dev/demo/SF-mismatched-foreign", domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	identity := `{"repository":"integration"}`
	if err := writer.RegisterWorktree(context.Background(), store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, Path: worktree, Branch: "dev/demo/SF-mismatched-foreign", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	transition, err := writer.Transition(context.Background(), store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateBuilding, Trigger: "integration", Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	ticket.Version = transition.Version
	ticket.State = domain.StateBuilding
	builderIdentity := domain.ProviderIdentity{Provider: "builder", Model: "builder-model", Family: "builder-family", Version: "1"}
	binaryDigest, err = owner.RegisterExecutable(builderIdentity, "/bin/sh")
	if err != nil {
		t.Fatalf("register fixture executable: %v", err)
	}
	qual := func(run, name, family string) store.ProviderQualification {
		binary := strings.Repeat("a", 64)
		if name == builderIdentity.Provider {
			binary = binaryDigest
		}
		policy := strings.Repeat("b", 64)
		if name == builderIdentity.Provider {
			policy = owner.PolicyDigest()
		}
		return store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: family, Version: "1"}, BinaryDigest: binary, PolicyDigest: policy, FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()}
	}
	builder, _, err := writer.RecordProviderQualification(context.Background(), qual("33333333333333333333333333333333", "builder", "builder-family"))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := writer.RecordProviderQualification(context.Background(), qual("44444444444444444444444444444444", "reviewer", "reviewer-family"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := writer.SelectProviderSet(context.Background(), domain.ChannelDev, builder.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	auth := sha256.Sum256([]byte("auth"))
	binding := contracts.RuntimeBinding{Identity: builder.Provider, BinaryDigest: builder.BinaryDigest, PolicyDigest: builder.PolicyDigest, FixtureDigest: builder.FixtureDigest, AuthDigest: fmt.Sprintf("%x", auth)}
	claimInput := contracts.PhaseInput{Ticket: ref, Phase: domain.PhaseBuild, LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch, ExpectedVersion: ticket.Version, Prompt: "foreign recovery fixture", Repository: worktree, Worktree: worktree, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}
	claim, err := writer.BeginProviderAttempt(context.Background(), store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: ticket.RunnerEpoch}, Phase: domain.PhaseBuild, Role: "builder", Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(), Repository: worktree, Worktree: worktree, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), SupervisorKey: owner.PublicKey(), Input: claimInput})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.RecordProviderLaunch(context.Background(), claim, contracts.ProviderLaunch{PID: foreign.Process.Pid, PGID: foreign.Process.Pid, BootIdentity: bootIdentity, ProcessStartIdentity: "mismatched-foreign-process-start", Worktree: worktree}); err != nil {
		t.Fatalf("record foreign launch: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("seed daemon left socket behind: %v", err)
	}

	foreground := exec.Command(binary, "daemon", "run")
	foreground.Env = append(os.Environ(), "HOME="+home)
	var foregroundOutput bytes.Buffer
	foreground.Stdout = &foregroundOutput
	foreground.Stderr = &foregroundOutput
	if err := foreground.Start(); err != nil {
		t.Fatal(err)
	}
	err = foreground.Wait()
	if err == nil {
		t.Fatal("daemon unexpectedly accepted mismatched foreign provider claim")
	}
	if exitError, ok := err.(*exec.ExitError); !ok || exitError.ExitCode() == 0 {
		t.Fatalf("daemon had unexpected exit error: %v\n%s", err, foregroundOutput.String())
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("daemon exposed socket despite failed recovery: %v\n%s", err, foregroundOutput.String())
	}
	attempts, err := writer.ProviderAttempts(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].State != "quarantined" || attempts[0].Outcome != "undrained_recovery" {
		t.Fatalf("mismatched claim was not durably quarantined: %+v", attempts)
	}
	if err := syscall.Kill(-foreign.Process.Pid, 0); err != nil {
		t.Fatalf("foreign provider group was signaled or exited: %v", err)
	}
}

// compiledIntegrationProvider is test-only: production composition registers
// no provider until a configured adapter has passed qualification.
type compiledIntegrationProvider struct {
	binding contracts.RuntimeBinding
}

type compiledNormalProvider struct {
	binding contracts.RuntimeBinding
}

func (p *compiledNormalProvider) Name() string { return p.binding.Identity.Provider }
func (p *compiledNormalProvider) Probe(context.Context) (domain.ProviderIdentity, error) {
	return p.binding.Identity, nil
}
func (p *compiledNormalProvider) Binding(context.Context) (contracts.RuntimeBinding, error) {
	return p.binding, nil
}
func (p *compiledNormalProvider) Invocation(context.Context, contracts.PhaseInput) (contracts.Invocation, error) {
	return contracts.Invocation{Argv: []string{"/bin/sh", "-c", "exit 0"}}, nil
}
func (p *compiledNormalProvider) Parse(context.Context, contracts.PhaseInput, contracts.CommandResult) (contracts.PhaseResult, error) {
	return contracts.PhaseResult{Artifact: []byte(`{"schema":"sf.builder/v1","summary":"compiled normal","changed_files":["main.go"],"commands":[["go","test"]]}`), Provider: p.binding.Identity, UsageTrusted: true}, nil
}

func (p *compiledIntegrationProvider) Name() string { return p.binding.Identity.Provider }
func (p *compiledIntegrationProvider) Probe(context.Context) (domain.ProviderIdentity, error) {
	return p.binding.Identity, nil
}
func (p *compiledIntegrationProvider) Binding(context.Context) (contracts.RuntimeBinding, error) {
	return p.binding, nil
}
func (p *compiledIntegrationProvider) Invocation(context.Context, contracts.PhaseInput) (contracts.Invocation, error) {
	return contracts.Invocation{Argv: []string{"/bin/sh", "-c", "sleep 10"}}, nil
}
func (p *compiledIntegrationProvider) Parse(context.Context, contracts.PhaseInput, contracts.CommandResult) (contracts.PhaseResult, error) {
	return contracts.PhaseResult{}, fmt.Errorf("compiled integration provider should be terminated by recovery")
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

func TestCompiledDevQualificationUsesDaemonAttestationAndNeverExecutesModel(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("qualification fails closed without macOS sandbox-exec")
	}
	home, err := os.MkdirTemp("/tmp", "sfh-")
	if err != nil {
		t.Fatal(err)
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	authHome := filepath.Join(home, ".codex")
	if err := os.Mkdir(authHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), []byte(`{"account":"fixture"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	fakeCodex := filepath.Join(bin, "codex")
	// The fake accepts only capability/help and locally sandboxed probe shapes.
	// Any real `codex exec` model invocation exits nonzero, so a green
	// qualification proves this path did not make a model call.
	fake := `#!/bin/sh
case "$1" in
  --version) echo 'codex 1.2.3'; exit 0 ;;
  login) [ "$2" = status ] && { echo 'Logged in using ChatGPT'; exit 0; }; exit 98 ;;
  exec) [ "$2" = '--help' ] && { echo '--json --output-schema --output-last-message --ephemeral --ignore-user-config --ignore-rules --config --model -C'; exit 0; }; exit 97 ;;
  sandbox) case "$*" in
    *curl*) for arg in "$@"; do url="$arg"; done; /usr/bin/curl -fsS --connect-timeout 1 "$url"; exit $? ;;
    *CODEX_HOME*) test -r "$CODEX_HOME/auth.json"; exit $? ;;
    *'test -r /etc/hosts'*) exit 0 ;;
    *'test -w /etc/hosts'*) exit 1 ;;
    *) exit 0 ;;
  esac ;;
esac
exit 98
`
	if err := os.WriteFile(fakeCodex, []byte(fake), 0o700); err != nil {
		t.Fatal(err)
	}
	binary := buildDevRuntimeBundle(t)
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	environment := append(os.Environ(), "HOME="+home, "CODEX_HOME="+authHome, "PATH="+bin+":/usr/bin:/bin")
	foreground := exec.Command(binary, "daemon", "run")
	foreground.Env = environment
	var daemonOutput bytes.Buffer
	foreground.Stdout, foreground.Stderr = &daemonOutput, &daemonOutput
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
			t.Fatalf("dev daemon did not create socket: %s", daemonOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	qualify := exec.Command(binary, "providers", "qualify", "--builder", "codex", "--reviewer", "codex", "--json")
	qualify.Env = environment
	output, err := qualify.CombinedOutput()
	if err != nil {
		t.Fatalf("qualified local Codex pair: %v\n%s\ndaemon=%s", err, output, daemonOutput.String())
	}
	if !strings.Contains(string(output), `"independent":true`) || !strings.Contains(string(output), `"model_call_made":false`) || strings.Contains(string(output), `"auth.json"`) {
		t.Fatalf("unsafe qualification response: %s", output)
	}
	// A second request must be rejected before qualification because the first
	// successful request activated the exact local runtime bundle.
	requalify := exec.Command(binary, "providers", "qualify", "--builder", "codex", "--reviewer", "codex", "--json")
	requalify.Env = environment
	output, err = requalify.CombinedOutput()
	exitError, ok := err.(*exec.ExitError)
	if !ok || exitError.ExitCode() != 3 || !strings.Contains(string(output), `"code":"runtime_already_active"`) {
		t.Fatalf("active runtime requalification error=%v output=%s", err, output)
	}
	if err := foreground.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := foreground.Wait(); err != nil {
		t.Fatalf("graceful SIGINT runtime drain: %v\ndaemon=%s", err, daemonOutput.String())
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket remained after graceful SIGINT: %v", err)
	}
}

// buildDevRuntimeBundle installs the complete development channel bundle in a
// private directory. Production runtime activation resolves sf-dev and its
// sibling sf-git-exec-dev by exact name; packaging the other channel helpers
// and known-host asset here keeps this compiled fixture faithful to build-dev.
func buildDevRuntimeBundle(t *testing.T) string {
	t.Helper()
	bundle := t.TempDir()
	ldflags := "-X github.com/nysa-company/sf/internal/version.Channel=dev"
	for _, target := range []struct {
		name        string
		packagePath string
	}{
		{name: "sf-dev", packagePath: "."},
		{name: "sf-ssh-dev", packagePath: "../../cmd/sf-ssh"},
		{name: "sf-git-exec-dev", packagePath: "../../cmd/sf-git-exec"},
		{name: "sf-git-credential-dev", packagePath: "../../cmd/sf-git-credential"},
	} {
		binary := filepath.Join(bundle, target.name)
		build := exec.Command("go", "build", "-ldflags", ldflags, "-o", binary, target.packagePath)
		build.Dir = "."
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build development runtime asset %s: %v\n%s", target.name, err, output)
		}
	}
	knownHosts, err := os.ReadFile(filepath.Join("..", "..", "internal", "gitssh", "github_known_hosts"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "github_known_hosts"), knownHosts, 0o644); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(bundle, "sf-dev")
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
