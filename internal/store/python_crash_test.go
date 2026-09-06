package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/executionpolicy"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/repositoryexec"
)

type pythonCrashInput struct {
	Database, Gate, GitHelper, GitHome, Snapshots string
	Claim                                         contracts.RepositoryCommandClaim
	Spec                                          contracts.CommandSpec
}

type recordingPythonDrainer struct {
	processsupervisor.RepositoryCommandDrainer
	err error
}

func (d *recordingPythonDrainer) DrainRepositoryCommandTree(ctx context.Context, launch contracts.RepositoryCommandLaunch, groups []contracts.RepositoryCommandLaunch) error {
	d.err = d.RepositoryCommandDrainer.DrainRepositoryCommandTree(ctx, launch, groups)
	return d.err
}

func TestPreparedPythonCrashChild(t *testing.T) {
	path := os.Getenv("SF_PYTHON_CRASH_INPUT")
	if path == "" {
		t.Skip("subprocess-only crash fixture")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > 32768 {
		t.Fatal("invalid crash input")
	}
	var input pythonCrashInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	db, err := Open(t.Context(), input.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	policy, err := executionpolicy.NewCommandSnapshot(input.Spec.Argv)
	if err != nil {
		t.Fatal(err)
	}
	s := processsupervisor.RepositoryCommandSupervisor{Executable: input.Gate, PythonSnapshots: input.Snapshots, GitRunner: gitboundary.Runner{Binary: "/usr/bin/git", ExecHelper: input.GitHelper, Home: input.GitHome}, SoftDrain: 100 * time.Millisecond, HardDrain: time.Second}
	_, err = (repositoryexec.Executor{Authority: db, Supervisor: s}).Run(t.Context(), repositoryexec.Request{Claim: input.Claim, Spec: input.Spec, Policy: policy})
	t.Fatalf("crash child unexpectedly returned: %v", err)
}

func assertPythonCrashRecovery(t *testing.T, db *Store, database string, s processsupervisor.RepositoryCommandSupervisor, claim contracts.RepositoryCommandClaim, spec contracts.CommandSpec, ambiguous bool) {
	t.Helper()
	owned := t.TempDir()
	if err := os.Chmod(owned, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(pythonCrashInput{database, s.Executable, s.GitRunner.ExecHelper, s.GitRunner.Home, s.PythonSnapshots, claim, spec})
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(owned, "input.json")
	if err := os.WriteFile(input, data, 0600); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestPreparedPythonCrashChild$", "-test.count=1")
	child.Env = append(os.Environ(), "SF_PYTHON_CRASH_INPUT="+input, "TMPDIR="+owned)
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	childReaped := false
	defer func() {
		if !childReaped {
			_ = child.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("crash executor did not reap")
			}
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	var launch contracts.RepositoryCommandLaunch
	for launch.PID == 0 {
		select {
		case err := <-done:
			childReaped = true
			t.Fatalf("executor exited before launch: %v", err)
		case <-ctx.Done():
			t.Fatal("launch was not persisted")
		case <-tick.C:
			_ = db.db.QueryRowContext(ctx, `SELECT process_pid,process_pgid,process_boot_identity,process_start_identity FROM repository_command_leases WHERE semantic_key=? AND launch_state='released'`, claim.SemanticKey).Scan(&launch.PID, &launch.PGID, &launch.BootIdentity, &launch.ProcessStartIdentity)
		}
	}
	drainer := processsupervisor.RepositoryCommandDrainer{SoftDrain: 100 * time.Millisecond, HardDrain: time.Second}
	// Register cleanup as soon as the child is authenticated, before any
	// subsequent assertion can fail while that child remains alive.
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := drainer.DrainRepositoryCommand(cleanup, launch); err != nil {
			t.Errorf("fixture child drain: %v", err)
		}
	}()
	// Keep the executor alive long enough to release its recorded gate.
	select {
	case <-time.After(2 * time.Second):
	case err := <-done:
		childReaped = true
		t.Fatalf("executor exited early: %v", err)
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		childReaped = true
	case <-time.After(5 * time.Second):
		t.Fatal("executor kill did not reap")
	}
	if err := syscall.Kill(launch.PID, 0); err != nil {
		t.Fatalf("fixture had no surviving recorded child: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	leader, err := reopened.AcquireLeader(ctx, claim.TicketRef.Channel, "python-restart")
	if err != nil {
		t.Fatal(err)
	}
	if ambiguous {
		if err := reopened.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, leader, repositoryRecoveryDrainer{err: processsupervisor.ErrUnclear}); err != nil {
			t.Fatal(err)
		}
		var state string
		if err := reopened.db.QueryRowContext(ctx, `SELECT state FROM repository_command_leases WHERE semantic_key=?`, claim.SemanticKey).Scan(&state); err != nil || state != "quarantined" {
			t.Fatalf("unclear lease=%q err=%v", state, err)
		}
		if err := syscall.Kill(launch.PID, 0); err != nil {
			t.Fatalf("unclear fixture lost surviving child: %v", err)
		}
		if _, err := reopened.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unclear recovery minted result: %v", err)
		}
		competing := RepositoryCommandIntent{EffectFence: EffectFence{SemanticKey: claim.SemanticKey + "-competing", Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: claim.RunnerEpoch}}, RequestDigest: claim.RequestDigest, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, Branch: claim.Branch, BaseRef: claim.BaseRef, BaseSHA: claim.BaseSHA, CommandDigest: claim.CommandDigest, SpecDigest: claim.SpecDigest, PolicyDigest: claim.PolicyDigest, ExecutablePath: claim.ExecutablePath, ExecutableDigest: claim.ExecutableDigest}
		if _, err := reopened.PlanEffect(ctx, EffectPlan{SemanticKey: competing.SemanticKey, Ref: claim.TicketRef, Kind: "repository_command", TicketVersion: claim.TicketVersion, Fence: competing.Fence, RequestDigest: claim.RequestDigest}); err != nil {
			t.Fatal(err)
		}
		next, err := reopened.IssueRepositoryCommandClaim(ctx, competing)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := reopened.AcquireRepositoryCommand(ctx, next)
		if lease != nil {
			_ = lease.Quarantine()
		}
		if err == nil || lease != nil {
			t.Fatal("quarantined Python admitted a competing repository writer")
		}
		if err := reopened.RetireUnleasedRepositoryCommand(ctx, next); err != nil {
			t.Fatal(err)
		}
	}
	observedDrainer := &recordingPythonDrainer{RepositoryCommandDrainer: drainer}
	if err := reopened.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, leader, observedDrainer); err != nil {
		t.Fatal(err)
	}
	var leases int
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE semantic_key=?`, claim.SemanticKey).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases == 1 && errors.Is(observedDrainer.err, processsupervisor.ErrUnclear) {
		// Darwin can report EPERM while the terminated orphan group is being
		// removed. Preserve quarantine; wait for independent ESRCH evidence
		// before asking Store to retry its exact authenticated recovery.
		var state string
		if err := reopened.db.QueryRowContext(ctx, `SELECT state FROM repository_command_leases WHERE semantic_key=?`, claim.SemanticKey).Scan(&state); err != nil || state != "quarantined" {
			t.Fatalf("ambiguous drain did not quarantine: %q %v", state, err)
		}
		t.Logf("first drain retained quarantine: %v; group observation: %v", observedDrainer.err, syscall.Kill(-launch.PGID, 0))
		gone, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		for !errors.Is(syscall.Kill(-launch.PGID, 0), syscall.ESRCH) {
			select {
			case <-gone.Done():
				t.Fatal("recorded group did not disappear after drain")
			case <-time.After(20 * time.Millisecond):
			}
		}
		if err := reopened.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, leader, observedDrainer); err != nil {
			t.Fatal(err)
		}
		if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE semantic_key=?`, claim.SemanticKey).Scan(&leases); err != nil {
			t.Fatal(err)
		}
	}
	if leases != 0 {
		t.Fatalf("recovery residue leases=%d err=%v drain=%v group=%v", leases, err, observedDrainer.err, syscall.Kill(-launch.PGID, 0))
	}
	if !errors.Is(syscall.Kill(-launch.PGID, 0), syscall.ESRCH) {
		t.Fatal("recorded process group survived recovery")
	}
	if _, err := reopened.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("crash minted result: %v", err)
	}
	effect, err := reopened.Effect(ctx, claim.SemanticKey)
	if err != nil || effect.State != EffectFailed {
		t.Fatalf("recovery effect=%v err=%v", effect.State, err)
	}
	if err := reopened.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, leader, drainer); err != nil {
		t.Fatalf("recovery replay: %v", err)
	}
}
