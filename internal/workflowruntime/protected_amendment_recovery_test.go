package workflowruntime_test

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

func TestRepositoryMaterializerPostbuildAmendmentPreparedIndexRecovery(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "same_fence"
		if restart {
			name = "new_leader"
		}
		t.Run(name, func(t *testing.T) { testPostbuildAmendmentPreparedIndexRecovery(t, restart) })
	}
}

func testPostbuildAmendmentPreparedIndexRecovery(t *testing.T, restart bool) {
	f := newMaterializerRealFixtureWithProvider(t, func(t *testing.T) string { return writeMaterializerAmendmentProvider(t, true) })
	f.worker.Engine = f.state.StateMachine
	for _, want := range []domain.State{domain.StateVerifying, domain.StateBuilding, domain.StateBuilding, domain.StateVerifying} {
		if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != want {
			t.Fatalf("setup=%+v want=%s err=%v", result, want, err)
		}
	}
	pending, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := f.db.PostbuildVerificationAmendmentContext(f.ctx, f.ref, pending.Version, f.fence)
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := os.ReadFile(filepath.Join(f.worktree, "tracked_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	staging := rawMaterializerGit(t, f.worktree, "diff", "--cached", "--binary", "--", "tracked_test.go")
	crash := errors.New("injected protected index synchronization loss")
	faulted := f.materializer
	faulted.BeforePreparedCommitObservation = func() error {
		// Commit already persisted its exact prepared tuple and crossed CAS.
		// Recreate only the ordinary protected index's pre-sync state, retaining
		// the accepted worktree bytes and all implementation index entries.
		runMaterializerGit(t, f.worktree, "reset", proof.Binding.OriginalCheckpointOID, "--", "proof_test.go")
		return crash
	}
	f.worker.CheckpointMaterializer = faulted
	if _, err := f.worker.Run(f.ctx, f.ref, f.fence); !errors.Is(err, crash) {
		t.Fatalf("crash=%v", err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 5)
	receipt, err := f.db.PostbuildAmendmentCheckpointSnapshot(f.ctx, f.ref, pending.Version, f.fence)
	if err != nil {
		t.Fatal(err)
	}
	prepared, found, err := f.db.PostbuildAmendmentPreparedCheckpoint(f.ctx, f.ref, pending.Version, f.fence)
	if err != nil || !found {
		t.Fatalf("prepared=%+v found=%t err=%v", prepared, found, err)
	}
	if got := rawMaterializerGit(t, f.worktree, "diff", "--cached", "--name-only", "--", "proof_test.go"); got != "proof_test.go" {
		t.Fatalf("fault did not leave protected index unsynced: %q", got)
	}
	observations, err := sql.Open("sqlite", "file:"+f.databasePath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	counts := func() [2]int {
		t.Helper()
		var values [2]int
		for i, table := range []string{"repository_command_results", "effects"} {
			if err := observations.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE channel=? AND project_id=? AND ticket_id=?", f.ref.Channel, f.ref.Project, f.ref.Ticket).Scan(&values[i]); err != nil {
				t.Fatal(err)
			}
		}
		return values
	}
	before := counts()
	if restart {
		leader, err := f.db.AcquireLeader(f.ctx, domain.ChannelDev, "prepared-amendment-restart")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.db.SetRecoveryAuthority(f.ctx, domain.ChannelDev, leader, f.supervisor.PublicKey()); err != nil {
			t.Fatal(err)
		}
		if changed, err := f.db.FenceRecoveredRunners(f.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
			t.Fatalf("restart changed=%d err=%v", changed, err)
		}
		pending, err = f.db.Ticket(f.ctx, f.ref)
		if err != nil {
			t.Fatal(err)
		}
		f.fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: pending.RunnerEpoch}
		preserved, err := f.db.PostbuildAmendmentCheckpointSnapshot(f.ctx, f.ref, pending.Version, f.fence)
		if err != nil || !reflect.DeepEqual(preserved, receipt) {
			t.Fatalf("restart changed receipt: %+v %v", preserved, err)
		}
	}
	coordinator := worktreecoord.Coordinator{Store: f.db, Git: f.materializer.Git}
	if _, err := coordinator.AuthenticatePreparedPostbuildAmendment(f.ctx, worktreecoord.EnsureRequest{Ref: f.ref, Version: pending.Version, Fence: f.fence}); err != nil {
		t.Fatalf("prepared-only admission: %v", err)
	}
	f.worker.CheckpointMaterializer = f.materializer
	result, err := f.worker.ResumePreparedPostbuildAmendment(f.ctx, f.ref, pending.Version, f.fence)
	if err != nil || result.State != domain.StateBuilding || !result.Transitioned {
		t.Fatalf("prepared resume=%+v err=%v", result, err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 5)
	if after := counts(); after != before {
		t.Fatalf("resume added command/effect: before=%v after=%v", before, after)
	}
	current, err := f.db.CurrentVerification(f.ctx, f.ref)
	if err != nil || current.Checkpoint != prepared || current.CommandBinding.Key != receipt.Command {
		t.Fatalf("resume replaced checkpoint: %+v %v", current, err)
	}
	if got := rawMaterializerGit(t, f.worktree, "diff", "--cached", "--name-only", "--", "proof_test.go"); got != "" {
		t.Fatalf("protected index still unsynced: %q", got)
	}
	after, err := os.ReadFile(filepath.Join(f.worktree, "tracked_test.go"))
	if err != nil || !reflect.DeepEqual(after, implementation) || rawMaterializerGit(t, f.worktree, "diff", "--cached", "--binary", "--", "tracked_test.go") != staging {
		t.Fatalf("resume altered implementation bytes/staging: %v", err)
	}
	if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != domain.StatePublishing {
		t.Fatalf("fresh Builder=%+v err=%v", result, err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 6)
}
