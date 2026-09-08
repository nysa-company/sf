package workflowruntime_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

func TestRepositoryMaterializerPostbuildRepairRealEndToEnd(t *testing.T) {
	f := newMaterializerRealFixture(t, true)
	// The fault wrapper intentionally exposes only the older engine contract.
	// Exercise the concrete engine's optional postbuild transition here.
	f.worker.Engine = f.state.StateMachine
	for _, want := range []domain.State{domain.StateVerifying, domain.StateBuilding} {
		if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != want {
			t.Fatalf("setup result=%+v want=%s err=%v", result, want, err)
		}
	}
	original, err := f.db.CurrentVerification(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := f.worker.Run(f.ctx, f.ref, f.fence)
	if err != nil || failed.State != domain.StateBuilding || failed.Version != before.Version+1 || !failed.Transitioned {
		t.Fatalf("failed build must enter a new repair entry: result=%+v err=%v", failed, err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 3)
	repair, err := f.db.PostbuildRepairBuildContext(f.ctx, f.ref, failed.Version, f.fence)
	if err != nil || repair.Repair.OriginalCheckpointOID != original.Checkpoint.CommitOID || !reflect.DeepEqual(repair.Verification, original) {
		t.Fatalf("repair lost original verification: context=%+v err=%v", repair, err)
	}
	coordinator := worktreecoord.Coordinator{Store: f.db, Git: f.materializer.Git}
	admitted, err := coordinator.AuthenticatePostbuildRepair(f.ctx, worktreecoord.EnsureRequest{Ref: f.ref, Version: failed.Version, Fence: f.fence})
	if err != nil || admitted.Path != f.worktree {
		t.Fatalf("exact retained checkout admission=%+v err=%v", admitted, err)
	}
	if _, err := f.db.LatestCandidate(f.ctx, f.ref); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed build created candidate: %v", err)
	}
	// Read-only observations of the real database, never fabricated evidence.
	observations, err := sql.Open("sqlite", "file:"+f.databasePath+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer observations.Close()
	count := func(query string) int {
		t.Helper()
		var value int
		if err := observations.QueryRowContext(f.ctx, query, f.ref.Channel, f.ref.Project, f.ref.Ticket).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	commandsSQL := `SELECT COUNT(*) FROM repository_command_results WHERE channel=? AND project_id=? AND ticket_id=?`
	commandsBefore := count(commandsSQL)
	result, err := f.worker.Run(f.ctx, f.ref, f.fence)
	if err != nil || result.State != domain.StatePublishing || !result.Transitioned {
		t.Fatalf("fresh repair Builder did not publish: result=%+v err=%v", result, err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 4)
	candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
	if err != nil || candidate.BuilderResult.AttemptID <= repair.Builder.AttemptID {
		t.Fatalf("candidate reused failed Builder: candidate=%+v err=%v", candidate, err)
	}
	retained, err := f.db.RecoverableVerification(f.ctx, f.ref)
	if err != nil || !reflect.DeepEqual(retained.Revision, original.Revision) || retained.Checkpoint != original.Checkpoint || retained.ProviderResult != original.ProviderResult || retained.CommandBinding != original.CommandBinding {
		t.Fatalf("repair replaced frozen proof: verification=%+v err=%v", retained, err)
	}
	commandsAfter := count(commandsSQL)
	if commandsAfter != commandsBefore+1 {
		t.Fatalf("fresh repair command count before=%d after=%d", commandsBefore, commandsAfter)
	}
	// A later scheduler tick must not launch another provider or command.
	if replay, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || replay.State != domain.StatePublishing || replay.Transitioned {
		t.Fatalf("publishing replay=%+v err=%v", replay, err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 4)
	if got := count(commandsSQL); got != commandsAfter {
		t.Fatalf("replay duplicated repository command: before=%d after=%d", commandsAfter, got)
	}
	if got := count(`SELECT COUNT(*) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`); got != 1 {
		t.Fatalf("candidate count=%d want=1", got)
	}
	if got := count(`SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`); got != 1 {
		t.Fatalf("correction count=%d want=1", got)
	}
	if got := count(`SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`); got != 1 {
		t.Fatalf("correction ledger count=%d want=1", got)
	}
}

func TestRepositoryMaterializerPreparePostbuildRepairRealBoundary(t *testing.T) {
	f := newMaterializerRealFixture(t, true)
	f.state.failVerification, f.state.failCandidate = false, false
	for _, want := range []domain.State{domain.StateVerifying, domain.StateBuilding} {
		if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != want {
			t.Fatalf("setup state=%+v want=%s err=%v", result, want, err)
		}
	}
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := f.db.Plan(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := f.db.CurrentVerification(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	request := workflowworker.PhaseRequest{Ticket: ticket, Worktree: worktree, Phase: domain.PhaseBuild, Fence: f.fence, Plan: &plan, Verification: &verification}
	// Run only the provider here. Worker must not consume/block the failure
	// before the physical preparation is tested at its exact Building fence.
	output, err := f.worker.Runner.Run(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	_, builder, err := f.db.LoadCurrentProviderAttemptResult(f.ctx, output.ProviderResult, ticket.Version, f.fence)
	if err != nil || builder.Builder == nil {
		t.Fatalf("builder=%+v err=%v", builder, err)
	}
	_, verifier, err := f.db.LoadHistoricalProviderAttemptResult(f.ctx, verification.ProviderResult)
	if err != nil || verifier.Verify == nil {
		t.Fatalf("verifier=%+v err=%v", verifier, err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(*verifier.Verify, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Checkpoint.CommitOID)
	if err != nil {
		t.Fatal(err)
	}
	_, materializeErr := f.materializer.MaterializeCandidate(f.ctx, request, planIdentity, verificationIdentity, *builder.Builder, output.ProviderResult)
	var failure *workflowworker.PostbuildFailure
	if !errors.As(materializeErr, &failure) || failure == nil {
		t.Fatalf("expected exact failed command, got %v", materializeErr)
	}
	command := failure.CommandResult
	prepared, err := f.materializer.PreparePostbuildRepair(f.ctx, request, output.ProviderResult, command)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Ref != f.ref || prepared.ExpectedVersion != ticket.Version || prepared.Fence != f.fence || prepared.BuilderResult != output.ProviderResult || prepared.CommandResult != command || !strings.HasPrefix(prepared.RetainedWorktreeDigest, "sha256:") || len(prepared.RetainedWorktreeDigest) != 71 {
		t.Fatal("preparation lost its exact source/fingerprint")
	}
	repeated, err := f.materializer.PreparePostbuildRepair(f.ctx, request, output.ProviderResult, command)
	if err != nil || repeated != prepared {
		t.Fatalf("unchanged bytes changed preparation: %v", err)
	}

	t.Run("protected_edit", func(t *testing.T) {
		path := filepath.Join(f.worktree, "proof.txt")
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(path, before, 0o600); err != nil {
				t.Error(err)
			}
		}()
		if err := os.WriteFile(path, []byte("changed protected proof\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := f.materializer.PreparePostbuildRepair(f.ctx, request, output.ProviderResult, command); err == nil {
			t.Fatal("protected edit admitted")
		}
	})
	t.Run("foreign_path", func(t *testing.T) {
		path := filepath.Join(f.worktree, "undeclared.txt")
		if err := os.WriteFile(path, []byte("outside plan\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.Remove(path); err != nil {
				t.Error(err)
			}
		}()
		if _, err := f.materializer.PreparePostbuildRepair(f.ctx, request, output.ProviderResult, command); err == nil {
			t.Fatal("foreign path admitted")
		}
	})
	t.Run("foreign_head", func(t *testing.T) {
		runMaterializerGit(t, f.worktree, "commit", "--allow-empty", "-m", "foreign endpoint")
		defer runMaterializerGit(t, f.worktree, "reset", "--soft", verification.Checkpoint.CommitOID)
		if _, err := f.materializer.PreparePostbuildRepair(f.ctx, request, output.ProviderResult, command); err == nil {
			t.Fatal("foreign HEAD admitted")
		}
	})
	t.Run("foreign_worktree_same_head", func(t *testing.T) {
		foreign := filepath.Join(t.TempDir(), "foreign")
		runMaterializerGit(t, f.worktree, "worktree", "add", "-b", "postbuild-proof-foreign", foreign, verification.Checkpoint.CommitOID)
		identity, err := f.materializer.Git.Snapshot(f.ctx, foreign, "main")
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(identity)
		if err != nil {
			t.Fatal(err)
		}
		wrong := request
		wrong.Worktree.Path, wrong.Worktree.Branch, wrong.Worktree.IdentityJSON, wrong.Worktree.BaseSHA = identity.Worktree, identity.HeadRef, encoded, identity.BaseHead
		if _, err := f.materializer.PreparePostbuildRepair(f.ctx, wrong, output.ProviderResult, command); err == nil {
			t.Fatal("unregistered physical checkout substituted for failed Builder worktree")
		}
	})
	for _, name := range []string{"missing_command", "wrong_builder", "wrong_ticket", "stale_fence"} {
		t.Run(name, func(t *testing.T) {
			wrong, key, commandKey := request, output.ProviderResult, command
			switch name {
			case "missing_command":
				commandKey = contracts.RepositoryCommandResultKey{SemanticKey: "not-observed", ClaimEpoch: 1}
			case "wrong_builder":
				key.AttemptID++
			case "wrong_ticket":
				wrong.Ticket.Ref.Ticket = "SF-foreign"
			case "stale_fence":
				wrong.Fence.RunnerEpoch++
			}
			if _, err := f.materializer.PreparePostbuildRepair(f.ctx, wrong, key, commandKey); err == nil {
				t.Fatal("uncertain/mismatched evidence prepared")
			}
		})
	}
	t.Run("unresolved_effect", func(t *testing.T) {
		if _, err := f.db.PlanEffect(f.ctx, store.EffectPlan{SemanticKey: "postbuild-prepare/unresolved", Ref: f.ref, Kind: "repository_command", TicketVersion: ticket.Version, Fence: f.fence, RequestDigest: "sha256:" + strings.Repeat("9", 64)}); err != nil {
			t.Fatal(err)
		}
		if _, err := f.materializer.PreparePostbuildRepair(f.ctx, request, output.ProviderResult, command); err == nil {
			t.Fatal("unresolved command authority prepared for repair")
		}
	})
	if after, err := f.db.Ticket(f.ctx, f.ref); err != nil || after.State != domain.StateBuilding || after.Version != ticket.Version {
		t.Fatalf("preparation mutated ticket: %+v %v", after, err)
	}
	assertMaterializerProviderAttempts(t, f.db, f.ref, 3)
	if _, err := f.db.LatestCandidate(f.ctx, f.ref); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed command produced candidate: %v", err)
	}
}
