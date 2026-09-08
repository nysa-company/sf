package workflowruntime_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

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
