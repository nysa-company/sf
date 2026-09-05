package worktreecoord

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

func uncertainCreationFixture(t *testing.T, id string) (coordinatorFixture, contracts.GitMutationClaim) {
	t.Helper()
	f := setupCoordinator(t, id)
	ctx := context.Background()
	path, err := f.db.TicketWorktreePath(f.ref)
	if err != nil {
		t.Fatal(err)
	}
	branch, err := (git.Allocator{Authority: f.db}).Allocate(ctx, f.ref.Channel, f.ref.Project, f.ref.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	repository, base, err := f.runner.ObserveRepositoryBase(ctx, f.project.Path, f.project.BaseRef)
	if err != nil {
		t.Fatal(err)
	}
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: f.ref, TicketVersion: f.request.Version, Fence: f.request.Fence}, RequestDigest: ensureDigest(f.ref, repository, path, branch, f.project.BaseRef, base), Repository: repository, Worktree: path, Branch: branch, Operation: "create-worktree", BaseRef: f.project.BaseRef, ExpectedBaseOID: base, ExpectedHeadOID: base}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if _, err := f.db.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: f.ref, Kind: "git/create-worktree", TicketVersion: f.request.Version, Fence: f.request.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.MarkEffectUncertain(ctx, store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	if err := ensureExactParent(path); err != nil {
		t.Fatal(err)
	}
	return f, claim
}

func TestCreationAbsenceCrashRecoveryPreservesUncertaintyAndRetries(t *testing.T) {
	f, claim := uncertainCreationFixture(t, "SF-absence-crash")
	ctx := context.Background()
	handle, err := f.db.BeginWorktreeCreationAbsence(ctx, claim, effectFence(claim))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.AcquireGitMutation(ctx, claim); err == nil {
		t.Fatal("revoked original claim acquired a mutation lease")
	}
	newLeader, err := f.db.AcquireLeader(ctx, domain.ChannelDev, "after-observation-crash")
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.CompleteAbsent(ctx); err == nil {
		t.Fatal("old leader settled absence")
	}
	// Startup must not request a process drainer for an unlaunchable observer.
	if err := f.db.RecoverGitMutationLeases(ctx, domain.ChannelDev, newLeader, nil); err != nil {
		t.Fatal(err)
	}
	if leases, err := f.db.ActiveGitMutationLeases(ctx, domain.ChannelDev); err != nil || len(leases) != 0 {
		t.Fatalf("observation lease residue=%+v err=%v", leases, err)
	}
	if effect, err := f.db.Effect(ctx, claim.SemanticKey); err != nil || effect.State != store.EffectUncertain {
		t.Fatalf("crash invented absence: %+v %v", effect, err)
	}
	if _, err := f.db.ReconcileEffects(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
		t.Fatal(err)
	}
	current, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	f.request = EnsureRequest{Ref: f.ref, Version: current.Version, Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: current.RunnerEpoch}}
	if _, err := coordinatorFor(f).Ensure(ctx, f.request); !errors.Is(err, ErrInProgress) {
		t.Fatalf("recovered absence: %v", err)
	}
	if _, err := coordinatorFor(f).Ensure(ctx, f.request); err != nil {
		t.Fatalf("fresh creation: %v", err)
	}
	if _, err := coordinatorFor(f).Ensure(ctx, f.request); err != nil {
		t.Fatalf("registered replay: %v", err)
	}
}

func TestEnsureCreationAbsenceSettlesAndFreshEnsureCreates(t *testing.T) {
	f, claim := uncertainCreationFixture(t, "SF-absence-settle")
	ctx := context.Background()
	facts, err := f.db.WorktreeCreationIntent(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := f.db.BeginWorktreeCreationAbsence(ctx, claim, store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: facts.Effect.ClaimEpoch}})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.runner.ObserveWorktreeCreationAbsent(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := handle.CompleteAbsent(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.AcquireGitMutation(ctx, claim); !errors.Is(err, store.ErrGitMutationIntent) {
		t.Fatalf("old claim accepted after absence settlement: %v", err)
	}
	if _, err := coordinatorFor(f).Ensure(ctx, f.request); err != nil {
		t.Fatalf("fresh Ensure after proven absence: %v", err)
	}
	if _, err := f.db.Worktree(ctx, f.ref); err != nil {
		t.Fatalf("fresh Ensure did not register worktree: %v", err)
	}
}

func TestObserveWorktreeCreationAbsentRefusesForeignArtifacts(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(*testing.T, coordinatorFixture, contracts.GitMutationClaim, string)
	}{
		{name: "branch", make: func(t *testing.T, f coordinatorFixture, claim contracts.GitMutationClaim, _ string) {
			mustGit(t, f.project.Path, "branch", "--", claim.Branch)
		}},
		{name: "private-ref", make: func(t *testing.T, f coordinatorFixture, claim contracts.GitMutationClaim, _ string) {
			sum := sha256.Sum256([]byte(claim.Branch))
			mustGit(t, f.project.Path, "update-ref", fmt.Sprintf("refs/sf/worktree-base/%x", sum), claim.ExpectedBaseOID)
		}},
		{name: "path", make: func(t *testing.T, _ coordinatorFixture, _ contracts.GitMutationClaim, path string) {
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "detached-admin", make: func(t *testing.T, f coordinatorFixture, claim contracts.GitMutationClaim, path string) {
			mustGit(t, f.project.Path, "worktree", "add", "--detach", path, claim.ExpectedBaseOID)
			// Only this disposable fixture path is removed: Git's administrative
			// registration deliberately survives and must forbid absence.
			if err := os.RemoveAll(path); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", make: func(t *testing.T, f coordinatorFixture, _ contracts.GitMutationClaim, path string) {
			if err := os.Symlink(f.project.Path, path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, claim := uncertainCreationFixture(t, "SF-absence-foreign-"+tc.name)
			path, err := f.db.TicketWorktreePath(f.ref)
			if err != nil {
				t.Fatal(err)
			}
			tc.make(t, f, claim, path)
			facts, err := f.db.WorktreeCreationIntent(context.Background(), f.ref)
			if err != nil {
				t.Fatal(err)
			}
			handle, err := f.db.BeginWorktreeCreationAbsence(context.Background(), claim, store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: facts.Effect.ClaimEpoch}})
			if err != nil {
				t.Fatal(err)
			}
			if err := f.runner.ObserveWorktreeCreationAbsent(context.Background(), claim); err == nil {
				t.Fatal("foreign artifact absence proof unexpectedly succeeded")
			}
			if err := handle.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreationAbsenceCannotDisplaceExistingWriter(t *testing.T) {
	f, original := uncertainCreationFixture(t, "SF-absence-writer")
	ctx := context.Background()
	if _, err := f.db.ObserveEffect(ctx, store.EffectObservation{EffectFence: effectFence(original), Present: false}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.db.IssueGitMutationClaim(ctx, store.GitMutationIntent{EffectFence: effectFence(original), RequestDigest: original.RequestDigest, Repository: original.Repository, Worktree: original.Worktree, Branch: original.Branch, Operation: original.Operation, BaseRef: original.BaseRef, ExpectedBaseOID: original.ExpectedBaseOID, ExpectedHeadOID: original.ExpectedHeadOID})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, err := f.db.MarkEffectUncertain(ctx, effectFence(claim)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.BeginWorktreeCreationAbsence(ctx, claim, effectFence(claim)); err == nil {
		t.Fatal("observation displaced existing writer")
	}
	effect, err := f.db.Effect(ctx, claim.SemanticKey)
	if err != nil || effect.State != store.EffectUncertain || effect.ClaimEpoch != claim.ClaimEpoch {
		t.Fatalf("failed observation changed epoch: %+v %v", effect, err)
	}
}
