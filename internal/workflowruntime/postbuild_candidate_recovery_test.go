package workflowruntime_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type lostPostbuildCandidateResponse struct {
	*store.Store
	err error
}

func (s lostPostbuildCandidateResponse) RecordCandidate(ctx context.Context, evidence store.CandidateEvidence) ([]store.InvalidationReceipt, error) {
	result, err := s.Store.RecordCandidate(ctx, evidence)
	if err != nil {
		return result, err
	}
	return result, s.err
}

func TestPostbuildAmendmentCandidateFinalizationRecovery(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("recorded_%t/restart_%t", recorded, restart), func(t *testing.T) {
				f := newMaterializerRealFixtureWithProvider(t, func(t *testing.T) string { return writeMaterializerAmendmentProvider(t, true) })
				f.worker.Engine = f.state.StateMachine
				for _, want := range []domain.State{domain.StateVerifying, domain.StateBuilding, domain.StateBuilding, domain.StateVerifying, domain.StateBuilding} {
					if got, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || got.State != want {
						t.Fatalf("setup=%+v want=%s err=%v", got, want, err)
					}
				}
				crash := errors.New("injected candidate finalization response loss")
				if recorded {
					f.worker.Evidence = lostPostbuildCandidateResponse{Store: f.db, err: crash}
				} else {
					faulted := f.materializer
					faulted.AfterCandidateCommit = func(workflowworker.CandidateWitness) error { return crash }
					f.worker.CandidateMaterializer = faulted
				}
				if _, err := f.worker.Run(f.ctx, f.ref, f.fence); !errors.Is(err, crash) {
					t.Fatalf("candidate response loss: %v", err)
				}
				assertMaterializerProviderAttempts(t, f.db, f.ref, 6)
				head := rawMaterializerGit(t, f.worktree, "rev-parse", "HEAD")
				if dirty := rawMaterializerGit(t, f.worktree, "status", "--porcelain=v1", "--untracked-files=all"); dirty != "" {
					t.Fatal("candidate was not fully committed")
				}
				observations, err := sql.Open("sqlite", "file:"+f.databasePath+"?mode=ro")
				if err != nil {
					t.Fatal(err)
				}
				defer observations.Close()
				counts := func() [2]int {
					var n [2]int
					for i, table := range []string{"repository_command_results", "effects"} {
						if err := observations.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE channel=? AND project_id=? AND ticket_id=?", f.ref.Channel, f.ref.Project, f.ref.Ticket).Scan(&n[i]); err != nil {
							t.Fatal(err)
						}
					}
					return n
				}
				before := counts()
				if restart {
					leader, err := f.db.AcquireLeader(f.ctx, domain.ChannelDev, "postbuild-candidate-finalization-restart")
					if err != nil {
						t.Fatal(err)
					}
					if err := f.db.SetRecoveryAuthority(f.ctx, domain.ChannelDev, leader, f.supervisor.PublicKey()); err != nil {
						t.Fatal(err)
					}
					if _, err := f.db.ReconcileEffects(f.ctx, domain.ChannelDev, leader); err != nil {
						t.Fatal(err)
					}
					if changed, err := f.db.FenceRecoveredRunners(f.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
						t.Fatalf("restart changed=%d err=%v", changed, err)
					}
					f.fence.LeaderEpoch = leader
				}
				pending, err := f.db.Ticket(f.ctx, f.ref)
				if err != nil || pending.State != domain.StateBuilding {
					t.Fatalf("retained Building: %v", err)
				}
				f.fence.RunnerEpoch = pending.RunnerEpoch
				coordinator := worktreecoord.Coordinator{Store: f.db, Git: f.materializer.Git}
				if _, err := coordinator.AuthenticatePostbuildVerificationAmendment(f.ctx, worktreecoord.EnsureRequest{Ref: f.ref, Version: pending.Version, Fence: f.fence}); err != nil {
					t.Fatalf("candidate finalization admission: %v", err)
				}
				f.worker.Evidence = f.db
				f.worker.CandidateMaterializer = f.materializer
				f.worker.Runner = nil // replay cannot fall back to another model call
				if got, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || got.State != domain.StatePublishing || !got.Replayed {
					t.Fatalf("candidate finalization=%+v err=%v", got, err)
				}
				assertMaterializerProviderAttempts(t, f.db, f.ref, 6)
				if after := counts(); after != before || rawMaterializerGit(t, f.worktree, "rev-parse", "HEAD") != head {
					t.Fatal("candidate recovery replaced commit or issued a command/effect")
				}
				candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
				if err != nil || candidate.Commit.CommitOID != head {
					t.Fatalf("final candidate: %v", err)
				}
			})
		}
	}
}
