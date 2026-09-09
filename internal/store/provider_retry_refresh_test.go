package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

func providerRetryRefreshFixture(t *testing.T) (*providerRetryWorktreeFixture, string) {
	t.Helper()
	// The callback variant records a real Store-owned creation intent before
	// registration; the lifecycle through publishing is otherwise unchanged.
	db, ctx, current, fence := publicationLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeGuarded, func(_ *Store, _ context.Context, ticket Ticket, _ domain.Fence) Ticket { return ticket })
	recordFixturePublication(t, db, ctx, current, fence)
	base := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: fence, RequestDigest: proof.ContextDigest}); err != nil {
		t.Fatal(err)
	}
	proofClaim, err := db.IssueGitMutationClaim(ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: proofClaim.SemanticKey, Ref: current.Ref, TicketVersion: proofClaim.TicketVersion, Fence: domain.Fence{LeaderEpoch: proofClaim.LeaderEpoch, RunnerEpoch: proofClaim.RunnerEpoch, ClaimEpoch: proofClaim.ClaimEpoch}}, proof.ObservedIdentity); err != nil {
		t.Fatal(err)
	}
	reservation, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, reservation.Mutation)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("8", 40), strings.Repeat("7", 40)
	if err := lease.(contracts.GitBaseRefreshPreparationLease).RecordBaseRefreshPreparation(ctx, commit, tree, [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: current.Ref, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, commit); err != nil {
		t.Fatal(err)
	}
	worktree, err := db.Worktree(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	var identity gitboundary.Identity
	if err := json.Unmarshal(worktree.IdentityJSON, &identity); err != nil {
		t.Fatal(err)
	}
	identity.BaseHead = base
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteProtectedBaseRefresh(ctx, claim, identityJSON); err != nil {
		t.Fatal(err)
	}
	current, err = db.Ticket(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err = db.Worktree(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	builder, reviewer := setupProviderPair(t, db, ctx)
	return &providerRetryWorktreeFixture{db: db, ctx: ctx, ticket: current, fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}, configDigest: current.ConfigDigest, builder: builder, reviewer: reviewer, worktree: worktree, base: base, candidateHead: claim.ExpectedHeadOID}, commit
}

func TestProviderRetryProtectedBaseRefreshLifecycle(t *testing.T) {
	providerRetryProtectedBaseRefreshLifecycle(t, "")
}

func TestProviderRetryProtectedBaseRefreshRejectsRecoveryLedgerTampering(t *testing.T) {
	for _, mode := range []string{"digest", "missing"} {
		t.Run(mode, func(t *testing.T) { providerRetryProtectedBaseRefreshLifecycle(t, mode) })
	}
}

func providerRetryProtectedBaseRefreshLifecycle(t *testing.T, tamper string) {
	f, head := providerRetryRefreshFixture(t)
	paused := f.exhaust(t, "invalid_artifact")
	proof, err := f.db.ProviderRetryWorktreeProof(f.ctx, paused.Ref, paused.Version, f.fence)
	if err != nil || proof.ExpectedHead != head || proof.ExpectedHead == f.candidateHead || !reflect.DeepEqual(proof.Worktree, f.worktree) {
		t.Fatalf("paused refresh proof=%+v err=%v", proof, err)
	}
	if err := f.db.SealRuntimeControl(f.ctx, paused.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.TransitionProviderRetry(f.ctx, Transition{Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateBuilding, ResumeState: domain.StateBuilding, Trigger: "operator_retry", Fence: f.fence}); err != nil {
		t.Fatal(err)
	}
	f.reload(t)
	for restart := 0; restart < 3; restart++ {
		if restart > 0 {
			if err := f.db.restoreRuntimeControls(f.ctx); err != nil {
				t.Fatal(err)
			}
			if restart == 2 && tamper != "" {
				trigger, query := "runner_recovery_ledger_immutable_update", `UPDATE runner_recovery_ledger SET recovery_digest='sha256:`+strings.Repeat("f", 64)+`' WHERE channel=? AND project_id=? AND ticket_id=?`
				if tamper == "missing" {
					trigger, query = "runner_recovery_ledger_immutable_delete", `DELETE FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`
				}
				if _, err := f.db.db.ExecContext(f.ctx, "DROP TRIGGER "+trigger); err != nil {
					t.Fatal(err)
				}
				result, err := f.db.db.ExecContext(f.ctx, query, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket)
				if err != nil {
					t.Fatal(err)
				}
				if n, err := result.RowsAffected(); err != nil || n != 1 {
					t.Fatalf("tamper ledger %s rows=%d err=%v", tamper, n, err)
				}
			}
			leader, err := f.db.AcquireLeader(f.ctx, paused.Ref.Channel, "refresh-provider-retry-restart")
			if err != nil {
				t.Fatal(err)
			}
			if restart == 2 && tamper != "" {
				if _, err := f.db.FenceRecoveredRunners(f.ctx, paused.Ref.Channel, leader); !errors.Is(err, ErrPublicationEvidence) {
					t.Fatalf("tampered %s ledger recovered: %v", tamper, err)
				}
				current, err := f.db.Ticket(f.ctx, paused.Ref)
				if err != nil || current.Version != f.ticket.Version || current.RunnerEpoch != f.ticket.RunnerEpoch {
					t.Fatalf("refused recovery changed ticket: %+v %v", current, err)
				}
				return
			}
			if changed, err := f.db.FenceRecoveredRunners(f.ctx, paused.Ref.Channel, leader); err != nil || changed != 1 {
				t.Fatalf("restart %d changed=%d err=%v", restart, changed, err)
			}
			f.fence.LeaderEpoch = leader
			f.reload(t)
		}
		proof, err := f.db.ProviderRetryWorktreeProof(f.ctx, paused.Ref, f.ticket.Version, f.fence)
		if err != nil || proof.ExpectedHead != head || !reflect.DeepEqual(proof.Worktree, f.worktree) {
			t.Fatalf("active refresh proof restart=%d proof=%+v err=%v", restart, proof, err)
		}
		if context, err := f.db.ProtectedBaseRefreshBuildContext(f.ctx, paused.Ref, f.ticket.Version, f.fence); err != nil || context.Completion.Preparation.CommitOID != head {
			t.Fatalf("refresh build context restart=%d err=%v", restart, err)
		}
		stopped, err := f.db.StoppedRuntimeTicket(f.ctx, paused.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if capability, err := f.db.ProviderRetryRearmProof(f.ctx, paused.Ref, stopped); err != nil || capability == nil {
			t.Fatalf("refresh rearm restart=%d err=%v", restart, err)
		}
	}
}

func TestProviderRetryProtectedBaseRefreshRejectsMalformedLineage(t *testing.T) {
	for _, mode := range []string{"registration", "event", "completion_digest", "missing_completion", "creation"} {
		t.Run(mode, func(t *testing.T) {
			f, _ := providerRetryRefreshFixture(t)
			paused := f.exhaust(t, "invalid_artifact")
			var query string
			switch mode {
			case "registration":
				query = `UPDATE worktrees SET head_sha='` + strings.Repeat("6", 40) + `' WHERE channel=? AND project_id=? AND ticket_id=?`
			case "event":
				query = `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='base_or_candidate_head_changed'`
			case "completion_digest":
				if _, err := f.db.db.ExecContext(f.ctx, `DROP TRIGGER protected_base_refresh_completions_immutable_update`); err != nil {
					t.Fatal(err)
				}
				query = `UPDATE protected_base_refresh_completions SET completion_digest='sha256:` + strings.Repeat("f", 64) + `' WHERE channel=? AND project_id=? AND ticket_id=?`
			case "missing_completion":
				if _, err := f.db.db.ExecContext(f.ctx, `DROP TRIGGER protected_base_refresh_completions_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				query = `DELETE FROM protected_base_refresh_completions WHERE channel=? AND project_id=? AND ticket_id=?`
			case "creation":
				query = `UPDATE effects SET observed_identity='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind='git/create-worktree'`
			}
			result, err := f.db.db.ExecContext(f.ctx, query, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("tamper %s rows=%d err=%v", mode, n, err)
			}
			if _, err := f.db.ProviderRetryWorktreeProof(f.ctx, paused.Ref, paused.Version, f.fence); !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("tamper %s proof=%v", mode, err)
			}
		})
	}
}
