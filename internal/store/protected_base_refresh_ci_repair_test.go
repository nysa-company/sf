package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

func TestProtectedBaseRefreshReservationPreservesCompletedCIRepairParent(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		state                         domain.State
		restartWaiting, restartReview bool
		pendingRestarts               int
	}{
		{"reviewing", domain.StateReviewing, false, false, 2},
		{"waiting_approval", domain.StateWaitingApproval, true, false, 2},
		{"reviewing_restart_then_fresh_pass", domain.StateWaitingApproval, false, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := tc.state
			completed := newCompletedCandidateRepairTestFixture(t)
			f := completedCandidateRepairReviewingFixtureWithRestart(t, completed, tc.restartWaiting)
			defer f.db.Close()
			if tc.restartReview {
				leader, err := f.db.AcquireLeader(f.ctx, f.ticket.Ref.Channel, "review-before-refresh")
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := f.db.FenceRecoveredRunners(f.ctx, f.ticket.Ref.Channel, leader); err != nil || changed != 1 {
					t.Fatalf("review restart: changed=%d err=%v", changed, err)
				}
				if err := f.db.RebindRecoveredPublishedCandidates(f.ctx, f.ticket.Ref.Channel, leader); err != nil {
					t.Fatal(err)
				}
				f.ticket, err = f.db.Ticket(f.ctx, f.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				f.fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: f.ticket.RunnerEpoch}
			}
			current := f.ticket
			if state == domain.StateWaitingApproval {
				completeFinalReview(t, f)
				if _, err := f.db.TransitionFinalReview(f.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReviewing, To: state, Trigger: "review_pass", Fence: f.fence, EventPayload: "{}"}); err != nil {
					t.Fatal(err)
				}
				var err error
				current, err = f.db.Ticket(f.ctx, current.Ref)
				if err != nil {
					t.Fatal(err)
				}
			}
			base := strings.Repeat("9", 40)
			proof, err := f.db.ProtectedBaseRefreshProofIntent(f.ctx, current.Ref, current.Version, f.fence, base)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.PlanEffect(f.ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: f.fence, RequestDigest: proof.ContextDigest}); err != nil {
				t.Fatal(err)
			}
			claim, err := f.db.IssueGitMutationClaim(f.ctx, proof.Intent)
			if err != nil {
				t.Fatal(err)
			}
			// Store-only fixture: independent Git tests cover exact-tip observation.
			if _, err := f.db.ConfirmEffect(f.ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: current.Ref, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, proof.ObservedIdentity); err != nil {
				t.Fatal(err)
			}
			reservation, err := f.db.ReserveProtectedBaseRefresh(f.ctx, current.Ref, current.Version, f.fence, base)
			if err != nil {
				t.Fatal(err)
			}
			pending, found, err := f.db.PendingProtectedBaseRefresh(f.ctx, current.Ref, current.Version, f.fence)
			if err != nil || !found || pending.ID != reservation.ID {
				t.Fatalf("reservation invalidated predecessor: found=%t err=%v", found, err)
			}
			currentFence := f.fence
			for restart := 0; restart < tc.pendingRestarts; restart++ {
				prior := current
				priorFence := currentFence
				leader, err := f.db.AcquireLeader(f.ctx, current.Ref.Channel, "pending-ci-repair-refresh")
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := f.db.FenceRecoveredRunners(f.ctx, current.Ref.Channel, leader); err != nil || changed != 1 {
					t.Fatalf("restart %d: changed=%d err=%v", restart, changed, err)
				}
				if err := f.db.RebindRecoveredPublishedCandidates(f.ctx, current.Ref.Channel, leader); err != nil {
					t.Fatal(err)
				}
				current, err = f.db.Ticket(f.ctx, current.Ref)
				if err != nil || current.Version != prior.Version+1 || current.RunnerEpoch != prior.RunnerEpoch+1 || current.State != state {
					t.Fatalf("restart did not preserve exact lifecycle: %v", err)
				}
				currentFence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
				if _, _, err := f.db.PendingProtectedBaseRefresh(f.ctx, current.Ref, prior.Version, priorFence); err == nil {
					t.Fatal("stale pre-recovery endpoint accepted")
				}
				pending, found, err = f.db.PendingProtectedBaseRefresh(f.ctx, current.Ref, current.Version, currentFence)
				if err != nil || !found || pending.ID != reservation.ID || pending.IntentDigest != reservation.IntentDigest || string(pending.IntentPayload) != string(reservation.IntentPayload) {
					t.Fatalf("restart %d invalidated immutable reservation: found=%t err=%v", restart, found, err)
				}
			}
			// Mirror the coordinator's no-launch planned-effect rebind before
			// issuing the first refresh mutation at the recovered endpoint.
			intent := pending.Mutation
			if _, err := f.db.PlanEffect(f.ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, TicketVersion: intent.TicketVersion, Fence: intent.Fence, Kind: "git/refresh-base", RequestDigest: intent.RequestDigest}); err != nil {
				t.Fatal(err)
			}
			refreshClaim, err := f.db.IssueGitMutationClaim(f.ctx, intent)
			if err != nil {
				t.Fatalf("exact refresh claim rejected after reservation: %v", err)
			}
			lease, err := f.db.AcquireGitMutation(f.ctx, refreshClaim)
			if err != nil {
				t.Fatal(err)
			}
			preparedHead := strings.Repeat("8", 40)
			if err := lease.(contracts.GitBaseRefreshPreparationLease).RecordBaseRefreshPreparation(f.ctx, preparedHead, strings.Repeat("7", 40), [2]string{refreshClaim.ExpectedHeadOID, refreshClaim.ExpectedBaseOID}); err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			bad := intent
			bad.ExpectedHeadOID = strings.Repeat("7", 40)
			if _, err := f.db.IssueGitMutationClaim(f.ctx, bad); err == nil {
				t.Fatal("retargeted predecessor accepted")
			}
			after, err := f.db.Ticket(f.ctx, current.Ref)
			if err != nil || after.Version != current.Version || after.State != state {
				t.Fatal("reservation changed lifecycle before completed refresh")
			}
			if _, err := f.db.ConfirmEffect(f.ctx, EffectFence{SemanticKey: refreshClaim.SemanticKey, Ref: current.Ref, TicketVersion: current.Version, Fence: domain.Fence{LeaderEpoch: currentFence.LeaderEpoch, RunnerEpoch: currentFence.RunnerEpoch, ClaimEpoch: refreshClaim.ClaimEpoch}}, preparedHead); err != nil {
				t.Fatal(err)
			}
			worktree, err := f.db.Worktree(f.ctx, current.Ref)
			if err != nil {
				t.Fatal(err)
			}
			var identity gitboundary.Identity
			if err := json.Unmarshal(worktree.IdentityJSON, &identity); err != nil {
				t.Fatal(err)
			}
			identity.BaseHead = base
			encoded, err := json.Marshal(identity)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := f.db.CompleteProtectedBaseRefresh(f.ctx, refreshClaim, encoded)
			if err != nil {
				t.Fatal(err)
			}
			if err := validateRunnerRecoveryAuthority(f.ctx, f.db.db, current.Ref, completion.Version, completion.Fence); err != nil {
				rows, queryErr := f.db.db.QueryContext(f.ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket)
				if queryErr != nil {
					t.Fatal(queryErr)
				}
				var steps [][6]uint64
				for rows.Next() {
					var step [6]uint64
					if err := rows.Scan(&step[0], &step[1], &step[2], &step[3], &step[4], &step[5]); err != nil {
						t.Fatal(err)
					}
					steps = append(steps, step)
				}
				rows.Close()
				for index, step := range steps {
					repair, repairErr := validateCandidateRepairRecoveryTarget(f.ctx, f.db.db, current.Ref, step[0], step[1], step[2])
					t.Logf("step %v repair-target=%t/%v refresh-target=%t", step, repair, repairErr, protectedBaseRefreshRecoveryTarget(f.ctx, f.db.db, current.Ref, step[0], step[1], step[2]))
					if index > 0 {
						p := steps[index-1]
						t.Logf("refresh-gap=%t", protectedBaseRefreshRecoveryGap(f.ctx, f.db.db, current.Ref, p[3], p[4], p[5], step[0], step[1], step[2]))
					}
				}
				t.Logf("final-refresh-gap=%t", protectedBaseRefreshRecoveryGap(f.ctx, f.db.db, current.Ref, current.Version, currentFence.RunnerEpoch, currentFence.LeaderEpoch, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch))
				t.Fatalf("completed refresh invalidated fresh Builder recovery authority: %v", err)
			}
			if !protectedBaseRefreshRecoveryGap(f.ctx, f.db.db, current.Ref, current.Version, currentFence.RunnerEpoch, currentFence.LeaderEpoch, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) {
				t.Fatal("exact refresh edge rejected")
			}
			if protectedBaseRefreshRecoveryGap(f.ctx, f.db.db, current.Ref, current.Version, currentFence.RunnerEpoch+1, currentFence.LeaderEpoch, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) || protectedBaseRefreshRecoveryGap(f.ctx, f.db.db, current.Ref, current.Version, currentFence.RunnerEpoch, currentFence.LeaderEpoch, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch+1) {
				t.Fatal("forged refresh endpoint accepted")
			}
			if tc.restartReview {
				value, _, err := protectedBaseRefreshForTicketAt(f.ctx, f.db.db, current.Ref)
				if err != nil {
					t.Fatal(err)
				}
				source := normalRecoveryEndpoint{version: f.ticket.Version, runner: f.fence.RunnerEpoch, leader: f.fence.LeaderEpoch}
				if err := protectedBaseRefreshPostCISourceToReservation(f.ctx, f.db.db, value, source); err != nil {
					t.Fatalf("review source to reservation: %v", err)
				}
				bad := source
				bad.leader++
				if protectedBaseRefreshPostCISourceToReservation(f.ctx, f.db.db, value, bad) == nil {
					t.Fatal("wrong reviewing source fence accepted")
				}
				if _, err := f.db.db.ExecContext(f.ctx, `UPDATE events SET payload='{"tampered":true}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass'`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
				if protectedBaseRefreshPostCISourceToReservation(f.ctx, f.db.db, value, source) == nil {
					t.Fatal("tampered review pass accepted")
				}
			}
		})
	}
}
