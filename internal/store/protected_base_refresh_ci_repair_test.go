package store

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProtectedBaseRefreshReservationPreservesCompletedCIRepairParent(t *testing.T) {
	for _, state := range []domain.State{domain.StateReviewing, domain.StateWaitingApproval} {
		t.Run(string(state), func(t *testing.T) {
			completed := newCompletedCandidateRepairTestFixture(t)
			f := completedCandidateRepairReviewingFixture(t, completed)
			defer f.db.Close()
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
			refreshClaim, err := f.db.IssueGitMutationClaim(f.ctx, reservation.Mutation)
			if err != nil {
				t.Fatalf("exact refresh claim rejected after reservation: %v", err)
			}
			lease, err := f.db.AcquireGitMutation(f.ctx, refreshClaim)
			if err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			bad := reservation.Mutation
			bad.ExpectedHeadOID = strings.Repeat("7", 40)
			if _, err := f.db.IssueGitMutationClaim(f.ctx, bad); err == nil {
				t.Fatal("retargeted predecessor accepted")
			}
			after, err := f.db.Ticket(f.ctx, current.Ref)
			if err != nil || after.Version != current.Version || after.State != state {
				t.Fatal("reservation changed lifecycle before completed refresh")
			}
		})
	}
}
