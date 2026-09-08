package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestReviewBlockedRecoveryRearmsExactSealedEndpoint(t *testing.T) {
	for _, scenario := range []int{0, 1, 2, 3} {
		replacement := scenario > 0
		name := "same-leader"
		if replacement {
			name = "replacement-leader"
		}
		if scenario == 2 {
			name = "reopen-after-committed-recover"
		}
		if scenario == 3 {
			name = "reopen-after-compensated-install"
		}
		t.Run(name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixtureWithPending(t, 1)
			db, ctx := fixture.db, fixture.ctx
			completeFinalReviewWith(t, fixture, phaseartifact.ReviewNeedsOperator, "operator")
			blocked, err := db.TransitionReviewNeedsOperator(ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateBlocked, ResumeState: domain.StateReviewing, Trigger: "typed_blocker", Fence: fixture.fence, EventPayload: `{"code":"review_needs_operator"}`})
			if err != nil {
				t.Fatal(err)
			}
			fence := fixture.fence
			if replacement {
				fence.LeaderEpoch, err = db.AcquireLeader(ctx, domain.ChannelDev, "review-rearm-replacement")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.FenceRecoveredRunners(ctx, domain.ChannelDev, fence.LeaderEpoch); err != nil {
					t.Fatal(err)
				}
			}
			if err = db.SealRuntimeControl(ctx, fixture.ticket.Ref); err != nil {
				t.Fatal(err)
			}
			stopped, err := db.StoppedRuntimeTicket(ctx, fixture.ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			result, err := db.Transition(ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StateReviewing, Trigger: "operator_recover", Fence: fence, EventPayload: `{"intent":"recover","operator":"fixture"}`})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == 2 {
				var path string
				if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				fence.LeaderEpoch, err = db.AcquireLeader(ctx, domain.ChannelDev, "review-rearm-after-commit")
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, fence.LeaderEpoch); err != nil || changed != 1 {
					t.Fatalf("fence after committed recover: %d %v", changed, err)
				}
				current, err := db.Ticket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				result.Version, fence.RunnerEpoch = current.Version, current.RunnerEpoch
				stopped, err = db.StoppedRuntimeTicket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
			}
			if pending, err := db.ReviewBlockedRecoveryPending(ctx, fixture.ticket.Ref); err != nil || !pending {
				t.Fatalf("lost-response discriminator: %v %v", pending, err)
			}
			var raw string
			if err := db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE ticket_id=? AND ticket_version=? AND trigger='operator_recover'`, fixture.ticket.Ref.Ticket, blocked.Version+1).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE events SET payload='{}' WHERE ticket_id=? AND ticket_version=? AND trigger='operator_recover'`, fixture.ticket.Ref.Ticket, blocked.Version+1); err != nil {
				t.Fatal(err)
			}
			if pending, err := db.ReviewBlockedRecoveryPending(ctx, fixture.ticket.Ref); err == nil || pending {
				t.Fatal("unbound recovery leader accepted")
			}
			if _, err := db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped); err == nil {
				t.Fatal("tampered bridge rearmed")
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE events SET payload=? WHERE ticket_id=? AND ticket_version=? AND trigger='operator_recover'`, raw, fixture.ticket.Ref.Ticket, blocked.Version+1); err != nil {
				t.Fatal(err)
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET stop_runner_epoch=stop_runner_epoch+1 WHERE ticket_id=?`, fixture.ticket.Ref.Ticket); err != nil {
				t.Fatal(err)
			}
			if _, err := db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped); err == nil {
				t.Fatal("changed stop tuple rearmed")
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET stop_runner_epoch=stop_runner_epoch-1 WHERE ticket_id=?`, fixture.ticket.Ref.Ticket); err != nil {
				t.Fatal(err)
			}
			if key, found, err := db.ReuseCurrentCompletedProviderAttempt(ctx, fixture.ticket.Ref, domain.PhaseReview, "reviewer", result.Version, fence); !errors.Is(err, ErrStaleFence) || found || key.AttemptID != 0 {
				t.Fatalf("old review reused at fresh endpoint: %+v %v %v", key, found, err)
			}
			capability, err := db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped)
			if err != nil || capability == nil {
				t.Fatalf("exact blocked review rearm: capability=%v err=%v", capability, err)
			}
			if ready, err := db.RuntimeAdmissionReady(ctx, fixture.ticket.Ref, result.Version, fence); err != nil || ready {
				t.Fatalf("proof must not open runtime: %v %v", ready, err)
			}
			installFailure := errors.New("runtime replacement not yet latched")
			if err := db.ActivateRearm(ctx, capability, func(*RuntimeAdmissionCapability) error { return installFailure }); !errors.Is(err, installFailure) {
				t.Fatalf("failed runtime installation: %v", err)
			}
			if scenario == 3 {
				var path string
				if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				fence.LeaderEpoch, err = db.AcquireLeader(ctx, domain.ChannelDev, "review-rearm-after-install-failure")
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, fence.LeaderEpoch); err != nil || changed != 1 {
					t.Fatalf("fence after compensated install: %d %v", changed, err)
				}
				current, err := db.Ticket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				result.Version, fence.RunnerEpoch = current.Version, current.RunnerEpoch
			}
			if pending, err := db.ReviewBlockedRecoveryPending(ctx, fixture.ticket.Ref); err != nil || !pending {
				t.Fatalf("compensated installation must remain recoverable: %v %v", pending, err)
			}
			if scenario == 3 {
				fence.LeaderEpoch, err = db.AcquireLeader(ctx, domain.ChannelDev, "review-rearm-second-recovery")
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, fence.LeaderEpoch); err != nil || changed != 1 {
					t.Fatalf("second recovery: %d %v", changed, err)
				}
				current, err := db.Ticket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				result.Version, fence.RunnerEpoch = current.Version, current.RunnerEpoch
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET authority_runner_epoch=authority_runner_epoch+1 WHERE ticket_id=?`, fixture.ticket.Ref.Ticket); err != nil {
				t.Fatal(err)
			}
			if pending, err := db.ReviewBlockedRecoveryPending(ctx, fixture.ticket.Ref); err == nil || pending {
				t.Fatal("unbound compensated authority accepted")
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET authority_runner_epoch=authority_runner_epoch-1 WHERE ticket_id=?`, fixture.ticket.Ref.Ticket); err != nil {
				t.Fatal(err)
			}
			capability, err = db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped)
			if err != nil {
				t.Fatalf("retry compensated installation: %v", err)
			}
			if err := db.ActivateRearm(ctx, capability, func(admission *RuntimeAdmissionCapability) error {
				ref, version, gotFence, ok := admission.ConsumeRuntimeAdmission()
				if !ok || ref != fixture.ticket.Ref || version != result.Version || gotFence != fence {
					t.Fatal("wrong admission")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if pending, err := db.ReviewBlockedRecoveryPending(ctx, fixture.ticket.Ref); err != nil || pending {
				t.Fatalf("armed recovery replayed: %v %v", pending, err)
			}
			if reused, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: result.Version, Fence: fence}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("consumed blocked review must request fresh review: key=%+v err=%v", reused.Key, err)
			}
			if err := db.openRuntimeAdmission(ctx, fixture.ticket.Ref, result.Version, fence); err != nil {
				t.Fatal(err)
			}
			fixture.db, fixture.fence = db, fence
			fixture.ticket, err = db.Ticket(ctx, fixture.ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			fresh := completeFinalReviewWith(t, fixture, phaseartifact.ReviewPass, "")
			if fresh.Attempt != 2 || fresh.ExpectedVersion != result.Version {
				t.Fatalf("fresh review reset attempt budget or used old endpoint: %+v", fresh)
			}
			if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: fresh.ID, Ref: fresh.Ref, Phase: fresh.Phase, Attempt: fresh.Attempt}, result.Version, fence); err != nil {
				t.Fatalf("fresh result current reader: %v", err)
			}
			wrongFence := fence
			wrongFence.LeaderEpoch++
			if _, _, err := db.LoadProviderAttemptResult(ctx, fresh, result.Version, wrongFence); !errors.Is(err, ErrStaleFence) {
				t.Fatalf("wrong leader accepted by result reader: %v", err)
			}
			if reused, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: result.Version, Fence: fence}); err != nil || reused.Key.AttemptID != fresh.ID {
				t.Fatalf("fresh review must remain reusable: key=%+v err=%v", reused.Key, err)
			}
			if scenario == 3 {
				var path string
				if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				fence.LeaderEpoch, err = db.AcquireLeader(ctx, domain.ChannelDev, "after-fresh-pass")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = db.FenceRecoveredRunners(ctx, domain.ChannelDev, fence.LeaderEpoch); err != nil {
					t.Fatal(err)
				}
				current, err := db.Ticket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				result.Version, fence.RunnerEpoch = current.Version, current.RunnerEpoch
				capability, err := db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.ActivateRearm(ctx, capability, func(c *RuntimeAdmissionCapability) error {
					_, _, _, ok := c.ConsumeRuntimeAdmission()
					if !ok {
						t.Fatal("missing admission")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.openRuntimeAdmission(ctx, fixture.ticket.Ref, result.Version, fence); err != nil {
					t.Fatal(err)
				}
			}
			waiting, err := db.TransitionFinalReview(ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: result.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fence, EventPayload: `{}`})
			if err != nil {
				t.Fatalf("fresh passing review must advance to approval: %v", err)
			}
			conn, err := db.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			endpoint, completionErr := db.reviewCompletionRecoveryEndpoint(ctx, conn, fixture.ticket.Ref, domain.StateWaitingApproval, waiting.Version)
			conn.Close()
			if completionErr != nil || endpoint.version != waiting.Version || endpoint.runner != fence.RunnerEpoch || endpoint.leader != fence.LeaderEpoch {
				t.Fatalf("completion reader: %+v %v", endpoint, completionErr)
			}
			if scenario == 3 {
				// Exercise the complete post-review merge/restart authority.
				if _, err := db.ApplyOperatorDecision(ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{Ref: fixture.ticket.Ref, ExpectedVersion: waiting.Version, Fence: fence, ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 701, Decision: "approved"}}); err != nil {
					t.Fatal(err)
				}
				merging, err := db.Ticket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				fixture.db = db
				bindTerminalMergeEffect(t, db, fixture, merging, fence, "recovered-review-merge")
				var path string
				if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				fence.LeaderEpoch, err = db.AcquireLeader(ctx, domain.ChannelDev, "after-confirmed-merge")
				if err != nil {
					t.Fatal(err)
				}
				if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, fence.LeaderEpoch); err != nil || changed != 1 {
					t.Fatalf("recover merged review: %d %v", changed, err)
				}
				current, err := db.Ticket(ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				fence.RunnerEpoch = current.RunnerEpoch
				if _, err := db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET stop_leader_epoch=stop_leader_epoch+1 WHERE ticket_id=?`, fixture.ticket.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
				if _, err := db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped); err == nil {
					t.Fatal("tampered review stop rearmed merge")
				}
				if _, err := db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET stop_leader_epoch=stop_leader_epoch-1 WHERE ticket_id=?`, fixture.ticket.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
				capability, err := db.PostPublicationRearmProof(ctx, fixture.ticket.Ref, stopped)
				if err != nil {
					t.Fatalf("rearm recovered merge: %v", err)
				}
				if err := db.ActivateRearm(ctx, capability, func(c *RuntimeAdmissionCapability) error {
					_, _, _, ok := c.ConsumeRuntimeAdmission()
					if !ok {
						t.Fatal("missing merge admission")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.openRuntimeAdmission(ctx, fixture.ticket.Ref, current.Version, fence); err != nil {
					t.Fatal(err)
				}
				// An old pass is insufficient without the immutable recovery bridge.
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER runner_recovery_ledger_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(ctx, `DELETE FROM runner_recovery_ledger WHERE ticket_id=? AND ticket_version=?`, fixture.ticket.Ref.Ticket, result.Version); err != nil {
					t.Fatal(err)
				}
				conn, err := db.db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.reviewCompletionRecoveryEndpoint(ctx, conn, fixture.ticket.Ref, domain.StateWaitingApproval, waiting.Version)
				conn.Close()
				if err == nil {
					t.Fatal("recovered review accepted without signed bridge")
				}
			}
		})
	}
}
