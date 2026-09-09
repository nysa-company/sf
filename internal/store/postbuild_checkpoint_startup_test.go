package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPostbuildCheckpointStartupDefersWithoutSettlingEffect(t *testing.T) {
	db, ctx, intent := postbuildCheckpointReclaimFixture(t)
	claim, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	before, err := db.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if err != nil {
		t.Fatal(err)
	}
	// No prepared tuple is necessary to defer: the accepted receipt may have
	// crossed the intent boundary but crashed before commit-tree or branch CAS.
	deferred, err := db.DeferPostbuildAmendmentCheckpointRecovery(ctx, claim)
	if err != nil || !deferred {
		t.Fatalf("deferred=%t err=%v", deferred, err)
	}
	after, err := db.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if err != nil || after.Claim != before.Claim || after.Effect.State != EffectUncertain || after.Effect.ObservedIdentity != "" || after.PreparedCommitOID != before.PreparedCommitOID {
		t.Fatalf("classification mutated effect: %v", err)
	}
	// A new daemon epoch must not make immutable receipt classification depend
	// on the live-phase reader's already-stale old leader fence.
	leader, err := db.AcquireLeader(ctx, intent.Ref.Channel, "checkpoint-startup-classifier")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileEffects(ctx, intent.Ref.Channel, leader); err != nil {
		t.Fatal(err)
	}
	recovered, err := db.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if err != nil || recovered.Claim != claim || recovered.Effect.LeaderEpoch != leader || recovered.Effect.ClaimEpoch <= claim.ClaimEpoch || recovered.Effect.State != EffectUncertain {
		t.Fatalf("effect recovery did not preserve immutable claim: %v", err)
	}
	if deferred, err := db.DeferPostbuildAmendmentCheckpointRecovery(ctx, claim); err != nil || !deferred {
		t.Fatalf("new-leader deferred=%t err=%v", deferred, err)
	}
	wrong := claim
	wrong.RequestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := db.DeferPostbuildAmendmentCheckpointRecovery(ctx, wrong); err == nil {
		t.Fatal("foreign claim accepted")
	}
}

func TestPostbuildCheckpointStartupRefusesDanglingReceiptAndTamperedCommand(t *testing.T) {
	for _, mode := range []string{"dangling receipt", "tampered command"} {
		t.Run(mode, func(t *testing.T) {
			db, ctx, intent := postbuildCheckpointReclaimFixture(t)
			claim, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := db.PostbuildAmendmentCheckpointSnapshot(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
			if err != nil {
				t.Fatal(err)
			}
			// Isolated fixture corruption beneath the immutable API. Pin the
			// connection so foreign-key settings cannot leak to another handle.
			conn, err := db.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			func() {
				defer conn.Close()
				if mode == "dangling receipt" {
					if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys=OFF`); err != nil {
						t.Fatal(err)
					}
					defer conn.ExecContext(ctx, `PRAGMA foreign_keys=ON`)
					if _, err := conn.ExecContext(ctx, `DROP TRIGGER postbuild_amendment_snapshots_immutable_delete`); err != nil {
						t.Fatal(err)
					}
					if _, err := conn.ExecContext(ctx, `DELETE FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket); err != nil {
						t.Fatal(err)
					}
					var remaining int
					if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_amendment_checkpoint_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&remaining); err != nil || remaining != 1 {
						t.Fatalf("receipt not retained: count=%d err=%v", remaining, err)
					}
				} else {
					if _, err := conn.ExecContext(ctx, `DROP TRIGGER repository_command_results_immutable_update`); err != nil {
						t.Fatal(err)
					}
					if _, err := conn.ExecContext(ctx, `UPDATE repository_command_results SET exit_code=255 WHERE semantic_key=? AND claim_epoch=?`, receipt.Command.SemanticKey, receipt.Command.ClaimEpoch); err != nil {
						t.Fatal(err)
					}
				}
			}()
			deferred, err := db.DeferPostbuildAmendmentCheckpointRecovery(ctx, claim)
			if deferred || !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("corruption became generic/deferred authority: deferred=%t err=%v", deferred, err)
			}
		})
	}
}
