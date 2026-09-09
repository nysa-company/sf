package store

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPostbuildRepairRetainsVerificationAcrossTwoRecoveries(t *testing.T) {
	db, ctx, failure := postbuildFailureFixture(t, 1)
	before, err := db.CurrentVerification(ctx, failure.Ref)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := db.TransitionPostbuildRepair(ctx, PostbuildRepairRequest{PostbuildFailureRequest: failure, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	version, fence := transition.Version, failure.Fence
	assertRetained := func() {
		t.Helper()
		verification, err := db.CurrentVerification(ctx, failure.Ref)
		if err != nil || verification.TicketVersion != version || verification.Fence != fence || verification.ProviderResult != before.ProviderResult || verification.Checkpoint != before.Checkpoint || verification.Revision.Revision != before.Revision.Revision || !bytes.Equal(verification.Intent, before.Intent) || !bytes.Equal(verification.Proof, before.Proof) || !equalStringSlices(verification.Revision.OwnedFiles, before.Revision.OwnedFiles) {
			t.Fatalf("frozen verification lost at version %d: %+v err=%v", version, verification, err)
		}
		initial, err := db.PostbuildRepairBuildContext(ctx, failure.Ref, version, fence)
		if err != nil || initial.Repair.EntryVersion != transition.Version || initial.Builder.Claim.ID != failure.BuilderResult.AttemptID || initial.FailedCommand.Key != failure.CommandResult || initial.FailedCommand.Result.ExitCode != 1 || initial.Verification.ProviderResult != before.ProviderResult {
			t.Fatalf("initial context=%+v err=%v", initial, err)
		}
		logical, err := db.PostbuildRepairContext(ctx, failure.Ref, version, fence)
		if err != nil || logical.Repair.BindingDigest != initial.Repair.BindingDigest {
			t.Fatalf("logical context: %v", err)
		}
		if _, err := db.PostbuildRepairCompletedBuildContext(ctx, failure.Ref, version, fence); !errors.Is(err, ErrNotFound) {
			t.Fatalf("absent fresh Builder=%v", err)
		}
		if _, err := db.PostbuildRepairPendingAmendment(ctx, failure.Ref, version, fence); !errors.Is(err, ErrNotFound) {
			t.Fatalf("absent fresh amendment Builder=%v", err)
		}
		if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: failure.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: version, Fence: fence}); err == nil {
			t.Fatal("consumed completed Builder reused across fresh entry")
		}
	}
	assertRetained()
	for restart := 1; restart <= 2; restart++ {
		leader, err := db.AcquireLeader(ctx, failure.Ref.Channel, fmt.Sprintf("postbuild-recovery-%d", restart))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.CurrentVerification(ctx, failure.Ref); err == nil {
			t.Fatal("leader-only takeover made old proof current")
		}
		if _, err := db.PostbuildRepairContext(ctx, failure.Ref, version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch}); err == nil {
			t.Fatal("unrecorded takeover admitted logical context")
		}
		if changed, err := db.FenceRecoveredRunners(ctx, failure.Ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("recovery %d changed=%d err=%v", restart, changed, err)
		}
		version++
		fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch + 1}
		assertRetained()
	}
}

func TestPostbuildRepairRecoveryRejectsTamperedBoundaryAndUnwitnessedGap(t *testing.T) {
	for _, name := range []string{"binding", "event", "missing_binding", "counter_gap"} {
		t.Run(name, func(t *testing.T) {
			db, ctx, failure := postbuildFailureFixture(t, 1)
			transition, err := db.TransitionPostbuildRepair(ctx, PostbuildRepairRequest{PostbuildFailureRequest: failure, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)})
			if err != nil {
				t.Fatal(err)
			}
			version, fence := transition.Version, failure.Fence
			// Deliberate below-API disk corruption is negative evidence only.
			switch name {
			case "binding":
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER postbuild_repair_entries_immutable_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(ctx, `UPDATE postbuild_repair_entries SET retained_worktree_digest=?`, "sha256:"+strings.Repeat("b", 64)); err != nil {
					t.Fatal(err)
				}
			case "event":
				if _, err := db.db.ExecContext(ctx, `UPDATE events SET payload='{"forged":true}' WHERE id=?`, transition.EventID); err != nil {
					t.Fatal(err)
				}
			case "missing_binding":
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER postbuild_repair_entries_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(ctx, `DELETE FROM postbuild_repair_entries`); err != nil {
					t.Fatal(err)
				}
			case "counter_gap":
				if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET version=version+1,runner_epoch=runner_epoch+1 WHERE channel=? AND project_id=? AND id=?`, failure.Ref.Channel, failure.Ref.Project, failure.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
				version++
				fence.RunnerEpoch++
			}
			if _, err := db.CurrentVerification(ctx, failure.Ref); err == nil {
				t.Fatal("tampering retained current verification")
			}
			if _, err := db.PostbuildRepairContext(ctx, failure.Ref, version, fence); err == nil || errors.Is(err, ErrNotFound) {
				t.Fatalf("malformed existing entry hidden: %v", err)
			}
			leader, err := db.AcquireLeader(ctx, failure.Ref.Channel, "postbuild-tamper-recovery")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.FenceRecoveredRunners(ctx, failure.Ref.Channel, leader); err == nil {
				t.Fatal("tampering admitted startup recovery")
			}
			var rows int
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, failure.Ref.Channel, failure.Ref.Project, failure.Ref.Ticket).Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("rejected recovery appended rows=%d err=%v", rows, err)
			}
		})
	}
}
