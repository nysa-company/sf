package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestTransitionPostbuildRepairExactReplayConsumesBudgetOnce(t *testing.T) {
	db, ctx, failure := postbuildFailureFixture(t, 1)
	request := PostbuildRepairRequest{PostbuildFailureRequest: failure, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)}
	verification, err := db.CurrentVerification(ctx, failure.Ref)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := db.TransitionPostbuildRepair(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if transition.Version != failure.ExpectedVersion+1 || transition.EventID <= 0 {
		t.Fatal("missing exact semantic entry")
	}
	value, err := loadPostbuildRepairEntry(ctx, db.db, failure.Ref, transition.Version)
	if err != nil {
		t.Fatal(err)
	}
	if value.PostbuildFailureRequest != failure || value.VerificationRevision != verification.Revision.Revision || value.OriginalCheckpointOID != verification.Checkpoint.CommitOID || value.Verification.ProviderResult != verification.ProviderResult || value.EventID != transition.EventID {
		t.Fatal("retained source changed")
	}
	replay, err := db.TransitionPostbuildRepair(ctx, request)
	if err != nil || replay != transition {
		t.Fatalf("exact replay=%+v err=%v", replay, err)
	}
	var uses, entries, used int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, failure.Ref.Channel, failure.Ref.Project, failure.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_repair_entries WHERE channel=? AND project_id=? AND ticket_id=?`, failure.Ref.Channel, failure.Ref.Project, failure.Ref.Ticket).Scan(&entries); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, failure.Ref.Channel, failure.Ref.Project, failure.Ref.Ticket).Scan(&used); err != nil {
		t.Fatal(err)
	}
	if uses != 1 || entries != 1 || used != 1 {
		t.Fatalf("uses=%d entries=%d used=%d", uses, entries, used)
	}
	ticket, err := db.Ticket(ctx, failure.Ref)
	if err != nil || ticket.State != domain.StateBuilding || ticket.Version != transition.Version || ticket.RunnerEpoch != failure.Fence.RunnerEpoch {
		t.Fatalf("ticket=%+v err=%v", ticket, err)
	}
	// A plausible but different snapshot must not turn the exact command's
	// unique binding into a second correction or an observed success.
	wrong := request
	wrong.RetainedWorktreeDigest = "sha256:" + strings.Repeat("b", 64)
	if _, err := db.TransitionPostbuildRepair(ctx, wrong); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("mismatched retained digest replay=%v", err)
	}
	wrong = request
	wrong.ExpectedVersion++
	if _, err := db.TransitionPostbuildRepair(ctx, wrong); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("same command rebound to new consumed version=%v", err)
	}
	if _, err := db.AcquireLeader(ctx, failure.Ref.Channel, "postbuild-repair-replay-new-leader"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPostbuildRepair(ctx, request); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old active endpoint accepted after leader replacement: %v", err)
	}
}

func TestTransitionPostbuildRepairRejectsMalformedAndStaleRequests(t *testing.T) {
	for _, name := range []string{"digest", "version", "leader", "runner", "claim_epoch"} {
		t.Run(name, func(t *testing.T) {
			db, ctx, failure := postbuildFailureFixture(t, 1)
			request := PostbuildRepairRequest{PostbuildFailureRequest: failure, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)}
			want := ErrStaleFence
			switch name {
			case "digest":
				request.RetainedWorktreeDigest = "untrusted"
				want = ErrEvidenceConflict
			case "version":
				request.ExpectedVersion++
			case "leader":
				request.Fence.LeaderEpoch++
			case "runner":
				request.Fence.RunnerEpoch++
			case "claim_epoch":
				request.Fence.ClaimEpoch = 1
				want = ErrEvidenceConflict
			}
			if _, err := db.TransitionPostbuildRepair(ctx, request); !errors.Is(err, want) {
				t.Fatalf("got %v want %v", err, want)
			}
			var entries, uses int
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_repair_entries`).Scan(&entries); err != nil {
				t.Fatal(err)
			}
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE kind='correction'`).Scan(&uses); err != nil {
				t.Fatal(err)
			}
			if entries != 0 || uses != 0 {
				t.Fatalf("refusal wrote entries=%d budget=%d", entries, uses)
			}
		})
	}
}
