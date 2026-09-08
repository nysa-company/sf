package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestLatestReviewDiagnosticAuthenticatesHistoricalResult(t *testing.T) {
	f := finalReviewLifecycleFixtureWithPending(t, 1)
	if _, err := f.db.LatestReviewDiagnostic(f.ctx, f.ticket.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty: %v", err)
	}
	claim := completeFinalReviewWith(t, f, phaseartifact.ReviewNeedsOperator, "operator")
	value, err := f.db.LatestReviewDiagnostic(f.ctx, f.ticket.Ref)
	if err != nil || value.Attempt != claim.Attempt || value.TicketVersion != claim.ExpectedVersion || value.Review.Decision != phaseartifact.ReviewNeedsOperator || len(value.Review.Findings) == 0 {
		t.Fatalf("diagnostic: %+v %v", value, err)
	}
	other := f.ticket.Ref
	other.Channel = domain.ChannelStable
	if _, err := f.db.LatestReviewDiagnostic(f.ctx, other); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign channel: %v", err)
	}
	if _, err := f.db.AcquireLeader(f.ctx, domain.ChannelDev, "diagnostic-only-replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.LatestReviewDiagnostic(f.ctx, f.ticket.Ref); err != nil {
		t.Fatalf("historical leader: %v", err)
	}
	if _, err := f.db.db.ExecContext(f.ctx, `UPDATE provider_attempts SET state='failed',outcome='invalid_artifact' WHERE id=?`, claim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.LatestReviewDiagnostic(f.ctx, f.ticket.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed latest: %v", err)
	}
	if _, err := f.db.db.ExecContext(f.ctx, `UPDATE provider_attempts SET state='completed',outcome='completed' WHERE id=?`, claim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(f.ctx, `DROP TRIGGER provider_attempt_results_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.db.ExecContext(f.ctx, `UPDATE provider_attempt_results SET typed_artifact='{}' WHERE provider_attempt_id=?`, claim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.LatestReviewDiagnostic(f.ctx, f.ticket.Ref); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("tampered result: %v", err)
	}
}
