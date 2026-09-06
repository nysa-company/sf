package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

// HistoricalReviewDiagnostic is authenticated historical evidence, not a
// current verdict, transition capability, or raw provider transcript.
type HistoricalReviewDiagnostic struct {
	Attempt       int
	TicketVersion uint64
	Review        phaseartifact.Reviewer
}

// LatestReviewDiagnostic never falls back past a newer incomplete/failed
// review. Selection and immutable result authentication share one snapshot.
func (s *Store) LatestReviewDiagnostic(ctx context.Context, ref domain.TicketRef) (HistoricalReviewDiagnostic, error) {
	var out HistoricalReviewDiagnostic
	if ref.Validate() != nil {
		return out, ErrProviderAttempt
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	key := ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseReview}
	var state, outcome string
	err = tx.QueryRowContext(ctx, `SELECT id,attempt,state,outcome FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND role='reviewer' ORDER BY id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, domain.PhaseReview).Scan(&key.AttemptID, &key.Attempt, &state, &outcome)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if state != "completed" || outcome != "completed" {
		return out, ErrNotFound
	}
	result, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, tx, key)
	if err != nil {
		return out, ErrProviderAttempt
	}
	if parsed.Reviewer == nil {
		return out, ErrProviderAttempt
	}
	out = HistoricalReviewDiagnostic{Attempt: key.Attempt, TicketVersion: result.Claim.ExpectedVersion, Review: *parsed.Reviewer}
	return out, tx.Commit()
}
