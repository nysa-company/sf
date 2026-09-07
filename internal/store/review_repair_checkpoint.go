package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/domain"
)

// ReviewRepairCheckpoint is historical candidate evidence authenticated at a
// current reviewer-owned repair endpoint. It is not Git mutation authority.
type ReviewRepairCheckpoint struct {
	ParentOID      string
	ProtectedPaths []string
}

func (s *Store) ReviewRepairVerificationCheckpoint(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (ReviewRepairCheckpoint, bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return ReviewRepairCheckpoint{}, false, ErrStaleFence
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ReviewRepairCheckpoint{}, false, err
	}
	defer tx.Rollback()
	var currentVersion, runner, leader uint64
	if err := tx.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentVersion, &runner, &leader); err != nil {
		return ReviewRepairCheckpoint{}, false, err
	}
	if currentVersion != version || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
		return ReviewRepairCheckpoint{}, false, ErrStaleFence
	}
	repair, found, err := s.reviewRepairVerificationAmendment(ctx, tx, ref, version, fence)
	if err != nil || !found {
		return ReviewRepairCheckpoint{}, false, err
	}
	candidate, err := s.latestCandidateFrom(ctx, tx, ref, false)
	if err != nil || candidate.Snapshot.HeadSHA != repair.ReviewedHead || candidate.Commit.CommitOID != repair.ReviewedHead || s.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, tx, ref, candidate) != nil {
		return ReviewRepairCheckpoint{}, false, ErrEvidenceConflict
	}
	_, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, tx, candidate.BuilderResult)
	if err != nil || parsed.Builder == nil {
		return ReviewRepairCheckpoint{}, false, ErrEvidenceConflict
	}
	return ReviewRepairCheckpoint{ParentOID: candidate.Snapshot.HeadSHA, ProtectedPaths: append([]string(nil), parsed.Builder.ChangedFiles...)}, true, nil
}
