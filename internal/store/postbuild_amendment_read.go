package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// PostbuildVerificationAmendmentContext preserves the logical request binding
// across the existing amendment lifecycle. It grants neither dirty-checkout
// admission nor restoration/commit permission; those require physical witnesses.
func (s *Store) PostbuildVerificationAmendmentContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildVerificationAmendmentContext, error) {
	var value PostbuildVerificationAmendmentContext
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return value, ErrEvidenceConflict
	}
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, ref, version, fence); err != nil {
			return err
		}
		var state domain.State
		if conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state) != nil {
			return ErrEvidenceConflict
		}
		switch state {
		case domain.StateVerifying:
			amendment, err := s.loadPendingVerificationAmendmentAtFence(ctx, conn, ref, version, fence)
			if err != nil {
				return err
			}
			value.Amendment = amendment
		case domain.StateBuilding:
			boundary, err := loadVerificationAmendmentBoundary(ctx, conn, ref, version, fence)
			if err != nil {
				return err
			}
			value.Amendment, value.Decision, value.Reviewer = boundary.Amendment, boundary.Decision, boundary.Reviewer
			current, err := s.verificationEvidenceForIdentityFrom(ctx, conn, ref, "", "", "")
			if err != nil {
				return ErrEvidenceConflict
			}
			value.CurrentVerification = current
		default:
			return ErrNotFound
		}
		binding, repair, err := loadPostbuildAmendmentBinding(ctx, conn, value.Amendment)
		if err != nil {
			return err
		}
		if err := assertNoVerificationAmendmentDownstream(ctx, conn, ref); err != nil {
			return err
		}
		var future int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND amendment_transition_version>?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&future) != nil || future != 0 {
			return ErrEvidenceConflict
		}
		builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, binding.BuilderResult)
		if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest == nil {
			return ErrEvidenceConflict
		}
		var worktree StoredWorktree
		if conn.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha,head_sha,ticket_version,leader_epoch,runner_epoch FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA, &worktree.HeadSHA, &worktree.TicketVersion, &worktree.Fence.LeaderEpoch, &worktree.Fence.RunnerEpoch) != nil || worktree.State != "registered" || worktree.Path != builder.Claim.Worktree || string(worktree.IdentityJSON) != builder.Claim.WorktreeIdentity || worktree.BaseSHA != builder.Claim.BaseSHA || !validOID(worktree.HeadSHA) {
			return ErrEvidenceConflict
		}
		plan, err := s.planFrom(ctx, conn, ref)
		if err != nil {
			return ErrEvidenceConflict
		}
		value.Binding, value.Repair, value.Snapshot = binding, repair, binding.Snapshot
		value.Worktree, value.Plan, value.Verification = worktree, plan, repair.Verification
		value.Builder, value.BuilderArtifact = builder, *parsed.Builder
		return nil
	})
	if err != nil {
		// An existing companion must never disappear behind a generic missing
		// amendment/decision result: that would select an ordinary pristine lane.
		if errors.Is(err, ErrNotFound) {
			var count int
			if s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND amendment_transition_version<=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&count) != nil {
				return PostbuildVerificationAmendmentContext{}, ErrEvidenceConflict
			}
			if count != 0 {
				return PostbuildVerificationAmendmentContext{}, ErrEvidenceConflict
			}
		}
		return PostbuildVerificationAmendmentContext{}, err
	}
	return value, nil
}
