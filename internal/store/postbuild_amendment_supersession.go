package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// Historical amendment rows never imply absence. Only a fully authenticated
// later CI correction or completed protected-base refresh owns this handoff.
func (s *Store) postbuildAmendmentSupersededAt(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64, fence domain.Fence, live bool) (bool, error) {
	if live {
		var future int
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND amendment_transition_version>?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&future) != nil || future != 0 {
			return false, ErrEvidenceConflict
		}
	}
	var amendmentVersion, leader, runner uint64
	err := q.QueryRowContext(ctx, `SELECT amendment_transition_version,consumed_leader_epoch,consumed_runner_epoch FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND amendment_transition_version<=? ORDER BY amendment_transition_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&amendmentVersion, &leader, &runner)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || amendmentVersion > version {
		return false, ErrEvidenceConflict
	}
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, version)
	if err != nil {
		return false, ErrEvidenceConflict
	}
	var refreshes int
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM protected_base_refresh_intents WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, amendmentVersion, version).Scan(&refreshes) != nil {
		return false, ErrEvidenceConflict
	}
	if entry.Trigger != "checks_red" && refreshes == 0 {
		return false, nil
	}
	if _, err := s.loadVerificationAmendment(ctx, q, ref, amendmentVersion, domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner}); err != nil {
		return false, ErrEvidenceConflict
	}
	if refreshes != 0 {
		if !live {
			_, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
			if err != nil || refreshes != 1 || completion.Version <= amendmentVersion || completion.Version > version || postbuildRepairSignedSourcePrefix(ctx, q, ref, completion.Version, completion.Fence, version, fence) != nil {
				return false, ErrEvidenceConflict
			}
			return true, nil
		}
		refresh, err := s.protectedBaseRefreshBuildContextAt(ctx, q, ref, version, fence)
		if err != nil || refreshes != 1 || refresh.Completion.Version <= amendmentVersion {
			return false, ErrEvidenceConflict
		}
		return true, nil
	}
	authority, err := s.candidateRepairBuildAuthorityAtMode(ctx, q, ref, version, fence, !live)
	if err != nil || authority.context.EntryTicketVersion != entry.Version || entry.Version <= amendmentVersion {
		return false, ErrEvidenceConflict
	}
	return true, nil
}
