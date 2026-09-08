package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// A pending request is selected at a bounded historical endpoint. This helper
// never calls the live/global recovery validator; its source checks finish at
// the consumed Building fence strictly before the request being authenticated.
func postbuildPendingAmendmentAt(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64, fence domain.Fence) (VerificationAmendment, error) {
	var entry, leader, runner uint64
	err := q.QueryRowContext(ctx, `SELECT transition_ticket_version,consumed_leader_epoch,consumed_runner_epoch FROM verification_amendment_requests a WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version<=? AND NOT EXISTS (SELECT 1 FROM events e WHERE e.channel=a.channel AND e.project_id=a.project_id AND e.ticket_id=a.ticket_id AND e.ticket_version>a.transition_ticket_version AND e.ticket_version<=? AND e.trigger IN ('amendment_accepted','amendment_rejected')) ORDER BY transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, version, version).Scan(&entry, &leader, &runner)
	if errors.Is(err, sql.ErrNoRows) {
		return VerificationAmendment{}, ErrNotFound
	}
	if err != nil || entry == 0 || leader == 0 || runner == 0 {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	amendment, err := (&Store{}).loadVerificationAmendment(ctx, q, ref, entry, domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner})
	if err != nil {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	if _, _, err := loadPostbuildAmendmentBinding(ctx, q, amendment); err != nil {
		return VerificationAmendment{}, err
	}
	var pending int
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM verification_amendment_requests a WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version<=? AND NOT EXISTS (SELECT 1 FROM events e WHERE e.channel=a.channel AND e.project_id=a.project_id AND e.ticket_id=a.ticket_id AND e.ticket_version>a.transition_ticket_version AND e.ticket_version<=? AND e.trigger IN ('amendment_accepted','amendment_rejected'))`, ref.Channel, ref.Project, ref.Ticket, version, version).Scan(&pending) != nil || pending != 1 || postbuildRepairSignedSourcePrefix(ctx, q, ref, entry, amendment.Fence, version, fence) != nil {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	return amendment, nil
}

func postbuildAmendmentRecoveryPredecessor(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	if state != domain.StateVerifying {
		return 0, false, nil
	}
	var requestVersion, sourceLeader uint64
	err := q.QueryRowContext(ctx, `SELECT a.transition_ticket_version,a.consumed_leader_epoch FROM verification_amendment_requests a JOIN postbuild_amendment_snapshots p ON p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.amendment_transition_version=a.transition_ticket_version WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.transition_ticket_version<=? ORDER BY a.transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&requestVersion, &sourceLeader)
	if errors.Is(err, sql.ErrNoRows) {
		// Missing companions on an active postbuild request are corruption, not
		// permission to fall back to the old completed Reviewer baseline.
		var requests int
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM verification_amendment_requests a JOIN provider_phase_entries e ON e.channel=a.channel AND e.project_id=a.project_id AND e.ticket_id=a.ticket_id AND e.phase='build' AND e.entry_trigger='postbuild_repair' WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.transition_ticket_version<=? AND e.entry_ticket_version=(SELECT MAX(pe.entry_ticket_version) FROM provider_phase_entries pe WHERE pe.channel=a.channel AND pe.project_id=a.project_id AND pe.ticket_id=a.ticket_id AND pe.phase='build' AND pe.entry_ticket_version<=a.consumed_ticket_version)`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&requests) != nil || requests != 0 {
			return 0, true, ErrPublicationEvidence
		}
		return 0, false, nil
	}
	if err != nil {
		return 0, true, ErrPublicationEvidence
	}
	if version != requestVersion {
		step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version)
		if err != nil || !found {
			return 0, true, ErrPublicationEvidence
		}
		sourceLeader = step.LeaderEpoch
	}
	if sourceLeader == 0 || sourceLeader >= newLeader {
		return 0, true, ErrPublicationEvidence
	}
	if _, err := postbuildPendingAmendmentAt(ctx, q, ref, version, domain.Fence{LeaderEpoch: sourceLeader, RunnerEpoch: runner}); err != nil {
		return 0, true, ErrPublicationEvidence
	}
	if authenticatePostbuildRecoveryPrefix(ctx, q, ref, version, runner, sourceLeader) != nil {
		return 0, true, ErrPublicationEvidence
	}
	return sourceLeader, true, nil
}
