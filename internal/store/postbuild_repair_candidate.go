package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/nysa-company/sf/internal/domain"
)

// PostbuildRepairPreparedCandidateWitness authenticates only the first candidate
// of the current direct repair entry. It shares the immutable command/commit
// validator with accepted amendments, not their lifecycle authority.
func (s *Store) PostbuildRepairPreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, builderKey ProviderAttemptResultKey) (PostbuildAmendmentPreparedCandidateWitness, bool, error) {
	var empty PostbuildAmendmentPreparedCandidateWitness
	value, err := s.PostbuildRepairContext(ctx, ref, version, fence)
	if errors.Is(err, ErrNotFound) {
		return empty, false, nil
	}
	if err != nil {
		return empty, false, err
	}
	if value.Candidate != nil {
		return empty, false, ErrEvidenceConflict
	}
	return s.postbuildPreparedCandidateWitness(ctx, ref, version, fence, builderKey, func(q *sql.Conn) (postbuildCandidateSource, error) {
		if superseded, err := postbuildRepairSupersededAt(ctx, q, ref, version, fence, true); err != nil || superseded {
			return postbuildCandidateSource{}, ErrEvidenceConflict
		}
		repair, err := postbuildRepairAtFence(ctx, q, ref, version, fence)
		if err != nil || !reflect.DeepEqual(repair, value.Repair) {
			return postbuildCandidateSource{}, ErrEvidenceConflict
		}
		verification, err := s.currentVerificationFrom(ctx, q, ref)
		if err != nil || !reflect.DeepEqual(verification, value.Verification) || verification.ProviderResult != repair.Verification.ProviderResult || verification.Checkpoint != repair.Verification.Checkpoint {
			return postbuildCandidateSource{}, ErrEvidenceConflict
		}
		return postbuildCandidateSource{EntryVersion: repair.EntryVersion, Verification: verification, Worktree: value.Worktree, Plan: value.Plan}, nil
	})
}

// Only exact later CI/refresh authority can retire an old direct repair. In
// particular a historical repair alone cannot hide a malformed current lane.
func postbuildRepairCandidateSupersededAt(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64, fence domain.Fence, repair PostbuildRepair, live bool) (bool, error) {
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, version)
	if err != nil {
		return false, ErrEvidenceConflict
	}
	var refreshes int
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM protected_base_refresh_intents WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, repair.EntryVersion, version).Scan(&refreshes) != nil {
		return false, ErrEvidenceConflict
	}
	if refreshes != 0 {
		if !live {
			_, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
			if err != nil || refreshes != 1 || completion.Version <= repair.EntryVersion || completion.Version > version || postbuildRepairSignedSourcePrefix(ctx, q, ref, completion.Version, completion.Fence, version, fence) != nil {
				return false, ErrEvidenceConflict
			}
			return true, nil
		}
		refresh, err := (&Store{}).protectedBaseRefreshBuildContextAt(ctx, q, ref, version, fence)
		if err != nil || refreshes != 1 || refresh.Completion.Version <= repair.EntryVersion {
			return false, ErrEvidenceConflict
		}
		return true, nil
	}
	if entry.Trigger != "checks_red" {
		return false, nil
	}
	authority, err := (&Store{}).candidateRepairBuildAuthorityAtMode(ctx, q, ref, version, fence, !live)
	if err != nil || authority.context.EntryTicketVersion != entry.Version || entry.Version <= repair.EntryVersion {
		return false, ErrEvidenceConflict
	}
	return true, nil
}
