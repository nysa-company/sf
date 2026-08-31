package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/domain"
)

// TransitionGuardedMergeObserved is the only Store boundary that records the
// guarded merging -> reconciling handoff. The state-machine guards select this
// transition, while this transaction durably proves that the exact current
// merge intent/effect, publication, and approval chain already exists.
func (s *Store) TransitionGuardedMergeObserved(ctx context.Context, transition Transition) (TransitionResult, error) {
	if !guardedMergeObservationTransition(transition) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if transition.EventPayload != "{}" {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		var mode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&mode); err != nil || mode != domain.MergeGuarded {
			return ErrEvidenceConflict
		}
		confirmed, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, transition.Ref)
		if err != nil || confirmed.version != version || confirmed.runner != runner || confirmed.leader != transition.Fence.LeaderEpoch {
			return ErrEvidenceConflict
		}
		return nil
	})
}
