package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

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
		// An open runtime admission continues through the authenticated
		// merging->reconciling handoff. Advance its durable authority in this
		// same transaction so a crash after the state transition can seal,
		// fence, and rearm from the exact reconciling endpoint rather than a
		// stale pre-transition tuple.
		control, controlErr := runtimeControlFrom(ctx, conn, transition.Ref)
		if controlErr == nil {
			if control.state != "open" || control.authority.version != version || control.authority.runner != runner || control.authority.leader != transition.Fence.LeaderEpoch || version == ^uint64(0) {
				return ErrEvidenceConflict
			}
			updated, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET authority_version=?,updated_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='open' AND generation=? AND authority_version=? AND authority_leader_epoch=? AND authority_runner_epoch=?`, version+1, time.Now().UTC().Format(time.RFC3339Nano), transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, control.generation, version, transition.Fence.LeaderEpoch, runner)
			if err != nil {
				return err
			}
			if changed, _ := updated.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
		} else if !errors.Is(controlErr, ErrStaleFence) {
			return controlErr
		}
		return nil
	})
}
