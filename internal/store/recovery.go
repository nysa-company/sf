package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// RecoverMergeIntent confirms a stranded merge using recovery's current
// uncertain-effect fence. The old launch fence in MergeIntent is immutable
// evidence only; ReconcileEffects deliberately revokes it on leader change.
func (s *Store) RecoverMergeIntent(ctx context.Context, semanticKey string, observer contracts.MergeIntentObserver) (Effect, error) {
	intent, found, err := s.MergeIntent(ctx, semanticKey)
	if err != nil || !found || observer == nil {
		return Effect{}, ErrNotFound
	}
	if err := validMergeIntent(intent); err != nil {
		return Effect{}, err
	}
	current, err := s.Effect(ctx, semanticKey)
	if err != nil {
		return Effect{}, err
	}
	if err := recoveryLinked(intent, current); err != nil {
		return Effect{}, err
	}
	if current.State == EffectConfirmed {
		return current, nil
	}
	if current.State != EffectUncertain {
		return Effect{}, ErrStaleFence
	}
	recoveryFence := EffectFence{Ref: current.Ref, SemanticKey: current.SemanticKey, TicketVersion: current.TicketVersion, Fence: domain.Fence{LeaderEpoch: current.LeaderEpoch, RunnerEpoch: current.RunnerEpoch, ClaimEpoch: current.ClaimEpoch}}
	identity, err := observer.ObserveMergeIntent(ctx, intent)
	if err != nil {
		return Effect{}, err
	}
	if identity == "" {
		return Effect{}, fmt.Errorf("observed merge identity is required")
	}
	return s.confirmRecoveredMerge(ctx, intent, recoveryFence, identity)
}

func recoveryLinked(intent domain.MergeIntent, effect Effect) error {
	if effect.SemanticKey != intent.SemanticKey || effect.Ref != intent.Ref || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest || effect.TicketVersion != intent.TicketVersion || effect.RunnerEpoch != intent.RunnerEpoch {
		return ErrStaleFence
	}
	// ReconcileEffects moves an executing effect to a new leader and increments
	// claim epoch exactly once per recovery. A lower/equal lineage cannot be a
	// recovery authority for this launch.
	if effect.LeaderEpoch < intent.LeaderEpoch || effect.ClaimEpoch <= intent.ClaimEpoch {
		return ErrStaleFence
	}
	return nil
}

func (s *Store) confirmRecoveredMerge(ctx context.Context, intent domain.MergeIntent, fence EffectFence, identity string) (Effect, error) {
	var result Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		current, err := effectFrom(ctx, conn, fence.SemanticKey)
		if err != nil {
			return err
		}
		if err := recoveryLinked(intent, current); err != nil {
			return err
		}
		if current.State == EffectConfirmed {
			result = current
			return nil
		}
		if current.State != EffectUncertain || current.Ref != fence.Ref || current.TicketVersion != fence.TicketVersion || current.LeaderEpoch != fence.Fence.LeaderEpoch || current.RunnerEpoch != fence.Fence.RunnerEpoch || current.ClaimEpoch != fence.Fence.ClaimEpoch {
			return ErrStaleFence
		}
		if err := s.assertTicketFence(ctx, conn, fence.Ref, fence.TicketVersion, fence.Fence); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='confirmed', observed_identity=? WHERE semantic_key=? AND state='uncertain' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, identity, fence.SemanticKey, fence.TicketVersion, fence.Fence.LeaderEpoch, fence.Fence.RunnerEpoch, fence.Fence.ClaimEpoch)
		if err != nil {
			return err
		}
		changed, _ := updated.RowsAffected()
		if changed != 1 {
			return ErrStaleFence
		}
		result, err = effectFrom(ctx, conn, fence.SemanticKey)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return result, err
}
