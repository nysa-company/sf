package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// RecoverMergeIntent authenticates the immutable pre-crash merge intent
// against the stranded uncertain effect, then confirms that same effect using
// the live ticket fence installed by FenceRecoveredRunners. MergeIntent is
// never rewritten: it remains evidence of the original launch.
func (s *Store) RecoverMergeIntent(ctx context.Context, semanticKey string, observer contracts.MergeIntentObserver) (Effect, error) {
	intent, found, err := s.MergeIntent(ctx, semanticKey)
	if err != nil || !found || observer == nil {
		return Effect{}, ErrNotFound
	}
	if err := validMergeIntent(intent); err != nil {
		return Effect{}, err
	}
	snapshot, err := s.recoverySnapshot(ctx, intent)
	if err != nil {
		return Effect{}, err
	}
	if snapshot.prior.State == EffectConfirmed {
		return snapshot.prior, nil
	}
	identity, err := observer.ObserveMergeIntent(ctx, intent)
	if err != nil {
		return Effect{}, err
	}
	if identity == "" {
		return Effect{}, fmt.Errorf("observed merge identity is required")
	}
	return s.confirmRecoveredMerge(ctx, intent, snapshot, identity)
}

type mergeRecoverySnapshot struct {
	prior Effect
	live  EffectFence
}

func (s *Store) recoverySnapshot(ctx context.Context, intent domain.MergeIntent) (mergeRecoverySnapshot, error) {
	prior, err := s.Effect(ctx, intent.SemanticKey)
	if err != nil {
		return mergeRecoverySnapshot{}, err
	}
	if err := recoveryLinked(intent, prior); err != nil {
		return mergeRecoverySnapshot{}, err
	}
	var version, runner, leader uint64
	err = s.db.QueryRowContext(ctx, `SELECT t.version, t.runner_epoch, d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&version, &runner, &leader)
	if err != nil {
		return mergeRecoverySnapshot{}, normalizeBusy(ctx, err)
	}
	live := EffectFence{SemanticKey: prior.SemanticKey, Ref: prior.Ref, TicketVersion: version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner, ClaimEpoch: prior.ClaimEpoch}}
	if prior.State == EffectConfirmed {
		if prior.TicketVersion != version || prior.RunnerEpoch != runner || prior.LeaderEpoch != leader || version != intent.TicketVersion+1 || runner != intent.RunnerEpoch+1 {
			return mergeRecoverySnapshot{}, ErrStaleFence
		}
		return mergeRecoverySnapshot{prior: prior, live: live}, nil
	}
	if prior.State != EffectUncertain || prior.TicketVersion != intent.TicketVersion || prior.RunnerEpoch != intent.RunnerEpoch || prior.LeaderEpoch != leader || version != prior.TicketVersion+1 || runner != prior.RunnerEpoch+1 {
		return mergeRecoverySnapshot{}, ErrStaleFence
	}
	return mergeRecoverySnapshot{prior: prior, live: live}, nil
}

func recoveryLinked(intent domain.MergeIntent, effect Effect) error {
	if effect.SemanticKey != intent.SemanticKey || effect.Ref != intent.Ref || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest {
		return ErrStaleFence
	}
	if effect.LeaderEpoch < intent.LeaderEpoch || effect.ClaimEpoch <= intent.ClaimEpoch {
		return ErrStaleFence
	}
	return nil
}

func (s *Store) confirmRecoveredMerge(ctx context.Context, intent domain.MergeIntent, snapshot mergeRecoverySnapshot, identity string) (Effect, error) {
	var result Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		current, err := effectFrom(ctx, conn, snapshot.prior.SemanticKey)
		if err != nil {
			return err
		}
		// Authenticate the unchanged prior uncertain effect before allowing its
		// fence fields to be promoted to the live recovered ticket identity.
		if err := recoveryLinked(intent, current); err != nil {
			return err
		}
		if current.State != EffectUncertain || current != snapshot.prior {
			return ErrStaleFence
		}
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&version, &runner); err != nil {
			return err
		}
		if version != snapshot.live.TicketVersion || runner != snapshot.live.Fence.RunnerEpoch || version != current.TicketVersion+1 || runner != current.RunnerEpoch+1 {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, intent.Ref.Channel, version, runner, snapshot.live.Fence); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='confirmed', observed_identity=?, ticket_version=?, runner_epoch=? WHERE semantic_key=? AND state='uncertain' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND request_digest=?`, identity, version, runner, current.SemanticKey, current.TicketVersion, current.LeaderEpoch, current.RunnerEpoch, current.ClaimEpoch, current.RequestDigest)
		if err != nil {
			return err
		}
		changed, _ := updated.RowsAffected()
		if changed != 1 {
			return ErrStaleFence
		}
		result, err = effectFrom(ctx, conn, current.SemanticKey)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return result, err
}
