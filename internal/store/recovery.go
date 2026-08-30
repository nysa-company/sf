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
	var state domain.State
	var version, runner, leader uint64
	err = s.db.QueryRowContext(ctx, `SELECT t.state, t.version, t.runner_epoch, d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&state, &version, &runner, &leader)
	if err != nil {
		return mergeRecoverySnapshot{}, normalizeBusy(ctx, err)
	}
	// Equal version/runner advances are not unique to startup fencing: pause,
	// take, and cancel also invalidate the runner while moving the ticket into a
	// control state. A merge witness is recoverable only while its normative
	// owner is still exactly the merging state.
	if state != domain.StateMerging {
		return mergeRecoverySnapshot{}, ErrStaleFence
	}
	live := EffectFence{SemanticKey: prior.SemanticKey, Ref: prior.Ref, TicketVersion: version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner, ClaimEpoch: prior.ClaimEpoch}}
	if prior.State == EffectConfirmed {
		// A confirmed observation may survive another daemon crash before the
		// workflow consumes it. Every startup fence advances ticket version and
		// runner epoch together, while an ordinary workflow transition advances
		// version only. Accept any positive, equal recovery delta from the
		// immutable launch intent, and either the exact confirming fence or a
		// later equally advanced recovery fence.
		if !equalRecoveryAdvance(intent.TicketVersion, intent.RunnerEpoch, prior.TicketVersion, prior.RunnerEpoch) ||
			!sameOrLaterRecoveryFence(prior, version, runner, leader) {
			return mergeRecoverySnapshot{}, ErrStaleFence
		}
		return mergeRecoverySnapshot{prior: prior, live: live}, nil
	}
	if prior.State != EffectUncertain || prior.TicketVersion != intent.TicketVersion || prior.RunnerEpoch != intent.RunnerEpoch || prior.LeaderEpoch != leader || !equalRecoveryAdvance(prior.TicketVersion, prior.RunnerEpoch, version, runner) {
		return mergeRecoverySnapshot{}, ErrStaleFence
	}
	return mergeRecoverySnapshot{prior: prior, live: live}, nil
}

// equalRecoveryAdvance distinguishes one or more startup fences from normal
// state progression: recovery increments version and runner together, whereas
// a workflow transition increments only version.
func equalRecoveryAdvance(fromVersion, fromRunner, toVersion, toRunner uint64) bool {
	return toVersion > fromVersion && toRunner > fromRunner && toVersion-fromVersion == toRunner-fromRunner
}

func sameOrLaterRecoveryFence(effect Effect, version, runner, leader uint64) bool {
	if version == effect.TicketVersion && runner == effect.RunnerEpoch {
		return leader == effect.LeaderEpoch
	}
	return leader > effect.LeaderEpoch && equalRecoveryAdvance(effect.TicketVersion, effect.RunnerEpoch, version, runner)
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
		var state domain.State
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&state, &version, &runner); err != nil {
			return err
		}
		if state != domain.StateMerging || version != snapshot.live.TicketVersion || runner != snapshot.live.Fence.RunnerEpoch || !equalRecoveryAdvance(current.TicketVersion, current.RunnerEpoch, version, runner) {
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
