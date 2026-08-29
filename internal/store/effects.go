package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type EffectState string

const (
	EffectPlanned   EffectState = "planned"
	EffectExecuting EffectState = "executing"
	EffectConfirmed EffectState = "confirmed"
	EffectUncertain EffectState = "uncertain"
	EffectFailed    EffectState = "failed"
)

type Effect struct {
	SemanticKey      string
	Ref              domain.TicketRef
	Kind             string
	State            EffectState
	TicketVersion    uint64
	LeaderEpoch      uint64
	RunnerEpoch      uint64
	ClaimEpoch       uint64
	RequestDigest    string
	ObservedIdentity string
}

type EffectPlan struct {
	SemanticKey   string
	Ref           domain.TicketRef
	Kind          string
	TicketVersion uint64
	Fence         domain.Fence
	RequestDigest string
}

// EffectFence is carried across the external-call boundary. The response may
// be persisted only while every ticket and effect epoch remains current.
type EffectFence struct {
	SemanticKey   string
	Ref           domain.TicketRef
	TicketVersion uint64
	Fence         domain.Fence
}

type EffectClaim struct {
	Effect  Effect
	Claimed bool
}

type EffectObservation struct {
	EffectFence
	Present  bool
	Identity string
}

func (s *Store) PlanEffect(ctx context.Context, plan EffectPlan) (Effect, error) {
	if err := plan.Ref.Validate(); err != nil {
		return Effect{}, err
	}
	if plan.SemanticKey == "" || plan.Kind == "" || plan.RequestDigest == "" {
		return Effect{}, fmt.Errorf("effect semantic key, kind, and request digest are required")
	}
	var effect Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, plan.Ref, plan.TicketVersion, plan.Fence); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO effects(semantic_key, channel, project_id, ticket_id, effect_kind, state, ticket_version, leader_epoch, runner_epoch, claim_epoch, request_digest)
			VALUES (?, ?, ?, ?, ?, 'planned', ?, ?, ?, 0, ?) ON CONFLICT(semantic_key) DO NOTHING`, plan.SemanticKey, plan.Ref.Channel, plan.Ref.Project, plan.Ref.Ticket, plan.Kind, plan.TicketVersion, plan.Fence.LeaderEpoch, plan.Fence.RunnerEpoch, plan.RequestDigest); err != nil {
			return err
		}
		var err error
		effect, err = effectFrom(ctx, conn, plan.SemanticKey)
		if err != nil {
			return err
		}
		if effect.Ref != plan.Ref || effect.Kind != plan.Kind || effect.RequestDigest != plan.RequestDigest || effect.TicketVersion != plan.TicketVersion {
			return fmt.Errorf("%w: %s", ErrEffectKey, plan.SemanticKey)
		}
		return nil
	})
	return effect, err
}

// ClaimEffect creates the only live execution claim for a semantic operation.
// A completed key returns Claimed=false; callers must never execute it again.
func (s *Store) ClaimEffect(ctx context.Context, fence EffectFence) (EffectClaim, error) {
	var result EffectClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, fence.Ref, fence.TicketVersion, fence.Fence); err != nil {
			return err
		}
		effect, err := effectFrom(ctx, conn, fence.SemanticKey)
		if err != nil {
			return err
		}
		if effect.Ref != fence.Ref || effect.TicketVersion != fence.TicketVersion {
			return fmt.Errorf("%w: %s", ErrEffectKey, fence.SemanticKey)
		}
		if effect.State == EffectConfirmed {
			result = EffectClaim{Effect: effect}
			return nil
		}
		if effect.State == EffectExecuting || effect.State == EffectUncertain {
			return fmt.Errorf("%w: %s", ErrEffectBusy, fence.SemanticKey)
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='executing', ticket_version=?, leader_epoch=?, runner_epoch=?, claim_epoch=claim_epoch+1, claimed_at=?
			WHERE semantic_key=? AND state IN ('planned','failed') AND claim_epoch=?`, fence.TicketVersion, fence.Fence.LeaderEpoch, fence.Fence.RunnerEpoch, time.Now().UTC().Format(time.RFC3339Nano), fence.SemanticKey, effect.ClaimEpoch)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrEffectBusy
		}
		effect, err = effectFrom(ctx, conn, fence.SemanticKey)
		if err != nil {
			return err
		}
		result = EffectClaim{Effect: effect, Claimed: true}
		return nil
	})
	return result, err
}

func (s *Store) ConfirmEffect(ctx context.Context, fence EffectFence, identity string) (Effect, error) {
	return s.ObserveEffect(ctx, EffectObservation{EffectFence: fence, Present: true, Identity: identity})
}

func (s *Store) FailEffect(ctx context.Context, fence EffectFence) (Effect, error) {
	return s.finishEffect(ctx, fence, EffectFailed, "")
}

// ObserveEffect is the read-before-retry reconciliation write. An observed
// present effect is confirmed; a proven semantic absence becomes failed and is
// eligible for a new, incremented claim. Unknown observations leave it
// uncertain, so time alone never permits another executor.
func (s *Store) ObserveEffect(ctx context.Context, observation EffectObservation) (Effect, error) {
	if observation.Present && strings.TrimSpace(observation.Identity) == "" {
		return Effect{}, fmt.Errorf("observed identity is required to confirm an effect")
	}
	state := EffectFailed
	if observation.Present {
		state = EffectConfirmed
	}
	return s.finishEffect(ctx, observation.EffectFence, state, observation.Identity)
}

// RecordStaleObservation preserves evidence discovered after recovery when a
// ticket advanced while the external result was lost. It deliberately leaves
// the effect uncertain: the evidence is useful for reconciliation, but cannot
// confirm an effect or advance a newer ticket identity.
func (s *Store) RecordStaleObservation(ctx context.Context, observation EffectObservation) (Effect, error) {
	if strings.TrimSpace(observation.Identity) == "" {
		return Effect{}, fmt.Errorf("observed identity is required for stale observation")
	}
	var effect Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		current, err := effectFrom(ctx, conn, observation.SemanticKey)
		if err != nil {
			return err
		}
		if current.Ref != observation.Ref || current.State != EffectUncertain || current.LeaderEpoch != observation.Fence.LeaderEpoch || current.ClaimEpoch != observation.Fence.ClaimEpoch {
			return ErrStaleFence
		}
		var leader, version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT d.leader_epoch, t.version, t.runner_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, observation.Ref.Channel, observation.Ref.Project, observation.Ref.Ticket).Scan(&leader, &version, &runner); err != nil {
			return err
		}
		if leader != observation.Fence.LeaderEpoch {
			return ErrStaleFence
		}
		if version == current.TicketVersion && runner == current.RunnerEpoch {
			return fmt.Errorf("stale observation requires a changed ticket identity")
		}
		if _, err := conn.ExecContext(ctx, `UPDATE effects SET observed_identity=? WHERE semantic_key=? AND state='uncertain' AND leader_epoch=? AND claim_epoch=?`, observation.Identity, observation.SemanticKey, observation.Fence.LeaderEpoch, observation.Fence.ClaimEpoch); err != nil {
			return err
		}
		effect, err = effectFrom(ctx, conn, observation.SemanticKey)
		return err
	})
	if err != nil {
		return effect, err
	}
	return effect, ErrStaleObservation
}

func (s *Store) finishEffect(ctx context.Context, fence EffectFence, state EffectState, identity string) (Effect, error) {
	var effect Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, fence.Ref, fence.TicketVersion, fence.Fence); err != nil {
			return err
		}
		current, err := effectFrom(ctx, conn, fence.SemanticKey)
		if err != nil {
			return err
		}
		if current.Ref != fence.Ref || current.TicketVersion != fence.TicketVersion || current.ClaimEpoch != fence.Fence.ClaimEpoch || current.LeaderEpoch != fence.Fence.LeaderEpoch || current.RunnerEpoch != fence.Fence.RunnerEpoch {
			return ErrStaleFence
		}
		if current.State == EffectConfirmed && state == EffectConfirmed {
			effect = current
			return nil
		}
		if current.State != EffectExecuting && current.State != EffectUncertain {
			return fmt.Errorf("cannot finish effect in state %q", current.State)
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state=?, observed_identity=? WHERE semantic_key=? AND state IN ('executing','uncertain') AND claim_epoch=? AND leader_epoch=? AND runner_epoch=? AND ticket_version=?`, state, identity, fence.SemanticKey, fence.Fence.ClaimEpoch, fence.Fence.LeaderEpoch, fence.Fence.RunnerEpoch, fence.TicketVersion)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		effect, err = effectFrom(ctx, conn, fence.SemanticKey)
		return err
	})
	return effect, err
}

// ReconcileEffects changes stranded executing rows to uncertain under the new
// leader. It does not claim or retry an external operation; callers must first
// perform a read-only semantic observation through ObserveEffect.
func (s *Store) ReconcileEffects(ctx context.Context, channel domain.Channel, leaderEpoch uint64) ([]Effect, error) {
	var effects []Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); err != nil {
			return err
		}
		if current != leaderEpoch {
			return ErrStaleFence
		}
		// Recovery revokes the crashed claimant and gives the observing leader a
		// fresh claim epoch. It must retain the ticket identity that crossed the
		// external boundary: a later ticket transition cannot be reinterpreted as
		// the identity of an old effect.
		if _, err := conn.ExecContext(ctx, `UPDATE effects
			SET state='uncertain', leader_epoch=?, claim_epoch=claim_epoch+1
			WHERE channel=? AND state IN ('executing','uncertain')`, leaderEpoch, channel); err != nil {
			return err
		}
		rows, err := conn.QueryContext(ctx, `SELECT semantic_key, channel, project_id, ticket_id, effect_kind, state, ticket_version, leader_epoch, runner_epoch, claim_epoch, request_digest, observed_identity FROM effects WHERE channel=? AND state='uncertain' ORDER BY semantic_key`, channel)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			effect, err := scanEffect(rows)
			if err != nil {
				return err
			}
			effects = append(effects, effect)
		}
		return rows.Err()
	})
	return effects, err
}

func (s *Store) Effect(ctx context.Context, semanticKey string) (Effect, error) {
	effect, err := effectFrom(ctx, s.db, semanticKey)
	return effect, normalizeBusy(ctx, err)
}

func (s *Store) MarkEffectUncertain(ctx context.Context, fence EffectFence) (Effect, error) {
	return s.finishEffect(ctx, fence, EffectUncertain, "")
}

func (s *Store) assertTicketFence(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) error {
	var version, runner uint64
	if err := conn.QueryRowContext(ctx, `SELECT version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if version != expectedVersion {
		return ErrStaleFence
	}
	return s.currentFence(ctx, conn, ref.Channel, version, runner, fence)
}

type effectScanner interface{ Scan(...any) error }

func effectFrom(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key string) (Effect, error) {
	return scanEffect(query.QueryRowContext(ctx, `SELECT semantic_key, channel, project_id, ticket_id, effect_kind, state, ticket_version, leader_epoch, runner_epoch, claim_epoch, request_digest, observed_identity FROM effects WHERE semantic_key=?`, key))
}

func scanEffect(row effectScanner) (Effect, error) {
	var effect Effect
	err := row.Scan(&effect.SemanticKey, &effect.Ref.Channel, &effect.Ref.Project, &effect.Ref.Ticket, &effect.Kind, &effect.State, &effect.TicketVersion, &effect.LeaderEpoch, &effect.RunnerEpoch, &effect.ClaimEpoch, &effect.RequestDigest, &effect.ObservedIdentity)
	if errors.Is(err, sql.ErrNoRows) {
		return Effect{}, ErrNotFound
	}
	return effect, err
}
