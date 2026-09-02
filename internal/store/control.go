package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// TransitionAndInvalidateRunner applies a normative control transition and
// revokes the current ticket runner in the same SQLite transaction. Keeping
// these writes atomic prevents an old provider completion from landing in the
// gap between entering stopping/cancelling and advancing the runner epoch.
//
// The workflow engine selects from the approved state-machine artifact, but
// this boundary independently admits only its two runner-invalidating control
// dispositions. It is never a generic lifecycle persistence escape hatch.
func (s *Store) TransitionAndInvalidateRunner(ctx context.Context, transition Transition) (TransitionResult, error) {
	if !validRunnerInvalidatingControlTransition(transition) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if err := transition.Ref.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if !transition.To.Valid() || !transition.From.Valid() || transition.Trigger == "" {
		return TransitionResult{}, fmt.Errorf("valid from/to state and trigger are required")
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, fmt.Errorf("control event payload must be bounded JSON")
	}

	if s.mutations == nil {
		return TransitionResult{}, ErrStaleFence
	}
	if err := s.mutations.lock(ctx); err != nil {
		return TransitionResult{}, err
	}
	defer s.mutations.unlock()
	var result TransitionResult
	var seal mutationRevocation
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var actual domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if actual != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets
			SET state=?, resume_state=?, version=version+1, runner_epoch=runner_epoch+1
			WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`,
			transition.To, nullableState(transition.ResumeState), transition.Ref.Channel, transition.Ref.Project,
			transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return ErrStaleFence
		}
		value := mutationRevocation{version: version + 1, leader: transition.Fence.LeaderEpoch, runner: runner + 1}
		if err := sealRuntimeControl(ctx, conn, transition.Ref, value); err != nil {
			return err
		}
		seal = value
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, transition.Ref.Channel, transition.Ref.Project,
			transition.Ref.Ticket, version+1, transition.Trigger, transition.From, transition.To,
			transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	if err == nil {
		s.mutations.latch(transition.Ref, seal)
	}
	return result, err
}

// validRunnerInvalidatingControlTransition is deliberately narrower than the
// state-machine selector. This Store primitive has enough authority to change
// both lifecycle state and runner epoch, so it may persist only the two
// operator control dispositions that require that atomic invalidation. Every
// normal phase progression, including all publication entry/continuation, must
// use its evidence-bearing Store boundary instead.
func validRunnerInvalidatingControlTransition(transition Transition) bool {
	pauseState := func(state domain.State) bool {
		switch state {
		case domain.StatePlanning, domain.StateVerifying, domain.StateBuilding, domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing, domain.StateWaitingApproval, domain.StateWaitingManualMerge, domain.StateMerging, domain.StateReconciling:
			return true
		default:
			return false
		}
	}
	switch transition.Trigger {
	case "operator_pause_or_take":
		return transition.To == domain.StateStopping && transition.ResumeState == transition.From && pauseState(transition.From)
	case "operator_cancel":
		return transition.To == domain.StateCancelling && transition.ResumeState == "" && runnerInvalidatingCancelSource(transition.From)
	default:
		return false
	}
}

func runnerInvalidatingCancelSource(state domain.State) bool {
	switch state {
	case domain.StateQueued, domain.StatePlanning, domain.StateVerifying, domain.StateBuilding,
		domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing,
		domain.StateWaitingApproval, domain.StateWaitingManualMerge, domain.StateMerging,
		domain.StateReconciling, domain.StatePaused, domain.StateBlocked, domain.StateStopping:
		return true
	default:
		return false
	}
}

// CompleteControlTransition commits a drained stopping/cancelling transition,
// closes any still-active phase attempt, and releases ticket capacity in one
// transaction. The caller must separately prove that no supervised process or
// writer remains; this method independently refuses while SQLite still holds
// an executing or uncertain external effect.
func (s *Store) CompleteControlTransition(ctx context.Context, transition Transition) (TransitionResult, error) {
	if err := transition.Ref.Validate(); err != nil {
		return TransitionResult{}, err
	}
	validPair := transition.From == domain.StateStopping && transition.To == domain.StatePaused || transition.From == domain.StateCancelling && transition.To == domain.StateCancelled
	if !validPair || transition.Trigger != "process_and_effects_drained" {
		return TransitionResult{}, errors.New("control completion requires the normative drained transition")
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, errors.New("control event payload must be bounded JSON")
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var actual domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if actual != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		if err := releaseTerminalCapacity(ctx, conn, transition.Ref); err != nil {
			return err
		}
		at := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := conn.ExecContext(ctx, `UPDATE phase_runs SET state='cancelled',completed_at=COALESCE(completed_at,?),outcome='cancelled'
			WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`, at, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE workflow_owners SET state=? WHERE channel=? AND project_id=? AND ticket_id=?`, transition.To, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=?,version=version+1
			WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`,
			transition.To, nullableState(transition.ResumeState), transition.Ref.Channel, transition.Ref.Project,
			transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at)
			VALUES (?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket,
			version+1, transition.Trigger, transition.From, transition.To, transition.EventPayload, at)
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}
