package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
)

// TransitionRecoverAsGuarded is the only durable autonomy-to-guarded escape
// hatch. The Engine selects the normative transition, but this Store boundary
// independently proves the frozen project policy, the exact blocker, and a
// freshly drained pre-publication control proof. Direct callers therefore
// cannot turn an arbitrary blocked ticket into a guarded merge ticket.
func (s *Store) TransitionRecoverAsGuarded(ctx context.Context, transition Transition) (TransitionResult, error) {
	if s == nil || transition.Ref.Validate() != nil || transition.Trigger != "operator_recover_as_guarded" || transition.From != domain.StateBlocked || transition.To != domain.StateBuilding || transition.ResumeState != "" || transition.ExpectedVersion == 0 || transition.Fence.LeaderEpoch == 0 || transition.Fence.RunnerEpoch == 0 || transition.Fence.ClaimEpoch != 0 {
		return TransitionResult{}, ErrEvidenceConflict
	}
	var payload struct {
		Intent      string `json:"intent"`
		Mode        string `json:"mode"`
		BlockedCode string `json:"blocked_code"`
	}
	if len(transition.EventPayload) == 0 || len(transition.EventPayload) > maxEvidenceJSON || json.Unmarshal([]byte(transition.EventPayload), &payload) != nil || payload.Intent != "recover" || payload.Mode != "guarded" || payload.BlockedCode != "autonomy_ineligible" {
		return TransitionResult{}, ErrEvidenceConflict
	}

	if s.mutations == nil {
		return TransitionResult{}, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return TransitionResult{}, err
	}
	defer g.unlock()

	// The drain proof, policy decode, and transition all run in one IMMEDIATE
	// transaction while the external-mutation gate is held. The daemon's drain
	// is useful host evidence, but direct Store callers cannot forge these
	// facts or race a writer between a read-only proof and the state update.
	var result TransitionResult
	proof, leader, err := s.controlProof(ctx, transition.Ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		return sealRuntimeControl(txCtx, conn, transition.Ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	}, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		if !proof.Drained() || !proof.StrictlyPrePublication() || proof.Ticket.State != domain.StateBlocked || proof.Ticket.ResumeState == "" || !prePublicationState(proof.Ticket.ResumeState) || proof.Ticket.Version != transition.ExpectedVersion || proof.Fence != transition.Fence || leader != transition.Fence.LeaderEpoch {
			return ErrControlNotDrained
		}
		var mode domain.MergeMode
		var blockedCode string
		var generation uint64
		var digest string
		var snapshot []byte
		if err := conn.QueryRowContext(txCtx, `SELECT merge_mode,blocked_code,config_generation,config_digest,config_snapshot_bytes FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&mode, &blockedCode, &generation, &digest, &snapshot); err != nil {
			return err
		}
		if mode != domain.MergeAutonomous || blockedCode != "autonomy_ineligible" || generation == 0 {
			return ErrEvidenceConflict
		}
		effective, decodeErr := config.DecodeSnapshot(snapshot, digest)
		// A project that permits guarded (or autonomous) operation may safely
		// narrow a legacy autonomous ticket to guarded. Manual is the one
		// project maximum that cannot admit this recovery. Never take this fact
		// from Engine attributes: the frozen, signed ticket configuration is the
		// authority that was in force when the ticket was started.
		if decodeErr != nil || effective.MergeMode == domain.MergeManual || effective.Name != string(transition.Ref.Project) {
			return ErrEvidenceConflict
		}
		control, controlErr := runtimeControlFrom(txCtx, conn, transition.Ref)
		if controlErr != nil || control.state != "sealed" || control.stop.version != transition.ExpectedVersion || control.stop.runner != transition.Fence.RunnerEpoch || control.stop.leader != transition.Fence.LeaderEpoch || control.authority != control.stop {
			return ErrControlNotDrained
		}
		// The immediately preceding proof deliberately sealed runtime admission.
		// This is a control completion under that seal, not a new external
		// admission, so authenticate the current ticket fence without rejecting
		// the seal we just established.
		if err := s.assertCurrentTicketFence(txCtx, conn, transition.Ref, transition.ExpectedVersion, transition.Fence); err != nil {
			return err
		}
		updated, err := conn.ExecContext(txCtx, `UPDATE tickets SET state='building',resume_state=NULL,merge_mode='guarded',blocked_code='',version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='blocked' AND version=? AND runner_epoch=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.ExpectedVersion, transition.Fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(txCtx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.ExpectedVersion+1, transition.Trigger, transition.From, transition.To, transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		eventID, err := created.LastInsertId()
		if err != nil {
			return err
		}
		var createdAt string
		if err := conn.QueryRowContext(txCtx, `SELECT created_at FROM events WHERE id=?`, eventID).Scan(&createdAt); err != nil {
			return err
		}
		if err := recordProviderPhaseEntry(txCtx, conn, transition.Ref, domain.PhaseBuild, transition.ExpectedVersion+1, transition.Fence.LeaderEpoch, transition.Fence.RunnerEpoch, eventID, createdAt, transition.From, domain.StateBuilding, transition.Trigger); err != nil {
			return err
		}
		result.Version = transition.ExpectedVersion + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return TransitionResult{}, ErrNotFound
	}
	if err == nil {
		g.latch(transition.Ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	}
	return result, err
}
