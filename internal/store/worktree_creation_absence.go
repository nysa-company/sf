package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// WorktreeCreationAbsence is the narrow repository lease held while a caller
// proves that an uncertain create-worktree invocation left no Git artefact.
// It deliberately does not implement contracts.GitMutationLease: no native
// mutating operation may be launched through this handle.
type WorktreeCreationAbsence struct {
	store  *Store
	claim  contracts.GitMutationClaim
	nonce  []byte
	closed bool
}

// BeginWorktreeCreationAbsence revokes the old claim epoch and reserves the
// repository for a read-only absence proof. The immutable intent is retained.
func (s *Store) BeginWorktreeCreationAbsence(ctx context.Context, original contracts.GitMutationClaim, current EffectFence) (*WorktreeCreationAbsence, error) {
	if s == nil || !validContractClaim(original) || original.Operation != "create-worktree" || current.SemanticKey != original.SemanticKey || current.Ref != original.TicketRef || current.Fence.ClaimEpoch == 0 {
		return nil, ErrGitMutationIntent
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("absence lease nonce: %w", err)
	}
	handle := &WorktreeCreationAbsence{store: s, nonce: nonce}
	err := s.write(ctx, func(conn *sql.Conn) error {
		facts, err := gitMutationIntentFactsFrom(ctx, conn, original.SemanticKey)
		if err != nil || facts.Claim != original {
			return ErrGitMutationIntent
		}
		effect, err := effectFrom(ctx, conn, original.SemanticKey)
		if err != nil || effect.Kind != "git/create-worktree" || effect.State != EffectUncertain || effect.ObservedIdentity != "" || effect.Ref != original.TicketRef || effect.RequestDigest != original.RequestDigest {
			return ErrGitMutationIntent
		}
		if effect.ClaimEpoch < original.ClaimEpoch || current.TicketVersion == 0 || current.Fence.LeaderEpoch == 0 || current.Fence.RunnerEpoch == 0 || effect.TicketVersion > current.TicketVersion || effect.RunnerEpoch > current.Fence.RunnerEpoch || effect.LeaderEpoch != current.Fence.LeaderEpoch || effect.ClaimEpoch != current.Fence.ClaimEpoch || !linkedGitRecoveryEffect(original, effect) {
			return ErrStaleFence
		}
		if validateRunnerRecoveryLedger(ctx, conn, original.TicketRef, original.TicketVersion, original.RunnerEpoch, original.LeaderEpoch, current.TicketVersion, current.Fence.RunnerEpoch, current.Fence.LeaderEpoch) != nil {
			return ErrStaleFence
		}
		var state domain.State
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, original.TicketRef.Channel, original.TicketRef.Project, original.TicketRef.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return err
		}
		if state != domain.StatePlanning || version != current.TicketVersion || runner != current.Fence.RunnerEpoch || leader != current.Fence.LeaderEpoch {
			return ErrStaleFence
		}
		var worktreeCount int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, original.TicketRef.Channel, original.TicketRef.Project, original.TicketRef.Ticket).Scan(&worktreeCount); err != nil || worktreeCount != 0 {
			return ErrGitMutationIntent
		}
		if err := repositoryHasProviderWriter(ctx, conn, original.Repository); err != nil {
			return err
		}
		if err := repositoryHasCommandWriter(ctx, conn, original.Repository); err != nil {
			return err
		}
		if effect.ClaimEpoch == ^uint64(0) {
			return ErrStaleFence
		}
		newEpoch := effect.ClaimEpoch + 1
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET ticket_version=?,leader_epoch=?,runner_epoch=?,claim_epoch=? WHERE semantic_key=? AND state='uncertain' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND observed_identity=''`, current.TicketVersion, current.Fence.LeaderEpoch, current.Fence.RunnerEpoch, newEpoch, original.SemanticKey, effect.TicketVersion, effect.LeaderEpoch, effect.RunnerEpoch, effect.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		claim := facts.Claim
		claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch = current.TicketVersion, current.Fence.LeaderEpoch, current.Fence.RunnerEpoch, newEpoch
		if _, err := conn.ExecContext(ctx, `INSERT INTO git_mutation_leases(repository_path,semantic_key,nonce,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,worktree_path,branch_ref,operation,base_ref,expected_base_oid,expected_head_oid,state,launch_state,observation_only,acquired_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'active','unrecorded',1,?)`, claim.Repository, claim.SemanticKey, nonce, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Worktree, claim.Branch, claim.Operation, claim.BaseRef, claim.ExpectedBaseOID, claim.ExpectedHeadOID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		handle.claim = claim
		return nil
	})
	if err != nil {
		return nil, err
	}
	return handle, nil
}

// Check authenticates the unrecorded absence-proof lease and current fence.
func (h *WorktreeCreationAbsence) Check(ctx context.Context) error {
	if h == nil || h.store == nil || h.closed {
		return ErrGitMutationLease
	}
	return h.store.write(ctx, func(conn *sql.Conn) error { return h.checkFrom(ctx, conn, true) })
}

func (h *WorktreeCreationAbsence) checkFrom(ctx context.Context, conn *sql.Conn, current bool) error {
	facts, err := gitMutationIntentFactsFrom(ctx, conn, h.claim.SemanticKey)
	if err != nil || facts.Effect.State != EffectUncertain || facts.Effect.ObservedIdentity != "" {
		return ErrGitMutationLease
	}
	expected := facts.Claim
	expected.TicketVersion, expected.LeaderEpoch, expected.RunnerEpoch, expected.ClaimEpoch = facts.Effect.TicketVersion, facts.Effect.LeaderEpoch, facts.Effect.RunnerEpoch, facts.Effect.ClaimEpoch
	if expected != h.claim || h.claim.Operation != "create-worktree" || h.claim.ClaimEpoch <= facts.Claim.ClaimEpoch {
		return ErrGitMutationLease
	}
	if err := validateRunnerRecoveryLedgerPrefix(ctx, conn, h.claim.TicketRef, facts.Claim.TicketVersion, facts.Claim.RunnerEpoch, facts.Claim.LeaderEpoch, h.claim.TicketVersion, h.claim.RunnerEpoch, h.claim.LeaderEpoch); err != nil {
		return ErrGitMutationLease
	}
	var count int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases
	 WHERE repository_path=? AND semantic_key=? AND nonce=? AND channel=? AND project_id=? AND ticket_id=? AND request_digest=?
	 AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND worktree_path=? AND branch_ref=? AND operation='create-worktree' AND base_ref=? AND expected_base_oid=? AND expected_head_oid=?
	 AND state='active' AND launch_state='unrecorded' AND observation_only=1 AND process_pid=0 AND process_pgid=0 AND process_boot_identity='' AND process_start_identity='' AND prepared_commit_oid='' AND prepared_tree_oid='' AND prior_remote_observed=0 AND prior_remote_oid=''`,
		h.claim.Repository, h.claim.SemanticKey, h.nonce, h.claim.TicketRef.Channel, h.claim.TicketRef.Project, h.claim.TicketRef.Ticket, h.claim.RequestDigest,
		h.claim.TicketVersion, h.claim.LeaderEpoch, h.claim.RunnerEpoch, h.claim.ClaimEpoch, h.claim.Worktree, h.claim.Branch, h.claim.BaseRef, h.claim.ExpectedBaseOID, h.claim.ExpectedHeadOID).Scan(&count)
	if err != nil || count != 1 {
		return ErrGitMutationLease
	}
	if current {
		if err := h.store.assertTicketFence(ctx, conn, h.claim.TicketRef, h.claim.TicketVersion, domain.Fence{LeaderEpoch: h.claim.LeaderEpoch, RunnerEpoch: h.claim.RunnerEpoch}); err != nil {
			return err
		}
	}
	return nil
}

// CompleteAbsent settles the exact uncertain effect only after the caller has
// performed all operation-specific native absence observations.
func (h *WorktreeCreationAbsence) CompleteAbsent(ctx context.Context) error {
	if h == nil || h.store == nil || h.closed {
		return ErrGitMutationLease
	}
	err := h.store.write(ctx, func(conn *sql.Conn) error {
		if err := h.checkFrom(ctx, conn, true); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='failed' WHERE semantic_key=? AND state='uncertain' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND observed_identity=''`, h.claim.SemanticKey, h.claim.TicketVersion, h.claim.LeaderEpoch, h.claim.RunnerEpoch, h.claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		deleted, err := conn.ExecContext(ctx, `DELETE FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state='unrecorded' AND observation_only=1`, h.claim.Repository, h.claim.SemanticKey, h.nonce)
		if err != nil {
			return err
		}
		if n, _ := deleted.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
	if err == nil {
		h.closed = true
	}
	return err
}

// Release abandons the observation without settling uncertainty.
func (h *WorktreeCreationAbsence) Release() error {
	if h == nil || h.store == nil || h.closed {
		return ErrGitMutationLease
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := h.store.write(ctx, func(conn *sql.Conn) error {
		if err := h.checkFrom(ctx, conn, false); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state='unrecorded' AND observation_only=1`, h.claim.Repository, h.claim.SemanticKey, h.nonce)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
	if err == nil {
		h.closed = true
	}
	return err
}
