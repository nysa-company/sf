package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
)

// CompleteRepositoryCommand durably records the bounded command result before
// the lease is released. A zero exit is never inferred from process absence;
// the exact command claim and observed output identity are persisted together.
func (s *Store) CompleteRepositoryCommand(ctx context.Context, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) error {
	if !validRepositoryCommandClaim(claim) || !result.Observed {
		return ErrRepositoryCommandIntent
	}
	sum := sha256.New()
	_, _ = sum.Write(result.Stdout)
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write(result.Stderr)
	identity := fmt.Sprintf("%s:exit=%d:output=sha256:%s", claim.SpecDigest, result.ExitCode, hex.EncodeToString(sum.Sum(nil)))
	state := EffectFailed
	if result.ExitCode == 0 {
		state = EffectConfirmed
	}
	return s.write(ctx, func(c *sql.Conn) error {
		if err := s.assertRepositoryCommandCurrent(ctx, c, claim); err != nil {
			return err
		}
		r, err := c.ExecContext(ctx, `UPDATE effects SET state=?,observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=?`, state, identity, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}

var _ contracts.RepositoryCommandResultRecorder = (*Store)(nil)

func (s *Store) MarkRepositoryCommandUncertain(ctx context.Context, claim contracts.RepositoryCommandClaim, reason string) error {
	if !validRepositoryCommandClaim(claim) || reason == "" {
		return ErrRepositoryCommandIntent
	}
	return s.write(ctx, func(c *sql.Conn) error {
		if err := s.assertRepositoryCommandCurrent(ctx, c, claim); err != nil {
			return err
		}
		result, err := c.ExecContext(ctx, `UPDATE effects SET state='uncertain',observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=?`, "uncertain:"+reason, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}

// ReconcileStaleRepositoryCommandObservation is deliberately narrower than
// completion: a result that arrived after runner invalidation may only retire
// its exact executing effect as failed. It authenticates immutable intent
// fields and claim epoch but does not require the now-stale ticket fence, and
// can never advance the ticket by recording confirmed.
func (s *Store) ReconcileStaleRepositoryCommandObservation(ctx context.Context, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) error {
	if !validRepositoryCommandClaim(claim) || !result.Observed {
		return ErrRepositoryCommandIntent
	}
	sum := sha256.New()
	_, _ = sum.Write(result.Stdout)
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write(result.Stderr)
	identity := fmt.Sprintf("stale:%s:exit=%d:output=sha256:%s", claim.SpecDigest, result.ExitCode, hex.EncodeToString(sum.Sum(nil)))
	return s.write(ctx, func(c *sql.Conn) error {
		r, err := c.ExecContext(ctx, `UPDATE effects SET state='failed',observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=? AND EXISTS (SELECT 1 FROM repository_command_intents i WHERE i.semantic_key=effects.semantic_key AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=? AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.claim_epoch=? AND i.repository_path=? AND i.worktree_path=? AND i.worktree_identity=? AND i.branch_ref=? AND i.base_ref=? AND i.base_sha=? AND i.command_digest=? AND i.spec_digest=? AND i.policy_digest=? AND i.executable_path=? AND i.executable_digest=?)`, identity, claim.SemanticKey, claim.ClaimEpoch, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Repository, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, claim.ExecutablePath, claim.ExecutableDigest)
		if err != nil {
			return err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}
