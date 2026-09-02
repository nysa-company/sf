package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

var ErrPreparedCommitRecovery = errors.New("prepared git commit recovery is uncertain")

// RetireUnpreparedGitCommit proves that a failed commit never reached the
// durable prepared-object boundary. It removes the immutable child intent in
// the same transaction that makes the effect retryable; otherwise the unique
// semantic key would strand every deterministic retry behind an old intent.
func (s *Store) RetireUnpreparedGitCommit(ctx context.Context, claim contracts.GitMutationClaim) error {
	if s == nil || !validContractClaim(claim) || claim.Operation != "commit" {
		return ErrGitMutationIntent
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		facts, err := gitMutationIntentFactsFrom(ctx, conn, claim.SemanticKey)
		if err != nil || facts.Claim != claim || facts.Effect.State != EffectExecuting || facts.PreparedCommitOID != "" || facts.PreparedTreeOID != "" {
			return ErrGitMutationIntent
		}
		if err := s.assertTicketFence(ctx, conn, claim.TicketRef, claim.TicketVersion, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}); err != nil {
			return err
		}
		if err := repositoryHasProviderWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		if err := repositoryHasCommandWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		if err := repositoryHasGitWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		deleted, err := conn.ExecContext(ctx, `DELETE FROM git_mutation_intents WHERE semantic_key=? AND claim_epoch=? AND operation='commit' AND prepared_commit_oid='' AND prepared_tree_oid=''`, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := deleted.RowsAffected(); n != 1 {
			return ErrGitMutationIntent
		}
		changed, err := conn.ExecContext(ctx, `UPDATE effects SET state='failed',observed_identity='' WHERE semantic_key=? AND state='executing' AND claim_epoch=? AND leader_epoch=? AND runner_epoch=? AND ticket_version=?`, claim.SemanticKey, claim.ClaimEpoch, claim.LeaderEpoch, claim.RunnerEpoch, claim.TicketVersion)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

// ConfirmPreparedCommit is the normal-path counterpart to recovery.  Git has
// already recorded the immutable commit/tree tuple before making the ref
// reachable; this final Store write accepts only an authenticated observation
// of that exact tuple under the still-live original fence.
func (s *Store) ConfirmPreparedCommit(ctx context.Context, claim contracts.GitMutationClaim, observation contracts.PreparedCommitObservation) (Effect, error) {
	if s == nil || !validContractClaim(claim) || claim.Operation != "commit" ||
		!validStoreOID(observation.CommitOID) || !validStoreOID(observation.ParentOID) || !validStoreOID(observation.TreeOID) ||
		len(observation.CommitOID) != len(claim.ExpectedBaseOID) || len(observation.TreeOID) != len(claim.ExpectedBaseOID) || len(observation.ParentOID) != len(claim.ExpectedHeadOID) {
		return Effect{}, ErrGitMutationIntent
	}
	var result Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		facts, err := gitMutationIntentFactsFrom(ctx, conn, claim.SemanticKey)
		if err != nil || facts.Claim != claim || facts.Effect.Kind != "git/commit" || facts.Effect.Ref != claim.TicketRef || facts.Effect.RequestDigest != claim.RequestDigest {
			return ErrGitMutationIntent
		}
		if facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" || observation.CommitOID != facts.PreparedCommitOID || observation.TreeOID != facts.PreparedTreeOID || observation.ParentOID != claim.ExpectedHeadOID {
			return ErrGitMutationIntent
		}
		if facts.Effect.State == EffectConfirmed {
			if facts.Effect.ObservedIdentity != observation.CommitOID {
				return ErrGitMutationIntent
			}
			result = facts.Effect
			return nil
		}
		// An interrupted materializer marks a prepared commit uncertain rather
		// than retrying the visible update-ref. A same-process retry may settle
		// that exact tuple only while the original ticket/fence/claim remains
		// current; every check below is shared with the executing normal path.
		if facts.Effect.State != EffectExecuting && facts.Effect.State != EffectUncertain {
			return ErrStaleFence
		}
		if err := s.assertTicketFence(ctx, conn, claim.TicketRef, claim.TicketVersion, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}); err != nil {
			return err
		}
		// Commit releases its own lease before the final read-only observation.
		// Do not let that small interval admit a different provider, repository
		// command, or Git writer and then bless an observation from a changing
		// repository.
		if err := repositoryHasProviderWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		if err := repositoryHasCommandWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		if err := repositoryHasGitWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='confirmed',observed_identity=? WHERE semantic_key=? AND state IN ('executing','uncertain') AND channel=? AND project_id=? AND ticket_id=? AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, observation.CommitOID, claim.SemanticKey, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		result, err = effectFrom(ctx, conn, claim.SemanticKey)
		return err
	})
	return result, err
}

func preparedCommitRecoveryFailure(err error) error {
	if err == nil {
		return ErrPreparedCommitRecovery
	}
	if errors.Is(err, ErrPreparedCommitRecovery) {
		return err
	}
	return errors.Join(ErrPreparedCommitRecovery, err)
}

// ConfirmRecoveredPreparedCommit confirms a commit whose update-ref may have
// completed before the original runner lost its response. It reloads the
// immutable intent and current effect inside the final Store write authority,
// then accepts only the exact prepared commit/tree tuple with the expected
// parent. No lease, intent, or mutation is minted or changed by this method.
//
// The first recovery pass runs before FenceRecoveredRunners, so the effect's
// recovery leader/claim fence must be current while its ticket version and
// runner epoch remain the exact pre-fence values. A confirmed exact result is
// an idempotent read, including after a later runner fence.
func (s *Store) ConfirmRecoveredPreparedCommit(ctx context.Context, claim contracts.GitMutationClaim, observation contracts.PreparedCommitObservation) (Effect, error) {
	if s == nil || !validContractClaim(claim) || claim.Operation != "commit" {
		return Effect{}, preparedCommitRecoveryFailure(ErrGitMutationIntent)
	}
	if !validStoreOID(observation.CommitOID) || !validStoreOID(observation.ParentOID) || !validStoreOID(observation.TreeOID) {
		return Effect{}, preparedCommitRecoveryFailure(fmt.Errorf("prepared commit observation is malformed"))
	}
	if len(observation.ParentOID) != len(claim.ExpectedHeadOID) || len(observation.CommitOID) != len(claim.ExpectedBaseOID) || len(observation.TreeOID) != len(claim.ExpectedBaseOID) {
		return Effect{}, preparedCommitRecoveryFailure(fmt.Errorf("prepared commit observation uses a different object format"))
	}
	var result Effect
	err := s.write(ctx, func(conn *sql.Conn) error {
		// This is the final authority boundary: both immutable intent and mutable
		// effect are read through the same SQLite connection that performs the
		// conditional confirmation below.
		facts, err := gitMutationIntentFactsFrom(ctx, conn, claim.SemanticKey)
		if err != nil {
			return preparedCommitRecoveryFailure(err)
		}
		if facts.Claim != claim || facts.Claim.Operation != "commit" || facts.Effect.Ref != claim.TicketRef || facts.Effect.Kind != "git/commit" || facts.Effect.RequestDigest != claim.RequestDigest {
			return preparedCommitRecoveryFailure(ErrGitMutationIntent)
		}
		if facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" || !validGitMutationFacts("commit", claim.ExpectedBaseOID, claim.ExpectedHeadOID, facts.PreparedCommitOID, facts.PreparedTreeOID, 0, "") {
			return preparedCommitRecoveryFailure(fmt.Errorf("prepared commit tuple is missing or malformed"))
		}
		if observation.CommitOID != facts.PreparedCommitOID || observation.TreeOID != facts.PreparedTreeOID || observation.ParentOID != claim.ExpectedHeadOID {
			return preparedCommitRecoveryFailure(fmt.Errorf("observed commit does not match the canonical prepared tuple"))
		}

		current := facts.Effect
		if current.State == EffectConfirmed {
			if current.ObservedIdentity != facts.PreparedCommitOID {
				return preparedCommitRecoveryFailure(fmt.Errorf("confirmed commit identity differs from the prepared commit"))
			}
			result = current
			return nil
		}
		if current.State != EffectUncertain {
			return preparedCommitRecoveryFailure(ErrStaleFence)
		}
		// ReconcileEffects advances only the effect's recovery leader and claim
		// while this lane runs. A ticket that was already fenced on a prior
		// startup, or a competing writer that changed the ticket during this
		// pass, is not a current recovery fence and must remain uncertain.
		var ticketVersion, ticketRunner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch
			FROM tickets t JOIN daemon_instances d ON d.channel=t.channel
			WHERE t.channel=? AND t.project_id=? AND t.id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).
			Scan(&ticketVersion, &ticketRunner, &leader); err != nil {
			return preparedCommitRecoveryFailure(err)
		}
		if current.LeaderEpoch != leader || current.LeaderEpoch <= claim.LeaderEpoch || current.ClaimEpoch <= claim.ClaimEpoch || current.TicketVersion != ticketVersion || current.RunnerEpoch != ticketRunner {
			return preparedCommitRecoveryFailure(ErrStaleFence)
		}
		if err := s.currentFence(ctx, conn, claim.TicketRef.Channel, ticketVersion, ticketRunner, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticketRunner}); err != nil {
			return preparedCommitRecoveryFailure(err)
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='confirmed', observed_identity=?
			WHERE semantic_key=? AND state='uncertain' AND channel=? AND project_id=? AND ticket_id=?
			AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?
			AND observed_identity=?`, facts.PreparedCommitOID, current.SemanticKey, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket,
			current.RequestDigest, current.TicketVersion, current.LeaderEpoch, current.RunnerEpoch, current.ClaimEpoch, current.ObservedIdentity)
		if err != nil {
			return preparedCommitRecoveryFailure(err)
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return preparedCommitRecoveryFailure(ErrStaleFence)
		}
		result, err = effectFrom(ctx, conn, current.SemanticKey)
		if err != nil {
			return preparedCommitRecoveryFailure(err)
		}
		return nil
	})
	if err != nil {
		return Effect{}, err
	}
	return result, nil
}
