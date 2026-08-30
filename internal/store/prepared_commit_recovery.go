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
