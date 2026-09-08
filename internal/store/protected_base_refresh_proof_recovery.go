package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// ReclaimProtectedBaseRefreshProof retries only the exact idempotent private
// proof-ref fetch at its original live endpoint. It cannot move a ticket/base,
// replay publication, or infer safety from elapsed time. Git still acquires
// the exclusive lease and re-observes the exact protected tip before fetching.
// Cross-fence unreserved proofs deliberately require separate recovery.
func (s *Store) ReclaimProtectedBaseRefreshProof(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, newBase string) (contracts.GitMutationClaim, error) {
	var claim contracts.GitMutationClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		value, err := s.protectedBaseRefreshContextAt(ctx, conn, ref, version, fence, newBase)
		if err != nil {
			return err
		}
		proof, err := protectedBaseRefreshProofFor(value)
		if err != nil {
			return err
		}
		if err := protectedBaseRefreshDrainedExceptProofAt(ctx, conn, value, proof.Intent, true); err != nil {
			return err
		}
		var reserved int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM protected_base_refresh_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&reserved); err != nil || reserved != 0 {
			return ErrEvidenceConflict
		}
		facts, err := gitMutationIntentFactsFrom(ctx, conn, proof.Intent.SemanticKey)
		if err != nil || !sameGitMutationBinding(proof.Intent, facts.Claim) || facts.Claim.TicketVersion != version || facts.Claim.LeaderEpoch != fence.LeaderEpoch || facts.Claim.RunnerEpoch != fence.RunnerEpoch || facts.Claim.ClaimEpoch == 0 || facts.Claim.ClaimEpoch > maxProtectedBaseRefreshReclaims || facts.Effect.Ref != ref || facts.Effect.Kind != "git/protected-ref-fetch" || facts.Effect.RequestDigest != proof.Intent.RequestDigest || facts.Effect.State != EffectUncertain || facts.Effect.TicketVersion != version || facts.Effect.LeaderEpoch != fence.LeaderEpoch || facts.Effect.RunnerEpoch != fence.RunnerEpoch || facts.Effect.ClaimEpoch != facts.Claim.ClaimEpoch || facts.Effect.ObservedIdentity != "" || facts.PreparedCommitOID != "" || facts.PreparedTreeOID != "" {
			return ErrGitMutationIntent
		}
		claim = facts.Claim
		claim.ClaimEpoch++
		changed, err := conn.ExecContext(ctx, `UPDATE effects SET state='executing',claim_epoch=? WHERE semantic_key=? AND state='uncertain' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND request_digest=? AND observed_identity=''`, claim.ClaimEpoch, claim.SemanticKey, version, fence.LeaderEpoch, fence.RunnerEpoch, facts.Claim.ClaimEpoch, claim.RequestDigest)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		changed, err = conn.ExecContext(ctx, `UPDATE git_mutation_intents SET claim_epoch=? WHERE semantic_key=? AND operation='protected-ref-fetch' AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, claim.ClaimEpoch, claim.SemanticKey, claim.RequestDigest, version, fence.LeaderEpoch, fence.RunnerEpoch, facts.Claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrGitMutationIntent
		}
		// This is an effect-claim change, not a lifecycle event. Both durable
		// rows retain their exact semantic target and matching bounded epoch.
		return nil
	})
	return claim, err
}
