package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// DeferPostbuildAmendmentCheckpointRecovery only classifies an immutable
// checkpoint intent. It grants no writer authority and does not settle the
// effect: HEAD alone cannot prove the protected index synchronization finished.
func (s *Store) DeferPostbuildAmendmentCheckpointRecovery(ctx context.Context, claim contracts.GitMutationClaim) (bool, error) {
	if s == nil || !validContractClaim(claim) || claim.Operation != "commit" {
		return false, ErrEvidenceConflict
	}
	deferRecovery := false
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		facts, err := gitMutationIntentFactsFrom(ctx, conn, claim.SemanticKey)
		if err != nil || facts.Claim != claim || facts.Effect.Kind != "git/commit" || facts.Effect.RequestDigest != claim.RequestDigest {
			return ErrEvidenceConflict
		}
		// An ordinary ticket without a companion stays in generic recovery.
		// Once a companion exists, missing/malformed checkpoint evidence must
		// never hide it behind the generic HEAD-only observer.
		var companions int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).Scan(&companions) != nil {
			return ErrEvidenceConflict
		}
		if companions == 0 {
			var receipts int
			if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM postbuild_amendment_checkpoint_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).Scan(&receipts) != nil || receipts != 0 {
				return ErrEvidenceConflict
			}
			return nil
		}
		if companions != 1 {
			return ErrEvidenceConflict
		}
		var entry uint64
		var amendmentFence domain.Fence
		if conn.QueryRowContext(ctx, `SELECT amendment_transition_version,consumed_leader_epoch,consumed_runner_epoch FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).Scan(&entry, &amendmentFence.LeaderEpoch, &amendmentFence.RunnerEpoch) != nil {
			return ErrEvidenceConflict
		}
		amendment, err := s.loadVerificationAmendment(ctx, conn, claim.TicketRef, entry, amendmentFence)
		if err != nil {
			return ErrEvidenceConflict
		}
		binding, _, err := loadPostbuildAmendmentBinding(ctx, conn, amendment)
		if err != nil {
			return ErrEvidenceConflict
		}
		// Later candidate commits descend from the amended checkpoint, not
		// this original parent, and retain the ordinary recovery protocol.
		if claim.ExpectedHeadOID != binding.OriginalCheckpointOID {
			return nil
		}
		receipt, err := loadCheckpointSnapshot(ctx, conn, claim.TicketRef, entry)
		if err != nil || receipt.CompanionBindingDigest != binding.BindingDigest || receipt.ImplementationDigest != binding.Snapshot.ImplementationDigest {
			return ErrEvidenceConflict
		}
		decision, err := s.verificationAmendmentDecisionFromAt(ctx, conn, amendment, claim.TicketRef, receipt.Version, receipt.Fence, receipt.Reviewer, false)
		if err != nil || decision != VerificationAmendmentAccepted {
			return ErrEvidenceConflict
		}
		provider, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, receipt.Reviewer)
		if err != nil || parsed.Verify == nil {
			return ErrEvidenceConflict
		}
		command, found, err := loadRepositoryCommandResult(ctx, conn, receipt.Command, true)
		if err != nil || !found {
			return ErrEvidenceConflict
		}
		intentBytes, err := workflowprompt.CanonicalVerificationIntentBytes(*parsed.Verify)
		if err != nil {
			return ErrEvidenceConflict
		}
		proofBytes, err := workflowprompt.CanonicalVerificationProofBytes(*parsed.Verify)
		if err != nil {
			return ErrEvidenceConflict
		}
		artifact := VerificationArtifact{Ref: claim.TicketRef, ExpectedVersion: receipt.Version, Fence: receipt.Fence, Intent: intentBytes, Proof: proofBytes, OwnedFiles: parsed.Verify.OwnedFiles, ProviderResult: &receipt.Reviewer, CommandResult: receipt.Command}
		if _, _, err := authenticateVerificationCommandEvidence(ctx, conn, artifact, parsed.Verify); err != nil {
			return ErrEvidenceConflict
		}
		var worktree StoredWorktree
		if conn.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA) != nil || worktree.State != "registered" || provider.Claim.Worktree != worktree.Path || provider.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || provider.Claim.Repository != claim.Repository || provider.Claim.BaseSHA != worktree.BaseSHA {
			return ErrEvidenceConflict
		}
		digest := CanonicalVerificationAmendmentCheckpointDigest(amendment, worktree, receipt.Reviewer, receipt.Command, command.ResultDigest, *parsed.Verify)
		intent := GitMutationIntent{EffectFence: EffectFence{Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}}, RequestDigest: digest, Repository: provider.Claim.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: claim.BaseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: binding.OriginalCheckpointOID}
		if claim.RequestDigest != digest || claim.SemanticKey != CanonicalGitMutationSemanticKey(intent) || claim.ExpectedHeadOID != binding.OriginalCheckpointOID || claim.Worktree != worktree.Path || claim.Branch != worktree.Branch || claim.ExpectedBaseOID != worktree.BaseSHA {
			return ErrEvidenceConflict
		}
		if validateRunnerRecoveryLedgerPrefix(ctx, conn, claim.TicketRef, receipt.Version, receipt.Fence.RunnerEpoch, receipt.Fence.LeaderEpoch, claim.TicketVersion, claim.RunnerEpoch, claim.LeaderEpoch) != nil {
			return ErrEvidenceConflict
		}
		deferRecovery = true
		return nil
	})
	return deferRecovery, err
}
