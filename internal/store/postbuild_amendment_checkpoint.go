package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// CanonicalVerificationAmendmentCheckpointDigest is the existing v1 checkpoint
// wire contract shared by issuance and crash recovery. The immutable amendment
// key also selects its uniquely bound postbuild snapshot, when one exists.
func CanonicalVerificationAmendmentCheckpointDigest(amendment VerificationAmendment, worktree StoredWorktree, provider ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey, resultDigest string, artifact phaseartifact.Verification) string {
	data, _ := json.Marshal(struct {
		Kind              string
		Ref               domain.TicketRef
		TransitionVersion uint64
		ConsumedVersion   uint64
		Prior             VerificationRevision
		Builder           ProviderAttemptResultKey
		BuilderTypedSHA   string
		ProposedDigest    string
		ProposedCommand   []string
		Reason            string
		Requester         string
		BudgetRequestID   string
		WorktreePath      string
		WorktreeBranch    string
		WorktreeIdentity  []byte
		BaseSHA           string
		Provider          ProviderAttemptResultKey
		Command           contracts.RepositoryCommandResultKey
		ResultDigest      string
		Artifact          phaseartifact.Verification
	}{"verification-amendment-checkpoint/v1", amendment.Ref, amendment.TransitionTicketVersion, amendment.ConsumedVersion, amendment.Prior, amendment.BuilderResult, amendment.BuilderTypedSHA256, amendment.ProposedDigest, amendment.ProposedCommand, amendment.Reason, amendment.Requester, amendment.BudgetRequestID, worktree.Path, worktree.Branch, worktree.IdentityJSON, worktree.BaseSHA, provider, command, resultDigest, artifact})
	return repositoryResultDigest(data)
}

// PostbuildAmendmentPreparedCheckpoint authenticates a prepared child during
// the commit-before-RecordVerification window. It grants no writer lease or
// permission to repair the index. Physical admission must still compare the
// accepted Reviewer's immutable FullSnapshotDigest before commit and the
// companion implementation digest after commit. Absence is false,nil; malformed or ambiguous evidence is
// never absence.
func (s *Store) PostbuildAmendmentPreparedCheckpoint(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (CommitObservation, bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return CommitObservation{}, false, ErrEvidenceConflict
	}
	var observation CommitObservation
	var found bool
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, ref, version, fence); err != nil {
			return err
		}
		var state domain.State
		if conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state) != nil || state != domain.StateVerifying {
			return ErrEvidenceConflict
		}
		amendment, err := s.loadPendingVerificationAmendmentAtFence(ctx, conn, ref, version, fence)
		if err != nil {
			return err
		}
		binding, repair, err := loadPostbuildAmendmentBinding(ctx, conn, amendment)
		if err != nil {
			return err
		}
		if err := assertNoVerificationAmendmentDownstream(ctx, conn, ref); err != nil {
			return err
		}
		var worktree StoredWorktree
		var repository, baseRef string
		if conn.QueryRowContext(ctx, `SELECT w.path,w.branch_ref,w.state,w.identity_json,w.base_sha,p.canonical_path,p.base_ref FROM worktrees w JOIN projects p ON p.channel=w.channel AND p.id=w.project_id WHERE w.channel=? AND w.project_id=? AND w.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA, &repository, &baseRef) != nil || worktree.State != "registered" {
			return ErrEvidenceConflict
		}
		builder, _, err := s.loadHistoricalProviderAttemptResult(ctx, conn, binding.BuilderResult)
		if err != nil || builder.Claim.Repository != repository || builder.Claim.Worktree != worktree.Path || builder.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || builder.Claim.BaseSHA != worktree.BaseSHA {
			return ErrEvidenceConflict
		}
		rows, err := conn.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit' AND repository_path=? AND worktree_path=? AND branch_ref=? AND base_ref=? AND expected_base_oid=? AND expected_head_oid=? AND (prepared_commit_oid<>'' OR prepared_tree_oid<>'') ORDER BY semantic_key LIMIT 2`, ref.Channel, ref.Project, ref.Ticket, repository, worktree.Path, worktree.Branch, baseRef, worktree.BaseSHA, repair.OriginalCheckpointOID)
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if len(keys) != 1 {
			return ErrEvidenceConflict
		}
		receipt, err := s.postbuildAmendmentCheckpointSnapshotFrom(ctx, conn, ref, version, fence)
		if err != nil {
			return ErrEvidenceConflict
		}
		facts, err := gitMutationIntentFactsFrom(ctx, conn, keys[0])
		if err != nil || !validOID(facts.PreparedCommitOID) || !validOID(facts.PreparedTreeOID) {
			return ErrEvidenceConflict
		}
		if validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, receipt.Version, receipt.Fence.RunnerEpoch, receipt.Fence.LeaderEpoch, facts.Claim.TicketVersion, facts.Claim.RunnerEpoch, facts.Claim.LeaderEpoch) != nil || validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, facts.Claim.TicketVersion, facts.Claim.RunnerEpoch, facts.Claim.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
			return ErrEvidenceConflict
		}
		key := ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseVerification}
		if conn.QueryRowContext(ctx, `SELECT id,attempt FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND role='reviewer' ORDER BY attempt DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&key.AttemptID, &key.Attempt) != nil {
			return ErrEvidenceConflict
		}
		decision, err := s.verificationAmendmentDecisionFrom(ctx, conn, amendment, ref, version, fence, key)
		if err != nil || decision != VerificationAmendmentAccepted {
			return ErrEvidenceConflict
		}
		provider, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
		if err != nil || parsed.Verify == nil || provider.Claim.Repository != repository || provider.Claim.Worktree != worktree.Path || provider.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || provider.Claim.BaseSHA != worktree.BaseSHA {
			return ErrEvidenceConflict
		}
		intent, err := workflowprompt.CanonicalVerificationIntentBytes(*parsed.Verify)
		if err != nil {
			return ErrEvidenceConflict
		}
		proof, err := workflowprompt.CanonicalVerificationProofBytes(*parsed.Verify)
		if err != nil {
			return ErrEvidenceConflict
		}
		commandRows, err := conn.QueryContext(ctx, `SELECT semantic_key,claim_epoch FROM repository_command_results WHERE channel=? AND project_id=? AND ticket_id=? AND semantic_key LIKE ? ORDER BY semantic_key,claim_epoch LIMIT 65`, ref.Channel, ref.Project, ref.Ticket, "repository-command-evidence/"+RepositoryCommandPurposePrebuildVerification+"/%")
		if err != nil {
			return err
		}
		var commands []contracts.RepositoryCommandResultKey
		for commandRows.Next() {
			var k contracts.RepositoryCommandResultKey
			if err := commandRows.Scan(&k.SemanticKey, &k.ClaimEpoch); err != nil {
				commandRows.Close()
				return err
			}
			commands = append(commands, k)
		}
		err = commandRows.Err()
		commandRows.Close()
		if err != nil {
			return err
		}
		if len(commands) > 64 {
			return ErrEvidenceConflict
		}
		matches := 0
		for _, commandKey := range commands {
			if commandKey != receipt.Command || key != receipt.Reviewer {
				continue
			}
			command, exists, err := loadRepositoryCommandResult(ctx, conn, commandKey, true)
			if err != nil || !exists {
				return ErrEvidenceConflict
			}
			if command.Claim.TicketVersion < provider.Claim.ExpectedVersion || command.Claim.TicketVersion > version || command.Claim.Repository != repository || command.Claim.Branch != worktree.Branch || command.Claim.BaseRef != baseRef {
				continue
			}
			commandFence := domain.Fence{LeaderEpoch: command.Claim.LeaderEpoch, RunnerEpoch: command.Claim.RunnerEpoch}
			if validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, provider.Claim.ExpectedVersion, provider.Claim.RunnerEpoch, provider.Claim.LeaderEpoch, command.Claim.TicketVersion, commandFence.RunnerEpoch, commandFence.LeaderEpoch) != nil {
				continue
			}
			artifact := VerificationArtifact{Ref: ref, ExpectedVersion: command.Claim.TicketVersion, Fence: commandFence, Intent: intent, Proof: proof, OwnedFiles: parsed.Verify.OwnedFiles, ProviderResult: &key, CommandResult: commandKey}
			if _, _, err := authenticateVerificationCommandEvidence(ctx, conn, artifact, parsed.Verify); err != nil {
				continue
			}
			if facts.Claim.RequestDigest == CanonicalVerificationAmendmentCheckpointDigest(amendment, worktree, key, commandKey, command.ResultDigest, *parsed.Verify) {
				matches++
			}
		}
		if matches != 1 {
			return ErrEvidenceConflict
		}
		observation = CommitObservation{CommitOID: facts.PreparedCommitOID, ParentOID: repair.OriginalCheckpointOID, TreeOID: facts.PreparedTreeOID}
		found = true
		return nil
	})
	if err != nil {
		return CommitObservation{}, false, err
	}
	return observation, found, nil
}
