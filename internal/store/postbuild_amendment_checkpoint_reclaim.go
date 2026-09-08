package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

const maxPostbuildAmendmentCheckpointReclaims = 8

// ReclaimPostbuildAmendmentCheckpoint can finish only the local protected
// checkpoint selected by an immutable accepted Reviewer receipt. It never
// creates a new semantic target, reruns a command, or reopens a confirmed effect.
// Git must still prove original HEAD/full snapshot or exact prepared child and
// unchanged implementation before performing the bounded CAS/index completion.
func (s *Store) ReclaimPostbuildAmendmentCheckpoint(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (contracts.GitMutationClaim, error) {
	var claim contracts.GitMutationClaim
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return claim, ErrEvidenceConflict
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		receipt, err := s.postbuildAmendmentCheckpointSnapshotFrom(ctx, conn, ref, version, fence)
		if err != nil {
			return err
		}
		amendment, binding, err := s.authenticateCheckpointSnapshotSource(ctx, conn, ref, version, fence, receipt.Reviewer, receipt.Command)
		if err != nil {
			return err
		}
		provider, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, receipt.Reviewer)
		if err != nil || parsed.Verify == nil {
			return ErrEvidenceConflict
		}
		command, found, err := loadRepositoryCommandResult(ctx, conn, receipt.Command, true)
		if err != nil || !found {
			return ErrEvidenceConflict
		}
		var worktree StoredWorktree
		var repository, baseRef string
		if conn.QueryRowContext(ctx, `SELECT w.path,w.branch_ref,w.state,w.identity_json,w.base_sha,p.canonical_path,p.base_ref FROM worktrees w JOIN projects p ON p.channel=w.channel AND p.id=w.project_id WHERE w.channel=? AND w.project_id=? AND w.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA, &repository, &baseRef) != nil || worktree.State != "registered" || provider.Claim.Repository != repository || provider.Claim.Worktree != worktree.Path || provider.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || provider.Claim.BaseSHA != worktree.BaseSHA {
			return ErrEvidenceConflict
		}
		digest := CanonicalVerificationAmendmentCheckpointDigest(amendment, worktree, receipt.Reviewer, receipt.Command, command.ResultDigest, *parsed.Verify)
		intent := GitMutationIntent{EffectFence: EffectFence{Ref: ref, TicketVersion: version, Fence: fence}, RequestDigest: digest, Repository: repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: baseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: binding.OriginalCheckpointOID}
		intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
		if !validGitIntent(intent) {
			return ErrGitMutationIntent
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return err
		}
		if effect.Ref != ref || effect.Kind != "git/commit" || effect.RequestDigest != digest || effect.ObservedIdentity != "" || effect.ClaimEpoch == ^uint64(0) || (effect.State != EffectPlanned && effect.State != EffectExecuting && effect.State != EffectUncertain) {
			return ErrGitMutationIntent
		}
		if effect.TicketVersion > version || effect.LeaderEpoch > fence.LeaderEpoch || effect.RunnerEpoch > fence.RunnerEpoch {
			return ErrStaleFence
		}
		if repositoryHasGitWriter(ctx, conn, repository) != nil || repositoryHasProviderWriter(ctx, conn, repository) != nil || repositoryHasCommandWriter(ctx, conn, repository) != nil {
			return ErrControlNotDrained
		}
		var count int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND semantic_key<>? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket, intent.SemanticKey).Scan(&count) != nil || count != 0 {
			return ErrControlNotDrained
		}
		var intentCount int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_intents WHERE semantic_key=?`, intent.SemanticKey).Scan(&intentCount) != nil {
			return ErrGitMutationIntent
		}
		var prior contracts.GitMutationClaim
		if effect.State == EffectPlanned {
			// No launch was issued. A planned row with an existing intent belongs
			// to another recovery protocol and is not inferred safe here.
			if intentCount != 0 || effect.ClaimEpoch != 0 {
				return ErrGitMutationIntent
			}
			if validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, receipt.Version, receipt.Fence.RunnerEpoch, receipt.Fence.LeaderEpoch, effect.TicketVersion, effect.RunnerEpoch, effect.LeaderEpoch) != nil || validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, effect.TicketVersion, effect.RunnerEpoch, effect.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
				return ErrStaleFence
			}
		} else {
			facts, err := gitMutationIntentFactsFrom(ctx, conn, intent.SemanticKey)
			if err != nil || intentCount != 1 {
				return ErrGitMutationIntent
			}
			prior = facts.Claim
			if prior.TicketRef != ref || prior.RequestDigest != digest || prior.Repository != repository || prior.Worktree != worktree.Path || prior.Branch != worktree.Branch || prior.Operation != "commit" || prior.BaseRef != baseRef || prior.ExpectedBaseOID != worktree.BaseSHA || prior.ExpectedHeadOID != binding.OriginalCheckpointOID {
				return ErrGitMutationIntent
			}
			if validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, receipt.Version, receipt.Fence.RunnerEpoch, receipt.Fence.LeaderEpoch, prior.TicketVersion, prior.RunnerEpoch, prior.LeaderEpoch) != nil || validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, prior.TicketVersion, prior.RunnerEpoch, prior.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
				return ErrStaleFence
			}
			if effect.State == EffectExecuting && prior.TicketVersion == version && prior.LeaderEpoch == fence.LeaderEpoch && prior.RunnerEpoch == fence.RunnerEpoch {
				claim = prior
				return nil
			}
		}
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='postbuild_amendment_checkpoint_reclaimed'`, ref.Channel, ref.Project, ref.Ticket).Scan(&count) != nil || count >= maxPostbuildAmendmentCheckpointReclaims {
			return ErrEvidenceConflict
		}
		claim = contracts.GitMutationClaim{TicketRef: ref, SemanticKey: intent.SemanticKey, RequestDigest: digest, TicketVersion: version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch + 1, Repository: repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: baseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: binding.OriginalCheckpointOID}
		payload, err := json.Marshal(struct {
			ReceiptDigest string
			Prior         contracts.GitMutationClaim
			PriorEffect   Effect
			Current       contracts.GitMutationClaim
		}{receipt.BindingDigest, prior, effect, claim})
		if err != nil || len(payload) > 16<<10 {
			return ErrEvidenceConflict
		}
		changed, err := conn.ExecContext(ctx, `UPDATE effects SET state='executing',ticket_version=?,leader_epoch=?,runner_epoch=?,claim_epoch=? WHERE semantic_key=? AND state=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND request_digest=? AND observed_identity=''`, version, fence.LeaderEpoch, fence.RunnerEpoch, claim.ClaimEpoch, intent.SemanticKey, effect.State, effect.TicketVersion, effect.LeaderEpoch, effect.RunnerEpoch, effect.ClaimEpoch, digest)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		if effect.State == EffectPlanned {
			_, err = conn.ExecContext(ctx, `INSERT INTO git_mutation_intents(semantic_key,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,branch_ref,operation,base_ref,expected_base_oid,expected_head_oid,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'commit',?,?,?,?)`, intent.SemanticKey, ref.Channel, ref.Project, ref.Ticket, digest, version, fence.LeaderEpoch, fence.RunnerEpoch, claim.ClaimEpoch, repository, worktree.Path, worktree.Branch, baseRef, worktree.BaseSHA, binding.OriginalCheckpointOID, time.Now().UTC().Format(time.RFC3339Nano))
		} else {
			changed, err = conn.ExecContext(ctx, `UPDATE git_mutation_intents SET ticket_version=?,leader_epoch=?,runner_epoch=?,claim_epoch=? WHERE semantic_key=? AND request_digest=? AND operation='commit' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, version, fence.LeaderEpoch, fence.RunnerEpoch, claim.ClaimEpoch, intent.SemanticKey, digest, prior.TicketVersion, prior.LeaderEpoch, prior.RunnerEpoch, prior.ClaimEpoch)
			if err == nil {
				if n, _ := changed.RowsAffected(); n != 1 {
					return ErrStaleFence
				}
			}
		}
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,'postbuild_amendment_checkpoint_reclaimed','verifying','verifying',?,?)`, ref.Channel, ref.Project, ref.Ticket, version, string(payload), time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
	if err != nil {
		return contracts.GitMutationClaim{}, err
	}
	return claim, nil
}
