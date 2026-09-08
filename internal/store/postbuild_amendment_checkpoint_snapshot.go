package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// PostbuildAmendmentCheckpointSnapshot binds the completed accepted Reviewer
// and prebuild command to the caller's physical precommit observation. Store
// authenticates provenance, not physical bytes; the caller must reobserve both
// the full snapshot and the unchanged companion implementation before mutation.
type PostbuildAmendmentCheckpointSnapshot struct {
	Ref                        domain.TicketRef
	AmendmentTransitionVersion uint64
	Version                    uint64
	Fence                      domain.Fence
	Reviewer                   ProviderAttemptResultKey
	Command                    contracts.RepositoryCommandResultKey
	FullSnapshotDigest         string
	ImplementationDigest       string
	CompanionBindingDigest     string
	BindingDigest              string
	CreatedAt                  string
}

func checkpointSnapshotDigest(value PostbuildAmendmentCheckpointSnapshot) string {
	value.BindingDigest = ""
	raw, _ := json.Marshal(value)
	return repositoryResultDigest(raw)
}

func (s *Store) RecordPostbuildAmendmentCheckpointSnapshot(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, reviewer ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey, fullSnapshotDigest string) (PostbuildAmendmentCheckpointSnapshot, error) {
	var value PostbuildAmendmentCheckpointSnapshot
	if s == nil || !validClaimDigest(fullSnapshotDigest) {
		return value, ErrEvidenceConflict
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		amendment, binding, err := s.authenticateCheckpointSnapshotSource(ctx, conn, ref, version, fence, reviewer, command)
		if err != nil {
			return err
		}
		var repository string
		if conn.QueryRowContext(ctx, `SELECT canonical_path FROM projects WHERE channel=? AND id=?`, ref.Channel, ref.Project).Scan(&repository) != nil {
			return ErrEvidenceConflict
		}
		if repositoryHasProviderWriter(ctx, conn, repository) != nil || repositoryHasCommandWriter(ctx, conn, repository) != nil {
			return ErrControlNotDrained
		}
		var writers int
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, repository).Scan(&writers) != nil || writers != 0 {
			return ErrControlNotDrained
		}
		old, err := loadCheckpointSnapshot(ctx, conn, ref, amendment.TransitionTicketVersion)
		if err == nil {
			if old.Reviewer != reviewer || old.Command != command || old.FullSnapshotDigest != fullSnapshotDigest || old.CompanionBindingDigest != binding.BindingDigest || old.ImplementationDigest != binding.Snapshot.ImplementationDigest {
				return ErrEvidenceConflict
			}
			if validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, old.Version, old.Fence.RunnerEpoch, old.Fence.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
				return ErrEvidenceConflict
			}
			value = old
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		// A new snapshot cannot be captured after an unresolved Git effect. Replay
		// must retain the already immutable snapshot rather than recapture bytes.
		if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket).Scan(&writers) != nil || writers != 0 {
			return ErrControlNotDrained
		}
		value = PostbuildAmendmentCheckpointSnapshot{Ref: ref, AmendmentTransitionVersion: amendment.TransitionTicketVersion, Version: version, Fence: fence, Reviewer: reviewer, Command: command, FullSnapshotDigest: fullSnapshotDigest, ImplementationDigest: binding.Snapshot.ImplementationDigest, CompanionBindingDigest: binding.BindingDigest, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		value.BindingDigest = checkpointSnapshotDigest(value)
		_, err = conn.ExecContext(ctx, `INSERT INTO postbuild_amendment_checkpoint_snapshots(channel,project_id,ticket_id,amendment_transition_version,ticket_version,leader_epoch,runner_epoch,reviewer_attempt_id,reviewer_attempt,reviewer_phase,reviewer_role,command_semantic_key,command_claim_epoch,full_snapshot_digest,implementation_digest,companion_binding_digest,binding_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,'verification','reviewer',?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, value.AmendmentTransitionVersion, version, fence.LeaderEpoch, fence.RunnerEpoch, reviewer.AttemptID, reviewer.Attempt, command.SemanticKey, command.ClaimEpoch, fullSnapshotDigest, value.ImplementationDigest, value.CompanionBindingDigest, value.BindingDigest, value.CreatedAt)
		return err
	})
	return value, err
}

func loadCheckpointSnapshot(ctx context.Context, q rowQueryer, ref domain.TicketRef, entry uint64) (PostbuildAmendmentCheckpointSnapshot, error) {
	v := PostbuildAmendmentCheckpointSnapshot{Ref: ref, AmendmentTransitionVersion: entry, Reviewer: ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseVerification}}
	err := q.QueryRowContext(ctx, `SELECT ticket_version,leader_epoch,runner_epoch,reviewer_attempt_id,reviewer_attempt,command_semantic_key,command_claim_epoch,full_snapshot_digest,implementation_digest,companion_binding_digest,binding_digest,created_at FROM postbuild_amendment_checkpoint_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND amendment_transition_version=? AND reviewer_phase='verification' AND reviewer_role='reviewer'`, ref.Channel, ref.Project, ref.Ticket, entry).Scan(&v.Version, &v.Fence.LeaderEpoch, &v.Fence.RunnerEpoch, &v.Reviewer.AttemptID, &v.Reviewer.Attempt, &v.Command.SemanticKey, &v.Command.ClaimEpoch, &v.FullSnapshotDigest, &v.ImplementationDigest, &v.CompanionBindingDigest, &v.BindingDigest, &v.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	if err != nil {
		return v, err
	}
	if !validClaimDigest(v.FullSnapshotDigest) || !validClaimDigest(v.ImplementationDigest) || !validClaimDigest(v.CompanionBindingDigest) || v.BindingDigest != checkpointSnapshotDigest(v) {
		return v, ErrEvidenceConflict
	}
	return v, nil
}

func (s *Store) authenticateCheckpointSnapshotSource(ctx context.Context, q *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, reviewer ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey) (VerificationAmendment, PostbuildAmendmentBinding, error) {
	var a VerificationAmendment
	var b PostbuildAmendmentBinding
	if err := s.assertTicketFence(ctx, q, ref, version, fence); err != nil {
		return a, b, fmt.Errorf("checkpoint snapshot live fence: %w", err)
	}
	var state domain.State
	if q.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state) != nil || state != domain.StateVerifying {
		return a, b, fmt.Errorf("checkpoint snapshot verifying state: %w", ErrEvidenceConflict)
	}
	a, err := s.loadPendingVerificationAmendmentAtFence(ctx, q, ref, version, fence)
	if err != nil {
		return a, b, fmt.Errorf("checkpoint snapshot pending request: %w", err)
	}
	b, _, err = loadPostbuildAmendmentBinding(ctx, q, a)
	if err != nil {
		return a, b, fmt.Errorf("checkpoint snapshot companion: %w", err)
	}
	if err := assertNoVerificationAmendmentDownstream(ctx, q, ref); err != nil {
		return a, b, fmt.Errorf("checkpoint snapshot downstream: %w", err)
	}
	decision, err := s.verificationAmendmentDecisionFrom(ctx, q, a, ref, version, fence, reviewer)
	if err != nil || decision != VerificationAmendmentAccepted {
		return a, b, fmt.Errorf("checkpoint snapshot accepted reviewer (decision=%s, cause=%v): %w", decision, err, ErrEvidenceConflict)
	}
	provider, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, reviewer)
	if err != nil || parsed.Verify == nil {
		return a, b, fmt.Errorf("checkpoint snapshot reviewer artifact: %w", ErrEvidenceConflict)
	}
	result, found, err := loadRepositoryCommandResult(ctx, q, command, true)
	if err != nil || !found || result.Claim.Repository != provider.Claim.Repository || result.Claim.Worktree != provider.Claim.Worktree || result.Claim.WorktreeIdentity != provider.Claim.WorktreeIdentity || result.Claim.BaseSHA != provider.Claim.BaseSHA {
		return a, b, fmt.Errorf("checkpoint snapshot command identity (found=%t, cause=%v): %w", found, err, ErrEvidenceConflict)
	}
	if validateRunnerRecoveryLedgerPrefix(ctx, q, ref, provider.Claim.ExpectedVersion, provider.Claim.RunnerEpoch, provider.Claim.LeaderEpoch, result.Claim.TicketVersion, result.Claim.RunnerEpoch, result.Claim.LeaderEpoch) != nil || validateRunnerRecoveryLedgerPrefix(ctx, q, ref, result.Claim.TicketVersion, result.Claim.RunnerEpoch, result.Claim.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
		return a, b, fmt.Errorf("checkpoint snapshot command recovery lineage: %w", ErrEvidenceConflict)
	}
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(*parsed.Verify)
	if err != nil {
		return a, b, ErrEvidenceConflict
	}
	proof, err := workflowprompt.CanonicalVerificationProofBytes(*parsed.Verify)
	if err != nil {
		return a, b, ErrEvidenceConflict
	}
	artifact := VerificationArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: parsed.Verify.OwnedFiles, ProviderResult: &reviewer, CommandResult: command}
	if _, _, err := authenticateVerificationCommandEvidence(ctx, q, artifact, parsed.Verify); err != nil {
		return a, b, fmt.Errorf("checkpoint snapshot command evidence: %w", err)
	}
	return a, b, nil
}

func (s *Store) postbuildAmendmentCheckpointSnapshotFrom(ctx context.Context, q *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildAmendmentCheckpointSnapshot, error) {
	a, err := s.loadPendingVerificationAmendmentAtFence(ctx, q, ref, version, fence)
	if err != nil {
		return PostbuildAmendmentCheckpointSnapshot{}, err
	}
	v, err := loadCheckpointSnapshot(ctx, q, ref, a.TransitionTicketVersion)
	if err != nil {
		return v, err
	}
	_, b, err := s.authenticateCheckpointSnapshotSource(ctx, q, ref, version, fence, v.Reviewer, v.Command)
	if err != nil {
		return v, err
	}
	if v.CompanionBindingDigest != b.BindingDigest || v.ImplementationDigest != b.Snapshot.ImplementationDigest || validateRunnerRecoveryLedgerPrefix(ctx, q, ref, v.Version, v.Fence.RunnerEpoch, v.Fence.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
		return v, ErrEvidenceConflict
	}
	return v, nil
}

func (s *Store) PostbuildAmendmentCheckpointSnapshot(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (PostbuildAmendmentCheckpointSnapshot, error) {
	var value PostbuildAmendmentCheckpointSnapshot
	if s == nil {
		return value, ErrEvidenceConflict
	}
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		var err error
		value, err = s.postbuildAmendmentCheckpointSnapshotFrom(ctx, conn, ref, version, fence)
		return err
	})
	return value, err
}
