package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type ProtectedBaseRefreshBuildContext struct {
	Completion   ProtectedBaseRefreshCompletion
	Predecessor  StoredCandidate
	Verification StoredVerification
}

func (s *Store) readProtectedBaseRefreshSnapshot(ctx context.Context, read func(*sql.Conn) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return normalizeBusy(ctx, err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(cleanup, "ROLLBACK")
	}()
	return read(conn)
}

func protectedBaseRefreshForTicketAt(ctx context.Context, q baseRefreshRowQueryer, ref domain.TicketRef) (protectedBaseRefreshIntent, ProtectedBaseRefreshCompletion, error) {
	var key string
	err := q.QueryRowContext(ctx, `SELECT refresh_effect_semantic_key FROM protected_base_refresh_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return protectedBaseRefreshIntent{}, ProtectedBaseRefreshCompletion{}, ErrNotFound
	}
	if err != nil {
		return protectedBaseRefreshIntent{}, ProtectedBaseRefreshCompletion{}, err
	}
	_, value, _, err := loadProtectedBaseRefreshReservationAt(ctx, q, key)
	if err != nil {
		return protectedBaseRefreshIntent{}, ProtectedBaseRefreshCompletion{}, err
	}
	completion, found, err := loadProtectedBaseRefreshCompletionAt(ctx, q, key)
	if err != nil || !found {
		return protectedBaseRefreshIntent{}, ProtectedBaseRefreshCompletion{}, ErrEvidenceConflict
	}
	return value, completion, nil
}

// evidenceWorktreeMatchesAt is for immutable historical evidence only. Live
// issue/launch paths continue to require the effective worktrees row verbatim.
// The sole alternate identity must be the exact old registration consumed by
// an authenticated completed refresh, with an evidence endpoint no later than
// that reservation. Arbitrary old paths, bases and future inputs never match.
func evidenceWorktreeMatchesAt(ctx context.Context, q baseRefreshRowQueryer, ref domain.TicketRef, version uint64, path, identity, base string) bool {
	var currentPath, currentIdentity, currentBase, state string
	if err := q.QueryRowContext(ctx, `SELECT path,identity_json,base_sha,state FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentPath, &currentIdentity, &currentBase, &state); err != nil || state != "registered" {
		return false
	}
	if path == currentPath && identity == currentIdentity && base == currentBase {
		return true
	}
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
	if err != nil || version == 0 || version > value.TicketVersion || path != value.Worktree.Path || identity != string(value.Worktree.IdentityJSON) || base != value.Worktree.BaseSHA {
		return false
	}
	return currentPath == completion.Worktree.Path && currentIdentity == string(completion.Worktree.IdentityJSON) && currentBase == completion.Worktree.BaseSHA
}

// HistoricalProviderWorktree returns the authenticated registration belonging
// to an immutable provider result. It does not make that result reusable at a
// current fence or authorize a process in the historical checkout identity.
func (s *Store) HistoricalProviderWorktree(ctx context.Context, key ProviderAttemptResultKey) (StoredWorktree, error) {
	var result StoredWorktree
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(q *sql.Conn) error {
		provider, _, err := s.loadHistoricalProviderAttemptResult(ctx, q, key)
		if err != nil || !evidenceWorktreeMatchesAt(ctx, q, key.Ref, provider.Claim.ExpectedVersion, provider.Claim.Worktree, provider.Claim.WorktreeIdentity, provider.Claim.BaseSHA) {
			return ErrEvidenceConflict
		}
		if err := q.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha,head_sha,ticket_version,leader_epoch,runner_epoch FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket).Scan(&result.Path, &result.Branch, &result.State, &result.IdentityJSON, &result.BaseSHA, &result.HeadSHA, &result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch); err != nil {
			return err
		}
		if result.BaseSHA != provider.Claim.BaseSHA || string(result.IdentityJSON) != provider.Claim.WorktreeIdentity {
			value, _, err := protectedBaseRefreshForTicketAt(ctx, q, key.Ref)
			if err != nil {
				return err
			}
			result = value.Worktree
		}
		return nil
	})
	return result, err
}

func (s *Store) ProtectedBaseRefreshBuildContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (ProtectedBaseRefreshBuildContext, error) {
	var result ProtectedBaseRefreshBuildContext
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		var err error
		result, err = s.protectedBaseRefreshBuildContextAt(ctx, conn, ref, version, fence)
		return err
	})
	return result, err
}

func (s *Store) protectedBaseRefreshBuildContextAt(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version uint64, fence domain.Fence) (ProtectedBaseRefreshBuildContext, error) {
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
	if err != nil {
		return ProtectedBaseRefreshBuildContext{}, err
	}
	var currentVersion, currentRunner, leader uint64
	var state domain.State
	var source, path, identity, base, head string
	if err := q.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch,t.source_digest,w.path,w.identity_json,w.base_sha,w.head_sha FROM tickets t JOIN daemon_instances d ON d.channel=t.channel JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &currentVersion, &currentRunner, &leader, &source, &path, &identity, &base, &head); err != nil || state != domain.StateBuilding || currentVersion != version || currentRunner != fence.RunnerEpoch || leader != fence.LeaderEpoch || fence.ClaimEpoch != 0 || source != value.SourceDigest || path != completion.Worktree.Path || identity != string(completion.Worktree.IdentityJSON) || base != completion.Worktree.BaseSHA || head != completion.Worktree.HeadSHA {
		return ProtectedBaseRefreshBuildContext{}, ErrEvidenceConflict
	}
	if err := validateRunnerRecoveryLedger(ctx, q, ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
		return ProtectedBaseRefreshBuildContext{}, ErrEvidenceConflict
	}
	verification, err := s.verificationEvidenceForIdentityFrom(ctx, q, ref, value.Candidate.Snapshot.VerificationIntentDigest, value.Candidate.Snapshot.ProofDigest, "")
	if err != nil || !equalStringSlices(verification.Revision.OwnedFiles, value.ProtectedPaths) {
		return ProtectedBaseRefreshBuildContext{}, ErrEvidenceConflict
	}
	// A later amendment or verification revision needs its own authority; the
	// original refresh cannot silently authorize replacement test contracts.
	var revision uint64
	var intent, proof []byte
	if err := q.QueryRowContext(ctx, `SELECT r.revision,r.intent_bytes,r.proof_bytes FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision WHERE v.channel=? AND v.project_id=? AND v.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&revision, &intent, &proof); err != nil || revision != verification.Revision.Revision || !bytes.Equal(intent, verification.Intent) || !bytes.Equal(proof, verification.Proof) {
		return ProtectedBaseRefreshBuildContext{}, ErrEvidenceConflict
	}
	return ProtectedBaseRefreshBuildContext{Completion: completion, Predecessor: value.Candidate, Verification: verification}, nil
}

// protectedBaseRefreshCandidateAt authenticates the one fresh child of the
// refresh anchor. The old candidate's Builder cannot satisfy this boundary:
// the new result must be issued after completion with the new full worktree
// identity, and both result and candidate endpoints need an exact ledger.
func protectedBaseRefreshCandidateAt(ctx context.Context, q candidateEvidenceQuerier, candidate StoredCandidate, builder ProviderAttemptResult) (ProtectedBaseRefreshCompletion, error) {
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, q, builder.Claim.Ref)
	if err != nil {
		return ProtectedBaseRefreshCompletion{}, err
	}
	if candidate.Snapshot.Generation != value.Candidate.Snapshot.Generation+1 {
		return ProtectedBaseRefreshCompletion{}, ErrNotFound
	}
	if candidate.BuilderResult.AttemptID != builder.Claim.ID || candidate.BuilderResult == value.Candidate.BuilderResult || candidate.Snapshot.BaseSHA != value.NewBaseSHA || candidate.Snapshot.SourceDigest != value.SourceDigest || candidate.Snapshot.VerificationIntentDigest != value.Candidate.Snapshot.VerificationIntentDigest || candidate.Snapshot.ProofDigest != value.Candidate.Snapshot.ProofDigest || candidate.Commit.ParentOID != completion.Preparation.CommitOID || builder.Claim.Phase != domain.PhaseBuild || builder.Claim.Role != "builder" || builder.Claim.Worktree != completion.Worktree.Path || builder.Claim.WorktreeIdentity != string(completion.Worktree.IdentityJSON) || builder.Claim.BaseSHA != value.NewBaseSHA || builder.Claim.ExpectedVersion < completion.Version {
		return ProtectedBaseRefreshCompletion{}, ErrEvidenceConflict
	}
	if err := validateRunnerRecoveryLedgerPrefix(ctx, q, value.Ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, builder.Claim.ExpectedVersion, builder.Claim.RunnerEpoch, builder.Claim.LeaderEpoch); err != nil {
		return ProtectedBaseRefreshCompletion{}, ErrEvidenceConflict
	}
	if err := providerResultReachesHistoricalFence(ctx, q, candidate.BuilderResult, builder, candidate.TicketVersion, candidate.Fence); err != nil {
		return ProtectedBaseRefreshCompletion{}, ErrEvidenceConflict
	}
	return completion, nil
}
