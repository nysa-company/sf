package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

// ProtectedBaseRefreshCompletion retains both the immutable old authority and
// the new checkout anchor. It is not a candidate: fresh Builder and repository
// command evidence must still authorize the next candidate generation.
type ProtectedBaseRefreshCompletion struct {
	RefreshID    int64
	IntentDigest string
	Preparation  contracts.GitBaseRefreshPreparation
	Worktree     StoredWorktree
	Version      uint64
	Fence        domain.Fence
	Digest       string
}

func protectedBaseRefreshEffectiveIdentity(value protectedBaseRefreshIntent, observed []byte) ([]byte, error) {
	var old, actual gitboundary.Identity
	if json.Unmarshal(value.Worktree.IdentityJSON, &old) != nil || json.Unmarshal(observed, &actual) != nil {
		return nil, ErrEvidenceConflict
	}
	old.BaseHead = value.NewBaseSHA
	canonical, err := json.Marshal(old)
	if err != nil || actual != old || !bytes.Equal(canonical, observed) || !validRepositoryWorktreeIdentity(string(canonical), value.Repository, value.Worktree.Path, value.Worktree.Branch, value.BaseRef, value.NewBaseSHA) {
		return nil, ErrEvidenceConflict
	}
	return canonical, nil
}

func protectedBaseRefreshCompletionDigest(value ProtectedBaseRefreshCompletion) (string, error) {
	value.Digest = ""
	payload, err := json.Marshal(struct {
		Format     string
		Completion ProtectedBaseRefreshCompletion
	}{"sf.protected-base-refresh.completion.v1", value})
	if err != nil || len(payload) > maxBaseRefreshPayload {
		return "", ErrEvidenceConflict
	}
	return ciAuthorityDigest(payload), nil
}

// CompleteProtectedBaseRefresh accepts only the exact confirmed refresh claim
// and the full identity observed by Git after its paired ref CAS. It changes
// the effective registration, appends the completion and enters Building in
// one transaction. It never edits old verification/provider/candidate rows.
// Restart recovery must supply a separately authenticated reclaimed claim;
// the immutable reservation cannot be updated by guessing a counter advance.
func (s *Store) CompleteProtectedBaseRefresh(ctx context.Context, claim contracts.GitMutationClaim, identity []byte) (ProtectedBaseRefreshCompletion, error) {
	if claim.Operation != "refresh-base" || len(identity) == 0 || len(identity) > maxBaseRefreshPayload {
		return ProtectedBaseRefreshCompletion{}, ErrEvidenceConflict
	}
	// Hold the launch gate through COMMIT. Unlike a normal one-way transition,
	// this API also replays a completion: draining the *current* endpoint on a
	// replay would revoke the already-entered Builder without advancing it.
	if err := s.mutations.lock(ctx); err != nil {
		return ProtectedBaseRefreshCompletion{}, err
	}
	defer s.mutations.unlock()
	var result ProtectedBaseRefreshCompletion
	err := s.write(ctx, func(conn *sql.Conn) error {
		id, value, mutation, err := loadProtectedBaseRefreshReservationAt(ctx, conn, claim.SemanticKey)
		if err != nil || !sameGitMutationBinding(mutation, claim) || claim.TicketVersion < value.TicketVersion || claim.TicketVersion == ^uint64(0) {
			return ErrEvidenceConflict
		}
		canonical, err := protectedBaseRefreshEffectiveIdentity(value, identity)
		if err != nil {
			return err
		}
		facts, err := gitMutationIntentFactsFrom(ctx, conn, claim.SemanticKey)
		if err != nil || facts.Claim != claim || facts.Effect.State != EffectConfirmed || facts.PreparedCommitOID == "" || facts.ObservedIdentity != facts.PreparedCommitOID || facts.Effect.ClaimEpoch != claim.ClaimEpoch || facts.Effect.TicketVersion != claim.TicketVersion || facts.Effect.LeaderEpoch != claim.LeaderEpoch || facts.Effect.RunnerEpoch != claim.RunnerEpoch {
			return ErrEvidenceConflict
		}
		if err := protectedBaseRefreshDrainedAt(ctx, conn, value, mutation); err != nil {
			return err
		}
		if existing, found, err := loadProtectedBaseRefreshCompletionAt(ctx, conn, claim.SemanticKey); err != nil {
			return err
		} else if found {
			if !bytes.Equal(existing.Worktree.IdentityJSON, canonical) {
				return ErrEvidenceConflict
			}
			if err := s.assertTicketFence(ctx, conn, claim.TicketRef, existing.Version, existing.Fence); err != nil {
				return err
			}
			if err := protectedBaseRefreshCurrentProjectionAt(ctx, conn, claim.TicketRef, existing); err != nil {
				return err
			}
			result = existing
			return nil
		}
		mutation.TicketVersion, mutation.Fence = claim.TicketVersion, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}
		if err := s.authenticateProtectedBaseRefreshMutationAt(ctx, conn, mutation); err != nil {
			return err
		}
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).Scan(&state); err != nil {
			return err
		}
		result = ProtectedBaseRefreshCompletion{RefreshID: id, IntentDigest: mutation.RequestDigest,
			Preparation: contracts.GitBaseRefreshPreparation{CommitOID: facts.PreparedCommitOID, TreeOID: facts.PreparedTreeOID, Parents: [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID}},
			Worktree:    value.Worktree, Version: claim.TicketVersion + 1, Fence: mutation.Fence}
		result.Worktree.BaseSHA, result.Worktree.HeadSHA, result.Worktree.IdentityJSON = value.NewBaseSHA, facts.PreparedCommitOID, canonical
		result.Worktree.TicketVersion, result.Worktree.Fence = result.Version, result.Fence
		result.Digest, err = protectedBaseRefreshCompletionDigest(result)
		if err != nil {
			return err
		}
		created := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := conn.ExecContext(ctx, `INSERT INTO protected_base_refresh_completions(refresh_id,channel,project_id,ticket_id,prepared_commit_oid,prepared_tree_oid,prepared_parent_1_oid,prepared_parent_2_oid,effective_base_sha,effective_worktree_identity_json,effective_worktree_identity_digest,completion_ticket_version,completion_leader_epoch,completion_runner_epoch,completion_digest,completed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, result.Preparation.CommitOID, result.Preparation.TreeOID, result.Preparation.Parents[0], result.Preparation.Parents[1], result.Worktree.BaseSHA, canonical, ciAuthorityDigest(canonical), result.Version, result.Fence.LeaderEpoch, result.Fence.RunnerEpoch, result.Digest, created); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE worktrees SET base_sha=?,head_sha=?,identity_json=?,ticket_version=?,leader_epoch=?,runner_epoch=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='registered' AND path=? AND branch_ref=? AND base_sha=? AND head_sha=? AND identity_json=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, result.Worktree.BaseSHA, result.Worktree.HeadSHA, string(canonical), result.Version, result.Fence.LeaderEpoch, result.Fence.RunnerEpoch, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, value.Worktree.Path, value.Worktree.Branch, value.Worktree.BaseSHA, value.Worktree.HeadSHA, string(value.Worktree.IdentityJSON), value.Worktree.TicketVersion, value.Worktree.Fence.LeaderEpoch, value.Worktree.Fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrEvidenceConflict
		}
		if err := advanceOpenRuntimeAuthority(ctx, conn, claim.TicketRef, claim.TicketVersion, mutation.Fence); err != nil {
			return err
		}
		updated, err = conn.ExecContext(ctx, `UPDATE tickets SET state='building',resume_state=NULL,blocked_code='',version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, state, claim.TicketVersion, claim.RunnerEpoch)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		payload, _ := json.Marshal(struct {
			RefreshID        int64
			CompletionDigest string
		}{id, result.Digest})
		event, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,'base_or_candidate_head_changed',?,'building',?,?)`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, result.Version, state, string(payload), created)
		if err != nil {
			return err
		}
		eventID, err := event.LastInsertId()
		if err != nil || eventID <= 0 {
			return ErrEvidenceConflict
		}
		return recordProviderPhaseEntry(ctx, conn, claim.TicketRef, domain.PhaseBuild, result.Version, result.Fence.LeaderEpoch, result.Fence.RunnerEpoch, eventID, created, state, domain.StateBuilding, "base_or_candidate_head_changed")
	})
	if err != nil {
		return ProtectedBaseRefreshCompletion{}, err
	}
	return result, nil
}

func protectedBaseRefreshCurrentProjectionAt(ctx context.Context, q baseRefreshRowQueryer, ref domain.TicketRef, value ProtectedBaseRefreshCompletion) error {
	var count int
	w := value.Worktree
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM tickets t JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id WHERE t.channel=? AND t.project_id=? AND t.id=? AND t.state='building' AND t.version=? AND t.runner_epoch=? AND w.path=? AND w.branch_ref=? AND w.state='registered' AND w.base_sha=? AND w.head_sha=? AND w.identity_json=? AND w.ticket_version=? AND w.leader_epoch=? AND w.runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, value.Version, value.Fence.RunnerEpoch, w.Path, w.Branch, w.BaseSHA, w.HeadSHA, string(w.IdentityJSON), w.TicketVersion, w.Fence.LeaderEpoch, w.Fence.RunnerEpoch).Scan(&count)
	if err != nil || count != 1 {
		return ErrEvidenceConflict
	}
	return nil
}

func loadProtectedBaseRefreshCompletionAt(ctx context.Context, q baseRefreshRowQueryer, key string) (ProtectedBaseRefreshCompletion, bool, error) {
	id, value, mutation, err := loadProtectedBaseRefreshReservationAt(ctx, q, key)
	if err != nil {
		return ProtectedBaseRefreshCompletion{}, false, err
	}
	result := ProtectedBaseRefreshCompletion{RefreshID: id, IntentDigest: mutation.RequestDigest, Worktree: value.Worktree}
	var identityDigest, created string
	err = q.QueryRowContext(ctx, `SELECT prepared_commit_oid,prepared_tree_oid,prepared_parent_1_oid,prepared_parent_2_oid,effective_base_sha,effective_worktree_identity_json,effective_worktree_identity_digest,completion_ticket_version,completion_leader_epoch,completion_runner_epoch,completion_digest,completed_at FROM protected_base_refresh_completions WHERE refresh_id=? AND channel=? AND project_id=? AND ticket_id=?`, id, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket).Scan(&result.Preparation.CommitOID, &result.Preparation.TreeOID, &result.Preparation.Parents[0], &result.Preparation.Parents[1], &result.Worktree.BaseSHA, &result.Worktree.IdentityJSON, &identityDigest, &result.Version, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &result.Digest, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return ProtectedBaseRefreshCompletion{}, false, nil
	}
	if err != nil {
		return ProtectedBaseRefreshCompletion{}, false, err
	}
	result.Worktree.HeadSHA, result.Worktree.TicketVersion, result.Worktree.Fence = result.Preparation.CommitOID, result.Version, result.Fence
	if result.Version <= value.TicketVersion || result.Worktree.BaseSHA != value.NewBaseSHA || result.Preparation.Parents != [2]string{value.Candidate.Snapshot.HeadSHA, value.NewBaseSHA} || identityDigest != ciAuthorityDigest(result.Worktree.IdentityJSON) {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	if _, err := protectedBaseRefreshEffectiveIdentity(value, result.Worktree.IdentityJSON); err != nil {
		return ProtectedBaseRefreshCompletion{}, false, err
	}
	if err := validateProtectedBaseRefreshPreparationAt(ctx, q, id, value, mutation.RequestDigest, result.Preparation.CommitOID, result.Preparation.TreeOID); err != nil {
		return ProtectedBaseRefreshCompletion{}, false, err
	}
	facts, err := gitMutationIntentFactsFrom(ctx, q, key)
	if err != nil || facts.Effect.State != EffectConfirmed || facts.Claim.TicketVersion+1 != result.Version || facts.Claim.LeaderEpoch != result.Fence.LeaderEpoch || facts.Claim.RunnerEpoch != result.Fence.RunnerEpoch || facts.ObservedIdentity != result.Preparation.CommitOID || facts.PreparedCommitOID != result.Preparation.CommitOID || facts.PreparedTreeOID != result.Preparation.TreeOID {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	ledger, ok := q.(candidateEvidenceQuerier)
	if !ok || validateRunnerRecoveryLedgerPrefix(ctx, ledger, value.Ref, value.TicketVersion, value.Fence.RunnerEpoch, value.Fence.LeaderEpoch, result.Version-1, result.Fence.RunnerEpoch, result.Fence.LeaderEpoch) != nil {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	digest, err := protectedBaseRefreshCompletionDigest(result)
	if err != nil || digest != result.Digest {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	if _, err := time.Parse(time.RFC3339Nano, created); err != nil {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	payload, _ := json.Marshal(struct {
		RefreshID        int64
		CompletionDigest string
	}{id, result.Digest})
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='base_or_candidate_head_changed' AND from_state IN ('publishing','waiting_ci','reviewing','waiting_approval') AND to_state='building' AND payload=? AND created_at=?`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, result.Version, string(payload), created).Scan(&count); err != nil || count != 1 {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, result.Version).Scan(&count); err != nil || count != 1 {
		return ProtectedBaseRefreshCompletion{}, false, ErrEvidenceConflict
	}
	return result, true, nil
}
