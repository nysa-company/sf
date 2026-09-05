package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

var _ contracts.GitBaseRefreshPreparationLease = (*gitMutationLease)(nil)
var _ contracts.GitBaseRefreshPreparedReader = (*gitMutationLease)(nil)

type baseRefreshRowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadProtectedBaseRefreshReservationAt(ctx context.Context, q baseRefreshRowQueryer, key string) (int64, protectedBaseRefreshIntent, GitMutationIntent, error) {
	var id int64
	var payload []byte
	var digest, created string
	if err := q.QueryRowContext(ctx, `SELECT refresh_id,intent_json,intent_digest,created_at FROM protected_base_refresh_intents WHERE refresh_effect_semantic_key=?`, key).Scan(&id, &payload, &digest, &created); err != nil {
		return 0, protectedBaseRefreshIntent{}, GitMutationIntent{}, ErrEvidenceConflict
	}
	value, err := decodeProtectedBaseRefreshIntent(payload, digest)
	if err != nil {
		return 0, protectedBaseRefreshIntent{}, GitMutationIntent{}, err
	}
	if _, err := time.Parse(time.RFC3339Nano, created); err != nil {
		return 0, protectedBaseRefreshIntent{}, GitMutationIntent{}, ErrEvidenceConflict
	}
	proof, err := protectedBaseRefreshProofFor(value)
	if err != nil || proof.Intent.SemanticKey != value.BaseProofSemanticKey {
		return 0, protectedBaseRefreshIntent{}, GitMutationIntent{}, ErrEvidenceConflict
	}
	mutation := protectedBaseRefreshMutationFor(value, digest)
	if mutation.SemanticKey != key || protectedBaseRefreshProjectionMatchesAt(ctx, q, id, value, digest, proof, mutation) != nil {
		return 0, protectedBaseRefreshIntent{}, GitMutationIntent{}, ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, q, proof.Intent.SemanticKey)
	if err != nil || !sameGitMutationBinding(proof.Intent, facts.Claim) || facts.Claim.TicketVersion != value.TicketVersion || facts.Claim.LeaderEpoch != value.Fence.LeaderEpoch || facts.Claim.RunnerEpoch != value.Fence.RunnerEpoch || facts.Effect.State != EffectConfirmed || facts.ObservedIdentity != proof.ObservedIdentity {
		return 0, protectedBaseRefreshIntent{}, GitMutationIntent{}, ErrEvidenceConflict
	}
	return id, value, mutation, nil
}

func (s *Store) authenticateProtectedBaseRefreshMutationAt(ctx context.Context, conn *sql.Conn, intent GitMutationIntent) error {
	_, stored, expected, err := loadProtectedBaseRefreshReservationAt(ctx, conn, intent.SemanticKey)
	if err != nil {
		return ErrGitMutationIntent
	}
	expected.TicketVersion, expected.Fence = intent.TicketVersion, intent.Fence
	if intent != expected || intent.Fence.ClaimEpoch != 0 || validateRunnerRecoveryLedger(ctx, conn, intent.Ref, stored.TicketVersion, stored.Fence.RunnerEpoch, stored.Fence.LeaderEpoch, intent.TicketVersion, intent.Fence.RunnerEpoch, intent.Fence.LeaderEpoch) != nil {
		return ErrGitMutationIntent
	}
	current, err := s.protectedBaseRefreshContextAt(ctx, conn, intent.Ref, intent.TicketVersion, intent.Fence, intent.ExpectedBaseOID)
	if err != nil {
		return ErrGitMutationIntent
	}
	current.BaseProofSemanticKey = stored.BaseProofSemanticKey
	current.TicketVersion, current.Fence = stored.TicketVersion, stored.Fence
	currentPayload, _, err := canonicalProtectedBaseRefreshIntent(current)
	storedPayload, _, storedErr := canonicalProtectedBaseRefreshIntent(stored)
	if err != nil || storedErr != nil || !bytes.Equal(currentPayload, storedPayload) {
		return ErrGitMutationIntent
	}
	return nil
}

type protectedBaseRefreshPrepared struct {
	Format       string
	RefreshID    int64
	IntentDigest string
	CommitOID    string
	TreeOID      string
	Parents      [2]string
}

func protectedBaseRefreshPreparedDigest(id int64, digest, commit, tree string, parents [2]string) (string, error) {
	if id <= 0 || !validCIAuthorityDigest(digest) || commit == "" || tree == "" || parents[0] == "" || parents[1] == "" || parents[0] == parents[1] || commit == parents[0] || commit == parents[1] || !validGitOIDWidth(parents[0], parents[1], commit, tree) {
		return "", ErrGitMutationLease
	}
	payload, err := json.Marshal(protectedBaseRefreshPrepared{Format: "sf.protected-base-refresh.prepared.v1", RefreshID: id, IntentDigest: digest, CommitOID: commit, TreeOID: tree, Parents: parents})
	if err != nil {
		return "", ErrGitMutationLease
	}
	return ciAuthorityDigest(payload), nil
}

func (l *gitMutationLease) RecordBaseRefreshPreparation(ctx context.Context, commit, tree string, parents [2]string) error {
	if l == nil || l.store == nil || l.claim.Operation != "refresh-base" || parents != [2]string{l.claim.ExpectedHeadOID, l.claim.ExpectedBaseOID} {
		return ErrGitMutationLease
	}
	return l.store.write(ctx, func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(ctx, conn, l.claim); err != nil {
			return ErrGitMutationLease
		}
		id, value, mutation, err := loadProtectedBaseRefreshReservationAt(ctx, conn, l.claim.SemanticKey)
		if err != nil {
			return ErrGitMutationLease
		}
		digest, err := protectedBaseRefreshPreparedDigest(id, mutation.RequestDigest, commit, tree, parents)
		if err != nil {
			return err
		}
		var count int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='refresh-base'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&count); err != nil || count != 1 {
			return ErrGitMutationLease
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO protected_base_refresh_preparations(refresh_id,channel,project_id,ticket_id,old_candidate_head_sha,new_base_sha,prepared_commit_oid,prepared_tree_oid,prepared_parent_1_oid,prepared_parent_2_oid,prepared_digest,prepared_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(refresh_id) DO NOTHING`, id, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, parents[0], parents[1], commit, tree, parents[0], parents[1], digest, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if err := validateProtectedBaseRefreshPreparationAt(ctx, conn, id, value, mutation.RequestDigest, commit, tree); err != nil {
			return err
		}
		for _, query := range []string{
			`UPDATE git_mutation_intents SET prepared_commit_oid=?,prepared_tree_oid=? WHERE semantic_key=? AND operation='refresh-base' AND ((prepared_commit_oid='' AND prepared_tree_oid='') OR (prepared_commit_oid=? AND prepared_tree_oid=?))`,
			`UPDATE git_mutation_leases SET prepared_commit_oid=?,prepared_tree_oid=? WHERE semantic_key=? AND operation='refresh-base' AND state='active' AND ((prepared_commit_oid='' AND prepared_tree_oid='') OR (prepared_commit_oid=? AND prepared_tree_oid=?))`,
		} {
			updated, err := conn.ExecContext(ctx, query, commit, tree, l.claim.SemanticKey, commit, tree)
			if err != nil {
				return err
			}
			if n, _ := updated.RowsAffected(); n != 1 {
				return ErrGitMutationLease
			}
		}
		return nil
	})
}

func (l *gitMutationLease) PreparedBaseRefresh(ctx context.Context) (contracts.GitBaseRefreshPreparation, bool, error) {
	var result contracts.GitBaseRefreshPreparation
	var found bool
	if l == nil || l.store == nil || l.claim.Operation != "refresh-base" {
		return result, false, ErrGitMutationLease
	}
	err := l.store.write(ctx, func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(ctx, conn, l.claim); err != nil {
			return err
		}
		var commit, tree string
		if err := conn.QueryRowContext(ctx, `SELECT prepared_commit_oid,prepared_tree_oid FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='refresh-base'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&commit, &tree); err != nil {
			return ErrGitMutationLease
		}
		facts, err := gitMutationIntentFactsFrom(ctx, conn, l.claim.SemanticKey)
		if err != nil || facts.Claim != l.claim {
			return ErrGitMutationLease
		}
		// A fresh nonce can observe the immutable preparation retained after an
		// earlier lease drained. It must not adopt a partially diverged lease.
		if (commit != "" || tree != "") && (commit != facts.PreparedCommitOID || tree != facts.PreparedTreeOID) {
			return ErrGitMutationLease
		}
		if facts.PreparedCommitOID == "" {
			return nil
		}
		result = contracts.GitBaseRefreshPreparation{CommitOID: facts.PreparedCommitOID, TreeOID: facts.PreparedTreeOID, Parents: [2]string{l.claim.ExpectedHeadOID, l.claim.ExpectedBaseOID}}
		found = true
		return nil
	})
	if err != nil {
		return contracts.GitBaseRefreshPreparation{}, false, err
	}
	return result, found, nil
}

func validateProtectedBaseRefreshPreparationAt(ctx context.Context, q baseRefreshRowQueryer, id int64, value protectedBaseRefreshIntent, intentDigest, commit, tree string) error {
	var storedCommit, storedTree, parent1, parent2, digest, created string
	var ref = value.Ref
	err := q.QueryRowContext(ctx, `SELECT prepared_commit_oid,prepared_tree_oid,prepared_parent_1_oid,prepared_parent_2_oid,prepared_digest,prepared_at FROM protected_base_refresh_preparations WHERE refresh_id=? AND channel=? AND project_id=? AND ticket_id=? AND old_candidate_head_sha=? AND new_base_sha=?`, id, ref.Channel, ref.Project, ref.Ticket, value.Candidate.Snapshot.HeadSHA, value.NewBaseSHA).Scan(&storedCommit, &storedTree, &parent1, &parent2, &digest, &created)
	if errors.Is(err, sql.ErrNoRows) && commit == "" && tree == "" {
		return nil
	}
	if err != nil || commit == "" || tree == "" || storedCommit != commit || storedTree != tree || [2]string{parent1, parent2} != [2]string{value.Candidate.Snapshot.HeadSHA, value.NewBaseSHA} {
		return ErrGitMutationLease
	}
	expected, err := protectedBaseRefreshPreparedDigest(id, intentDigest, commit, tree, [2]string{parent1, parent2})
	if err != nil || expected != digest {
		return ErrGitMutationLease
	}
	if _, err := time.Parse(time.RFC3339Nano, created); err != nil {
		return ErrGitMutationLease
	}
	return nil
}
