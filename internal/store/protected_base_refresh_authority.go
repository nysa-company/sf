package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// ProtectedBaseRefreshProof is a proposal for the narrow protected-ref fetch,
// not permission to change a checkout or to enter building. The normal effect
// and Git claim APIs must fence its launch. The refresh writer must authenticate
// the resulting exact-tip observation before it reserves the single refresh.
type ProtectedBaseRefreshProof struct {
	Intent           GitMutationIntent
	Worktree         StoredWorktree
	ContextDigest    string
	ObservedIdentity string
}

// ProtectedBaseRefreshProofIntent derives every old-input field from SQLite in
// one transaction. The only external input is a proposed distinct protected
// tip; Git must subsequently prove it is the exact fresh tip, not an ancestor.
func (s *Store) ProtectedBaseRefreshProofIntent(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, newBase string) (ProtectedBaseRefreshProof, error) {
	var result ProtectedBaseRefreshProof
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
		result = proof
		return nil
	})
	if err != nil {
		return ProtectedBaseRefreshProof{}, err
	}
	return result, nil
}

// This helper also belongs on the refresh writer's connection: a successful
// earlier read is never sufficient authority after Git has released its lease.
func (s *Store) protectedBaseRefreshContextAt(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, newBase string) (protectedBaseRefreshIntent, error) {
	var value protectedBaseRefreshIntent
	if ref.Validate() != nil || fence.ClaimEpoch != 0 || !validStoreOID(newBase) {
		return value, ErrEvidenceConflict
	}
	if err := s.assertTicketFence(ctx, conn, ref, version, fence); err != nil {
		return value, err
	}
	var state domain.State
	var mode domain.MergeMode
	var kind domain.TicketType
	var snapshot, registeredSnapshot []byte
	var registeredDigest string
	value.Format, value.Ref, value.TicketVersion, value.Fence, value.NewBaseSHA = "sf.protected-base-refresh.v1", ref, version, fence, newBase
	err := conn.QueryRowContext(ctx, `SELECT t.state,t.merge_mode,t.ticket_type,t.source_digest,t.config_generation,t.config_digest,t.config_snapshot_bytes,c.digest,c.snapshot_bytes,p.canonical_path,p.base_ref
		FROM tickets t JOIN projects p ON p.channel=t.channel AND p.id=t.project_id
		JOIN project_configurations c ON c.channel=t.channel AND c.project_id=t.project_id AND c.generation=t.config_generation
		WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).
		Scan(&state, &mode, &kind, &value.SourceDigest, &value.ConfigGeneration, &value.ConfigDigest, &snapshot, &registeredDigest, &registeredSnapshot, &value.Repository, &value.BaseRef)
	if err != nil || mode != domain.MergeGuarded || kind == domain.TicketSpike || len(snapshot) == 0 || value.ConfigDigest != registeredDigest || !bytes.Equal(snapshot, registeredSnapshot) {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	value.ConfigSnapshotDigest = sha256Digest(snapshot)
	// Once merge is claimed, base movement belongs to exact merge observation,
	// never to a rewrite. Manual-mode external merge races are likewise outside
	// this first guarded refresh authority.
	switch state {
	case domain.StatePublishing:
		if err := s.authenticatePostPublicationState(ctx, conn, ref, state, version, fence); err != nil {
			return protectedBaseRefreshIntent{}, ErrPublicationEvidence
		}
	case domain.StateReviewing:
		candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
		if err != nil {
			return protectedBaseRefreshIntent{}, ErrPublicationEvidence
		}
		observation, reviewVersion, err := s.authenticateHistoricalFinalReview(ctx, conn, ref, candidate)
		if err != nil || authenticateCurrentPostPublicationEndpointBridge(ctx, conn, ref, state,
			normalRecoveryEndpoint{version: reviewVersion, runner: observation.ObservedFence.RunnerEpoch, leader: observation.ObservedFence.LeaderEpoch},
			normalRecoveryEndpoint{version: version, runner: fence.RunnerEpoch, leader: fence.LeaderEpoch}) != nil {
			return protectedBaseRefreshIntent{}, ErrPublicationEvidence
		}
	case domain.StateWaitingApproval:
		// Recovery advances the live endpoint without minting another review
		// pass. Authenticate the immutable completion and its exact signed
		// suffix instead of requiring a review event at the recovered version.
		prior, err := s.finalReviewRecoveryEndpoint(ctx, conn, ref, state)
		if err != nil || authenticateCurrentPostPublicationEndpointBridge(ctx, conn, ref, state, prior,
			normalRecoveryEndpoint{version: version, runner: fence.RunnerEpoch, leader: fence.LeaderEpoch}) != nil {
			return protectedBaseRefreshIntent{}, ErrPublicationEvidence
		}
	case domain.StateWaitingCI:
		if _, err := loadCICurrentPublication(ctx, conn, ref); err != nil {
			return protectedBaseRefreshIntent{}, ErrPublicationEvidence
		}
	default:
		return protectedBaseRefreshIntent{}, ErrPublicationEvidence
	}
	value.Candidate, err = s.latestCandidateFrom(ctx, conn, ref, false)
	if err != nil {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	if err := s.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, conn, ref, value.Candidate); err != nil {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	verification, err := s.verificationEvidenceForIdentityFrom(ctx, conn, ref, value.Candidate.Snapshot.VerificationIntentDigest, value.Candidate.Snapshot.ProofDigest, "")
	if err != nil {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	value.ProtectedPaths = append([]string(nil), verification.Revision.OwnedFiles...)
	err = conn.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha,head_sha,ticket_version,leader_epoch,runner_epoch FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).
		Scan(&value.Worktree.Path, &value.Worktree.Branch, &value.Worktree.State, &value.Worktree.IdentityJSON, &value.Worktree.BaseSHA, &value.Worktree.HeadSHA, &value.Worktree.TicketVersion, &value.Worktree.Fence.LeaderEpoch, &value.Worktree.Fence.RunnerEpoch)
	if err != nil {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	// A fixed domain tag avoids a circular digest dependency: the proof's key is
	// derived from this canonical context, then the final refresh intent binds
	// that key. It is not a caller-controlled semantic key.
	value.BaseProofSemanticKey = "sf.protected-base-refresh.exact-tip-proof.v1"
	if _, _, err := canonicalProtectedBaseRefreshIntent(value); err != nil {
		return protectedBaseRefreshIntent{}, err
	}
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != 0 {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	// A PR mutation without its immutable publication witness is an unresolved
	// ownership/merge-observation boundary, even if the effect was confirmed.
	// Do not rewrite a branch whose numbered external target is unavailable.
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil {
		return protectedBaseRefreshIntent{}, err
	}
	if count == 0 {
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind IN ('draft_pr','pr_edit','pr_ready','merge') AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != 0 {
			return protectedBaseRefreshIntent{}, ErrControlNotDrained
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind IN ('draft_pr','pr_edit','pr_ready','merge')`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != 0 {
			return protectedBaseRefreshIntent{}, ErrEvidenceConflict
		}
	}
	return value, nil
}

func protectedBaseRefreshProofFor(value protectedBaseRefreshIntent) (ProtectedBaseRefreshProof, error) {
	value.BaseProofSemanticKey = "sf.protected-base-refresh.exact-tip-proof.v1"
	_, digest, err := canonicalProtectedBaseRefreshIntent(value)
	if err != nil {
		return ProtectedBaseRefreshProof{}, err
	}
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: value.Ref, TicketVersion: value.TicketVersion, Fence: value.Fence}, RequestDigest: digest,
		Repository: value.Repository, Worktree: value.Worktree.Path, Branch: value.Worktree.Branch, Operation: "protected-ref-fetch", BaseRef: value.BaseRef,
		ExpectedBaseOID: value.Candidate.Snapshot.BaseSHA, ExpectedHeadOID: value.NewBaseSHA}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if !validGitIntent(intent) {
		return ProtectedBaseRefreshProof{}, ErrEvidenceConflict
	}
	return ProtectedBaseRefreshProof{Intent: intent, Worktree: value.Worktree, ContextDigest: digest,
		ObservedIdentity: "base-refresh/exact-tip/v1/" + value.Candidate.Snapshot.BaseSHA + "/" + value.NewBaseSHA + "/" + digest}, nil
}

// ProtectedBaseRefresh is the immutable pre-mutation reservation. Merely
// returning it does not change ticket state, worktree registration or Git refs.
// The dedicated refresh mutation remains responsible for authenticating the
// physical checkout and recording its prepared object before any ref CAS.
type ProtectedBaseRefresh struct {
	ID            int64
	IntentDigest  string
	IntentPayload []byte
	Mutation      GitMutationIntent
	Candidate     StoredCandidate
	Worktree      StoredWorktree
	NewBaseSHA    string
}

// ReserveProtectedBaseRefresh consumes an exact confirmed proof into a single
// append-only reservation and its planned Git effect, atomically. It accepts no
// caller-supplied candidate, worktree, config, approval or arbitrary Git target.
// A lost response at this unchanged endpoint returns the identical reservation.
func (s *Store) ReserveProtectedBaseRefresh(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, newBase string) (ProtectedBaseRefresh, error) {
	var result ProtectedBaseRefresh
	err := s.write(ctx, func(conn *sql.Conn) error {
		value, err := s.protectedBaseRefreshContextAt(ctx, conn, ref, version, fence, newBase)
		if err != nil {
			return err
		}
		proof, err := protectedBaseRefreshProofFor(value)
		if err != nil {
			return err
		}
		facts, err := gitMutationIntentFactsFrom(ctx, conn, proof.Intent.SemanticKey)
		if err != nil || !sameGitMutationBinding(proof.Intent, facts.Claim) || facts.Claim.TicketVersion != version || facts.Claim.LeaderEpoch != fence.LeaderEpoch || facts.Claim.RunnerEpoch != fence.RunnerEpoch || facts.Effect.State != EffectConfirmed || facts.Effect.TicketVersion != version || facts.Effect.LeaderEpoch != fence.LeaderEpoch || facts.Effect.RunnerEpoch != fence.RunnerEpoch || facts.Effect.ClaimEpoch != facts.Claim.ClaimEpoch || facts.ObservedIdentity != proof.ObservedIdentity {
			return ErrEvidenceConflict
		}
		value.BaseProofSemanticKey = proof.Intent.SemanticKey
		payload, digest, err := canonicalProtectedBaseRefreshIntent(value)
		if err != nil {
			return err
		}
		mutation := protectedBaseRefreshMutationFor(value, digest)
		result = ProtectedBaseRefresh{IntentDigest: digest, IntentPayload: append([]byte(nil), payload...), Mutation: mutation, Candidate: value.Candidate, Worktree: value.Worktree, NewBaseSHA: newBase}
		var storedPayload []byte
		var storedDigest, key, request, baseProofDigest, created string
		err = conn.QueryRowContext(ctx, `SELECT refresh_id,intent_json,intent_digest,refresh_effect_semantic_key,refresh_effect_request_digest,base_proof_digest,created_at FROM protected_base_refresh_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).
			Scan(&result.ID, &storedPayload, &storedDigest, &key, &request, &baseProofDigest, &created)
		if err == nil {
			if _, decodeErr := decodeProtectedBaseRefreshIntent(storedPayload, storedDigest); decodeErr != nil || !bytes.Equal(payload, storedPayload) || digest != storedDigest || key != mutation.SemanticKey || request != digest || baseProofDigest != proof.ContextDigest {
				return ErrEvidenceConflict
			}
			if _, err := time.Parse(time.RFC3339Nano, created); err != nil {
				return ErrEvidenceConflict
			}
			if err := protectedBaseRefreshProjectionMatchesAt(ctx, conn, result.ID, value, digest, proof, mutation); err != nil {
				return err
			}
			effect, err := effectFrom(ctx, conn, key)
			if err != nil || effect.Ref != ref || effect.Kind != "git/refresh-base" || effect.RequestDigest != digest || effect.TicketVersion != version || effect.LeaderEpoch != fence.LeaderEpoch || effect.RunnerEpoch != fence.RunnerEpoch {
				return ErrEvidenceConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := protectedBaseRefreshDrainedAt(ctx, conn, value, proof.Intent); err != nil {
			return err
		}
		// No ON CONFLICT shortcut: an unexpected preexisting key must roll back
		// the whole reservation, never be adopted from an unrelated caller.
		if _, err := conn.ExecContext(ctx, `INSERT INTO effects(semantic_key,channel,project_id,ticket_id,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest) VALUES(?,?,?,?,'git/refresh-base','planned',?,?,?,0,?)`, mutation.SemanticKey, ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch, digest); err != nil {
			return err
		}
		insert, err := conn.ExecContext(ctx, `INSERT INTO protected_base_refresh_intents(channel,project_id,ticket_id,ticket_version,leader_epoch,runner_epoch,source_digest,config_generation,config_digest,config_snapshot_digest,intent_json,intent_digest,old_candidate_generation,old_candidate_head_sha,old_candidate_tree_sha,old_candidate_base_sha,worktree_path,worktree_identity_json,worktree_identity_digest,new_base_sha,base_proof_digest,base_proof_semantic_key,refresh_effect_semantic_key,refresh_effect_request_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch, value.SourceDigest, value.ConfigGeneration, value.ConfigDigest, value.ConfigSnapshotDigest, payload, digest, value.Candidate.Snapshot.Generation, value.Candidate.Snapshot.HeadSHA, value.Candidate.Snapshot.TreeSHA, value.Candidate.Snapshot.BaseSHA, value.Worktree.Path, string(value.Worktree.IdentityJSON), ciAuthorityDigest(value.Worktree.IdentityJSON), newBase, proof.ContextDigest, proof.Intent.SemanticKey, mutation.SemanticKey, digest, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.ID, err = insert.LastInsertId()
		if err != nil || result.ID <= 0 {
			return fmt.Errorf("%w: refresh reservation identity", ErrEvidenceConflict)
		}
		return nil
	})
	if err != nil {
		return ProtectedBaseRefresh{}, err
	}
	return result, nil
}

func protectedBaseRefreshMutationFor(value protectedBaseRefreshIntent, digest string) GitMutationIntent {
	mutation := GitMutationIntent{EffectFence: EffectFence{Ref: value.Ref, TicketVersion: value.TicketVersion, Fence: value.Fence}, RequestDigest: digest, Repository: value.Repository, Worktree: value.Worktree.Path, Branch: value.Worktree.Branch, Operation: "refresh-base", BaseRef: value.BaseRef, ExpectedBaseOID: value.NewBaseSHA, ExpectedHeadOID: value.Candidate.Snapshot.HeadSHA}
	mutation.SemanticKey = CanonicalGitMutationSemanticKey(mutation)
	return mutation
}

func protectedBaseRefreshProjectionMatchesAt(ctx context.Context, conn baseRefreshRowQueryer, id int64, value protectedBaseRefreshIntent, digest string, proof ProtectedBaseRefreshProof, mutation GitMutationIntent) error {
	var matches int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM protected_base_refresh_intents WHERE refresh_id=? AND channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND source_digest=? AND config_generation=? AND config_digest=? AND config_snapshot_digest=? AND intent_digest=? AND old_candidate_generation=? AND old_candidate_head_sha=? AND old_candidate_tree_sha=? AND old_candidate_base_sha=? AND worktree_path=? AND worktree_identity_json=? AND worktree_identity_digest=? AND new_base_sha=? AND base_proof_digest=? AND base_proof_semantic_key=? AND refresh_effect_semantic_key=? AND refresh_effect_request_digest=?`,
		id, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.TicketVersion, value.Fence.LeaderEpoch, value.Fence.RunnerEpoch, value.SourceDigest, value.ConfigGeneration, value.ConfigDigest, value.ConfigSnapshotDigest, digest, value.Candidate.Snapshot.Generation, value.Candidate.Snapshot.HeadSHA, value.Candidate.Snapshot.TreeSHA, value.Candidate.Snapshot.BaseSHA, value.Worktree.Path, string(value.Worktree.IdentityJSON), ciAuthorityDigest(value.Worktree.IdentityJSON), value.NewBaseSHA, proof.ContextDigest, proof.Intent.SemanticKey, mutation.SemanticKey, mutation.RequestDigest).Scan(&matches)
	if err != nil || id <= 0 || matches != 1 {
		return ErrEvidenceConflict
	}
	return nil
}

func protectedBaseRefreshDrainedAt(ctx context.Context, conn *sql.Conn, value protectedBaseRefreshIntent, proof GitMutationIntent) error {
	return protectedBaseRefreshDrainedExceptProofAt(ctx, conn, value, proof, false)
}

func protectedBaseRefreshDrainedExceptProofAt(ctx context.Context, conn *sql.Conn, value protectedBaseRefreshIntent, proof GitMutationIntent, allowUncertainProof bool) error {
	if err := repositoryHasProviderWriter(ctx, conn, value.Repository); err != nil {
		return err
	}
	if err := repositoryHasCommandWriter(ctx, conn, value.Repository); err != nil {
		return err
	}
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, value.Repository).Scan(&count); err != nil || count != 0 {
		return ErrGitMutationLease
	}
	// Only the read/reclaim path may exclude this exact uncertain proof. It
	// returns no launch authority: the dedicated reclaim writer authenticates
	// its immutable claim and revokes it before issuing a replacement.
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND (state='executing' OR (state='uncertain' AND NOT (? AND semantic_key=?)) OR (state='planned' AND semantic_key<>?))`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, allowUncertainProof, proof.SemanticKey, proof.SemanticKey).Scan(&count); err != nil || count != 0 {
		return ErrControlNotDrained
	}
	return nil
}
