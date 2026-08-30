package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// StoredPlan is the authenticated Planner checkpoint used by recovery.
type StoredPlan struct {
	Digest        string
	Document      PlanDocument
	TicketVersion uint64
	Fence         domain.Fence
	CreatedAt     time.Time
}

// StoredVerification contains the current immutable verification revision.
// It deliberately contains proof artifacts, not a provider transcript.
type StoredVerification struct {
	Revision        VerificationRevision
	Intent          []byte
	Proof           []byte
	TicketVersion   uint64
	Fence           domain.Fence
	AmendmentReason string
	Requester       string
	CreatedAt       time.Time
	ProviderResult  ProviderAttemptResultKey
	Checkpoint      CommitObservation
}

// StoredCandidate is the latest immutable candidate generation.
type StoredCandidate struct {
	Snapshot      domain.CandidateSnapshot
	TicketVersion uint64
	Fence         domain.Fence
	CreatedAt     time.Time
	BuilderResult ProviderAttemptResultKey
	Commit        CommitObservation
}

// StoredWorktree is SQLite's registration of a ticket worktree. The Git
// boundary must still re-prove this identity before every use.
type StoredWorktree struct {
	Path         string
	Branch       string
	State        string
	IdentityJSON []byte
	BaseSHA      string
	// HeadSHA is the immutable registration-time witness, not the current
	// candidate head. Candidate adoption binds its commit observation directly.
	HeadSHA       string
	TicketVersion uint64
	Fence         domain.Fence
}

// AssertTicketFence is a narrow pre-provider admission check.  It proves the
// caller's ticket version, runner epoch, and live leader epoch immediately
// before invoking an external PhaseRunner; provider admission must repeat its
// own check, but the worker never starts a runner on an already stale fence.
func (s *Store) AssertTicketFence(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) error {
	if ref.Validate() != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ErrStaleFence
	}
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return err
	}
	if ticket.Version != expectedVersion || ticket.RunnerEpoch != fence.RunnerEpoch {
		return ErrStaleFence
	}
	var leader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader); err != nil {
		return normalizeBusy(ctx, err)
	}
	if leader != fence.LeaderEpoch {
		return ErrStaleFence
	}
	return nil
}

// StoredPhaseAttempt is one durable provider phase attempt.
type StoredPhaseAttempt struct {
	Phase           domain.Phase
	Attempt         int
	State           string
	Provider        domain.ProviderIdentity
	WorktreeID      string
	BaseSHA         string
	ExpectedVersion uint64
	Fence           domain.Fence
	StartedAt       time.Time
	FinishedAt      time.Time
	Outcome         string
	UsageJSON       []byte
}

// StoredOperatorDecision is a bounded human decision bound to one head.
type StoredOperatorDecision struct {
	ID            int64
	ReviewedHead  string
	OperatorUID   uint32
	Decision      string
	Invalidated   bool
	CreatedAt     time.Time
	TicketVersion uint64
}

// ValidateCurrentCandidateForBuildTransition authenticates the exact
// candidate that may replay a lost build_pass response. A LatestCandidate
// read alone is insufficient: this method binds the generation to the
// current building ticket/fence, source, worktree base, and current
// verification identities. It performs no mutation.
func (s *Store) ValidateCurrentCandidateForBuildTransition(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (StoredCandidate, error) {
	if err := ref.Validate(); err != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return StoredCandidate{}, ErrStaleFence
	}
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return StoredCandidate{}, err
	}
	if ticket.State != domain.StateBuilding || ticket.Version != expectedVersion || ticket.RunnerEpoch != fence.RunnerEpoch {
		return StoredCandidate{}, ErrStaleFence
	}
	var leader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader); err != nil {
		return StoredCandidate{}, normalizeBusy(ctx, err)
	}
	if leader != fence.LeaderEpoch {
		return StoredCandidate{}, ErrStaleFence
	}
	candidate, err := s.LatestCandidate(ctx, ref)
	if err != nil {
		return StoredCandidate{}, err
	}
	if candidate.TicketVersion != expectedVersion || candidate.Fence != fence || candidate.Snapshot.SourceDigest != ticket.SourceDigest {
		return StoredCandidate{}, ErrStaleFence
	}
	worktree, err := s.Worktree(ctx, ref)
	if err != nil {
		return StoredCandidate{}, err
	}
	if candidate.Snapshot.BaseSHA != worktree.BaseSHA {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	verification, err := s.CurrentVerification(ctx, ref)
	if err != nil {
		return StoredCandidate{}, err
	}
	if candidate.Snapshot.VerificationIntentDigest != verification.Revision.IntentDigest || candidate.Snapshot.ProofDigest != verification.Revision.ProofDigest {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	if verification.Checkpoint.CommitOID != verification.Revision.CheckpointID || !validOID(verification.Checkpoint.ParentOID) || !validOID(verification.Checkpoint.TreeOID) || candidate.Commit.ParentOID != verification.Checkpoint.CommitOID {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	return candidate, nil
}

func (s *Store) Plan(ctx context.Context, ref domain.TicketRef) (StoredPlan, error) {
	if err := ref.Validate(); err != nil {
		return StoredPlan{}, err
	}
	var result StoredPlan
	var body string
	var artifact []byte
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT p.digest,p.body,p.artifact_bytes,
		COALESCE(b.binding_ticket_version,p.ticket_version),
		COALESCE(b.leader_epoch,p.leader_epoch),
		COALESCE(b.runner_epoch,p.runner_epoch),p.created_at
		FROM plans p
		LEFT JOIN plan_result_bindings b ON b.rowid=(
			SELECT latest.rowid FROM plan_result_bindings latest
			WHERE latest.channel=p.channel AND latest.project_id=p.project_id AND latest.ticket_id=p.ticket_id AND latest.plan_digest=p.digest
			ORDER BY latest.binding_ticket_version DESC,latest.leader_epoch DESC,latest.runner_epoch DESC LIMIT 1
		)
		WHERE p.channel=? AND p.project_id=? AND p.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&result.Digest, &body, &artifact, &result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredPlan{}, ErrNotFound
	}
	if err != nil {
		return StoredPlan{}, normalizeBusy(ctx, err)
	}
	if len(artifact) == 0 || !bytes.Equal([]byte(body), artifact) || sha256Digest(artifact) != result.Digest {
		return StoredPlan{}, ErrEvidenceConflict
	}
	if err := decodeEvidenceJSON(artifact, &result.Document); err != nil {
		return StoredPlan{}, ErrEvidenceConflict
	}
	canonical, err := validatePlanDocument(result.Document)
	if err != nil || !bytes.Equal(canonical, artifact) {
		return StoredPlan{}, ErrEvidenceConflict
	}
	if result.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil || result.TicketVersion == 0 || result.Fence.LeaderEpoch == 0 || result.Fence.RunnerEpoch == 0 {
		return StoredPlan{}, ErrEvidenceConflict
	}
	return result, nil
}

func (s *Store) CurrentVerification(ctx context.Context, ref domain.TicketRef) (StoredVerification, error) {
	if err := ref.Validate(); err != nil {
		return StoredVerification{}, err
	}
	var result StoredVerification
	var owned, created string
	var amends sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT r.revision,r.ticket_version,r.leader_epoch,r.runner_epoch,
		r.intent_digest,r.intent_bytes,r.proof_digest,r.proof_bytes,r.owned_files_json,r.checkpoint_id,
		r.amends_revision,r.amendment_reason,r.requester,r.created_at
		FROM verifications v JOIN verification_revisions r
		ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
		WHERE v.channel=? AND v.project_id=? AND v.ticket_id=?
		AND v.intent_digest=r.intent_digest AND v.proof_digest=r.proof_digest`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&result.Revision.Revision, &result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch,
		&result.Revision.IntentDigest, &result.Intent, &result.Revision.ProofDigest, &result.Proof, &owned,
		&result.Revision.CheckpointID, &amends, &result.AmendmentReason, &result.Requester, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredVerification{}, ErrNotFound
	}
	if err != nil {
		return StoredVerification{}, normalizeBusy(ctx, err)
	}
	if err := json.Unmarshal([]byte(owned), &result.Revision.OwnedFiles); err != nil || validOwnedFiles(result.Revision.OwnedFiles) != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if sha256Digest(result.Intent) != result.Revision.IntentDigest || sha256Digest(result.Proof) != result.Revision.ProofDigest || !validOID(result.Revision.CheckpointID) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if amends.Valid {
		if amends.Int64 <= 0 || uint64(amends.Int64) >= result.Revision.Revision || !boundedText(result.AmendmentReason, 2_000) || !boundedText(result.Requester, 200) {
			return StoredVerification{}, ErrEvidenceConflict
		}
		result.Revision.Amends = uint64(amends.Int64)
	} else if result.AmendmentReason != "" || result.Requester != "" {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if result.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil || result.TicketVersion == 0 || result.Fence.LeaderEpoch == 0 || result.Fence.RunnerEpoch == 0 {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if err := s.db.QueryRowContext(ctx, `SELECT b.binding_ticket_version,b.leader_epoch,b.runner_epoch,b.provider_attempt_id,b.provider_attempt,b.checkpoint_commit_oid,b.checkpoint_parent_oid,b.checkpoint_tree_oid FROM verification_result_bindings b JOIN provider_attempt_results pr ON pr.provider_attempt_id=b.provider_attempt_id JOIN provider_attempts a ON a.id=pr.provider_attempt_id AND a.attempt=b.provider_attempt AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id AND a.phase='verification' AND a.role='reviewer' AND a.state='completed' AND a.outcome='completed' WHERE b.channel=? AND b.project_id=? AND b.ticket_id=? AND b.revision=? ORDER BY b.binding_ticket_version DESC,b.leader_epoch DESC,b.runner_epoch DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, result.Revision.Revision).Scan(&result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &result.ProviderResult.AttemptID, &result.ProviderResult.Attempt, &result.Checkpoint.CommitOID, &result.Checkpoint.ParentOID, &result.Checkpoint.TreeOID); err == nil {
		result.ProviderResult.Ref, result.ProviderResult.Phase = ref, domain.PhaseVerification
		if result.Checkpoint.CommitOID != result.Revision.CheckpointID || !validOID(result.Checkpoint.ParentOID) || !validOID(result.Checkpoint.TreeOID) {
			return StoredVerification{}, ErrEvidenceConflict
		}
	}
	// Events are human/audit projections only. Provider and checkpoint authority
	// comes exclusively from the constrained v34 binding above; legacy rows
	// therefore expose no binding and worker transitions fail closed.
	return result, nil
}

func (s *Store) LatestCandidate(ctx context.Context, ref domain.TicketRef) (StoredCandidate, error) {
	if err := ref.Validate(); err != nil {
		return StoredCandidate{}, err
	}
	var result StoredCandidate
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,
		source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest,created_at
		FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY generation DESC LIMIT 1`,
		ref.Channel, ref.Project, ref.Ticket).Scan(
		&result.Snapshot.Generation, &result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch,
		&result.Snapshot.BaseSHA, &result.Snapshot.HeadSHA, &result.Snapshot.TreeSHA, &result.Snapshot.SourceDigest,
		&result.Snapshot.VerificationIntentDigest, &result.Snapshot.ProofDigest, &result.Snapshot.CommandPolicyDigest, &result.Snapshot.BuilderEvidenceDigest, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredCandidate{}, ErrNotFound
	}
	if err != nil {
		return StoredCandidate{}, normalizeBusy(ctx, err)
	}
	if result.Snapshot.Generation == 0 || validateCandidate(result.Snapshot) != nil || result.TicketVersion == 0 || result.Fence.LeaderEpoch == 0 || result.Fence.RunnerEpoch == 0 {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	if result.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	if err := s.db.QueryRowContext(ctx, `SELECT b.binding_ticket_version,b.leader_epoch,b.runner_epoch,b.provider_attempt_id,b.provider_attempt,b.commit_parent_oid FROM candidate_result_bindings b JOIN provider_attempt_results pr ON pr.provider_attempt_id=b.provider_attempt_id JOIN provider_attempts a ON a.id=pr.provider_attempt_id AND a.attempt=b.provider_attempt AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id AND a.phase='build' AND a.role='builder' AND a.state='completed' AND a.outcome='completed' WHERE b.channel=? AND b.project_id=? AND b.ticket_id=? AND b.generation=? ORDER BY b.binding_ticket_version DESC,b.leader_epoch DESC,b.runner_epoch DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, result.Snapshot.Generation).Scan(&result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &result.BuilderResult.AttemptID, &result.BuilderResult.Attempt, &result.Commit.ParentOID); err == nil {
		result.BuilderResult.Ref, result.BuilderResult.Phase = ref, domain.PhaseBuild
		result.Commit.CommitOID, result.Commit.TreeOID = result.Snapshot.HeadSHA, result.Snapshot.TreeSHA
	}
	if result.BuilderResult.AttemptID == 0 || result.Commit.ParentOID == "" {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	return result, nil
}

func (s *Store) Worktree(ctx context.Context, ref domain.TicketRef) (StoredWorktree, error) {
	if err := ref.Validate(); err != nil {
		return StoredWorktree{}, err
	}
	var result StoredWorktree
	err := s.db.QueryRowContext(ctx, `SELECT path,branch_ref,state,identity_json,base_sha,head_sha,ticket_version,leader_epoch,runner_epoch
		FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&result.Path, &result.Branch, &result.State, &result.IdentityJSON, &result.BaseSHA, &result.HeadSHA,
		&result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredWorktree{}, ErrNotFound
	}
	if err != nil {
		return StoredWorktree{}, normalizeBusy(ctx, err)
	}
	if !boundedText(result.Path, 1_000) || !boundedText(result.Branch, 300) || !boundedText(result.State, 100) || !validJSON(result.IdentityJSON) || !validOID(result.BaseSHA) || !validOID(result.HeadSHA) || result.TicketVersion == 0 || result.Fence.LeaderEpoch == 0 || result.Fence.RunnerEpoch == 0 {
		return StoredWorktree{}, ErrEvidenceConflict
	}
	return result, nil
}

func (s *Store) PhaseAttempts(ctx context.Context, ref domain.TicketRef) ([]StoredPhaseAttempt, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT phase,attempt,state,provider,model,family,provider_version,worktree_identity,base_sha,
		expected_ticket_version,leader_epoch,runner_epoch,COALESCE(started_at,''),COALESCE(completed_at,failed_at,''),outcome,usage_json
		FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY rowid`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var result []StoredPhaseAttempt
	for rows.Next() {
		var item StoredPhaseAttempt
		var started, finished string
		if err := rows.Scan(&item.Phase, &item.Attempt, &item.State, &item.Provider.Provider, &item.Provider.Model,
			&item.Provider.Family, &item.Provider.Version, &item.WorktreeID, &item.BaseSHA, &item.ExpectedVersion,
			&item.Fence.LeaderEpoch, &item.Fence.RunnerEpoch, &started, &finished, &item.Outcome, &item.UsageJSON); err != nil {
			return nil, err
		}
		if !validPhase(item.Phase) || item.Attempt < 1 || !validProvider(item.Provider) || !boundedText(item.WorktreeID, 500) || !validOID(item.BaseSHA) || item.ExpectedVersion == 0 || item.Fence.LeaderEpoch == 0 || item.Fence.RunnerEpoch == 0 || !validJSON(item.UsageJSON) {
			return nil, ErrEvidenceConflict
		}
		if item.State != "active" && item.State != "completed" && item.State != "failed" && item.State != "cancelled" {
			return nil, ErrEvidenceConflict
		}
		if started != "" {
			if item.StartedAt, err = time.Parse(time.RFC3339Nano, started); err != nil {
				return nil, ErrEvidenceConflict
			}
		}
		if finished != "" {
			if item.FinishedAt, err = time.Parse(time.RFC3339Nano, finished); err != nil {
				return nil, ErrEvidenceConflict
			}
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) OperatorDecisions(ctx context.Context, ref domain.TicketRef) ([]StoredOperatorDecision, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,reviewed_head,operator_uid,decision,invalidated,created_at,ticket_version
		FROM approvals WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY id`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var result []StoredOperatorDecision
	for rows.Next() {
		var item StoredOperatorDecision
		var invalidated int
		var created string
		if err := rows.Scan(&item.ID, &item.ReviewedHead, &item.OperatorUID, &item.Decision, &invalidated, &created, &item.TicketVersion); err != nil {
			return nil, err
		}
		item.Invalidated = invalidated == 1
		if item.ID <= 0 || !validOID(item.ReviewedHead) || item.OperatorUID == 0 || (item.Decision != "approved" && item.Decision != "rejected") || (invalidated != 0 && invalidated != 1) || item.TicketVersion == 0 {
			return nil, ErrEvidenceConflict
		}
		if item.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			return nil, ErrEvidenceConflict
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func decodeEvidenceJSON(data []byte, destination any) error {
	if len(data) == 0 || len(data) > maxEvidenceBytes {
		return errors.New("evidence exceeds byte bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("evidence contains trailing data")
	}
	return nil
}
