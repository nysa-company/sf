package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

const (
	maxEvidenceBytes = 1 << 20
	maxEvidenceJSON  = 64 << 10
)

// PlanDocument is the bounded, typed Planner output retained for recovery and
// review. It is intentionally not a provider transcript.
type PlanDocument struct {
	Acceptance []string   `json:"acceptance"`
	ProofKind  string     `json:"proof_kind"`
	Paths      []string   `json:"paths"`
	Commands   [][]string `json:"commands"`
	Risks      []string   `json:"risks"`
}

type PlanArtifact struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Document        PlanDocument
}

type VerificationArtifact struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Intent          []byte
	Proof           []byte
	OwnedFiles      []string
	CheckpointID    string
	AmendsRevision  uint64
	Reason          string
	Requester       string
}

type VerificationRevision struct {
	Revision     uint64
	IntentDigest string
	ProofDigest  string
	OwnedFiles   []string
	CheckpointID string
	Amends       uint64
}

type CandidateEvidence struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Snapshot        domain.CandidateSnapshot
	BuilderResult   ProviderAttemptResultKey
	Commit          CommitObservation
	Reason          string
}

// CommitObservation is a Store-neutral, Git-bound commit witness. Store only
// binds the three object identities and deliberately does not import Git.
type CommitObservation struct {
	CommitOID string
	ParentOID string
	TreeOID   string
}

type InvalidationReceipt struct {
	Generation uint64
	Kind       string
	Reason     string
	CreatedAt  time.Time
}

type PhaseAttempt struct {
	Ref             domain.TicketRef
	Phase           domain.Phase
	Attempt         int
	ExpectedVersion uint64
	Fence           domain.Fence
	Provider        domain.ProviderIdentity
	WorktreeID      string
	BaseSHA         string
	Outcome         string
	UsageJSON       []byte
}

type WorktreeRegistration struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Path            string
	Branch          string
	IdentityJSON    []byte
	BaseSHA         string
	HeadSHA         string
}

type OperatorDecision struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	ReviewedHead    string
	OperatorUID     uint32
	Decision        string
}

type BudgetUse struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Kind            string
	RequestID       string
}

func (s *Store) RecordPlan(ctx context.Context, artifact PlanArtifact) (string, error) {
	if err := artifact.Ref.Validate(); err != nil {
		return "", err
	}
	body, err := validatePlanDocument(artifact.Document)
	if err != nil {
		return "", err
	}
	digest := sha256Digest(body)
	err = s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, artifact.Ref, artifact.ExpectedVersion, artifact.Fence); err != nil {
			return err
		}
		var existingDigest string
		err := conn.QueryRowContext(ctx, `SELECT digest FROM plans WHERE channel=? AND project_id=? AND ticket_id=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket).Scan(&existingDigest)
		if err == nil {
			if existingDigest == digest {
				return nil
			}
			return ErrEvidenceConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO plans(channel, project_id, ticket_id, digest, body, artifact_bytes, ticket_version, leader_epoch, runner_epoch, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, digest, string(body), body, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch, now())
		if err != nil {
			return err
		}
		return evidenceEvent(ctx, conn, artifact.Ref, artifact.ExpectedVersion, "plan_recorded", map[string]string{"digest": digest})
	})
	return digest, err
}

func (s *Store) RecordVerification(ctx context.Context, artifact VerificationArtifact) (VerificationRevision, error) {
	if err := artifact.Ref.Validate(); err != nil {
		return VerificationRevision{}, err
	}
	if err := validBlob(artifact.Intent, "verification intent"); err != nil {
		return VerificationRevision{}, err
	}
	if err := validBlob(artifact.Proof, "verification proof"); err != nil {
		return VerificationRevision{}, err
	}
	if err := validOwnedFiles(artifact.OwnedFiles); err != nil || !validOID(artifact.CheckpointID) {
		return VerificationRevision{}, fmt.Errorf("bounded verification checkpoint and owned files are required")
	}
	if (artifact.AmendsRevision == 0) != (artifact.Reason == "" && artifact.Requester == "") {
		return VerificationRevision{}, fmt.Errorf("verification amendment must bind revision, reason, and requester")
	}
	if artifact.AmendsRevision > 0 && (!boundedText(artifact.Reason, 2_000) || !boundedText(artifact.Requester, 200)) {
		return VerificationRevision{}, fmt.Errorf("bounded amendment reason and requester are required")
	}
	owned, _ := json.Marshal(artifact.OwnedFiles)
	intentDigest, proofDigest := sha256Digest(artifact.Intent), sha256Digest(artifact.Proof)
	result := VerificationRevision{IntentDigest: intentDigest, ProofDigest: proofDigest, OwnedFiles: append([]string(nil), artifact.OwnedFiles...), CheckpointID: artifact.CheckpointID, Amends: artifact.AmendsRevision}
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, artifact.Ref, artifact.ExpectedVersion, artifact.Fence); err != nil {
			return err
		}
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT current_revision FROM verifications WHERE channel=? AND project_id=? AND ticket_id=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket).Scan(&current); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if current > 0 {
			var oldIntent string
			if err := conn.QueryRowContext(ctx, `SELECT intent_digest FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, current).Scan(&oldIntent); err != nil {
				return err
			}
			if artifact.AmendsRevision == 0 {
				var oldProof, oldCheckpoint string
				if err := conn.QueryRowContext(ctx, `SELECT proof_digest, checkpoint_id FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, current).Scan(&oldProof, &oldCheckpoint); err != nil {
					return err
				}
				if oldIntent == intentDigest && oldProof == proofDigest && oldCheckpoint == artifact.CheckpointID {
					result.Revision = current
					return nil
				}
				return ErrEvidenceConflict
			}
			if artifact.AmendsRevision != current || oldIntent == intentDigest {
				return ErrEvidenceConflict
			}
		} else if artifact.AmendsRevision != 0 {
			return ErrEvidenceConflict
		}
		result.Revision = current + 1
		_, err := conn.ExecContext(ctx, `INSERT INTO verification_revisions(channel, project_id, ticket_id, revision, ticket_version, leader_epoch, runner_epoch, intent_digest, intent_bytes, proof_digest, proof_bytes, owned_files_json, checkpoint_id, amends_revision, amendment_reason, requester, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, result.Revision, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch, intentDigest, artifact.Intent, proofDigest, artifact.Proof, string(owned), artifact.CheckpointID, nullableUint(artifact.AmendsRevision), artifact.Reason, artifact.Requester, now())
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO verifications(channel, project_id, ticket_id, intent_digest, proof_digest, current_revision) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(channel, project_id, ticket_id) DO UPDATE SET intent_digest=excluded.intent_digest, proof_digest=excluded.proof_digest, current_revision=excluded.current_revision`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, intentDigest, proofDigest, result.Revision)
		if err != nil {
			return err
		}
		return evidenceEvent(ctx, conn, artifact.Ref, artifact.ExpectedVersion, "verification_recorded", map[string]any{"revision": result.Revision, "intent_digest": intentDigest, "proof_digest": proofDigest})
	})
	return result, err
}

// RecordCandidate creates an immutable generation and always writes the full
// invalidation set, even when a prior gate was absent. Consumers can therefore
// reason from receipts rather than attempting to infer invalidation from NULLs.
func (s *Store) RecordCandidate(ctx context.Context, evidence CandidateEvidence) ([]InvalidationReceipt, error) {
	if err := evidence.Ref.Validate(); err != nil {
		return nil, err
	}
	if err := validateCandidate(evidence.Snapshot); err != nil || !boundedText(evidence.Reason, 2_000) || evidence.BuilderResult.AttemptID <= 0 || evidence.BuilderResult.Ref != evidence.Ref || evidence.BuilderResult.Phase != domain.PhaseBuild || evidence.BuilderResult.Attempt <= 0 || !validOID(evidence.Commit.CommitOID) || !validOID(evidence.Commit.ParentOID) || !validOID(evidence.Commit.TreeOID) || evidence.Commit.CommitOID != evidence.Snapshot.HeadSHA || evidence.Commit.ParentOID != evidence.Snapshot.BaseSHA || evidence.Commit.TreeOID != evidence.Snapshot.TreeSHA {
		return nil, fmt.Errorf("valid bounded candidate evidence and reason are required")
	}
	// Immutable provider evidence is authenticated before opening the write
	// transaction. The transaction below re-selects the newest terminal Builder
	// attempt, so a later malformed or failed completion cannot be skipped while
	// waiting for the writer.
	builder, parsed, err := s.LoadHistoricalProviderAttemptResult(ctx, evidence.BuilderResult)
	if err != nil || builder.Claim.Role != "builder" || builder.Claim.Phase != domain.PhaseBuild || parsed.Builder == nil {
		return nil, ErrEvidenceConflict
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil || builderDigest != evidence.Snapshot.BuilderEvidenceDigest {
		return nil, ErrEvidenceConflict
	}
	receipts := make([]InvalidationReceipt, 0, 4)
	err = s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, evidence.Ref, evidence.ExpectedVersion, evidence.Fence); err != nil {
			return err
		}
		var state, source string
		if err := conn.QueryRowContext(ctx, `SELECT state,source_digest FROM tickets WHERE channel=? AND project_id=? AND id=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&state, &source); err != nil {
			return err
		}
		if domain.State(state) != domain.StateBuilding || source != evidence.Snapshot.SourceDigest {
			return ErrEvidenceConflict
		}
		var newest ProviderAttemptResultKey
		var resultID sql.NullInt64
		err := conn.QueryRowContext(ctx, `SELECT r.provider_attempt_id,a.attempt
			FROM provider_attempts a LEFT JOIN provider_attempt_results r ON r.provider_attempt_id=a.id
			WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase='build' AND a.role='builder' AND a.finished_at IS NOT NULL
			ORDER BY a.attempt DESC,a.id DESC LIMIT 1`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&resultID, &newest.Attempt)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if !resultID.Valid {
			return ErrEvidenceConflict
		}
		newest.AttemptID, newest.Ref, newest.Phase = resultID.Int64, evidence.Ref, domain.PhaseBuild
		if newest != evidence.BuilderResult {
			return ErrEvidenceConflict
		}
		var path, identity, base string
		if err := conn.QueryRowContext(ctx, `SELECT path,identity_json,base_sha FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&path, &identity, &base); err != nil {
			return ErrEvidenceConflict
		}
		if path != builder.Claim.Worktree || identity != builder.Claim.WorktreeIdentity || base != builder.Claim.BaseSHA || base != evidence.Snapshot.BaseSHA {
			return ErrEvidenceConflict
		}
		var intent, proof, owned, checkpoint string
		var intentBytes, proofBytes []byte
		if err := conn.QueryRowContext(ctx, `SELECT r.intent_digest,r.intent_bytes,r.proof_digest,r.proof_bytes,r.owned_files_json,r.checkpoint_id FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision WHERE v.channel=? AND v.project_id=? AND v.ticket_id=? AND v.intent_digest=r.intent_digest AND v.proof_digest=r.proof_digest`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&intent, &intentBytes, &proof, &proofBytes, &owned, &checkpoint); err != nil || intent != evidence.Snapshot.VerificationIntentDigest || proof != evidence.Snapshot.ProofDigest || sha256Digest(intentBytes) != intent || sha256Digest(proofBytes) != proof || !validOID(checkpoint) {
			return ErrEvidenceConflict
		}
		var ownedFiles []string
		if json.Unmarshal([]byte(owned), &ownedFiles) != nil || validOwnedFiles(ownedFiles) != nil {
			return ErrEvidenceConflict
		}
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation), 0) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&current); err != nil {
			return err
		}
		if evidence.Snapshot.Generation == 0 {
			evidence.Snapshot.Generation = current + 1
		}
		if evidence.Snapshot.Generation < current {
			return ErrEvidenceConflict
		}
		if evidence.Snapshot.Generation == current {
			var existing domain.CandidateSnapshot
			err := conn.QueryRowContext(ctx, `SELECT generation,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, evidence.Snapshot.Generation).Scan(&existing.Generation, &existing.BaseSHA, &existing.HeadSHA, &existing.TreeSHA, &existing.SourceDigest, &existing.VerificationIntentDigest, &existing.ProofDigest, &existing.CommandPolicyDigest, &existing.BuilderEvidenceDigest)
			if err != nil || existing != evidence.Snapshot {
				return ErrEvidenceConflict
			}
			return nil
		}
		if evidence.Snapshot.Generation != current+1 {
			return ErrEvidenceConflict
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO candidate_snapshots(channel, project_id, ticket_id, generation, ticket_version, leader_epoch, runner_epoch, base_sha, head_sha, tree_sha, source_digest, verification_intent_digest, proof_digest, command_policy_digest, builder_evidence_digest, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, evidence.Snapshot.Generation, evidence.ExpectedVersion, evidence.Fence.LeaderEpoch, evidence.Fence.RunnerEpoch, evidence.Snapshot.BaseSHA, evidence.Snapshot.HeadSHA, evidence.Snapshot.TreeSHA, evidence.Snapshot.SourceDigest, evidence.Snapshot.VerificationIntentDigest, evidence.Snapshot.ProofDigest, evidence.Snapshot.CommandPolicyDigest, evidence.Snapshot.BuilderEvidenceDigest, now())
		if err != nil {
			return err
		}
		for _, kind := range []string{"proof_result", "github_checks", "final_review", "approval"} {
			at := time.Now().UTC()
			if _, err := conn.ExecContext(ctx, `INSERT INTO invalidation_receipts(channel, project_id, ticket_id, generation, kind, ticket_version, reason, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, evidence.Snapshot.Generation, kind, evidence.ExpectedVersion, evidence.Reason, at.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			receipts = append(receipts, InvalidationReceipt{Generation: evidence.Snapshot.Generation, Kind: kind, Reason: evidence.Reason, CreatedAt: at})
		}
		if _, err := conn.ExecContext(ctx, `UPDATE approvals SET invalidated=1 WHERE channel=? AND project_id=? AND ticket_id=? AND invalidated=0 AND reviewed_head<>?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, evidence.Snapshot.HeadSHA); err != nil {
			return err
		}
		return evidenceEvent(ctx, conn, evidence.Ref, evidence.ExpectedVersion, "candidate_recorded", map[string]any{"generation": evidence.Snapshot.Generation, "head": evidence.Snapshot.HeadSHA})
	})
	return receipts, err
}

func (s *Store) StartPhaseAttempt(ctx context.Context, attempt PhaseAttempt) error {
	return s.recordPhaseAttempt(ctx, attempt, "active")
}
func (s *Store) CompletePhaseAttempt(ctx context.Context, attempt PhaseAttempt) error {
	return s.recordPhaseAttempt(ctx, attempt, "completed")
}
func (s *Store) FailPhaseAttempt(ctx context.Context, attempt PhaseAttempt) error {
	return s.recordPhaseAttempt(ctx, attempt, "failed")
}

func (s *Store) recordPhaseAttempt(ctx context.Context, attempt PhaseAttempt, disposition string) error {
	if err := attempt.Ref.Validate(); err != nil {
		return err
	}
	if !validPhase(attempt.Phase) || attempt.Attempt < 1 || !validProvider(attempt.Provider) || !boundedText(attempt.WorktreeID, 500) || !validOID(attempt.BaseSHA) || (disposition != "active" && !boundedText(attempt.Outcome, 300)) {
		return fmt.Errorf("invalid phase attempt identity")
	}
	if disposition != "active" && !validJSON(attempt.UsageJSON) {
		return fmt.Errorf("bounded usage JSON is required")
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, attempt.Ref, attempt.ExpectedVersion, attempt.Fence); err != nil {
			return err
		}
		var state, provider, model, family, version, worktree, base, outcome string
		err := conn.QueryRowContext(ctx, `SELECT state, provider, model, family, provider_version, worktree_identity, base_sha, outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=?`, attempt.Ref.Channel, attempt.Ref.Project, attempt.Ref.Ticket, attempt.Phase, attempt.Attempt).Scan(&state, &provider, &model, &family, &version, &worktree, &base, &outcome)
		if errors.Is(err, sql.ErrNoRows) {
			if disposition != "active" {
				return ErrStaleFence
			}
			_, err = conn.ExecContext(ctx, `INSERT INTO phase_runs(channel, project_id, ticket_id, phase, attempt, state, leader_epoch, runner_epoch, expected_ticket_version, provider, model, family, provider_version, worktree_identity, base_sha, started_at, outcome, usage_json) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', '{}')`, attempt.Ref.Channel, attempt.Ref.Project, attempt.Ref.Ticket, attempt.Phase, attempt.Attempt, attempt.Fence.LeaderEpoch, attempt.Fence.RunnerEpoch, attempt.ExpectedVersion, attempt.Provider.Provider, attempt.Provider.Model, attempt.Provider.Family, attempt.Provider.Version, attempt.WorktreeID, attempt.BaseSHA, now())
			if err != nil {
				return err
			}
			return evidenceEvent(ctx, conn, attempt.Ref, attempt.ExpectedVersion, "phase_attempt_started", map[string]any{"phase": attempt.Phase, "attempt": attempt.Attempt})
		}
		if err != nil {
			return err
		}
		if provider != attempt.Provider.Provider || model != attempt.Provider.Model || family != attempt.Provider.Family || version != attempt.Provider.Version || worktree != attempt.WorktreeID || base != attempt.BaseSHA {
			return ErrEvidenceConflict
		}
		if disposition == "active" {
			if state == "active" {
				return nil
			}
			return ErrEvidenceConflict
		}
		if state == disposition && outcome == attempt.Outcome {
			return nil
		}
		if state != "active" {
			return ErrEvidenceConflict
		}
		column := "completed_at"
		if disposition == "failed" {
			column = "failed_at"
		}
		query := `UPDATE phase_runs SET state=?, ` + column + `=?, outcome=?, usage_json=? WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=?`
		result, err := conn.ExecContext(ctx, query, disposition, now(), attempt.Outcome, string(attempt.UsageJSON), attempt.Ref.Channel, attempt.Ref.Project, attempt.Ref.Ticket, attempt.Phase, attempt.Attempt, attempt.Fence.LeaderEpoch, attempt.Fence.RunnerEpoch, attempt.ExpectedVersion)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		return evidenceEvent(ctx, conn, attempt.Ref, attempt.ExpectedVersion, "phase_attempt_"+disposition, map[string]any{"phase": attempt.Phase, "attempt": attempt.Attempt, "outcome": attempt.Outcome})
	})
}

func (s *Store) RegisterWorktree(ctx context.Context, registration WorktreeRegistration) error {
	if err := registration.Ref.Validate(); err != nil {
		return err
	}
	if !boundedText(registration.Path, 1_000) || !boundedText(registration.Branch, 300) || !validJSON(registration.IdentityJSON) || !validOID(registration.BaseSHA) || !validOID(registration.HeadSHA) {
		return fmt.Errorf("valid bounded worktree identity is required")
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, registration.Ref, registration.ExpectedVersion, registration.Fence); err != nil {
			return err
		}
		var path, branch, identity, base, head string
		err := conn.QueryRowContext(ctx, `SELECT path, branch_ref, identity_json, base_sha, head_sha FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, registration.Ref.Channel, registration.Ref.Project, registration.Ref.Ticket).Scan(&path, &branch, &identity, &base, &head)
		if err == nil {
			if path == registration.Path && branch == registration.Branch && identity == string(registration.IdentityJSON) && base == registration.BaseSHA && head == registration.HeadSHA {
				return nil
			}
			return ErrEvidenceConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO worktrees(channel, project_id, ticket_id, path, branch_ref, state, identity_json, base_sha, head_sha, ticket_version, leader_epoch, runner_epoch) VALUES (?, ?, ?, ?, ?, 'registered', ?, ?, ?, ?, ?, ?)`, registration.Ref.Channel, registration.Ref.Project, registration.Ref.Ticket, registration.Path, registration.Branch, string(registration.IdentityJSON), registration.BaseSHA, registration.HeadSHA, registration.ExpectedVersion, registration.Fence.LeaderEpoch, registration.Fence.RunnerEpoch)
		if err != nil {
			return err
		}
		return evidenceEvent(ctx, conn, registration.Ref, registration.ExpectedVersion, "worktree_registered", map[string]string{"branch": registration.Branch, "base": registration.BaseSHA})
	})
}

func (s *Store) RecordOperatorDecision(ctx context.Context, decision OperatorDecision) error {
	if err := decision.Ref.Validate(); err != nil {
		return err
	}
	if !validOID(decision.ReviewedHead) || decision.OperatorUID == 0 || (decision.Decision != "approved" && decision.Decision != "rejected") {
		return fmt.Errorf("exact reviewed head, operator, and decision are required")
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, decision.Ref, decision.ExpectedVersion, decision.Fence); err != nil {
			return err
		}
		var head string
		if err := conn.QueryRowContext(ctx, `SELECT head_sha FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY generation DESC LIMIT 1`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket).Scan(&head); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if head != decision.ReviewedHead {
			return ErrStaleFence
		}
		var prior string
		err := conn.QueryRowContext(ctx, `SELECT decision FROM approvals WHERE channel=? AND project_id=? AND ticket_id=? AND reviewed_head=? AND operator_uid=? AND invalidated=0`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket, decision.ReviewedHead, decision.OperatorUID).Scan(&prior)
		if err == nil {
			if prior == decision.Decision {
				return nil
			}
			return ErrEvidenceConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO approvals(channel, project_id, ticket_id, reviewed_head, operator_uid, decision, invalidated, created_at, ticket_version) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket, decision.ReviewedHead, decision.OperatorUID, decision.Decision, now(), decision.ExpectedVersion)
		if err != nil {
			return err
		}
		return evidenceEvent(ctx, conn, decision.Ref, decision.ExpectedVersion, "operator_"+decision.Decision, map[string]string{"head": decision.ReviewedHead})
	})
}

func (s *Store) ConsumeBudget(ctx context.Context, use BudgetUse) (int, error) {
	if err := use.Ref.Validate(); err != nil {
		return 0, err
	}
	limit := 0
	if use.Kind == "correction" {
		limit = 2
	} else if use.Kind == "fallback" {
		limit = 1
	}
	if limit == 0 || !boundedText(use.RequestID, 300) {
		return 0, fmt.Errorf("valid bounded budget use is required")
	}
	used := 0
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, use.Ref, use.ExpectedVersion, use.Fence); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO ticket_counters(channel, project_id, ticket_id, kind, used, limit_count) VALUES (?, ?, ?, ?, 0, ?) ON CONFLICT(channel, project_id, ticket_id, kind) DO NOTHING`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind, limit)
		if err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO ticket_budget_uses(channel, project_id, ticket_id, kind, request_id, ticket_version, leader_epoch, runner_epoch, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(channel, project_id, ticket_id, kind, request_id) DO NOTHING`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind, use.RequestID, use.ExpectedVersion, use.Fence.LeaderEpoch, use.Fence.RunnerEpoch, now())
		if err != nil {
			return err
		}
		if inserted, _ := result.RowsAffected(); inserted == 1 {
			updated, err := conn.ExecContext(ctx, `UPDATE ticket_counters SET used=used+1 WHERE channel=? AND project_id=? AND ticket_id=? AND kind=? AND used<limit_count`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind)
			if err != nil {
				return err
			}
			if count, _ := updated.RowsAffected(); count != 1 {
				return ErrBudgetExhausted
			}
			if err := evidenceEvent(ctx, conn, use.Ref, use.ExpectedVersion, "budget_"+use.Kind, map[string]string{"request_id": use.RequestID}); err != nil {
				return err
			}
		}
		return conn.QueryRowContext(ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind=?`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind).Scan(&used)
	})
	return used, err
}

func evidenceEvent(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, trigger string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil || len(encoded) > maxEvidenceJSON {
		return fmt.Errorf("encode bounded evidence event")
	}
	var state domain.State
	if err := conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, ref.Channel, ref.Project, ref.Ticket, version, trigger, state, state, string(encoded), now())
	return err
}

func validatePlanDocument(document PlanDocument) ([]byte, error) {
	if !boundedText(document.ProofKind, 100) || len(document.Acceptance) == 0 || len(document.Paths) == 0 || len(document.Commands) == 0 {
		return nil, fmt.Errorf("plan requires bounded acceptance, proof kind, paths, and commands")
	}
	for _, values := range [][]string{document.Acceptance, document.Paths, document.Risks} {
		if len(values) > 256 {
			return nil, fmt.Errorf("plan field exceeds item bound")
		}
		for _, value := range values {
			if !boundedText(value, 2_000) {
				return nil, fmt.Errorf("plan field contains unbounded text")
			}
		}
	}
	if len(document.Commands) > 20 {
		return nil, fmt.Errorf("plan commands exceed item bound")
	}
	for _, argv := range document.Commands {
		if len(argv) == 0 || len(argv) > 64 {
			return nil, fmt.Errorf("plan command requires 1 to 64 argv values")
		}
		for _, value := range argv {
			if !boundedText(value, 2_000) {
				return nil, fmt.Errorf("plan command contains unbounded argv")
			}
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil || len(encoded) > maxEvidenceBytes {
		return nil, fmt.Errorf("plan artifact exceeds bound")
	}
	return encoded, nil
}
func validBlob(value []byte, name string) error {
	if len(value) == 0 || len(value) > maxEvidenceBytes {
		return fmt.Errorf("%s exceeds byte bound", name)
	}
	return nil
}
func validJSON(value []byte) bool {
	return len(value) > 0 && len(value) <= maxEvidenceJSON && json.Valid(value)
}
func validOwnedFiles(files []string) error {
	if len(files) == 0 || len(files) > 256 {
		return errors.New("verification owned files must be bounded")
	}
	seen := map[string]bool{}
	for _, file := range files {
		if !boundedText(file, 1_000) || strings.HasPrefix(file, "/") || strings.Contains(file, "..") || seen[file] {
			return errors.New("invalid verification owned file")
		}
		seen[file] = true
	}
	return nil
}
func validPhase(phase domain.Phase) bool {
	for _, candidate := range []domain.Phase{domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhasePublish, domain.PhaseReview, domain.PhaseMerge, domain.PhaseReconcile} {
		if phase == candidate {
			return true
		}
	}
	return false
}
func validProvider(provider domain.ProviderIdentity) bool {
	return boundedText(provider.Provider, 100) && boundedText(provider.Model, 200) && boundedText(provider.Family, 100) && boundedText(provider.Version, 200)
}
func validOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	if strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func validateCandidate(snapshot domain.CandidateSnapshot) error {
	if !validOID(snapshot.BaseSHA) || !validOID(snapshot.HeadSHA) || !validOID(snapshot.TreeSHA) {
		return errors.New("candidate git identities must be canonical object ids")
	}
	for _, digest := range []string{snapshot.SourceDigest, snapshot.VerificationIntentDigest, snapshot.ProofDigest, snapshot.CommandPolicyDigest, snapshot.BuilderEvidenceDigest} {
		if !validDigest(digest) {
			return errors.New("candidate digest must be canonical SHA-256")
		}
	}
	return nil
}
func validDigest(value string) bool {
	return len(value) == 64 && strings.ToLower(value) == value && func() bool { _, err := hex.DecodeString(value); return err == nil }()
}
func sha256Digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
func boundedText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}
func nullableUint(value uint64) any {
	if value == 0 {
		return nil
	}
	return value
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// VerificationRevisions returns immutable amendment history in sequence.
func (s *Store) VerificationRevisions(ctx context.Context, ref domain.TicketRef) ([]VerificationRevision, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT revision,intent_digest,proof_digest,owned_files_json,checkpoint_id,COALESCE(amends_revision,0) FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY revision`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var result []VerificationRevision
	for rows.Next() {
		var item VerificationRevision
		var owned string
		if err := rows.Scan(&item.Revision, &item.IntentDigest, &item.ProofDigest, &owned, &item.CheckpointID, &item.Amends); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(owned), &item.OwnedFiles); err != nil || validOwnedFiles(item.OwnedFiles) != nil || !validDigest(item.IntentDigest) || !validDigest(item.ProofDigest) || !validOID(item.CheckpointID) {
			return nil, ErrEvidenceConflict
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) InvalidationReceipts(ctx context.Context, ref domain.TicketRef, generation uint64) ([]InvalidationReceipt, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT generation,kind,reason,created_at FROM invalidation_receipts WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? ORDER BY kind`, ref.Channel, ref.Project, ref.Ticket, generation)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var result []InvalidationReceipt
	for rows.Next() {
		var receipt InvalidationReceipt
		var at string
		if err := rows.Scan(&receipt.Generation, &receipt.Kind, &receipt.Reason, &at); err != nil {
			return nil, err
		}
		if receipt.CreatedAt, err = time.Parse(time.RFC3339Nano, at); err != nil {
			return nil, err
		}
		result = append(result, receipt)
	}
	return result, rows.Err()
}

func sortedReceiptKinds(receipts []InvalidationReceipt) []string {
	result := make([]string, 0, len(receipts))
	for _, receipt := range receipts {
		result = append(result, receipt.Kind)
	}
	sort.Strings(result)
	return result
}
