package store

import (
	"bytes"
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

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

const (
	maxEvidenceBytes = 1 << 20
	maxEvidenceJSON  = 64 << 10
)

// PlanDocument is the bounded, typed Planner output retained for recovery and
// review. It is intentionally not a provider transcript.
type PlanDocument struct {
	// Planner is the complete canonical Planner artifact.  The older summary
	// fields remain readable for pre-worker records, but new workflow-worker
	// records must carry this exact artifact and its immutable provider result.
	Planner        *phaseartifact.Planner    `json:"planner,omitempty"`
	ProviderResult *ProviderAttemptResultKey `json:"provider_result,omitempty"`
	Acceptance     []string                  `json:"acceptance"`
	ProofKind      string                    `json:"proof_kind"`
	Paths          []string                  `json:"paths"`
	Commands       [][]string                `json:"commands"`
	Risks          []string                  `json:"risks"`
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
	// ProviderResult is checked at admission.  The immutable provider result
	// remains the canonical full Verification artifact; intent/proof are only
	// its durable projections.
	ProviderResult *ProviderAttemptResultKey
	Checkpoint     CommitObservation
	// CommandResult is the exact, drained repository-command observation that
	// authenticated the provider-declared pre-build verification command.
	CommandResult contracts.RepositoryCommandResultKey
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
	// CommandResult is the exact post-build repository verification result.
	CommandResult contracts.RepositoryCommandResultKey
}

// RepositoryCommandResultBinding is the append-only, consumer-specific
// projection of immutable command evidence. Repeating the claim identities in
// the binding keeps verification/candidate reads independent of a caller's
// proposed command or policy.
type RepositoryCommandResultBinding struct {
	Key                                                                       contracts.RepositoryCommandResultKey
	TicketVersion, LeaderEpoch, RunnerEpoch                                   uint64
	CommandDigest, SpecDigest, PolicyDigest, ExecutablePath, ExecutableDigest string
	ExpectedOutcome                                                           string
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

// ApplyOperatorDecision is the lifecycle authority for the two human
// decisions.  Recording an approval in one transaction and entering merging
// in another would leave a restart-visible authority gap; the same applies to
// a rejection and its repair admission.  The caller supplies only the
// authenticated peer UID and a bounded, already-redacted reason digest.
type OperatorDecisionRequest struct {
	OperatorDecision
	ReasonDigest string
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
	if artifact.Document.Planner != nil || artifact.Document.ProviderResult != nil {
		if artifact.Document.Planner == nil || artifact.Document.ProviderResult == nil {
			return "", ErrEvidenceConflict
		}
		result, parsed, loadErr := s.LoadHistoricalProviderAttemptResult(ctx, *artifact.Document.ProviderResult)
		if loadErr != nil || result.Claim.Role != "planner" || result.Claim.Phase != domain.PhasePlanning || result.Claim.Ref != artifact.Ref || parsed.Planner == nil {
			return "", ErrEvidenceConflict
		}
		current := result.Claim.ExpectedVersion == artifact.ExpectedVersion && result.Claim.LeaderEpoch == artifact.Fence.LeaderEpoch && result.Claim.RunnerEpoch == artifact.Fence.RunnerEpoch
		if !current {
			reusable, reuseErr := s.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: artifact.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: artifact.ExpectedVersion, Fence: artifact.Fence})
			if reuseErr != nil || reusable.Key != *artifact.Document.ProviderResult || !reusable.Recovered {
				return "", ErrEvidenceConflict
			}
			result, parsed = reusable.Result, reusable.Parsed
		}
		canonical, _, canonicalErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: parsed.Provider, Planner: artifact.Document.Planner})
		stored, _, storedErr := phaseartifact.CanonicalTypedArtifact(phaseartifact.Parsed{Phase: domain.PhasePlanning, Provider: parsed.Provider, Planner: parsed.Planner})
		if canonicalErr != nil || storedErr != nil || !bytes.Equal(canonical, stored) {
			return "", ErrEvidenceConflict
		}
	}
	digest := sha256Digest(body)
	err = s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, artifact.Ref, artifact.ExpectedVersion, artifact.Fence); err != nil {
			return err
		}
		if artifact.Document.ProviderResult != nil {
			if err := assertNewestBoundResult(ctx, conn, artifact.Ref, domain.PhasePlanning, "planner", *artifact.Document.ProviderResult); err != nil {
				return err
			}
		}
		var existingDigest string
		err := conn.QueryRowContext(ctx, `SELECT digest FROM plans WHERE channel=? AND project_id=? AND ticket_id=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket).Scan(&existingDigest)
		if err == nil {
			if existingDigest == digest {
				return ensurePlanBinding(ctx, conn, artifact, digest)
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
		if err := ensurePlanBinding(ctx, conn, artifact, digest); err != nil {
			return err
		}
		return evidenceEvent(ctx, conn, artifact.Ref, artifact.ExpectedVersion, "plan_recorded", map[string]string{"digest": digest})
	})
	return digest, err
}

func assertNewestBoundResult(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, role string, key ProviderAttemptResultKey) error {
	var id int64
	var attempt int
	err := conn.QueryRowContext(ctx, `SELECT r.provider_attempt_id,a.attempt FROM provider_attempts a LEFT JOIN provider_attempt_results r ON r.provider_attempt_id=a.id WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.role=? AND a.finished_at IS NOT NULL ORDER BY a.attempt DESC,a.id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, phase, role).Scan(&id, &attempt)
	if err != nil || id != key.AttemptID || attempt != key.Attempt || key.Ref != ref || key.Phase != phase {
		return ErrEvidenceConflict
	}
	return nil
}

func ensurePlanBinding(ctx context.Context, conn *sql.Conn, artifact PlanArtifact, digest string) error {
	if artifact.Document.ProviderResult == nil {
		return nil
	}
	key := *artifact.Document.ProviderResult
	var id int64
	var attempt int
	var existingDigest string
	err := conn.QueryRowContext(ctx, `SELECT plan_digest,provider_attempt_id,provider_attempt FROM plan_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch).Scan(&existingDigest, &id, &attempt)
	if err == nil {
		if existingDigest == digest && id == key.AttemptID && attempt == key.Attempt {
			return nil
		}
		return ErrEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO plan_result_bindings(channel,project_id,ticket_id,plan_digest,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt) VALUES(?,?,?,?,?,?,?,?,?)`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, digest, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch, key.AttemptID, key.Attempt)
	return err
}

type reviewRepairAmendment struct {
	PriorRevision uint64
	Reason        string
	Requester     string
}

// reviewRepairVerificationAmendment exposes only the Store-authenticated
// amendment context created by a reviewer-owned final-review repair. It never
// trusts a Worker-provided reason or lets a builder repair alter verification.
func reviewRepairVerificationAmendment(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64) (reviewRepairAmendment, bool, error) {
	var result reviewRepairAmendment
	var transitionVersion uint64
	var state domain.State
	err := conn.QueryRowContext(ctx, `SELECT b.prior_verification_revision,b.amendment_reason,b.requester,b.transition_ticket_version,t.state
		FROM final_review_repair_boundaries b JOIN tickets t ON t.channel=b.channel AND t.project_id=b.project_id AND t.id=b.ticket_id
		WHERE b.channel=? AND b.project_id=? AND b.ticket_id=? AND b.target_state='verifying' AND b.transition_ticket_version<=?
		ORDER BY b.transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&result.PriorRevision, &result.Reason, &result.Requester, &transitionVersion, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return reviewRepairAmendment{}, false, nil
	}
	if err != nil {
		return reviewRepairAmendment{}, false, err
	}
	if state != domain.StateVerifying || transitionVersion == 0 || transitionVersion > version || result.PriorRevision == 0 || !boundedText(result.Reason, 2_000) || !boundedText(result.Requester, 200) {
		return reviewRepairAmendment{}, false, ErrEvidenceConflict
	}
	return result, true, nil
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
	if err := validOwnedFiles(artifact.OwnedFiles); err != nil || !validOID(artifact.CheckpointID) || artifact.Checkpoint.CommitOID != "" && (artifact.Checkpoint.CommitOID != artifact.CheckpointID || artifact.Checkpoint.ParentOID == "" || artifact.Checkpoint.TreeOID == "") {
		return VerificationRevision{}, fmt.Errorf("bounded verification checkpoint and owned files are required")
	}
	var providerVerify *phaseartifact.Verification
	var provider ProviderAttemptResult
	if artifact.ProviderResult != nil {
		result, parsed, loadErr := s.LoadHistoricalProviderAttemptResult(ctx, *artifact.ProviderResult)
		if loadErr != nil || result.Claim.Role != "reviewer" || result.Claim.Phase != domain.PhaseVerification || result.Claim.Ref != artifact.Ref || parsed.Verify == nil {
			return VerificationRevision{}, ErrEvidenceConflict
		}
		if result.Claim.ExpectedVersion != artifact.ExpectedVersion || result.Claim.LeaderEpoch != artifact.Fence.LeaderEpoch || result.Claim.RunnerEpoch != artifact.Fence.RunnerEpoch {
			reusable, reuseErr := s.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: artifact.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: artifact.ExpectedVersion, Fence: artifact.Fence})
			if reuseErr != nil || !reusable.Recovered || reusable.Key != *artifact.ProviderResult {
				return VerificationRevision{}, ErrEvidenceConflict
			}
		}
		intent, intentErr := workflowprompt.CanonicalVerificationIntentBytes(*parsed.Verify)
		proof, proofErr := workflowprompt.CanonicalVerificationProofBytes(*parsed.Verify)
		if intentErr != nil || proofErr != nil || !bytes.Equal(intent, artifact.Intent) || !bytes.Equal(proof, artifact.Proof) || !equalStringSlices(parsed.Verify.OwnedFiles, artifact.OwnedFiles) {
			return VerificationRevision{}, ErrEvidenceConflict
		}
		providerVerify = parsed.Verify
		provider = result
	} else {
		// A verification artifact without the typed provider declaration has no
		// command to authenticate against frozen configuration.
		return VerificationRevision{}, ErrEvidenceConflict
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
		if err := assertNewestBoundResult(ctx, conn, artifact.Ref, domain.PhaseVerification, "reviewer", *artifact.ProviderResult); err != nil {
			return err
		}
		_, commandBinding, err := authenticateVerificationCommandEvidence(ctx, conn, artifact, providerVerify)
		if err != nil {
			return err
		}
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT current_revision FROM verifications WHERE channel=? AND project_id=? AND ticket_id=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket).Scan(&current); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if artifact.AmendsRevision == 0 {
			amendment, found, amendmentErr := reviewRepairVerificationAmendment(ctx, conn, artifact.Ref, artifact.ExpectedVersion)
			if amendmentErr != nil {
				return amendmentErr
			}
			if found {
				artifact.AmendsRevision, artifact.Reason, artifact.Requester = amendment.PriorRevision, amendment.Reason, amendment.Requester
				result.Amends = amendment.PriorRevision
				if current == amendment.PriorRevision {
					// First persistence of the newly reviewed verification below.
				} else if current > amendment.PriorRevision {
					// A crash may occur after the amended revision is durable but
					// before the phase transition. A recovered exact provider result
					// must append only its live binding, never try to create a second
					// amendment revision or reject the boundary-derived metadata.
					var storedAmends uint64
					var storedReason, storedRequester, storedIntent, storedProof, storedCheckpoint string
					if err := conn.QueryRowContext(ctx, `SELECT COALESCE(amends_revision,0),amendment_reason,requester,intent_digest,proof_digest,checkpoint_id FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, current).Scan(&storedAmends, &storedReason, &storedRequester, &storedIntent, &storedProof, &storedCheckpoint); err != nil {
						return err
					}
					if storedAmends != amendment.PriorRevision || storedReason != amendment.Reason || storedRequester != amendment.Requester || storedIntent != intentDigest || storedProof != proofDigest || storedCheckpoint != artifact.CheckpointID {
						return ErrEvidenceConflict
					}
					result.Revision = current
					if err := ensureVerificationBinding(ctx, conn, artifact, current, provider); err != nil {
						return err
					}
					return ensureVerificationCommandBinding(ctx, conn, artifact.Ref, current, commandBinding)
				} else {
					return ErrEvidenceConflict
				}
			}
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
					if err := ensureVerificationBinding(ctx, conn, artifact, current, provider); err != nil {
						return err
					}
					if err := ensureVerificationCommandBinding(ctx, conn, artifact.Ref, current, commandBinding); err != nil {
						return err
					}
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
		_, err = conn.ExecContext(ctx, `INSERT INTO verification_revisions(channel, project_id, ticket_id, revision, ticket_version, leader_epoch, runner_epoch, intent_digest, intent_bytes, proof_digest, proof_bytes, owned_files_json, checkpoint_id, amends_revision, amendment_reason, requester, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, result.Revision, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch, intentDigest, artifact.Intent, proofDigest, artifact.Proof, string(owned), artifact.CheckpointID, nullableUint(artifact.AmendsRevision), artifact.Reason, artifact.Requester, now())
		if err != nil {
			return err
		}
		if err := ensureVerificationBinding(ctx, conn, artifact, result.Revision, provider); err != nil {
			return err
		}
		if err := ensureVerificationCommandBinding(ctx, conn, artifact.Ref, result.Revision, commandBinding); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO verifications(channel, project_id, ticket_id, intent_digest, proof_digest, current_revision) VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(channel, project_id, ticket_id) DO UPDATE SET intent_digest=excluded.intent_digest, proof_digest=excluded.proof_digest, current_revision=excluded.current_revision`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, intentDigest, proofDigest, result.Revision)
		if err != nil {
			return err
		}
		if artifact.Checkpoint.CommitOID != artifact.CheckpointID || !validOID(artifact.Checkpoint.ParentOID) || !validOID(artifact.Checkpoint.TreeOID) {
			return ErrEvidenceConflict
		}
		return evidenceEvent(ctx, conn, artifact.Ref, artifact.ExpectedVersion, "verification_recorded", map[string]any{"revision": result.Revision, "intent_digest": intentDigest, "proof_digest": proofDigest, "provider_attempt_id": artifact.ProviderResult.AttemptID, "provider_attempt": artifact.ProviderResult.Attempt, "provider_phase": artifact.ProviderResult.Phase, "checkpoint_commit": artifact.Checkpoint.CommitOID, "checkpoint_parent": artifact.Checkpoint.ParentOID, "checkpoint_tree": artifact.Checkpoint.TreeOID, "repository_command_semantic_key": commandBinding.Key.SemanticKey, "repository_command_claim_epoch": commandBinding.Key.ClaimEpoch, "repository_command_policy_digest": commandBinding.PolicyDigest, "prebuild_outcome": commandBinding.ExpectedOutcome})
	})
	return result, err
}

func ensureVerificationBinding(ctx context.Context, conn *sql.Conn, artifact VerificationArtifact, revision uint64, provider ProviderAttemptResult) error {
	if artifact.ProviderResult == nil {
		return nil
	}
	key := *artifact.ProviderResult
	if err := providerResultReachesFence(ctx, conn, key, provider, artifact.ExpectedVersion, artifact.Fence); err != nil {
		return ErrEvidenceConflict
	}
	var id int64
	var attempt int
	var commit, parent, tree string
	err := conn.QueryRowContext(ctx, `SELECT provider_attempt_id,provider_attempt,checkpoint_commit_oid,checkpoint_parent_oid,checkpoint_tree_oid FROM verification_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, revision, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch).Scan(&id, &attempt, &commit, &parent, &tree)
	if err == nil {
		if id == key.AttemptID && attempt == key.Attempt && commit == artifact.Checkpoint.CommitOID && parent == artifact.Checkpoint.ParentOID && tree == artifact.Checkpoint.TreeOID {
			return nil
		}
		return ErrEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO verification_result_bindings(channel,project_id,ticket_id,revision,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,checkpoint_commit_oid,checkpoint_parent_oid,checkpoint_tree_oid) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, revision, artifact.ExpectedVersion, artifact.Fence.LeaderEpoch, artifact.Fence.RunnerEpoch, key.AttemptID, key.Attempt, artifact.Checkpoint.CommitOID, artifact.Checkpoint.ParentOID, artifact.Checkpoint.TreeOID)
	return err
}

// RecordCandidate creates an immutable generation and always writes the full
// invalidation set, even when a prior gate was absent. Consumers can therefore
// reason from receipts rather than attempting to infer invalidation from NULLs.
func (s *Store) RecordCandidate(ctx context.Context, evidence CandidateEvidence) ([]InvalidationReceipt, error) {
	if err := evidence.Ref.Validate(); err != nil {
		return nil, err
	}
	if err := validateCandidate(evidence.Snapshot); err != nil || !boundedText(evidence.Reason, 2_000) || evidence.BuilderResult.AttemptID <= 0 || evidence.BuilderResult.Ref != evidence.Ref || evidence.BuilderResult.Phase != domain.PhaseBuild || evidence.BuilderResult.Attempt <= 0 || !validOID(evidence.Commit.CommitOID) || !validOID(evidence.Commit.ParentOID) || !validOID(evidence.Commit.TreeOID) || evidence.Commit.CommitOID != evidence.Snapshot.HeadSHA || evidence.Commit.TreeOID != evidence.Snapshot.TreeSHA {
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
	if builder.Claim.ExpectedVersion != evidence.ExpectedVersion || builder.Claim.LeaderEpoch != evidence.Fence.LeaderEpoch || builder.Claim.RunnerEpoch != evidence.Fence.RunnerEpoch {
		reusable, reuseErr := s.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: evidence.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: evidence.ExpectedVersion, Fence: evidence.Fence})
		if reuseErr != nil || !reusable.Recovered || reusable.Key != evidence.BuilderResult {
			return nil, ErrEvidenceConflict
		}
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
		liveState := domain.State(state)
		if (liveState != domain.StateBuilding && liveState != domain.StatePublishing) || source != evidence.Snapshot.SourceDigest {
			return ErrEvidenceConflict
		}
		if liveState == domain.StatePublishing && evidence.Snapshot.Generation == 0 {
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
		var revision uint64
		var intent, proof, owned, checkpoint string
		var intentBytes, proofBytes []byte
		if err := conn.QueryRowContext(ctx, `SELECT r.revision,r.intent_digest,r.intent_bytes,r.proof_digest,r.proof_bytes,r.owned_files_json,r.checkpoint_id FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision WHERE v.channel=? AND v.project_id=? AND v.ticket_id=? AND v.intent_digest=r.intent_digest AND v.proof_digest=r.proof_digest`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&revision, &intent, &intentBytes, &proof, &proofBytes, &owned, &checkpoint); err != nil || revision == 0 || intent != evidence.Snapshot.VerificationIntentDigest || proof != evidence.Snapshot.ProofDigest || sha256Digest(intentBytes) != intent || sha256Digest(proofBytes) != proof || !validOID(checkpoint) || evidence.Commit.ParentOID != checkpoint {
			return ErrEvidenceConflict
		}
		var ownedFiles []string
		if json.Unmarshal([]byte(owned), &ownedFiles) != nil || validOwnedFiles(ownedFiles) != nil {
			return ErrEvidenceConflict
		}
		if _, err := loadVerificationCommandBinding(ctx, conn, evidence.Ref, revision); err != nil {
			return ErrEvidenceConflict
		}
		_, commandBinding, err := authenticateCandidateCommandEvidence(ctx, conn, evidence, builder, intent, proof, checkpoint)
		if err != nil {
			return err
		}
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation), 0) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&current); err != nil {
			return err
		}
		// A red CI transition opens one immutable repair lineage. A Builder
		// result in the old generation must not bypass that pending successor.
		var pendingRepair int
		if current > 0 {
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND predecessor_generation=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, current).Scan(&pendingRepair); err != nil {
				return err
			}
			if pendingRepair != 0 && evidence.Snapshot.Generation != current+1 {
				return ErrEvidenceConflict
			}
		}
		if evidence.Snapshot.Generation == 0 && current > 0 {
			var existing domain.CandidateSnapshot
			if err := conn.QueryRowContext(ctx, `SELECT generation,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, current).Scan(&existing.Generation, &existing.BaseSHA, &existing.HeadSHA, &existing.TreeSHA, &existing.SourceDigest, &existing.VerificationIntentDigest, &existing.ProofDigest, &existing.CommandPolicyDigest, &existing.BuilderEvidenceDigest); err != nil {
				return ErrEvidenceConflict
			}
			candidate := evidence.Snapshot
			candidate.Generation = current
			if existing == candidate {
				if err := ensureCandidateBinding(ctx, conn, evidence, current, builder); err != nil {
					return err
				}
				return ensureCandidateCommandBinding(ctx, conn, evidence.Ref, current, commandBinding)
			}
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
			if err := ensureCandidateBinding(ctx, conn, evidence, current, builder); err != nil {
				return err
			}
			return ensureCandidateCommandBinding(ctx, conn, evidence.Ref, current, commandBinding)
		}
		if liveState == domain.StatePublishing {
			// Publishing recovery can only append a live binding to the latest
			// immutable candidate. It must never mint a new generation, receipts,
			// or a post-transition invalidation.
			return ErrEvidenceConflict
		}
		if evidence.Snapshot.Generation != current+1 {
			return ErrEvidenceConflict
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO candidate_snapshots(channel, project_id, ticket_id, generation, ticket_version, leader_epoch, runner_epoch, base_sha, head_sha, tree_sha, source_digest, verification_intent_digest, proof_digest, command_policy_digest, builder_evidence_digest, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, evidence.Snapshot.Generation, evidence.ExpectedVersion, evidence.Fence.LeaderEpoch, evidence.Fence.RunnerEpoch, evidence.Snapshot.BaseSHA, evidence.Snapshot.HeadSHA, evidence.Snapshot.TreeSHA, evidence.Snapshot.SourceDigest, evidence.Snapshot.VerificationIntentDigest, evidence.Snapshot.ProofDigest, evidence.Snapshot.CommandPolicyDigest, evidence.Snapshot.BuilderEvidenceDigest, now())
		if err != nil {
			return err
		}
		if err := ensureCandidateBinding(ctx, conn, evidence, evidence.Snapshot.Generation, builder); err != nil {
			return err
		}
		if err := ensureCandidateCommandBinding(ctx, conn, evidence.Ref, evidence.Snapshot.Generation, commandBinding); err != nil {
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
		return evidenceEvent(ctx, conn, evidence.Ref, evidence.ExpectedVersion, "candidate_recorded", map[string]any{"generation": evidence.Snapshot.Generation, "head": evidence.Snapshot.HeadSHA, "builder_attempt_id": evidence.BuilderResult.AttemptID, "builder_attempt": evidence.BuilderResult.Attempt, "builder_evidence_digest": evidence.Snapshot.BuilderEvidenceDigest, "command_policy_digest": evidence.Snapshot.CommandPolicyDigest, "repository_command_semantic_key": commandBinding.Key.SemanticKey, "repository_command_claim_epoch": commandBinding.Key.ClaimEpoch})
	})
	return receipts, err
}

func ensureCandidateBinding(ctx context.Context, conn *sql.Conn, evidence CandidateEvidence, generation uint64, provider ProviderAttemptResult) error {
	if err := providerResultReachesFence(ctx, conn, evidence.BuilderResult, provider, evidence.ExpectedVersion, evidence.Fence); err != nil {
		return ErrEvidenceConflict
	}
	var id int64
	var attempt int
	var parent string
	err := conn.QueryRowContext(ctx, `SELECT provider_attempt_id,provider_attempt,commit_parent_oid FROM candidate_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, generation, evidence.ExpectedVersion, evidence.Fence.LeaderEpoch, evidence.Fence.RunnerEpoch).Scan(&id, &attempt, &parent)
	if err == nil {
		if id == evidence.BuilderResult.AttemptID && attempt == evidence.BuilderResult.Attempt && parent == evidence.Commit.ParentOID {
			return nil
		}
		return ErrEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO candidate_result_bindings(channel,project_id,ticket_id,generation,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,commit_parent_oid) VALUES(?,?,?,?,?,?,?,?,?,?)`, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket, generation, evidence.ExpectedVersion, evidence.Fence.LeaderEpoch, evidence.Fence.RunnerEpoch, evidence.BuilderResult.AttemptID, evidence.BuilderResult.Attempt, evidence.Commit.ParentOID)
	return err
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
		var state domain.State
		var mergeMode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT state,merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket).Scan(&state, &mergeMode); err != nil {
			return err
		}
		// A decision is meaningful only after the exact candidate has reached
		// its final-review waiting boundary.  In particular, no caller can
		// pre-approve a building/reviewing head and carry that authority over a
		// later review or candidate refresh.
		if (decision.Decision == "approved" && (state != domain.StateWaitingApproval || mergeMode != domain.MergeGuarded)) || (decision.Decision == "rejected" && state != domain.StateWaitingApproval && state != domain.StateWaitingManualMerge) {
			return ErrEvidenceConflict
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

// ApplyOperatorDecision records an exact-head decision and its normative
// transition atomically.  It deliberately re-reads both the current candidate
// and published PR identity inside SQLite: a CLI label or historical candidate
// can never authorize a merge after a correction has replaced the review head.
func (s *Store) ApplyOperatorDecision(ctx context.Context, request OperatorDecisionRequest) (TransitionResult, error) {
	decision := request.OperatorDecision
	if err := decision.Ref.Validate(); err != nil || !validOID(decision.ReviewedHead) || decision.OperatorUID == 0 || (decision.Decision != "approved" && decision.Decision != "rejected") {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if request.ReasonDigest != "" && !validSHA256(request.ReasonDigest) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var mode domain.MergeMode
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,merge_mode,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket).Scan(&state, &mode, &version, &runner); err != nil {
			return err
		}
		if err := s.currentFence(ctx, conn, decision.Ref.Channel, version, runner, decision.Fence); err != nil || version != decision.ExpectedVersion {
			return ErrStaleFence
		}
		var head string
		if err := conn.QueryRowContext(ctx, `SELECT head_sha FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY generation DESC LIMIT 1`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket).Scan(&head); err != nil || head != decision.ReviewedHead {
			return ErrStaleFence
		}
		// Publication evidence is immutable, but it is the sole durable PR
		// identity.  Requiring its exact source head prevents accepting a
		// decision for a local candidate that was never the reviewed PR.
		var prHead string
		if err := conn.QueryRowContext(ctx, `SELECT github_head_oid FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY rowid DESC LIMIT 1`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket).Scan(&prHead); err != nil || prHead != head {
			return ErrEvidenceConflict
		}
		var prior string
		err := conn.QueryRowContext(ctx, `SELECT decision FROM approvals WHERE channel=? AND project_id=? AND ticket_id=? AND reviewed_head=? AND operator_uid=? AND invalidated=0`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket, head, decision.OperatorUID).Scan(&prior)
		if err == nil && prior != decision.Decision {
			return ErrEvidenceConflict
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// A client can lose the daemon response after the transaction commits.
		// Replaying the same authenticated decision must report the already
		// durable outcome, never create a second decision/effect or refuse a
		// command that the operator cannot safely classify.
		if err == nil && prior == decision.Decision {
			if (decision.Decision == "approved" && state == domain.StateMerging && mode == domain.MergeGuarded) ||
				(decision.Decision == "rejected" && state == domain.StateBuilding) {
				result.Version = version
				return nil
			}
		}
		if (decision.Decision == "approved" && (state != domain.StateWaitingApproval || mode != domain.MergeGuarded)) ||
			(decision.Decision == "rejected" && state != domain.StateWaitingApproval && state != domain.StateWaitingManualMerge) {
			return ErrEvidenceConflict
		}
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := conn.ExecContext(ctx, `INSERT INTO approvals(channel,project_id,ticket_id,reviewed_head,operator_uid,decision,invalidated,created_at,ticket_version) VALUES(?,?,?,?,?,?,0,?,?)`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket, head, decision.OperatorUID, decision.Decision, now(), version); err != nil {
				return err
			}
		}
		to := domain.StateMerging
		if decision.Decision == "rejected" {
			to = domain.StateBuilding
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=NULL,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, to, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket, state, version, runner)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		payload := map[string]string{"head": head}
		if request.ReasonDigest != "" {
			payload["reason_digest"] = request.ReasonDigest
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, decision.Ref.Channel, decision.Ref.Project, decision.Ref.Ticket, version+1, "operator_"+decision.Decision, state, to, string(body), now())
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
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
		var err error
		used, err = s.consumeBudgetFrom(ctx, conn, use)
		return err
	})
	return used, err
}

func (s *Store) consumeBudgetFrom(ctx context.Context, conn *sql.Conn, use BudgetUse) (int, error) {
	return s.consumeBudgetAtFence(ctx, conn, use, true)
}

// consumeBudgetDuringTransition is for a lifecycle transaction that has
// already drained external mutations. DrainExternalMutations intentionally
// revokes the pre-transition fence, so re-entering assertTicketFence here
// would reject the transition's own exact authority. The caller still holds
// the Store write transaction and this helper rechecks the ticket version,
// runner epoch, and daemon leader before charging the bounded counter.
func (s *Store) consumeBudgetDuringTransition(ctx context.Context, conn *sql.Conn, use BudgetUse) (int, error) {
	return s.consumeBudgetAtFence(ctx, conn, use, false)
}

func (s *Store) consumeBudgetAtFence(ctx context.Context, conn *sql.Conn, use BudgetUse, requireUnrevoked bool) (int, error) {
	limit := 0
	if use.Kind == "correction" {
		limit = 2
	} else if use.Kind == "fallback" {
		limit = 1
	}
	if limit == 0 || !boundedText(use.RequestID, 300) {
		return 0, ErrEvidenceConflict
	}
	if requireUnrevoked {
		if err := s.assertTicketFence(ctx, conn, use.Ref, use.ExpectedVersion, use.Fence); err != nil {
			return 0, err
		}
	} else {
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket).Scan(&version, &runner); err != nil || version != use.ExpectedVersion {
			return 0, ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, use.Ref.Channel, version, runner, use.Fence); err != nil {
			return 0, err
		}
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO ticket_counters(channel, project_id, ticket_id, kind, used, limit_count) VALUES (?, ?, ?, ?, 0, ?) ON CONFLICT(channel, project_id, ticket_id, kind) DO NOTHING`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind, limit); err != nil {
		return 0, err
	}
	result, err := conn.ExecContext(ctx, `INSERT INTO ticket_budget_uses(channel, project_id, ticket_id, kind, request_id, ticket_version, leader_epoch, runner_epoch, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(channel, project_id, ticket_id, kind, request_id) DO NOTHING`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind, use.RequestID, use.ExpectedVersion, use.Fence.LeaderEpoch, use.Fence.RunnerEpoch, now())
	if err != nil {
		return 0, err
	}
	if inserted, _ := result.RowsAffected(); inserted == 1 {
		updated, err := conn.ExecContext(ctx, `UPDATE ticket_counters SET used=used+1 WHERE channel=? AND project_id=? AND ticket_id=? AND kind=? AND used<limit_count`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind)
		if err != nil {
			return 0, err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return 0, ErrBudgetExhausted
		}
		if err := evidenceEvent(ctx, conn, use.Ref, use.ExpectedVersion, "budget_"+use.Kind, map[string]string{"request_id": use.RequestID}); err != nil {
			return 0, err
		}
	}
	var used int
	if err := conn.QueryRowContext(ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind=?`, use.Ref.Channel, use.Ref.Project, use.Ref.Ticket, use.Kind).Scan(&used); err != nil {
		return 0, err
	}
	return used, nil
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

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
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
