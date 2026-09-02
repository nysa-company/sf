package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// VerificationAmendment is the Store-authenticated request that places a
// ticket back in verification. It is deliberately a projection of immutable
// Builder evidence, not a caller-authored amendment record.
type VerificationAmendment struct {
	Ref                     domain.TicketRef
	TransitionTicketVersion uint64
	Prior                   VerificationRevision
	BuilderResult           ProviderAttemptResultKey
	BuilderTypedSHA256      string
	ProposedDigest          string
	ProposedCommand         []string
	Reason                  string
	Requester               string
	ConsumedVersion         uint64
	Fence                   domain.Fence
	BudgetRequestID         string
}

type VerificationAmendmentDecision string

const (
	VerificationAmendmentAccepted VerificationAmendmentDecision = "accepted"
	VerificationAmendmentRejected VerificationAmendmentDecision = "rejected"
)

// TransitionVerificationAmendmentRequest is the only Builder -> Reviewer
// amendment boundary. It binds the completed Builder artifact, the exact
// pre-amendment verification revision, and the bounded correction charge in
// one transaction with building -> verifying.
func (s *Store) TransitionVerificationAmendmentRequest(ctx context.Context, transition Transition, key ProviderAttemptResultKey) (TransitionResult, error) {
	if transition.From != domain.StateBuilding || transition.To != domain.StateVerifying || transition.Trigger != "verification_amendment_requested" || transition.EventPayload != "{}" || key.Ref != transition.Ref || key.Phase != domain.PhaseBuild || key.AttemptID <= 0 || key.Attempt <= 0 {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		result, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
		if err != nil || result.Claim.Ref != transition.Ref || result.Claim.Phase != domain.PhaseBuild || result.Claim.Role != "builder" || result.Claim.Attempt != key.Attempt || result.Claim.ID != key.AttemptID || parsed.Builder == nil || parsed.Builder.AmendmentRequest == nil {
			return ErrEvidenceConflict
		}
		// The Builder result may predate a daemon takeover, but it must still
		// reach this exact live Building fence through the signed recovery
		// ledger. The immutable claim remains historical; only the amendment
		// endpoint is advanced by this transition.
		if err := providerResultReachesFence(ctx, conn, key, result, version, transition.Fence); err != nil {
			return ErrEvidenceConflict
		}
		if err := assertNewestBoundResult(ctx, conn, transition.Ref, domain.PhaseBuild, "builder", key); err != nil {
			return ErrEvidenceConflict
		}
		request := parsed.Builder.AmendmentRequest
		if request == nil {
			return ErrEvidenceConflict
		}
		proposedCommand, err := canonicalVerificationAmendmentCommand(request.ProposedCommand)
		if err != nil || request.OldProofDigest == "" || !validSHA256(request.OldProofDigest) || !validSHA256(request.ProposedDigest) || request.OldProofDigest == request.ProposedDigest || !boundedText(request.Reason, 2_000) {
			return ErrEvidenceConflict
		}
		var prior VerificationRevision
		var current uint64
		var owned string
		if err := conn.QueryRowContext(ctx, `SELECT v.current_revision,r.intent_digest,r.proof_digest,r.owned_files_json,r.checkpoint_id
			FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
			WHERE v.channel=? AND v.project_id=? AND v.ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&current, &prior.IntentDigest, &prior.ProofDigest, &owned, &prior.CheckpointID); err != nil || current == 0 {
			return ErrEvidenceConflict
		}
		prior.Revision = current
		if request.OldProofDigest != prior.ProofDigest || request.ProposedDigest == prior.ProofDigest || !validSHA256(prior.IntentDigest) || !validSHA256(prior.ProofDigest) || !validOID(prior.CheckpointID) {
			return ErrEvidenceConflict
		}
		// The normative boundary is Building -> Verifying, before any candidate
		// or publication can be derived from this proof. Refuse rather than
		// silently leaving downstream authority attached to the old proof.
		if err := assertNoVerificationAmendmentDownstream(ctx, conn, transition.Ref); err != nil {
			return err
		}
		requestID := fmt.Sprintf("verification-amendment/%d/%s", key.AttemptID, result.TypedSHA256)
		if _, err := s.consumeBudgetDuringTransition(ctx, conn, BudgetUse{Ref: transition.Ref, ExpectedVersion: version, Fence: transition.Fence, Kind: "correction", RequestID: requestID}); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO verification_amendment_requests(channel,project_id,ticket_id,transition_ticket_version,prior_verification_revision,prior_intent_digest,prior_proof_digest,prior_checkpoint_id,builder_attempt_id,builder_attempt,builder_result_phase,builder_result_role,builder_typed_sha256,proposed_digest,proposed_command_json,amendment_reason,requester,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,correction_budget_kind,correction_budget_request_id,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, prior.Revision, prior.IntentDigest, prior.ProofDigest, prior.CheckpointID, key.AttemptID, key.Attempt, domain.PhaseBuild, "builder", result.TypedSHA256, request.ProposedDigest, string(proposedCommand), request.Reason, "builder", version, transition.Fence.LeaderEpoch, runner, "correction", requestID, now())
		return err
	})
}

func assertNoVerificationAmendmentDownstream(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) error {
	for _, table := range []string{"candidate_snapshots", "publication_evidence", "ci_observations", "merge_intents", "manual_merge_observations"} {
		var count int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != 0 {
			return ErrEvidenceConflict
		}
	}
	return nil
}

// countUnresolvedVerificationAmendmentRequests makes request selection fail
// closed when more than one immutable request could explain a decision. A
// LIMIT 1 lookup would otherwise silently choose one row and widen recovery
// authority over an ambiguous history.
func countUnresolvedVerificationAmendmentRequests(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, beforeVersion uint64) (int, error) {
	if beforeVersion == 0 {
		return 0, ErrEvidenceConflict
	}
	var unresolved int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM verification_amendment_requests a
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.transition_ticket_version<?
		AND NOT EXISTS (
			SELECT 1 FROM events resolved
			WHERE resolved.channel=a.channel
			AND resolved.project_id=a.project_id
			AND resolved.ticket_id=a.ticket_id
			AND resolved.ticket_version>a.transition_ticket_version
			AND resolved.ticket_version<?
			AND resolved.trigger IN ('amendment_accepted','amendment_rejected')
			AND resolved.from_state='verifying' AND resolved.to_state='building' AND resolved.payload='{}'
		)`, ref.Channel, ref.Project, ref.Ticket, beforeVersion, beforeVersion).Scan(&unresolved)
	return unresolved, err
}

// PendingVerificationAmendment returns the exact amendment being reviewed at
// the current fence. It does not turn a historical row into launch authority.
func (s *Store) PendingVerificationAmendment(ctx context.Context, ref domain.TicketRef, expected uint64, fence domain.Fence) (VerificationAmendment, error) {
	if ref.Validate() != nil || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	if err := s.AssertTicketFence(ctx, ref, expected, fence); err != nil {
		return VerificationAmendment{}, err
	}
	value, err := s.loadVerificationAmendment(ctx, s.db, ref, expected, fence)
	if !errors.Is(err, ErrNotFound) {
		return value, err
	}
	return s.loadRecoveredPendingVerificationAmendment(ctx, s.db, ref, expected, fence)
}

// loadRecoveredPendingVerificationAmendment carries an immutable amendment
// request across runner recovery. The request remains keyed to the exact
// building -> verifying endpoint, including its immutable fence. A decision
// event resolves a request, so a current verifying ticket must have exactly
// one unresolved row. Every source
// and budget fact is then rehydrated at the original endpoint before the exact
// recovery/control lineage is authenticated to the current fence.
func (s *Store) loadRecoveredPendingVerificationAmendment(ctx context.Context, q rowQueryer, ref domain.TicketRef, expected uint64, fence domain.Fence) (VerificationAmendment, error) {
	var state domain.State
	if err := q.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state); err != nil {
		return VerificationAmendment{}, err
	}
	var unresolved int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM verification_amendment_requests a
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.transition_ticket_version<=?
		AND NOT EXISTS (
			SELECT 1 FROM events e
			WHERE e.channel=a.channel AND e.project_id=a.project_id AND e.ticket_id=a.ticket_id
			AND e.ticket_version>a.transition_ticket_version
			AND e.trigger IN ('amendment_accepted','amendment_rejected')
		)`, ref.Channel, ref.Project, ref.Ticket, expected).Scan(&unresolved); err != nil {
		return VerificationAmendment{}, err
	}
	if unresolved == 0 {
		return VerificationAmendment{}, ErrNotFound
	}
	if unresolved != 1 || state != domain.StateVerifying {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	var transitionVersion, consumedVersion, consumedLeader, consumedRunner uint64
	if err := q.QueryRowContext(ctx, `SELECT transition_ticket_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch
		FROM verification_amendment_requests
		WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version<=?
		AND NOT EXISTS (
			SELECT 1 FROM events e
			WHERE e.channel=verification_amendment_requests.channel AND e.project_id=verification_amendment_requests.project_id AND e.ticket_id=verification_amendment_requests.ticket_id
			AND e.ticket_version>verification_amendment_requests.transition_ticket_version
			AND e.trigger IN ('amendment_accepted','amendment_rejected')
		)
		ORDER BY transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, expected).Scan(&transitionVersion, &consumedVersion, &consumedLeader, &consumedRunner); err != nil {
		return VerificationAmendment{}, err
	}
	if transitionVersion == 0 || consumedVersion == 0 || transitionVersion != consumedVersion+1 || consumedLeader == 0 || consumedRunner == 0 || transitionVersion > expected {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	originalFence := domain.Fence{LeaderEpoch: consumedLeader, RunnerEpoch: consumedRunner}
	value, err := s.loadVerificationAmendment(ctx, q, ref, transitionVersion, originalFence)
	if err != nil {
		return VerificationAmendment{}, err
	}
	var requestEvents int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger='verification_amendment_requested' AND from_state='building' AND to_state='verifying' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket, transitionVersion).Scan(&requestEvents); err != nil || requestEvents != 1 {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	if err := validateRunnerRecoveryLedger(ctx, q, ref, transitionVersion, originalFence.RunnerEpoch, originalFence.LeaderEpoch, expected, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	// TransitionTicketVersion and Fence are the immutable Builder->verifying
	// endpoint. Keep both intact across recovery: repository-command and
	// checkpoint identities bind that endpoint, not mutable live counters.
	return value, nil
}

// loadVerificationAmendment rehydrates every immutable source named by an
// amendment row. The row itself is append-only, but it must not become a
// standalone authority: a durable corruption must fail closed rather than
// retarget the Builder request, prior proof, or correction charge.
func (s *Store) loadVerificationAmendment(ctx context.Context, q rowQueryer, ref domain.TicketRef, expected uint64, fence domain.Fence) (VerificationAmendment, error) {
	var value VerificationAmendment
	value.Ref, value.TransitionTicketVersion, value.Fence = ref, expected, fence
	var builderID int64
	var builderPhase domain.Phase
	var builderRole string
	var proposedCommandJSON []byte
	err := q.QueryRowContext(ctx, `SELECT a.prior_verification_revision,a.prior_intent_digest,a.prior_proof_digest,a.prior_checkpoint_id,a.builder_attempt_id,a.builder_attempt,a.builder_typed_sha256,a.proposed_digest,a.proposed_command_json,a.amendment_reason,a.requester,a.consumed_ticket_version,a.consumed_leader_epoch,a.consumed_runner_epoch,a.correction_budget_request_id,a.builder_result_phase,a.builder_result_role
		FROM verification_amendment_requests a
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.transition_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, expected).Scan(&value.Prior.Revision, &value.Prior.IntentDigest, &value.Prior.ProofDigest, &value.Prior.CheckpointID, &builderID, &value.BuilderResult.Attempt, &value.BuilderTypedSHA256, &value.ProposedDigest, &proposedCommandJSON, &value.Reason, &value.Requester, &value.ConsumedVersion, &value.Fence.LeaderEpoch, &value.Fence.RunnerEpoch, &value.BudgetRequestID, &builderPhase, &builderRole)
	if errors.Is(err, sql.ErrNoRows) {
		return VerificationAmendment{}, ErrNotFound
	}
	if err != nil {
		return VerificationAmendment{}, err
	}
	if _, err := canonicalVerificationAmendmentCommandJSON(proposedCommandJSON, &value.ProposedCommand); err != nil {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	value.BuilderResult = ProviderAttemptResultKey{AttemptID: builderID, Ref: ref, Phase: builderPhase, Attempt: value.BuilderResult.Attempt}
	if value.Fence != fence || value.Prior.Revision == 0 || !validSHA256(value.Prior.IntentDigest) || !validSHA256(value.Prior.ProofDigest) || !validOID(value.Prior.CheckpointID) || value.BuilderResult.Phase != domain.PhaseBuild || builderRole != "builder" || value.BuilderResult.AttemptID <= 0 || value.BuilderResult.Attempt <= 0 || !validSHA256(value.BuilderTypedSHA256) || !validSHA256(value.ProposedDigest) || !boundedText(value.Reason, 2_000) || value.Requester != "builder" || value.ConsumedVersion+1 != expected || !boundedText(value.BudgetRequestID, 300) {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	var intent, proof, checkpoint string
	if err := q.QueryRowContext(ctx, `SELECT intent_digest,proof_digest,checkpoint_id
		FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, ref.Channel, ref.Project, ref.Ticket, value.Prior.Revision).Scan(&intent, &proof, &checkpoint); err != nil || intent != value.Prior.IntentDigest || proof != value.Prior.ProofDigest || checkpoint != value.Prior.CheckpointID {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, value.BuilderResult)
	if err != nil || builder.Claim.Ref != ref || builder.Claim.Phase != domain.PhaseBuild || builder.Claim.Role != "builder" || builder.Claim.ID != value.BuilderResult.AttemptID || builder.Claim.Attempt != value.BuilderResult.Attempt || builder.TypedSHA256 != value.BuilderTypedSHA256 || parsed.Builder == nil || parsed.Builder.AmendmentRequest == nil {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	if value.BudgetRequestID != fmt.Sprintf("verification-amendment/%d/%s", value.BuilderResult.AttemptID, value.BuilderTypedSHA256) {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	// This validates the immutable Builder source at the endpoint consumed by
	// the request.  The request transition has already advanced the live ticket
	// to Verifying, so requiring *live* authority here would make every honest
	// request unreadable immediately after it is committed.  The exact request
	// event and (on recovery) the signed ledger from this endpoint to live are
	// checked by the callers; this historical check deliberately does not bless
	// an unrecorded later fence.
	if err := providerResultReachesFenceAt(ctx, q, value.BuilderResult, builder, value.ConsumedVersion, value.Fence, false); err != nil {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	request := parsed.Builder.AmendmentRequest
	command, err := canonicalVerificationAmendmentCommand(request.ProposedCommand)
	if err != nil || request.OldProofDigest != value.Prior.ProofDigest || request.ProposedDigest != value.ProposedDigest || request.Reason != value.Reason || string(command) != string(proposedCommandJSON) {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	var requestEvents, stateChanges int
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN trigger='verification_amendment_requested' AND from_state='building' AND to_state='verifying' AND payload='{}' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0)
		FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, expected).Scan(&requestEvents, &stateChanges); err != nil || requestEvents != 1 || stateChanges != 1 {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	var budgetCount int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses
		WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, value.BudgetRequestID, value.ConsumedVersion, value.Fence.LeaderEpoch, value.Fence.RunnerEpoch).Scan(&budgetCount); err != nil || budgetCount != 1 {
		return VerificationAmendment{}, ErrEvidenceConflict
	}
	return value, nil
}

func canonicalVerificationAmendmentCommand(value []string) ([]byte, error) {
	if len(value) == 0 || len(value) > 32 {
		return nil, ErrEvidenceConflict
	}
	for _, part := range value {
		if !boundedText(part, 4_096) {
			return nil, ErrEvidenceConflict
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) < 3 || len(encoded) > 16_384 {
		return nil, ErrEvidenceConflict
	}
	return encoded, nil
}

func canonicalVerificationAmendmentCommandJSON(encoded []byte, target *[]string) ([]byte, error) {
	if len(encoded) < 3 || len(encoded) > 16_384 || json.Unmarshal(encoded, target) != nil {
		return nil, ErrEvidenceConflict
	}
	canonical, err := canonicalVerificationAmendmentCommand(*target)
	if err != nil || string(canonical) != string(encoded) {
		return nil, ErrEvidenceConflict
	}
	return canonical, nil
}

// VerificationAmendmentDecision classifies an immutable fresh Reviewer result
// against the durable request. Returning the old proof rejects without
// changing it; returning the exact Builder-bound proposed proof accepts it.
// Any third digest is malformed and must take the typed fail-closed path.
func (s *Store) VerificationAmendmentDecision(ctx context.Context, ref domain.TicketRef, expected uint64, fence domain.Fence, key ProviderAttemptResultKey) (VerificationAmendmentDecision, error) {
	if key.Ref != ref || key.Phase != domain.PhaseVerification || key.AttemptID <= 0 || key.Attempt <= 0 {
		return "", ErrEvidenceConflict
	}
	value, err := s.PendingVerificationAmendment(ctx, ref, expected, fence)
	if err != nil {
		return "", err
	}
	return s.verificationAmendmentDecisionFrom(ctx, s.db, value, ref, expected, fence, key)
}

func (s *Store) verificationAmendmentDecisionFrom(ctx context.Context, q rowQueryer, amendment VerificationAmendment, ref domain.TicketRef, expected uint64, fence domain.Fence, key ProviderAttemptResultKey) (VerificationAmendmentDecision, error) {
	return s.verificationAmendmentDecisionFromAt(ctx, q, amendment, ref, expected, fence, key, true)
}

func (s *Store) verificationAmendmentDecisionFromAt(ctx context.Context, q rowQueryer, amendment VerificationAmendment, ref domain.TicketRef, expected uint64, fence domain.Fence, key ProviderAttemptResultKey, requireLiveAuthority bool) (VerificationAmendmentDecision, error) {
	result, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, key)
	if err != nil || result.Claim.Ref != ref || result.Claim.Phase != domain.PhaseVerification || result.Claim.Role != "reviewer" || result.Claim.ID != key.AttemptID || result.Claim.Attempt != key.Attempt || amendment.ConsumedVersion == ^uint64(0) || amendment.TransitionTicketVersion != amendment.ConsumedVersion+1 || result.Claim.ExpectedVersion < amendment.TransitionTicketVersion || expected < result.Claim.ExpectedVersion || parsed.Verify == nil {
		return "", ErrEvidenceConflict
	}
	// The amendment Reviewer may be launched at the original request endpoint or
	// after one or more signed recoveries while the request remains unresolved.
	// Prove that its claim descends from that exact request before proving it to
	// the decision's live fence; a pre-amendment Reviewer cannot satisfy either
	// direction of this lineage.
	if err := verificationAmendmentRequestReachesReviewer(ctx, q, ref, amendment, result); err != nil {
		return "", ErrEvidenceConflict
	}
	// The completed result then crosses only its signed runner-recovery chain to
	// the live decision endpoint. This admits response-loss replay without
	// treating recovery counters as independent review authority.
	if err := validateRunnerRecoveryLedgerPrefix(ctx, q, key.Ref, result.Claim.ExpectedVersion, result.Claim.RunnerEpoch, result.Claim.LeaderEpoch, expected, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
		return "", ErrEvidenceConflict
	}
	if requireLiveAuthority && validateRunnerRecoveryAuthority(ctx, q, key.Ref, expected, fence) != nil {
		return "", ErrEvidenceConflict
	}
	if err := assertNewestBoundResult(ctx, q, ref, domain.PhaseVerification, "reviewer", key); err != nil {
		return "", ErrEvidenceConflict
	}
	if !equalStringSlices(parsed.Verify.Command, amendment.ProposedCommand) {
		return "", ErrEvidenceConflict
	}
	proof, err := canonicalVerificationProofDigest(*parsed.Verify)
	if err != nil || result.Claim.Binding.Identity.Family == "" {
		return "", ErrEvidenceConflict
	}
	if err := amendmentReviewerIndependent(ctx, q, amendment, result); err != nil {
		return "", err
	}
	if proof == amendment.Prior.ProofDigest {
		return VerificationAmendmentRejected, nil
	}
	if proof != amendment.ProposedDigest {
		return "", ErrEvidenceConflict
	}
	return VerificationAmendmentAccepted, nil
}

func canonicalVerificationProofDigest(value phaseartifact.Verification) (string, error) {
	proof, err := workflowprompt.CanonicalVerificationProofBytes(value)
	if err != nil {
		return "", err
	}
	return sha256Digest(proof), nil
}

// builderVerificationAmendmentForRecord derives amendment metadata only from
// the durable Builder request. It also proves the new Reviewer is independent
// of the Builder family before a new verification revision can be appended.
func (s *Store) builderVerificationAmendmentForRecord(ctx context.Context, conn *sql.Conn, artifact VerificationArtifact, reviewer ProviderAttemptResult, verify *phaseartifact.Verification, proofDigest string) (VerificationAmendment, bool, error) {
	value, err := s.loadPendingVerificationAmendmentAtFence(ctx, conn, artifact.Ref, artifact.ExpectedVersion, artifact.Fence)
	if errors.Is(err, ErrNotFound) {
		return VerificationAmendment{}, false, nil
	}
	if err != nil {
		return VerificationAmendment{}, false, err
	}
	if proofDigest != value.ProposedDigest || artifact.AmendsRevision != 0 || artifact.Reason != "" || artifact.Requester != "" || artifact.ProviderResult == nil || reviewer.Claim.Role != "reviewer" || verify == nil || !equalStringSlices(verify.Command, value.ProposedCommand) {
		return VerificationAmendment{}, false, ErrEvidenceConflict
	}
	// RecordVerification is allowed to append only the exact accepted amendment
	// decision. Re-run the same lineage and recovery proof used by the state
	// transition instead of treating a matching proof digest as authority.
	decision, err := s.verificationAmendmentDecisionFrom(ctx, conn, value, artifact.Ref, artifact.ExpectedVersion, artifact.Fence, *artifact.ProviderResult)
	if err != nil || decision != VerificationAmendmentAccepted {
		return VerificationAmendment{}, false, ErrEvidenceConflict
	}
	return value, true, nil
}

func (s *Store) loadPendingVerificationAmendmentAtFence(ctx context.Context, q rowQueryer, ref domain.TicketRef, expected uint64, fence domain.Fence) (VerificationAmendment, error) {
	value, err := s.loadVerificationAmendment(ctx, q, ref, expected, fence)
	if !errors.Is(err, ErrNotFound) {
		return value, err
	}
	return s.loadRecoveredPendingVerificationAmendment(ctx, q, ref, expected, fence)
}

func amendmentReviewerIndependent(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, amendment VerificationAmendment, reviewer ProviderAttemptResult) error {
	if reviewer.Claim.Role != "reviewer" || reviewer.Claim.Binding.Identity.Family == "" {
		return ErrProviderPairRefused
	}
	var family string
	if err := q.QueryRowContext(ctx, `SELECT family FROM provider_attempts WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' AND attempt=?`, amendment.BuilderResult.AttemptID, amendment.Ref.Channel, amendment.Ref.Project, amendment.Ref.Ticket, amendment.BuilderResult.Attempt).Scan(&family); err != nil || family == "" || family == reviewer.Claim.Binding.Identity.Family {
		return ErrProviderPairRefused
	}
	return nil
}

// TransitionVerificationAmendmentAccepted consumes the exact amended
// revision. RecordVerification is deliberately separate so a crash can replay
// the immutable provider result and append only its live binding.
func (s *Store) TransitionVerificationAmendmentAccepted(ctx context.Context, transition Transition, key ProviderAttemptResultKey) (TransitionResult, error) {
	if transition.From != domain.StateVerifying || transition.To != domain.StateBuilding || transition.Trigger != "amendment_accepted" || transition.EventPayload != "{}" || key.Ref != transition.Ref || key.Phase != domain.PhaseVerification || key.AttemptID <= 0 || key.Attempt <= 0 {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		amendment, err := s.loadPendingVerificationAmendmentAtFence(ctx, conn, transition.Ref, version, transition.Fence)
		if err != nil {
			return err
		}
		decision, err := s.verificationAmendmentDecisionFrom(ctx, conn, amendment, transition.Ref, version, transition.Fence, key)
		if err != nil || decision != VerificationAmendmentAccepted {
			return ErrEvidenceConflict
		}
		var current uint64
		var amends uint64
		var proof, reason, requester string
		err = conn.QueryRowContext(ctx, `SELECT v.current_revision,COALESCE(r.amends_revision,0),r.proof_digest,r.amendment_reason,r.requester
			FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
			WHERE v.channel=? AND v.project_id=? AND v.ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&current, &amends, &proof, &reason, &requester)
		var boundID int64
		var boundAttempt int
		bindingErr := conn.QueryRowContext(ctx, `SELECT provider_attempt_id,provider_attempt FROM verification_result_bindings
			WHERE channel=? AND project_id=? AND ticket_id=? AND revision=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`,
			transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, current, version, transition.Fence.LeaderEpoch, runner).Scan(&boundID, &boundAttempt)
		if err != nil || current != amendment.Prior.Revision+1 || amends != amendment.Prior.Revision || proof != amendment.ProposedDigest || reason != amendment.Reason || requester != amendment.Requester || bindingErr != nil || boundID != key.AttemptID || boundAttempt != key.Attempt {
			return ErrEvidenceConflict
		}
		return nil
	})
}

func (s *Store) TransitionVerificationAmendmentRejected(ctx context.Context, transition Transition, key ProviderAttemptResultKey) (TransitionResult, error) {
	if transition.From != domain.StateVerifying || transition.To != domain.StateBuilding || transition.Trigger != "amendment_rejected" || transition.EventPayload != "{}" || key.Ref != transition.Ref || key.Phase != domain.PhaseVerification {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		amendment, err := s.loadPendingVerificationAmendmentAtFence(ctx, conn, transition.Ref, version, transition.Fence)
		if err != nil {
			return err
		}
		decision, err := s.verificationAmendmentDecisionFrom(ctx, conn, amendment, transition.Ref, version, transition.Fence, key)
		if err != nil || decision != VerificationAmendmentRejected {
			return ErrEvidenceConflict
		}
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT current_revision FROM verifications WHERE channel=? AND project_id=? AND ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&current); err != nil || current != amendment.Prior.Revision {
			return ErrEvidenceConflict
		}
		return nil
	})
}

// verificationAmendmentBoundary is the narrow authority bridge for a
// completed amendment.  The ordinary phase-pass bridge cannot authenticate
// these transitions because the trigger is decision-specific.  Keep the
// request, fresh Reviewer result, decision trigger, and resulting revision in
// one proof so a matching counter can never widen authority by itself.
type verificationAmendmentBoundary struct {
	Amendment       VerificationAmendment
	Decision        VerificationAmendmentDecision
	Reviewer        ProviderAttemptResultKey
	DecisionVersion uint64
}

func loadVerificationAmendmentBoundary(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, liveVersion uint64, liveFence domain.Fence) (verificationAmendmentBoundary, error) {
	return loadVerificationAmendmentBoundaryAt(ctx, q, ref, liveVersion, liveFence, true)
}

func loadVerificationAmendmentBoundaryAt(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, liveVersion uint64, liveFence domain.Fence, requireLiveAuthority bool) (verificationAmendmentBoundary, error) {
	if liveVersion == 0 || liveFence.LeaderEpoch == 0 || liveFence.RunnerEpoch == 0 {
		return verificationAmendmentBoundary{}, ErrNotFound
	}
	var decisionVersion uint64
	var trigger string
	if err := q.QueryRowContext(ctx, `SELECT ticket_version,trigger FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version<=?
		AND trigger IN ('amendment_accepted','amendment_rejected')
		AND from_state='verifying' AND to_state='building' AND payload='{}'
		ORDER BY ticket_version DESC,id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, liveVersion).Scan(&decisionVersion, &trigger); errors.Is(err, sql.ErrNoRows) {
		return verificationAmendmentBoundary{}, ErrNotFound
	} else if err != nil || decisionVersion <= 1 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	var decisionEvents int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger IN ('amendment_accepted','amendment_rejected')
		AND from_state='verifying' AND to_state='building' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket, decisionVersion).Scan(&decisionEvents); err != nil || decisionEvents != 1 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	unresolved, err := countUnresolvedVerificationAmendmentRequests(ctx, q, ref, decisionVersion)
	if err != nil || unresolved != 1 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	var requestVersion, consumedVersion, consumedLeader, consumedRunner uint64
	if err := q.QueryRowContext(ctx, `SELECT transition_ticket_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch
		FROM verification_amendment_requests
		WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version<?
		AND NOT EXISTS (
			SELECT 1 FROM events resolved
			WHERE resolved.channel=verification_amendment_requests.channel
			AND resolved.project_id=verification_amendment_requests.project_id
			AND resolved.ticket_id=verification_amendment_requests.ticket_id
			AND resolved.ticket_version>verification_amendment_requests.transition_ticket_version
			AND resolved.ticket_version<?
			AND resolved.trigger IN ('amendment_accepted','amendment_rejected')
			AND resolved.from_state='verifying' AND resolved.to_state='building' AND resolved.payload='{}'
		)
		ORDER BY transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, decisionVersion, decisionVersion).Scan(&requestVersion, &consumedVersion, &consumedLeader, &consumedRunner); err != nil || requestVersion == 0 || requestVersion >= decisionVersion || consumedVersion+1 != requestVersion || consumedLeader == 0 || consumedRunner == 0 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	amendmentFence := domain.Fence{LeaderEpoch: consumedLeader, RunnerEpoch: consumedRunner}
	store := &Store{}
	amendment, err := store.loadVerificationAmendment(ctx, q, ref, requestVersion, amendmentFence)
	if err != nil {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	var requestEvents int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger='verification_amendment_requested' AND from_state='building' AND to_state='verifying' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket, requestVersion).Scan(&requestEvents); err != nil || requestEvents != 1 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	decisionFence, err := amendmentDecisionFence(ctx, q, ref, requestVersion, amendmentFence, decisionVersion)
	if err != nil {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	var reviewerID int64
	var reviewerAttempt int
	if err := q.QueryRowContext(ctx, `SELECT r.provider_attempt_id,a.attempt
		FROM provider_attempts a JOIN provider_attempt_results r ON r.provider_attempt_id=a.id
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase='verification' AND a.role='reviewer'
		AND a.expected_ticket_version>=? AND a.expected_ticket_version<=? AND a.state='completed' AND a.outcome='completed'
		AND a.finished_at IS NOT NULL AND a.finished_at<>''
		ORDER BY a.expected_ticket_version DESC,a.attempt DESC,a.id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, requestVersion, decisionVersion-1).Scan(&reviewerID, &reviewerAttempt); err != nil || reviewerID <= 0 || reviewerAttempt <= 0 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	reviewer := ProviderAttemptResultKey{AttemptID: reviewerID, Ref: ref, Phase: domain.PhaseVerification, Attempt: reviewerAttempt}
	decision, err := store.verificationAmendmentDecisionFromAt(ctx, q, amendment, ref, decisionVersion-1, decisionFence, reviewer, requireLiveAuthority)
	if err != nil || (decision == VerificationAmendmentAccepted && trigger != "amendment_accepted") || (decision == VerificationAmendmentRejected && trigger != "amendment_rejected") {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	if err := amendmentBoundaryReachesLive(ctx, q, ref, decisionVersion, decisionFence, liveVersion, liveFence); err != nil {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	var current uint64
	var intent, proof, checkpoint string
	var amends sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT v.current_revision,r.intent_digest,r.proof_digest,r.checkpoint_id,r.amends_revision
		FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
		WHERE v.channel=? AND v.project_id=? AND v.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&current, &intent, &proof, &checkpoint, &amends); err != nil || current == 0 {
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	switch decision {
	case VerificationAmendmentAccepted:
		if current != amendment.Prior.Revision+1 || !amends.Valid || uint64(amends.Int64) != amendment.Prior.Revision || proof != amendment.ProposedDigest || intent == "" || !validOID(checkpoint) {
			return verificationAmendmentBoundary{}, ErrEvidenceConflict
		}
		var boundID int64
		var boundAttempt int
		var boundCommit, boundParent, boundTree string
		if err := q.QueryRowContext(ctx, `SELECT provider_attempt_id,provider_attempt,checkpoint_commit_oid,checkpoint_parent_oid,checkpoint_tree_oid FROM verification_result_bindings
			WHERE channel=? AND project_id=? AND ticket_id=? AND revision=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, current, decisionVersion-1, decisionFence.LeaderEpoch, decisionFence.RunnerEpoch).Scan(&boundID, &boundAttempt, &boundCommit, &boundParent, &boundTree); err != nil || boundID != reviewer.AttemptID || boundAttempt != reviewer.Attempt || boundCommit != checkpoint || !validOID(boundParent) || !validOID(boundTree) {
			return verificationAmendmentBoundary{}, ErrEvidenceConflict
		}
	case VerificationAmendmentRejected:
		if current != amendment.Prior.Revision || intent != amendment.Prior.IntentDigest || proof != amendment.Prior.ProofDigest || checkpoint != amendment.Prior.CheckpointID || amends.Valid {
			return verificationAmendmentBoundary{}, ErrEvidenceConflict
		}
	default:
		return verificationAmendmentBoundary{}, ErrEvidenceConflict
	}
	return verificationAmendmentBoundary{Amendment: amendment, Decision: decision, Reviewer: reviewer, DecisionVersion: decisionVersion}, nil
}

// verificationAmendmentRecoveryPredecessor returns the exact old-leader
// endpoint consumed by an amendment decision immediately before startup
// recovery. The decision may follow one or more signed recovery rows while the
// request remained pending, so neither the original Builder nor a generic
// phase-pass bridge is sufficient authority for the next fence.
func verificationAmendmentRecoveryPredecessor(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, state domain.State, liveVersion, liveRunner, newLeader uint64) (uint64, bool, error) {
	if state != domain.StateBuilding {
		return 0, false, nil
	}
	if liveVersion == 0 || liveRunner == 0 || newLeader == 0 {
		return 0, false, ErrPublicationEvidence
	}
	var decisions int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger IN ('amendment_accepted','amendment_rejected')
		AND from_state='verifying' AND to_state='building' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket, liveVersion).Scan(&decisions); err != nil {
		return 0, false, err
	}
	if decisions == 0 {
		return 0, false, nil
	}
	if decisions != 1 {
		return 0, false, fmt.Errorf("authenticate amendment recovery decision cardinality: %w", ErrPublicationEvidence)
	}
	unresolved, err := countUnresolvedVerificationAmendmentRequests(ctx, q, ref, liveVersion)
	if err != nil || unresolved != 1 {
		return 0, false, fmt.Errorf("authenticate amendment recovery request cardinality: %w", ErrPublicationEvidence)
	}

	var requestVersion, consumedVersion, consumedLeader, consumedRunner uint64
	if err := q.QueryRowContext(ctx, `SELECT transition_ticket_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch
		FROM verification_amendment_requests
		WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version<?
		AND NOT EXISTS (
			SELECT 1 FROM events resolved
			WHERE resolved.channel=verification_amendment_requests.channel
			AND resolved.project_id=verification_amendment_requests.project_id
			AND resolved.ticket_id=verification_amendment_requests.ticket_id
			AND resolved.ticket_version>verification_amendment_requests.transition_ticket_version
			AND resolved.ticket_version<?
			AND resolved.trigger IN ('amendment_accepted','amendment_rejected')
			AND resolved.from_state='verifying' AND resolved.to_state='building' AND resolved.payload='{}'
		)
		ORDER BY transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, liveVersion, liveVersion).Scan(&requestVersion, &consumedVersion, &consumedLeader, &consumedRunner); err != nil || requestVersion == 0 || consumedVersion+1 != requestVersion || consumedLeader == 0 || consumedRunner == 0 {
		return 0, false, fmt.Errorf("authenticate amendment recovery request: %w", ErrPublicationEvidence)
	}
	requestFence := domain.Fence{LeaderEpoch: consumedLeader, RunnerEpoch: consumedRunner}
	decisionFence, err := amendmentDecisionFence(ctx, q, ref, requestVersion, requestFence, liveVersion)
	if err != nil || decisionFence.RunnerEpoch != liveRunner || decisionFence.LeaderEpoch == 0 || decisionFence.LeaderEpoch >= newLeader {
		return 0, false, fmt.Errorf("authenticate amendment recovery decision fence: %w", ErrPublicationEvidence)
	}
	boundary, err := loadVerificationAmendmentBoundaryAt(ctx, q, ref, liveVersion, decisionFence, false)
	if err != nil || boundary.DecisionVersion != liveVersion {
		return 0, false, fmt.Errorf("authenticate amendment recovery boundary: %w", ErrPublicationEvidence)
	}
	return decisionFence.LeaderEpoch, true, nil
}

// validateRunnerVerificationAmendmentAdvance admits one exact amendment
// decision between runner-recovery endpoints. It is deliberately separate from
// the ordinary phase-pass validator because the request, fresh Reviewer result,
// decision trigger, and accepted-or-rejected revision are all Store-owned
// authority.
func validateRunnerVerificationAmendmentAdvance(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, startVersion, startRunner, startLeader, endVersion, endRunner, endLeader uint64) error {
	if startVersion == 0 || (endVersion != startVersion+1 && endVersion != startVersion+2) || startRunner == 0 || startRunner != endRunner || startLeader == 0 || startLeader != endLeader {
		return ErrPublicationEvidence
	}
	if endVersion == startVersion+2 {
		var requestTrigger, requestPayload string
		var requestFrom, requestTo domain.State
		var requestTransitions int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, startVersion+1).Scan(&requestTransitions); err != nil || requestTransitions != 1 {
			return ErrPublicationEvidence
		}
		if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, startVersion+1).Scan(&requestTrigger, &requestFrom, &requestTo, &requestPayload); err != nil || !validInitialVerificationAmendmentRequestAtFence(ctx, q, ref, startVersion, domain.Fence{LeaderEpoch: startLeader, RunnerEpoch: startRunner}, startVersion+1, requestTrigger, requestFrom, requestTo, requestPayload) {
			return ErrPublicationEvidence
		}
	}
	var trigger, payload string
	var from, to domain.State
	var transitions int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, endVersion).Scan(&transitions); err != nil || transitions != 1 {
		return ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, endVersion).Scan(&trigger, &from, &to, &payload); err != nil {
		return ErrPublicationEvidence
	}
	if validInitialVerificationAmendmentRequestAtFence(ctx, q, ref, startVersion, domain.Fence{LeaderEpoch: startLeader, RunnerEpoch: startRunner}, endVersion, trigger, from, to, payload) {
		return nil
	}
	boundary, err := loadVerificationAmendmentBoundaryAt(ctx, q, ref, endVersion, domain.Fence{LeaderEpoch: endLeader, RunnerEpoch: endRunner}, false)
	if err != nil || boundary.DecisionVersion != endVersion {
		return ErrPublicationEvidence
	}
	return nil
}

func validInitialVerificationAmendmentDecision(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, version uint64, trigger string, from, to domain.State, payload string) bool {
	if version <= 1 || (trigger != "amendment_accepted" && trigger != "amendment_rejected") || from != domain.StateVerifying || to != domain.StateBuilding || payload != "{}" {
		return false
	}
	unresolved, err := countUnresolvedVerificationAmendmentRequests(ctx, q, ref, version)
	if err != nil || unresolved != 1 {
		return false
	}
	var requestVersion, consumedVersion, consumedLeader, consumedRunner uint64
	if q.QueryRowContext(ctx, `SELECT transition_ticket_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch
		FROM verification_amendment_requests
		WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version<?
		AND NOT EXISTS (
			SELECT 1 FROM events resolved
			WHERE resolved.channel=verification_amendment_requests.channel
			AND resolved.project_id=verification_amendment_requests.project_id
			AND resolved.ticket_id=verification_amendment_requests.ticket_id
			AND resolved.ticket_version>verification_amendment_requests.transition_ticket_version
			AND resolved.ticket_version<?
			AND resolved.trigger IN ('amendment_accepted','amendment_rejected')
			AND resolved.from_state='verifying' AND resolved.to_state='building' AND resolved.payload='{}'
		)
		ORDER BY transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, version, version).Scan(&requestVersion, &consumedVersion, &consumedLeader, &consumedRunner) != nil || requestVersion == 0 || consumedVersion+1 != requestVersion || consumedLeader == 0 || consumedRunner == 0 {
		return false
	}
	requestFence := domain.Fence{LeaderEpoch: consumedLeader, RunnerEpoch: consumedRunner}
	decisionFence, err := amendmentDecisionFence(ctx, q, ref, requestVersion, requestFence, version)
	if err != nil {
		return false
	}
	boundary, err := loadVerificationAmendmentBoundaryAt(ctx, q, ref, version, decisionFence, false)
	return err == nil && boundary.DecisionVersion == version && ((boundary.Decision == VerificationAmendmentAccepted && trigger == "amendment_accepted") || (boundary.Decision == VerificationAmendmentRejected && trigger == "amendment_rejected"))
}

// amendmentDecisionFence reconstructs the exact verifying endpoint consumed
// by an amendment decision. Recovery rows may sit between the immutable
// Builder->verifying request and the later decision; the event table stores
// versions but deliberately carries no mutable fence counters.
func amendmentDecisionFence(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, requestVersion uint64, requestFence domain.Fence, decisionVersion uint64) (domain.Fence, error) {
	if requestVersion == 0 || decisionVersion == 0 || requestVersion >= decisionVersion || requestFence.LeaderEpoch == 0 || requestFence.RunnerEpoch == 0 {
		return domain.Fence{}, ErrEvidenceConflict
	}
	predecessor := decisionVersion - 1
	if predecessor == requestVersion {
		return requestFence, nil
	}
	row, found, err := loadRunnerRecoveryAt(ctx, q, ref, predecessor)
	if err != nil || !found || row.TicketVersion != predecessor || row.RunnerEpoch == 0 || row.LeaderEpoch == 0 {
		return domain.Fence{}, ErrEvidenceConflict
	}
	decisionFence := domain.Fence{LeaderEpoch: row.LeaderEpoch, RunnerEpoch: row.RunnerEpoch}
	if err := validateRunnerRecoveryLedgerPrefix(ctx, q, ref, requestVersion, requestFence.RunnerEpoch, requestFence.LeaderEpoch, predecessor, decisionFence.RunnerEpoch, decisionFence.LeaderEpoch); err != nil {
		return domain.Fence{}, ErrEvidenceConflict
	}
	return decisionFence, nil
}

func amendmentBoundaryReachesLive(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, boundaryVersion uint64, boundaryFence domain.Fence, liveVersion uint64, liveFence domain.Fence) error {
	if liveVersion < boundaryVersion || boundaryVersion == 0 || boundaryFence.LeaderEpoch == 0 || boundaryFence.RunnerEpoch == 0 {
		return ErrPublicationEvidence
	}
	if liveVersion == boundaryVersion {
		if liveFence != boundaryFence {
			return ErrPublicationEvidence
		}
		return nil
	}
	return validateRunnerRecoveryLedger(ctx, q, ref, boundaryVersion, boundaryFence.RunnerEpoch, boundaryFence.LeaderEpoch, liveVersion, liveFence.RunnerEpoch, liveFence.LeaderEpoch)
}

func verificationResultReachesAmendmentRequest(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key ProviderAttemptResultKey, result ProviderAttemptResult, amendment VerificationAmendment) error {
	claim := result.Claim
	if claim.Ref != key.Ref || claim.Phase != domain.PhaseVerification || claim.Role != "reviewer" || claim.ExpectedVersion == 0 || claim.RunnerEpoch == 0 || claim.LeaderEpoch == 0 {
		return ErrStaleFence
	}
	if claim.ExpectedVersion == amendment.ConsumedVersion && claim.RunnerEpoch == amendment.Fence.RunnerEpoch && claim.LeaderEpoch == amendment.Fence.LeaderEpoch {
		return nil
	}
	if claim.ExpectedVersion == ^uint64(0) {
		return ErrStaleFence
	}
	var transitions int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='verifying' AND to_state='building' AND payload='{}'`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, claim.ExpectedVersion+1).Scan(&transitions); err != nil || transitions != 1 {
		return ErrStaleFence
	}
	if err := validateRunnerRecoveryLedgerPrefix(ctx, q, key.Ref, claim.ExpectedVersion+1, claim.RunnerEpoch, claim.LeaderEpoch, amendment.ConsumedVersion, amendment.Fence.RunnerEpoch, amendment.Fence.LeaderEpoch); err != nil {
		return ErrStaleFence
	}
	return nil
}

// verificationAmendmentRequestReachesReviewer proves the only permitted
// amendment-review launch lineage. A daemon may recover before the Reviewer is
// started, so the claim need not carry the request's original fence; it must
// nevertheless be a signed successor of that immutable request endpoint.
func verificationAmendmentRequestReachesReviewer(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, amendment VerificationAmendment, reviewer ProviderAttemptResult) error {
	claim := reviewer.Claim
	if claim.Ref != ref || claim.Phase != domain.PhaseVerification || claim.Role != "reviewer" || amendment.TransitionTicketVersion == 0 || amendment.Fence.LeaderEpoch == 0 || amendment.Fence.RunnerEpoch == 0 || claim.ExpectedVersion < amendment.TransitionTicketVersion || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 {
		return ErrStaleFence
	}
	if claim.ExpectedVersion == amendment.TransitionTicketVersion {
		if claim.LeaderEpoch != amendment.Fence.LeaderEpoch || claim.RunnerEpoch != amendment.Fence.RunnerEpoch {
			return ErrStaleFence
		}
		return nil
	}
	return validateRunnerRecoveryLedgerPrefix(ctx, q, ref, amendment.TransitionTicketVersion, amendment.Fence.RunnerEpoch, amendment.Fence.LeaderEpoch, claim.ExpectedVersion, claim.RunnerEpoch, claim.LeaderEpoch)
}
