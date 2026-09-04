package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/workflowprompt"
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
	CommandBinding  RepositoryCommandResultBinding
}

// StoredCandidate is the latest immutable candidate generation.
type StoredCandidate struct {
	Snapshot       domain.CandidateSnapshot
	TicketVersion  uint64
	Fence          domain.Fence
	CreatedAt      time.Time
	BuilderResult  ProviderAttemptResultKey
	Commit         CommitObservation
	CommandBinding RepositoryCommandResultBinding
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

// FinalReviewAuthority is the single read-only authority presented to the
// final-review adapter. It joins the immutable candidate and pre-build proof
// with the exact green CI transition that entered reviewing. It intentionally
// does not reuse CurrentVerification: that reader is for the earlier
// verifying/building boundary and must fail closed once a candidate exists.
type FinalReviewAuthority struct {
	Candidate    StoredCandidate
	Verification StoredVerification
	Checks       workflowprompt.ChecksIdentity
}

// FinalReviewAuthority authenticates the current reviewing endpoint. A
// restart may retain older candidate/proof bindings, but only an exact green
// CI transition plus the signed recovery lineage may carry them to the live
// reviewing fence.
func (s *Store) FinalReviewAuthority(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (FinalReviewAuthority, error) {
	return s.finalReviewAuthorityFrom(ctx, s.db, ref, expectedVersion, fence)
}

func (s *Store) finalReviewAuthorityFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (FinalReviewAuthority, error) {
	if ref.Validate() != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return FinalReviewAuthority{}, ErrStaleFence
	}
	var state domain.State
	var ticketType domain.TicketType
	var version, runner, leader uint64
	var source string
	if err := q.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch,t.source_digest,t.ticket_type FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader, &source, &ticketType); err != nil || state != domain.StateReviewing || version != expectedVersion || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
		return FinalReviewAuthority{}, ErrStaleFence
	}
	candidate, err := s.latestCandidateFrom(ctx, q, ref, false)
	if err != nil || candidate.Snapshot.SourceDigest != source {
		return FinalReviewAuthority{}, ErrEvidenceConflict
	}
	verification, err := s.verificationEvidenceForCandidateFrom(ctx, q, ref)
	if err != nil || s.authenticateCandidateVerificationParentFrom(ctx, q, candidate, verification) != nil {
		return FinalReviewAuthority{}, ErrEvidenceConflict
	}
	if ticketType == domain.TicketSpike {
		_, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, verification.ProviderResult)
		if err != nil || parsed.Verify == nil || parsed.Verify.PrebuildOutcome != "report_ready" {
			return FinalReviewAuthority{}, ErrEvidenceConflict
		}
		return FinalReviewAuthority{Candidate: candidate, Verification: verification}, nil
	}
	observation, checks, reviewVersion, err := finalReviewCIAuthorityFrom(ctx, q, ref, candidate)
	if err != nil {
		return FinalReviewAuthority{}, ErrEvidenceConflict
	}
	// This is a current-fence reader. The CI and post-publication proofs below
	// authenticate every recovery through expectedVersion; a later row would
	// be contradictory durable authority and must not be ignored.
	var futureRecoveries int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?`, ref.Channel, ref.Project, ref.Ticket, expectedVersion).Scan(&futureRecoveries); err != nil || futureRecoveries != 0 {
		return FinalReviewAuthority{}, ErrStaleFence
	}
	// Reviewing can survive a leader/runner recovery, but the recovery proof
	// must begin at the exact CI-created reviewing endpoint; candidate rows
	// themselves predate publication and cannot be used as a loose shortcut.
	if reviewVersion != expectedVersion || observation.ObservedFence.RunnerEpoch != fence.RunnerEpoch || observation.ObservedFence.LeaderEpoch != fence.LeaderEpoch {
		// A final review may continue after an authenticated operator
		// pause/drain/resume. That exact control triplet advances three ticket
		// versions but only one runner epoch, so the phase-only recovery ledger
		// is deliberately insufficient here. The post-publication bridge accepts
		// only that triplet (and signed recovery rows), never a bare gap.
		if err := validatePostPublicationEndpointAdvance(ctx, q, ref, domain.StateReviewing,
			normalRecoveryEndpoint{version: reviewVersion, runner: observation.ObservedFence.RunnerEpoch, leader: observation.ObservedFence.LeaderEpoch},
			normalRecoveryEndpoint{version: expectedVersion, runner: fence.RunnerEpoch, leader: fence.LeaderEpoch}); err != nil {
			return FinalReviewAuthority{}, ErrStaleFence
		}
	}
	return FinalReviewAuthority{Candidate: candidate, Verification: verification, Checks: checks}, nil
}

// authenticateCandidateVerificationParentFrom keeps the ordinary candidate
// parent invariant and the sole CI-repair exception in one proof. A repaired
// candidate may be a child of its predecessor candidate only after the exact
// repair binding, Builder result, candidate, and completion authenticate.
func (s *Store) authenticateCandidateVerificationParentFrom(ctx context.Context, q candidateEvidenceQuerier, candidate StoredCandidate, verification StoredVerification) error {
	if candidate.Snapshot.VerificationIntentDigest != verification.Revision.IntentDigest || candidate.Snapshot.ProofDigest != verification.Revision.ProofDigest {
		return ErrEvidenceConflict
	}
	if candidate.Commit.ParentOID == verification.Checkpoint.CommitOID {
		return nil
	}
	builder, _, builderErr := s.loadHistoricalProviderAttemptResult(ctx, q, candidate.BuilderResult)
	repair, repairErr := completedCandidateRepairContextAt(ctx, q, candidate, builder)
	if builderErr != nil || repairErr != nil || candidate.Commit.ParentOID != repair.PredecessorHeadSHA || !reflect.DeepEqual(repair.Verification, verification) {
		return ErrEvidenceConflict
	}
	return nil
}

// finalReviewCIAuthorityFrom authenticates the complete v43 CI chain using
// the caller's query scope. In particular, transition writes call this with
// their active *sql.Conn: opening a second database read there could observe a
// different snapshot or deadlock under the intentionally bounded pool.
func finalReviewCIAuthorityFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, candidate StoredCandidate) (CIObservation, workflowprompt.ChecksIdentity, uint64, error) {
	publication, err := loadCIPublicationBase(ctx, q, ref)
	if err != nil || !publicationFound(publication) || publication.Candidate.Snapshot != candidate.Snapshot {
		return CIObservation{}, workflowprompt.ChecksIdentity{}, 0, fmt.Errorf("%w: final review CI publication", ErrEvidenceConflict)
	}
	policy, err := scanCurrentCIPolicy(ctx, q, ref, publication)
	if err != nil {
		// A blank policy_witness_digest is the v42 compatibility default and
		// cannot authorize final review. There is no safe reconstruction from
		// an observation chosen before the v43 policy witness existed.
		return CIObservation{}, workflowprompt.ChecksIdentity{}, 0, fmt.Errorf("%w: final review CI policy", ErrEvidenceConflict)
	}
	observation, reviewVersion, err := finalReviewCIPendingChainFrom(ctx, q, ref, publication, policy)
	if err != nil {
		return CIObservation{}, workflowprompt.ChecksIdentity{}, 0, err
	}
	checks := make([]workflowprompt.Check, 0, len(observation.RequiredChecks))
	for _, check := range observation.RequiredChecks {
		if check.NormalizedState != "success" {
			return CIObservation{}, workflowprompt.ChecksIdentity{}, 0, ErrEvidenceConflict
		}
		checks = append(checks, workflowprompt.Check{Name: check.CanonicalName, ExternalID: check.ExternalID, Status: check.NormalizedState})
	}
	identity, err := workflowprompt.NewChecksIdentity(fmt.Sprintf("%d", observation.ObservationID), candidate.Snapshot.HeadSHA, checks)
	// The CI policy digest names the server-required set (name and external
	// ID); the reviewer prompt identity additionally includes each observed
	// status. They are intentionally different digest domains. Exact policy
	// equality was authenticated above by policyMatchesObservation.
	if err != nil {
		return CIObservation{}, workflowprompt.ChecksIdentity{}, 0, ErrEvidenceConflict
	}
	return observation, identity, reviewVersion, nil
}

// finalReviewCIPendingChainFrom proves the complete CI history which starts
// at the publication-created waiting_ci endpoint and ends at exactly one
// green transition. Pending observations are stateful authority, not a
// dispensable prelude to green: each must be contiguous, policy-matched, and
// bound to its exact event and digest.
func finalReviewCIPendingChainFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, publication PublishedCandidateEvidence, policy CIRequiredCheckPolicy) (CIObservation, uint64, error) {
	waitingVersion := publication.CurrentTicketVersion + 1
	payload, err := json.Marshal(struct {
		WitnessDigest    string `json:"witness_digest"`
		WitnessCreatedAt string `json:"witness_created_at"`
	}{publication.WitnessDigest, publication.CreatedAt.Format(time.RFC3339Nano)})
	if err != nil {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI publication payload", ErrEvidenceConflict)
	}
	var publicationEvents int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events e JOIN publication_transition_evidence p ON p.channel=e.channel AND p.project_id=e.project_id AND p.ticket_id=e.ticket_id AND p.ticket_version=e.ticket_version AND p.event_created_at=e.created_at WHERE e.channel=? AND e.project_id=? AND e.ticket_id=? AND e.ticket_version=? AND e.trigger='effects_confirmed' AND e.from_state='publishing' AND e.to_state='waiting_ci' AND e.payload=? AND p.witness_digest=? AND p.witness_created_at=?`, ref.Channel, ref.Project, ref.Ticket, waitingVersion, string(payload), publication.WitnessDigest, publication.CreatedAt.Format(time.RFC3339Nano)).Scan(&publicationEvents); err != nil || publicationEvents != 1 {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI publication boundary", ErrEvidenceConflict)
	}
	if err := validateCIWaitingVersionEvents(ctx, q, ref, waitingVersion, string(payload), publication); err != nil {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI publication event", ErrEvidenceConflict)
	}
	var greenCount int
	var reviewVersion uint64
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(ticket_version),0) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=? AND observation_classification='green' AND resulting_state='reviewing' AND resulting_trigger='checks_green'`, ref.Channel, ref.Project, ref.Ticket, publication.Candidate.Snapshot.Generation, publication.Candidate.Snapshot.HeadSHA, publication.Candidate.Snapshot.TreeSHA).Scan(&greenCount, &reviewVersion); err != nil || greenCount != 1 || reviewVersion <= waitingVersion {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI green cardinality", ErrEvidenceConflict)
	}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI recovery cardinality", ErrEvidenceConflict)
	}
	expectedRunner, expectedLeader := publication.CurrentFence.RunnerEpoch, publication.CurrentFence.LeaderEpoch
	var green CIObservation
	for version := waitingVersion + 1; version <= reviewVersion; version++ {
		recovery, recovered, err := loadRunnerRecoveryAt(ctx, q, ref, version)
		if err != nil {
			return CIObservation{}, 0, fmt.Errorf("%w: final review CI recovery read", ErrEvidenceConflict)
		}
		var transitionCount int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&transitionCount); err != nil {
			return CIObservation{}, 0, err
		}
		if recovered {
			var events int
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&events); err != nil || transitionCount != 0 || events != 0 || !validRunnerRecovery(recovery) || recovery.PriorTicketVersion != version-1 || recovery.PriorRunnerEpoch != expectedRunner || recovery.PriorLeaderEpoch != expectedLeader {
				return CIObservation{}, 0, fmt.Errorf("%w: final review CI recovery step", ErrEvidenceConflict)
			}
			expectedRunner, expectedLeader = recovery.RunnerEpoch, recovery.LeaderEpoch
			continue
		}
		if transitionCount != 1 {
			return CIObservation{}, 0, fmt.Errorf("%w: final review CI transition gap", ErrEvidenceConflict)
		}
		var generation, eventID int64
		var evidenceVersion, observationVersion, observationLeader, observationRunner uint64
		var head, tree, classification, observationDigest, witness, priorState, resultingState, resultingTrigger, transitionDigest, evidenceCreated string
		var joinedID int64
		var eventCreated, fromState, toState, eventTrigger, eventPayload string
		err = q.QueryRowContext(ctx, `SELECT c.ticket_version,c.event_id,c.event_created_at,c.candidate_generation,c.candidate_head_sha,c.candidate_tree_sha,c.observation_classification,c.observation_digest,c.observation_ticket_version,c.observation_leader_epoch,c.observation_runner_epoch,c.prior_publication_witness_digest,c.prior_state,c.resulting_state,c.resulting_trigger,c.transition_digest,e.id,e.created_at,e.from_state,e.to_state,e.trigger,e.payload FROM ci_transition_evidence c JOIN events e ON e.channel=c.channel AND e.project_id=c.project_id AND e.ticket_id=c.ticket_id AND e.ticket_version=c.ticket_version AND e.id=c.event_id AND e.created_at=c.event_created_at WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? AND c.ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&evidenceVersion, &eventID, &evidenceCreated, &generation, &head, &tree, &classification, &observationDigest, &observationVersion, &observationLeader, &observationRunner, &witness, &priorState, &resultingState, &resultingTrigger, &transitionDigest, &joinedID, &eventCreated, &fromState, &toState, &eventTrigger, &eventPayload)
		if err != nil || evidenceVersion != version || eventID <= 0 || eventID != joinedID || evidenceCreated != eventCreated || generation != int64(publication.Candidate.Snapshot.Generation) || head != publication.Candidate.Snapshot.HeadSHA || tree != publication.Candidate.Snapshot.TreeSHA || observationDigest == "" || observationVersion != version-1 || observationLeader != expectedLeader || observationRunner != expectedRunner || witness != publication.WitnessDigest || priorState != string(domain.StateWaitingCI) || fromState != string(domain.StateWaitingCI) {
			return CIObservation{}, 0, fmt.Errorf("%w: final review CI transition identity", ErrEvidenceConflict)
		}
		observation, found, err := scanCIObservation(ctx, q, false, ref, observationDigest)
		if err != nil || !found || !ciObservationMatchesPublication(observation, publication) || observation.ObservedTicketVersion != observationVersion || observation.ObservedFence.LeaderEpoch != observationLeader || observation.ObservedFence.RunnerEpoch != observationRunner || !policyMatchesObservation(policy, observation) || transitionDigest != ciTransitionDigest(ref, observation, version, eventID, eventCreated, resultingState, resultingTrigger, eventPayload) {
			return CIObservation{}, 0, fmt.Errorf("%w: final review CI observation or digest", ErrEvidenceConflict)
		}
		switch classification {
		case "pending":
			if version == reviewVersion || observation.Classification != "pending" || resultingState != string(domain.StateWaitingCI) || resultingTrigger != "checks_pending" || toState != string(domain.StateWaitingCI) || eventTrigger != "checks_pending" {
				return CIObservation{}, 0, fmt.Errorf("%w: final review CI pending transition", ErrEvidenceConflict)
			}
		case "green":
			if version != reviewVersion || observation.Classification != "green" || resultingState != string(domain.StateReviewing) || resultingTrigger != "checks_green" || toState != string(domain.StateReviewing) || eventTrigger != "checks_green" {
				return CIObservation{}, 0, fmt.Errorf("%w: final review CI green transition", ErrEvidenceConflict)
			}
			green = observation
		default:
			return CIObservation{}, 0, fmt.Errorf("%w: final review CI non-green chain transition", ErrEvidenceConflict)
		}
	}
	if green.Ref != ref {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI missing green endpoint", ErrEvidenceConflict)
	}
	var afterGreen int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?`, ref.Channel, ref.Project, ref.Ticket, reviewVersion).Scan(&afterGreen); err != nil || afterGreen != 0 {
		return CIObservation{}, 0, fmt.Errorf("%w: final review CI trailing transition", ErrEvidenceConflict)
	}
	return green, reviewVersion, nil
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
	// A candidate is bound to an immutable verification revision that predates
	// the Builder result. Authenticate that exact revision directly rather than
	// requiring its reviewer binding to traverse later candidate recovery rows.
	verification, err := s.verificationEvidenceForCandidate(ctx, ref)
	if err != nil {
		return StoredCandidate{}, err
	}
	if verification.Checkpoint.CommitOID != verification.Revision.CheckpointID || !validOID(verification.Checkpoint.ParentOID) || !validOID(verification.Checkpoint.TreeOID) {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	if err := s.authenticateCandidateVerificationParentFrom(ctx, s.db, candidate, verification); err != nil {
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
	return s.currentVerificationFrom(ctx, s.db, ref)
}

// currentVerificationFrom is the strict verification reader for callers that
// already hold Store's write connection. It keeps the ticket/leader/event and
// immutable command/provider/checkpoint checks in one SQLite view.
func (s *Store) currentVerificationFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef) (StoredVerification, error) {
	if err := ref.Validate(); err != nil {
		return StoredVerification{}, err
	}
	var result StoredVerification
	var owned, created string
	var amends sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT r.revision,r.ticket_version,r.leader_epoch,r.runner_epoch,
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
	if err := q.QueryRowContext(ctx, `SELECT b.binding_ticket_version,b.leader_epoch,b.runner_epoch,b.provider_attempt_id,b.provider_attempt,b.checkpoint_commit_oid,b.checkpoint_parent_oid,b.checkpoint_tree_oid FROM verification_result_bindings b JOIN provider_attempt_results pr ON pr.provider_attempt_id=b.provider_attempt_id JOIN provider_attempts a ON a.id=pr.provider_attempt_id AND a.attempt=b.provider_attempt AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id AND a.phase='verification' AND a.role='reviewer' AND a.state='completed' AND a.outcome='completed' WHERE b.channel=? AND b.project_id=? AND b.ticket_id=? AND b.revision=? ORDER BY b.binding_ticket_version DESC,b.leader_epoch DESC,b.runner_epoch DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, result.Revision.Revision).Scan(&result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &result.ProviderResult.AttemptID, &result.ProviderResult.Attempt, &result.Checkpoint.CommitOID, &result.Checkpoint.ParentOID, &result.Checkpoint.TreeOID); err == nil {
		result.ProviderResult.Ref, result.ProviderResult.Phase = ref, domain.PhaseVerification
		if result.Checkpoint.CommitOID != result.Revision.CheckpointID || !validOID(result.Checkpoint.ParentOID) || !validOID(result.Checkpoint.TreeOID) {
			return StoredVerification{}, ErrEvidenceConflict
		}
	}
	if result.ProviderResult.AttemptID == 0 || result.Checkpoint.CommitOID == "" {
		return StoredVerification{}, ErrEvidenceConflict
	}
	// This is the strict current verification reader. It admits the normal,
	// unrecovered verifying->building handoff, but never silently consumes a
	// prior-fence revision after a runner recovery. Immutable evidence remains
	// available through RecoverableVerification for that recovery rebind.
	var ticketVersion, ticketRunner, leader uint64
	var ticketState domain.State
	if err := q.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ticketState, &ticketVersion, &ticketRunner, &leader); err != nil || ticketVersion == 0 || ticketRunner == 0 || leader == 0 {
		return StoredVerification{}, ErrEvidenceConflict
	}
	exact := ticketVersion == result.TicketVersion && ticketRunner == result.Fence.RunnerEpoch && leader == result.Fence.LeaderEpoch
	amendmentAuthenticated := false
	repairAuthenticated := false
	if !exact {
		boundaryPhase := domain.PhaseVerification
		if ticketState == domain.StateBuilding {
			boundaryPhase = domain.PhaseBuild
		}
		boundary, boundaryErr := reviewRepairBoundaryFrom(ctx, q, ref, boundaryPhase, ticketVersion, result.TicketVersion)
		if boundaryErr != nil {
			return StoredVerification{}, ErrEvidenceConflict
		}
		if boundary && ticketState == domain.StateVerifying {
			// A reviewer-owned repair starts a new verification cycle. Hiding
			// the old revision makes both Worker and PhaseRunner launch a fresh
			// verifier instead of treating it as a normal restart predecessor.
			return StoredVerification{}, ErrNotFound
		}
		if boundary && ticketState == domain.StateBuilding {
			// A builder-owned repair still needs the prior verification as its
			// immutable input, but the boundary separately rejects reuse of the
			// prior Builder result. The final-review transition authenticated this
			// verification when it consumed the repair decision.
			exact = true
		}
	}
	if !exact && ticketState == domain.StateBuilding {
		repair, repairErr := s.candidateRepairBuildContextAt(ctx, q, ref, ticketVersion, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticketRunner})
		if repairErr == nil {
			retained := repair.Verification
			if result.Revision.Revision != retained.Revision.Revision || result.Revision.IntentDigest != retained.Revision.IntentDigest || result.Revision.ProofDigest != retained.Revision.ProofDigest || result.Revision.CheckpointID != retained.Revision.CheckpointID || result.Revision.Amends != retained.Revision.Amends || !equalStringSlices(result.Revision.OwnedFiles, retained.Revision.OwnedFiles) || !bytes.Equal(result.Intent, retained.Intent) || !bytes.Equal(result.Proof, retained.Proof) || result.AmendmentReason != retained.AmendmentReason || result.Requester != retained.Requester || result.ProviderResult != retained.ProviderResult || result.Checkpoint != retained.Checkpoint {
				return StoredVerification{}, fmt.Errorf("candidate repair verification binding: %w", ErrEvidenceConflict)
			}
			exact = true
			repairAuthenticated = true
		} else if !errors.Is(repairErr, ErrNotFound) {
			return StoredVerification{}, fmt.Errorf("candidate repair verification boundary: %w", repairErr)
		}
	}
	if !exact && ticketState == domain.StateBuilding {
		// Amendment decisions are an authenticated verification boundary, but
		// they intentionally do not use the generic phase_pass trigger.  Prove
		// the exact request, fresh Reviewer decision, decision trigger, and
		// resulting revision before accepting the live Building fence.
		amendment, amendmentErr := loadVerificationAmendmentBoundary(ctx, q, ref, ticketVersion, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticketRunner})
		if amendmentErr == nil {
			switch amendment.Decision {
			case VerificationAmendmentAccepted:
				if result.ProviderResult != amendment.Reviewer {
					return StoredVerification{}, fmt.Errorf("verification amendment reviewer binding: %w", ErrEvidenceConflict)
				}
			case VerificationAmendmentRejected:
				historical, _, historicalErr := s.loadHistoricalProviderAttemptResult(ctx, q, result.ProviderResult)
				if historicalErr != nil || verificationResultReachesAmendmentRequest(ctx, q, result.ProviderResult, historical, amendment.Amendment) != nil {
					return StoredVerification{}, fmt.Errorf("verification rejected amendment source: %w", ErrEvidenceConflict)
				}
			default:
				return StoredVerification{}, ErrEvidenceConflict
			}
			exact = true
			amendmentAuthenticated = true
		} else if !errors.Is(amendmentErr, ErrNotFound) {
			return StoredVerification{}, fmt.Errorf("verification amendment boundary: %w", amendmentErr)
		}
	}
	if !exact {
		var transitions int
		if ticketVersion != result.TicketVersion+1 || ticketRunner != result.Fence.RunnerEpoch || leader != result.Fence.LeaderEpoch || q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='verifying' AND to_state='building'`, ref.Channel, ref.Project, ref.Ticket, ticketVersion).Scan(&transitions) != nil || transitions != 1 {
			return StoredVerification{}, ErrEvidenceConflict
		}
	}
	binding, err := loadVerificationCommandBinding(ctx, q, ref, result.Revision.Revision)
	if err != nil {
		return StoredVerification{}, fmt.Errorf("verification command binding: %w", ErrEvidenceConflict)
	}
	result.CommandBinding = binding
	var commandErr error
	if amendmentAuthenticated || repairAuthenticated {
		commandErr = s.reauthenticateStoredVerificationCommandHistoricalFrom(ctx, q, ref, result)
	} else {
		commandErr = s.reauthenticateStoredVerificationCommandFrom(ctx, q, ref, result)
	}
	if commandErr != nil {
		return StoredVerification{}, fmt.Errorf("verification command reauthentication: %w", ErrEvidenceConflict)
	}
	if amendmentAuthenticated || repairAuthenticated {
		// The amendment boundary and signed recovery suffix are themselves the
		// live binding for this decision-specific transition. Project that exact
		// current endpoint while retaining the immutable provider/command witnesses
		// at the fences where they were recorded.
		result.TicketVersion = ticketVersion
		result.Fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticketRunner}
	}
	// Events are human/audit projections only. Provider, checkpoint, and
	// repository-command authority come from constrained immutable bindings.
	return result, nil
}

// RecoverableVerification reads the immutable revision/binding tuple solely
// for an append-only live-fence recovery rebind. It is not current transition
// authority: RecordVerification must authenticate the reviewer result through
// LatestReusableProviderAttempt at the caller's exact live fence.
func (s *Store) RecoverableVerification(ctx context.Context, ref domain.TicketRef) (StoredVerification, error) {
	if err := ref.Validate(); err != nil {
		return StoredVerification{}, err
	}
	return s.verificationEvidenceForCandidate(ctx, ref)
}

// HistoricalVerification authenticates the immutable verification checkpoint
// at the fence where it was recorded. It is for read-only status projections;
// it deliberately does not make that historical checkpoint current transition
// authority.
func (s *Store) HistoricalVerification(ctx context.Context, ref domain.TicketRef) (StoredVerification, error) {
	verification, err := s.RecoverableVerification(ctx, ref)
	if err != nil {
		return StoredVerification{}, err
	}
	if err := s.reauthenticateStoredVerificationCommandHistoricalFrom(ctx, s.db, ref, verification); err != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	return verification, nil
}

func (s *Store) LatestCandidate(ctx context.Context, ref domain.TicketRef) (StoredCandidate, error) {
	return s.latestCandidate(ctx, ref, true)
}

// RecoverableCandidate loads the immutable candidate/binding tuple for an
// append-only live-fence rebind. The subsequent RecordCandidate call is the
// authority check for the Builder source-to-live chain; ordinary readers must
// use LatestCandidate, which rejects a stale historical binding.
func (s *Store) RecoverableCandidate(ctx context.Context, ref domain.TicketRef) (StoredCandidate, error) {
	return s.latestCandidate(ctx, ref, false)
}

// HistoricalCandidate authenticates the immutable candidate checkpoint at the
// fence where it was recorded. It is for read-only status projections; callers
// that need current transition authority must continue to use LatestCandidate.
func (s *Store) HistoricalCandidate(ctx context.Context, ref domain.TicketRef) (StoredCandidate, error) {
	candidate, err := s.RecoverableCandidate(ctx, ref)
	if err != nil {
		return StoredCandidate{}, err
	}
	if err := s.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, s.db, ref, candidate); err != nil {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	return candidate, nil
}

func (s *Store) latestCandidate(ctx context.Context, ref domain.TicketRef, authenticateFence bool) (StoredCandidate, error) {
	return s.latestCandidateFrom(ctx, s.db, ref, authenticateFence)
}

// latestCandidateFrom lets a recovery fence inspect the immutable candidate
// and binding under its own write connection.  The caller chooses whether to
// authenticate a live fence; recovery uses false and subsequently proves the
// Builder result before appending a binding at the new fence.
type candidateEvidenceQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) latestCandidateFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, authenticateFence bool) (StoredCandidate, error) {
	if err := ref.Validate(); err != nil {
		return StoredCandidate{}, err
	}
	var result StoredCandidate
	var created string
	err := q.QueryRowContext(ctx, `SELECT generation,ticket_version,leader_epoch,runner_epoch,base_sha,head_sha,tree_sha,
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
	if err := q.QueryRowContext(ctx, `SELECT b.binding_ticket_version,b.leader_epoch,b.runner_epoch,b.provider_attempt_id,b.provider_attempt,b.commit_parent_oid FROM candidate_result_bindings b JOIN provider_attempt_results pr ON pr.provider_attempt_id=b.provider_attempt_id JOIN provider_attempts a ON a.id=pr.provider_attempt_id AND a.attempt=b.provider_attempt AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id AND a.phase='build' AND a.role='builder' AND a.state='completed' AND a.outcome='completed' WHERE b.channel=? AND b.project_id=? AND b.ticket_id=? AND b.generation=? ORDER BY b.binding_ticket_version DESC,b.leader_epoch DESC,b.runner_epoch DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, result.Snapshot.Generation).Scan(&result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &result.BuilderResult.AttemptID, &result.BuilderResult.Attempt, &result.Commit.ParentOID); err == nil {
		result.BuilderResult.Ref, result.BuilderResult.Phase = ref, domain.PhaseBuild
		result.Commit.CommitOID, result.Commit.TreeOID = result.Snapshot.HeadSHA, result.Snapshot.TreeSHA
	}
	if result.BuilderResult.AttemptID == 0 || result.Commit.ParentOID == "" {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	binding, err := loadCandidateCommandBinding(ctx, q, ref, result.Snapshot.Generation)
	if err != nil || !candidatePolicyMatches(result.Snapshot.CommandPolicyDigest, binding.PolicyDigest) {
		return StoredCandidate{}, ErrEvidenceConflict
	}
	result.CommandBinding = binding
	if authenticateFence {
		if err := s.reauthenticateStoredCandidateCommandFrom(ctx, q, ref, result); err != nil {
			return StoredCandidate{}, ErrEvidenceConflict
		}
		var liveVersion, liveRunner, liveLeader uint64
		if err := q.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&liveVersion, &liveRunner, &liveLeader); err != nil || liveVersion != result.TicketVersion || liveRunner != result.Fence.RunnerEpoch || liveLeader != result.Fence.LeaderEpoch {
			return StoredCandidate{}, ErrStaleFence
		}
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
		if !validPhase(item.Phase) || item.Attempt < 1 || !validProvider(item.Provider) || !boundedText(item.WorktreeID, 16<<10) || !validOID(item.BaseSHA) || item.ExpectedVersion == 0 || item.Fence.LeaderEpoch == 0 || item.Fence.RunnerEpoch == 0 || !validJSON(item.UsageJSON) {
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
