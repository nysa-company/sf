package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

// TicketControlProof is one linearized control observation. It is produced
// while the external-mutation gate is held and a BEGIN IMMEDIATE transaction
// has fenced the current durable identity and inspected every writer/effect.
type TicketControlProof struct {
	Ticket                               Ticket
	Fence                                domain.Fence
	ProviderWriters                      int
	RepositoryCommandWriters             int
	GitMutationWriters                   int
	UnreconciledEffects                  int
	PublicationOrMergeEffects            int
	MergeIntents                         int
	OutstandingPublicationOrMergeEffects int
	OutstandingMergeIntents              int
}

// RuntimeRearmCapability is an opaque, one-use authorization produced only by
// RearmProof. Its fields are deliberately private and it can be redeemed only
// through Store.ActivateRearm, never handed directly to a runtime bundle.
type RuntimeRearmCapability struct {
	mu         sync.Mutex
	ref        domain.TicketRef
	version    uint64
	fence      domain.Fence
	issued     bool
	activating bool
}

type durableRuntimeControl struct {
	state      string
	generation uint64
	stop       mutationRevocation
	authority  mutationRevocation
}

func runtimeControlFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (durableRuntimeControl, error) {
	var value durableRuntimeControl
	err := conn.QueryRowContext(ctx, `SELECT state,generation,stop_version,stop_leader_epoch,stop_runner_epoch,authority_version,authority_leader_epoch,authority_runner_epoch
		FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&value.state, &value.generation, &value.stop.version, &value.stop.leader, &value.stop.runner,
		&value.authority.version, &value.authority.leader, &value.authority.runner)
	if errors.Is(err, sql.ErrNoRows) {
		return durableRuntimeControl{}, ErrStaleFence
	}
	return value, err
}

// restoreRuntimeControls converts every interrupted rearm or active admission
// into a sealed generation and rebuilds the in-process process-start gate. A
// persisted open state has lost its owning scheduler on process crash and
// must never authorize a fresh Store writer after reopen.
func (s *Store) restoreRuntimeControls(ctx context.Context) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET state='sealed',updated_at=? WHERE state IN ('armed','open')`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		rows, err := conn.QueryContext(ctx, `SELECT channel,project_id,ticket_id,authority_version,authority_leader_epoch,authority_runner_epoch FROM runtime_ticket_controls WHERE state='sealed'`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ref domain.TicketRef
			var value mutationRevocation
			if err := rows.Scan(&ref.Channel, &ref.Project, &ref.Ticket, &value.version, &value.leader, &value.runner); err != nil {
				return err
			}
			s.mutations.latch(ref, value)
		}
		return rows.Err()
	})
}

func sealRuntimeControl(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, value mutationRevocation) error {
	_, err := conn.ExecContext(ctx, `INSERT INTO runtime_ticket_controls(
		channel,project_id,ticket_id,state,generation,stop_version,stop_leader_epoch,stop_runner_epoch,authority_version,authority_leader_epoch,authority_runner_epoch,updated_at)
		VALUES(?,?,?,'sealed',1,?,?,?,?,?,?,?)
		ON CONFLICT(channel,project_id,ticket_id) DO UPDATE SET state='sealed',generation=runtime_ticket_controls.generation+1,
		stop_version=excluded.stop_version,stop_leader_epoch=excluded.stop_leader_epoch,stop_runner_epoch=excluded.stop_runner_epoch,
		authority_version=excluded.authority_version,authority_leader_epoch=excluded.authority_leader_epoch,authority_runner_epoch=excluded.authority_runner_epoch,updated_at=excluded.updated_at`,
		ref.Channel, ref.Project, ref.Ticket, value.version, value.leader, value.runner, value.version, value.leader, value.runner, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// RuntimeAdmissionCapability is minted only while ActivateRearm holds Store's
// mutation gate. It is the narrow object a runtime control bundle may accept
// to install the exact local admission token.
type RuntimeAdmissionCapability struct {
	mu       sync.Mutex
	ref      domain.TicketRef
	version  uint64
	fence    domain.Fence
	issued   bool
	consumed bool
	opening  bool
	opened   bool
	open     func(context.Context) error
	suspend  func(context.Context) (bool, error)
	seal     func(context.Context) error
}

func (c *RuntimeAdmissionCapability) ConsumeRuntimeAdmission() (domain.TicketRef, uint64, domain.Fence, bool) {
	if c == nil {
		return domain.TicketRef{}, 0, domain.Fence{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.issued {
		return domain.TicketRef{}, 0, domain.Fence{}, false
	}
	c.issued = false
	c.consumed = true
	return c.ref, c.version, c.fence, true
}

func (c *RuntimeAdmissionCapability) wasConsumed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumed
}

func (c *RuntimeAdmissionCapability) discard() {
	c.mu.Lock()
	c.issued = false
	c.open = nil
	c.suspend = nil
	c.seal = nil
	c.mu.Unlock()
}

// OpenStoreAdmission is intentionally useful only after ConsumeRuntimeAdmission
// has transferred the exact identity into the scheduler. The scheduler calls
// it from its first matching Begin while its per-ticket admission lock is
// held. Until then the Store hard latch rejects every authority tuple.
func (c *RuntimeAdmissionCapability) OpenStoreAdmission(ctx context.Context) error {
	if c == nil {
		return ErrStaleFence
	}
	c.mu.Lock()
	if !c.consumed || c.opened || c.opening || c.open == nil {
		c.mu.Unlock()
		return ErrStaleFence
	}
	c.opening = true
	open := c.open
	c.mu.Unlock()
	err := open(ctx)
	c.mu.Lock()
	c.opening = false
	if err == nil {
		c.opened = true
	}
	c.mu.Unlock()
	return err
}

// SuspendStoreAdmission compensates a Begin cancelled before it committed to
// runtime activity. It restores armed state only for the same exact authority.
// A concurrent operator seal stays sealed and reports non-retryable.
func (c *RuntimeAdmissionCapability) SuspendStoreAdmission(ctx context.Context) (bool, error) {
	if c == nil {
		return false, ErrStaleFence
	}
	c.mu.Lock()
	if !c.consumed || !c.opened || c.opening || c.suspend == nil {
		c.mu.Unlock()
		return false, ErrStaleFence
	}
	c.opening = true
	suspend := c.suspend
	c.mu.Unlock()
	retryable, err := suspend(ctx)
	c.mu.Lock()
	c.opening = false
	if err == nil {
		c.opened = false
	}
	c.mu.Unlock()
	return retryable, err
}

// SealStoreAdmission permanently seals an already-admitted activity. An
// earlier Controller seal is idempotent only for the identical authority.
func (c *RuntimeAdmissionCapability) SealStoreAdmission(ctx context.Context) error {
	if c == nil {
		return ErrStaleFence
	}
	c.mu.Lock()
	if !c.consumed || !c.opened || c.opening || c.seal == nil {
		c.mu.Unlock()
		return ErrStaleFence
	}
	c.opening = true
	seal := c.seal
	c.mu.Unlock()
	err := seal(ctx)
	c.mu.Lock()
	c.opening = false
	if err == nil {
		c.opened = false
	}
	c.mu.Unlock()
	return err
}

// RuntimeRetirementCapability is the terminal-only counterpart. It is issued
// after a linearized terminal proof and cannot authorize any work.
type RuntimeRetirementCapability struct {
	mu          sync.Mutex
	ref         domain.TicketRef
	issued      bool
	retiring    bool
	retireStore func(context.Context) error
}

// ConsumeRuntimeRetirement returns the terminal authorization exactly once.
// It is intentionally opaque: only Store can create a valid capability.
func (c *RuntimeRetirementCapability) ConsumeRuntimeRetirement() (domain.TicketRef, bool) {
	if c == nil {
		return domain.TicketRef{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.issued {
		return domain.TicketRef{}, false
	}
	c.issued = false
	return c.ref, true
}

// RetireRuntime applies the runtime side first, then removes Store's terminal
// latch and watermark. Both operations are retry-safe: a failed Store cleanup
// keeps the capability live and cannot create an admission route.
func (c *RuntimeRetirementCapability) RetireRuntime(ctx context.Context, retire func(domain.TicketRef) error) error {
	if c == nil || retire == nil {
		return ErrStaleFence
	}
	c.mu.Lock()
	if !c.issued || c.retiring || c.retireStore == nil {
		c.mu.Unlock()
		return ErrStaleFence
	}
	c.retiring = true
	ref, retireStore := c.ref, c.retireStore
	c.mu.Unlock()
	fail := func(err error) error {
		c.mu.Lock()
		c.retiring = false
		c.mu.Unlock()
		return err
	}
	if err := retire(ref); err != nil {
		return fail(err)
	}
	if err := retireStore(ctx); err != nil {
		return fail(err)
	}
	c.mu.Lock()
	c.issued = false
	c.retiring = false
	c.mu.Unlock()
	return nil
}

func (p TicketControlProof) Drained() bool {
	// Historical pre-publication effects may be excluded only when they are
	// settled (confirmed/failed) or merely planned. The allowlist is limited to
	// local worktree/commit and repository-command records; executing or
	// uncertain rows are always counted above. Publication/merge rows and merge
	// intents are counted whenever outstanding, while their historical presence
	// still makes StrictlyPrePublication fail closed below.
	return p.ProviderWriters == 0 && p.RepositoryCommandWriters == 0 && p.GitMutationWriters == 0 && p.UnreconciledEffects == 0 && p.OutstandingPublicationOrMergeEffects == 0 && p.OutstandingMergeIntents == 0
}

// StrictlyPrePublication is intentionally an allowlist. Any unknown effect
// may be a future publication path, so it fails closed.
func (p TicketControlProof) StrictlyPrePublication() bool {
	if p.PublicationOrMergeEffects != 0 || p.MergeIntents != 0 {
		return false
	}
	switch p.Ticket.State {
	case domain.StateQueued:
		return true
	case domain.StateStopping, domain.StateCancelling, domain.StatePaused, domain.StateBlocked:
		return prePublicationState(p.Ticket.ResumeState)
	default:
		return prePublicationState(p.Ticket.State)
	}
}

func prePublicationState(state domain.State) bool {
	return state == domain.StatePlanning || state == domain.StateVerifying || state == domain.StateBuilding
}

// postPublicationState is the explicit allowlist for a resumed ticket whose
// durable publication boundary has already been crossed.  Keeping this
// separate from prePublicationState is important: RecoverAsGuarded and the
// ordinary RearmProof must never gain a route around their pre-publication
// evidence rules.
func postPublicationState(state domain.State) bool {
	switch state {
	case domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing,
		domain.StateWaitingApproval, domain.StateWaitingManualMerge,
		domain.StateMerging, domain.StateReconciling:
		return true
	default:
		return false
	}
}

func runtimeRearmableState(state domain.State) bool {
	return prePublicationState(state) || postPublicationState(state)
}

// authenticatePostPublicationState proves the immutable evidence appropriate
// to the current phase on the same SQLite connection as the control proof.
// It intentionally does not reconstruct a missing witness or call a reader
// through Store (which could observe a different transaction).
func (s *Store) authenticatePostPublicationState(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, baselineVersion uint64, baselineFence domain.Fence) error {
	switch state {
	case domain.StatePublishing:
		// Publishing may legitimately be between the candidate and publication
		// witness.  The authenticated candidate is the fresh boundary here;
		// once publication has been recorded, its row is checked below instead.
		candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
		if err != nil || candidate.Fence != baselineFence {
			return ErrControlNotDrained
		}
		if candidate.TicketVersion == baselineVersion {
			return nil
		}
		// TransitionCandidate records the immutable candidate at the building
		// endpoint, then the authenticated phase_pass advances the ticket into
		// publishing.  A pause/resume proof for a publishing ticket therefore
		// authenticates this one exact predecessor event rather than requiring an
		// impossible candidate row at the publishing version.
		if candidate.TicketVersion == ^uint64(0) || candidate.TicketVersion+1 != baselineVersion {
			return ErrControlNotDrained
		}
		var transitions int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state='building' AND to_state='publishing'`, ref.Channel, ref.Project, ref.Ticket, baselineVersion).Scan(&transitions); err != nil || transitions != 1 {
			return ErrControlNotDrained
		}
		return nil
	case domain.StateWaitingCI:
		publication, err := loadCIPublicationBase(ctx, conn, ref)
		candidate, candidateErr := s.latestCandidateFrom(ctx, conn, ref, false)
		if err != nil || candidateErr != nil || !publicationCandidateEqual(candidate, publication.Candidate) || publication.CurrentFence != baselineFence {
			return ErrControlNotDrained
		}
		// The publication witness is recorded at publishing version N; the
		// authenticated effects_confirmed transition establishes waiting_ci at
		// N+1.  A recovered/rebound witness may already be at the live endpoint,
		// so accept either exact endpoint, or this one canonical transition.
		if publication.CurrentTicketVersion != baselineVersion &&
			(publication.CurrentTicketVersion == ^uint64(0) || publication.CurrentTicketVersion+1 != baselineVersion ||
				authenticatePublishedWaitingEvent(ctx, conn, ref, publication, baselineVersion) != nil) {
			return ErrControlNotDrained
		}
		return nil
	case domain.StateReviewing:
		candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
		if err != nil {
			return ErrControlNotDrained
		}
		observation, reviewVersion, err := s.authenticateHistoricalFinalReview(ctx, conn, ref, candidate)
		if err != nil || reviewVersion != baselineVersion || observation.ObservedFence != baselineFence {
			return ErrControlNotDrained
		}
		return nil
	case domain.StateWaitingApproval, domain.StateWaitingManualMerge:
		if err := s.authenticatePostPublicationReviewCompletion(ctx, conn, ref, state, baselineVersion, baselineFence); err != nil {
			return ErrControlNotDrained
		}
		return nil
	case domain.StateMerging:
		if err := s.authenticatePostPublicationMergeState(ctx, conn, ref, baselineVersion, baselineFence); err != nil {
			return ErrControlNotDrained
		}
		return nil
	case domain.StateReconciling:
		var mergeMode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&mergeMode); err != nil {
			return ErrControlNotDrained
		}
		if mergeMode == domain.MergeManual {
			if err := s.authenticatePostPublicationManualReconcile(ctx, conn, ref, baselineVersion, baselineFence); err != nil {
				return ErrControlNotDrained
			}
			return nil
		}
		if mergeMode != domain.MergeGuarded || s.authenticatePostPublicationGuardedReconcile(ctx, conn, ref, baselineVersion, baselineFence) != nil {
			return ErrControlNotDrained
		}
		return nil
	default:
		return ErrControlNotDrained
	}
}

func (s *Store) authenticatePostPublicationManualReconcile(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, baselineVersion uint64, baselineFence domain.Fence) error {
	value, found, err := loadManualMergeObservation(ctx, conn, ref)
	if err != nil || !found || validManualMergeObservation(value) != nil || value.CurrentTicketVersion == ^uint64(0) {
		return ErrPublicationEvidence
	}
	if err := s.authenticateManualMergePublication(ctx, conn, value); err != nil {
		return err
	}
	reconciling := normalRecoveryEndpoint{version: value.CurrentTicketVersion + 1, runner: value.CurrentFence.RunnerEpoch, leader: value.CurrentFence.LeaderEpoch}
	var events, stateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='external_merge_observed' AND from_state='waiting_manual_merge' AND to_state='reconciling' AND payload=?`, ref.Channel, ref.Project, ref.Ticket, reconciling.version, manualMergeObservationEventPayload(value.ObservationDigest)).Scan(&events, &stateChanges); err != nil || events != 1 || stateChanges != 1 {
		return ErrPublicationEvidence
	}
	leader, err := normalRecoveryLeaderAt(ctx, conn, ref, reconciling, baselineVersion, baselineFence.RunnerEpoch)
	if err != nil || leader != baselineFence.LeaderEpoch {
		return ErrPublicationEvidence
	}
	var intents, mergeEffects int
	if err := conn.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?),(SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind='merge')`, ref.Channel, ref.Project, ref.Ticket, ref.Channel, ref.Project, ref.Ticket).Scan(&intents, &mergeEffects); err != nil || intents != 0 || mergeEffects != 0 {
		return ErrPublicationEvidence
	}
	return nil
}

func (s *Store) authenticatePostPublicationReviewCompletion(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, target domain.State, baselineVersion uint64, baselineFence domain.Fence) error {
	publication, err := loadCIPublicationBase(ctx, conn, ref)
	if err != nil {
		return err
	}
	candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
	if err != nil || !publicationCandidateEqual(candidate, publication.Candidate) {
		return ErrPublicationEvidence
	}
	observation, reviewVersion, err := s.authenticateHistoricalFinalReview(ctx, conn, ref, candidate)
	if err != nil {
		return err
	}
	if observation.ObservedFence.LeaderEpoch != baselineFence.LeaderEpoch || observation.ObservedFence.RunnerEpoch != baselineFence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	var transitionCount, stateChangingCount int
	var transitionVersion uint64
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(ticket_version),0),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='review_pass' AND from_state='reviewing' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, baselineVersion, target).Scan(&transitionCount, &transitionVersion, &stateChangingCount); err != nil || transitionCount != 1 || stateChangingCount != 1 || transitionVersion != baselineVersion || reviewVersion+1 != baselineVersion || baselineFence.LeaderEpoch == 0 || baselineFence.RunnerEpoch == 0 {
		return ErrEvidenceConflict
	}
	var attemptID int64
	var attempt int
	var resultCount int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.phase='review' AND r.role='reviewer' AND a.state='completed' AND a.outcome='completed' AND a.expected_ticket_version=? AND a.leader_epoch=? AND a.runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, reviewVersion, baselineFence.LeaderEpoch, baselineFence.RunnerEpoch).Scan(&resultCount); err != nil || resultCount != 1 {
		return ErrEvidenceConflict
	}
	if err := conn.QueryRowContext(ctx, `SELECT r.provider_attempt_id,r.attempt FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.phase='review' AND r.role='reviewer' AND a.state='completed' AND a.outcome='completed' AND a.expected_ticket_version=? AND a.leader_epoch=? AND a.runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, reviewVersion, baselineFence.LeaderEpoch, baselineFence.RunnerEpoch).Scan(&attemptID, &attempt); err != nil {
		return ErrEvidenceConflict
	}
	key := ProviderAttemptResultKey{AttemptID: attemptID, Ref: ref, Phase: domain.PhaseReview, Attempt: attempt}
	result, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
	if err != nil || parsed.Reviewer == nil || parsed.Reviewer.Decision != phaseartifact.ReviewPass || parsed.Reviewer.ReviewedHead != candidate.Snapshot.HeadSHA || parsed.Reviewer.ProofDigest != candidate.Snapshot.ProofDigest || result.Claim.ExpectedVersion != reviewVersion || providerResultReachesHistoricalFence(ctx, conn, key, result, result.Claim.ExpectedVersion, domain.Fence{LeaderEpoch: result.Claim.LeaderEpoch, RunnerEpoch: result.Claim.RunnerEpoch}) != nil {
		return ErrEvidenceConflict
	}
	return nil
}

// authenticateHistoricalFinalReview is the post-publication counterpart to
// FinalReviewAuthority. It proves the candidate's source and verification
// chain plus the complete immutable CI chain, but deliberately evaluates them
// at the pre-stop endpoint; a resumed ticket has a newer control version.
func (s *Store) authenticateHistoricalFinalReview(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, candidate StoredCandidate) (CIObservation, uint64, error) {
	var source string
	if err := conn.QueryRowContext(ctx, `SELECT source_digest FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&source); err != nil || source != candidate.Snapshot.SourceDigest {
		return CIObservation{}, 0, ErrEvidenceConflict
	}
	verification, err := s.verificationEvidenceForCandidateFrom(ctx, conn, ref)
	if err != nil || candidate.Snapshot.VerificationIntentDigest != verification.Revision.IntentDigest || candidate.Snapshot.ProofDigest != verification.Revision.ProofDigest || candidate.Commit.ParentOID != verification.Checkpoint.CommitOID {
		return CIObservation{}, 0, ErrEvidenceConflict
	}
	observation, _, reviewVersion, err := finalReviewCIAuthorityFrom(ctx, conn, ref, candidate)
	if err != nil {
		return CIObservation{}, 0, ErrEvidenceConflict
	}
	return observation, reviewVersion, nil
}

func (s *Store) authenticatePostPublicationMergeState(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, baselineVersion uint64, baselineFence domain.Fence) error {
	// Merging is guarded authority, not merely a publication-shaped state.  A
	// pause/resume must therefore re-establish the final-review and exact-head
	// operator approval chain before it can reuse either a failed or a confirmed
	// merge effect.
	approval, err := s.approvalRecoveryEndpoint(ctx, conn, ref)
	if err != nil {
		return ErrPublicationEvidence
	}
	intent, found, err := singleRecoveryMergeIntent(ctx, conn, ref)
	if err != nil || !found {
		return ErrPublicationEvidence
	}
	current := normalRecoveryEndpoint{version: baselineVersion, runner: baselineFence.RunnerEpoch, leader: baselineFence.LeaderEpoch}
	if err := s.authenticateMergingRecoveryEffect(ctx, conn, ref, approval, current, baselineFence.LeaderEpoch, intent); err != nil {
		return ErrPublicationEvidence
	}
	// The common no-recovery case has one immutable claim.  A later signed
	// recovery may promote the effect claim, which authenticateMergingRecoveryEffect
	// validates above; do not mistake that authenticated promotion for a free
	// claim-epoch substitution.
	effect, err := effectFrom(ctx, conn, intent.SemanticKey)
	if err != nil {
		return ErrPublicationEvidence
	}
	if effect.TicketVersion == intent.TicketVersion && effect.RunnerEpoch == intent.RunnerEpoch && effect.LeaderEpoch == intent.LeaderEpoch && effect.ClaimEpoch != intent.ClaimEpoch {
		return ErrPublicationEvidence
	}
	return nil
}

// authenticatePostPublicationGuardedReconcile proves the actual guarded
// merging -> reconciling handoff before following only signed recovery rows to
// the sealed endpoint. A reconciling ticket is one state version newer than
// the confirmed merge intent/effect, so treating it as a merging endpoint
// would either reject every real recovery or weaken the intent binding.
func (s *Store) authenticatePostPublicationGuardedReconcile(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, baselineVersion uint64, baselineFence domain.Fence) error {
	confirmed, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, ref)
	if err != nil || confirmed.version == ^uint64(0) {
		return ErrPublicationEvidence
	}
	reconciling := normalRecoveryEndpoint{version: confirmed.version + 1, runner: confirmed.runner, leader: confirmed.leader}
	if err := canonicalGuardedMergeObservation(ctx, conn, ref, reconciling.version); err != nil {
		return ErrPublicationEvidence
	}
	leader, err := normalRecoveryLeaderAt(ctx, conn, ref, reconciling, baselineVersion, baselineFence.RunnerEpoch)
	if err != nil || leader != baselineFence.LeaderEpoch {
		return ErrPublicationEvidence
	}
	return nil
}

// SealRuntimeControl durably closes a ticket before any runtime cancellation
// is requested.  It intentionally performs no drain count: callers must
// release Store's gate, join the runtime, then obtain ControlProof.
func (s *Store) SealRuntimeControl(ctx context.Context, ref domain.TicketRef) error {
	if s == nil || s.mutations == nil || ref.Validate() != nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	var sealed mutationRevocation
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner, &leader); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		value := mutationRevocation{version: version, leader: leader, runner: runner}
		if err := sealRuntimeControl(ctx, conn, ref, value); err != nil {
			return err
		}
		sealed = value
		return nil
	})
	if err == nil {
		g.latch(ref, sealed)
	}
	return err
}

// StoppedRuntimeTicket reconstructs the stopped tuple from durable control
// authority. It is used after a Controller restart; Controller.tickets is an
// optimization, never the source of rearm authority.
func (s *Store) StoppedRuntimeTicket(ctx context.Context, ref domain.TicketRef) (Ticket, error) {
	if s == nil || ref.Validate() != nil {
		return Ticket{}, ErrStaleFence
	}
	var result Ticket
	err := s.db.QueryRowContext(ctx, `SELECT stop_version,stop_runner_epoch FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=? AND state='sealed'`, ref.Channel, ref.Project, ref.Ticket).Scan(&result.Version, &result.RunnerEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return Ticket{}, ErrStaleFence
	}
	if err != nil {
		return Ticket{}, normalizeBusy(ctx, err)
	}
	result.Ref = ref
	return result, nil
}

// MergeObservationPrePublication is a read-only classification used before a
// merge observer runs. It deliberately creates no control row or latch.
func (s *Store) MergeObservationPrePublication(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if s == nil || ref.Validate() != nil {
		return false, ErrStaleFence
	}
	var ticket Ticket
	var resume sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT state,resume_state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ticket.State, &resume, &ticket.Version, &ticket.RunnerEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	ticket.Ref = ref
	if resume.Valid {
		ticket.ResumeState = domain.State(resume.String)
	}
	var publication, intents int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command')`, ref.Channel, ref.Project, ref.Ticket).Scan(&publication); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&intents); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	return (TicketControlProof{Ticket: ticket, PublicationOrMergeEffects: publication, MergeIntents: intents}).StrictlyPrePublication(), nil
}

// ControlProof atomically revokes this ticket's current identity and proves
// whether writers/effects are drained. Callers must not compose a read
// snapshot with a later drain. Its memory revocation is installed inside the
// IMMEDIATE transaction before counts are read, which closes the post-proof
// start gap while the mutation gate remains held.
func (s *Store) ControlProof(ctx context.Context, ref domain.TicketRef) (TicketControlProof, error) {
	if err := ref.Validate(); err != nil {
		return TicketControlProof{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if g == nil {
		return TicketControlProof{}, ErrStaleFence
	}
	if err := g.lock(ctx); err != nil {
		return TicketControlProof{}, err
	}
	defer g.unlock()
	proof, leader, err := s.controlProof(ctx, ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		value := mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch}
		return sealRuntimeControl(txCtx, conn, ref, value)
	}, nil)
	if err != nil {
		return TicketControlProof{}, err
	}
	// The gate stayed held through COMMIT, so mirroring after commit cannot
	// leave a volatile latch behind when a durable seal rolls back.
	g.latch(ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	return proof, nil
}

// RearmProof checks a newer active pre-publication identity while holding the
// mutation gate and a BEGIN IMMEDIATE transaction. It deliberately retains
// the stopped revocation; the controller turns this exact proof into one
// runtime admission token while holding its control mutex.
func (s *Store) RearmProof(ctx context.Context, ref domain.TicketRef, stopped Ticket) (*RuntimeRearmCapability, error) {
	if err := ref.Validate(); err != nil || stopped.Ref != ref || stopped.Version == 0 || stopped.RunnerEpoch == 0 {
		return nil, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if g == nil {
		return nil, ErrStaleFence
	}
	if err := g.lock(ctx); err != nil {
		return nil, err
	}
	defer g.unlock()
	proof, leader, err := s.controlProof(ctx, ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		_, latched := g.control(ref)
		// Memory is only the process-start serialization gate. The durable row
		// is the authority that survives a Store reopen and authenticates the
		// exact stopped generation used by this rearm.
		control, err := runtimeControlFrom(txCtx, conn, ref)
		if err != nil || control.state != "sealed" || control.stop.version != stopped.Version || control.stop.runner != stopped.RunnerEpoch {
			return ErrStaleFence
		}
		// Authenticate the stop record against the exact current hard latch.
		// A leader-only successor is not a rearm: it has no newer ticket
		// identity and remains stopped until a real lifecycle transition.
		if !latched || (proof.Ticket.Version <= stopped.Version && proof.Ticket.RunnerEpoch == stopped.RunnerEpoch) {
			return ErrStaleFence
		}
		return nil
	}, func(_ context.Context, _ *sql.Conn, proof TicketControlProof, leader uint64) error {
		if !proof.Drained() || !proof.StrictlyPrePublication() || !prePublicationState(proof.Ticket.State) {
			return ErrControlNotDrained
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	g.latch(ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	return &RuntimeRearmCapability{ref: ref, version: proof.Ticket.Version, fence: proof.Fence, issued: true}, nil
}

// PostPublicationRearmProof is the separate authority for resuming a ticket
// after publication has begun.  It shares the sealed/drained serialization
// with RearmProof but has its own state and evidence allowlist; in particular,
// it never broadens the pre-publication proof used by RecoverAsGuarded.
func (s *Store) PostPublicationRearmProof(ctx context.Context, ref domain.TicketRef, stopped Ticket) (*RuntimeRearmCapability, error) {
	if err := ref.Validate(); err != nil || stopped.Ref != ref || stopped.Version == 0 || stopped.RunnerEpoch == 0 {
		return nil, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if g == nil {
		return nil, ErrStaleFence
	}
	if err := g.lock(ctx); err != nil {
		return nil, err
	}
	defer g.unlock()
	proof, leader, err := s.controlProof(ctx, ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		_, latched := g.control(ref)
		control, err := runtimeControlFrom(txCtx, conn, ref)
		stopMatches := control.stop.version == stopped.Version && control.stop.runner == stopped.RunnerEpoch
		if stopped.Version < ^uint64(0) && stopped.RunnerEpoch < ^uint64(0) {
			stopMatches = stopMatches || (control.stop.version == stopped.Version+1 && control.stop.runner == stopped.RunnerEpoch+1)
		}
		if err != nil || control.state != "sealed" || !stopMatches {
			return ErrStaleFence
		}
		if control.authority != control.stop {
			// A prior daemon may have fenced the resumed endpoint before the
			// rearm retry ran. The immutable resumed endpoint is stop+2; later
			// authorities are accepted only through every signed +1/+1 ledger row
			// from that endpoint. Never infer depth from counters alone.
			if control.stop.version > ^uint64(0)-2 || control.stop.runner == ^uint64(0) || control.authority.version < control.stop.version+2 || control.authority.version == ^uint64(0) || control.authority.runner == ^uint64(0) || proof.Ticket.Version != control.authority.version+1 || proof.Ticket.RunnerEpoch != control.authority.runner+1 || (stopped.State != "" && proof.Ticket.State != stopped.State) {
				return ErrStaleFence
			}
			if control.authority.version == control.stop.version+2 {
				if control.authority.runner != control.stop.runner || control.authority.leader != control.stop.leader {
					return ErrStaleFence
				}
			} else {
				if control.authority.version <= control.stop.version+2 || control.authority.runner <= control.stop.runner || control.authority.leader == 0 || control.authority.leader >= leader {
					return ErrStaleFence
				}
			}
			// Authenticate the complete recovery chain from the immutable
			// resumed endpoint through the current proof endpoint. The final
			// row must be the exact authority predecessor; otherwise a stale
			// authority could be paired with an unrelated future recovery.
			if err := validateRunnerRecoveryLedger(txCtx, conn, ref, control.stop.version+2, control.stop.runner, control.stop.leader, proof.Ticket.Version, proof.Ticket.RunnerEpoch, leader); err != nil {
				return ErrStaleFence
			}
			finalRecovery, found, err := loadRunnerRecoveryAt(txCtx, conn, ref, proof.Ticket.Version)
			if err != nil || !found || finalRecovery.PriorTicketVersion != control.authority.version || finalRecovery.PriorRunnerEpoch != control.authority.runner || finalRecovery.PriorLeaderEpoch != control.authority.leader || finalRecovery.TicketVersion != proof.Ticket.Version || finalRecovery.RunnerEpoch != proof.Ticket.RunnerEpoch || finalRecovery.LeaderEpoch != leader {
				return ErrStaleFence
			}
		}
		if !latched || proof.Ticket.Version < stopped.Version || proof.Ticket.RunnerEpoch < stopped.RunnerEpoch || !postPublicationState(proof.Ticket.State) {
			return ErrStaleFence
		}
		return nil
	}, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, _ uint64) error {
		if !proof.Drained() || !postPublicationState(proof.Ticket.State) {
			return ErrControlNotDrained
		}
		control, controlErr := runtimeControlFrom(txCtx, conn, ref)
		if controlErr != nil {
			return ErrControlNotDrained
		}
		if control.stop.version == 0 || control.stop.runner <= 1 {
			return ErrControlNotDrained
		}
		baseline := stopped
		// The durable stop row is the invalidated stopping endpoint (the
		// operator_pause event), while the controller may still hold the ticket
		// tuple from just before that event.  Normalize both forms to the
		// authenticated pre-stop business-evidence endpoint.
		baseline.Version = control.stop.version - 1
		baseline.RunnerEpoch = control.stop.runner - 1
		if err := authenticatePostPublicationResume(txCtx, conn, ref, baseline, proof.Ticket, control.stop); err != nil {
			return ErrControlNotDrained
		}
		return s.authenticatePostPublicationState(txCtx, conn, ref, proof.Ticket.State, baseline.Version, domain.Fence{LeaderEpoch: control.stop.leader, RunnerEpoch: baseline.RunnerEpoch})
	})
	if err != nil {
		return nil, err
	}
	g.latch(ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	return &RuntimeRearmCapability{ref: ref, version: proof.Ticket.Version, fence: proof.Fence, issued: true}, nil
}

// authenticatePostPublicationResume binds the resumed ticket to the exact
// operator control triplet that followed the sealed stop.  A newer counter
// without this three-event chain is ambiguous and cannot authorize rearm.
func authenticatePostPublicationResume(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, stopped, current Ticket, stop mutationRevocation) error {
	if current.Version < stopped.Version || stopped.Version > ^uint64(0)-3 || stopped.RunnerEpoch > ^uint64(0)-1 || current.Version < stopped.Version+3 || current.RunnerEpoch < stopped.RunnerEpoch+1 || stop.version != stopped.Version+1 || stop.runner != stopped.RunnerEpoch+1 || stop.leader == 0 {
		return ErrStaleFence
	}
	if stopped.State == "" {
		if err := conn.QueryRowContext(ctx, `SELECT from_state FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_pause_or_take' AND to_state='stopping'`, ref.Channel, ref.Project, ref.Ticket, stopped.Version+1).Scan(&stopped.State); err != nil {
			return ErrStaleFence
		}
	}
	checks := []struct {
		version uint64
		trigger string
		from    domain.State
		to      domain.State
	}{
		{stopped.Version + 1, "operator_pause_or_take", stopped.State, domain.StateStopping},
		{stopped.Version + 2, "process_and_effects_drained", domain.StateStopping, domain.StatePaused},
		{stopped.Version + 3, "operator_resume|operator_retry", domain.StatePaused, current.State},
	}
	for _, check := range checks {
		var count, total int
		trigger := check.trigger
		whereTrigger := "trigger=?"
		args := []any{ref.Channel, ref.Project, ref.Ticket, check.version, ref.Channel, ref.Project, ref.Ticket, check.version, trigger, check.from, check.to}
		if trigger == "operator_resume|operator_retry" {
			whereTrigger = "trigger IN ('operator_resume','operator_retry')"
			args = []any{ref.Channel, ref.Project, ref.Ticket, check.version, ref.Channel, ref.Project, ref.Ticket, check.version, check.from, check.to}
		}
		query := `SELECT COUNT(*),COALESCE((SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND ` + whereTrigger + ` AND from_state=? AND to_state=?`
		if err := conn.QueryRowContext(ctx, query, args...).Scan(&count, &total); err != nil || count != 1 || total != 1 {
			return ErrStaleFence
		}
	}
	// A daemon restart may fence the exact resumed endpoint before the retry
	// reaches Rearm. Accept only a contiguous signed recovery chain after the
	// +3 resume, never a bare counter jump. The recovery row itself supplies
	// the resumed leader endpoint that the event table intentionally omits.
	if current.Version > stopped.Version+3 {
		recovery, found, err := loadRunnerRecoveryAt(ctx, conn, ref, stopped.Version+4)
		if err != nil || !found || recovery.PriorTicketVersion != stopped.Version+3 || recovery.PriorRunnerEpoch != stopped.RunnerEpoch+1 || recovery.RunnerEpoch != stopped.RunnerEpoch+2 {
			return ErrStaleFence
		}
		var currentLeader uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&currentLeader); err != nil || currentLeader == 0 {
			return ErrStaleFence
		}
		if err := validateRunnerRecoveryLedger(ctx, conn, ref, stopped.Version+3, recovery.PriorRunnerEpoch, recovery.PriorLeaderEpoch, current.Version, current.RunnerEpoch, currentLeader); err != nil {
			return ErrStaleFence
		}
	}
	return nil
}

// RuntimeRearmNeeded is a read-only retry discriminator for the narrow crash
// window after an operator-resume transition commits but before the controller
// installs its runtime admission. It never opens admission itself. A caller
// may retry Rearm only while the exact durable control row remains sealed;
// armed/open rows prove an earlier rearm was already attempted. Both the
// pre-publication and separately authenticated post-publication authorities
// use this discriminator.
func (s *Store) RuntimeRearmNeeded(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if s == nil || ref.Validate() != nil {
		return false, ErrStaleFence
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	control, err := runtimeControlFrom(ctx, conn, ref)
	if err != nil {
		return false, err
	}
	var state domain.State
	if err := conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, ErrNotFound
		}
		return false, normalizeBusy(ctx, err)
	}
	if !runtimeRearmableState(state) || (control.state != "sealed" && control.state != "armed" && control.state != "open") {
		return false, ErrStaleFence
	}
	return control.state == "sealed", nil
}

// RetryablePause authenticates the exact terminal event that put a ticket in
// its current paused state. It is deliberately a ticket-scoped query rather
// than a bounded event-feed scan: a quiet paused ticket must not lose its
// retry path merely because other tickets produced many later audit events.
func (s *Store) RetryablePause(ctx context.Context, ticket Ticket) (bool, error) {
	if s == nil || ticket.Ref.Validate() != nil || ticket.State != domain.StatePaused || ticket.Version == 0 || ticket.ResumeState == "" {
		return false, nil
	}
	var trigger string
	err := s.db.QueryRowContext(ctx, `SELECT trigger FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND from_state=? AND to_state='paused'
		ORDER BY id DESC LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version, ticket.ResumeState).Scan(&trigger)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	return trigger == "retry_or_correction_exhausted" || trigger == "ci_red_exhausted", nil
}

// ActivateRearm is the only proof-to-runtime handoff. It holds the mutation
// gate, consumes the opaque proof, and checks the newer durable identity.
// It then releases the gate while the hard latch remains closed to install
// runtime state. Only the scheduler's matching Begin can open that latch.
// A transition or direct Store writer in either ordering fails closed instead
// of inheriting the rearm authorization.
func (s *Store) ActivateRearm(ctx context.Context, capability *RuntimeRearmCapability, install func(*RuntimeAdmissionCapability) error) error {
	if capability == nil || install == nil || s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	capability.mu.Lock()
	if !capability.issued || capability.activating {
		capability.mu.Unlock()
		g.unlock()
		return ErrStaleFence
	}
	capability.activating = true
	ref, version, fence := capability.ref, capability.version, capability.fence
	capability.mu.Unlock()
	fail := func(err error) error {
		capability.mu.Lock()
		capability.activating = false
		capability.mu.Unlock()
		return err
	}

	current, fenced := g.control(ref)
	if !fenced || current != (mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}) {
		g.unlock()
		return fail(ErrStaleFence)
	}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		var currentVersion, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentVersion, &runner, &leader); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if currentVersion != version || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
			return ErrStaleFence
		}
		control, err := runtimeControlFrom(ctx, conn, ref)
		if err != nil || control.state != "sealed" {
			return ErrStaleFence
		}
		updated, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET state='armed',authority_version=?,authority_leader_epoch=?,authority_runner_epoch=?,updated_at=?
			WHERE channel=? AND project_id=? AND ticket_id=? AND state='sealed' AND generation=?`, version, fence.LeaderEpoch, fence.RunnerEpoch, time.Now().UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket, control.generation)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		return nil
	}); err != nil {
		g.unlock()
		return fail(err)
	}
	// Do not hold Store's gate while installing scheduler state. The hard latch
	// remains closed, and the scheduler's first exact Begin re-validates this
	// tuple before it can release the latch. This gives every path one lock
	// order and avoids Store-gate -> admission versus admission -> Store-gate
	// inversion across tickets.
	g.unlock()
	admission := &RuntimeAdmissionCapability{ref: ref, version: version, fence: fence, issued: true}
	admission.open = func(openCtx context.Context) error {
		return s.openRuntimeAdmission(openCtx, ref, version, fence)
	}
	admission.suspend = func(suspendCtx context.Context) (bool, error) {
		return s.suspendRuntimeAdmission(suspendCtx, ref, version, fence)
	}
	admission.seal = func(sealCtx context.Context) error {
		return s.sealRuntimeAdmission(sealCtx, ref, version, fence)
	}
	if err := install(admission); err != nil {
		sealCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		sealErr := s.sealRuntimeAdmission(sealCtx, ref, version, fence)
		cancel()
		if sealErr != nil {
			return fail(errors.Join(err, sealErr))
		}
		return fail(err)
	}
	if !admission.wasConsumed() {
		admission.discard()
		sealCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		sealErr := s.sealRuntimeAdmission(sealCtx, ref, version, fence)
		cancel()
		if sealErr != nil {
			return fail(sealErr)
		}
		return fail(ErrStaleFence)
	}
	capability.mu.Lock()
	capability.issued = false
	capability.activating = false
	capability.mu.Unlock()
	return nil
}

// openRuntimeAdmission is reachable only through the consumed opaque
// capability held by the scheduler. It validates the current durable tuple
// under the external gate, then opens only that exact hard latch.
func (s *Store) openRuntimeAdmission(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	current, latched := g.control(ref)
	expected := mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}
	if !latched || current != expected {
		return ErrStaleFence
	}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		var gotVersion, gotRunner, gotLeader uint64
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &gotVersion, &gotRunner, &gotLeader); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if !runtimeRearmableState(state) || gotVersion != version || gotRunner != fence.RunnerEpoch || gotLeader != fence.LeaderEpoch {
			return ErrStaleFence
		}
		updated, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET state='open',updated_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='armed' AND authority_version=? AND authority_leader_epoch=? AND authority_runner_epoch=?`, time.Now().UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		return nil
	}); err != nil {
		return err
	}
	if !g.openControl(ref, expected) {
		return ErrStaleFence
	}
	return nil
}

// suspendRuntimeAdmission returns a pre-commit cancellation to armed state
// only while its exact authority remains current. A matching permanent seal
// wins the race and is reported as non-retryable; mismatched seals fail.
func (s *Store) suspendRuntimeAdmission(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (bool, error) {
	if s == nil || s.mutations == nil {
		return false, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	// Durable seal comes first. The memory latch only mirrors that committed
	// authority for an already-running process; it is never the proof itself.
	expected := mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := g.lock(closeCtx); err != nil {
		return false, err
	}
	defer g.unlock()
	retryable := false
	err := s.write(closeCtx, func(conn *sql.Conn) error {
		control, err := runtimeControlFrom(closeCtx, conn, ref)
		if err != nil {
			return err
		}
		if control.authority != expected {
			return ErrStaleFence
		}
		if control.state == "sealed" {
			return nil
		}
		if control.state != "open" {
			return ErrStaleFence
		}
		updated, err := conn.ExecContext(closeCtx, `UPDATE runtime_ticket_controls SET state='armed',updated_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='open' AND authority_version=? AND authority_leader_epoch=? AND authority_runner_epoch=?`, time.Now().UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		retryable = true
		return nil
	})
	if err != nil {
		return false, normalizeBusy(closeCtx, err)
	}
	g.latch(ref, expected)
	return retryable, nil
}

// sealRuntimeAdmission permanently seals an exact active or pending
// authority. Controller's Store-first seal is idempotent for that exact
// authority. It also recognizes the one immediate successor created by the
// normative stopping/cancelling transition: that successor was already sealed
// atomically with runner invalidation, so the old activity may be cancelled
// and joined without ever modifying the newer authority.
func (s *Store) sealRuntimeAdmission(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	expected := mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}
	latched := expected
	sealCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := g.lock(sealCtx); err != nil {
		return err
	}
	defer g.unlock()
	err := s.write(sealCtx, func(conn *sql.Conn) error {
		control, err := runtimeControlFrom(sealCtx, conn, ref)
		if err != nil {
			return err
		}
		if control.authority != expected {
			var state domain.State
			var version, runner, leader uint64
			if err := conn.QueryRowContext(sealCtx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrNotFound
				}
				return err
			}
			// This is not a second seal. The Store's atomic control transition
			// already sealed Y before Controller asked the runtime to stop X.
			// Accept no other mismatch: preserving the exact +1 tuple and the
			// stopping/cancelling state prevents an old activity from closing a
			// different or later owner.
			if control.state == "sealed" && (state == domain.StateStopping || state == domain.StateCancelling) {
				if control.authority.version == expected.version+1 && control.authority.runner == expected.runner+1 && control.authority.leader == expected.leader && version == control.authority.version && runner == control.authority.runner && leader == control.authority.leader {
					latched = control.authority
					return nil
				}
			}
			return ErrStaleFence
		}
		if control.state == "sealed" {
			return nil
		}
		if control.state != "open" && control.state != "armed" {
			return ErrStaleFence
		}
		return sealRuntimeControl(sealCtx, conn, ref, expected)
	})
	if err != nil {
		return normalizeBusy(sealCtx, err)
	}
	g.latch(ref, latched)
	return nil
}

// TerminalControlProof safely clears a retained in-memory stop record only
// after Store proves terminal state and no writer/publication ambiguity.
func (s *Store) TerminalControlProof(ctx context.Context, ref domain.TicketRef) (*RuntimeRetirementCapability, error) {
	proof, err := s.ControlProof(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !proof.Ticket.State.Terminal() || !proof.Drained() {
		return nil, ErrControlNotDrained
	}
	value := mutationRevocation{version: proof.Ticket.Version, leader: proof.Fence.LeaderEpoch, runner: proof.Ticket.RunnerEpoch}
	return &RuntimeRetirementCapability{ref: ref, issued: true, retireStore: func(retireCtx context.Context) error {
		return s.retireRuntimeControl(retireCtx, ref, value)
	}}, nil
}

func (s *Store) retireRuntimeControl(ctx context.Context, ref domain.TicketRef, value mutationRevocation) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	if current, ok := g.control(ref); !ok || current != value {
		return ErrStaleFence
	}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state); err != nil {
			return err
		}
		if !state.Terminal() {
			return ErrStaleFence
		}
		control, err := runtimeControlFrom(ctx, conn, ref)
		if err != nil || control.authority != value {
			return ErrStaleFence
		}
		if _, err := conn.ExecContext(ctx, `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=? AND state='sealed'`, ref.Channel, ref.Project, ref.Ticket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !g.retireControl(ref, value) {
		return ErrStaleFence
	}
	return nil
}

func (s *Store) controlProof(ctx context.Context, ref domain.TicketRef, gate *ExternalMutationGate, beforeCounts, afterCounts func(context.Context, *sql.Conn, TicketControlProof, uint64) error) (TicketControlProof, uint64, error) {
	var proof TicketControlProof
	var leader uint64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var resume sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT state,resume_state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&proof.Ticket.State, &resume, &proof.Ticket.Version, &proof.Ticket.RunnerEpoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		proof.Ticket.Ref = ref
		if resume.Valid {
			proof.Ticket.ResumeState = domain.State(resume.String)
		}
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader); err != nil {
			return err
		}
		proof.Fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: proof.Ticket.RunnerEpoch}
		if beforeCounts != nil {
			// This store-side fence is deliberately installed while this BEGIN
			// IMMEDIATE transaction owns SQLite's writer slot. An admission that
			// committed before us is visible to the following counts; one that
			// starts after our commit observes this fence and is rejected.
			if err := beforeCounts(ctx, conn, proof, leader); err != nil {
				return err
			}
		}
		// Only this three-kind allowlist can be historical yet non-outstanding:
		// planned means no process began, confirmed/failed are terminal durable
		// observations, and executing/uncertain are included separately. Every
		// other kind is future-publication-sensitive and therefore remains in
		// both the historical and outstanding publication/merge checks.
		counts := []struct {
			query string
			out   *int
		}{
			{`SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, &proof.ProviderWriters},
			{`SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, &proof.RepositoryCommandWriters},
			{`SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, &proof.GitMutationWriters},
			{`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('executing','uncertain')`, &proof.UnreconciledEffects},
			{`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command')`, &proof.PublicationOrMergeEffects},
			{`SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, &proof.MergeIntents},
			{`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command') AND state IN ('planned','executing','uncertain')`, &proof.OutstandingPublicationOrMergeEffects},
			{`SELECT COUNT(*) FROM merge_intents m JOIN effects e ON e.semantic_key=m.semantic_key WHERE m.channel=? AND m.project_id=? AND m.ticket_id=? AND e.state IN ('planned','executing','uncertain')`, &proof.OutstandingMergeIntents},
		}
		for _, count := range counts {
			if err := conn.QueryRowContext(ctx, count.query, ref.Channel, ref.Project, ref.Ticket).Scan(count.out); err != nil {
				return err
			}
		}
		if afterCounts != nil {
			if err := afterCounts(ctx, conn, proof, leader); err != nil {
				return err
			}
		}
		if s.controlProofHook != nil {
			s.controlProofHook()
		}
		return nil
	})
	return proof, leader, err
}
