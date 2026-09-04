package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// ProviderRetryWorktreeProof is Store's read-only authority for inspecting a
// retained checkout around a provider retry. ExpectedHead is not caller
// supplied: it is the exact registration, verification, or candidate endpoint
// consumed by the Store transition that opened the phase. A verification or
// candidate endpoint is additionally bound to its exact confirmed Store-owned
// commit intent. The filesystem boundary must still reauthenticate Worktree and
// prove that its current HEAD equals ExpectedHead and that it is clean.
type ProviderRetryWorktreeProof struct {
	Ref          domain.TicketRef
	Phase        domain.Phase
	Version      uint64
	Fence        domain.Fence
	Worktree     StoredWorktree
	ExpectedHead string
}

// ProviderRetryWorktreeProof authenticates either the exact first
// provider-retry pause before it is consumed, or the exact active endpoint
// created by that one durable retry epoch. This lets runtime control perform
// a paused preflight and then repeat the same strict physical proof while the
// active runtime remains sealed, immediately before rearming the worker.
// Every read, including ticket/leader authority, the authenticated terminal
// pair, registration, semantic predecessor, and commit lineage, is made
// through one read transaction. The method never mutates or creates a path.
func (s *Store) ProviderRetryWorktreeProof(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (ProviderRetryWorktreeProof, error) {
	if s == nil || ref.Validate() != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return ProviderRetryWorktreeProof{}, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProviderRetryWorktreeProof{}, normalizeBusy(ctx, err)
	}
	defer tx.Rollback()
	return s.providerRetryWorktreeProofFrom(ctx, tx, ref, expectedVersion, fence)
}

// providerRetryWorktreeProofFrom is the connection-scoped form used by
// runtime-control admission. Its caller owns the surrounding read or write
// transaction, so retry evidence and the durable seal can be authenticated in
// one SQLite snapshot without a nested Store.db read.
func (s *Store) providerRetryWorktreeProofFrom(ctx context.Context, q rowQueryer, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (ProviderRetryWorktreeProof, error) {
	if s == nil || q == nil || ref.Validate() != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return ProviderRetryWorktreeProof{}, ErrStaleFence
	}
	var state domain.State
	var resume sql.NullString
	var version, runner, leader uint64
	err := q.QueryRowContext(ctx, `SELECT t.state,t.resume_state,t.version,t.runner_epoch,d.leader_epoch
		FROM tickets t JOIN daemon_instances d ON d.channel=t.channel
		WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).
		Scan(&state, &resume, &version, &runner, &leader)
	if errors.Is(err, sql.ErrNoRows) {
		return ProviderRetryWorktreeProof{}, ErrNotFound
	}
	if err != nil {
		return ProviderRetryWorktreeProof{}, normalizeBusy(ctx, err)
	}
	if version != expectedVersion || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
		return ProviderRetryWorktreeProof{}, ErrStaleFence
	}
	resumeState := state
	pausedEndpoint := state == domain.StatePaused
	if pausedEndpoint {
		if !resume.Valid {
			return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
		}
		resumeState = domain.State(resume.String)
	} else if resume.Valid {
		return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
	}
	phase, ok := providerPhaseForState(resumeState)
	if !ok {
		return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
	}
	exhaustionVersion := version
	var activeEpoch providerRetryEpoch
	if !pausedEndpoint {
		var found bool
		activeEpoch, found, err = loadProviderRetryEpoch(ctx, q, ref, phase)
		if err != nil || !found || activeEpoch.RetryVersion == 0 || activeEpoch.ExhaustionVersion == 0 || version < activeEpoch.RetryVersion || runner < activeEpoch.RetryRunner {
			return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
		}
		if version == activeEpoch.RetryVersion {
			if runner != activeEpoch.RetryRunner || leader != activeEpoch.RetryLeader {
				return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
			}
		} else if validateRunnerRecoveryLedger(ctx, q, ref, activeEpoch.RetryVersion, activeEpoch.RetryRunner, activeEpoch.RetryLeader, version, runner, leader) != nil {
			return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
		}
		exhaustionVersion = activeEpoch.ExhaustionVersion
	}
	if err := exactStateChangeEvent(ctx, q, ref, exhaustionVersion, "retry_or_correction_exhausted", resumeState, domain.StatePaused); err != nil {
		return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
	}
	var trigger, raw string
	var from, to domain.State
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger='retry_or_correction_exhausted' AND from_state=? AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, exhaustionVersion, resumeState, domain.StatePaused).
		Scan(&trigger, &from, &to, &raw); err != nil {
		return ProviderRetryWorktreeProof{}, normalizeBusy(ctx, err)
	}
	var exhaustion providerExhaustionPayload
	if trigger != "retry_or_correction_exhausted" || from != resumeState || to != domain.StatePaused || len(raw) == 0 || len(raw) > maxEvidenceJSON || json.Unmarshal([]byte(raw), &exhaustion) != nil || exhaustion.Schema != providerExhaustionSchema || exhaustion.Phase != phase || exhaustion.RetryEpoch != 0 || exhaustion.EntryTicketVersion == 0 || len(exhaustion.Attempts) != 2 || exhaustion.Attempts[0] <= 0 || exhaustion.Attempts[1] != exhaustion.Attempts[0]+1 {
		return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
	}
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, phase, exhaustion.EntryTicketVersion)
	if err != nil || entry.Version != exhaustion.EntryTicketVersion || entry.State != resumeState {
		return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
	}
	if err := authenticateProviderRetryPhaseEntryEvent(ctx, q, ref, entry); err != nil {
		return ProviderRetryWorktreeProof{}, err
	}
	pair, err := authenticateProviderRetryAttemptPair(ctx, q, ref, phase, entry, exhaustion.Attempts[0], exhaustion.Attempts[1])
	if err != nil || pair.Reason != exhaustion.Reason || (pair.Reason != "" && pair.Reason != providerExhaustionReasonInvalidArtifact) {
		return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
	}
	if providerRetryLineageRequiresResubmission(phase, entry) {
		return ProviderRetryWorktreeProof{}, ErrProviderRetryRequiresResubmission
	}
	epoch, found, err := loadProviderRetryEpochForEntry(ctx, q, ref, phase, entry.Version)
	if err != nil {
		return ProviderRetryWorktreeProof{}, normalizeBusy(ctx, err)
	}
	if pausedEndpoint {
		if found {
			return ProviderRetryWorktreeProof{}, ErrBudgetExhausted
		}
	} else {
		if !found || epoch != activeEpoch || epoch.ExhaustionVersion != exhaustionVersion || epoch.EntryVersion != exhaustion.EntryTicketVersion || epoch.InitialFirst != exhaustion.Attempts[0] || epoch.InitialLast != exhaustion.Attempts[1] {
			return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
		}
		if err := validateProviderRetryAdvance(ctx, q, ref, phase, epoch.ExhaustionVersion-1, epoch.ExhaustionRunner, epoch.ExhaustionLeader, epoch.RetryVersion, epoch.RetryRunner, epoch.RetryLeader); err != nil {
			return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
		}
	}

	worktree, project, err := providerRetryWorktreeFrom(ctx, q, ref)
	if err != nil {
		return ProviderRetryWorktreeProof{}, err
	}
	for _, claim := range pair.Claims {
		if claim.Ref != ref || claim.Phase != phase || claim.Repository != project.Path || claim.Worktree != worktree.Path || claim.WorktreeIdentity != string(worktree.IdentityJSON) || claim.BaseSHA != worktree.BaseSHA {
			return ProviderRetryWorktreeProof{}, ErrEvidenceConflict
		}
	}
	expectedHead, err := s.providerRetryPhaseExpectedHead(ctx, q, ref, phase, entry, project, worktree)
	if err != nil {
		return ProviderRetryWorktreeProof{}, err
	}
	return ProviderRetryWorktreeProof{Ref: ref, Phase: phase, Version: version, Fence: fence, Worktree: worktree, ExpectedHead: expectedHead}, nil
}

// ProviderRetryRearmProof is the narrow runtime-admission authority for a
// provider retry that was transitioned while its runtime remained sealed. It
// exists separately from RearmProof and PostPublicationRearmProof: the former
// deliberately excludes reviewing, while the latter requires a successful
// final-review result which an exhausted review phase cannot possess.
//
// The caller must repeat the physical registered-worktree authentication with
// ProviderRetryWorktreeProof after the retry transition and before redeeming
// this capability. This method independently binds the same semantic endpoint,
// exact retry epoch, durable pre-transition seal, current ticket/leader, and
// drained Store writers in one mutation-gated transaction.
func (s *Store) ProviderRetryRearmProof(ctx context.Context, ref domain.TicketRef, stopped Ticket) (*RuntimeRearmCapability, error) {
	if s == nil || s.mutations == nil || ref.Validate() != nil || stopped.Ref != ref || stopped.Version == 0 || stopped.RunnerEpoch == 0 ||
		(stopped.State != "" && stopped.State != domain.StatePaused) ||
		(stopped.ResumeState != "" && !providerStateForPhaseTransition(stopped.ResumeState)) {
		return nil, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return nil, err
	}
	defer g.unlock()
	proof, leader, err := s.controlProof(ctx, ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		_, latched := g.control(ref)
		if !latched || proof.Ticket.Version <= stopped.Version || proof.Ticket.RunnerEpoch < stopped.RunnerEpoch ||
			(stopped.ResumeState != "" && proof.Ticket.State != stopped.ResumeState) {
			return ErrStaleFence
		}
		worktreeProof, err := s.providerRetryWorktreeProofFrom(txCtx, conn, ref, proof.Ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: proof.Ticket.RunnerEpoch})
		if errors.Is(err, ErrProviderRetryRequiresResubmission) {
			return err
		}
		if err != nil || (stopped.ResumeState != "" && worktreeProof.Phase != phaseForProviderState(stopped.ResumeState)) {
			return ErrStaleFence
		}
		current := mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch}
		control, epoch, err := providerRetrySealedControlFrom(txCtx, conn, ref, worktreeProof.Phase, current)
		if err != nil || control.stop.version != stopped.Version || control.stop.runner != stopped.RunnerEpoch {
			return ErrStaleFence
		}
		if epoch.RetryVersion <= stopped.Version {
			return ErrStaleFence
		}
		return nil
	}, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		if !proof.Drained() {
			return ErrControlNotDrained
		}
		worktreeProof, err := s.providerRetryWorktreeProofFrom(txCtx, conn, ref, proof.Ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: proof.Ticket.RunnerEpoch})
		if errors.Is(err, ErrProviderRetryRequiresResubmission) {
			return err
		}
		if err != nil || (stopped.ResumeState != "" && worktreeProof.Phase != phaseForProviderState(stopped.ResumeState)) {
			return ErrControlNotDrained
		}
		current := mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch}
		if _, _, err := providerRetrySealedControlFrom(txCtx, conn, ref, worktreeProof.Phase, current); err != nil {
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

func providerRetrySealedControlFrom(ctx context.Context, q rowQueryer, ref domain.TicketRef, phase domain.Phase, current mutationRevocation) (durableRuntimeControl, providerRetryEpoch, error) {
	control, epoch, err := providerRetryRuntimeControlFrom(ctx, q, ref, phase, current)
	if err != nil {
		return durableRuntimeControl{}, providerRetryEpoch{}, err
	}
	if control.state != "sealed" {
		return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
	}
	return control, epoch, nil
}

// providerRetryRuntimeControlFrom authenticates the durable control generation
// which encloses one Store-owned provider retry. The stop endpoint is always
// the original exhausted pause. Before the first rearm, authority remains at
// that stop. After a successful rearm (and after restoreRuntimeControls seals
// an interrupted armed/open runtime), authority may instead be the retry
// endpoint or a later endpoint reached only through the signed recovery
// ledger. No other control generation can inherit this retry authority.
func providerRetryRuntimeControlFrom(ctx context.Context, q rowQueryer, ref domain.TicketRef, phase domain.Phase, current mutationRevocation) (durableRuntimeControl, providerRetryEpoch, error) {
	if current.version == 0 || current.leader == 0 || current.runner == 0 {
		return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
	}
	epoch, found, err := loadProviderRetryEpoch(ctx, q, ref, phase)
	if err != nil || !found {
		if err != nil {
			return durableRuntimeControl{}, providerRetryEpoch{}, err
		}
		return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
	}
	var control durableRuntimeControl
	if err := q.QueryRowContext(ctx, `SELECT state,generation,stop_version,stop_leader_epoch,stop_runner_epoch,authority_version,authority_leader_epoch,authority_runner_epoch
		FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&control.state, &control.generation, &control.stop.version, &control.stop.leader, &control.stop.runner,
		&control.authority.version, &control.authority.leader, &control.authority.runner); err != nil {
		return durableRuntimeControl{}, providerRetryEpoch{}, err
	}
	exhaustion := mutationRevocation{version: epoch.ExhaustionVersion, leader: epoch.ExhaustionLeader, runner: epoch.ExhaustionRunner}
	if control.generation == 0 || control.stop != exhaustion {
		return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
	}
	retry := mutationRevocation{version: epoch.RetryVersion, leader: epoch.RetryLeader, runner: epoch.RetryRunner}
	if current != retry {
		if current.version < retry.version || current.runner < retry.runner || current.leader < retry.leader ||
			validateRunnerRecoveryLedgerPrefix(ctx, q, ref, retry.version, retry.runner, retry.leader, current.version, current.runner, current.leader) != nil {
			return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
		}
	}
	switch control.state {
	case "sealed":
		if control.authority == exhaustion {
			return control, epoch, nil
		}
		if control.authority != retry {
			if control.authority.version < retry.version || control.authority.runner < retry.runner || control.authority.leader < retry.leader ||
				validateRunnerRecoveryLedgerPrefix(ctx, q, ref, retry.version, retry.runner, retry.leader, control.authority.version, control.authority.runner, control.authority.leader) != nil {
				return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
			}
		}
		if control.authority != current && validateRunnerRecoveryLedgerPrefix(ctx, q, ref, control.authority.version, control.authority.runner, control.authority.leader, current.version, current.runner, current.leader) != nil {
			return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
		}
	case "armed", "open":
		if control.authority != current {
			return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
		}
	default:
		return durableRuntimeControl{}, providerRetryEpoch{}, ErrEvidenceConflict
	}
	return control, epoch, nil
}

// providerRetryPhaseExpectedHead derives the only repository head admitted by
// the Store transition which opened this provider phase. It authenticates the
// immutable transition event and the checkpoint/candidate evidence on the
// caller's read transaction. Checkpoint-bearing evidence is accepted only when
// its exact parent/child/tree tuple belongs to one confirmed Store-owned commit
// intent. Unrelated confirmed commits (for example a rejected amendment) grant
// no authority and do not prevent an operator from restoring the semantic
// endpoint before retrying.
func (s *Store) providerRetryPhaseExpectedHead(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, phase domain.Phase, entry providerPhaseEntry, project Project, worktree StoredWorktree) (string, error) {
	if entry.Phase != phase || entry.Version == 0 || entry.State != providerStateForPhase(phase) {
		return "", ErrEvidenceConflict
	}
	if err := authenticateProviderRetryPhaseEntryEvent(ctx, q, ref, entry); err != nil {
		return "", err
	}
	if providerRetryLineageRequiresResubmission(phase, entry) {
		return "", ErrProviderRetryRequiresResubmission
	}

	verificationHead := func() (string, error) {
		stored, err := s.verificationEvidenceForCandidateFrom(ctx, q, ref)
		if err != nil || s.reauthenticateStoredVerificationCommandHistoricalFrom(ctx, q, ref, stored) != nil || stored.TicketVersion == 0 || stored.TicketVersion >= entry.Version || stored.Checkpoint.CommitOID != stored.Revision.CheckpointID || !validOID(stored.Checkpoint.CommitOID) || len(stored.Checkpoint.CommitOID) != len(worktree.BaseSHA) || providerRetryConfirmedCommit(ctx, q, ref, project, worktree, stored.Checkpoint, entry.Version) != nil {
			return "", ErrEvidenceConflict
		}
		return stored.Checkpoint.CommitOID, nil
	}
	candidateHead := func() (string, error) {
		stored, err := s.latestCandidateFrom(ctx, q, ref, false)
		if err != nil || s.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, q, ref, stored) != nil || stored.TicketVersion == 0 || stored.TicketVersion >= entry.Version || stored.Commit.CommitOID != stored.Snapshot.HeadSHA || stored.Snapshot.BaseSHA != worktree.BaseSHA || !validOID(stored.Snapshot.HeadSHA) || len(stored.Snapshot.HeadSHA) != len(worktree.BaseSHA) || providerRetryConfirmedCommit(ctx, q, ref, project, worktree, stored.Commit, entry.Version) != nil {
			return "", ErrEvidenceConflict
		}
		return stored.Snapshot.HeadSHA, nil
	}

	switch phase {
	case domain.PhasePlanning:
		if entry.From == domain.StateQueued && (entry.Trigger == "operator_start" || entry.Trigger == "start_or_adopt") {
			return worktree.HeadSHA, nil
		}
	case domain.PhaseVerification:
		switch {
		case entry.From == domain.StatePlanning && entry.Trigger == "phase_pass":
			return worktree.HeadSHA, nil
		case entry.From == domain.StateReviewing && entry.Trigger == "review_repair":
			return candidateHead()
		}
	case domain.PhaseBuild:
		switch {
		case entry.From == domain.StateVerifying && entry.Trigger == "phase_pass":
			return verificationHead()
		case entry.From == domain.StateVerifying && (entry.Trigger == "amendment_accepted" || entry.Trigger == "amendment_rejected"):
			boundary, err := loadVerificationAmendmentBoundaryAt(ctx, q, ref, entry.Version, domain.Fence{LeaderEpoch: entry.Leader, RunnerEpoch: entry.Runner}, false)
			if err != nil || boundary.DecisionVersion != entry.Version ||
				(boundary.Decision == VerificationAmendmentAccepted && entry.Trigger != "amendment_accepted") ||
				(boundary.Decision == VerificationAmendmentRejected && entry.Trigger != "amendment_rejected") {
				return "", ErrEvidenceConflict
			}
			return verificationHead()
		case (entry.From == domain.StateReviewing && entry.Trigger == "review_repair") ||
			(entry.From == domain.StateWaitingCI && entry.Trigger == "checks_red") ||
			((entry.From == domain.StateWaitingApproval || entry.From == domain.StateWaitingManualMerge) && entry.Trigger == "operator_rejected") ||
			(entry.From == domain.StateBlocked && entry.Trigger == "operator_recover"):
			return candidateHead()
		}
	case domain.PhaseReview:
		if (entry.From == domain.StateBuilding && entry.Trigger == "phase_pass") || (entry.From == domain.StateWaitingCI && entry.Trigger == "checks_green") {
			return candidateHead()
		}
	}
	return "", ErrEvidenceConflict
}

func authenticateProviderRetryPhaseEntryEvent(ctx context.Context, q rowQueryer, ref domain.TicketRef, entry providerPhaseEntry) error {
	var trigger string
	var from, to domain.State
	var created string
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,created_at FROM events
		WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, entry.EventID, ref.Channel, ref.Project, ref.Ticket, entry.Version).
		Scan(&trigger, &from, &to, &created); err != nil || trigger != entry.Trigger || from != entry.From || to != entry.State || created != entry.EventCreated {
		return ErrEvidenceConflict
	}
	if err := exactStateChangeEvent(ctx, q, ref, entry.Version, entry.Trigger, entry.From, entry.State); err != nil {
		return ErrEvidenceConflict
	}
	return nil
}

func providerRetryLineageRequiresResubmission(phase domain.Phase, entry providerPhaseEntry) bool {
	return phase == domain.PhaseVerification &&
		((entry.From == domain.StateBuilding && entry.Trigger == "verification_amendment_requested") ||
			(entry.From == domain.StatePaused && entry.Trigger == "operator_resume"))
}

func providerRetryWorktreeFrom(ctx context.Context, q rowQueryer, ref domain.TicketRef) (StoredWorktree, Project, error) {
	var worktree StoredWorktree
	var project Project
	err := q.QueryRowContext(ctx, `SELECT w.path,w.branch_ref,w.state,w.identity_json,w.base_sha,w.head_sha,w.ticket_version,w.leader_epoch,w.runner_epoch,p.canonical_path,p.base_ref
		FROM worktrees w JOIN projects p ON p.channel=w.channel AND p.id=w.project_id
		WHERE w.channel=? AND w.project_id=? AND w.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).
		Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA, &worktree.HeadSHA, &worktree.TicketVersion, &worktree.Fence.LeaderEpoch, &worktree.Fence.RunnerEpoch, &project.Path, &project.BaseRef)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredWorktree{}, Project{}, ErrNotFound
	}
	if err != nil {
		return StoredWorktree{}, Project{}, err
	}
	project.Channel, project.ID = ref.Channel, ref.Project
	if worktree.State != "registered" || !validStorePath(project.Path) || !boundedText(project.BaseRef, 300) || !validStorePath(worktree.Path) || !boundedText(worktree.Branch, 300) || !validOID(worktree.BaseSHA) || !validOID(worktree.HeadSHA) || worktree.TicketVersion == 0 || worktree.Fence.LeaderEpoch == 0 || worktree.Fence.RunnerEpoch == 0 || !validRepositoryWorktreeIdentity(string(worktree.IdentityJSON), project.Path, worktree.Path, worktree.Branch, project.BaseRef, worktree.BaseSHA) {
		return StoredWorktree{}, Project{}, ErrEvidenceConflict
	}
	if err := providerRetryWorktreeCreationBinding(ctx, q, ref, project, worktree); err != nil {
		return StoredWorktree{}, Project{}, err
	}
	return worktree, project, nil
}

// providerRetryWorktreeCreationBinding prevents a mutable registration row
// from becoming head authority by itself. The exact path, branch, base, head,
// and identity must be the sole confirmed Store-owned create-worktree intent.
func providerRetryWorktreeCreationBinding(ctx context.Context, q rowQueryer, ref domain.TicketRef, project Project, worktree StoredWorktree) error {
	rows, err := q.QueryContext(ctx, `SELECT i.semantic_key FROM git_mutation_intents i
		JOIN effects e ON e.semantic_key=i.semantic_key
		WHERE i.channel=? AND i.project_id=? AND i.ticket_id=?
		AND i.operation='create-worktree' AND e.state IN ('executing','uncertain','confirmed')
		ORDER BY i.semantic_key`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
		if len(keys) > 1 {
			rows.Close()
			return ErrEvidenceConflict
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(keys) != 1 {
		return ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, q, keys[0])
	if err != nil || facts.Claim.TicketRef != ref || facts.Claim.Operation != "create-worktree" || facts.Claim.Repository != project.Path || facts.Claim.Worktree != worktree.Path || facts.Claim.Branch != worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != worktree.BaseSHA || facts.Claim.ExpectedHeadOID != worktree.HeadSHA || facts.Effect.State != EffectConfirmed || facts.ObservedIdentity != string(worktree.IdentityJSON) {
		return ErrEvidenceConflict
	}
	return nil
}

// providerRetryConfirmedCommit binds one semantic checkpoint to exactly one
// immutable, confirmed git/commit intent. It deliberately ignores commits for
// other semantic endpoints: those may be legitimate discarded amendment or
// correction generations and cannot grant authority to this checkpoint.
func providerRetryConfirmedCommit(ctx context.Context, q rowQueryer, ref domain.TicketRef, project Project, worktree StoredWorktree, commit CommitObservation, entryVersion uint64) error {
	if entryVersion == 0 || !validOID(commit.CommitOID) || !validOID(commit.ParentOID) || !validOID(commit.TreeOID) || len(commit.CommitOID) != len(worktree.BaseSHA) || len(commit.ParentOID) != len(worktree.BaseSHA) || len(commit.TreeOID) != len(worktree.BaseSHA) {
		return ErrEvidenceConflict
	}
	rows, err := q.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents
		WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit'
		AND prepared_commit_oid=? AND prepared_tree_oid=?
		ORDER BY semantic_key`, ref.Channel, ref.Project, ref.Ticket, commit.CommitOID, commit.TreeOID)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
		if len(keys) > 1 {
			rows.Close()
			return ErrEvidenceConflict
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(keys) != 1 {
		return ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, q, keys[0])
	if err != nil || facts.Claim.TicketRef != ref || facts.Claim.Operation != "commit" || facts.Claim.TicketVersion >= entryVersion || facts.Claim.Repository != project.Path || facts.Claim.Worktree != worktree.Path || facts.Claim.Branch != worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != worktree.BaseSHA || facts.Claim.ExpectedHeadOID != commit.ParentOID || facts.Effect.State != EffectConfirmed || facts.PreparedCommitOID != commit.CommitOID || facts.PreparedTreeOID != commit.TreeOID || facts.ObservedIdentity != commit.CommitOID {
		return ErrEvidenceConflict
	}
	return nil
}
