package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

const maxProtectedBaseRefreshReclaims = 8

func protectedBaseRefreshRecoveryGap(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, fromVersion, fromRunner, fromLeader, toVersion, toRunner, toLeader uint64) bool {
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
	if err != nil || fromVersion >= completion.Version || fromVersion > toVersion {
		return false
	}
	if toVersion < completion.Version {
		// The completed refresh also authenticates its historical reviewed
		// prefix. Only a target on the reservation's exact signed suffix may
		// use this proof; arbitrary earlier lifecycle endpoints are not admitted.
		if toVersion < value.TicketVersion {
			return false
		}
		prefix := validateRunnerRecoveryLedgerPrefix(ctx, q, ref, fromVersion, fromRunner, fromLeader, value.TicketVersion, value.Fence.RunnerEpoch, value.Fence.LeaderEpoch) == nil
		if !prefix {
			prefix = protectedBaseRefreshReviewedPrefix(ctx, q, value, fromVersion, fromRunner, fromLeader) == nil
		}
		if !prefix {
			prefix = protectedBaseRefreshPostCISourceToReservation(ctx, q, value, normalRecoveryEndpoint{version: fromVersion, runner: fromRunner, leader: fromLeader}) == nil
		}
		return prefix &&
			validateRunnerRecoveryLedgerPrefix(ctx, q, ref, value.TicketVersion, value.Fence.RunnerEpoch, value.Fence.LeaderEpoch, toVersion, toRunner, toLeader) == nil &&
			validateRunnerRecoveryLedgerPrefix(ctx, q, ref, toVersion, toRunner, toLeader, completion.Version-1, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) == nil
	}
	prefix := validateRunnerRecoveryLedgerPrefix(ctx, q, ref, fromVersion, fromRunner, fromLeader, completion.Version-1, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) == nil
	if !prefix {
		prefix = protectedBaseRefreshReviewedPrefix(ctx, q, value, fromVersion, fromRunner, fromLeader) == nil
	}
	if !prefix {
		prefix = protectedBaseRefreshPostCISourceToReservation(ctx, q, value, normalRecoveryEndpoint{version: fromVersion, runner: fromRunner, leader: fromLeader}) == nil &&
			validateRunnerRecoveryLedgerPrefix(ctx, q, ref, value.TicketVersion, value.Fence.RunnerEpoch, value.Fence.LeaderEpoch, completion.Version-1, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) == nil
	}
	return prefix && validateRunnerRecoveryLedgerPrefix(ctx, q, ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, toVersion, toRunner, toLeader) == nil
}

// A restart in waiting_ci may precede green CI and final review, followed by
// a protected-base refresh. Generic phase/control gaps intentionally do not
// accept checks_green/review_pass. Authenticate their exact historical
// publication, CI policy/observations/events and reviewer result instead.
func protectedBaseRefreshReviewedPrefix(ctx context.Context, q candidateEvidenceQuerier, value protectedBaseRefreshIntent, version, runner, leader uint64) error {
	ref := value.Ref
	if version == 0 || version >= value.TicketVersion || value.TicketVersion-version > 64 {
		return ErrPublicationEvidence
	}
	publication, initial, err := protectedBaseRefreshGreenEndpoint(ctx, q, value)
	if err != nil || version >= initial.version {
		return ErrPublicationEvidence
	}
	// The CI validator authenticated every endpoint through green. Require
	// this gap's source to be the exact recovery row within that chain.
	step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version)
	if err != nil || !found || !validRunnerRecovery(step) || step.RunnerEpoch != runner || step.LeaderEpoch != leader || version < publication.CurrentTicketVersion {
		return ErrPublicationEvidence
	}
	if version == publication.CurrentTicketVersion && publication.CurrentFence != (domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner}) {
		return ErrPublicationEvidence
	}
	pass, err := protectedBaseRefreshPassEndpoint(ctx, q, value, initial)
	if err != nil {
		return err
	}
	return validatePostPublicationEndpointAdvance(ctx, q, ref, domain.StateWaitingApproval, pass,
		normalRecoveryEndpoint{version: value.TicketVersion, runner: value.Fence.RunnerEpoch, leader: value.Fence.LeaderEpoch})
}

// These endpoints belong to the reservation's candidate and stop at its
// immutable version. Later publications and review results cannot replace
// the historical CI or review evidence consumed by a completed refresh.
func protectedBaseRefreshGreenEndpoint(ctx context.Context, q candidateEvidenceQuerier, value protectedBaseRefreshIntent) (PublishedCandidateEvidence, normalRecoveryEndpoint, error) {
	ref, candidate := value.Ref, value.Candidate
	var witness string
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(witness_digest),'') FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=?`, ref.Channel, ref.Project, ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, candidate.Snapshot.TreeSHA).Scan(&count, &witness); err != nil || count != 1 {
		return PublishedCandidateEvidence{}, normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	publication, found, err := loadPublicationEvidenceRowMatching(ctx, q, ref, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, candidate.Snapshot.TreeSHA, witness)
	if err != nil || !found || !publicationCandidateEqual(publication.Candidate, candidate) || loadLatestPublicationRebind(ctx, q, &publication) != nil {
		return PublishedCandidateEvidence{}, normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	policy, err := scanCurrentCIPolicy(ctx, q, ref, publication)
	if err != nil {
		return PublishedCandidateEvidence{}, normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	green, reviewVersion, err := finalReviewCIPendingChainThrough(ctx, q, ref, publication, policy, value.TicketVersion)
	if err != nil || reviewVersion == 0 || green.ObservedFence.RunnerEpoch == 0 || green.ObservedFence.LeaderEpoch == 0 {
		return PublishedCandidateEvidence{}, normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	return publication, normalRecoveryEndpoint{version: reviewVersion, runner: green.ObservedFence.RunnerEpoch, leader: green.ObservedFence.LeaderEpoch}, nil
}

func protectedBaseRefreshPassEndpoint(ctx context.Context, q candidateEvidenceQuerier, value protectedBaseRefreshIntent, initial normalRecoveryEndpoint) (normalRecoveryEndpoint, error) {
	ref, candidate := value.Ref, value.Candidate
	var passVersion uint64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(ticket_version),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass' AND from_state='reviewing' AND to_state='waiting_approval' AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, value.TicketVersion).Scan(&passVersion); err != nil || initial.version == 0 || passVersion <= initial.version {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if err := exactStateChangeEvent(ctx, q, ref, passVersion, "review_pass", domain.StateReviewing, domain.StateWaitingApproval); err != nil {
		return normalRecoveryEndpoint{}, err
	}
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='review_pass' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket, passVersion).Scan(&count); err != nil || count != 1 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	var id int64
	var attempt int
	var resultVersion, resultRunner, resultLeader uint64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(expected_ticket_version),0) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review' AND role='reviewer' AND state='completed' AND outcome='completed' AND expected_ticket_version<?`, ref.Channel, ref.Project, ref.Ticket, passVersion).Scan(&resultVersion); err != nil || resultVersion < initial.version {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(id),0) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review' AND role='reviewer' AND state='completed' AND outcome='completed' AND expected_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, resultVersion).Scan(&count, &id); err != nil || count != 1 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT attempt,runner_epoch,leader_epoch FROM provider_attempts WHERE id=?`, id).Scan(&attempt, &resultRunner, &resultLeader); err != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	result, parsed, err := (&Store{}).loadHistoricalProviderAttemptResult(ctx, q, ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseReview, AttemptID: id, Attempt: attempt})
	if err != nil || parsed.Reviewer == nil || parsed.Reviewer.Decision != phaseartifact.ReviewPass || parsed.Reviewer.ReviewedHead != candidate.Snapshot.HeadSHA || parsed.Reviewer.ProofDigest != candidate.Snapshot.ProofDigest || result.Claim.ExpectedVersion != resultVersion || result.Claim.RunnerEpoch != resultRunner || result.Claim.LeaderEpoch != resultLeader {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	claimed := normalRecoveryEndpoint{version: resultVersion, runner: resultRunner, leader: resultLeader}
	// Derive the pass fence from the selected result or an exact recovery
	// endpoint before the pass, never from the later reservation's fence.
	if resultVersion < passVersion-1 {
		step, found, err := loadRunnerRecoveryAt(ctx, q, ref, passVersion-1)
		if err != nil || !found || !validRunnerRecovery(step) {
			return normalRecoveryEndpoint{}, ErrPublicationEvidence
		}
		resultRunner, resultLeader = step.RunnerEpoch, step.LeaderEpoch
	}
	if validatePostPublicationEndpointAdvance(ctx, q, ref, domain.StateReviewing, initial, claimed) != nil || validateRunnerRecoveryLedgerPrefix(ctx, q, ref, claimed.version, claimed.runner, claimed.leader, passVersion-1, resultRunner, resultLeader) != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	return normalRecoveryEndpoint{version: passVersion, runner: resultRunner, leader: resultLeader}, nil
}

func protectedBaseRefreshRecoveryTarget(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version, runner, leader uint64) bool {
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
	if err != nil {
		return false
	}
	if version < completion.Version {
		// A first recovery can follow pending CI polls. Those same-state
		// transitions are not generic initial lifecycle events. Anchor the
		// original publication endpoint, then authenticate the complete CI
		// and review history containing this exact first recovery row.
		step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version+1)
		if err != nil || !found || !validRunnerRecovery(step) || step.PriorTicketVersion != version || step.PriorRunnerEpoch != runner || step.PriorLeaderEpoch != leader {
			return false
		}
		if protectedBaseRefreshPostCITarget(ctx, q, value, completion, version, runner, leader) {
			return true
		}
		var publicationVersion uint64
		var count int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(ticket_version),0) FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND candidate_generation=? AND candidate_head_sha=? AND candidate_tree_sha=?`, ref.Channel, ref.Project, ref.Ticket, value.Candidate.Snapshot.Generation, value.Candidate.Snapshot.HeadSHA, value.Candidate.Snapshot.TreeSHA).Scan(&count, &publicationVersion); err != nil || count != 1 || publicationVersion == 0 || publicationVersion >= version {
			return false
		}
		if validateInitialLifecycleAdvance(ctx, q, ref, publicationVersion+1) != nil {
			// The completed refresh authenticates its exact immutable candidate
			// and publication. A repaired candidate has a checks_red source, not
			// an ordinary initial lifecycle; prove that source independently.
			candidate := value.Candidate
			if ok, err := validateCandidateRepairRecoveryTarget(ctx, q, ref, candidate.TicketVersion, candidate.Fence.RunnerEpoch, candidate.Fence.LeaderEpoch); err != nil || !ok {
				return false
			}
		}
		return protectedBaseRefreshReviewedPrefix(ctx, q, value, step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch) == nil
	}
	if validateInitialLifecycleAdvance(ctx, q, ref, value.TicketVersion) != nil {
		return false
	}
	return validateRunnerRecoveryLedgerPrefix(ctx, q, ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, version, runner, leader) == nil
}

// A first restart may already be at green CI or final review. Authenticate
// that exact endpoint against the completed refresh's retained candidate;
// the generic initial lifecycle cannot represent these typed CI transitions.
func protectedBaseRefreshPostCITarget(ctx context.Context, q candidateEvidenceQuerier, value protectedBaseRefreshIntent, completion ProtectedBaseRefreshCompletion, version, runner, leader uint64) bool {
	candidate := value.Candidate
	if validateInitialLifecycleAdvance(ctx, q, value.Ref, candidate.TicketVersion) != nil {
		if ok, err := validateCandidateRepairRecoveryTarget(ctx, q, value.Ref, candidate.TicketVersion, candidate.Fence.RunnerEpoch, candidate.Fence.LeaderEpoch); err != nil || !ok {
			return false
		}
	}
	if version >= completion.Version {
		return false
	}
	target := normalRecoveryEndpoint{version: version, runner: runner, leader: leader}
	reserved := normalRecoveryEndpoint{version: value.TicketVersion, runner: value.Fence.RunnerEpoch, leader: value.Fence.LeaderEpoch}
	if version <= value.TicketVersion {
		return protectedBaseRefreshPostCISourceToReservation(ctx, q, value, target) == nil
	}
	return protectedBaseRefreshPostCISourceToReservation(ctx, q, value, reserved) == nil &&
		validateRunnerRecoveryLedgerPrefix(ctx, q, value.Ref, reserved.version, reserved.runner, reserved.leader, version, runner, leader) == nil &&
		validateRunnerRecoveryLedgerPrefix(ctx, q, value.Ref, version, runner, leader, completion.Version-1, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) == nil
}

// A recovery source can precede the final review pass while the reservation
// follows it. Cross only that authenticated pass; the segments on either side
// must independently prove their exact same-state endpoints.
func protectedBaseRefreshPostCISourceToReservation(ctx context.Context, q candidateEvidenceQuerier, value protectedBaseRefreshIntent, source normalRecoveryEndpoint) error {
	reserved := normalRecoveryEndpoint{version: value.TicketVersion, runner: value.Fence.RunnerEpoch, leader: value.Fence.LeaderEpoch}
	_, green, err := protectedBaseRefreshGreenEndpoint(ctx, q, value)
	if err != nil || source.version < green.version || source.version > reserved.version || reserved.version-green.version > 64 {
		return ErrPublicationEvidence
	}
	if validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateReviewing, green, source) == nil &&
		validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateReviewing, source, reserved) == nil {
		return nil
	}
	pass, err := protectedBaseRefreshPassEndpoint(ctx, q, value, green)
	if err != nil {
		return ErrPublicationEvidence
	}
	if source.version < pass.version {
		beforePass := normalRecoveryEndpoint{version: pass.version - 1, runner: pass.runner, leader: pass.leader}
		if validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateReviewing, green, source) != nil ||
			validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateReviewing, source, beforePass) != nil {
			return ErrPublicationEvidence
		}
		return validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateWaitingApproval, pass, reserved)
	}
	if validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateWaitingApproval, pass, source) != nil {
		return ErrPublicationEvidence
	}
	return validatePostPublicationEndpointAdvance(ctx, q, value.Ref, domain.StateWaitingApproval, source, reserved)
}

func (s *Store) protectedBaseRefreshRecoveryPredecessor(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64, latest RunnerRecoveryLedger, latestFound bool) (uint64, bool, error) {
	if state != domain.StateBuilding {
		return 0, false, nil
	}
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, conn, ref)
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, true, err
	}
	entry, err := loadProviderPhaseEntryAt(ctx, conn, ref, domain.PhaseBuild, version)
	if err != nil {
		return 0, true, ErrPublicationEvidence
	}
	if entry.Version > completion.Version {
		return 0, false, nil
	}
	if entry.Version != completion.Version || entry.Trigger != "base_or_candidate_head_changed" || entry.Leader != completion.Fence.LeaderEpoch || entry.Runner != completion.Fence.RunnerEpoch {
		return 0, true, ErrPublicationEvidence
	}
	leader := completion.Fence.LeaderEpoch
	if latestFound && latest.TicketVersion == version && latest.RunnerEpoch == runner {
		leader = latest.LeaderEpoch
	} else if control, found, err := loadRuntimeControlEndpointLeader(ctx, conn, ref, version, runner); err != nil {
		return 0, true, err
	} else if found {
		leader = control
	}
	if leader == 0 || leader >= newLeader || validateRunnerRecoveryLedger(ctx, conn, ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, version, runner, leader) != nil {
		return 0, true, ErrPublicationEvidence
	}
	var path, identity, base, head string
	if err := conn.QueryRowContext(ctx, `SELECT path,identity_json,base_sha,head_sha FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=? AND state='registered'`, ref.Channel, ref.Project, ref.Ticket).Scan(&path, &identity, &base, &head); err != nil || path != value.Worktree.Path || identity != string(completion.Worktree.IdentityJSON) || base != completion.Worktree.BaseSHA || head != completion.Worktree.HeadSHA {
		return 0, true, ErrPublicationEvidence
	}
	return leader, true, nil
}

func (s *Store) pendingProtectedBaseRefreshAt(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence) (ProtectedBaseRefresh, bool, error) {
	var key string
	err := conn.QueryRowContext(ctx, `SELECT refresh_effect_semantic_key FROM protected_base_refresh_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return ProtectedBaseRefresh{}, false, nil
	}
	if err != nil {
		return ProtectedBaseRefresh{}, false, err
	}
	id, value, mutation, err := loadProtectedBaseRefreshReservationAt(ctx, conn, key)
	if err != nil {
		return ProtectedBaseRefresh{}, false, err
	}
	if _, completed, err := loadProtectedBaseRefreshCompletionAt(ctx, conn, key); err != nil {
		return ProtectedBaseRefresh{}, false, err
	} else if completed {
		return ProtectedBaseRefresh{}, false, nil
	}
	mutation.TicketVersion, mutation.Fence = version, fence
	if err := s.authenticateProtectedBaseRefreshMutationAt(ctx, conn, mutation); err != nil {
		return ProtectedBaseRefresh{}, false, err
	}
	payload, digest, err := canonicalProtectedBaseRefreshIntent(value)
	if err != nil {
		return ProtectedBaseRefresh{}, false, err
	}
	return ProtectedBaseRefresh{ID: id, IntentDigest: digest, IntentPayload: payload, Mutation: mutation, Candidate: value.Candidate, Worktree: value.Worktree, NewBaseSHA: value.NewBaseSHA}, true, nil
}

// PendingProtectedBaseRefresh is a current-fence recovery reader. The old
// payload stays immutable; only an authenticated signed recovery chain can
// project its mutation descriptor to the caller's current endpoint.
func (s *Store) PendingProtectedBaseRefresh(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (ProtectedBaseRefresh, bool, error) {
	var value ProtectedBaseRefresh
	var found bool
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		var err error
		value, found, err = s.pendingProtectedBaseRefreshAt(ctx, conn, ref, version, fence)
		return err
	})
	return value, found, err
}

// ReclaimProtectedBaseRefresh is not generic mutation replay. Git's refresh
// adapter must recognize exactly old/old, staged/old, or staged/new paired-ref
// states, all bound to this one reservation. It never repeats a remote push.
// A surviving/quarantined writer blocks replacement; eight durable reclaim
// attempts bound recovery, while an exact current executing claim just replays.
func (s *Store) ReclaimProtectedBaseRefresh(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (contracts.GitMutationClaim, error) {
	var claim contracts.GitMutationClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		reservation, found, err := s.pendingProtectedBaseRefreshAt(ctx, conn, ref, version, fence)
		if err != nil || !found {
			return ErrGitMutationIntent
		}
		intent := reservation.Mutation
		facts, err := gitMutationIntentFactsFrom(ctx, conn, intent.SemanticKey)
		if err != nil || facts.Effect.ClaimEpoch == ^uint64(0) || facts.Claim.ClaimEpoch == 0 || facts.Effect.Ref != ref || facts.Effect.RequestDigest != intent.RequestDigest || facts.Effect.Kind != "git/refresh-base" {
			return ErrGitMutationIntent
		}
		if facts.Effect.State == EffectExecuting && facts.Claim.TicketVersion == version && facts.Claim.LeaderEpoch == fence.LeaderEpoch && facts.Claim.RunnerEpoch == fence.RunnerEpoch && facts.Effect.TicketVersion == version && facts.Effect.LeaderEpoch == fence.LeaderEpoch && facts.Effect.RunnerEpoch == fence.RunnerEpoch && facts.Effect.ClaimEpoch == facts.Claim.ClaimEpoch {
			claim = facts.Claim
			return nil
		}
		if facts.Effect.State != EffectExecuting && facts.Effect.State != EffectUncertain && facts.Effect.State != EffectConfirmed {
			return ErrGitMutationIntent
		}
		if facts.Effect.ObservedIdentity != "" && facts.Effect.ObservedIdentity != facts.PreparedCommitOID {
			return ErrGitMutationIntent
		}
		if facts.Effect.ClaimEpoch < facts.Claim.ClaimEpoch || facts.Claim.TicketVersion > version || facts.Claim.LeaderEpoch > fence.LeaderEpoch || facts.Claim.RunnerEpoch > fence.RunnerEpoch {
			return ErrStaleFence
		}
		if err := validateRunnerRecoveryLedger(ctx, conn, ref, facts.Claim.TicketVersion, facts.Claim.RunnerEpoch, facts.Claim.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
			return err
		}
		if err := repositoryHasGitWriter(ctx, conn, intent.Repository); err != nil {
			return err
		}
		if err := repositoryHasProviderWriter(ctx, conn, intent.Repository); err != nil {
			return err
		}
		if err := repositoryHasCommandWriter(ctx, conn, intent.Repository); err != nil {
			return err
		}
		var count int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND semantic_key<>? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket, intent.SemanticKey).Scan(&count); err != nil || count != 0 {
			return ErrControlNotDrained
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='protected_base_refresh_reclaimed'`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count >= maxProtectedBaseRefreshReclaims {
			return ErrEvidenceConflict
		}
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state); err != nil {
			return err
		}
		next := facts.Effect.ClaimEpoch + 1
		claim = facts.Claim
		claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch = version, fence.LeaderEpoch, fence.RunnerEpoch, next
		payload, err := json.Marshal(struct {
			IntentDigest string
			Prior        contracts.GitMutationClaim
			PriorEffect  Effect
			Current      contracts.GitMutationClaim
		}{intent.RequestDigest, facts.Claim, facts.Effect, claim})
		if err != nil || len(payload) > maxBaseRefreshPayload {
			return ErrEvidenceConflict
		}
		changed, err := conn.ExecContext(ctx, `UPDATE effects SET state='executing',ticket_version=?,leader_epoch=?,runner_epoch=?,claim_epoch=?,observed_identity='' WHERE semantic_key=? AND state=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, version, fence.LeaderEpoch, fence.RunnerEpoch, next, intent.SemanticKey, facts.Effect.State, facts.Effect.TicketVersion, facts.Effect.LeaderEpoch, facts.Effect.RunnerEpoch, facts.Effect.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		changed, err = conn.ExecContext(ctx, `UPDATE git_mutation_intents SET ticket_version=?,leader_epoch=?,runner_epoch=?,claim_epoch=? WHERE semantic_key=? AND operation='refresh-base' AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, version, fence.LeaderEpoch, fence.RunnerEpoch, next, intent.SemanticKey, intent.RequestDigest, facts.Claim.TicketVersion, facts.Claim.LeaderEpoch, facts.Claim.RunnerEpoch, facts.Claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrGitMutationIntent
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,'protected_base_refresh_reclaimed',?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, version, state, state, string(payload), time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
	return claim, err
}
