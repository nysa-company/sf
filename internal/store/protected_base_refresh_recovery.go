package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

const maxProtectedBaseRefreshReclaims = 8

func protectedBaseRefreshRecoveryGap(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, fromVersion, fromRunner, fromLeader, toVersion, toRunner, toLeader uint64) bool {
	_, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
	if err != nil || fromVersion >= completion.Version || toVersion < completion.Version {
		return false
	}
	return validateRunnerRecoveryLedgerPrefix(ctx, q, ref, fromVersion, fromRunner, fromLeader, completion.Version-1, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch) == nil && validateRunnerRecoveryLedgerPrefix(ctx, q, ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, toVersion, toRunner, toLeader) == nil
}

func protectedBaseRefreshRecoveryTarget(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, version, runner, leader uint64) bool {
	value, completion, err := protectedBaseRefreshForTicketAt(ctx, q, ref)
	if err != nil || version < completion.Version || validateInitialLifecycleAdvance(ctx, q, ref, value.TicketVersion) != nil {
		return false
	}
	return validateRunnerRecoveryLedgerPrefix(ctx, q, ref, completion.Version, completion.Fence.RunnerEpoch, completion.Fence.LeaderEpoch, version, runner, leader) == nil
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
