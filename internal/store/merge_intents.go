package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// RecordMergeIntent persists the evidence needed to reconcile a merge after a
// crash. It deliberately validates the currently executing durable effect;
// callers cannot store a free-floating request hash as merge authority.
func (s *Store) RecordMergeIntent(ctx context.Context, intent domain.MergeIntent) error {
	if err := validMergeIntent(intent); err != nil {
		return err
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, intent.Ref, intent.TicketVersion, domain.Fence{LeaderEpoch: intent.LeaderEpoch, RunnerEpoch: intent.RunnerEpoch, ClaimEpoch: intent.ClaimEpoch}); err != nil {
			return err
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return err
		}
		if effect.Ref != intent.Ref || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest || effect.State != EffectExecuting || effect.TicketVersion != intent.TicketVersion || effect.LeaderEpoch != intent.LeaderEpoch || effect.RunnerEpoch != intent.RunnerEpoch || effect.ClaimEpoch != intent.ClaimEpoch {
			return ErrStaleFence
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO merge_intents(semantic_key, channel, project_id, ticket_id, request_digest, ticket_version, leader_epoch, runner_epoch, claim_epoch, repository_host, repository_owner, repository_name, pull_request_number, head_owner, head_repository, head_ref, head_oid, base_ref, original_base_oid, protection_rule_id, protection_kind, protection_checks_digest, strict_status_checks, admin_enforced, active_ruleset_count, method, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(semantic_key) DO NOTHING`,
			intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, intent.TicketVersion, intent.LeaderEpoch, intent.RunnerEpoch, intent.ClaimEpoch, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.PullRequestNumber, intent.HeadOwner, intent.HeadRepository, intent.HeadRef, intent.HeadOID, intent.BaseRef, intent.OriginalBaseOID, intent.ProtectionRuleID, intent.ProtectionKind, intent.ProtectionChecksDigest, boolInt(intent.StrictStatusChecks), boolInt(intent.AdminEnforced), intent.ActiveRulesetCount, intent.Method, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if inserted, _ := result.RowsAffected(); inserted == 1 {
			return nil
		}
		existing, found, err := mergeIntentFrom(ctx, conn, intent.SemanticKey)
		if err != nil || !found {
			return err
		}
		if existing != intent {
			return ErrEffectKey
		}
		return nil
	})
}

func (s *Store) MergeIntent(ctx context.Context, semanticKey string) (domain.MergeIntent, bool, error) {
	return mergeIntentFrom(ctx, s.db, semanticKey)
}

// MergeReconciliationReady proves that a merging ticket can make progress
// without opening its local checkout.  This is intentionally narrower than a
// generic "merge exists" lookup: the merge intent and both settled effects
// must be the unique, current, guarded operation for the ticket, and no other
// effect may still require external work.  Callers may use the result only to
// skip worktree preparation; the worker repeats its normal evidence checks.
func (s *Store) MergeReconciliationReady(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return false, nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()

	var state string
	var mergeMode string
	var currentVersion, currentRunner, currentLeader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,t.merge_mode,t.version,t.runner_epoch,d.leader_epoch
		FROM tickets t JOIN daemon_instances d ON d.channel=t.channel
		WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).
		Scan(&state, &mergeMode, &currentVersion, &currentRunner, &currentLeader); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, normalizeBusy(ctx, err)
	}
	if domain.State(state) != domain.StateMerging || domain.MergeMode(mergeMode) != domain.MergeGuarded || currentVersion != version || currentRunner != fence.RunnerEpoch || currentLeader != fence.LeaderEpoch {
		return false, nil
	}

	intent, found, err := singleRecoveryMergeIntent(ctx, conn, ref)
	if err != nil || !found || intent.TicketVersion != version || intent.LeaderEpoch != fence.LeaderEpoch || intent.RunnerEpoch != fence.RunnerEpoch || validMergeIntentForProof(intent) != nil {
		return false, nil
	}
	// Reuse the same strict publication/approval/recovery authority as merge
	// replay. A synthetic intent or a PR/head/base drift must never turn this
	// optimization into an uncredentialed read-only path.
	endpoint, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, ref)
	if err != nil || endpoint.version != version || endpoint.runner != fence.RunnerEpoch || endpoint.leader != fence.LeaderEpoch {
		return false, nil
	}
	observation, observed, err := guardedMergeObservationFrom(ctx, conn, intent.SemanticKey)
	if err != nil || !observed || !guardedMergeObservationMatchesIntent(observation, intent) {
		return false, nil
	}
	if guardedMergeProtectedRefFetchMatches(ctx, conn, intent, observation.MergeOID, endpoint) != nil {
		return false, nil
	}
	effect, err := effectFrom(ctx, conn, intent.SemanticKey)
	if err != nil || effect.Ref != ref || effect.SemanticKey != intent.SemanticKey || effect.Kind != "merge" || effect.State != EffectConfirmed || effect.TicketVersion != version || effect.LeaderEpoch != fence.LeaderEpoch || effect.RunnerEpoch != fence.RunnerEpoch || effect.ClaimEpoch == 0 || effect.ClaimEpoch != intent.ClaimEpoch || effect.RequestDigest != intent.RequestDigest || !guardedMergeParentEffectIdentityMatches(observation, intent, effect.ObservedIdentity) {
		return false, nil
	}

	// A confirmed merge is only a read-only continuation when the exact ready
	// effect for the same source head is settled as well.  Derive its key from
	// the authenticated intent rather than accepting an arbitrary pr_ready row.
	readyKey := "merge-ready/" + string(ref.Channel) + "/" + string(ref.Project) + "/" + string(ref.Ticket) + "/" + intent.HeadOID
	ready, err := effectFrom(ctx, conn, readyKey)
	if err != nil || ready.Ref != ref || ready.SemanticKey != readyKey || ready.Kind != "pr_ready" || ready.State != EffectConfirmed || ready.TicketVersion != version || ready.LeaderEpoch != fence.LeaderEpoch || ready.RunnerEpoch != fence.RunnerEpoch || ready.ClaimEpoch == 0 || ready.RequestDigest != canonicalReadyRequestDigest(intent) || ready.ObservedIdentity != "ready/"+intent.HeadOID {
		return false, nil
	}

	var unresolved int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket).Scan(&unresolved); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	return unresolved == 0, nil
}

// MergeIntentForProof returns only the currently live durable merge intent
// matching a sealed exact GitHub post-merge observation. A GitHub adapter
// receives no Git authority; this lookup lets the root composition mint a
// separate fenced Git proof effect only after the source PR identity and its
// observed post-merge commit have been bound together durably.
func (s *Store) MergeIntentForProof(ctx context.Context, repositoryHost, owner, name, baseRef, originalBaseOID, mergeOID string) (domain.MergeIntent, error) {
	if repositoryHost != "github.com" || owner == "" || name == "" || baseRef == "" || !validOID(originalBaseOID) || !validOID(mergeOID) || len(originalBaseOID) != len(mergeOID) {
		return domain.MergeIntent{}, ErrEvidenceConflict
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return domain.MergeIntent{}, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	rows, err := conn.QueryContext(ctx, `SELECT m.semantic_key,t.state FROM guarded_merge_observations o JOIN merge_intents m ON m.semantic_key=o.semantic_key JOIN effects e ON e.semantic_key=m.semantic_key JOIN tickets t ON t.channel=m.channel AND t.project_id=m.project_id AND t.id=m.ticket_id
		WHERE o.repository_host=? AND o.repository_owner=? AND o.repository_name=? AND o.base_ref=? AND o.original_base_oid=? AND o.merge_oid=?
		AND e.effect_kind='merge' AND ((t.state='merging' AND e.state IN ('executing','uncertain','confirmed')) OR (t.state='reconciling' AND e.state='confirmed')) ORDER BY m.created_at DESC`, repositoryHost, owner, name, baseRef, originalBaseOID, mergeOID)
	if err != nil {
		return domain.MergeIntent{}, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var key string
	var state domain.State
	count := 0
	for rows.Next() {
		if err := rows.Scan(&key, &state); err != nil {
			return domain.MergeIntent{}, err
		}
		count++
		if count > 1 {
			return domain.MergeIntent{}, ErrEvidenceConflict
		}
	}
	if err := rows.Err(); err != nil {
		return domain.MergeIntent{}, err
	}
	if err := rows.Close(); err != nil {
		return domain.MergeIntent{}, err
	}
	if count != 1 {
		// Proof lookup is an authority boundary, not a convenience lookup. An
		// absent (or ambiguous) post-merge observation must fail as conflicting
		// evidence rather than invite a caller to infer a descendant witness.
		return domain.MergeIntent{}, fmt.Errorf("%w: merge proof observation cardinality", ErrEvidenceConflict)
	}
	intent, found, err := mergeIntentFrom(ctx, conn, key)
	observation, observed, observationErr := guardedMergeObservationFrom(ctx, conn, key)
	if err != nil || !found || observationErr != nil || !observed || intent.OriginalBaseOID != originalBaseOID || intent.BaseRef != baseRef || observation.MergeOID != mergeOID || !guardedMergeObservationMatchesIntent(observation, intent) || validMergeIntentForProof(intent) != nil {
		return domain.MergeIntent{}, fmt.Errorf("%w: merge proof intent or observation mismatch", ErrEvidenceConflict)
	}
	if state == domain.StateMerging {
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil || effect.Ref != intent.Ref || effect.SemanticKey != intent.SemanticKey || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest {
			return domain.MergeIntent{}, ErrEvidenceConflict
		}
		exactLaunch := effect.TicketVersion == intent.TicketVersion && effect.LeaderEpoch == intent.LeaderEpoch && effect.RunnerEpoch == intent.RunnerEpoch && effect.ClaimEpoch == intent.ClaimEpoch
		if !exactLaunch && effect.State != EffectConfirmed {
			// GitHub can apply the merge after the command handoff but before its
			// exact merged-PR observation is recorded. A restart makes the parent
			// effect uncertain under a new leader and fences the ticket. The
			// observation writer has already authenticated that narrow recovery
			// shape; the immediately following protected-ref proof must accept the
			// same historical intent, never a generic promoted effect.
			if effect.State != EffectUncertain {
				return domain.MergeIntent{}, ErrEvidenceConflict
			}
			var current normalRecoveryEndpoint
			var liveState domain.State
			if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&liveState, &current.version, &current.runner, &current.leader); err != nil || liveState != domain.StateMerging {
				return domain.MergeIntent{}, ErrEvidenceConflict
			}
			approval, err := s.approvalRecoveryEndpoint(ctx, conn, intent.Ref)
			if err != nil || s.authenticateMergingRecoveryEffect(ctx, conn, intent.Ref, approval, current, current.leader, intent) != nil {
				return domain.MergeIntent{}, ErrEvidenceConflict
			}
		}
		if effect.State == EffectConfirmed {
			confirmed, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, intent.Ref)
			if err != nil || confirmed.version != effect.TicketVersion || confirmed.runner != effect.RunnerEpoch || confirmed.leader != effect.LeaderEpoch {
				return domain.MergeIntent{}, fmt.Errorf("%w: confirmed merge endpoint mismatch", ErrEvidenceConflict)
			}
			var live normalRecoveryEndpoint
			var liveState domain.State
			if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&liveState, &live.version, &live.runner, &live.leader); err != nil || liveState != domain.StateMerging {
				return domain.MergeIntent{}, fmt.Errorf("%w: live merge endpoint unavailable", ErrEvidenceConflict)
			}
			var controls int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&controls); err != nil || controls > 1 {
				return domain.MergeIntent{}, fmt.Errorf("%w: runtime control cardinality", ErrEvidenceConflict)
			}
			if controls == 1 {
				control, err := runtimeControlFrom(ctx, conn, intent.Ref)
				if err != nil || control.state != "open" || control.authority.version != live.version || control.authority.runner != live.runner || control.authority.leader != live.leader {
					return domain.MergeIntent{}, fmt.Errorf("%w: runtime control does not authorize live merge endpoint", ErrEvidenceConflict)
				}
			} else if live != confirmed {
				return domain.MergeIntent{}, fmt.Errorf("%w: live merge endpoint has no runtime control", ErrEvidenceConflict)
			}
			if authenticateCurrentPostPublicationEndpointBridge(ctx, conn, intent.Ref, domain.StateMerging, confirmed, live) != nil {
				return domain.MergeIntent{}, fmt.Errorf("%w: merge endpoint control lineage", ErrEvidenceConflict)
			}
		}
	}
	if state == domain.StateReconciling {
		// Reconciling is recovery-only: the parent merge and child protected-ref
		// proof have already completed. Authenticate the immutable confirmed
		// merge endpoint, its one state transition, and every subsequent signed
		// runner recovery before exposing the intent again.
		confirmed, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, intent.Ref)
		if err != nil || confirmed.version == ^uint64(0) {
			return domain.MergeIntent{}, ErrEvidenceConflict
		}
		reconciling := normalRecoveryEndpoint{version: confirmed.version + 1, runner: confirmed.runner, leader: confirmed.leader}
		if err := canonicalGuardedMergeObservation(ctx, conn, intent.Ref, reconciling.version); err != nil {
			return domain.MergeIntent{}, ErrEvidenceConflict
		}
		var version, runner, leader uint64
		var liveState domain.State
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&liveState, &version, &runner, &leader); err != nil || liveState != domain.StateReconciling {
			return domain.MergeIntent{}, ErrEvidenceConflict
		}
		current := normalRecoveryEndpoint{version: version, runner: runner, leader: leader}
		if current != reconciling {
			currentLeader, normalErr := normalRecoveryLeaderAt(ctx, conn, intent.Ref, reconciling, version, runner)
			if normalErr != nil || currentLeader != leader {
				control, controlErr := runtimeControlFrom(ctx, conn, intent.Ref)
				if controlErr != nil || control.state != "open" || control.authority.version != current.version || control.authority.runner != current.runner || control.authority.leader != current.leader {
					return domain.MergeIntent{}, ErrEvidenceConflict
				}
			}
		}
		if authenticateCurrentPostPublicationEndpointBridge(ctx, conn, intent.Ref, domain.StateReconciling, reconciling, current) != nil {
			return domain.MergeIntent{}, ErrEvidenceConflict
		}
	}
	return intent, nil
}

type mergeIntentQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func mergeIntentFrom(ctx context.Context, q mergeIntentQuerier, semanticKey string) (domain.MergeIntent, bool, error) {
	var intent domain.MergeIntent
	var strict, admin int
	err := q.QueryRowContext(ctx, `SELECT channel, project_id, ticket_id, request_digest, ticket_version, leader_epoch, runner_epoch, claim_epoch, repository_host, repository_owner, repository_name, pull_request_number, head_owner, head_repository, head_ref, head_oid, base_ref, original_base_oid, protection_rule_id, protection_kind, protection_checks_digest, strict_status_checks, admin_enforced, active_ruleset_count, method FROM merge_intents WHERE semantic_key=?`, semanticKey).Scan(&intent.Ref.Channel, &intent.Ref.Project, &intent.Ref.Ticket, &intent.RequestDigest, &intent.TicketVersion, &intent.LeaderEpoch, &intent.RunnerEpoch, &intent.ClaimEpoch, &intent.RepositoryHost, &intent.RepositoryOwner, &intent.RepositoryName, &intent.PullRequestNumber, &intent.HeadOwner, &intent.HeadRepository, &intent.HeadRef, &intent.HeadOID, &intent.BaseRef, &intent.OriginalBaseOID, &intent.ProtectionRuleID, &intent.ProtectionKind, &intent.ProtectionChecksDigest, &strict, &admin, &intent.ActiveRulesetCount, &intent.Method)
	if err == sql.ErrNoRows {
		return domain.MergeIntent{}, false, nil
	}
	intent.SemanticKey, intent.StrictStatusChecks, intent.AdminEnforced = semanticKey, strict == 1, admin == 1
	return intent, err == nil, normalizeBusy(ctx, err)
}

// QuarantineExternalMutations is safe to call from any command path. In
// particular, an observation can discover an escaped writer before a durable
// effect claim exists; later launches still fail after restart.
func (s *Store) QuarantineExternalMutations(ctx context.Context) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO external_mutation_quarantine(singleton, reason, observed_at) VALUES(1, 'cleanup_uncertain', ?) ON CONFLICT(singleton) DO NOTHING`, time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
}

func (s *Store) externalMutationsQuarantined(ctx context.Context) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT singleton FROM external_mutation_quarantine WHERE singleton=1`).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil && one == 1, normalizeBusy(ctx, err)
}

func (s *Store) ExternalMutationsQuarantined(ctx context.Context) (bool, error) {
	return s.externalMutationsQuarantined(ctx)
}

func validMergeIntent(intent domain.MergeIntent) error {
	if err := intent.Ref.Validate(); err != nil {
		return err
	}
	if intent.SemanticKey == "" || intent.RequestDigest == "" || intent.TicketVersion == 0 || intent.LeaderEpoch == 0 || intent.RunnerEpoch == 0 || intent.ClaimEpoch == 0 || intent.RepositoryHost != "github.com" || intent.RepositoryOwner == "" || intent.RepositoryName == "" || intent.PullRequestNumber <= 0 || intent.HeadOwner == "" || intent.HeadRepository == "" || intent.HeadRef == "" || intent.HeadOID == "" || intent.BaseRef == "" || intent.OriginalBaseOID == "" || intent.ProtectionRuleID == "" || !intent.StrictStatusChecks || !intent.AdminEnforced || (intent.Method != "merge" && intent.Method != "squash" && intent.Method != "rebase") {
		return fmt.Errorf("invalid merge intent")
	}
	return intent.ValidateProtectionWitness()
}

// validMergeIntentForProof is stricter than the write-time shape check. A
// merge proof may use only an intent whose request digest binds the exact PR
// source identity, source head, protected base, method, and all four exact
// base-authorization facts. Without this check a tampered or legacy arbitrary
// digest could authorize a protected-ref proof for a different source PR while
// the repository/base tuple still matched.
func validMergeIntentForProof(intent domain.MergeIntent) error {
	if err := validMergeIntent(intent); err != nil {
		return err
	}
	if !validOID(intent.HeadOID) || !validOID(intent.OriginalBaseOID) || intent.RequestDigest != canonicalMergeRequestDigest(intent) {
		return fmt.Errorf("merge intent request digest does not bind source identity")
	}
	return nil
}

func canonicalMergeRequestDigest(intent domain.MergeIntent) string {
	input := "merge\x00" + intent.RepositoryOwner + "/" + intent.RepositoryName + "\x00" + intent.HeadOwner + "\x00" + intent.HeadRepository + "\x00" + intent.HeadRef + "\x00" + intent.HeadOID + "\x00" + intent.BaseRef + "\x00" + intent.OriginalBaseOID
	for _, value := range []string{intent.HeadOID, intent.Method, intent.OriginalBaseOID, intent.OriginalBaseOID, intent.OriginalBaseOID, intent.OriginalBaseOID} {
		input += "\x00" + value
	}
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func canonicalReadyRequestDigest(intent domain.MergeIntent) string {
	input := "pr_ready\x00" + intent.RepositoryOwner + "/" + intent.RepositoryName + "\x00" + intent.HeadOwner + "\x00" + intent.HeadRepository + "\x00" + intent.HeadRef + "\x00" + intent.HeadOID + "\x00" + intent.BaseRef + "\x00" + intent.OriginalBaseOID
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}
