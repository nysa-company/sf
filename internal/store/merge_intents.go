package store

import (
	"context"
	"database/sql"
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
		result, err := conn.ExecContext(ctx, `INSERT INTO merge_intents(semantic_key, channel, project_id, ticket_id, request_digest, ticket_version, leader_epoch, runner_epoch, claim_epoch, repository_host, repository_owner, repository_name, pull_request_number, head_oid, base_ref, original_base_oid, protection_rule_id, strict_status_checks, admin_enforced, active_ruleset_count, method, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(semantic_key) DO NOTHING`,
			intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, intent.TicketVersion, intent.LeaderEpoch, intent.RunnerEpoch, intent.ClaimEpoch, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.PullRequestNumber, intent.HeadOID, intent.BaseRef, intent.OriginalBaseOID, intent.ProtectionRuleID, boolInt(intent.StrictStatusChecks), boolInt(intent.AdminEnforced), intent.ActiveRulesetCount, intent.Method, time.Now().UTC().Format(time.RFC3339Nano))
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

type mergeIntentQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func mergeIntentFrom(ctx context.Context, q mergeIntentQuerier, semanticKey string) (domain.MergeIntent, bool, error) {
	var intent domain.MergeIntent
	var strict, admin int
	err := q.QueryRowContext(ctx, `SELECT channel, project_id, ticket_id, request_digest, ticket_version, leader_epoch, runner_epoch, claim_epoch, repository_host, repository_owner, repository_name, pull_request_number, head_oid, base_ref, original_base_oid, protection_rule_id, strict_status_checks, admin_enforced, active_ruleset_count, method FROM merge_intents WHERE semantic_key=?`, semanticKey).Scan(&intent.Ref.Channel, &intent.Ref.Project, &intent.Ref.Ticket, &intent.RequestDigest, &intent.TicketVersion, &intent.LeaderEpoch, &intent.RunnerEpoch, &intent.ClaimEpoch, &intent.RepositoryHost, &intent.RepositoryOwner, &intent.RepositoryName, &intent.PullRequestNumber, &intent.HeadOID, &intent.BaseRef, &intent.OriginalBaseOID, &intent.ProtectionRuleID, &strict, &admin, &intent.ActiveRulesetCount, &intent.Method)
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
	if intent.SemanticKey == "" || intent.RequestDigest == "" || intent.TicketVersion == 0 || intent.LeaderEpoch == 0 || intent.RunnerEpoch == 0 || intent.ClaimEpoch == 0 || intent.RepositoryHost != "github.com" || intent.RepositoryOwner == "" || intent.RepositoryName == "" || intent.PullRequestNumber <= 0 || intent.HeadOID == "" || intent.BaseRef == "" || intent.OriginalBaseOID == "" || intent.ProtectionRuleID == "" || !intent.StrictStatusChecks || !intent.AdminEnforced || intent.ActiveRulesetCount != 0 || (intent.Method != "merge" && intent.Method != "squash" && intent.Method != "rebase") {
		return fmt.Errorf("invalid merge intent")
	}
	return nil
}
