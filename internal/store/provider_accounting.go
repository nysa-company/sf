package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// Estimated-policy tickets pin every role, including a Codex role paired with
// Claude, to their immutable project snapshot. This is checked in the same
// transaction as attempt admission, independently of workflow caller hints.
func validateEstimatedProviderRoute(ctx context.Context, q candidateEvidenceQuerier, r ProviderAttemptRequest) error {
	_, err := loadProviderAccountingPolicy(ctx, q, r.Ref)
	if errors.Is(err, ErrNotFound) && !contracts.UsesMultiCLIRequestLimit(r.Binding.Identity.Provider) {
		return nil // historical Codex-only admission is unchanged
	}
	if err != nil {
		return ErrProviderAttempt
	}
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT config_snapshot_bytes FROM tickets WHERE channel=? AND project_id=? AND id=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket).Scan(&raw); err != nil {
		return err
	}
	effective, err := config.DecodeSnapshot(raw, r.ConfigDigest)
	if err != nil {
		return ErrProviderPairRefused
	}
	var names []string
	switch r.Role {
	case "planner":
		names = effective.Providers.Planner
	case "builder":
		names = effective.Providers.Builder
	case "reviewer":
		names = effective.Providers.Reviewer
	}
	if len(names) != 1 || names[0] != r.Binding.Identity.Provider {
		return ErrProviderPairRefused
	}
	return nil
}

// v59 does not backfill consent or change historical monetary usage. A policy
// can only be installed before the first provider attempt. The ticket key is
// immutable across subsequent runner/leader recovery.
var migrationV59 = []string{
	`CREATE TABLE provider_accounting_policies (
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		policy TEXT NOT NULL CHECK(policy='reported_estimate_v1'),
		request_limit INTEGER NOT NULL CHECK(request_limit=16),
		request_timeout_ns INTEGER NOT NULL CHECK(request_timeout_ns=2700000000000),
		estimate_limit_micro_usd INTEGER NOT NULL CHECK(estimate_limit_micro_usd>0),
		ticket_version INTEGER NOT NULL CHECK(ticket_version>0),
		leader_epoch INTEGER NOT NULL CHECK(leader_epoch>0), runner_epoch INTEGER NOT NULL CHECK(runner_epoch>0),
		PRIMARY KEY(channel,project_id,ticket_id),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES tickets(channel,project_id,id)
	)`,
	`CREATE TRIGGER provider_accounting_policies_immutable_update BEFORE UPDATE ON provider_accounting_policies BEGIN SELECT RAISE(ABORT,'provider accounting policy is immutable'); END`,
	`CREATE TRIGGER provider_accounting_policies_immutable_delete BEFORE DELETE ON provider_accounting_policies BEGIN SELECT RAISE(ABORT,'provider accounting policy is append-only'); END`,
	`CREATE TABLE provider_cost_estimates (
		provider_attempt_id INTEGER PRIMARY KEY REFERENCES provider_attempts(id),
		channel TEXT NOT NULL, project_id TEXT NOT NULL, ticket_id TEXT NOT NULL,
		estimate_micro_usd INTEGER CHECK(estimate_micro_usd>=0),
		FOREIGN KEY(channel,project_id,ticket_id) REFERENCES provider_accounting_policies(channel,project_id,ticket_id)
	)`,
	`CREATE TRIGGER provider_cost_estimates_immutable_update BEFORE UPDATE ON provider_cost_estimates BEGIN SELECT RAISE(ABORT,'provider cost estimate is immutable'); END`,
	`CREATE TRIGGER provider_cost_estimates_immutable_delete BEFORE DELETE ON provider_cost_estimates BEGIN SELECT RAISE(ABORT,'provider cost estimate is append-only'); END`,
}

// RecordProviderCostEstimate records an unverified estimate (nil means unknown)
// after authenticated drain. It is separate from monetary charges and cannot
// complete an attempt or authorize another launch. Exact replay is idempotent.
func (s *Store) RecordProviderCostEstimate(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, estimate *int64) error {
	if !contracts.UsesMultiCLIRequestLimit(claim.Binding.Identity.Provider) || estimate != nil && *estimate < 0 {
		return ErrProviderAttempt
	}
	if !contracts.VerifyDrainProof(claim.SupervisorKey, drainRequestForClaim(claim), proof) {
		return ErrProviderDrain
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		persisted, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, claim.ID)
		if err != nil || !sameImmutableProviderAttemptClaim(claim, persisted) {
			return ErrProviderAttempt
		}
		if _, err := loadProviderAccountingPolicy(ctx, conn, claim.Ref); err != nil {
			return err
		}
		var prior sql.NullInt64
		err = conn.QueryRowContext(ctx, `SELECT estimate_micro_usd FROM provider_cost_estimates WHERE provider_attempt_id=? AND channel=? AND project_id=? AND ticket_id=?`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&prior)
		if err == nil {
			if prior.Valid != (estimate != nil) || estimate != nil && prior.Int64 != *estimate {
				return ErrEvidenceConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&version, &runner); err != nil {
			return err
		}
		if version != claim.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, claim.Ref.Channel, version, runner, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}); err != nil {
			return err
		}
		var state string
		if err := conn.QueryRowContext(ctx, `SELECT state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&state); err != nil {
			return err
		}
		if state != "active" {
			return ErrProviderAttempt
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO provider_cost_estimates VALUES(?,?,?,?,?)`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, estimate)
		return err
	})
}

// ProviderCostEstimate returns known=false for an explicit unknown estimate,
// ErrNotFound for no observation. Neither case is a verified zero charge.
func (s *Store) ProviderCostEstimate(ctx context.Context, claim ProviderAttemptClaim) (microUSD int64, known bool, err error) {
	persisted, err := loadAuthenticatedProviderAttemptClaim(ctx, s.db, claim.ID)
	if err != nil || !sameImmutableProviderAttemptClaim(claim, persisted) {
		return 0, false, ErrProviderAttempt
	}
	var value sql.NullInt64
	err = s.db.QueryRowContext(ctx, `SELECT estimate_micro_usd FROM provider_cost_estimates WHERE provider_attempt_id=? AND channel=? AND project_id=? AND ticket_id=?`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, ErrNotFound
	}
	return value.Int64, value.Valid, err
}

// ProviderResultAccountingAccepted authenticates accounting independently of
// artifact validity. Estimated mode requires explicit durable policy and an
// exact drained observation; it never changes the meaning of UsageTrusted.
func (s *Store) ProviderResultAccountingAccepted(ctx context.Context, claim ProviderAttemptClaim, raw contracts.PhaseResult) bool {
	if raw.Provider != claim.Binding.Identity || raw.UsageUnits < 0 {
		return false
	}
	if raw.UsageTrusted {
		return true
	}
	if raw.UsageUnits != 0 || !contracts.UsesMultiCLIRequestLimit(claim.Binding.Identity.Provider) {
		return false
	}
	if _, err := s.ProviderAccountingPolicy(ctx, claim.Ref); err != nil {
		return false
	}
	value, known, err := s.ProviderCostEstimate(ctx, claim)
	return err == nil && known == (raw.ReportedCostEstimateMicroUSD != nil) && (!known || value == *raw.ReportedCostEstimateMicroUSD)
}

// ProviderAccountingPolicy describes estimates, never verified charges or a
// hard-dollar guarantee. A nil/missing policy means no opt-in. RequestLimit
// counts SF launches, not provider-internal model calls.
type ProviderAccountingPolicy struct {
	Policy                string
	RequestLimit          int
	RequestTimeout        time.Duration
	EstimateLimitMicroUSD int64
}

func loadProviderAccountingPolicy(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef) (ProviderAccountingPolicy, error) {
	var value ProviderAccountingPolicy
	err := q.QueryRowContext(ctx, `SELECT policy,request_limit,request_timeout_ns,estimate_limit_micro_usd FROM provider_accounting_policies WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&value.Policy, &value.RequestLimit, &value.RequestTimeout, &value.EstimateLimitMicroUSD)
	if errors.Is(err, sql.ErrNoRows) {
		return value, ErrNotFound
	}
	if err != nil {
		return value, err
	}
	if value.Policy != "reported_estimate_v1" || value.RequestLimit != contracts.MultiCLIRequestLimit || value.RequestTimeout != contracts.MultiCLIRequestTimeout || value.EstimateLimitMicroUSD <= 0 {
		return ProviderAccountingPolicy{}, ErrEvidenceConflict
	}
	return value, nil
}

func (s *Store) ProviderAccountingPolicy(ctx context.Context, ref domain.TicketRef) (ProviderAccountingPolicy, error) {
	return loadProviderAccountingPolicy(ctx, s.db, ref)
}

// ApproveProviderEstimatedAccounting records explicit operator choice before
// the first attempt. Callers must not infer it from browser login. Replaying an
// existing exact policy is harmless; changing an in-flight ticket is refused.
// This API does not itself enable execution or certify reported prices.
func (s *Store) ApproveProviderEstimatedAccounting(ctx context.Context, ref domain.TicketRef, expected uint64, fence domain.Fence) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var maxCost int64
		var state string
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch,max_cost_micro_usd,state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner, &maxCost, &state); err != nil {
			return err
		}
		if version != expected {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		if maxCost <= 0 {
			return ErrBudgetExhausted
		}
		prior, err := loadProviderAccountingPolicy(ctx, conn, ref)
		if err == nil {
			if prior.EstimateLimitMicroUSD != maxCost {
				return ErrEvidenceConflict
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		if state != string(domain.StatePlanning) && state != string(domain.StateQueued) {
			return ErrProviderAttempt
		}
		var attempts int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&attempts); err != nil {
			return err
		}
		if attempts != 0 {
			return ErrProviderAttempt
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO provider_accounting_policies VALUES(?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, "reported_estimate_v1", contracts.MultiCLIRequestLimit, int64(contracts.MultiCLIRequestTimeout), maxCost, expected, fence.LeaderEpoch, fence.RunnerEpoch)
		return err
	})
}
