package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type ProviderAttemptRequest struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Phase           domain.Phase
	Role            string
	Binding         contracts.RuntimeBinding
	ConfigDigest    string
	Capacity        int
	At              time.Time
}
type ProviderAttemptClaim struct {
	ID              int64
	Ref             domain.TicketRef
	Phase           domain.Phase
	Role            string
	Attempt         int
	Binding         contracts.RuntimeBinding
	QualificationID int64
	LeaseKey        string
}
type ProviderAttempt struct {
	ProviderAttemptClaim
	State, Outcome        string
	UsageUnits            int64
	StartedAt, FinishedAt time.Time
}

func (s *Store) BeginProviderAttempt(ctx context.Context, r ProviderAttemptRequest) (ProviderAttemptClaim, error) {
	if r.Ref.Validate() != nil || !validProviderPhase(r.Phase) || !validProviderRole(r.Role) || r.ExpectedVersion == 0 || r.Fence.LeaderEpoch == 0 || r.Fence.RunnerEpoch == 0 || r.Capacity < 1 || r.Capacity > 16 || r.At.IsZero() || !validRuntimeBinding(r.Binding) {
		return ProviderAttemptClaim{}, ErrProviderAttempt
	}
	var claim ProviderAttemptClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var state string
		var maxDuration, maxCost int64
		var created, config string
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch,state,max_duration_ns,max_cost_micro_usd,created_at,config_digest FROM tickets WHERE channel=? AND project_id=? AND id=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket).Scan(&version, &runner, &state, &maxDuration, &maxCost, &created, &config); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if domain.State(state).Terminal() || state == string(domain.StateBlocked) || version != r.ExpectedVersion || config == "" || config != r.ConfigDigest {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, r.Ref.Channel, version, runner, r.Fence); err != nil {
			return err
		}
		createdAt, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return err
		}
		if maxDuration <= 0 || maxCost <= 0 || !r.At.Before(createdAt.Add(time.Duration(maxDuration))) {
			return ErrBudgetExhausted
		}
		qualification, err := currentRuntimeQualification(ctx, conn, r.Ref.Channel, r.Role, r.Binding)
		if err != nil {
			return err
		}
		var spent int64
		if err = conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(usage_units),0) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket).Scan(&spent); err != nil {
			return err
		}
		if spent >= maxCost {
			return ErrBudgetExhausted
		}
		var active int
		if err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return ErrProviderAttempt
		}
		var prior int
		if err = conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(attempt),0) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase).Scan(&prior); err != nil {
			return err
		}
		if prior >= 2 {
			return ErrBudgetExhausted
		}
		prior++
		if err := independentProvider(ctx, conn, r.Ref, r.Role, r.Binding.Identity.Family); err != nil {
			return err
		}
		resource := r.Binding.Identity.Provider + ":" + r.Binding.AuthDigest
		lease, ok, err := acquireLease(ctx, conn, r.Ref, runner, LeaseRequest{Scope: "provider", Resource: resource, Capacity: r.Capacity}, r.At.UTC())
		if err != nil {
			return err
		}
		if !ok {
			return ErrProviderCapacity
		}
		if _, err = conn.ExecContext(ctx, `INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version) VALUES(?,?,?,?,?,'active',?,?,?)`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, prior, r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion); err != nil {
			return err
		}
		outcome := "running"
		bindingDigest := bindingDigest(r.Binding)
		row, err := conn.ExecContext(ctx, `INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,qualification_id,binding_digest,provider_lease_key) VALUES(?,?,?,?,?,?,?,?,? ,? ,?,'active',0,?,'',?,?,?)`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, prior, r.Binding.Identity.Provider, r.Binding.Identity.Model, r.Binding.Identity.Family, r.Binding.Identity.Version, outcome, r.Role, r.At.UTC().Format(time.RFC3339Nano), qualification.ID, bindingDigest, lease.ScopeKey)
		if err != nil {
			return err
		}
		id, err := row.LastInsertId()
		if err != nil {
			return err
		}
		claim = ProviderAttemptClaim{ID: id, Ref: r.Ref, Phase: r.Phase, Role: r.Role, Attempt: prior, Binding: r.Binding, QualificationID: qualification.ID, LeaseKey: lease.ScopeKey}
		return nil
	})
	return claim, err
}

func (s *Store) FinishProviderAttempt(ctx context.Context, claim ProviderAttemptClaim, expected uint64, fence domain.Fence, state, outcome string, usage int64, finished time.Time) error {
	if claim.ID <= 0 || !validAttemptState(state) || !safeOutcome(outcome) || usage < 0 || finished.IsZero() {
		return ErrProviderAttempt
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&version, &runner); err != nil {
			return err
		}
		if version != expected {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, claim.Ref.Channel, version, runner, fence); err != nil {
			return err
		}
		row, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state=?,outcome=?,usage_units=?,finished_at=? WHERE id=? AND state='active'`, state, outcome, usage, finished.UTC().Format(time.RFC3339Nano), claim.ID)
		if err != nil {
			return err
		}
		n, _ := row.RowsAffected()
		if n != 1 {
			return ErrStaleFence
		}
		row, err = conn.ExecContext(ctx, `UPDATE phase_runs SET state=?,completed_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active'`, state, finished.UTC().Format(time.RFC3339Nano), claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt)
		if err != nil {
			return err
		}
		n, _ = row.RowsAffected()
		if n != 1 {
			return ErrStaleFence
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, claim.Ref.Channel, claim.LeaseKey, claim.Ref.Project, claim.Ref.Ticket, fence.RunnerEpoch)
		return err
	})
}

// RecoverProviderAttempts releases only claims whose old process group has
// been proven drained. Without that proof it fails closed and leaves capacity.
func (s *Store) RecoverProviderAttempts(ctx context.Context, ref domain.TicketRef, staleRunner, leader uint64, drained bool, at time.Time) error {
	if ref.Validate() != nil || staleRunner == 0 || leader == 0 || at.IsZero() {
		return ErrProviderAttempt
	}
	if !drained {
		return ErrProviderDrain
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader, currentRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if currentLeader != leader {
			return ErrStaleFence
		}
		if err := conn.QueryRowContext(ctx, `SELECT runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentRunner); err != nil {
			return err
		}
		if currentRunner == staleRunner {
			return ErrProviderDrain
		}
		if _, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='cancelled',outcome='drained_recovery',finished_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`, at.UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE phase_runs SET state='cancelled',completed_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`, at.UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket); err != nil {
			return err
		}
		_, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=? AND scope='provider'`, ref.Channel, ref.Project, ref.Ticket, staleRunner)
		return err
	})
}

func (s *Store) ProviderAttempts(ctx context.Context, ref domain.TicketRef) ([]ProviderAttempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,phase,attempt,provider,model,family,version,role,state,outcome,usage_units,started_at,finished_at,qualification_id,binding_digest,provider_lease_key FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY id`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var out []ProviderAttempt
	for rows.Next() {
		var v ProviderAttempt
		var started, finished string
		if err := rows.Scan(&v.ID, &v.Phase, &v.Attempt, &v.Binding.Identity.Provider, &v.Binding.Identity.Model, &v.Binding.Identity.Family, &v.Binding.Identity.Version, &v.Role, &v.State, &v.Outcome, &v.UsageUnits, &started, &finished, &v.QualificationID, &v.Binding.BinaryDigest, &v.LeaseKey); err != nil {
			return nil, err
		}
		var err error
		v.StartedAt, err = time.Parse(time.RFC3339Nano, started)
		if err != nil {
			return nil, err
		}
		if finished != "" {
			v.FinishedAt, err = time.Parse(time.RFC3339Nano, finished)
			if err != nil {
				return nil, err
			}
		}
		v.Ref = ref
		out = append(out, v)
	}
	return out, rows.Err()
}

func currentRuntimeQualification(ctx context.Context, conn *sql.Conn, channel domain.Channel, role string, b contracts.RuntimeBinding) (ProviderQualification, error) {
	var id int64
	q := ProviderQualification{}
	q.ID = 0
	query := `SELECT q.id,q.channel,q.run_id,q.provider,q.model,q.family,q.provider_version,q.binary_digest,q.policy_digest,q.fixture_digest,q.profile,q.failed_probes_json,q.reason_code,q.created_at FROM provider_qualifications q`
	where := ` WHERE q.channel=? AND q.provider=? AND q.model=? AND q.family=? AND q.provider_version=? AND q.binary_digest=? AND q.policy_digest=? AND q.fixture_digest=? AND q.profile IN ('qualified_guarded','autonomous_eligible')`
	args := []any{channel, b.Identity.Provider, b.Identity.Model, b.Identity.Family, b.Identity.Version, b.BinaryDigest, b.PolicyDigest, b.FixtureDigest}
	if role == "builder" || role == "reviewer" {
		col := "builder_qualification_id"
		if role == "reviewer" {
			col = "reviewer_qualification_id"
		}
		query += ` JOIN provider_pair_selections p ON p.channel=q.channel AND q.id=p.` + col
		where += ` AND NOT EXISTS (SELECT 1 FROM provider_qualifications newer WHERE newer.channel=q.channel AND newer.provider=q.provider AND newer.model=q.model AND newer.family=q.family AND newer.provider_version=q.provider_version AND newer.id>q.id)`
	}
	value, err := scanQualification(conn.QueryRowContext(ctx, query+where, args...))
	if err != nil {
		return ProviderQualification{}, ErrProviderPairRefused
	}
	id = value.ID
	_ = id
	return value, nil
}
func independentProvider(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, role, family string) error {
	if role != "builder" && role != "reviewer" {
		return nil
	}
	var count int
	other := "builder"
	if role == "builder" {
		other = "reviewer"
	}
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND role=? AND family=?`, ref.Channel, ref.Project, ref.Ticket, other, family).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return ErrProviderPairRefused
	}
	return nil
}
func validProviderPhase(p domain.Phase) bool {
	switch p {
	case domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview:
		return true
	}
	return false
}
func validProviderRole(v string) bool { return v == "planner" || v == "builder" || v == "reviewer" }
func validAttemptState(v string) bool { return v == "completed" || v == "failed" || v == "cancelled" }
func safeOutcome(v string) bool {
	if v == "completed" || v == "failed" || v == "cancelled" || v == "invalid_artifact" || v == "drained_recovery" {
		return true
	}
	return false
}
func validRuntimeBinding(v contracts.RuntimeBinding) bool {
	return v.Identity.Provider != "" && hexDigest(v.BinaryDigest) && hexDigest(v.PolicyDigest) && hexDigest(v.FixtureDigest) && hexDigest(v.AuthDigest)
}
func hexDigest(v string) bool {
	return len(v) == 64 && strings.ToLower(v) == v && strings.Trim(v, "0123456789abcdef") == ""
}
func bindingDigest(v contracts.RuntimeBinding) string { return v.BinaryDigest } // exact qualification already binds binary/policy/fixture; no credential-bearing data is stored.
