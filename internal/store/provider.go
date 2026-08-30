package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type ProviderAttemptRequest struct {
	Ref              domain.TicketRef
	ExpectedVersion  uint64
	Fence            domain.Fence
	Phase            domain.Phase
	Role             string
	Binding          contracts.RuntimeBinding
	ConfigDigest     string
	Capacity         int
	At               time.Time
	ExpectedHead     string
	ExpectedProof    string
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
	SupervisorKey    []byte
}
type ProviderAttemptClaim struct {
	ID               int64
	Ref              domain.TicketRef
	Phase            domain.Phase
	Role             string
	Attempt          int
	Binding          contracts.RuntimeBinding
	QualificationID  int64
	LeaseKey         string
	BindingDigest    string
	LeaderEpoch      uint64
	RunnerEpoch      uint64
	ExpectedVersion  uint64
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
	SupervisorKey    []byte
}
type ProviderAttempt struct {
	ProviderAttemptClaim
	State, Outcome        string
	UsageUnits            int64
	StartedAt, FinishedAt time.Time
}

func (s *Store) ProviderLaunchIdentity(ctx context.Context, claim ProviderAttemptClaim) (contracts.ProviderLaunch, error) {
	var launch contracts.ProviderLaunch
	err := s.db.QueryRowContext(ctx, `SELECT process_pid,process_pgid,process_boot_identity,process_start_identity,worktree_path FROM provider_attempts WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND state IN ('active','quarantined') AND launch_state='released'`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey).Scan(&launch.PID, &launch.PGID, &launch.BootIdentity, &launch.ProcessStartIdentity, &launch.Worktree)
	if err != nil {
		return contracts.ProviderLaunch{}, err
	}
	if launch.PID <= 0 || launch.PGID <= 0 || launch.BootIdentity == "" || launch.ProcessStartIdentity == "" || launch.Worktree == "" {
		return contracts.ProviderLaunch{}, ErrProviderDrain
	}
	return launch, nil
}

func (s *Store) SetRecoveryAuthority(ctx context.Context, channel domain.Channel, leader uint64, key []byte) error {
	if !channel.Valid() || leader == 0 || len(key) != 32 {
		return ErrProviderAttempt
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		row, err := conn.ExecContext(ctx, `UPDATE daemon_instances SET recovery_public_key=? WHERE channel=? AND leader_epoch=?`, key, channel, leader)
		if err != nil {
			return err
		}
		n, _ := row.RowsAffected()
		if n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

// RecordProviderLaunch is the pre-exec gate's durable publication point. The
// wrapper remains blocked until this exact PID/PGID record commits.
func (s *Store) RecordProviderLaunch(ctx context.Context, claim ProviderAttemptClaim, launch contracts.ProviderLaunch) error {
	if claim.ID <= 0 || claim.Ref.Validate() != nil || claim.Phase == "" || !validProviderRole(claim.Role) || claim.Attempt <= 0 || claim.LeaseKey == "" || claim.BindingDigest == "" || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ExpectedVersion == 0 || launch.PID <= 0 || launch.PGID <= 0 || launch.PID != launch.PGID || launch.BootIdentity == "" || launch.ProcessStartIdentity == "" || launch.Worktree == "" || claim.Worktree != launch.Worktree {
		return ErrProviderAttempt
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		row, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET process_pid=?,process_pgid=?,process_boot_identity=?,process_start_identity=?,launch_state='released' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND state='active' AND launch_state='launching' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND worktree_path=?`, launch.PID, launch.PGID, launch.BootIdentity, launch.ProcessStartIdentity, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey, launch.Worktree)
		if err != nil {
			return err
		}
		n, _ := row.RowsAffected()
		if n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

// ValidateFinalReviewEvidence binds a final Reviewer launch to the newest
// durable candidate. Caller-supplied head/proof values are only hints until
// this check proves they match SQLite authority.
func (s *Store) ValidateFinalReviewEvidence(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, expectedHead, expectedProof string) error {
	if ref.Validate() != nil || expectedVersion == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || expectedHead == "" || expectedProof == "" {
		return ErrEvidenceConflict
	}
	return validateFinalReviewEvidence(ctx, s.db, ref, expectedVersion, fence, expectedHead, expectedProof)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func validateFinalReviewEvidence(ctx context.Context, query rowQueryer, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, expectedHead, expectedProof string) error {
	var head, proof, intent, source, ticketSource string
	var ticketVersion, runner, candidateRunner uint64
	var verificationIntent, verificationProof sql.NullString
	if err := query.QueryRowContext(ctx, `SELECT c.head_sha,c.proof_digest,c.verification_intent_digest,c.source_digest,t.version,t.runner_epoch,c.runner_epoch,t.source_digest,v.intent_digest,v.proof_digest
		FROM candidate_snapshots c
		JOIN tickets t ON t.channel=c.channel AND t.project_id=c.project_id AND t.id=c.ticket_id
		LEFT JOIN verifications v ON v.channel=c.channel AND v.project_id=c.project_id AND v.ticket_id=c.ticket_id
		WHERE c.channel=? AND c.project_id=? AND c.ticket_id=? ORDER BY c.generation DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&head, &proof, &intent, &source, &ticketVersion, &runner, &candidateRunner, &ticketSource, &verificationIntent, &verificationProof); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrEvidenceConflict
		}
		return normalizeBusy(ctx, err)
	}
	var leader uint64
	if err := query.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader); err != nil {
		return normalizeBusy(ctx, err)
	}
	if leader != fence.LeaderEpoch || head != expectedHead || proof != expectedProof || ticketVersion > expectedVersion || candidateRunner != fence.RunnerEpoch || runner != fence.RunnerEpoch || source != ticketSource || !verificationIntent.Valid || !verificationProof.Valid || intent != verificationIntent.String || proof != verificationProof.String {
		return ErrEvidenceConflict
	}
	return nil
}

// ActiveProviderAttempts returns durable claims that still require process
// supervision during restart. Callers must drain each exact provider before
// invoking RecoverProviderAttempts.
func (s *Store) ActiveProviderAttempts(ctx context.Context, channel domain.Channel) ([]ProviderAttempt, error) {
	if !channel.Valid() {
		return nil, errors.New("valid channel is required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.project_id,a.ticket_id,a.phase,a.attempt,a.provider,a.model,a.family,a.version,a.role,a.state,a.outcome,a.usage_units,a.started_at,a.finished_at,a.qualification_id,a.binding_digest,a.provider_lease_key,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,a.supervisor_key,COALESCE(q.binary_digest,''),COALESCE(q.policy_digest,''),COALESCE(q.fixture_digest,'') FROM provider_attempts a LEFT JOIN provider_qualifications q ON q.id=a.qualification_id WHERE a.channel=? AND a.state IN ('active','quarantined') ORDER BY a.id`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var out []ProviderAttempt
	for rows.Next() {
		var value ProviderAttempt
		var project, ticket, started, finished string
		var qualification sql.NullInt64
		if err := rows.Scan(&value.ID, &project, &ticket, &value.Phase, &value.Attempt, &value.Binding.Identity.Provider, &value.Binding.Identity.Model, &value.Binding.Identity.Family, &value.Binding.Identity.Version, &value.Role, &value.State, &value.Outcome, &value.UsageUnits, &started, &finished, &qualification, &value.BindingDigest, &value.LeaseKey, &value.LeaderEpoch, &value.RunnerEpoch, &value.ExpectedVersion, &value.Repository, &value.Worktree, &value.WorktreeIdentity, &value.BaseSHA, &value.SupervisorKey, &value.Binding.BinaryDigest, &value.Binding.PolicyDigest, &value.Binding.FixtureDigest); err != nil {
			return nil, err
		}
		value.Ref = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
		if qualification.Valid {
			value.QualificationID = qualification.Int64
		}
		if started != "" {
			value.StartedAt, err = time.Parse(time.RFC3339Nano, started)
			if err != nil {
				return nil, err
			}
		}
		if finished != "" {
			value.FinishedAt, err = time.Parse(time.RFC3339Nano, finished)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, value)
	}
	return out, rows.Err()
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
		if domain.State(state).Terminal() || state == string(domain.StateBlocked) || version != r.ExpectedVersion || config == "" || config != r.ConfigDigest || !providerAdmissionState(domain.State(state), r.Phase, r.Role) {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, r.Ref.Channel, version, runner, r.Fence); err != nil {
			return err
		}
		var projectPath, durablePath, durableIdentity, durableBase string
		if err := conn.QueryRowContext(ctx, `SELECT p.canonical_path,w.path,w.identity_json,w.base_sha FROM projects p JOIN worktrees w ON w.channel=p.channel AND w.project_id=p.id AND w.ticket_id=? WHERE p.channel=? AND p.id=?`, r.Ref.Ticket, r.Ref.Channel, r.Ref.Project).Scan(&projectPath, &durablePath, &durableIdentity, &durableBase); err != nil {
			return ErrEvidenceConflict
		}
		// Old in-process Store callers had no identity fields. They can only use
		// the already-registered durable record; runnable coordinator requests
		// always supply and are checked against every exact field.
		if r.Repository == "" && r.Worktree == "" && r.WorktreeIdentity == "" && r.BaseSHA == "" {
			r.Repository, r.Worktree, r.WorktreeIdentity, r.BaseSHA = projectPath, durablePath, durableIdentity, durableBase
		}
		if len(r.SupervisorKey) == 0 {
			return ErrProviderAttempt
		}
		if projectPath != r.Repository || durablePath != r.Worktree || durableIdentity != r.WorktreeIdentity || durableBase != r.BaseSHA {
			return ErrEvidenceConflict
		}
		// Provider admission is one side of the repository writer exclusion.
		// It is checked in this same IMMEDIATE transaction as the eventual
		// provider-attempt insert, so a Git writer cannot
		// slip between a read-only preflight and process launch.
		var activeGitWriter int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, r.Repository).Scan(&activeGitWriter); err != nil {
			return err
		}
		if activeGitWriter != 0 {
			return ErrProviderAttempt
		}
		if r.Phase == domain.PhaseReview && r.Role == "reviewer" {
			if err := validateFinalReviewEvidence(ctx, conn, r.Ref, r.ExpectedVersion, r.Fence, r.ExpectedHead, r.ExpectedProof); err != nil {
				return err
			}
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
		if qualification.ID <= 0 {
			return ErrProviderPairRefused
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
		outcome := "running"
		if _, err = conn.ExecContext(ctx, `INSERT INTO phase_runs(channel,project_id,ticket_id,phase,attempt,state,leader_epoch,runner_epoch,expected_ticket_version,provider,model,family,provider_version,worktree_identity,base_sha,started_at,outcome) VALUES(?,?,?,?,?,'active',?,?,?,?,?,?,?,?,?,?,?)`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, prior, r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion, r.Binding.Identity.Provider, r.Binding.Identity.Model, r.Binding.Identity.Family, r.Binding.Identity.Version, r.WorktreeIdentity, r.BaseSHA, r.At.UTC().Format(time.RFC3339Nano), outcome); err != nil {
			return err
		}
		bindingDigest := bindingDigest(r.Binding)
		row, err := conn.ExecContext(ctx, `INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,qualification_id,binding_digest,provider_lease_key,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,supervisor_key,launch_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, prior, r.Binding.Identity.Provider, r.Binding.Identity.Model, r.Binding.Identity.Family, r.Binding.Identity.Version, outcome, r.Role, "active", 0, r.At.UTC().Format(time.RFC3339Nano), "", qualification.ID, bindingDigest, lease.ScopeKey, r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion, r.Repository, r.Worktree, r.WorktreeIdentity, r.BaseSHA, r.SupervisorKey, "launching")
		if err != nil {
			return err
		}
		id, err := row.LastInsertId()
		if err != nil {
			return err
		}
		claim = ProviderAttemptClaim{ID: id, Ref: r.Ref, Phase: r.Phase, Role: r.Role, Attempt: prior, Binding: r.Binding, QualificationID: qualification.ID, LeaseKey: lease.ScopeKey, BindingDigest: bindingDigest, LeaderEpoch: r.Fence.LeaderEpoch, RunnerEpoch: r.Fence.RunnerEpoch, ExpectedVersion: r.ExpectedVersion, Repository: r.Repository, Worktree: r.Worktree, WorktreeIdentity: r.WorktreeIdentity, BaseSHA: r.BaseSHA, SupervisorKey: append([]byte(nil), r.SupervisorKey...)}
		return nil
	})
	return claim, err
}

func (s *Store) FinishProviderAttempt(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, state, outcome string, usage int64, finished time.Time) error {
	if claim.ID <= 0 || claim.ExpectedVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || !validAttemptState(state) || !safeOutcome(outcome) || usage < 0 || finished.IsZero() {
		return ErrProviderAttempt
	}
	if claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch {
		return ErrStaleFence
	}
	if !contracts.VerifyDrainProof(claim.SupervisorKey, drainRequestForClaim(claim), proof) {
		return ErrProviderDrain
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var persisted ProviderAttemptClaim
		var persistedRole, persistedState, persistedBinding string
		var persistedQualification sql.NullInt64
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&version, &runner); err != nil {
			return err
		}
		if version != expected {
			return ErrStaleFence
		}
		if claim.ExpectedVersion != expected {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, claim.Ref.Channel, version, runner, fence); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT provider,model,family,version,role,state,qualification_id,binding_digest FROM provider_attempts WHERE id=?`, claim.ID).Scan(&persisted.Binding.Identity.Provider, &persisted.Binding.Identity.Model, &persisted.Binding.Identity.Family, &persisted.Binding.Identity.Version, &persistedRole, &persistedState, &persistedQualification, &persistedBinding); err != nil {
			return err
		}
		if persistedRole != claim.Role || persistedState != "active" || !persistedQualification.Valid || persistedQualification.Int64 != claim.QualificationID || persistedBinding == "" || persistedBinding != bindingDigest(claim.Binding) || claim.BindingDigest != "" && claim.BindingDigest != persistedBinding {
			return ErrStaleFence
		}
		var maxCost, spent, maxDuration int64
		if err := conn.QueryRowContext(ctx, `SELECT max_cost_micro_usd FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&maxCost); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(usage_units),0) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND id<>?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.ID).Scan(&spent); err != nil {
			return err
		}
		if maxCost <= 0 || usage > maxCost-spent {
			return ErrBudgetExhausted
		}
		var created string
		if err := conn.QueryRowContext(ctx, `SELECT created_at,max_duration_ns FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&created, &maxDuration); err != nil {
			return err
		}
		createdAt, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return err
		}
		if maxDuration <= 0 || finished.After(createdAt.Add(time.Duration(maxDuration))) {
			return ErrBudgetExhausted
		}
		row, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state=?,outcome=?,usage_units=?,finished_at=?,launch_state='drained' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND state='active'`, state, outcome, usage, finished.UTC().Format(time.RFC3339Nano), claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion)
		if err != nil {
			return err
		}
		n, _ := row.RowsAffected()
		if n != 1 {
			return ErrStaleFence
		}
		row, err = conn.ExecContext(ctx, `UPDATE phase_runs SET state=?,completed_at=?,provider=?,model=?,family=?,provider_version=?,outcome=? WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND provider=? AND model=? AND family=? AND provider_version=?`, state, finished.UTC().Format(time.RFC3339Nano), claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, outcome, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version)
		if err != nil {
			return err
		}
		n, _ = row.RowsAffected()
		if n != 1 {
			return ErrStaleFence
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, claim.Ref.Channel, claim.LeaseKey, claim.Ref.Project, claim.Ref.Ticket, claim.RunnerEpoch)
		return err
	})
}

// QuarantineProviderAttempt keeps a fenced claim and its capacity reserved
// when the supervisor cannot prove that the provider is drained. It is
// intentionally terminal only for the attempt row; the active phase remains
// blocked until recovery establishes a real drain proof.
func (s *Store) QuarantineProviderAttempt(ctx context.Context, claim ProviderAttemptClaim, expected uint64, fence domain.Fence, at time.Time) error {
	if claim.ID <= 0 || claim.ExpectedVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ExpectedVersion != expected || claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch || at.IsZero() {
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
			return ErrStaleFence
		}
		result, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='quarantined',outcome='undrained' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND state='active'`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

// FailProviderAttemptBudget closes a drained attempt without recording usage
// beyond the ticket ceiling. It is used when a provider reports more units
// than remain; the ticket is blocked for operator reconciliation rather than
// releasing a still-active claim.
func (s *Store) FailProviderAttemptBudget(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, at time.Time) error {
	if claim.ID <= 0 || claim.ExpectedVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ExpectedVersion != expected || claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch || at.IsZero() {
		return ErrProviderAttempt
	}
	if !contracts.VerifyDrainProof(claim.SupervisorKey, drainRequestForClaim(claim), proof) {
		return ErrProviderDrain
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
		result, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='failed',outcome='budget_exhausted',usage_units=0,finished_at=?,launch_state='drained' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND state='active'`, at.UTC().Format(time.RFC3339Nano), claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		result, err = conn.ExecContext(ctx, `UPDATE phase_runs SET state='failed',completed_at=?,outcome='budget_exhausted' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=?`, at.UTC().Format(time.RFC3339Nano), claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, claim.Ref.Channel, claim.LeaseKey, claim.Ref.Project, claim.Ref.Ticket, claim.RunnerEpoch)
		return err
	})
}

// recoverProviderAttempts is retained only for local quarantine bookkeeping;
// it never releases a lease. Exact release is proof-only below.
func (s *Store) recoverProviderAttempts(ctx context.Context, ref domain.TicketRef, staleRunner, leader uint64, at time.Time) error {
	if ref.Validate() != nil || staleRunner == 0 || leader == 0 || at.IsZero() {
		return ErrProviderAttempt
	}
	return s.quarantineProviderAttempts(ctx, ref, staleRunner, leader, at)
}

// RecoverProviderAttemptClaim is the restart-safe recovery primitive. Unlike
// the legacy ref-based helper, it carries the original leader epoch and every
// persisted claim identity, so a new leader can release only this exact old
// runner after the supervisor proves that runner drained.
func (s *Store) recoverProviderAttemptClaim(ctx context.Context, claim ProviderAttempt, leader uint64, at time.Time) error {
	if claim.Ref.Validate() != nil || claim.ID <= 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ExpectedVersion == 0 || claim.BindingDigest == "" || claim.LeaseKey == "" || leader == 0 || at.IsZero() {
		return ErrProviderAttempt
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader, currentRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, claim.Ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if currentLeader != leader {
			return ErrStaleFence
		}
		if err := conn.QueryRowContext(ctx, `SELECT runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&currentRunner); err != nil {
			return err
		}
		if currentRunner == claim.RunnerEpoch {
			return ErrProviderDrain
		}
		var state string
		if err := conn.QueryRowContext(ctx, `SELECT state FROM provider_attempts WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=?`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleFence
			}
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='cancelled',outcome='drained_recovery',finished_at=?,launch_state='drained' WHERE id=? AND state IN ('active','quarantined')`, at.UTC().Format(time.RFC3339Nano), claim.ID); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `UPDATE phase_runs SET state='cancelled',completed_at=?,outcome='drained_recovery' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND provider=? AND model=? AND family=? AND provider_version=?`, at.UTC().Format(time.RFC3339Nano), claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, claim.Ref.Channel, claim.LeaseKey, claim.Ref.Project, claim.Ref.Ticket, claim.RunnerEpoch)
		return err
	})
}

// RecoverProviderAttemptClaimWithProof is the daemon recovery authority. A
// replacement leader must present a proof signed by the supervisor key stored
// with the old claim; an arbitrary new supervisor key cannot release it.
func (s *Store) RecoverProviderAttemptClaimWithProof(ctx context.Context, claim ProviderAttempt, leader uint64, proof contracts.DrainProof, at time.Time) error {
	var key []byte
	if err := s.db.QueryRowContext(ctx, `SELECT recovery_public_key FROM daemon_instances WHERE channel=? AND leader_epoch=?`, claim.Ref.Channel, leader).Scan(&key); err != nil {
		return err
	}
	if !contracts.VerifyDrainProof(key, drainRequestForClaim(claim.ProviderAttemptClaim), proof) {
		return ErrProviderDrain
	}
	return s.recoverProviderAttemptClaim(ctx, claim, leader, at)
}

// QuarantineRecoveredProviderAttemptClaim is used when a launch record is
// absent or no longer proves the live Unix process is ours. It intentionally
// retains the provider lease and active phase: no replacement run may start
// while an unverified process could still exist.
func (s *Store) QuarantineRecoveredProviderAttemptClaim(ctx context.Context, claim ProviderAttempt, leader uint64, at time.Time) error {
	if claim.Ref.Validate() != nil || claim.ID <= 0 || leader == 0 || at.IsZero() {
		return ErrProviderAttempt
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, claim.Ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if currentLeader != leader {
			return ErrStaleFence
		}
		row, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='quarantined',outcome='undrained_recovery',launch_state='quarantined' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND state IN ('active','quarantined')`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey)
		if err != nil {
			return err
		}
		if n, _ := row.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

func (s *Store) quarantineProviderAttempts(ctx context.Context, ref domain.TicketRef, staleRunner, leader uint64, at time.Time) error {
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
		_, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='quarantined',outcome='undrained_recovery' WHERE channel=? AND project_id=? AND ticket_id=? AND state='active' AND leader_epoch=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, leader, staleRunner)
		return err
	})
}

func (s *Store) ProviderAttempts(ctx context.Context, ref domain.TicketRef) ([]ProviderAttempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.phase,a.attempt,a.provider,a.model,a.family,a.version,a.role,a.state,a.outcome,a.usage_units,a.started_at,a.finished_at,a.qualification_id,a.binding_digest,a.provider_lease_key,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,a.supervisor_key,COALESCE(q.binary_digest,''),COALESCE(q.policy_digest,''),COALESCE(q.fixture_digest,'') FROM provider_attempts a LEFT JOIN provider_qualifications q ON q.id=a.qualification_id WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? ORDER BY a.id`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var out []ProviderAttempt
	for rows.Next() {
		var v ProviderAttempt
		var started, finished string
		var qualification sql.NullInt64
		if err := rows.Scan(&v.ID, &v.Phase, &v.Attempt, &v.Binding.Identity.Provider, &v.Binding.Identity.Model, &v.Binding.Identity.Family, &v.Binding.Identity.Version, &v.Role, &v.State, &v.Outcome, &v.UsageUnits, &started, &finished, &qualification, &v.BindingDigest, &v.LeaseKey, &v.LeaderEpoch, &v.RunnerEpoch, &v.ExpectedVersion, &v.Repository, &v.Worktree, &v.WorktreeIdentity, &v.BaseSHA, &v.SupervisorKey, &v.Binding.BinaryDigest, &v.Binding.PolicyDigest, &v.Binding.FixtureDigest); err != nil {
			return nil, err
		}
		if qualification.Valid {
			v.QualificationID = qualification.Int64
		}
		var err error
		if started != "" {
			v.StartedAt, err = time.Parse(time.RFC3339Nano, started)
			if err != nil {
				return nil, err
			}
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
	if role == "planner" || role == "builder" || role == "reviewer" {
		col := "planner_qualification_id"
		if role == "builder" {
			col = "builder_qualification_id"
		}
		if role == "reviewer" {
			col = "reviewer_qualification_id"
		}
		query += ` JOIN provider_pair_selections p ON p.channel=q.channel AND q.id=p.` + col
	}
	where += ` AND NOT EXISTS (SELECT 1 FROM provider_qualifications newer WHERE newer.channel=q.channel AND newer.provider=q.provider AND newer.model=q.model AND newer.family=q.family AND newer.provider_version=q.provider_version AND newer.id>q.id) AND NOT EXISTS (SELECT 1 FROM provider_qualifications disabled WHERE disabled.channel=q.channel AND disabled.provider=q.provider AND disabled.provider_version=q.provider_version AND disabled.profile='disabled' AND disabled.id>q.id) AND NOT EXISTS (SELECT 1 FROM provider_pair_selections ps JOIN provider_qualifications b ON b.id=ps.builder_qualification_id JOIN provider_qualifications r ON r.id=ps.reviewer_qualification_id WHERE ps.channel=q.channel AND (b.channel<>q.channel OR r.channel<>q.channel OR b.profile='disabled' OR r.profile='disabled' OR b.family=r.family))`
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
func providerAdmissionState(state domain.State, phase domain.Phase, role string) bool {
	switch {
	case role == "planner" && phase == domain.PhasePlanning:
		return state == domain.StatePlanning
	case role == "builder" && phase == domain.PhaseBuild:
		return state == domain.StateBuilding
	case role == "reviewer" && phase == domain.PhaseVerification:
		return state == domain.StateVerifying
	case role == "reviewer" && phase == domain.PhaseReview:
		return state == domain.StateReviewing
	default:
		return false
	}
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
func validProviderIdentityClaim(r ProviderAttemptRequest) bool {
	return r.Repository != "" && r.Worktree != "" && r.WorktreeIdentity != "" && validOID(r.BaseSHA) && len(r.SupervisorKey) == 32
}
func drainRequestForClaim(c ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: c.ID, Identity: c.Binding.Identity, Ref: c.Ref, Phase: c.Phase, Role: c.Role, Attempt: c.Attempt, LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch, ExpectedVersion: c.ExpectedVersion, LeaseKey: c.LeaseKey, BindingDigest: c.BindingDigest, BinaryDigest: c.Binding.BinaryDigest, PolicyDigest: c.Binding.PolicyDigest, AuthDigest: c.Binding.AuthDigest, Repository: c.Repository, Worktree: c.Worktree, WorktreeIdentity: c.WorktreeIdentity, BaseSHA: c.BaseSHA}
}
func hexDigest(v string) bool {
	return len(v) == 64 && strings.ToLower(v) == v && strings.Trim(v, "0123456789abcdef") == ""
}
func bindingDigest(v contracts.RuntimeBinding) string {
	// Include every runtime fact, including the account digest, while never
	// persisting credential material itself. The qualification ID separately
	// anchors the non-secret facts to the durable root authority.
	sum := sha256.Sum256([]byte(v.Identity.Provider + "\x00" + v.Identity.Model + "\x00" + v.Identity.Family + "\x00" + v.Identity.Version + "\x00" + v.BinaryDigest + "\x00" + v.PolicyDigest + "\x00" + v.FixtureDigest + "\x00" + v.AuthDigest))
	return hex.EncodeToString(sum[:])
}
