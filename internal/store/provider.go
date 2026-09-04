package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
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
	Input            contracts.PhaseInput
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
	Input            contracts.PhaseInput
	RequestDigest    string
	RequestPayload   []byte
}
type ProviderAttempt struct {
	ProviderAttemptClaim
	State, Outcome        string
	UsageUnits            int64
	StartedAt, FinishedAt time.Time
}

// ProviderAttemptResult is immutable, transcript-free completion evidence.
// Every digest is plain lowercase SHA-256 hex (not phaseartifact's display
// prefix) so SQLite and Store have exactly one durable spelling.
type ProviderAttemptResult struct {
	AttemptID                                                  int64
	RawArtifact, TypedArtifact, Validation                     []byte
	RawSHA256, TypedSHA256, ValidationSHA256, TranscriptSHA256 string
	Claim                                                      ProviderAttemptClaim
}

// ProviderAttemptResultKey identifies an immutable historical provider result.
// Unlike ProviderAttemptClaim it deliberately carries no live fence: results
// remain readable after the workflow transitions or a daemon restarts.
type ProviderAttemptResultKey struct {
	AttemptID int64
	Ref       domain.TicketRef
	Phase     domain.Phase
	Attempt   int
}

// ProviderArtifactFailure is bounded, transcript-free evidence for a
// repairable invalid_artifact provider attempt. It deliberately stores only a
// closed reason enum and immutable claim identity, never raw artifact bytes,
// provider stderr, transcript text, or an adapter error string.
type ProviderArtifactFailure struct {
	AttemptID       int64
	Ref             domain.TicketRef
	Phase           domain.Phase
	Role            string
	Attempt         int
	RequestDigest   string
	LeaderEpoch     uint64
	RunnerEpoch     uint64
	ExpectedVersion uint64
	Reason          contracts.ArtifactFailureReason
	Digest          string
	CreatedAt       time.Time
}

type providerArtifactFailureCanonical struct {
	AttemptID       int64                           `json:"attempt_id"`
	Channel         domain.Channel                  `json:"channel"`
	Project         domain.ProjectID                `json:"project"`
	Ticket          domain.TicketID                 `json:"ticket"`
	Phase           domain.Phase                    `json:"phase"`
	Role            string                          `json:"role"`
	Attempt         int                             `json:"attempt"`
	RequestDigest   string                          `json:"request_digest"`
	LeaderEpoch     uint64                          `json:"leader_epoch"`
	RunnerEpoch     uint64                          `json:"runner_epoch"`
	ExpectedVersion uint64                          `json:"expected_version"`
	Reason          contracts.ArtifactFailureReason `json:"reason"`
	CreatedAt       string                          `json:"created_at"`
}

func providerArtifactFailureDigest(value ProviderArtifactFailure) (string, error) {
	if value.AttemptID <= 0 || value.Ref.Validate() != nil || !validProviderPhase(value.Phase) || !validProviderRole(value.Role) || value.Attempt <= 0 || !validSHA256(value.RequestDigest) || value.LeaderEpoch == 0 || value.RunnerEpoch == 0 || value.ExpectedVersion == 0 || !contracts.ValidArtifactFailureReason(value.Reason) || value.CreatedAt.IsZero() {
		return "", ErrProviderAttempt
	}
	canonical, err := json.Marshal(providerArtifactFailureCanonical{
		AttemptID:       value.AttemptID,
		Channel:         value.Ref.Channel,
		Project:         value.Ref.Project,
		Ticket:          value.Ref.Ticket,
		Phase:           value.Phase,
		Role:            value.Role,
		Attempt:         value.Attempt,
		RequestDigest:   value.RequestDigest,
		LeaderEpoch:     value.LeaderEpoch,
		RunnerEpoch:     value.RunnerEpoch,
		ExpectedVersion: value.ExpectedVersion,
		Reason:          value.Reason,
		CreatedAt:       value.CreatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return "", err
	}
	return rawDigest(canonical), nil
}

func providerArtifactFailureForClaim(claim ProviderAttemptClaim, reason contracts.ArtifactFailureReason, createdAt time.Time) (ProviderArtifactFailure, error) {
	value := ProviderArtifactFailure{
		AttemptID:       claim.ID,
		Ref:             claim.Ref,
		Phase:           claim.Phase,
		Role:            claim.Role,
		Attempt:         claim.Attempt,
		RequestDigest:   claim.RequestDigest,
		LeaderEpoch:     claim.LeaderEpoch,
		RunnerEpoch:     claim.RunnerEpoch,
		ExpectedVersion: claim.ExpectedVersion,
		Reason:          reason,
		CreatedAt:       createdAt.UTC(),
	}
	digest, err := providerArtifactFailureDigest(value)
	if err != nil {
		return ProviderArtifactFailure{}, err
	}
	value.Digest = digest
	return value, nil
}

// LatestReusableProviderAttemptRequest asks for the single newest completed
// immutable provider result eligible to be reused after a restart.  Reuse is
// deliberately limited to the non-mutating Planner and Reviewer roles.
type LatestReusableProviderAttemptRequest struct {
	Ref             domain.TicketRef
	Phase           domain.Phase
	Role            string
	ExpectedVersion uint64
	Fence           domain.Fence
}

// LatestReusableProviderAttemptResult is authenticated evidence, never a
// provider transcript. Recovered reports the bounded runner/version recovery
// path rather than an exact-current fence read.
type LatestReusableProviderAttemptResult struct {
	Key       ProviderAttemptResultKey
	Result    ProviderAttemptResult
	Parsed    phaseartifact.Parsed
	Recovered bool
}

// LoadCurrentProviderAttemptResult authenticates an exact completed result
// against the live ticket and leader fence.  It is the only worker-facing
// route for a PhaseRunner's opaque result key; callers never supply Parsed
// provider output as transition authority.
func (s *Store) LoadCurrentProviderAttemptResult(ctx context.Context, key ProviderAttemptResultKey, expected uint64, fence domain.Fence) (ProviderAttemptResult, phaseartifact.Parsed, error) {
	result, parsed, err := s.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, err
	}
	if result.Claim.ExpectedVersion != expected || result.Claim.LeaderEpoch != fence.LeaderEpoch || result.Claim.RunnerEpoch != fence.RunnerEpoch {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
	}
	if err := s.AssertTicketFence(ctx, key.Ref, expected, fence); err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, err
	}
	if result.Claim.Phase == domain.PhaseBuild && result.Claim.Role == "builder" {
		if _, repairErr := s.candidateRepairBuildContextAt(ctx, s.db, key.Ref, expected, fence); repairErr == nil {
			if candidateRepairBuilderEntryResultReachesFence(ctx, s.db, key, result, expected, fence) != nil {
				return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
			}
			return result, parsed, nil
		} else if !errors.Is(repairErr, ErrNotFound) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
		}
	}
	if err := validateRunnerRecoveryAuthority(ctx, s.db, key.Ref, expected, fence); err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
	}
	return result, parsed, nil
}

func rawDigest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func validSHA256(value string) bool {
	return len(value) == 64 && strings.ToLower(value) == value && strings.Trim(value, "0123456789abcdef") == ""
}

func makeProviderAttemptResult(claim ProviderAttemptClaim, raw contracts.PhaseResult, validation phaseartifact.Validation, parsed phaseartifact.Parsed) (ProviderAttemptResult, error) {
	typed, typedDigest, err := phaseartifact.CanonicalTypedArtifact(parsed)
	if err != nil {
		return ProviderAttemptResult{}, err
	}
	validationBytes, validationDigest, err := phaseartifact.CanonicalValidation(validation)
	if err != nil {
		return ProviderAttemptResult{}, err
	}
	return ProviderAttemptResult{AttemptID: claim.ID, RawArtifact: append([]byte(nil), raw.Artifact...), TypedArtifact: typed, Validation: validationBytes, RawSHA256: rawDigest(raw.Artifact), TypedSHA256: typedDigest, ValidationSHA256: validationDigest, TranscriptSHA256: rawDigest([]byte(raw.Transcript)), Claim: claim}, nil
}

func (s *Store) ProviderLaunchIdentity(ctx context.Context, claim ProviderAttemptClaim) (contracts.ProviderLaunch, error) {
	var launch contracts.ProviderLaunch
	err := s.db.QueryRowContext(ctx, `SELECT process_pid,process_pgid,process_boot_identity,process_start_identity,worktree_path FROM provider_attempts a JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id WHERE a.id=? AND a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.attempt=? AND a.role=? AND a.leader_epoch=? AND a.runner_epoch=? AND a.expected_ticket_version=? AND a.binding_digest=? AND a.provider_lease_key=? AND i.request_digest=? AND a.state IN ('active','quarantined') AND a.launch_state='released'`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey, claim.RequestDigest).Scan(&launch.PID, &launch.PGID, &launch.BootIdentity, &launch.ProcessStartIdentity, &launch.Worktree)
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
	if claim.ID <= 0 || claim.Ref.Validate() != nil || claim.Phase == "" || !validProviderRole(claim.Role) || claim.Attempt <= 0 || claim.LeaseKey == "" || claim.BindingDigest == "" || claim.RequestDigest == "" || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ExpectedVersion == 0 || launch.PID <= 0 || launch.PGID <= 0 || launch.PID != launch.PGID || launch.BootIdentity == "" || launch.ProcessStartIdentity == "" || launch.Worktree == "" || claim.Worktree != launch.Worktree {
		return ErrProviderAttempt
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		row, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET process_pid=?,process_pgid=?,process_boot_identity=?,process_start_identity=?,launch_state='released' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND state='active' AND launch_state='launching' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND worktree_path=? AND EXISTS(SELECT 1 FROM provider_attempt_inputs WHERE provider_attempt_id=provider_attempts.id AND request_digest=?)`, launch.PID, launch.PGID, launch.BootIdentity, launch.ProcessStartIdentity, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey, launch.Worktree, claim.RequestDigest)
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
	return s.validateFinalReviewEvidence(ctx, s.db, ref, expectedVersion, fence, expectedHead, expectedProof)
}

type rowQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *Store) validateFinalReviewEvidence(ctx context.Context, query rowQueryer, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, expectedHead, expectedProof string) error {
	authority, err := s.finalReviewAuthorityFrom(ctx, query, ref, expectedVersion, fence)
	if err != nil || authority.Candidate.Snapshot.HeadSHA != expectedHead || authority.Candidate.Snapshot.ProofDigest != expectedProof {
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
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.project_id,a.ticket_id,a.phase,a.attempt,a.provider,a.model,a.family,a.version,a.role,a.state,a.outcome,a.usage_units,a.started_at,a.finished_at,a.qualification_id,a.binding_digest,a.provider_lease_key,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,a.supervisor_key,a.auth_digest,a.auth_mode,COALESCE(q.binary_digest,''),COALESCE(q.policy_digest,''),COALESCE(q.fixture_digest,''),COALESCE(i.request_digest,''),COALESCE(i.canonical_input,X'') FROM provider_attempts a LEFT JOIN provider_qualifications q ON q.id=a.qualification_id LEFT JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id WHERE a.channel=? AND a.state IN ('active','quarantined') ORDER BY a.id`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var out []ProviderAttempt
	for rows.Next() {
		var value ProviderAttempt
		var project, ticket, started, finished string
		var qualification sql.NullInt64
		if err := rows.Scan(&value.ID, &project, &ticket, &value.Phase, &value.Attempt, &value.Binding.Identity.Provider, &value.Binding.Identity.Model, &value.Binding.Identity.Family, &value.Binding.Identity.Version, &value.Role, &value.State, &value.Outcome, &value.UsageUnits, &started, &finished, &qualification, &value.BindingDigest, &value.LeaseKey, &value.LeaderEpoch, &value.RunnerEpoch, &value.ExpectedVersion, &value.Repository, &value.Worktree, &value.WorktreeIdentity, &value.BaseSHA, &value.SupervisorKey, &value.Binding.AuthDigest, &value.Binding.AuthMode, &value.Binding.BinaryDigest, &value.Binding.PolicyDigest, &value.Binding.FixtureDigest, &value.RequestDigest, &value.RequestPayload); err != nil {
			return nil, err
		}
		value.Ref = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
		if err := hydrateProviderAttemptInput(&value.ProviderAttemptClaim); err != nil {
			// Reconciliation leaves an ambiguous invalid claim visible as a
			// quarantine rather than silently dropping it. Do not hand a malformed
			// drain request to a supervisor; make startup surface the typed blocker.
			return nil, ErrProviderRecoveryBlocked
		}
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

// reconcileProviderAttemptInputs is the Go-level half of migration v28. It is
// intentionally idempotent and is run on every writable Store open, not just
// while applying v28: immutable SQLite blobs can be crafted outside normal
// Store admission, while SQL can only check their superficial JSON shape.
func (s *Store) reconcileProviderAttemptInputs(ctx context.Context) error {
	type row struct {
		claim              ProviderAttemptClaim
		state, launchState string
		pid, pgid          int64
		boot, start        string
		phaseExact         int
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, `SELECT
			a.id,a.channel,a.project_id,a.ticket_id,a.phase,a.attempt,a.provider,a.model,a.family,a.version,a.role,a.state,a.qualification_id,a.binding_digest,a.provider_lease_key,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,a.supervisor_key,a.auth_digest,a.auth_mode,COALESCE(q.binary_digest,''),COALESCE(q.policy_digest,''),COALESCE(q.fixture_digest,''),COALESCE(i.request_digest,''),COALESCE(i.canonical_input,X''),a.launch_state,a.process_pid,a.process_pgid,a.process_boot_identity,a.process_start_identity,
			CASE WHEN EXISTS(SELECT 1 FROM phase_runs p WHERE p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.phase=a.phase AND p.attempt=a.attempt AND p.state='active' AND p.provider=a.provider AND p.model=a.model AND p.family=a.family AND p.provider_version=a.version AND p.leader_epoch=a.leader_epoch AND p.runner_epoch=a.runner_epoch AND p.expected_ticket_version=a.expected_ticket_version AND p.worktree_identity=a.worktree_identity AND p.base_sha=a.base_sha) THEN 1 ELSE 0 END
			FROM provider_attempts a
			LEFT JOIN provider_qualifications q ON q.id=a.qualification_id
			LEFT JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id
			WHERE a.state IN ('active','quarantined') ORDER BY a.id`)
		if err != nil {
			return err
		}
		var values []row
		for rows.Next() {
			var value row
			var channel, project, ticket string
			var qualification sql.NullInt64
			if err := rows.Scan(&value.claim.ID, &channel, &project, &ticket, &value.claim.Phase, &value.claim.Attempt, &value.claim.Binding.Identity.Provider, &value.claim.Binding.Identity.Model, &value.claim.Binding.Identity.Family, &value.claim.Binding.Identity.Version, &value.claim.Role, &value.state, &qualification, &value.claim.BindingDigest, &value.claim.LeaseKey, &value.claim.LeaderEpoch, &value.claim.RunnerEpoch, &value.claim.ExpectedVersion, &value.claim.Repository, &value.claim.Worktree, &value.claim.WorktreeIdentity, &value.claim.BaseSHA, &value.claim.SupervisorKey, &value.claim.Binding.AuthDigest, &value.claim.Binding.AuthMode, &value.claim.Binding.BinaryDigest, &value.claim.Binding.PolicyDigest, &value.claim.Binding.FixtureDigest, &value.claim.RequestDigest, &value.claim.RequestPayload, &value.launchState, &value.pid, &value.pgid, &value.boot, &value.start, &value.phaseExact); err != nil {
				rows.Close()
				return err
			}
			value.claim.Ref = domain.TicketRef{Channel: domain.Channel(channel), Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
			if qualification.Valid {
				value.claim.QualificationID = qualification.Int64
			}
			values = append(values, value)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for _, value := range values {
			valid := value.phaseExact == 1 && hydrateProviderAttemptInput(&value.claim) == nil && validPersistedProviderAttemptClaim(value.claim)
			if valid {
				continue
			}
			definiteNoLaunch := value.launchState == "launching" && value.pid == 0 && value.pgid == 0 && value.boot == "" && value.start == ""
			if definiteNoLaunch {
				result, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='failed',outcome='legacy_unverifiable',finished_at=CASE WHEN finished_at='' THEN ? ELSE finished_at END,launch_state='legacy_unverifiable' WHERE id=? AND state IN ('active','quarantined') AND launch_state='launching' AND process_pid=0 AND process_pgid=0 AND process_boot_identity='' AND process_start_identity=''`, now, value.claim.ID)
				if err != nil {
					return err
				}
				if n, _ := result.RowsAffected(); n != 1 {
					return ErrProviderAttempt
				}
				if _, err := conn.ExecContext(ctx, `UPDATE phase_runs SET state='failed',completed_at=COALESCE(completed_at,?),outcome='legacy_unverifiable' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active'`, now, value.claim.Ref.Channel, value.claim.Ref.Project, value.claim.Ref.Ticket, value.claim.Phase, value.claim.Attempt); err != nil {
					return err
				}
				if _, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, value.claim.Ref.Channel, value.claim.LeaseKey, value.claim.Ref.Project, value.claim.Ref.Ticket, value.claim.RunnerEpoch); err != nil {
					return err
				}
				continue
			}
			// A released or otherwise ambiguous record might name a live writer.
			// Retain its exact lease and active phase; only a verified operator
			// recovery can clear this quarantine.
			if _, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='quarantined',outcome='undrained_recovery',launch_state='quarantined' WHERE id=? AND state IN ('active','quarantined')`, value.claim.ID); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReuseCurrentCompletedProviderAttempt atomically observes one exact completed
// result under the caller's live fence. It is intentionally independent of a
// provider binding so Coordinator can avoid even adapter Binding/Invocation
// work when another runner completed between phase read-side checks and launch.
func (s *Store) ReuseCurrentCompletedProviderAttempt(ctx context.Context, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence) (ProviderAttemptResultKey, bool, error) {
	if ref.Validate() != nil || !validProviderPhase(phase) || !validProviderRole(role) || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ProviderAttemptResultKey{}, false, ErrProviderAttempt
	}
	var key ProviderAttemptResultKey
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, ref, expected, fence); err != nil {
			return err
		}
		var ids []struct {
			id      int64
			attempt int
		}
		rows, err := conn.QueryContext(ctx, `SELECT r.provider_attempt_id,a.attempt FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id AND a.channel=r.channel AND a.project_id=r.project_id AND a.ticket_id=r.ticket_id AND a.phase=r.phase AND a.role=r.role AND a.attempt=r.attempt AND a.leader_epoch=r.leader_epoch AND a.runner_epoch=r.runner_epoch AND a.expected_ticket_version=r.expected_ticket_version JOIN phase_runs p ON p.channel=r.channel AND p.project_id=r.project_id AND p.ticket_id=r.ticket_id AND p.phase=r.phase AND p.attempt=r.attempt AND p.provider=r.provider AND p.model=r.model AND p.family=r.family AND p.provider_version=r.provider_version AND p.leader_epoch=r.leader_epoch AND p.runner_epoch=r.runner_epoch AND p.expected_ticket_version=r.expected_ticket_version AND p.worktree_identity=r.worktree_identity AND p.base_sha=r.base_sha WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.phase=? AND r.role=? AND r.expected_ticket_version=? AND r.leader_epoch=? AND r.runner_epoch=? AND a.state='completed' AND a.outcome='completed' AND p.state='completed' AND p.outcome='completed'`, ref.Channel, ref.Project, ref.Ticket, phase, role, expected, fence.LeaderEpoch, fence.RunnerEpoch)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var v struct {
				id      int64
				attempt int
			}
			if err := rows.Scan(&v.id, &v.attempt); err != nil {
				return err
			}
			ids = append(ids, v)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) > 1 {
			return ErrEvidenceConflict
		}
		if len(ids) == 1 {
			key = ProviderAttemptResultKey{AttemptID: ids[0].id, Ref: ref, Phase: phase, Attempt: ids[0].attempt}
		}
		return nil
	})
	if err != nil {
		return ProviderAttemptResultKey{}, false, err
	}
	return key, key.AttemptID != 0, nil
}

// PendingProviderRepair returns the exact authenticated invalid-artifact
// claim that requires one same-binding repair in the current phase entry. It
// is intentionally checked before Coordinator calls Provider.Binding so a
// daemon restart followed by missing/rotated credentials cannot silently
// turn the repair into a fallback or an active-ticket retry loop.
func (s *Store) PendingProviderRepair(ctx context.Context, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence) (ProviderAttemptClaim, bool, error) {
	if ref.Validate() != nil || !validProviderPhase(phase) || !validProviderRole(role) || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ProviderAttemptClaim{}, false, ErrProviderAttempt
	}
	var pending ProviderAttemptClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		claim, found, err := s.pendingProviderTerminalClaim(ctx, conn, ref, phase, role, expected, fence, "invalid_artifact", true, true)
		if err != nil {
			return err
		}
		if found {
			pending = claim
		}
		return nil
	})
	if err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	return pending, pending.ID > 0, nil
}

// PendingProviderResultIndeterminate returns the newest authenticated terminal
// claim in the current phase entry when its provider result was not durably
// established.  It remains valid across a fenced daemon recovery: the ticket
// fence is current, while the immutable claim retains its original launch
// fence and is checked against the recovered entry lineage.
func (s *Store) PendingProviderResultIndeterminate(ctx context.Context, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence) (ProviderAttemptClaim, bool, error) {
	if ref.Validate() != nil || !validProviderPhase(phase) || !validProviderRole(role) || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ProviderAttemptClaim{}, false, ErrProviderAttempt
	}
	var pending ProviderAttemptClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		claim, found, err := s.pendingProviderTerminalClaim(ctx, conn, ref, phase, role, expected, fence, "result_indeterminate", false, true)
		if err != nil {
			return err
		}
		if found {
			pending = claim
		}
		return nil
	})
	if err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	return pending, pending.ID > 0, nil
}

// PendingProviderRepairUnavailable returns the authoritative latest repair
// claim when a Store-issued same-binding repair was interrupted after its
// invalid-artifact predecessor.  It recognizes only the two bounded repair
// positions (attempt 2 or 4 in a phase entry), so an ordinary failed pair
// cannot be relabeled as a non-recoverable repair failure.
func (s *Store) PendingProviderRepairUnavailable(ctx context.Context, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence) (ProviderAttemptClaim, bool, error) {
	if ref.Validate() != nil || !validProviderPhase(phase) || !validProviderRole(role) || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ProviderAttemptClaim{}, false, ErrProviderAttempt
	}
	var pending ProviderAttemptClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		pair, found, err := s.pendingProviderRepairUnavailable(ctx, conn, ref, phase, role, expected, fence, true)
		if err != nil {
			return err
		}
		if found {
			pending = pair.Latest
		}
		return nil
	})
	if err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	return pending, pending.ID > 0, nil
}

type pendingProviderRepairPair struct {
	Latest  ProviderAttemptClaim
	Outcome string
}

type providerAttemptLifecycle struct {
	AttemptState, AttemptOutcome string
	LaunchState, FinishedAt      string
	PhaseState, PhaseOutcome     string
	PhaseFinished                string
	Results                      int
}

func loadProviderAttemptLifecycle(ctx context.Context, conn *sql.Conn, claim ProviderAttemptClaim) (providerAttemptLifecycle, error) {
	var lifecycle providerAttemptLifecycle
	if err := conn.QueryRowContext(ctx, `SELECT a.state,a.outcome,a.launch_state,a.finished_at,p.state,p.outcome,p.completed_at
		FROM provider_attempts a JOIN phase_runs p ON p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.phase=a.phase AND p.attempt=a.attempt AND p.provider=a.provider AND p.model=a.model AND p.family=a.family AND p.provider_version=a.version AND p.leader_epoch=a.leader_epoch AND p.runner_epoch=a.runner_epoch AND p.expected_ticket_version=a.expected_ticket_version AND p.worktree_identity=a.worktree_identity AND p.base_sha=a.base_sha
		WHERE a.id=?`, claim.ID).Scan(&lifecycle.AttemptState, &lifecycle.AttemptOutcome, &lifecycle.LaunchState, &lifecycle.FinishedAt, &lifecycle.PhaseState, &lifecycle.PhaseOutcome, &lifecycle.PhaseFinished); err != nil {
		return providerAttemptLifecycle{}, ErrEvidenceConflict
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&lifecycle.Results); err != nil {
		return providerAttemptLifecycle{}, err
	}
	return lifecycle, nil
}

func (lifecycle providerAttemptLifecycle) drainedTerminalWithoutResult() bool {
	return lifecycle.LaunchState == "drained" && lifecycle.FinishedAt != "" && lifecycle.PhaseFinished != "" && lifecycle.Results == 0
}

// pendingProviderRepairUnavailable is the private transaction form used by
// both admission and typed blocking.  The public form uses admission fencing;
// the blocker calls it after DrainExternalMutations, when only the current
// durable ticket fence remains admissible.
func (s *Store) pendingProviderRepairUnavailable(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence, admit bool) (pendingProviderRepairPair, bool, error) {
	assert := s.assertCurrentTicketFence
	if admit {
		assert = s.assertTicketFence
	}
	if err := assert(ctx, conn, ref, expected, fence); err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, conn, ref, phase, expected, fence.RunnerEpoch, fence.LeaderEpoch)
	if err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	if err := validateProviderPhaseEntryBindings(ctx, conn, ref, entry); err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	var entryRuns int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, phase, entry.Version).Scan(&entryRuns); err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	if entryRuns != 2 && entryRuns != 4 {
		return pendingProviderRepairPair{}, false, nil
	}
	rows, err := conn.QueryContext(ctx, `SELECT provider_attempt_id FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=? ORDER BY attempt DESC LIMIT 2`, ref.Channel, ref.Project, ref.Ticket, phase, entry.Version)
	if err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return pendingProviderRepairPair{}, false, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return pendingProviderRepairPair{}, false, err
	}
	if err := rows.Close(); err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	if len(ids) != 2 {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	latest, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, ids[0])
	if err != nil || latest.Ref != ref || latest.Phase != phase || latest.Role != role || latest.BindingDigest == "" || latest.BindingDigest != bindingDigest(latest.Binding) {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	prior, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, ids[1])
	if err != nil || prior.Ref != ref || prior.Phase != phase || prior.Role != role || prior.BindingDigest == "" || prior.BindingDigest != bindingDigest(prior.Binding) {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	if prior.Attempt+1 != latest.Attempt {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	priorLifecycle, err := loadProviderAttemptLifecycle(ctx, conn, prior)
	if err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	latestLifecycle, err := loadProviderAttemptLifecycle(ctx, conn, latest)
	if err != nil {
		return pendingProviderRepairPair{}, false, err
	}
	if priorLifecycle.AttemptState != priorLifecycle.PhaseState || priorLifecycle.AttemptOutcome != priorLifecycle.PhaseOutcome || latestLifecycle.AttemptState != latestLifecycle.PhaseState || latestLifecycle.AttemptOutcome != latestLifecycle.PhaseOutcome {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	// A fallback pair with no Store-issued repair marker is never an
	// interrupted repair. In particular, an invocation failure may be followed
	// by an ordinary fallback on a different qualified binding; that pair is
	// handled by bounded exhaustion, not this non-recoverable blocker.
	if latest.Input.Repair == nil {
		return pendingProviderRepairPair{}, false, nil
	}
	if prior.Binding != latest.Binding || prior.BindingDigest != latest.BindingDigest || prior.Role != latest.Role || prior.Ref != latest.Ref || prior.Phase != latest.Phase || !contracts.ValidProviderRepairContext(latest.Input.Repair) || latest.Input.Repair.PriorAttempt != prior.Attempt || latest.Input.Repair.PriorRequestDigest != prior.RequestDigest {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	priorInvalid := priorLifecycle.AttemptState == "failed" && priorLifecycle.AttemptOutcome == "invalid_artifact"
	if !priorInvalid {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	if !priorLifecycle.drainedTerminalWithoutResult() {
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	if !latestLifecycle.drainedTerminalWithoutResult() {
		// An active repair is not terminal evidence. Any other partial or
		// terminal-looking lifecycle is a corrupt mixed pair, not a candidate
		// for a fresh provider admission.
		if latestLifecycle.AttemptState == "active" && latestLifecycle.AttemptOutcome == "running" && latestLifecycle.PhaseState == "active" && latestLifecycle.PhaseOutcome == "running" {
			return pendingProviderRepairPair{}, false, nil
		}
		return pendingProviderRepairPair{}, false, ErrEvidenceConflict
	}
	switch {
	case latestLifecycle.AttemptState == "failed" && latestLifecycle.AttemptOutcome == "invocation_failed":
		return pendingProviderRepairPair{Latest: latest, Outcome: "invocation_failed"}, true, nil
	case latestLifecycle.AttemptState == "failed" && latestLifecycle.AttemptOutcome == "failed":
		// Compatibility for pre-v53 Store-issued repair attempts. Their generic
		// failed endpoint does not prove a complete provider result, so it must
		// remain non-recoverable rather than enter an ordinary exhaustion pause.
		return pendingProviderRepairPair{Latest: latest, Outcome: "failed"}, true, nil
	case latestLifecycle.AttemptState == "cancelled" && latestLifecycle.AttemptOutcome == "drained_recovery":
		return pendingProviderRepairPair{Latest: latest, Outcome: "drained_recovery"}, true, nil
	case latestLifecycle.AttemptState == "cancelled" && latestLifecycle.AttemptOutcome == "cancelled":
		// A signed, per-attempt cancellation leaves the ticket otherwise
		// current, but it is still the terminal endpoint of a Store-issued
		// repair. Reusing that repair window would make its exact outcome
		// ambiguous, so it is non-recoverable just like invocation failure.
		return pendingProviderRepairPair{Latest: latest, Outcome: "cancelled"}, true, nil
	default:
		// A fully terminal repair which produced another ordinary provider
		// failure is governed by the bounded provider-attempt exhaustion path,
		// not by the non-recoverable interrupted-repair blocker.
		return pendingProviderRepairPair{}, false, nil
	}
}

// pendingProviderTerminalClaim is the shared Store authority for terminal
// provider-result blockers.  The latest attempt is selected only from the
// current immutable phase entry, then rehydrated from its input and qualified
// binding before its matching phase lifecycle is accepted.  A later daemon
// recovery changes the ticket fence but never changes the terminal claim.
func (s *Store) pendingProviderTerminalClaim(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence, outcome string, repairWindow, admit bool) (ProviderAttemptClaim, bool, error) {
	if outcome != "invalid_artifact" && outcome != "result_indeterminate" {
		return ProviderAttemptClaim{}, false, ErrEvidenceConflict
	}
	assert := s.assertCurrentTicketFence
	if admit {
		assert = s.assertTicketFence
	}
	if err := assert(ctx, conn, ref, expected, fence); err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, conn, ref, phase, expected, fence.RunnerEpoch, fence.LeaderEpoch)
	if err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	if err := validateProviderPhaseEntryBindings(ctx, conn, ref, entry); err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	var entryRuns int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, phase, entry.Version).Scan(&entryRuns); err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	if entryRuns == 0 || (repairWindow && entryRuns != 1 && entryRuns != 3) {
		return ProviderAttemptClaim{}, false, nil
	}
	var id int64
	if err := conn.QueryRowContext(ctx, `SELECT provider_attempt_id FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=? ORDER BY attempt DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, phase, entry.Version).Scan(&id); err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	claim, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, id)
	if err != nil || claim.Ref != ref || claim.Phase != phase || claim.Role != role || claim.BindingDigest == "" || claim.BindingDigest != bindingDigest(claim.Binding) {
		return ProviderAttemptClaim{}, false, ErrEvidenceConflict
	}
	var attemptState, attemptOutcome, launchState, phaseState, phaseOutcome, finishedAt, phaseFinished string
	if err := conn.QueryRowContext(ctx, `SELECT a.state,a.outcome,a.launch_state,a.finished_at,p.state,p.outcome,p.completed_at
		FROM provider_attempts a JOIN phase_runs p ON p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.phase=a.phase AND p.attempt=a.attempt AND p.provider=a.provider AND p.model=a.model AND p.family=a.family AND p.provider_version=a.version AND p.leader_epoch=a.leader_epoch AND p.runner_epoch=a.runner_epoch AND p.expected_ticket_version=a.expected_ticket_version AND p.worktree_identity=a.worktree_identity AND p.base_sha=a.base_sha
		WHERE a.id=?`, claim.ID).Scan(&attemptState, &attemptOutcome, &launchState, &finishedAt, &phaseState, &phaseOutcome, &phaseFinished); err != nil {
		return ProviderAttemptClaim{}, false, ErrEvidenceConflict
	}
	attemptTerminal := attemptState == "failed" && attemptOutcome == outcome
	phaseTerminal := phaseState == "failed" && phaseOutcome == outcome
	if attemptTerminal != phaseTerminal {
		return ProviderAttemptClaim{}, false, ErrEvidenceConflict
	}
	if !attemptTerminal {
		return ProviderAttemptClaim{}, false, nil
	}
	var results int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&results); err != nil {
		return ProviderAttemptClaim{}, false, err
	}
	if launchState != "drained" || finishedAt == "" || phaseFinished == "" || results != 0 {
		return ProviderAttemptClaim{}, false, ErrEvidenceConflict
	}
	return claim, true, nil
}

func (s *Store) reuseCurrentCompletedProviderAttempt(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, role string, expected uint64, fence domain.Fence) (ProviderAttemptResultKey, bool, error) {
	var id int64
	var attempt int
	rows, err := conn.QueryContext(ctx, `SELECT r.provider_attempt_id,a.attempt FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id AND a.channel=r.channel AND a.project_id=r.project_id AND a.ticket_id=r.ticket_id AND a.phase=r.phase AND a.role=r.role AND a.attempt=r.attempt AND a.leader_epoch=r.leader_epoch AND a.runner_epoch=r.runner_epoch AND a.expected_ticket_version=r.expected_ticket_version JOIN phase_runs p ON p.channel=r.channel AND p.project_id=r.project_id AND p.ticket_id=r.ticket_id AND p.phase=r.phase AND p.attempt=r.attempt AND p.provider=r.provider AND p.model=r.model AND p.family=r.family AND p.provider_version=r.provider_version AND p.leader_epoch=r.leader_epoch AND p.runner_epoch=r.runner_epoch AND p.expected_ticket_version=r.expected_ticket_version AND p.worktree_identity=r.worktree_identity AND p.base_sha=r.base_sha WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.phase=? AND r.role=? AND r.expected_ticket_version=? AND r.leader_epoch=? AND r.runner_epoch=? AND a.state='completed' AND a.outcome='completed' AND p.state='completed' AND p.outcome='completed'`, ref.Channel, ref.Project, ref.Ticket, phase, role, expected, fence.LeaderEpoch, fence.RunnerEpoch)
	if err != nil {
		return ProviderAttemptResultKey{}, false, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		if err := rows.Scan(&id, &attempt); err != nil {
			return ProviderAttemptResultKey{}, false, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return ProviderAttemptResultKey{}, false, err
	}
	if count > 1 {
		return ProviderAttemptResultKey{}, false, ErrEvidenceConflict
	}
	if count == 0 {
		return ProviderAttemptResultKey{}, false, nil
	}
	return ProviderAttemptResultKey{AttemptID: id, Ref: ref, Phase: phase, Attempt: attempt}, true, nil
}

func (s *Store) BeginProviderAttempt(ctx context.Context, r ProviderAttemptRequest) (ProviderAttemptClaim, error) {
	// Repair context is Store authority, not a caller launch parameter. Clear
	// any proposed value before admission; a qualifying retry below receives a
	// fresh marker derived from the immutable prior attempt instead.
	r.Input.Repair = nil
	if r.Ref.Validate() != nil || !validProviderPhase(r.Phase) || !validProviderRole(r.Role) || r.ExpectedVersion == 0 || r.Fence.LeaderEpoch == 0 || r.Fence.RunnerEpoch == 0 || r.Capacity < 1 || r.Capacity > 16 || r.At.IsZero() || !validRuntimeBinding(r.Binding) || !validProviderAttemptInput(r) {
		return ProviderAttemptClaim{}, ErrProviderAttempt
	}
	var claim ProviderAttemptClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, r.Ref, r.ExpectedVersion, r.Fence); err != nil {
			return err
		}
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
		var projectPath, durablePath, durableIdentity, durableBase string
		if err := conn.QueryRowContext(ctx, `SELECT p.canonical_path,w.path,w.identity_json,w.base_sha FROM projects p JOIN worktrees w ON w.channel=p.channel AND w.project_id=p.id AND w.ticket_id=? WHERE p.channel=? AND p.id=?`, r.Ref.Ticket, r.Ref.Channel, r.Ref.Project).Scan(&projectPath, &durablePath, &durableIdentity, &durableBase); err != nil {
			return ErrEvidenceConflict
		}
		if len(r.SupervisorKey) == 0 {
			return ErrProviderAttempt
		}
		if projectPath != r.Repository || durablePath != r.Worktree || durableIdentity != r.WorktreeIdentity || durableBase != r.BaseSHA {
			return ErrEvidenceConflict
		}
		if _, reusable, reuseErr := s.reuseCurrentCompletedProviderAttempt(ctx, conn, r.Ref, r.Phase, r.Role, r.ExpectedVersion, r.Fence); reuseErr != nil {
			return reuseErr
		} else if reusable {
			return ErrProviderAttemptReusable
		}
		entry, err := loadCurrentProviderPhaseEntry(ctx, conn, r.Ref, r.Phase, version, runner, r.Fence.LeaderEpoch)
		if err != nil {
			return err
		}
		if err := validateProviderPhaseEntryBindings(ctx, conn, r.Ref, entry); err != nil {
			return err
		}
		// Terminal result indeterminacy and an interrupted Store-issued repair
		// are non-recoverable in v1.  Refuse them in the same IMMEDIATE
		// transaction that would otherwise allocate a new provider lease and
		// attempt number, before generic budget or attempt-limit admission.
		if _, found, err := s.pendingProviderTerminalClaim(ctx, conn, r.Ref, r.Phase, r.Role, r.ExpectedVersion, r.Fence, "result_indeterminate", false, true); err != nil {
			return err
		} else if found {
			return ErrProviderResultIndeterminate
		}
		if _, found, err := s.pendingProviderRepairUnavailable(ctx, conn, r.Ref, r.Phase, r.Role, r.ExpectedVersion, r.Fence, true); err != nil {
			return err
		} else if found {
			return ErrProviderRepairUnavailable
		}
		// A Store-authenticated budget rejection is terminal for this immutable
		// ticket/phase entry.  In particular, a daemon crash after the drained
		// rejection but before the ticket blocker is recorded must not admit a
		// second paid provider run.
		var budgetRejected int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries pe
			JOIN provider_attempts a ON a.id=pe.provider_attempt_id
			JOIN phase_runs pr ON pr.channel=pe.channel AND pr.project_id=pe.project_id AND pr.ticket_id=pe.ticket_id AND pr.phase=pe.phase AND pr.attempt=pe.attempt
			WHERE pe.channel=? AND pe.project_id=? AND pe.ticket_id=? AND pe.phase=? AND pe.entry_ticket_version=?
			AND a.state='failed' AND a.outcome='budget_exhausted' AND a.launch_state='drained'
			AND pr.state='failed' AND pr.outcome='budget_exhausted'`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, entry.Version).Scan(&budgetRejected); err != nil {
			return err
		}
		if budgetRejected > 1 {
			return ErrEvidenceConflict
		}
		if budgetRejected == 1 {
			return ErrBudgetExhausted
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
		var activeCommandWriter int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE repository_path=? AND state IN ('active','quarantined')`, r.Repository).Scan(&activeCommandWriter); err != nil {
			return err
		}
		if activeCommandWriter != 0 {
			return ErrProviderAttempt
		}
		if r.Phase == domain.PhaseReview && r.Role == "reviewer" {
			if err := s.validateFinalReviewEvidence(ctx, conn, r.Ref, r.ExpectedVersion, r.Fence, r.ExpectedHead, r.ExpectedProof); err != nil {
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
		var entryRuns, prior int
		if err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, entry.Version).Scan(&entryRuns); err != nil {
			return err
		}
		if err = conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(attempt),0) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase).Scan(&prior); err != nil {
			return err
		}
		epoch, hasRetryEpoch, err := loadProviderRetryEpoch(ctx, conn, r.Ref, r.Phase)
		if err != nil {
			return err
		}
		if hasRetryEpoch && epoch.EntryVersion != entry.Version {
			hasRetryEpoch = false
		}
		limit := 2
		if hasRetryEpoch {
			limit = 4
		}
		if entryRuns >= limit {
			return ErrProviderAttemptLimit
		}
		prior++ // globally monotonic per ticket+phase, independent of entry.
		launchInput := r.Input
		repair, err := providerRepairContextForRetry(ctx, conn, r, entry, entryRuns)
		if err != nil {
			return err
		}
		launchInput.Repair = repair
		launchInput.Ticket, launchInput.Phase = r.Ref, r.Phase
		launchInput.Provider, launchInput.AuthMode = r.Binding.Identity, r.Binding.AuthMode
		launchInput.Attempt, launchInput.LeaderEpoch, launchInput.RunnerEpoch, launchInput.ExpectedVersion = prior, r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion
		launchInput.Repository, launchInput.Worktree, launchInput.WorktreeIdentity, launchInput.BaseSHA = r.Repository, r.Worktree, r.WorktreeIdentity, r.BaseSHA
		payload, requestDigest, err := contracts.CanonicalPhaseInput(launchInput)
		if err != nil || len(payload) == 0 || len(payload) > 2<<20 {
			return ErrProviderAttempt
		}
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
		row, err := conn.ExecContext(ctx, `INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,role,state,usage_units,started_at,finished_at,qualification_id,binding_digest,provider_lease_key,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,supervisor_key,auth_digest,auth_mode,launch_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, prior, r.Binding.Identity.Provider, r.Binding.Identity.Model, r.Binding.Identity.Family, r.Binding.Identity.Version, outcome, r.Role, "active", 0, r.At.UTC().Format(time.RFC3339Nano), "", qualification.ID, bindingDigest, lease.ScopeKey, r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion, r.Repository, r.Worktree, r.WorktreeIdentity, r.BaseSHA, r.SupervisorKey, r.Binding.AuthDigest, r.Binding.AuthMode, "launching")
		if err != nil {
			return err
		}
		id, err := row.LastInsertId()
		if err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO provider_attempt_inputs(provider_attempt_id,request_digest,canonical_input,created_at) VALUES(?,?,?,?)`, id, requestDigest, payload, r.At.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO provider_phase_attempt_entries(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,entry_ticket_version,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, id, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, r.Role, prior, entry.Version, r.At.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		launchInput.RequestDigest = requestDigest
		claim = ProviderAttemptClaim{ID: id, Ref: r.Ref, Phase: r.Phase, Role: r.Role, Attempt: prior, Binding: r.Binding, QualificationID: qualification.ID, LeaseKey: lease.ScopeKey, BindingDigest: bindingDigest, LeaderEpoch: r.Fence.LeaderEpoch, RunnerEpoch: r.Fence.RunnerEpoch, ExpectedVersion: r.ExpectedVersion, Repository: r.Repository, Worktree: r.Worktree, WorktreeIdentity: r.WorktreeIdentity, BaseSHA: r.BaseSHA, SupervisorKey: append([]byte(nil), r.SupervisorKey...), Input: launchInput, RequestDigest: requestDigest, RequestPayload: append([]byte(nil), payload...)}
		return nil
	})
	return claim, err
}

// providerRepairContextForRetry derives the sole repair marker from the
// previous attempt in this exact immutable phase entry. It intentionally does
// not read provider output: the next provider needs only the fact that a
// schema-invalid response was drained, not any potentially unsafe content.
func providerRepairContextForRetry(ctx context.Context, conn *sql.Conn, r ProviderAttemptRequest, entry providerPhaseEntry, entryRuns int) (*contracts.ProviderRepairContext, error) {
	// Each Store-owned window contains an ordinary attempt followed by at most
	// one repair: attempts 1->2 initially and 3->4 only after an explicit
	// operator-approved retry epoch.
	if entryRuns != 1 && entryRuns != 3 {
		return nil, nil
	}
	var priorAttempt int
	var priorDigest, priorRole, priorBinding, attemptState, attemptOutcome, launchState, runState, runOutcome string
	err := conn.QueryRowContext(ctx, `SELECT a.attempt,i.request_digest,a.role,a.binding_digest,a.state,a.outcome,a.launch_state,pr.state,pr.outcome
		FROM provider_phase_attempt_entries pe
		JOIN provider_attempts a ON a.id=pe.provider_attempt_id
		JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id
		JOIN phase_runs pr ON pr.channel=pe.channel AND pr.project_id=pe.project_id AND pr.ticket_id=pe.ticket_id AND pr.phase=pe.phase AND pr.attempt=pe.attempt
		WHERE pe.channel=? AND pe.project_id=? AND pe.ticket_id=? AND pe.phase=? AND pe.entry_ticket_version=?
		ORDER BY a.attempt DESC LIMIT 1`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket, r.Phase, entry.Version).Scan(&priorAttempt, &priorDigest, &priorRole, &priorBinding, &attemptState, &attemptOutcome, &launchState, &runState, &runOutcome)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrEvidenceConflict
	}
	if err != nil {
		return nil, err
	}
	attemptInvalid := attemptState == "failed" && attemptOutcome == "invalid_artifact"
	runInvalid := runState == "failed" && runOutcome == "invalid_artifact"
	if attemptInvalid != runInvalid {
		return nil, ErrEvidenceConflict
	}
	if !attemptInvalid {
		return nil, nil
	}
	// Once an invalid artifact is durable, its one automatic repair must use
	// the exact same role and full runtime binding. A changed executable,
	// policy, fixture, or authentication binding cannot silently become an
	// ordinary second launch.
	if launchState != "drained" || priorRole != r.Role || priorBinding == "" {
		return nil, ErrEvidenceConflict
	}
	if priorBinding != bindingDigest(r.Binding) {
		return nil, ErrProviderRepairUnavailable
	}
	context := &contracts.ProviderRepairContext{PriorAttempt: priorAttempt, PriorRequestDigest: priorDigest}
	if !contracts.ValidProviderRepairContext(context) {
		return nil, ErrEvidenceConflict
	}
	return context, nil
}

func (s *Store) FinishProviderAttempt(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, state, outcome string, usage int64, finished time.Time) error {
	if state == "completed" || outcome == "completed" {
		return ErrProviderAttempt
	}
	return s.finishProviderAttempt(ctx, claim, proof, expected, fence, state, outcome, usage, finished, nil, nil)
}

// FinishProviderAttemptWithArtifactFailure records the only durable detail for
// a repairable invalid artifact. The reason is a closed enum and is written in
// the same transaction as the drained failed attempt, phase run, and lease
// release. It never accepts or persists provider-controlled diagnostic text.
func (s *Store) FinishProviderAttemptWithArtifactFailure(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, reason contracts.ArtifactFailureReason, usage int64, finished time.Time) error {
	if !contracts.ValidArtifactFailureReason(reason) {
		return ErrProviderAttempt
	}
	failure, err := providerArtifactFailureForClaim(claim, reason, finished)
	if err != nil {
		return err
	}
	return s.finishProviderAttempt(ctx, claim, proof, expected, fence, "failed", contracts.PhaseResultInvalidArtifact, usage, finished, nil, &failure)
}

// RetireProviderAttemptAfterControlInvalidation is the narrow terminal path
// for an attempt handed to the provider supervisor, drained by that exact
// supervisor, and then deprived of its old ticket fence by an operator
// pause/take/cancel. Provider output observed after that revocation is never
// completion evidence. The exact old attempt and phase are cancelled instead
// so ticket-level control can prove that no writer remains.
func (s *Store) RetireProviderAttemptAfterControlInvalidation(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, finished time.Time) error {
	if claim.ID <= 0 || finished.IsZero() {
		return ErrProviderAttempt
	}
	if !contracts.VerifyDrainProof(claim.SupervisorKey, drainRequestForClaim(claim), proof) {
		return ErrProviderDrain
	}
	return s.retireProviderAttemptAfterControlInvalidation(ctx, claim, finished, false)
}

// RetireUnlaunchedProviderAttemptAfterControlInvalidation is the matching
// pre-launch path. It is admissible only while Store still proves that no PID
// or process group was ever recorded for the exact claim.
func (s *Store) RetireUnlaunchedProviderAttemptAfterControlInvalidation(ctx context.Context, claim ProviderAttemptClaim, finished time.Time) error {
	if claim.ID <= 0 || finished.IsZero() {
		return ErrProviderAttempt
	}
	return s.retireProviderAttemptAfterControlInvalidation(ctx, claim, finished, true)
}

func (s *Store) retireProviderAttemptAfterControlInvalidation(ctx context.Context, claim ProviderAttemptClaim, finished time.Time, requireUnlaunched bool) error {
	if claim.ExpectedVersion == ^uint64(0) || claim.RunnerEpoch == ^uint64(0) {
		return ErrStaleFence
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var resume sql.NullString
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.resume_state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&state, &resume, &version, &runner, &leader); err != nil {
			return err
		}
		if leader != claim.LeaderEpoch || version != claim.ExpectedVersion+1 || runner != claim.RunnerEpoch+1 || (state != domain.StateStopping && state != domain.StateCancelling) {
			return ErrStaleFence
		}

		var eventCount int
		var trigger string
		var from, to domain.State
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(trigger),''),COALESCE(MAX(from_state),''),COALESCE(MAX(to_state),'') FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, version).Scan(&eventCount, &trigger, &from, &to); err != nil {
			return err
		}
		expectedTrigger := "operator_pause_or_take"
		if state == domain.StateCancelling {
			expectedTrigger = "operator_cancel"
		}
		if eventCount != 1 || trigger != expectedTrigger || to != state || !providerAdmissionState(from, claim.Phase, claim.Role) {
			return ErrStaleFence
		}
		if state == domain.StateStopping {
			if !resume.Valid || domain.State(resume.String) != from {
				return ErrStaleFence
			}
		} else if resume.Valid {
			return ErrStaleFence
		}

		control, err := runtimeControlFrom(ctx, conn, claim.Ref)
		current := mutationRevocation{version: version, leader: leader, runner: runner}
		if err != nil || control.state != "sealed" || control.stop != current || control.authority != current {
			return ErrStaleFence
		}
		persisted, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, claim.ID)
		if err != nil || !sameImmutableProviderAttemptClaim(claim, persisted) || claim.Input.RequestDigest != persisted.Input.RequestDigest {
			return ErrProviderAttempt
		}
		var persistedState, launchState string
		var pid, pgid int
		var bootIdentity, startIdentity string
		if err := conn.QueryRowContext(ctx, `SELECT state,launch_state,process_pid,process_pgid,process_boot_identity,process_start_identity FROM provider_attempts WHERE id=?`, claim.ID).Scan(&persistedState, &launchState, &pid, &pgid, &bootIdentity, &startIdentity); err != nil {
			return err
		}
		if persistedState != "active" {
			return ErrStaleFence
		}
		if requireUnlaunched && (launchState != "launching" || pid != 0 || pgid != 0 || bootIdentity != "" || startIdentity != "") {
			return ErrProviderDrain
		}

		updated, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='cancelled',outcome='cancelled',usage_units=0,finished_at=?,launch_state='drained' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND state='active'`, finished.UTC().Format(time.RFC3339Nano), claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		updated, err = conn.ExecContext(ctx, `UPDATE phase_runs SET state='cancelled',completed_at=?,outcome='cancelled' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND provider=? AND model=? AND family=? AND provider_version=?`, finished.UTC().Format(time.RFC3339Nano), claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		deleted, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=? AND EXISTS(SELECT 1 FROM provider_attempts a WHERE a.id=? AND a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.attempt=? AND a.role=? AND a.binding_digest=? AND a.provider_lease_key=? AND a.leader_epoch=? AND a.runner_epoch=? AND a.expected_ticket_version=? AND a.state='cancelled' AND a.outcome='cancelled')`, claim.Ref.Channel, claim.LeaseKey, claim.Ref.Project, claim.Ref.Ticket, claim.RunnerEpoch, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.BindingDigest, claim.LeaseKey, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion)
		if err != nil {
			return err
		}
		if n, _ := deleted.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

// CompleteProviderAttemptSuccess is the only success terminal path. It writes
// immutable evidence, marks both lifecycle rows completed, and releases the
// exact lease in one SQLite transaction.
func (s *Store) CompleteProviderAttemptSuccess(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, raw contracts.PhaseResult, validation phaseartifact.Validation, finished time.Time) (ProviderAttemptResult, error) {
	if !raw.UsageTrusted || raw.UsageUnits < 0 {
		return ProviderAttemptResult{}, ErrProviderAttempt
	}
	immutable, err := loadAuthenticatedProviderAttemptClaim(ctx, s.db, claim.ID)
	if err != nil || !sameImmutableProviderAttemptClaim(claim, immutable) || claim.Input.RequestDigest != immutable.Input.RequestDigest {
		return ProviderAttemptResult{}, ErrProviderAttempt
	}
	parsed, err := phaseartifact.Parse(claim.Phase, raw, validation)
	if err != nil {
		return ProviderAttemptResult{}, ErrProviderAttempt
	}
	if err := phaseartifact.ValidateMutationPaths(parsed, raw.ChangedFiles, immutable.Input.AllowedPaths); err != nil {
		return ProviderAttemptResult{}, ErrProviderAttempt
	}
	result, err := makeProviderAttemptResult(claim, raw, validation, parsed)
	if err != nil {
		return ProviderAttemptResult{}, ErrProviderAttempt
	}
	if prior, _, loadErr := s.LoadProviderAttemptResult(ctx, claim, expected, fence); loadErr == nil {
		if sameProviderAttemptResult(prior, result) {
			return prior, nil
		}
		return ProviderAttemptResult{}, ErrProviderAttempt
	}
	err = s.finishProviderAttempt(ctx, claim, proof, expected, fence, "completed", "completed", raw.UsageUnits, finished, &result, nil)
	if err != nil {
		// A concurrent exact replay may have committed between the initial
		// lookup and our transaction.  Re-read instead of weakening the update
		// fence; a different durable value always fails closed.
		if prior, _, loadErr := s.LoadProviderAttemptResult(ctx, claim, expected, fence); loadErr == nil && sameProviderAttemptResult(prior, result) {
			return prior, nil
		}
	}
	return result, err
}

// immutableProviderAttemptInput rehydrates the Store-issued launch input
// before result admission. Callers hold a copy of ProviderAttemptClaim and
// must not be able to widen its allowed paths after BeginProviderAttempt.
func loadAuthenticatedProviderAttemptClaim(ctx context.Context, query rowQueryer, id int64) (ProviderAttemptClaim, error) {
	var value ProviderAttemptClaim
	var channel, project, ticket string
	var qualification sql.NullInt64
	err := query.QueryRowContext(ctx, `SELECT a.channel,a.project_id,a.ticket_id,a.phase,a.attempt,a.provider,a.model,a.family,a.version,a.role,a.qualification_id,a.binding_digest,a.provider_lease_key,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,a.supervisor_key,a.auth_digest,a.auth_mode,q.binary_digest,q.policy_digest,q.fixture_digest,i.request_digest,i.canonical_input FROM provider_attempts a JOIN provider_qualifications q ON q.id=a.qualification_id AND q.channel=a.channel AND q.provider=a.provider AND q.model=a.model AND q.family=a.family AND q.provider_version=a.version AND q.profile IN ('qualified_guarded','autonomous_eligible') AND (a.provider<>'codex' OR (q.auth_digest=a.auth_digest AND q.auth_mode=a.auth_mode AND length(q.probe_digest)=64 AND q.attested_leader_epoch>0 AND length(q.attestation_signature)=64)) JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id WHERE a.id=?`, id).Scan(&channel, &project, &ticket, &value.Phase, &value.Attempt, &value.Binding.Identity.Provider, &value.Binding.Identity.Model, &value.Binding.Identity.Family, &value.Binding.Identity.Version, &value.Role, &qualification, &value.BindingDigest, &value.LeaseKey, &value.LeaderEpoch, &value.RunnerEpoch, &value.ExpectedVersion, &value.Repository, &value.Worktree, &value.WorktreeIdentity, &value.BaseSHA, &value.SupervisorKey, &value.Binding.AuthDigest, &value.Binding.AuthMode, &value.Binding.BinaryDigest, &value.Binding.PolicyDigest, &value.Binding.FixtureDigest, &value.RequestDigest, &value.RequestPayload)
	if err != nil {
		return ProviderAttemptClaim{}, err
	}
	value.ID, value.Ref = id, domain.TicketRef{Channel: domain.Channel(channel), Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
	if qualification.Valid {
		value.QualificationID = qualification.Int64
	}
	if !qualification.Valid {
		return ProviderAttemptClaim{}, ErrProviderAttempt
	}
	if err := hydrateProviderAttemptInput(&value); err != nil {
		return ProviderAttemptClaim{}, ErrProviderAttempt
	}
	if !validPersistedProviderAttemptClaim(value) {
		return ProviderAttemptClaim{}, ErrProviderAttempt
	}
	return value, nil
}

func sameImmutableProviderAttemptClaim(a, b ProviderAttemptClaim) bool {
	return a.ID == b.ID && a.Ref == b.Ref && a.Phase == b.Phase && a.Role == b.Role && a.Attempt == b.Attempt && a.Binding == b.Binding && a.QualificationID == b.QualificationID && a.LeaseKey == b.LeaseKey && a.BindingDigest == b.BindingDigest && a.LeaderEpoch == b.LeaderEpoch && a.RunnerEpoch == b.RunnerEpoch && a.ExpectedVersion == b.ExpectedVersion && a.Repository == b.Repository && a.Worktree == b.Worktree && a.WorktreeIdentity == b.WorktreeIdentity && a.BaseSHA == b.BaseSHA && bytes.Equal(a.SupervisorKey, b.SupervisorKey) && a.RequestDigest == b.RequestDigest && bytes.Equal(a.RequestPayload, b.RequestPayload) && reflect.DeepEqual(a.Input, b.Input)
}

func resultClaimMatchesSource(result, source ProviderAttemptClaim) bool {
	return result.ID == source.ID && result.Ref == source.Ref && result.Phase == source.Phase && result.Role == source.Role && result.Attempt == source.Attempt && result.Binding.Identity == source.Binding.Identity && result.RequestDigest == source.RequestDigest && result.LeaderEpoch == source.LeaderEpoch && result.RunnerEpoch == source.RunnerEpoch && result.ExpectedVersion == source.ExpectedVersion && result.Repository == source.Repository && result.Worktree == source.Worktree && result.WorktreeIdentity == source.WorktreeIdentity && result.BaseSHA == source.BaseSHA
}

func sameProviderAttemptResult(a, b ProviderAttemptResult) bool {
	return a.AttemptID == b.AttemptID && a.RawSHA256 == b.RawSHA256 && a.TypedSHA256 == b.TypedSHA256 && a.ValidationSHA256 == b.ValidationSHA256 && a.TranscriptSHA256 == b.TranscriptSHA256 && bytes.Equal(a.RawArtifact, b.RawArtifact) && bytes.Equal(a.TypedArtifact, b.TypedArtifact) && bytes.Equal(a.Validation, b.Validation)
}

func (s *Store) finishProviderAttempt(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, state, outcome string, usage int64, finished time.Time, result *ProviderAttemptResult, artifactFailure *ProviderArtifactFailure) error {
	if claim.ID <= 0 || claim.ExpectedVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || !validAttemptState(state) || !safeOutcome(outcome) || usage < 0 || finished.IsZero() {
		return ErrProviderAttempt
	}
	if result != nil && (state != "completed" || outcome != "completed") {
		return ErrProviderAttempt
	}
	if artifactFailure != nil {
		if state != "failed" || outcome != contracts.PhaseResultInvalidArtifact || artifactFailure.AttemptID != claim.ID || artifactFailure.Ref != claim.Ref || artifactFailure.Phase != claim.Phase || artifactFailure.Role != claim.Role || artifactFailure.Attempt != claim.Attempt || artifactFailure.RequestDigest != claim.RequestDigest || artifactFailure.LeaderEpoch != claim.LeaderEpoch || artifactFailure.RunnerEpoch != claim.RunnerEpoch || artifactFailure.ExpectedVersion != claim.ExpectedVersion || !artifactFailure.CreatedAt.Equal(finished.UTC()) {
			return ErrProviderAttempt
		}
		digest, err := providerArtifactFailureDigest(*artifactFailure)
		if err != nil || digest != artifactFailure.Digest {
			return ErrProviderAttempt
		}
	}
	if claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch {
		return ErrStaleFence
	}
	if !contracts.VerifyDrainProof(claim.SupervisorKey, drainRequestForClaim(claim), proof) {
		return ErrProviderDrain
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var persistedState string
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
		persisted, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, claim.ID)
		if err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&persistedState); err != nil {
			return err
		}
		if persistedState != "active" || !sameImmutableProviderAttemptClaim(claim, persisted) || claim.Input.RequestDigest != persisted.Input.RequestDigest || persisted.BindingDigest == "" || persisted.BindingDigest != bindingDigest(persisted.Binding) {
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
		if maxDuration <= 0 || !finished.Before(createdAt.Add(time.Duration(maxDuration))) {
			return ErrBudgetExhausted
		}
		if result != nil {
			if result.AttemptID != claim.ID || result.Claim.ID != claim.ID || result.Claim.Ref != claim.Ref || result.Claim.Phase != claim.Phase || result.Claim.Role != claim.Role || result.Claim.Attempt != claim.Attempt || result.Claim.Binding.Identity != claim.Binding.Identity || result.Claim.RequestDigest != claim.RequestDigest || result.Claim.LeaderEpoch != claim.LeaderEpoch || result.Claim.RunnerEpoch != claim.RunnerEpoch || result.Claim.ExpectedVersion != claim.ExpectedVersion || result.Claim.Repository != claim.Repository || result.Claim.Worktree != claim.Worktree || result.Claim.WorktreeIdentity != claim.WorktreeIdentity || result.Claim.BaseSHA != claim.BaseSHA || !validSHA256(result.RawSHA256) || !validSHA256(result.TypedSHA256) || !validSHA256(result.ValidationSHA256) || !validSHA256(result.TranscriptSHA256) || rawDigest(result.RawArtifact) != result.RawSHA256 || rawDigest(result.TypedArtifact) != result.TypedSHA256 || rawDigest(result.Validation) != result.ValidationSHA256 {
				return ErrProviderAttempt
			}
			if artifact, err := phaseartifact.DecodeCanonicalTypedArtifact(result.TypedArtifact); err != nil || artifact.Phase != claim.Phase || artifact.Provider != claim.Binding.Identity {
				return ErrProviderAttempt
			}
			validation, err := phaseartifact.DecodeCanonicalValidation(result.Validation)
			if err != nil {
				return ErrProviderAttempt
			}
			parsed, err := phaseartifact.Parse(claim.Phase, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: result.RawArtifact}, validation)
			canonical, _, canonicalErr := phaseartifact.CanonicalTypedArtifact(parsed)
			if err != nil || canonicalErr != nil || !bytes.Equal(canonical, result.TypedArtifact) {
				return ErrProviderAttempt
			}
			_, err = conn.ExecContext(ctx, `INSERT INTO provider_attempt_results(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,provider,model,family,provider_version,request_digest,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,raw_artifact,raw_sha256,typed_artifact,typed_sha256,validation,validation_sha256,transcript_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Role, claim.Attempt, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, claim.RequestDigest, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Repository, claim.Worktree, claim.WorktreeIdentity, claim.BaseSHA, result.RawArtifact, result.RawSHA256, result.TypedArtifact, result.TypedSHA256, result.Validation, result.ValidationSHA256, result.TranscriptSHA256, finished.UTC().Format(time.RFC3339Nano))
			if err != nil {
				return err
			}
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
		if artifactFailure != nil {
			_, err = conn.ExecContext(ctx, `INSERT INTO provider_artifact_failures(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,request_digest,leader_epoch,runner_epoch,expected_ticket_version,failure_reason,failure_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, artifactFailure.AttemptID, artifactFailure.Ref.Channel, artifactFailure.Ref.Project, artifactFailure.Ref.Ticket, artifactFailure.Phase, artifactFailure.Role, artifactFailure.Attempt, artifactFailure.RequestDigest, artifactFailure.LeaderEpoch, artifactFailure.RunnerEpoch, artifactFailure.ExpectedVersion, artifactFailure.Reason, artifactFailure.Digest, artifactFailure.CreatedAt.UTC().Format(time.RFC3339Nano))
			if err != nil {
				return err
			}
		}
		row, err = conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND scope='provider' AND scope_key=? AND project_id=? AND ticket_id=? AND runner_epoch=? AND EXISTS(SELECT 1 FROM provider_attempts a WHERE a.id=? AND a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.attempt=? AND a.role=? AND a.provider=? AND a.model=? AND a.family=? AND a.version=? AND a.binding_digest=? AND a.provider_lease_key=? AND a.leader_epoch=? AND a.runner_epoch=? AND a.expected_ticket_version=? AND a.repository_path=? AND a.worktree_path=? AND a.worktree_identity=? AND a.base_sha=? AND a.auth_digest=? AND a.auth_mode=?)`, claim.Ref.Channel, claim.LeaseKey, claim.Ref.Project, claim.Ref.Ticket, claim.RunnerEpoch, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, claim.BindingDigest, claim.LeaseKey, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Repository, claim.Worktree, claim.WorktreeIdentity, claim.BaseSHA, claim.Binding.AuthDigest, claim.Binding.AuthMode)
		if err != nil {
			return err
		}
		if n, _ := row.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		return nil
	})
}

// LoadProviderAttemptResult verifies immutable bindings and every canonical
// representation before re-running semantic Parse. It is safe for restart
// consumers: a model is never invoked here.
func (s *Store) LoadProviderAttemptResult(ctx context.Context, claim ProviderAttemptClaim, expected uint64, fence domain.Fence) (ProviderAttemptResult, phaseartifact.Parsed, error) {
	if claim.ID <= 0 || claim.ExpectedVersion != expected || claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
	}
	candidateRepairAuthority := false
	if claim.Phase == domain.PhaseBuild && claim.Role == "builder" {
		if _, repairErr := s.candidateRepairBuildContextAt(ctx, s.db, claim.Ref, expected, fence); repairErr == nil {
			candidateRepairAuthority = true
		} else if !errors.Is(repairErr, ErrNotFound) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
		}
	}
	if !candidateRepairAuthority {
		if err := validateRunnerRecoveryAuthority(ctx, s.db, claim.Ref, expected, fence); err != nil {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
		}
	}
	out, c, err := loadProviderAttemptResultRow(ctx, s.db, claim.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
		}
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, normalizeBusy(ctx, err)
	}
	source, sourceErr := loadAuthenticatedProviderAttemptClaim(ctx, s.db, out.AttemptID)
	if sourceErr != nil || !resultClaimMatchesSource(c, source) {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	c, out.Claim = source, source
	attemptState, attemptOutcome, phaseState, phaseOutcome, err := loadProviderAttemptResultLifecycle(ctx, s.db, c)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
		}
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, normalizeBusy(ctx, err)
	}
	ticketVersion, ticketRunner, leader, err := loadProviderAttemptResultCurrentFence(ctx, s.db, c.Ref)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
		}
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, normalizeBusy(ctx, err)
	}
	if attemptState != "completed" || attemptOutcome != "completed" || phaseState != "completed" || phaseOutcome != "completed" || ticketVersion != expected || ticketRunner != fence.RunnerEpoch || leader != fence.LeaderEpoch || c.Ref != claim.Ref || c.Phase != claim.Phase || c.Role != claim.Role || c.Attempt != claim.Attempt || c.Binding.Identity != claim.Binding.Identity || c.RequestDigest != claim.RequestDigest || c.LeaderEpoch != claim.LeaderEpoch || c.RunnerEpoch != claim.RunnerEpoch || c.ExpectedVersion != claim.ExpectedVersion || c.Repository != claim.Repository || c.Worktree != claim.Worktree || c.WorktreeIdentity != claim.WorktreeIdentity || c.BaseSHA != claim.BaseSHA || hydrateProviderAttemptInput(&c) != nil || !validSHA256(out.RawSHA256) || !validSHA256(out.TypedSHA256) || !validSHA256(out.ValidationSHA256) || !validSHA256(out.TranscriptSHA256) || rawDigest(out.RawArtifact) != out.RawSHA256 || rawDigest(out.TypedArtifact) != out.TypedSHA256 || rawDigest(out.Validation) != out.ValidationSHA256 {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	artifact, err := phaseartifact.DecodeCanonicalTypedArtifact(out.TypedArtifact)
	if err != nil || artifact.Phase != c.Phase || artifact.Provider != c.Binding.Identity {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	validation, err := phaseartifact.DecodeCanonicalValidation(out.Validation)
	if err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	parsed, err := phaseartifact.Parse(c.Phase, contracts.PhaseResult{Provider: c.Binding.Identity, Artifact: out.RawArtifact}, validation)
	if err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	canonical, _, canonicalErr := phaseartifact.CanonicalTypedArtifact(parsed)
	if canonicalErr != nil || !bytes.Equal(canonical, out.TypedArtifact) {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	if candidateRepairAuthority {
		key := ProviderAttemptResultKey{AttemptID: out.AttemptID, Ref: out.Claim.Ref, Phase: out.Claim.Phase, Attempt: out.Claim.Attempt}
		if candidateRepairBuilderEntryResultReachesFence(ctx, s.db, key, out, expected, fence) != nil {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrStaleFence
		}
	}
	return out, parsed, nil
}

// loadProviderAttemptResultRow reads immutable output first by its sole key.
// The companion lifecycle reads intentionally stay keyed as well: result
// authentication compares every duplicated binding in Go after rehydrating the
// authoritative launch claim, instead of asking SQLite to plan a wide graph of
// redundant equality predicates before it can reject a missing result.
func loadProviderAttemptResultRow(ctx context.Context, query rowQueryer, id int64) (ProviderAttemptResult, ProviderAttemptClaim, error) {
	var out ProviderAttemptResult
	var claim ProviderAttemptClaim
	err := query.QueryRowContext(ctx, `SELECT provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,provider,model,family,provider_version,request_digest,leader_epoch,runner_epoch,expected_ticket_version,repository_path,worktree_path,worktree_identity,base_sha,raw_artifact,raw_sha256,typed_artifact,typed_sha256,validation,validation_sha256,transcript_sha256 FROM provider_attempt_results WHERE provider_attempt_id=?`, id).Scan(&out.AttemptID, &claim.Ref.Channel, &claim.Ref.Project, &claim.Ref.Ticket, &claim.Phase, &claim.Role, &claim.Attempt, &claim.Binding.Identity.Provider, &claim.Binding.Identity.Model, &claim.Binding.Identity.Family, &claim.Binding.Identity.Version, &claim.RequestDigest, &claim.LeaderEpoch, &claim.RunnerEpoch, &claim.ExpectedVersion, &claim.Repository, &claim.Worktree, &claim.WorktreeIdentity, &claim.BaseSHA, &out.RawArtifact, &out.RawSHA256, &out.TypedArtifact, &out.TypedSHA256, &out.Validation, &out.ValidationSHA256, &out.TranscriptSHA256)
	if err != nil {
		return ProviderAttemptResult{}, ProviderAttemptClaim{}, err
	}
	claim.ID = out.AttemptID
	return out, claim, nil
}

func loadProviderAttemptResultLifecycle(ctx context.Context, query rowQueryer, claim ProviderAttemptClaim) (attemptState, attemptOutcome, phaseState, phaseOutcome string, err error) {
	err = query.QueryRowContext(ctx, `SELECT state,outcome FROM provider_attempts WHERE id=?`, claim.ID).Scan(&attemptState, &attemptOutcome)
	if err != nil {
		return "", "", "", "", err
	}
	err = query.QueryRowContext(ctx, `SELECT state,outcome FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND provider=? AND model=? AND family=? AND provider_version=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND worktree_identity=? AND base_sha=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.WorktreeIdentity, claim.BaseSHA).Scan(&phaseState, &phaseOutcome)
	return attemptState, attemptOutcome, phaseState, phaseOutcome, err
}

func loadProviderAttemptResultCurrentFence(ctx context.Context, query rowQueryer, ref domain.TicketRef) (ticketVersion, ticketRunner, leader uint64, err error) {
	err = query.QueryRowContext(ctx, `SELECT version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ticketVersion, &ticketRunner)
	if err != nil {
		return 0, 0, 0, err
	}
	err = query.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader)
	return ticketVersion, ticketRunner, leader, err
}

// LoadHistoricalProviderAttemptResult reads a completed immutable result by
// its exact historical identity. It authenticates all duplicated bindings
// against provider_attempts, provider_attempt_inputs, and phase_runs, but does
// not require those original fences to still be current.
func (s *Store) LoadHistoricalProviderAttemptResult(ctx context.Context, key ProviderAttemptResultKey) (ProviderAttemptResult, phaseartifact.Parsed, error) {
	return s.loadHistoricalProviderAttemptResult(ctx, s.db, key)
}

// loadHistoricalProviderAttemptResult authenticates immutable provider output
// through the caller's connection. Recovery fencing invokes it inside an
// IMMEDIATE write transaction, so it must not open a second DB connection.
func (s *Store) loadHistoricalProviderAttemptResult(ctx context.Context, query rowQueryer, key ProviderAttemptResultKey) (ProviderAttemptResult, phaseartifact.Parsed, error) {
	if key.AttemptID <= 0 || key.Ref.Validate() != nil || key.Phase == "" || key.Attempt <= 0 {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	out, c, err := loadProviderAttemptResultRow(ctx, query, key.AttemptID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrNotFound
		}
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, normalizeBusy(ctx, err)
	}
	if c.Ref != key.Ref || c.Phase != key.Phase || c.Attempt != key.Attempt {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrNotFound
	}
	source, sourceErr := loadAuthenticatedProviderAttemptClaim(ctx, query, out.AttemptID)
	if errors.Is(sourceErr, sql.ErrNoRows) {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrNotFound
	}
	if sourceErr != nil || !resultClaimMatchesSource(c, source) {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	c, out.Claim = source, source
	attemptState, attemptOutcome, phaseState, phaseOutcome, err := loadProviderAttemptResultLifecycle(ctx, query, c)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrNotFound
		}
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, normalizeBusy(ctx, err)
	}
	if c.Ref != key.Ref || c.Phase != key.Phase || c.Attempt != key.Attempt || attemptState != "completed" || attemptOutcome != "completed" || phaseState != "completed" || phaseOutcome != "completed" || hydrateProviderAttemptInput(&out.Claim) != nil || !validSHA256(out.RawSHA256) || !validSHA256(out.TypedSHA256) || !validSHA256(out.ValidationSHA256) || !validSHA256(out.TranscriptSHA256) || rawDigest(out.RawArtifact) != out.RawSHA256 || rawDigest(out.TypedArtifact) != out.TypedSHA256 || rawDigest(out.Validation) != out.ValidationSHA256 {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	typed, err := phaseartifact.DecodeCanonicalTypedArtifact(out.TypedArtifact)
	if err != nil || typed.Phase != c.Phase || typed.Provider != c.Binding.Identity {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	validation, err := phaseartifact.DecodeCanonicalValidation(out.Validation)
	if err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	parsed, err := phaseartifact.Parse(c.Phase, contracts.PhaseResult{Provider: c.Binding.Identity, Artifact: out.RawArtifact}, validation)
	if err != nil {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	canonical, _, err := phaseartifact.CanonicalTypedArtifact(parsed)
	if err != nil || !bytes.Equal(canonical, out.TypedArtifact) {
		return ProviderAttemptResult{}, phaseartifact.Parsed{}, ErrProviderAttempt
	}
	return out, parsed, nil
}

// reviewRepairBoundaryFrom proves that a final-review repair permanently cuts
// off an earlier target-phase result. The boundary is keyed to the repair
// transition version (not a runner fence), so it remains effective after a
// crash/restart recovery advances the runner epoch.
func reviewRepairBoundaryFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, phase domain.Phase, liveVersion uint64, historicalVersion uint64) (bool, error) {
	target := domain.State("")
	switch phase {
	case domain.PhaseVerification:
		target = domain.StateVerifying
	case domain.PhaseBuild:
		target = domain.StateBuilding
	default:
		return false, ErrEvidenceConflict
	}
	var transitionVersion, attemptID int64
	var attempt int
	var typedSHA, requestID string
	err := q.QueryRowContext(ctx, `SELECT transition_ticket_version,reviewer_attempt_id,reviewer_attempt,reviewer_typed_sha256,correction_budget_request_id
		FROM final_review_repair_boundaries
		WHERE channel=? AND project_id=? AND ticket_id=? AND target_state=? AND transition_ticket_version<=?
		ORDER BY transition_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, target, liveVersion).Scan(&transitionVersion, &attemptID, &attempt, &typedSHA, &requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if transitionVersion <= 0 || attemptID <= 0 || attempt <= 0 || !validSHA256(typedSHA) || !boundedText(requestID, 300) {
		return false, ErrEvidenceConflict
	}
	var storedAttempt int
	var storedTyped, phaseName, role, state, outcome string
	if err := q.QueryRowContext(ctx, `SELECT a.attempt,r.typed_sha256,a.phase,a.role,a.state,a.outcome
		FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id
		WHERE r.provider_attempt_id=? AND r.channel=? AND r.project_id=? AND r.ticket_id=?`, attemptID, ref.Channel, ref.Project, ref.Ticket).Scan(&storedAttempt, &storedTyped, &phaseName, &role, &state, &outcome); err != nil || storedAttempt != attempt || storedTyped != typedSHA || phaseName != string(domain.PhaseReview) || role != "reviewer" || state != "completed" || outcome != "completed" {
		return false, ErrEvidenceConflict
	}
	var uses int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, requestID, transitionVersion-1).Scan(&uses); err != nil || uses != 1 {
		return false, ErrEvidenceConflict
	}
	return historicalVersion < uint64(transitionVersion), nil
}

// LatestReusableProviderAttemptResult returns exactly the newest completed
// Planner or Reviewer result. It intentionally never falls back to an older
// row when that newest row is malformed or stale: doing so would let a restart
// adopt evidence after a later provider attempt superseded it.
func (s *Store) LatestReusableProviderAttempt(ctx context.Context, request LatestReusableProviderAttemptRequest) (LatestReusableProviderAttemptResult, error) {
	if request.Ref.Validate() != nil || request.ExpectedVersion == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || !((request.Phase == domain.PhasePlanning && request.Role == "planner") || (request.Phase == domain.PhaseVerification && request.Role == "reviewer") || (request.Phase == domain.PhaseBuild && request.Role == "builder") || (request.Phase == domain.PhaseReview && request.Role == "reviewer")) {
		return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
	}
	liveBefore, liveErr := s.Ticket(ctx, request.Ref)
	if liveErr != nil {
		return LatestReusableProviderAttemptResult{}, liveErr
	}
	verificationBuildingRecovery := request.Phase == domain.PhaseVerification && liveBefore.State == domain.StateBuilding
	builderPublishingRecovery := request.Phase == domain.PhaseBuild && liveBefore.State == domain.StatePublishing
	if verificationBuildingRecovery {
		var candidates int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket).Scan(&candidates); err != nil || candidates != 0 {
			return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
		}
	}
	if builderPublishingRecovery {
		// Review-repair loops may leave multiple immutable generations. Recovery
		// is limited to the latest one, and RecordCandidate later requires the
		// reusable Builder key to equal that generation's binding.
		if _, err := s.RecoverableCandidate(ctx, request.Ref); err != nil {
			return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
		}
	}
	if (request.Phase == domain.PhaseBuild && liveBefore.State != domain.StateBuilding && !builderPublishingRecovery) || (request.Phase == domain.PhaseVerification && liveBefore.State != domain.StateVerifying && !verificationBuildingRecovery) {
		return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
	}
	var key ProviderAttemptResultKey
	var resultID sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT r.provider_attempt_id,a.attempt
		FROM provider_attempts a LEFT JOIN provider_attempt_results r ON r.provider_attempt_id=a.id
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.role=?
		AND a.state='completed' AND a.outcome='completed' AND a.finished_at IS NOT NULL AND a.finished_at <> ''
		ORDER BY a.attempt DESC,a.id DESC LIMIT 1`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, request.Phase, request.Role).Scan(&resultID, &key.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return LatestReusableProviderAttemptResult{}, ErrNotFound
	}
	if err != nil {
		return LatestReusableProviderAttemptResult{}, normalizeBusy(ctx, err)
	}
	if !resultID.Valid {
		return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
	}
	key.AttemptID, key.Ref, key.Phase = resultID.Int64, request.Ref, request.Phase
	historical, parsed, err := s.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil {
		return LatestReusableProviderAttemptResult{}, err
	}
	if historical.Claim.Role != request.Role || historical.Claim.Phase != request.Phase {
		return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
	}
	live, err := s.Ticket(ctx, request.Ref)
	if err != nil {
		return LatestReusableProviderAttemptResult{}, err
	}
	var liveLeader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, request.Ref.Channel).Scan(&liveLeader); err != nil {
		return LatestReusableProviderAttemptResult{}, normalizeBusy(ctx, err)
	}
	if liveLeader == 0 {
		return LatestReusableProviderAttemptResult{}, ErrStaleFence
	}
	// The request is a live caller fence, never a historical hint. Without
	// this check an arbitrary old request could satisfy the relative recovery
	// arithmetic below.
	if live.Version != request.ExpectedVersion || live.RunnerEpoch != request.Fence.RunnerEpoch || liveLeader != request.Fence.LeaderEpoch {
		return LatestReusableProviderAttemptResult{}, ErrStaleFence
	}
	candidateRepairAuthority := false
	if request.Phase == domain.PhaseBuild && live.State == domain.StateBuilding {
		if _, contextErr := s.candidateRepairBuildContextAt(ctx, s.db, request.Ref, request.ExpectedVersion, request.Fence); contextErr == nil {
			repairErr := candidateRepairBuilderEntryResultReachesFence(ctx, s.db, key, historical, request.ExpectedVersion, request.Fence)
			if repairErr == nil {
				candidateRepairAuthority = true
			} else if errors.Is(repairErr, ErrNotFound) {
				// The newest completed Builder predates the checks_red repair entry.
				// It remains predecessor provenance, never reusable successor work.
				return LatestReusableProviderAttemptResult{}, ErrNotFound
			} else {
				return LatestReusableProviderAttemptResult{}, ErrEvidenceConflict
			}
		} else if !errors.Is(contextErr, ErrNotFound) {
			return LatestReusableProviderAttemptResult{}, ErrEvidenceConflict
		}
	}
	if request.Phase == domain.PhaseVerification || request.Phase == domain.PhaseBuild {
		boundary, boundaryErr := reviewRepairBoundaryFrom(ctx, s.db, request.Ref, request.Phase, live.Version, historical.Claim.ExpectedVersion)
		if boundaryErr != nil {
			return LatestReusableProviderAttemptResult{}, ErrEvidenceConflict
		}
		if boundary {
			// The boundary itself is the authenticated authority to reject a
			// predecessor result. Check it after proving the caller's exact live
			// fence, but before generic runner-recovery validation that only
			// knows normal phase bridges. A fresh same-cycle result has an
			// expected version at or beyond the boundary and is not hidden.
			return LatestReusableProviderAttemptResult{}, ErrNotFound
		}
	}
	current := historical.Claim.ExpectedVersion == request.ExpectedVersion && historical.Claim.RunnerEpoch == request.Fence.RunnerEpoch && historical.Claim.LeaderEpoch == request.Fence.LeaderEpoch
	candidateRepairRecovery := candidateRepairAuthority && !current
	// Final-review results use the stricter CI/publication authority below.
	// That proof authenticates the immutable checks_green endpoint and every
	// exact pause/resume or signed recovery step to the live reviewing fence.
	// The generic audit cannot represent a first recovery whose predecessor is
	// normal lifecycle history followed by a post-publication control triplet.
	// A completed candidate-repair witness is likewise a narrower Store-owned
	// source anchor than the ticket-wide initial lifecycle audit.
	if request.Phase != domain.PhaseReview && !candidateRepairAuthority {
		if err := validateRunnerRecoveryAuthority(ctx, s.db, request.Ref, request.ExpectedVersion, request.Fence); err != nil {
			return LatestReusableProviderAttemptResult{}, ErrStaleFence
		}
	}
	result := LatestReusableProviderAttemptResult{Key: key, Result: historical, Parsed: parsed}
	if current {
		currentResult, currentParsed, loadErr := s.LoadProviderAttemptResult(ctx, historical.Claim, request.ExpectedVersion, request.Fence)
		if loadErr != nil {
			return LatestReusableProviderAttemptResult{}, loadErr
		}
		result.Result, result.Parsed = currentResult, currentParsed
	} else {
		allowedState := (request.Phase == domain.PhasePlanning && live.State == domain.StatePlanning) || (request.Phase == domain.PhaseVerification && (live.State == domain.StateVerifying || (live.State == domain.StateBuilding && verificationBuildingRecovery))) || (request.Phase == domain.PhaseBuild && (live.State == domain.StateBuilding || (live.State == domain.StatePublishing && builderPublishingRecovery))) || (request.Phase == domain.PhaseReview && live.State == domain.StateReviewing)
		if !allowedState {
			return LatestReusableProviderAttemptResult{}, ErrStaleFence
		}
		if !candidateRepairRecovery {
			if err := s.ProviderResultReachesFence(ctx, key, request.ExpectedVersion, request.Fence); err != nil {
				return LatestReusableProviderAttemptResult{}, ErrStaleFence
			}
		}
		result.Recovered = true
	}
	worktree, err := s.Worktree(ctx, request.Ref)
	if err != nil || worktree.Path != result.Result.Claim.Worktree || string(worktree.IdentityJSON) != result.Result.Claim.WorktreeIdentity || worktree.BaseSHA != result.Result.Claim.BaseSHA {
		return LatestReusableProviderAttemptResult{}, ErrEvidenceConflict
	}
	if request.Phase == domain.PhasePlanning {
		if result.Parsed.Planner == nil {
			return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
		}
		return result, nil
	}
	if request.Phase == domain.PhaseVerification {
		if result.Parsed.Verify == nil {
			return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
		}
		return result, nil
	}
	if request.Phase == domain.PhaseBuild {
		if result.Parsed.Builder == nil {
			return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
		}
		return result, nil
	}
	validation, err := phaseartifact.DecodeCanonicalValidation(result.Result.Validation)
	if err != nil || result.Parsed.Reviewer == nil || validation.ExpectedReviewedHead == "" || validation.ExpectedProofDigest == "" {
		return LatestReusableProviderAttemptResult{}, ErrProviderAttempt
	}
	candidate, err := s.RecoverableCandidate(ctx, request.Ref)
	if err != nil || candidate.Snapshot.HeadSHA != validation.ExpectedReviewedHead || candidate.Snapshot.ProofDigest != validation.ExpectedProofDigest || result.Parsed.Reviewer.ReviewedHead != validation.ExpectedReviewedHead || result.Parsed.Reviewer.ProofDigest != validation.ExpectedProofDigest {
		return LatestReusableProviderAttemptResult{}, ErrEvidenceConflict
	}
	authority, err := s.FinalReviewAuthority(ctx, request.Ref, request.ExpectedVersion, request.Fence)
	if err != nil || authority.Candidate.Snapshot != candidate.Snapshot || authority.Verification.Revision.IntentDigest != candidate.Snapshot.VerificationIntentDigest || authority.Verification.Revision.ProofDigest != candidate.Snapshot.ProofDigest {
		return LatestReusableProviderAttemptResult{}, ErrEvidenceConflict
	}
	return result, nil
}

// FailProviderAttemptBeforeLaunch releases a claim only when adapter
// invocation failed before any process was handed to the supervisor. Any
// supervisor.Run error remains ambiguous because a child may have crossed the
// pre-exec gate; callers must quarantine that path and leave recovery to an
// operator-backed drain proof.
func (s *Store) FailProviderAttemptBeforeLaunch(ctx context.Context, claim ProviderAttemptClaim, expected uint64, fence domain.Fence, at time.Time) error {
	if claim.ID <= 0 || claim.ExpectedVersion != expected || claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch || claim.RequestDigest == "" || at.IsZero() {
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
		row, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='failed',outcome='invocation_failed',finished_at=?,launch_state='drained' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND state='active' AND launch_state='launching' AND process_pid=0 AND process_pgid=0 AND process_boot_identity='' AND process_start_identity='' AND EXISTS(SELECT 1 FROM provider_attempt_inputs WHERE provider_attempt_id=provider_attempts.id AND request_digest=?)`, at.UTC().Format(time.RFC3339Nano), claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey, claim.RequestDigest)
		if err != nil {
			return err
		}
		if n, _ := row.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		row, err = conn.ExecContext(ctx, `UPDATE phase_runs SET state='failed',completed_at=?,outcome='invocation_failed' WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND provider=? AND model=? AND family=? AND provider_version=?`, at.UTC().Format(time.RFC3339Nano), claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version)
		if err != nil {
			return err
		}
		if n, _ := row.RowsAffected(); n != 1 {
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

// FailProviderAttemptBudget closes a drained attempt after Store proves that
// the trusted reported usage exceeds the remaining ticket cost or that the
// result arrived at/after the immutable ticket deadline.  Usage is charged up
// to the configured ceiling, and the durable budget_exhausted outcome blocks
// any later provider admission even across a crash before lifecycle blocking.
func (s *Store) FailProviderAttemptBudget(ctx context.Context, claim ProviderAttemptClaim, proof contracts.DrainProof, expected uint64, fence domain.Fence, rejectedUsage int64, at time.Time) error {
	if claim.ID <= 0 || claim.ExpectedVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ExpectedVersion != expected || claim.LeaderEpoch != fence.LeaderEpoch || claim.RunnerEpoch != fence.RunnerEpoch || rejectedUsage < 0 || at.IsZero() {
		return ErrProviderAttempt
	}
	if !contracts.VerifyDrainProof(claim.SupervisorKey, drainRequestForClaim(claim), proof) {
		return ErrProviderDrain
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var created string
		var maxDuration, maxCost int64
		if err := conn.QueryRowContext(ctx, `SELECT version,runner_epoch,created_at,max_duration_ns,max_cost_micro_usd FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&version, &runner, &created, &maxDuration, &maxCost); err != nil {
			return err
		}
		if version != expected {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, claim.Ref.Channel, version, runner, fence); err != nil {
			return err
		}
		persisted, err := loadAuthenticatedProviderAttemptClaim(ctx, conn, claim.ID)
		if err != nil || !sameImmutableProviderAttemptClaim(claim, persisted) {
			return ErrProviderAttempt
		}
		createdAt, err := time.Parse(time.RFC3339Nano, created)
		if err != nil || maxDuration <= 0 || maxCost <= 0 {
			return ErrEvidenceConflict
		}
		var spent int64
		if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(usage_units),0) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND id<>?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.ID).Scan(&spent); err != nil {
			return err
		}
		remaining := maxCost - spent
		deadlineRejected := !at.Before(createdAt.Add(time.Duration(maxDuration)))
		costRejected := remaining <= 0 || rejectedUsage > remaining
		if !deadlineRejected && !costRejected {
			return ErrEvidenceConflict
		}
		chargedUsage := rejectedUsage
		if remaining <= 0 {
			chargedUsage = 0
		} else if chargedUsage > remaining {
			chargedUsage = remaining
		}
		result, err := conn.ExecContext(ctx, `UPDATE provider_attempts SET state='failed',outcome='budget_exhausted',usage_units=?,finished_at=?,launch_state='drained' WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=? AND state='active'`, chargedUsage, at.UTC().Format(time.RFC3339Nano), claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey)
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
		var ticketState domain.State
		var blockedCode string
		if err := conn.QueryRowContext(ctx, `SELECT state,runner_epoch,blocked_code FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&ticketState, &currentRunner, &blockedCode); err != nil {
			return err
		}
		if currentRunner == claim.RunnerEpoch && (ticketState != domain.StateBlocked || blockedCode != "legacy_provider_phase_entry_unverifiable" || currentLeader <= claim.LeaderEpoch) {
			return ErrProviderDrain
		}
		var state, launchState, launchWorktree, bootIdentity, startIdentity string
		var pid, pgid int
		if err := conn.QueryRowContext(ctx, `SELECT state,launch_state,process_pid,process_pgid,process_boot_identity,process_start_identity,worktree_path FROM provider_attempts WHERE id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND role=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND binding_digest=? AND provider_lease_key=?`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Role, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.BindingDigest, claim.LeaseKey).Scan(&state, &launchState, &pid, &pgid, &bootIdentity, &startIdentity, &launchWorktree); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrStaleFence
			}
			return err
		}
		if currentRunner == claim.RunnerEpoch {
			var phaseRuns, leases int
			if (state != "active" && state != "quarantined") || launchState != "released" || pid <= 0 || pgid <= 0 || pid != pgid || bootIdentity == "" || startIdentity == "" || launchWorktree != claim.Worktree || conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND provider=? AND model=? AND family=? AND provider_version=? AND worktree_identity=? AND base_sha=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, claim.WorktreeIdentity, claim.BaseSHA).Scan(&phaseRuns) != nil || phaseRuns != 1 || conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope='provider' AND scope_key=? AND runner_epoch=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.LeaseKey, claim.RunnerEpoch).Scan(&leases) != nil || leases != 1 {
				return ErrProviderDrain
			}
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
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.phase,a.attempt,a.provider,a.model,a.family,a.version,a.role,a.state,a.outcome,a.usage_units,a.started_at,a.finished_at,a.qualification_id,a.binding_digest,a.provider_lease_key,a.leader_epoch,a.runner_epoch,a.expected_ticket_version,a.repository_path,a.worktree_path,a.worktree_identity,a.base_sha,a.supervisor_key,a.auth_digest,a.auth_mode,COALESCE(q.binary_digest,''),COALESCE(q.policy_digest,''),COALESCE(q.fixture_digest,''),COALESCE(i.request_digest,''),COALESCE(i.canonical_input,X'') FROM provider_attempts a LEFT JOIN provider_qualifications q ON q.id=a.qualification_id LEFT JOIN provider_attempt_inputs i ON i.provider_attempt_id=a.id WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? ORDER BY a.id`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var out []ProviderAttempt
	for rows.Next() {
		var v ProviderAttempt
		var started, finished string
		var qualification sql.NullInt64
		if err := rows.Scan(&v.ID, &v.Phase, &v.Attempt, &v.Binding.Identity.Provider, &v.Binding.Identity.Model, &v.Binding.Identity.Family, &v.Binding.Identity.Version, &v.Role, &v.State, &v.Outcome, &v.UsageUnits, &started, &finished, &qualification, &v.BindingDigest, &v.LeaseKey, &v.LeaderEpoch, &v.RunnerEpoch, &v.ExpectedVersion, &v.Repository, &v.Worktree, &v.WorktreeIdentity, &v.BaseSHA, &v.SupervisorKey, &v.Binding.AuthDigest, &v.Binding.AuthMode, &v.Binding.BinaryDigest, &v.Binding.PolicyDigest, &v.Binding.FixtureDigest, &v.RequestDigest, &v.RequestPayload); err != nil {
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
		if len(v.RequestPayload) != 0 && hydrateProviderAttemptInput(&v.ProviderAttemptClaim) != nil {
			return nil, ErrProviderAttempt
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ProviderArtifactFailures returns authenticated, non-secret explanations for
// repairable invalid provider artifacts on one ticket. Legacy failures made
// before this evidence table existed intentionally have no row rather than a
// guessed classification.
func (s *Store) ProviderArtifactFailures(ctx context.Context, ref domain.TicketRef) ([]ProviderArtifactFailure, error) {
	if ref.Validate() != nil {
		return nil, ErrProviderAttempt
	}
	rows, err := s.db.QueryContext(ctx, `SELECT provider_attempt_id,phase,role,attempt,request_digest,leader_epoch,runner_epoch,expected_ticket_version,failure_reason,failure_digest,created_at FROM provider_artifact_failures WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY provider_attempt_id`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	// Materialize and close this cursor before authenticating each row below.
	// Store is intentionally usable with a one-connection SQLite pool, where a
	// nested query while this cursor is open would otherwise wait for itself.
	var values []ProviderArtifactFailure
	for rows.Next() {
		var value ProviderArtifactFailure
		var created string
		if err := rows.Scan(&value.AttemptID, &value.Phase, &value.Role, &value.Attempt, &value.RequestDigest, &value.LeaderEpoch, &value.RunnerEpoch, &value.ExpectedVersion, &value.Reason, &value.Digest, &created); err != nil {
			return nil, err
		}
		createdAt, err := time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, ErrEvidenceConflict
		}
		value.Ref, value.CreatedAt = ref, createdAt
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	var out []ProviderArtifactFailure
	for _, value := range values {
		claim, err := loadAuthenticatedProviderAttemptClaim(ctx, s.db, value.AttemptID)
		if err != nil || claim.Ref != ref || claim.Phase != value.Phase || claim.Role != value.Role || claim.Attempt != value.Attempt || claim.RequestDigest != value.RequestDigest || claim.LeaderEpoch != value.LeaderEpoch || claim.RunnerEpoch != value.RunnerEpoch || claim.ExpectedVersion != value.ExpectedVersion {
			return nil, ErrEvidenceConflict
		}
		var attemptState, attemptOutcome, phaseState, phaseOutcome string
		var resultCount int
		if err := s.db.QueryRowContext(ctx, `SELECT a.state,a.outcome,p.state,p.outcome,(SELECT COUNT(*) FROM provider_attempt_results r WHERE r.provider_attempt_id=a.id) FROM provider_attempts a JOIN phase_runs p ON p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.phase=a.phase AND p.attempt=a.attempt AND p.provider=a.provider AND p.model=a.model AND p.family=a.family AND p.provider_version=a.version AND p.leader_epoch=a.leader_epoch AND p.runner_epoch=a.runner_epoch AND p.expected_ticket_version=a.expected_ticket_version AND p.worktree_identity=a.worktree_identity AND p.base_sha=a.base_sha WHERE a.id=?`, value.AttemptID).Scan(&attemptState, &attemptOutcome, &phaseState, &phaseOutcome, &resultCount); err != nil || attemptState != "failed" || attemptOutcome != contracts.PhaseResultInvalidArtifact || phaseState != "failed" || phaseOutcome != contracts.PhaseResultInvalidArtifact || resultCount != 0 {
			return nil, ErrEvidenceConflict
		}
		digest, err := providerArtifactFailureDigest(value)
		if err != nil || digest != value.Digest {
			return nil, ErrEvidenceConflict
		}
		out = append(out, value)
	}
	return out, nil
}

func currentRuntimeQualification(ctx context.Context, conn *sql.Conn, channel domain.Channel, role string, b contracts.RuntimeBinding) (ProviderQualification, error) {
	var id int64
	q := ProviderQualification{}
	q.ID = 0
	query := `SELECT q.id,q.channel,q.run_id,q.provider,q.model,q.family,q.provider_version,q.binary_digest,q.policy_digest,q.fixture_digest,q.profile,q.failed_probes_json,q.reason_code,q.created_at,q.auth_digest,q.auth_mode,q.probe_digest,q.attested_leader_epoch,q.attestation_signature FROM provider_qualifications q`
	where := ` WHERE q.channel=? AND q.provider=? AND q.model=? AND q.family=? AND q.provider_version=? AND q.binary_digest=? AND q.policy_digest=? AND q.fixture_digest=? AND q.profile IN ('qualified_guarded','autonomous_eligible') AND (q.provider <> 'codex' OR (q.auth_digest=? AND q.auth_mode=? AND length(q.probe_digest)=64 AND q.attested_leader_epoch>0 AND length(q.attestation_signature)=64))`
	args := []any{channel, b.Identity.Provider, b.Identity.Model, b.Identity.Family, b.Identity.Version, b.BinaryDigest, b.PolicyDigest, b.FixtureDigest}
	if b.Identity.Provider == "codex" {
		args = append(args, b.AuthDigest, b.AuthMode)
	} else {
		args = append(args, "", "")
	}
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
	if value.Provider.Provider == "codex" {
		var epoch uint64
		var key []byte
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch,recovery_public_key FROM daemon_instances WHERE channel=?`, channel).Scan(&epoch, &key); err != nil || value.AttestedLeaderEpoch != epoch || !contracts.VerifyQualificationAttestation(key, contracts.QualificationAttestation{Channel: value.Channel, RunID: value.RunID, Identity: value.Provider, BinaryDigest: value.BinaryDigest, PolicyDigest: value.PolicyDigest, FixtureDigest: value.FixtureDigest, AuthDigest: value.AuthDigest, AuthMode: value.AuthMode, ProbeDigest: value.ProbeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: value.CreatedAt.UnixNano(), LeaderEpoch: value.AttestedLeaderEpoch, Nonce: value.RunID, Signature: value.AttestationSignature}) {
			return ProviderQualification{}, ErrProviderPairRefused
		}
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

// validProviderAttemptInput duplicates the coordinator's admission boundary
// because Store is the issuing authority. Direct callers must not be able to
// mint a durable claim whose actual launch fields were never admissible.
func validProviderAttemptInput(r ProviderAttemptRequest) bool {
	return validPhaseInputForAttempt(r, 0)
}

func validPhaseInputForAttempt(r ProviderAttemptRequest, attempt int) bool {
	in := r.Input
	if in.Ticket != r.Ref || in.Phase != r.Phase || in.Attempt != attempt || in.LeaderEpoch != r.Fence.LeaderEpoch || in.RunnerEpoch != r.Fence.RunnerEpoch || in.ExpectedVersion != r.ExpectedVersion || in.Provider != r.Binding.Identity || in.AuthMode != r.Binding.AuthMode || in.Repository != r.Repository || in.Worktree != r.Worktree || in.WorktreeIdentity != r.WorktreeIdentity || in.BaseSHA != r.BaseSHA || in.Profile != contracts.ProfileGuarded || (attempt == 0 && in.RequestDigest != "") || strings.TrimSpace(in.Prompt) == "" || len(in.Prompt) > 64<<10 || strings.ContainsRune(in.Prompt, '\x00') || in.Timeout <= 0 || in.Timeout > 45*time.Minute || len(in.Schema) == 0 || len(in.Schema) > 1<<20 || !json.Valid(in.Schema) {
		return false
	}
	if in.Repair != nil && !contracts.ValidProviderRepairContext(in.Repair) {
		return false
	}
	if len(in.AllowedPaths) == 0 || len(in.AllowedPaths) > 256 {
		return false
	}
	seen := make(map[string]struct{}, len(in.AllowedPaths))
	for _, path := range in.AllowedPaths {
		if path == "" || len(path) > 4096 || filepath.IsAbs(path) || filepath.Clean(path) != path || (path != "." && (path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)))) {
			return false
		}
		if _, ok := seen[path]; ok {
			return false
		}
		seen[path] = struct{}{}
	}
	return true
}

func validPersistedProviderAttemptClaim(claim ProviderAttemptClaim) bool {
	r := ProviderAttemptRequest{Ref: claim.Ref, ExpectedVersion: claim.ExpectedVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, Phase: claim.Phase, Role: claim.Role, Binding: claim.Binding, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, SupervisorKey: claim.SupervisorKey, Input: claim.Input}
	return claim.ID > 0 && claim.Ref.Validate() == nil && claim.QualificationID > 0 && validProviderPhase(claim.Phase) && validProviderRole(claim.Role) && claim.Attempt > 0 && claim.ExpectedVersion > 0 && claim.LeaderEpoch > 0 && claim.RunnerEpoch > 0 && claim.RequestDigest != "" && claim.BindingDigest == bindingDigest(claim.Binding) && validRuntimeBinding(claim.Binding) && validProviderIdentityClaim(r) && validPhaseInputForAttempt(r, claim.Attempt)
}
func validAttemptState(v string) bool { return v == "completed" || v == "failed" || v == "cancelled" }
func safeOutcome(v string) bool {
	if v == "completed" || v == "failed" || v == "cancelled" || v == "invalid_artifact" || v == "result_indeterminate" || v == "drained_recovery" {
		return true
	}
	return false
}
func validRuntimeBinding(v contracts.RuntimeBinding) bool {
	return v.Identity.Provider != "" && hexDigest(v.BinaryDigest) && hexDigest(v.PolicyDigest) && hexDigest(v.FixtureDigest) && hexDigest(v.AuthDigest) && (v.Identity.Provider != "codex" || v.AuthMode == "chatgpt_subscription")
}
func validProviderIdentityClaim(r ProviderAttemptRequest) bool {
	return r.Repository != "" && r.Worktree != "" && r.WorktreeIdentity != "" && validOID(r.BaseSHA) && len(r.SupervisorKey) == 32
}
func drainRequestForClaim(c ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: c.ID, Identity: c.Binding.Identity, Ref: c.Ref, Phase: c.Phase, Role: c.Role, Attempt: c.Attempt, LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch, ExpectedVersion: c.ExpectedVersion, LeaseKey: c.LeaseKey, BindingDigest: c.BindingDigest, BinaryDigest: c.Binding.BinaryDigest, PolicyDigest: c.Binding.PolicyDigest, AuthDigest: c.Binding.AuthDigest, AuthMode: c.Binding.AuthMode, Repository: c.Repository, Worktree: c.Worktree, WorktreeIdentity: c.WorktreeIdentity, BaseSHA: c.BaseSHA, RequestDigest: c.RequestDigest}
}

// hydrateProviderAttemptInput makes recovery fail closed: a digest is useful
// only when its canonical bytes still decode to the exact attempt identity.
func hydrateProviderAttemptInput(claim *ProviderAttemptClaim) error {
	if claim == nil || len(claim.RequestDigest) != 64 || len(claim.RequestPayload) == 0 || !hexDigest(claim.RequestDigest) {
		return ErrProviderAttempt
	}
	sum := sha256.Sum256(claim.RequestPayload)
	if hex.EncodeToString(sum[:]) != claim.RequestDigest {
		return ErrProviderAttempt
	}
	input, err := contracts.DecodeCanonicalPhaseInput(claim.RequestPayload)
	if err != nil || !contracts.PhaseInputDigestMatches(input, claim.RequestDigest) || input.Ticket != claim.Ref || input.Phase != claim.Phase || input.Provider != claim.Binding.Identity || input.AuthMode != claim.Binding.AuthMode || input.Attempt != claim.Attempt || input.LeaderEpoch != claim.LeaderEpoch || input.RunnerEpoch != claim.RunnerEpoch || input.ExpectedVersion != claim.ExpectedVersion || input.Repository != claim.Repository || input.Worktree != claim.Worktree || input.WorktreeIdentity != claim.WorktreeIdentity || input.BaseSHA != claim.BaseSHA {
		return ErrProviderAttempt
	}
	input.RequestDigest = claim.RequestDigest
	claim.Input = input
	claim.RequestPayload = append([]byte(nil), claim.RequestPayload...)
	return nil
}
func hexDigest(v string) bool {
	return len(v) == 64 && strings.ToLower(v) == v && strings.Trim(v, "0123456789abcdef") == ""
}
func bindingDigest(v contracts.RuntimeBinding) string {
	// Include every runtime fact, including the account digest, while never
	// persisting credential material itself. The qualification ID separately
	// anchors the non-secret facts to the durable root authority.
	sum := sha256.Sum256([]byte(v.Identity.Provider + "\x00" + v.Identity.Model + "\x00" + v.Identity.Family + "\x00" + v.Identity.Version + "\x00" + v.BinaryDigest + "\x00" + v.PolicyDigest + "\x00" + v.FixtureDigest + "\x00" + v.AuthDigest + "\x00" + v.AuthMode))
	return hex.EncodeToString(sum[:])
}
