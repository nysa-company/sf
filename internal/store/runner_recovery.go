package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// RunnerRecoveryLedger is the authenticated proof for one durable +1/+1
// runner fence. A zero prior leader means the ticket predates this ledger and
// has no recoverable leader predecessor; publication/waiting tickets always
// bind their prior leader to the publication witness before using the row.
type RunnerRecoveryLedger struct {
	Ref                domain.TicketRef
	PriorTicketVersion uint64
	PriorRunnerEpoch   uint64
	PriorLeaderEpoch   uint64
	TicketVersion      uint64
	RunnerEpoch        uint64
	LeaderEpoch        uint64
	RecoveryDigest     string
	CreatedAt          time.Time
}

// runnerStartAuthority is the immutable source endpoint recorded alongside
// queued->planning. It is deliberately separate from the event projection:
// an event is not a one-row-per-ticket authority and cannot be replayed as the
// physical start fence after a daemon restart.
type runnerStartAuthority struct {
	Ref                domain.TicketRef
	StartTicketVersion uint64
	RunnerEpoch        uint64
	LeaderEpoch        uint64
	WorkflowID         string
	WorkflowDigest     string
	CreatedAt          time.Time
	AuthorityDigest    string
}

type runnerStartAuthorityDigestPayload struct {
	Channel            domain.Channel `json:"channel"`
	Project            string         `json:"project"`
	Ticket             string         `json:"ticket"`
	StartTicketVersion uint64         `json:"start_ticket_version"`
	RunnerEpoch        uint64         `json:"runner_epoch"`
	LeaderEpoch        uint64         `json:"leader_epoch"`
	WorkflowID         string         `json:"workflow_id"`
	CreatedAt          string         `json:"created_at"`
}

func runnerStartAuthorityPayload(ref domain.TicketRef, version, runner, leader uint64, workflowID, createdAt string) ([]byte, error) {
	if ref.Validate() != nil || version != 2 || runner != 1 || leader == 0 || !boundedText(workflowID, 300) || createdAt == "" {
		return nil, ErrPublicationEvidence
	}
	return json.Marshal(runnerStartAuthorityDigestPayload{Channel: ref.Channel, Project: string(ref.Project), Ticket: string(ref.Ticket), StartTicketVersion: version, RunnerEpoch: runner, LeaderEpoch: leader, WorkflowID: workflowID, CreatedAt: createdAt})
}

func recordRunnerStartAuthority(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, workflowID, createdAt string) error {
	payload, err := runnerStartAuthorityPayload(ref, version, fence.RunnerEpoch, fence.LeaderEpoch, workflowID, createdAt)
	if err != nil {
		return ErrPublicationEvidence
	}
	if _, err := parseRunnerRecoveryTime(createdAt); err != nil {
		return ErrPublicationEvidence
	}
	workflowDigest := publicationIdentityDigest([]byte(workflowID))
	authorityDigest := publicationIdentityDigest(payload)
	_, err = conn.ExecContext(ctx, `INSERT INTO runner_start_authorities(channel,project_id,ticket_id,start_ticket_version,runner_epoch,leader_epoch,workflow_id,workflow_digest,created_at,authority_digest) VALUES(?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, version, fence.RunnerEpoch, fence.LeaderEpoch, workflowID, workflowDigest, createdAt, authorityDigest)
	return err
}

func loadRunnerStartAuthority(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, liveVersion, expectedRunner uint64) (runnerStartAuthority, bool, error) {
	if ref.Validate() != nil || liveVersion != 2 || expectedRunner != 1 {
		return runnerStartAuthority{}, false, ErrPublicationEvidence
	}
	var authority runnerStartAuthority
	var createdAt, persistedWorkflowID string
	err := q.QueryRowContext(ctx, `SELECT a.start_ticket_version,a.runner_epoch,a.leader_epoch,a.workflow_id,a.workflow_digest,a.created_at,a.authority_digest,t.workflow_id FROM runner_start_authorities a JOIN tickets t ON t.channel=a.channel AND t.project_id=a.project_id AND t.id=a.ticket_id WHERE a.channel=? AND a.project_id=? AND a.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&authority.StartTicketVersion, &authority.RunnerEpoch, &authority.LeaderEpoch, &authority.WorkflowID, &authority.WorkflowDigest, &createdAt, &authority.AuthorityDigest, &persistedWorkflowID)
	if errors.Is(err, sql.ErrNoRows) {
		return runnerStartAuthority{}, false, nil
	}
	if err != nil {
		return runnerStartAuthority{}, false, err
	}
	authority.Ref = ref
	authority.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
	if err != nil || authority.StartTicketVersion != 2 || authority.StartTicketVersion > liveVersion || authority.RunnerEpoch != 1 || authority.RunnerEpoch != expectedRunner || authority.LeaderEpoch == 0 || !boundedText(authority.WorkflowID, 300) || authority.WorkflowID != persistedWorkflowID || authority.WorkflowDigest != publicationIdentityDigest([]byte(authority.WorkflowID)) {
		return runnerStartAuthority{}, false, ErrPublicationEvidence
	}
	payload, err := runnerStartAuthorityPayload(ref, authority.StartTicketVersion, authority.RunnerEpoch, authority.LeaderEpoch, authority.WorkflowID, createdAt)
	if err != nil || authority.AuthorityDigest != publicationIdentityDigest(payload) {
		return runnerStartAuthority{}, false, ErrPublicationEvidence
	}
	return authority, true, nil
}

// validateInitialLifecycleAdvance authenticates the ordinary ticket history
// before the first recovery row. In the queued->planning crash window this
// proves the durable start event that the runner-start authority accompanies;
// it never infers a predecessor from counters alone.
func validateInitialLifecycleAdvance(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, endVersion uint64) error {
	if endVersion == 0 {
		return ErrPublicationEvidence
	}
	var initialCount int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=1 AND ((trigger='submit_valid' AND from_state='none' AND to_state='queued' AND payload='{}') OR (trigger='ticket_submitted' AND from_state='queued' AND to_state='queued' AND payload='{}'))`, ref.Channel, ref.Project, ref.Ticket).Scan(&initialCount); err != nil || initialCount != 1 {
		return ErrPublicationEvidence
	}
	var initialTrigger, initialPayload string
	var initialFrom, initialTo domain.State
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=1 AND ((trigger='submit_valid' AND from_state='none' AND to_state='queued' AND payload='{}') OR (trigger='ticket_submitted' AND from_state='queued' AND to_state='queued' AND payload='{}'))`, ref.Channel, ref.Project, ref.Ticket).Scan(&initialTrigger, &initialFrom, &initialTo, &initialPayload); err != nil || initialPayload != "{}" || initialTo != domain.StateQueued || !((initialTrigger == "submit_valid" && initialFrom == domain.State("none")) || (initialTrigger == "ticket_submitted" && initialFrom == domain.StateQueued)) {
		return ErrPublicationEvidence
	}
	var initialStateChanges int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=1 AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket).Scan(&initialStateChanges); err != nil || (initialTrigger == "submit_valid" && initialStateChanges != 1) || (initialTrigger == "ticket_submitted" && initialStateChanges != 0) {
		return ErrPublicationEvidence
	}
	if endVersion == 1 {
		return nil
	}
	var startCount int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=2 AND trigger IN ('operator_start','start_or_adopt') AND from_state='queued' AND to_state='planning' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket).Scan(&startCount); err != nil || startCount != 1 {
		return ErrPublicationEvidence
	}
	var lastID int64
	var startTrigger, startPayload string
	var startFrom, startTo domain.State
	if err := q.QueryRowContext(ctx, `SELECT id,trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=2 AND trigger IN ('operator_start','start_or_adopt') AND from_state='queued' AND to_state='planning' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket).Scan(&lastID, &startTrigger, &startFrom, &startTo, &startPayload); err != nil || startPayload != "{}" || startFrom != domain.StateQueued || startTo != domain.StatePlanning || (startTrigger != "operator_start" && startTrigger != "start_or_adopt") {
		return ErrPublicationEvidence
	}
	var startStateChanges int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=2 AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket).Scan(&startStateChanges); err != nil || startStateChanges != 1 {
		return ErrPublicationEvidence
	}
	if endVersion == 2 {
		return nil
	}
	rows, err := q.QueryContext(ctx, `SELECT id,ticket_version,trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND from_state<>to_state ORDER BY ticket_version,id`, ref.Channel, ref.Project, ref.Ticket, 2, endVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	last := uint64(2)
	prior := domain.StatePlanning
	for rows.Next() {
		var id int64
		var version uint64
		var trigger, payload string
		var from, to domain.State
		if err := rows.Scan(&id, &version, &trigger, &from, &to, &payload); err != nil || id <= lastID || version != last+1 || trigger == "" || !from.Valid() || !to.Valid() || !json.Valid([]byte(payload)) || len(payload) > maxEvidenceJSON || from != prior || !validInitialLifecycleTransition(trigger, from, to) {
			return ErrPublicationEvidence
		}
		lastID = id
		last, prior = version, to
	}
	if err := rows.Err(); err != nil || last != endVersion {
		return ErrPublicationEvidence
	}
	return nil
}

func validInitialLifecycleTransition(trigger string, from, to domain.State) bool {
	switch {
	case from == domain.StatePlanning && to == domain.StateVerifying:
		return trigger == "phase_pass"
	case from == domain.StateVerifying && to == domain.StateBuilding:
		return trigger == "phase_pass"
	case from == domain.StateBuilding && to == domain.StatePublishing:
		return trigger == "phase_pass"
	case from == domain.StatePublishing && to == domain.StateWaitingCI:
		return trigger == "effects_confirmed"
	default:
		return false
	}
}

func validateWaitingRecoveryLedger(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, baselineVersion, baselineRunner, baselineLeader, liveVersion, liveRunner, liveLeader uint64) error {
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	if _, found, err := loadRunnerRecoveryAt(ctx, q, ref, baselineVersion); err != nil {
		return err
	} else if found {
		// The transition version is the exact publication witness successor;
		// recovery rows may begin only after it. A row here would be an
		// unproven extra state advance, not a zero-recovery replay.
		return ErrPublicationEvidence
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baselineVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	expectedVersion, expectedRunner, expectedLeader := baselineVersion, baselineRunner, baselineLeader
	var count int
	for rows.Next() {
		count++
		if count > 64 {
			return ErrPublicationEvidence
		}
		var step RunnerRecoveryLedger
		var createdAt string
		if err := rows.Scan(&step.PriorTicketVersion, &step.PriorRunnerEpoch, &step.PriorLeaderEpoch, &step.TicketVersion, &step.RunnerEpoch, &step.LeaderEpoch, &step.RecoveryDigest, &createdAt); err != nil {
			return err
		}
		step.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
		if err != nil {
			return ErrPublicationEvidence
		}
		step.Ref = ref
		if !validRunnerRecovery(step) || step.PriorTicketVersion != expectedVersion || step.PriorRunnerEpoch != expectedRunner || step.PriorLeaderEpoch != expectedLeader || step.TicketVersion != expectedVersion+1 || step.RunnerEpoch != expectedRunner+1 || step.LeaderEpoch <= expectedLeader {
			return ErrPublicationEvidence
		}
		var events int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&events); err != nil || events != 0 {
			return ErrPublicationEvidence
		}
		expectedVersion, expectedRunner, expectedLeader = step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if liveVersion != expectedVersion || liveRunner != expectedRunner || liveLeader != expectedLeader {
		return ErrPublicationEvidence
	}
	return nil
}

func runnerRecoveryPayload(value RunnerRecoveryLedger) ([]byte, error) {
	return json.Marshal(struct {
		Channel            domain.Channel   `json:"channel"`
		Project            domain.ProjectID `json:"project"`
		Ticket             domain.TicketID  `json:"ticket"`
		PriorTicketVersion uint64           `json:"prior_ticket_version"`
		PriorRunnerEpoch   uint64           `json:"prior_runner_epoch"`
		PriorLeaderEpoch   uint64           `json:"prior_leader_epoch"`
		TicketVersion      uint64           `json:"ticket_version"`
		RunnerEpoch        uint64           `json:"runner_epoch"`
		LeaderEpoch        uint64           `json:"leader_epoch"`
		CreatedAt          string           `json:"created_at"`
	}{value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.PriorTicketVersion, value.PriorRunnerEpoch, value.PriorLeaderEpoch, value.TicketVersion, value.RunnerEpoch, value.LeaderEpoch, value.CreatedAt.UTC().Format(time.RFC3339Nano)})
}

func validRunnerRecovery(value RunnerRecoveryLedger) bool {
	if value.Ref.Validate() != nil || value.PriorTicketVersion == 0 || value.PriorRunnerEpoch == 0 || value.PriorLeaderEpoch == 0 || value.TicketVersion != value.PriorTicketVersion+1 || value.RunnerEpoch != value.PriorRunnerEpoch+1 || value.LeaderEpoch == 0 || value.LeaderEpoch <= value.PriorLeaderEpoch || value.CreatedAt.IsZero() || !validClaimDigest(value.RecoveryDigest) {
		return false
	}
	payload, err := runnerRecoveryPayload(value)
	return err == nil && publicationIdentityDigest(payload) == value.RecoveryDigest
}

func loadRunnerRecoveryAt(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, version uint64) (RunnerRecoveryLedger, bool, error) {
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	var value RunnerRecoveryLedger
	var createdAt string
	err := q.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&value.PriorTicketVersion, &value.PriorRunnerEpoch, &value.PriorLeaderEpoch, &value.TicketVersion, &value.RunnerEpoch, &value.LeaderEpoch, &value.RecoveryDigest, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RunnerRecoveryLedger{}, false, nil
	}
	if err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	value.Ref = ref
	value.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
	if err != nil {
		return RunnerRecoveryLedger{}, false, ErrPublicationEvidence
	}
	return value, true, nil
}

func loadLatestRunnerRecovery(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef) (RunnerRecoveryLedger, bool, error) {
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	var value RunnerRecoveryLedger
	var createdAt string
	err := q.QueryRowContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&value.PriorTicketVersion, &value.PriorRunnerEpoch, &value.PriorLeaderEpoch, &value.TicketVersion, &value.RunnerEpoch, &value.LeaderEpoch, &value.RecoveryDigest, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return RunnerRecoveryLedger{}, false, nil
	}
	if err != nil {
		return RunnerRecoveryLedger{}, false, err
	}
	value.Ref = ref
	value.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
	if err != nil {
		return RunnerRecoveryLedger{}, false, ErrPublicationEvidence
	}
	return value, true, nil
}

// validateRunnerRecoveryCardinality enforces the ticket-wide, lifetime cap.
// Publication recovery has a later semantic baseline, but no phase is allowed
// to mint a second 64-row recovery budget.
func validateRunnerRecoveryCardinality(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef) error {
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil {
		return err
	}
	if count > 64 {
		return ErrPublicationEvidence
	}
	return nil
}

func parseRunnerRecoveryTime(raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || parsed.Location() != time.UTC || parsed.Format(time.RFC3339Nano) != raw {
		return time.Time{}, errors.New("invalid runner recovery timestamp")
	}
	return parsed, nil
}

func authenticateRunnerRecoveryStep(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, priorVersion uint64, priorFence domain.Fence, version uint64, fence domain.Fence) error {
	step, found, err := loadRunnerRecoveryAt(ctx, q, ref, version)
	if err != nil || !found || !validRunnerRecovery(step) || step.PriorTicketVersion != priorVersion || step.PriorRunnerEpoch != priorFence.RunnerEpoch || step.PriorLeaderEpoch != priorFence.LeaderEpoch || step.TicketVersion != version || step.RunnerEpoch != fence.RunnerEpoch || step.LeaderEpoch != fence.LeaderEpoch {
		return ErrPublicationEvidence
	}
	return nil
}

func recordRunnerRecovery(ctx context.Context, conn *sql.Conn, value RunnerRecoveryLedger) error {
	if value.CreatedAt.IsZero() {
		return ErrPublicationEvidence
	}
	if !validRunnerRecovery(value) {
		return ErrPublicationEvidence
	}
	_, err := conn.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.PriorTicketVersion, value.PriorRunnerEpoch, value.PriorLeaderEpoch, value.TicketVersion, value.RunnerEpoch, value.LeaderEpoch, value.RecoveryDigest, value.CreatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

func runnerRecoveryDigest(value RunnerRecoveryLedger) string {
	payload, _ := runnerRecoveryPayload(value)
	return publicationIdentityDigest(payload)
}

func (s *Store) publicationRecoveryBaseline(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version, runner uint64) (uint64, bool, error) {
	publication, found, err := loadPublicationEvidenceRow(ctx, conn, ref)
	if err != nil || !found {
		return 0, false, err
	}
	if err := loadLatestPublicationRebind(ctx, conn, &publication); err != nil {
		return 0, false, err
	}
	// A publishing witness may be consumed by the one-version
	// publishing->waiting_ci transition before its first recovery fence. In
	// that case the current ticket is exactly one version beyond the witness,
	// with the same runner; this is still the witness's authenticated leader
	// predecessor, not a counter-based inference.
	if (publication.CurrentTicketVersion != version && publication.CurrentTicketVersion+1 != version) || publication.CurrentFence.RunnerEpoch != runner {
		return 0, false, nil
	}
	return publication.CurrentFence.LeaderEpoch, true, nil
}

// providerRecoveryBaseline authenticates the original provider launch fence
// when a daemon dies after admitting a phase attempt but before any recovery
// row exists. The attempt itself is the durable source endpoint; matching
// counters without rehydrating its immutable qualification/input is not.
func providerRecoveryBaseline(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, state domain.State, version, runner uint64) (uint64, bool, error) {
	phase, role, ok := recoveryProviderPhase(state)
	if !ok {
		return 0, false, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT id FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND role=? AND expected_ticket_version=? AND runner_epoch=? AND state IN ('active','quarantined') ORDER BY id`, ref.Channel, ref.Project, ref.Ticket, phase, role, version, runner)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	var id int64
	count := 0
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, err
		}
		count++
		if count > 1 {
			return 0, false, ErrPublicationEvidence
		}
	}
	if err := rows.Err(); err != nil {
		return 0, false, err
	}
	if count == 0 {
		return 0, false, nil
	}
	claim, err := loadAuthenticatedProviderAttemptClaim(ctx, q, id)
	if err != nil || claim.Ref != ref || claim.ExpectedVersion != version || claim.RunnerEpoch != runner || claim.Phase != phase || claim.Role != role || claim.LeaderEpoch == 0 {
		return 0, false, ErrPublicationEvidence
	}
	return claim.LeaderEpoch, true, nil
}

func recoveryProviderPhase(state domain.State) (domain.Phase, string, bool) {
	switch state {
	case domain.StatePlanning:
		return domain.PhasePlanning, "planner", true
	case domain.StateVerifying:
		return domain.PhaseVerification, "reviewer", true
	case domain.StateBuilding:
		return domain.PhaseBuild, "builder", true
	case domain.StateReviewing:
		return domain.PhaseReview, "reviewer", true
	default:
		return "", "", false
	}
}
