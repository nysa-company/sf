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
// runner fence. Every row must bind its predecessor to a nonzero authenticated
// leader; a leader-only takeover cannot mint a zero-prior recovery row.
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

// RunnerStartAuthority is the immutable source endpoint recorded in the same
// transaction as the canonical queued->planning transition. It exists outside
// the event projection because an event is not a physical, one-row-per-ticket
// authority and can be duplicated by an untrusted writer.
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

// recordRunnerStartAuthority persists a single immutable bootstrap endpoint.
// The caller invokes it only after the actual queued->planning update, within
// that update's transaction, so a partial start cannot leave an authority row.
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
	if err != nil {
		return err
	}
	return nil
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

// validateRunnerRecoveryLedger proves that an immutable source fence reaches
// an exact live fence solely through contiguous daemon recovery rows.  It is
// deliberately phase-neutral: provider, command, checkpoint, and candidate
// readers all consume the same authority rather than inferring epochs.
func validateRunnerRecoveryLedger(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, baselineVersion, baselineRunner, baselineLeader, liveVersion, liveRunner, liveLeader uint64) error {
	var future int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?`, ref.Channel, ref.Project, ref.Ticket, liveVersion).Scan(&future); err != nil || future != 0 {
		return ErrPublicationEvidence
	}
	return validateRunnerRecoveryLedgerPrefix(ctx, q, ref, baselineVersion, baselineRunner, baselineLeader, liveVersion, liveRunner, liveLeader)
}

// validateRunnerRecoveryLedgerPrefix authenticates a bounded historical
// segment. Callers must independently authenticate every later segment; the
// ordinary validator above deliberately refuses any future row instead.
func validateRunnerRecoveryLedgerPrefix(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, baselineVersion, baselineRunner, baselineLeader, liveVersion, liveRunner, liveLeader uint64) error {
	if baselineVersion == 0 || baselineRunner == 0 || baselineLeader == 0 || liveVersion == 0 || liveRunner == 0 || liveLeader == 0 {
		return ErrPublicationEvidence
	}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baselineVersion, liveVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	steps := make([]RunnerRecoveryLedger, 0, 8)
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
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, step := range steps {
		if !validRunnerRecovery(step) || step.PriorTicketVersion < expectedVersion || step.PriorRunnerEpoch < expectedRunner || step.PriorLeaderEpoch < expectedLeader {
			return ErrPublicationEvidence
		}
		// A complete operator pause/take may advance the ticket version and
		// runner epoch between two daemon recovery rows. Authenticate that
		// control-only segment before consuming the next +1 recovery row; a
		// counter gap without its stopping/drained/resume event triplet remains
		// invalid.
		if err := validateRunnerControlAdvance(ctx, q, ref, expectedVersion, expectedRunner, expectedLeader, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch); err != nil {
			if err := validateRunnerPhaseChain(ctx, q, ref, expectedVersion, expectedRunner, step.PriorTicketVersion, step.PriorRunnerEpoch); err != nil && validateRunnerVerificationAmendmentAdvance(ctx, q, ref, expectedVersion, expectedRunner, expectedLeader, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch) != nil {
				return ErrPublicationEvidence
			}
		}
		if step.PriorLeaderEpoch == 0 || step.TicketVersion != step.PriorTicketVersion+1 || step.RunnerEpoch != step.PriorRunnerEpoch+1 || step.LeaderEpoch <= step.PriorLeaderEpoch {
			return ErrPublicationEvidence
		}
		var transitions int
		// Evidence/audit projections may be appended at the recovered version;
		// only a durable state transition would make this ledger predecessor
		// ambiguous. The row itself remains authenticated by its digest/chain.
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&transitions); err != nil || transitions != 0 {
			return ErrPublicationEvidence
		}
		expectedVersion, expectedRunner, expectedLeader = step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch
	}
	if validateRunnerControlAdvance(ctx, q, ref, expectedVersion, expectedRunner, expectedLeader, liveVersion, liveRunner, liveLeader) != nil {
		if err := validateRunnerPhaseAdvance(ctx, q, ref, expectedVersion, expectedRunner, liveVersion, liveRunner); err != nil && validateRunnerVerificationAmendmentAdvance(ctx, q, ref, expectedVersion, expectedRunner, expectedLeader, liveVersion, liveRunner, liveLeader) != nil {
			return ErrPublicationEvidence
		}
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
// to mint a second 64-row recovery budget. Exact replays audit it too.
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

// validateWaitingRecoveryLedger authenticates the recovery rows which may
// precede the first CI observation. The publication->waiting_ci transition is
// the baseline version, so a recovery row at that version is never accepted;
// each later row must be contiguous and must not have a competing event.
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

// validateRunnerControlAdvance proves the only non-ledger runner advance that
// can sit between recovery rows: a complete operator pause/take handoff. The
// control primitive increments runner_epoch atomically with its exact event;
// drained completion and operator resume then return the ticket to a runnable
// state without changing the runner. A raw counter gap is never enough.
func validateRunnerControlAdvance(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, ref domain.TicketRef, startVersion, startRunner, startLeader, endVersion, endRunner, endLeader uint64) error {
	if startVersion == 0 || startRunner == 0 || startLeader == 0 || endVersion < startVersion || endRunner < startRunner || endLeader < startLeader {
		return ErrPublicationEvidence
	}
	// Generic readers never treat a daemon takeover as a control advance.  A
	// leader-only change has no signed ticket-local predecessor, so accepting it
	// here would make old provider or publication evidence current before the
	// startup fence appends its recovery row.
	if endVersion == startVersion && endRunner == startRunner {
		if endLeader != startLeader {
			return ErrPublicationEvidence
		}
		return nil
	}
	if endVersion <= startVersion {
		return ErrPublicationEvidence
	}
	rows, err := q.QueryContext(ctx, `SELECT id,ticket_version,trigger,from_state,to_state FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? ORDER BY ticket_version,id`, ref.Channel, ref.Project, ref.Ticket, startVersion, endVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	type transition struct {
		id      int64
		version uint64
		trigger string
		from    domain.State
		to      domain.State
	}
	var transitions []transition
	lastEventVersion := startVersion
	lastStateChangeVersion := startVersion
	for rows.Next() {
		var value transition
		if err := rows.Scan(&value.id, &value.version, &value.trigger, &value.from, &value.to); err != nil {
			return err
		}
		if (value.version != lastEventVersion && value.version != lastEventVersion+1) || !value.from.Valid() || !value.to.Valid() {
			return ErrPublicationEvidence
		}
		lastEventVersion = value.version
		if value.from == value.to {
			// Same-state events are evidence projections, not runner authority.
			// Only a publication_rebind paired with the exact resume event at the
			// same version is admissible; semantic self-transitions such as
			// checks_pending or bounded_phase_retry invalidate the old baseline.
			if value.trigger != "publication_rebind" || len(transitions) == 0 || transitions[len(transitions)-1].version != value.version || (transitions[len(transitions)-1].trigger != "operator_resume" && transitions[len(transitions)-1].trigger != "operator_retry") {
				return ErrPublicationEvidence
			}
			continue
		}
		// Between recovery rows only the three control transitions may change
		// lifecycle state. A phase transition such as checks_red or a source
		// refresh invalidates the prior phase result and cannot be mistaken for
		// a pause/take handoff.
		if value.version != lastStateChangeVersion+1 {
			return ErrPublicationEvidence
		}
		if len(transitions) > 0 && value.from != transitions[len(transitions)-1].to {
			return ErrPublicationEvidence
		}
		transitions = append(transitions, value)
		lastStateChangeVersion = value.version
	}
	if err := rows.Err(); err != nil || lastEventVersion != endVersion {
		return ErrPublicationEvidence
	}
	pauses := 0
	for index := 0; index < len(transitions); index++ {
		current := transitions[index]
		if current.trigger != "operator_pause_or_take" || current.to != domain.StateStopping || index+2 >= len(transitions) {
			return ErrPublicationEvidence
		}
		drained, resumed := transitions[index+1], transitions[index+2]
		if drained.trigger != "process_and_effects_drained" || drained.from != domain.StateStopping || drained.to != domain.StatePaused || (resumed.trigger != "operator_resume" && resumed.trigger != "operator_retry") || resumed.from != domain.StatePaused || !resumeTargetState(resumed.to) {
			return ErrPublicationEvidence
		}
		if current.version+1 != drained.version || drained.version+1 != resumed.version {
			return ErrPublicationEvidence
		}
		index += 2
		pauses++
	}
	if uint64(pauses) != endRunner-startRunner || (endLeader != startLeader && pauses == 0) {
		return ErrPublicationEvidence
	}
	return nil
}

// validateRunnerPhaseAdvance admits one ordinary phase_pass lifecycle
// transition between an immutable provider result and a later recovery row.
// It is deliberately narrower than a generic state-change count, so arbitrary
// ticket mutations cannot bridge a runner gap.
func validateRunnerPhaseAdvance(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, startVersion, startRunner, endVersion, endRunner uint64) error {
	if startVersion == 0 || startRunner == 0 || endVersion != startVersion+1 || endRunner != startRunner {
		return ErrPublicationEvidence
	}
	var transitions int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND ((from_state='planning' AND to_state='verifying') OR (from_state='verifying' AND to_state='building') OR (from_state='building' AND to_state='publishing'))`, ref.Channel, ref.Project, ref.Ticket, endVersion).Scan(&transitions); err != nil || transitions != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

// validateRunnerPhaseChain is the multi-phase form used when a recovery row
// follows several ordinary lifecycle completions. The final version may be a
// recovery-only version (with no state-changing event), but only when the
// signed recovery row occupies that exact endpoint.
func validateRunnerPhaseChain(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, startVersion, startRunner, endVersion, endRunner uint64) error {
	if startVersion == 0 || startRunner == 0 || endVersion <= startVersion || endRunner != startRunner {
		return ErrPublicationEvidence
	}
	rows, err := q.QueryContext(ctx, `SELECT ticket_version,trigger,from_state,to_state FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND from_state<>to_state ORDER BY ticket_version,id`, ref.Channel, ref.Project, ref.Ticket, startVersion, endVersion)
	if err != nil {
		return err
	}
	defer rows.Close()
	lastVersion := startVersion
	prior := domain.State("")
	count := 0
	for rows.Next() {
		var version uint64
		var trigger string
		var from, to domain.State
		if err := rows.Scan(&version, &trigger, &from, &to); err != nil || version != lastVersion+1 || !from.Valid() || !to.Valid() || (count > 0 && from != prior) || !validInitialLifecycleTransition(trigger, from, to) {
			return ErrPublicationEvidence
		}
		lastVersion, prior, count = version, to, count+1
	}
	if err := rows.Err(); err != nil || count == 0 {
		return ErrPublicationEvidence
	}
	if lastVersion == endVersion {
		return nil
	}
	if lastVersion+1 != endVersion {
		return ErrPublicationEvidence
	}
	step, found, err := loadRunnerRecoveryAt(ctx, q, ref, endVersion)
	if err != nil || !found || step.PriorTicketVersion != lastVersion || step.PriorRunnerEpoch != startRunner || step.RunnerEpoch != endRunner {
		return ErrPublicationEvidence
	}
	return nil
}

func resumeTargetState(state domain.State) bool {
	switch state {
	case domain.StatePlanning, domain.StateVerifying, domain.StateBuilding,
		domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing,
		domain.StateWaitingApproval, domain.StateWaitingManualMerge,
		domain.StateMerging, domain.StateReconciling:
		return true
	default:
		return false
	}
}

// validateRunnerRecoveryAuthority audits the complete ticket ledger before a
// current-fence reader accepts any provider result. A result whose own claim
// is current must not bypass a forged future row, invalid signed row, broken
// lineage, or the lifetime cap merely because it needs no recovery traversal.
// The source-to-target predecessor chain is checked separately by
// providerResultReachesFence.
func validateRunnerRecoveryAuthority(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, liveVersion uint64, liveFence domain.Fence) error {
	if liveVersion == 0 || liveFence.LeaderEpoch == 0 || liveFence.RunnerEpoch == 0 {
		return ErrPublicationEvidence
	}
	// Readers also authenticate immutable bindings at their historical fence.
	// Audit against the durable *current* ticket rather than that binding so a
	// legitimate later recovery is not mistaken for a forged future row, while
	// a row beyond the actual ticket remains fatal to every reader.
	var currentVersion, currentRunner, currentLeader uint64
	if err := q.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentVersion, &currentRunner, &currentLeader); err != nil || currentVersion == 0 || currentRunner == 0 || currentLeader == 0 {
		return ErrPublicationEvidence
	}
	liveVersion = currentVersion
	liveFence = domain.Fence{LeaderEpoch: currentLeader, RunnerEpoch: currentRunner}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return err
	}
	defer rows.Close()
	steps := make([]RunnerRecoveryLedger, 0, 8)
	created := make([]string, 0, 8)
	for rows.Next() {
		var step RunnerRecoveryLedger
		var createdAt string
		if err := rows.Scan(&step.PriorTicketVersion, &step.PriorRunnerEpoch, &step.PriorLeaderEpoch, &step.TicketVersion, &step.RunnerEpoch, &step.LeaderEpoch, &step.RecoveryDigest, &createdAt); err != nil {
			return err
		}
		step.Ref = ref
		steps = append(steps, step)
		created = append(created, createdAt)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	var previous *RunnerRecoveryLedger
	for index := range steps {
		step := steps[index]
		step.CreatedAt, err = parseRunnerRecoveryTime(created[index])
		if err != nil || step.TicketVersion > liveVersion || step.RunnerEpoch > liveFence.RunnerEpoch || step.LeaderEpoch > liveFence.LeaderEpoch || !validRunnerRecovery(step) {
			return ErrPublicationEvidence
		}
		var stateTransitions int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&stateTransitions); err != nil || stateTransitions != 0 {
			return ErrPublicationEvidence
		}
		if previous == nil {
			// The first row must begin at the ticket's durable initial counters,
			// reaching its predecessor only through recorded lifecycle/control
			// events. Its leader is the first independently witnessed leader.
			if step.PriorRunnerEpoch == 1 {
				if err := validateInitialLifecycleAdvance(ctx, q, ref, step.PriorTicketVersion); err != nil {
					return ErrPublicationEvidence
				}
			} else if err := validateRunnerControlAdvance(ctx, q, ref, 1, 1, step.PriorLeaderEpoch, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch); err != nil {
				return ErrPublicationEvidence
			}
		} else if !step.CreatedAt.After(previous.CreatedAt) || step.PriorTicketVersion < previous.TicketVersion || step.PriorLeaderEpoch < previous.LeaderEpoch {
			return ErrPublicationEvidence
		} else if err := validateRunnerControlAdvance(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, previous.LeaderEpoch, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch); err != nil {
			// Several ordinary phase completions may occur under one runner before
			// the next daemon recovery. The chain is accepted only when every
			// intervening event is a contiguous canonical lifecycle phase_pass.
			if err := validateRunnerPhaseChain(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, step.PriorTicketVersion, step.PriorRunnerEpoch); err != nil && validateRunnerVerificationAmendmentAdvance(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, previous.LeaderEpoch, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch) != nil {
				return ErrPublicationEvidence
			}
		}
		copy := step
		previous = &copy
	}
	// A current result may follow a pause while the daemon restarted: paused
	// tickets are intentionally not fenced, so its leader need not be recorded
	// in this ledger. The runner gap still requires the exact control history.
	// Historical recovery instead calls validateRunnerRecoveryLedger below and
	// therefore continues to require an exact source-to-live ledger chain.
	if previous != nil {
		if err := validateRunnerControlAdvance(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, previous.LeaderEpoch, liveVersion, liveFence.RunnerEpoch, liveFence.LeaderEpoch); err != nil {
			// A normal phase transition may follow the latest recovery row without
			// changing the runner. It is current phase authority, not a control
			// handoff; require the exact canonical phase event.
			if err := validateRunnerPhaseChain(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, liveVersion, liveFence.RunnerEpoch); err != nil && validateRunnerVerificationAmendmentAdvance(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, previous.LeaderEpoch, liveVersion, liveFence.RunnerEpoch, liveFence.LeaderEpoch) != nil {
				return ErrPublicationEvidence
			}
		}
	}
	return nil
}

// validateInitialLifecycleAdvance authenticates the ordinary ticket history
// before the first recovery row (start, phase completion, and other normative
// transitions). Once a phase result is the source, only recovery rows or the
// narrow control handoff are accepted by validateRunnerRecoveryLedger.
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
		if err := rows.Scan(&id, &version, &trigger, &from, &to, &payload); err != nil || id <= lastID || version != last+1 || trigger == "" || !from.Valid() || !to.Valid() || len(payload) > maxEvidenceJSON || !json.Valid([]byte(payload)) {
			return ErrPublicationEvidence
		}
		lastID = id
		last = version
		if from != prior {
			return ErrPublicationEvidence
		}
		if !validInitialLifecycleTransition(trigger, from, to) && !validInitialVerificationAmendmentRequest(ctx, q, ref, version, trigger, from, to, payload) && !validInitialVerificationAmendmentDecision(ctx, q, ref, version, trigger, from, to, payload) {
			return ErrPublicationEvidence
		}
		if trigger == "checks_green" {
			var ciTransitions int
			if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND event_id=? AND event_created_at=(SELECT created_at FROM events WHERE id=?) AND observation_classification='green' AND prior_state='waiting_ci' AND resulting_state='reviewing' AND resulting_trigger='checks_green'`, ref.Channel, ref.Project, ref.Ticket, version, id, id).Scan(&ciTransitions); err != nil || ciTransitions != 1 {
				return ErrPublicationEvidence
			}
		}
		prior = to
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if last != endVersion {
		return ErrPublicationEvidence
	}
	return nil
}

// validInitialVerificationAmendmentRequest admits the one Builder -> Reviewer
// lifecycle edge which is deliberately not a generic phase_pass.  It is used
// only while establishing the first startup-recovery predecessor: the event
// projection alone is never authority.  Rehydrate the immutable Store request
// at this exact transition version so a forged event, a missing correction
// budget, or a retargeted Builder result cannot bootstrap a recovery row.
func validInitialVerificationAmendmentRequest(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, version uint64, trigger string, from, to domain.State, payload string) bool {
	return validInitialVerificationAmendmentRequestAtFence(ctx, q, ref, 0, domain.Fence{}, version, trigger, from, to, payload)
}

// validInitialVerificationAmendmentRequestAtFence additionally binds the
// immutable request row to the exact recovery segment that is being bridged.
// Without this comparison, a well-formed request could be paired with an
// unrelated starting fence merely because its event exists at the expected
// ticket version.
func validInitialVerificationAmendmentRequestAtFence(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, consumedVersion uint64, consumedFence domain.Fence, version uint64, trigger string, from, to domain.State, payload string) bool {
	if version == 0 || trigger != "verification_amendment_requested" || from != domain.StateBuilding || to != domain.StateVerifying || payload != "{}" {
		return false
	}
	// loadVerificationAmendment performs the source/result/typed-artifact,
	// prior-revision, command, and exact correction-budget checks at the
	// immutable consumed endpoint.  It intentionally uses the row's persisted
	// fence rather than a caller-supplied live fence.
	var rowConsumedVersion, consumedLeader, consumedRunner uint64
	if q.QueryRowContext(ctx, `SELECT consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch FROM verification_amendment_requests WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&rowConsumedVersion, &consumedLeader, &consumedRunner) != nil || rowConsumedVersion+1 != version || consumedLeader == 0 || consumedRunner == 0 {
		return false
	}
	if consumedVersion != 0 && (rowConsumedVersion != consumedVersion || consumedFence.LeaderEpoch == 0 || consumedFence.RunnerEpoch == 0 || consumedLeader != consumedFence.LeaderEpoch || consumedRunner != consumedFence.RunnerEpoch) {
		return false
	}
	amendment, err := (&Store{}).loadVerificationAmendment(ctx, q, ref, version, domain.Fence{LeaderEpoch: consumedLeader, RunnerEpoch: consumedRunner})
	return err == nil && amendment.TransitionTicketVersion == version && amendment.ConsumedVersion+1 == version && amendment.Fence.LeaderEpoch == consumedLeader && amendment.Fence.RunnerEpoch == consumedRunner
}

// validInitialLifecycleTransition is deliberately narrower than the generic
// event schema.  The first-fence fallback may consume only the ordinary
// forward lifecycle; control, repair, and arbitrary audit transitions need
// their own authenticated authority and must not become a bootstrap bridge.
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
	case from == domain.StateWaitingCI && to == domain.StateReviewing:
		return trigger == "checks_green"
	default:
		return false
	}
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

// loadRuntimeControlEndpointLeader returns the leader that authenticated the
// current stopped/resumed identity. A phase claim's leader is only the source
// of the first recovery; after a pause/take and resume, the endpoint is the
// durable runtime-control authority at the live version/runner.
func loadRuntimeControlEndpointLeader(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, version, runner uint64) (uint64, bool, error) {
	if version == 0 || runner == 0 {
		return 0, false, ErrPublicationEvidence
	}
	var leader, authorityVersion, authorityRunner uint64
	err := q.QueryRowContext(ctx, `SELECT authority_leader_epoch,authority_version,authority_runner_epoch FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&leader, &authorityVersion, &authorityRunner)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if leader == 0 || authorityVersion != version || authorityRunner != runner {
		return 0, false, nil
	}
	return leader, true, nil
}

// loadRegisteredWorktreeEndpoint returns the immutable leader that registered
// the active worktree identity. It is a first-recovery predecessor when the
// registration's runner matches and its version is at or before the live
// ticket; an older version must first pass the full lifecycle proof. A stale
// path/identity/base row cannot mint runner authority.
func loadRegisteredWorktreeEndpoint(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, version, runner uint64) (uint64, bool, error) {
	if version == 0 || runner == 0 {
		return 0, false, ErrPublicationEvidence
	}
	var path, branch, identity, base, head, repository, baseRef string
	var registeredVersion, registeredLeader, registeredRunner uint64
	err := q.QueryRowContext(ctx, `SELECT w.path,w.branch_ref,w.identity_json,w.base_sha,w.head_sha,w.ticket_version,w.leader_epoch,w.runner_epoch,p.canonical_path,p.base_ref FROM worktrees w JOIN projects p ON p.channel=w.channel AND p.id=w.project_id WHERE w.channel=? AND w.project_id=? AND w.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&path, &branch, &identity, &base, &head, &registeredVersion, &registeredLeader, &registeredRunner, &repository, &baseRef)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !validStorePath(repository) || !validStorePath(path) || !boundedText(branch, 300) || !boundedText(baseRef, 300) || !validOID(base) || !validOID(head) || registeredRunner != runner || registeredVersion != version || registeredLeader == 0 || !validRepositoryWorktreeIdentity(identity, repository, path, branch, baseRef, base) {
		return 0, false, ErrPublicationEvidence
	}
	return registeredLeader, true, nil
}

// ProviderResultReachesFence authenticates an immutable completed provider
// result for use by a live worker.  Historical output is never made current
// by matching counters: every intervening fence must be a signed ledger row.
func (s *Store) ProviderResultReachesFence(ctx context.Context, key ProviderAttemptResultKey, expected uint64, fence domain.Fence) error {
	result, _, err := s.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil {
		return err
	}
	if err := s.AssertTicketFence(ctx, key.Ref, expected, fence); err != nil {
		return err
	}
	if result.Claim.Phase != domain.PhaseReview {
		if err := validateRunnerRecoveryAuthority(ctx, s.db, key.Ref, expected, fence); err != nil {
			return ErrStaleFence
		}
	}
	if err := providerResultReachesFence(ctx, s.db, key, result, expected, fence); err != nil {
		return ErrStaleFence
	}
	if result.Claim.Phase == domain.PhaseReview {
		// Review authority is not a generic phase bridge: it starts at the
		// authenticated green-CI reviewing endpoint and may cross only the exact
		// post-publication control/recovery lineage. This also audits recovery rows
		// that predate the reviewer claim.
		if _, err := s.FinalReviewAuthority(ctx, key.Ref, expected, fence); err != nil {
			return ErrStaleFence
		}
	}
	return nil
}

// providerResultReachesFence is the transaction-safe counterpart to
// ProviderResultReachesFence.  Rebinding keeps the immutable provider result
// and its command/Git witnesses untouched; it only appends a result binding
// after the source claim reaches the exact live recovery fence.
func providerResultReachesFence(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	return providerResultReachesFenceAt(ctx, q, key, result, expected, fence, true)
}

// providerResultReachesHistoricalFence authenticates a startup-only source
// claim to an already witnessed predecessor. FenceRecoveredRunners invokes
// this before appending the new signed recovery row: the daemon has already
// acquired the next leader, so requiring live daemon authority here would
// reject the historical predecessor by construction.
func providerResultReachesHistoricalFence(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	return providerResultReachesFenceAt(ctx, q, key, result, expected, fence, false)
}

func providerResultReachesFenceAt(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence, requireLiveAuthority bool) error {
	claim := result.Claim
	if key.Ref != claim.Ref || key.Phase != claim.Phase || key.AttemptID != claim.ID || key.Attempt != claim.Attempt || !providerRoleMatchesPhase(claim.Phase, claim.Role) || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ErrStaleFence
	}
	if requireLiveAuthority && claim.Phase != domain.PhaseReview {
		if err := validateRunnerRecoveryAuthority(ctx, q, key.Ref, expected, fence); err != nil {
			return ErrStaleFence
		}
	}
	if claim.ExpectedVersion == expected && claim.RunnerEpoch == fence.RunnerEpoch && claim.LeaderEpoch == fence.LeaderEpoch {
		return nil
	}
	// Verification amendments use decision-specific triggers rather than the
	// ordinary phase_pass bridge. Authenticate that exact request/Reviewer/
	// decision tuple before considering any generic recovery arithmetic.
	if claim.Phase == domain.PhaseVerification && claim.Role == "reviewer" && claim.ExpectedVersion < expected {
		boundary, boundaryErr := loadVerificationAmendmentBoundary(ctx, q, key.Ref, expected, fence)
		if boundaryErr == nil {
			if boundary.Reviewer != key {
				return ErrStaleFence
			}
			return nil
		}
		if !errors.Is(boundaryErr, ErrNotFound) {
			return ErrStaleFence
		}
	}
	if err := validateRunnerRecoveryLedger(ctx, q, key.Ref, claim.ExpectedVersion, claim.RunnerEpoch, claim.LeaderEpoch, expected, fence.RunnerEpoch, fence.LeaderEpoch); err == nil {
		return nil
	}
	// A result can predate exactly one canonical phase_pass transition before its
	// first runner recovery.  The transition must be the direct successor of the
	// immutable source claim and retain its runner; this is not a general
	// version-gap shortcut.  Planner, reviewer, and builder each have one such
	// supported boundary before the next phase has produced its own evidence.
	if claim.ExpectedVersion == ^uint64(0) {
		return ErrStaleFence
	}
	var from, to domain.State
	switch {
	case claim.Phase == domain.PhasePlanning && claim.Role == "planner":
		from, to = domain.StatePlanning, domain.StateVerifying
	case claim.Phase == domain.PhaseVerification && claim.Role == "reviewer":
		from, to = domain.StateVerifying, domain.StateBuilding
	case claim.Phase == domain.PhaseBuild && claim.Role == "builder":
		from, to = domain.StateBuilding, domain.StatePublishing
	default:
		return ErrStaleFence
	}
	var transitions int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='phase_pass' AND from_state=? AND to_state=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, claim.ExpectedVersion+1, from, to).Scan(&transitions); err != nil || transitions != 1 {
		return ErrStaleFence
	}
	if err := validateRunnerRecoveryLedger(ctx, q, key.Ref, claim.ExpectedVersion+1, claim.RunnerEpoch, claim.LeaderEpoch, expected, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
		return ErrStaleFence
	}
	return nil
}

func providerRoleMatchesPhase(phase domain.Phase, role string) bool {
	switch phase {
	case domain.PhasePlanning:
		return role == "planner"
	case domain.PhaseVerification:
		return role == "reviewer"
	case domain.PhaseBuild:
		return role == "builder"
	case domain.PhaseReview:
		return role == "reviewer"
	default:
		return false
	}
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
	// An operator pause/take invalidates the runner atomically at stopping,
	// then records drained->paused and resume as two ordinary ticket events.
	// For waiting_ci that places the resumed endpoint four versions after the
	// publication witness (publishing->waiting_ci, stop, paused, resume) and
	// advances the runner once. Authenticate that exact control triplet before
	// using the witness leader as the predecessor for startup fencing.
	if publication.CurrentTicketVersion <= ^uint64(0)-4 &&
		publication.CurrentFence.RunnerEpoch < ^uint64(0) &&
		publication.CurrentTicketVersion+4 == version &&
		publication.CurrentFence.RunnerEpoch+1 == runner {
		stopped := Ticket{Ref: ref, Version: publication.CurrentTicketVersion + 1, RunnerEpoch: publication.CurrentFence.RunnerEpoch, State: domain.StateWaitingCI}
		current := Ticket{Ref: ref, Version: version, RunnerEpoch: runner, State: domain.StateWaitingCI}
		stop := mutationRevocation{version: publication.CurrentTicketVersion + 2, runner: runner, leader: publication.CurrentFence.LeaderEpoch}
		if authenticatePostPublicationResume(ctx, conn, ref, stopped, current, stop) == nil {
			return publication.CurrentFence.LeaderEpoch, true, nil
		}
	}
	// A publishing witness may be consumed by the one-version
	// publishing->waiting_ci transition before its first recovery fence. In
	// that case the current ticket is exactly one version beyond the witness,
	// with the same runner; this is still the witness's authenticated leader
	// predecessor, not a counter-based inference.
	if publication.CurrentFence.RunnerEpoch != runner {
		return 0, false, nil
	}
	if publication.CurrentTicketVersion == version || publication.CurrentTicketVersion+1 == version {
		return publication.CurrentFence.LeaderEpoch, true, nil
	}
	// waiting_ci may have crossed an authenticated typed-blocker or semantic
	// pause/resume pair after the publication->waiting transition. The resumed
	// endpoint is the immutable publication witness plus two lifecycle versions;
	// only that exact pair may bridge to the first runner recovery.
	if publication.CurrentTicketVersion > ^uint64(0)-3 || publication.CurrentTicketVersion+3 != version {
		return 0, false, nil
	}
	if err := authenticatePublishedWaitingEvent(ctx, conn, ref, publication, publication.CurrentTicketVersion+1); err != nil {
		return 0, false, nil
	}
	if authenticateBlockedPublicationResume(ctx, conn, ref, publication.CurrentTicketVersion+2, version, domain.StateWaitingCI, domain.StateWaitingCI) != nil && authenticateSemanticPublicationResume(ctx, conn, ref, publication.CurrentTicketVersion+2, version, domain.StateWaitingCI) != nil {
		return 0, false, nil
	}
	return publication.CurrentFence.LeaderEpoch, true, nil
}

// postPublicationRecoveryBaseline authenticates a restart after an operator
// pause/take has been resumed in a publication-sensitive state. The durable
// stop row identifies the invalidated endpoint, while the three exact control
// events identify the resumed endpoint. Business evidence is then checked at
// the pre-stop endpoint before its leader is used as the predecessor for a
// normal signed +1/+1 recovery row.
func (s *Store) postPublicationRecoveryBaseline(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	if !postPublicationState(state) || version < 2 || runner == 0 || newLeader == 0 {
		return 0, false, nil
	}
	var controls int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&controls); err != nil {
		return 0, false, err
	}
	if controls == 0 {
		if version < 3 || runner <= 1 {
			return 0, false, nil
		}
		triplet, err := postPublicationControlTripletPresent(ctx, conn, ref, state, version)
		if err != nil {
			return 0, false, err
		}
		if triplet {
			return 0, false, ErrPublicationEvidence
		}
		return 0, false, nil
	}
	control, err := runtimeControlFrom(ctx, conn, ref)
	if err != nil {
		return 0, false, ErrPublicationEvidence
	}
	if semanticMergeRetryControl(control) {
		current := Ticket{Ref: ref, State: state, Version: version, RunnerEpoch: runner}
		priorLeader, semanticErr := s.authenticatePostPublicationSemanticRetryPreFence(ctx, conn, ref, control, current)
		if semanticErr != nil || priorLeader == 0 || priorLeader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		return priorLeader, true, nil
	}
	if state == domain.StateReconciling && controlledMergeReconcileShape(control) {
		current := Ticket{Ref: ref, State: state, Version: version, RunnerEpoch: runner}
		endpointLeader := uint64(0)
		switch {
		case version == control.authority.version && runner == control.authority.runner:
			endpointLeader = control.authority.leader
		default:
			recovery, found, recoveryErr := loadRunnerRecoveryAt(ctx, conn, ref, version)
			if recoveryErr != nil || !found || !validRunnerRecovery(recovery) || recovery.TicketVersion != version || recovery.RunnerEpoch != runner {
				return 0, false, ErrPublicationEvidence
			}
			endpointLeader = recovery.LeaderEpoch
		}
		if endpointLeader == 0 || endpointLeader >= newLeader || s.authenticateControlledMergeToReconcile(ctx, conn, ref, control, current, endpointLeader) != nil {
			return 0, false, ErrPublicationEvidence
		}
		return endpointLeader, true, nil
	}
	if state == domain.StateReconciling && semanticMergeReconcileShape(control) {
		current := Ticket{Ref: ref, State: state, Version: version, RunnerEpoch: runner}
		endpointLeader := uint64(0)
		switch {
		case version == control.authority.version && runner == control.authority.runner:
			endpointLeader = control.authority.leader
		default:
			recovery, found, recoveryErr := loadRunnerRecoveryAt(ctx, conn, ref, version)
			if recoveryErr != nil || !found || !validRunnerRecovery(recovery) || recovery.TicketVersion != version || recovery.RunnerEpoch != runner {
				return 0, false, ErrPublicationEvidence
			}
			endpointLeader = recovery.LeaderEpoch
		}
		if endpointLeader != 0 && endpointLeader < newLeader && s.authenticateSemanticMergeToReconcile(ctx, conn, ref, control, current, endpointLeader) == nil {
			return endpointLeader, true, nil
		}
	}
	if state == domain.StateMerging && controlledApprovalMergeShape(control) {
		// Crash after an exact approval and before the first merge intent/effect.
		// The open authority was advanced atomically to the approval successor;
		// restoreRuntimeControls sealed it on reopen. Authenticate that complete
		// approval lineage before minting the first signed recovery row.
		if version != control.authority.version || runner != control.authority.runner || control.authority.leader == 0 || control.authority.leader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		current := Ticket{Ref: ref, State: state, Version: version, RunnerEpoch: runner}
		if s.authenticateControlledApprovalToMerging(ctx, conn, ref, control, current, control.authority.leader) != nil {
			return 0, false, ErrPublicationEvidence
		}
		return control.authority.leader, true, nil
	}
	// A manual external-merge observation is an authenticated same-runner
	// waiting_manual_merge -> reconciling successor. A runtime that was rearmed
	// at the waiting endpoint advances its open authority atomically with that
	// observation, so validate that direct endpoint here. Guarded reconciling
	// with the same sealed/current counters is instead a normal pause/resume of
	// an already-reconciling ticket; it must continue into the generic exact
	// control-triplet and guarded-merge proof below.
	if state == domain.StateReconciling && control.state == "sealed" &&
		control.authority.version == version && control.authority.runner == runner {
		var mergeMode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&mergeMode); err != nil {
			return 0, false, ErrPublicationEvidence
		}
		switch mergeMode {
		case domain.MergeManual:
			origin, err := s.reconcilingRecoveryEndpoint(ctx, conn, ref)
			if err != nil || origin.version != version || origin.runner != runner || origin.leader != control.authority.leader || origin.leader == 0 || origin.leader >= newLeader {
				return 0, false, ErrPublicationEvidence
			}
			return origin.leader, true, nil
		case domain.MergeGuarded:
			// Continue below: authenticate the exact pause/drain/resume triplet
			// and the immutable guarded reconciling origin at the pre-stop fence.
		default:
			return 0, false, ErrPublicationEvidence
		}
	}
	if version < 3 || runner <= 1 {
		return 0, false, nil
	}
	if control.state != "sealed" || control.stop.version == 0 || control.stop.runner == 0 || control.stop.leader == 0 {
		return 0, false, ErrPublicationEvidence
	}
	if control.authority != control.stop {
		// ActivateRearm records the resumed live tuple as authority before the
		// scheduler's first Begin. A crash in that narrow handoff restores the
		// row as sealed; accept it only when the authority is exactly the current
		// ticket identity and the original stop/resume chain still authenticates.
		if control.authority.version == version && control.authority.runner == runner {
			if control.authority.leader == 0 || control.authority.leader >= newLeader || control.stop.version > ^uint64(0)-2 || control.stop.runner > ^uint64(0)-1 {
				return 0, false, ErrPublicationEvidence
			}
			// With no intervening recovery, authority is the resumed endpoint
			// itself: stop+2 in the ticket stream and the original leader. When
			// a prior startup already fenced that endpoint, authority is instead
			// the later current tuple. Authenticate the complete signed ledger
			// from the resumed stop endpoint to that authority; never infer it
			// from the counter gap.
			if version == control.stop.version+2 && runner == control.stop.runner {
				if control.authority.leader != control.stop.leader {
					return 0, false, ErrPublicationEvidence
				}
			} else {
				if version <= control.stop.version+2 || runner <= control.stop.runner || control.authority.leader <= control.stop.leader {
					return 0, false, ErrPublicationEvidence
				}
				if err := validateRunnerRecoveryLedger(ctx, conn, ref, control.stop.version+2, control.stop.runner, control.stop.leader, version, runner, control.authority.leader); err != nil {
					return 0, false, ErrPublicationEvidence
				}
			}
		} else {
			// A valid but stale control row can remain after a later independently
			// authenticated phase/publication transition. It must not suppress that
			// authority, while malformed/current control gaps remain fail-closed.
			if control.authority.version > version || control.authority.runner > runner || control.authority.leader == 0 {
				return 0, false, ErrPublicationEvidence
			}
			return 0, false, nil
		}
	} else if control.stop.version != version-2 || control.stop.runner != runner {
		return 0, false, ErrPublicationEvidence
	}
	if control.stop.version == 0 || control.stop.runner <= 1 {
		return 0, false, ErrPublicationEvidence
	}
	baseline := Ticket{Ref: ref, Version: control.stop.version - 1, RunnerEpoch: control.stop.runner - 1, State: state}
	current := Ticket{Ref: ref, Version: version, RunnerEpoch: runner, State: state}
	currentLeader := control.stop.leader
	if control.authority != control.stop {
		currentLeader = control.authority.leader
	}
	if state == domain.StateMerging || state == domain.StateReconciling {
		prior := normalRecoveryEndpoint{version: baseline.Version, runner: baseline.RunnerEpoch, leader: control.stop.leader}
		resumed := normalRecoveryEndpoint{version: current.Version, runner: current.RunnerEpoch, leader: currentLeader}
		if err := authenticateCurrentPostPublicationEndpointBridge(ctx, conn, ref, state, prior, resumed); err != nil {
			// Once a sealed control row and a post-publication mutation
			// endpoint are present, malformed or contradictory evidence is
			// not an ordinary "not this baseline" miss. Falling through to
			// a weaker publication/provider fallback could mint a recovery
			// row without authenticating the mutation lineage.
			return 0, false, ErrPublicationEvidence
		}
	} else if err := authenticatePostPublicationResume(ctx, conn, ref, baseline, current, control.stop); err != nil {
		return 0, false, ErrPublicationEvidence
	}
	if err := s.authenticatePostPublicationState(ctx, conn, ref, state, baseline.Version, domain.Fence{LeaderEpoch: control.stop.leader, RunnerEpoch: baseline.RunnerEpoch}); err != nil {
		return 0, false, ErrPublicationEvidence
	}
	return currentLeader, true, nil
}

// normalPostPublicationRecoveryPredecessor is the no-control restart bridge
// for post-publication states. It derives the predecessor from immutable
// final-review, approval, and (when present) merge evidence, never from state
// counters alone.
func (s *Store) normalPostPublicationRecoveryPredecessor(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	if version == 0 || runner == 0 || newLeader == 0 {
		return 0, false, nil
	}
	current := normalRecoveryEndpoint{version: version, runner: runner}
	switch state {
	case domain.StateWaitingApproval, domain.StateWaitingManualMerge:
		baseline, err := s.finalReviewRecoveryEndpoint(ctx, conn, ref, state)
		if err != nil {
			return 0, false, err
		}
		if baseline.leader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		current.leader, err = normalRecoveryLeaderAt(ctx, conn, ref, baseline, version, runner)
		if err != nil {
			return 0, false, err
		}
		if current.leader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		return current.leader, true, nil
	case domain.StateMerging:
		baseline, err := s.approvalRecoveryEndpoint(ctx, conn, ref)
		if err != nil {
			return 0, false, err
		}
		if baseline.leader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		current.leader, err = normalRecoveryLeaderAt(ctx, conn, ref, baseline, version, runner)
		if err != nil {
			return 0, false, err
		}
		if current.leader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		intent, found, err := singleRecoveryMergeIntent(ctx, conn, ref)
		if err != nil {
			return 0, false, err
		}
		// A daemon may die immediately after approval and before the merge intent
		// is written. Once an intent exists, however, its exact immutable launch
		// tuple and the stranded effect are mandatory recovery authority.
		if found {
			if err := s.authenticateMergingRecoveryEffect(ctx, conn, ref, baseline, current, newLeader, intent); err != nil {
				return 0, false, ErrPublicationEvidence
			}
		} else if err := authenticateUnissuedMergeEffect(ctx, conn, ref, current, newLeader); err != nil {
			return 0, false, ErrPublicationEvidence
		}
		return current.leader, true, nil
	case domain.StateReconciling:
		if manual, found, err := loadManualMergeObservation(ctx, conn, ref); err != nil {
			return 0, false, err
		} else if found {
			if err := validManualMergeObservation(manual); err != nil || manual.CurrentTicketVersion == ^uint64(0) {
				return 0, false, ErrPublicationEvidence
			}
			reconciling := normalRecoveryEndpoint{version: manual.CurrentTicketVersion + 1, runner: manual.CurrentFence.RunnerEpoch, leader: manual.CurrentFence.LeaderEpoch}
			var events, stateChanges int
			if err := conn.QueryRowContext(ctx, `SELECT
				COALESCE(SUM(CASE WHEN trigger='external_merge_observed' AND from_state='waiting_manual_merge' AND to_state='reconciling' AND payload=? THEN 1 ELSE 0 END),0),
				COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0)
				FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, manualMergeObservationEventPayload(manual.ObservationDigest), ref.Channel, ref.Project, ref.Ticket, reconciling.version).Scan(&events, &stateChanges); err != nil || events != 1 || stateChanges != 1 {
				return 0, false, ErrPublicationEvidence
			}
			current.leader, err = normalRecoveryLeaderAt(ctx, conn, ref, reconciling, version, runner)
			if err != nil {
				return 0, false, err
			}
			if current.leader >= newLeader {
				return 0, false, ErrPublicationEvidence
			}
			return current.leader, true, nil
		}
		baseline, err := s.confirmedMergeRecoveryEndpoint(ctx, conn, ref)
		if err != nil {
			return 0, false, err
		}
		if baseline.version == ^uint64(0) {
			return 0, false, ErrPublicationEvidence
		}
		reconciling := normalRecoveryEndpoint{version: baseline.version + 1, runner: baseline.runner, leader: baseline.leader}
		if err := canonicalGuardedMergeObservation(ctx, conn, ref, reconciling.version); err != nil {
			return 0, false, ErrPublicationEvidence
		}
		current.leader, err = normalRecoveryLeaderAt(ctx, conn, ref, reconciling, version, runner)
		if err != nil {
			return 0, false, err
		}
		if current.leader >= newLeader {
			return 0, false, ErrPublicationEvidence
		}
		return current.leader, true, nil
	default:
		return 0, false, nil
	}
}

type normalRecoveryEndpoint struct {
	version uint64
	runner  uint64
	leader  uint64
}

// canonicalGuardedMergeObservation proves the sole state-changing event at a
// reconciling version is the default-payload guarded merging handoff. Events
// have audit entries as well as transitions, so filtering first by the desired
// row is insufficient: a second state change at the same version must fail
// closed instead of being silently ignored.
func canonicalGuardedMergeObservation(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, reconcilingVersion uint64) error {
	if reconcilingVersion == 0 {
		return ErrPublicationEvidence
	}
	var exactEvents, stateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN trigger='merge_observed' AND from_state='merging' AND to_state='reconciling' AND payload='{}' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, reconcilingVersion).Scan(&exactEvents, &stateChanges); err != nil || exactEvents != 1 || stateChanges != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

func (s *Store) finalReviewRecoveryEndpoint(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State) (normalRecoveryEndpoint, error) {
	if state != domain.StateWaitingApproval && state != domain.StateWaitingManualMerge {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	// This reader is also used after the ticket has entered merging or
	// reconciling, so it must derive the immutable completion endpoint rather
	// than assuming the requested waiting state is still live.
	var completionVersion uint64
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(MAX(ticket_version),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass' AND from_state='reviewing' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, state).Scan(&completionVersion); err != nil || completionVersion == 0 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	// A correction can legitimately produce an earlier review_pass for an
	// invalidated candidate.  The latest pass is the only candidate for the
	// current publication, but its ticket version must still contain exactly
	// one state-changing review_pass rather than an ambiguous audit bundle.
	var matching, stateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(CASE WHEN trigger='review_pass' AND from_state='reviewing' AND to_state=? THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0)
		FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, state, ref.Channel, ref.Project, ref.Ticket, completionVersion).Scan(&matching, &stateChanges); err != nil || matching != 1 || stateChanges != 1 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	endpoint, err := s.reviewCompletionRecoveryEndpoint(ctx, conn, ref, state, completionVersion)
	if err != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	return endpoint, nil
}

func (s *Store) approvalRecoveryEndpoint(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (normalRecoveryEndpoint, error) {
	waiting, err := s.finalReviewRecoveryEndpoint(ctx, conn, ref, domain.StateWaitingApproval)
	if err != nil {
		return normalRecoveryEndpoint{}, err
	}
	candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
	if err != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	var approvalVersion uint64
	var approvals int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(ticket_version),0) FROM approvals WHERE channel=? AND project_id=? AND ticket_id=? AND reviewed_head=? AND decision='approved' AND invalidated=0`, ref.Channel, ref.Project, ref.Ticket, candidate.Snapshot.HeadSHA).Scan(&approvals, &approvalVersion); err != nil || approvals != 1 || approvalVersion == 0 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if approvalVersion < waiting.version || approvalVersion-waiting.version > 64 || approvalVersion == ^uint64(0) {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	// The waiting_approval endpoint can be historical by the time a merge
	// observation/reconciliation asks for it.  Collect only independently
	// authenticated endpoint witnesses, then require them to agree.  In
	// particular, never derive a leader from a version delta: pause/resume
	// events deliberately do not contain one.
	candidates := make([]normalRecoveryEndpoint, 0, 4)
	appendCandidate := func(value normalRecoveryEndpoint) error {
		if value.version != approvalVersion || value.runner == 0 || value.leader == 0 {
			return ErrPublicationEvidence
		}
		if err := validatePostPublicationEndpointAdvance(ctx, conn, ref, domain.StateWaitingApproval, waiting, value); err != nil {
			return ErrPublicationEvidence
		}
		for _, existing := range candidates {
			if existing == value {
				return nil
			}
		}
		candidates = append(candidates, value)
		return nil
	}
	if approvalVersion == waiting.version {
		if err := appendCandidate(waiting); err != nil {
			return normalRecoveryEndpoint{}, err
		}
	}
	// Do not use the live daemon leader as a historical endpoint witness.  A
	// leader takeover can occur before its signed recovery row exists; that
	// leader is precisely what this proof must not trust.  The waiting
	// endpoint, runtime control authority, signed recovery, and merge intent
	// below are all immutable/counter-bound witnesses instead.
	intent, foundIntent, intentErr := singleRecoveryMergeIntent(ctx, conn, ref)
	if intentErr != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if foundIntent {
		if approvalVersion == ^uint64(0) || intent.TicketVersion <= approvalVersion {
			return normalRecoveryEndpoint{}, ErrPublicationEvidence
		}
		// Only an intent at the direct merging successor can witness the
		// historical approval endpoint. A newer intent is downstream evidence
		// (for example after a signed daemon recovery) and is authenticated later
		// by authenticateMergingRecoveryEffect; it must not make the older
		// approval baseline ambiguous.
		if intent.TicketVersion == approvalVersion+1 {
			if err := appendCandidate(normalRecoveryEndpoint{version: approvalVersion, runner: intent.RunnerEpoch, leader: intent.LeaderEpoch}); err != nil {
				return normalRecoveryEndpoint{}, err
			}
		}
	}
	control, controlErr := runtimeControlFrom(ctx, conn, ref)
	if controlErr != nil && !errors.Is(controlErr, ErrStaleFence) {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if controlErr == nil && control.authority.version == approvalVersion {
		if (control.state != "open" && control.state != "sealed") || control.authority.runner == 0 || control.authority.leader == 0 {
			return normalRecoveryEndpoint{}, ErrPublicationEvidence
		}
		if err := appendCandidate(normalRecoveryEndpoint{version: control.authority.version, runner: control.authority.runner, leader: control.authority.leader}); err != nil {
			return normalRecoveryEndpoint{}, err
		}
	}
	// A pause/take stores the stopping endpoint (pre-stop version +1) and the
	// incremented runner. After drained + resume/retry, the historical waiting
	// endpoint is therefore stop.version+2. This remains available after the
	// open authority advances through approval and a later daemon recovery.
	// appendCandidate independently authenticates the exact control triplet.
	if controlErr == nil && control.stop.version <= ^uint64(0)-2 && control.stop.version+2 == approvalVersion {
		if (control.state != "open" && control.state != "sealed") || control.stop.runner == 0 || control.stop.leader == 0 {
			return normalRecoveryEndpoint{}, ErrPublicationEvidence
		}
		if err := appendCandidate(normalRecoveryEndpoint{version: approvalVersion, runner: control.stop.runner, leader: control.stop.leader}); err != nil {
			return normalRecoveryEndpoint{}, err
		}
	}
	recovery, foundRecovery, recoveryErr := loadRunnerRecoveryAt(ctx, conn, ref, approvalVersion)
	if recoveryErr != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	if foundRecovery {
		if !validRunnerRecovery(recovery) || recovery.TicketVersion != approvalVersion {
			return normalRecoveryEndpoint{}, ErrPublicationEvidence
		}
		if err := appendCandidate(normalRecoveryEndpoint{version: recovery.TicketVersion, runner: recovery.RunnerEpoch, leader: recovery.LeaderEpoch}); err != nil {
			return normalRecoveryEndpoint{}, err
		}
	}
	if len(candidates) != 1 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	preApproval := candidates[0]
	if preApproval.version != approvalVersion {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	var payload string
	var events, stateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0),COALESCE(MAX(payload),'') FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_approved' AND from_state='waiting_approval' AND to_state='merging'`, ref.Channel, ref.Project, ref.Ticket, approvalVersion+1).Scan(&events, &stateChanges, &payload); err != nil || events != 1 || stateChanges != 1 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	var totalStateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, approvalVersion+1).Scan(&totalStateChanges); err != nil || totalStateChanges != 1 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	var approved map[string]string
	if json.Unmarshal([]byte(payload), &approved) != nil || approved["head"] != candidate.Snapshot.HeadSHA || (approved["reason_digest"] != "" && !validSHA256(approved["reason_digest"])) {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	canonical, err := json.Marshal(approved)
	if err != nil || string(canonical) != payload || len(approved) > 2 {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	return normalRecoveryEndpoint{version: approvalVersion + 1, runner: preApproval.runner, leader: preApproval.leader}, nil
}

// normalRecoveryLeaderAt authenticates a historical endpoint through only
// contiguous +1/+1 runner-recovery rows. Unlike validateRunnerRecoveryLedger,
// it deliberately stops at targetVersion so immutable merge intent/effect
// endpoints remain provable after later daemon restarts append more rows.
func normalRecoveryLeaderAt(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, source normalRecoveryEndpoint, targetVersion, targetRunner uint64) (uint64, error) {
	if source.version == 0 || source.runner == 0 || source.leader == 0 || targetVersion < source.version || targetRunner < source.runner || targetVersion-source.version != targetRunner-source.runner || targetVersion-source.version > 64 {
		return 0, ErrPublicationEvidence
	}
	if err := validateRunnerRecoveryCardinality(ctx, conn, ref); err != nil {
		return 0, err
	}
	if targetVersion == source.version {
		return source.leader, nil
	}
	rows, err := conn.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, source.version, targetVersion)
	if err != nil {
		return 0, err
	}
	steps := make([]RunnerRecoveryLedger, 0, targetVersion-source.version)
	for rows.Next() {
		var step RunnerRecoveryLedger
		var createdAt string
		if err := rows.Scan(&step.PriorTicketVersion, &step.PriorRunnerEpoch, &step.PriorLeaderEpoch, &step.TicketVersion, &step.RunnerEpoch, &step.LeaderEpoch, &step.RecoveryDigest, &createdAt); err != nil {
			return 0, err
		}
		step.Ref = ref
		step.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
		if err != nil || !validRunnerRecovery(step) {
			rows.Close()
			return 0, ErrPublicationEvidence
		}
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	expected := source
	var previousCreated time.Time
	for _, step := range steps {
		if step.PriorTicketVersion != expected.version || step.PriorRunnerEpoch != expected.runner || step.PriorLeaderEpoch != expected.leader {
			return 0, ErrPublicationEvidence
		}
		if !previousCreated.IsZero() && !step.CreatedAt.After(previousCreated) {
			return 0, ErrPublicationEvidence
		}
		var transitions int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&transitions); err != nil || transitions != 0 {
			return 0, ErrPublicationEvidence
		}
		expected = normalRecoveryEndpoint{version: step.TicketVersion, runner: step.RunnerEpoch, leader: step.LeaderEpoch}
		previousCreated = step.CreatedAt
	}
	if uint64(len(steps)) != targetVersion-source.version || expected.version != targetVersion || expected.runner != targetRunner {
		return 0, ErrPublicationEvidence
	}
	return expected.leader, nil
}

// authenticateUnissuedMergeEffect handles only the proven pre-handoff crash
// window. MergeExactHead records the immutable intent before it enters the
// mutation guard, so an uncertain claim with no intent cannot have launched a
// hosted merge. Confirmed/executing rows without an intent are contradictory
// and fail closed.
func authenticateUnissuedMergeEffect(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, current normalRecoveryEndpoint, newLeader uint64) error {
	rows, err := conn.QueryContext(ctx, `SELECT semantic_key,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest,observed_identity FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind='merge' ORDER BY semantic_key`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return err
	}
	var effects []Effect
	for rows.Next() {
		var effect Effect
		if err := rows.Scan(&effect.SemanticKey, &effect.Kind, &effect.State, &effect.TicketVersion, &effect.LeaderEpoch, &effect.RunnerEpoch, &effect.ClaimEpoch, &effect.RequestDigest, &effect.ObservedIdentity); err != nil {
			rows.Close()
			return err
		}
		effect.Ref = ref
		effects = append(effects, effect)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(effects) == 0 {
		return nil
	}
	if len(effects) != 1 {
		return ErrPublicationEvidence
	}
	effect := effects[0]
	if effect.State == EffectPlanned {
		return nil
	}
	if effect.State != EffectUncertain || effect.ObservedIdentity != "" || effect.TicketVersion != current.version || effect.RunnerEpoch != current.runner || effect.LeaderEpoch != newLeader || effect.ClaimEpoch == 0 || effect.RequestDigest == "" {
		return ErrPublicationEvidence
	}
	return nil
}

func singleRecoveryMergeIntent(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (domain.MergeIntent, bool, error) {
	rows, err := conn.QueryContext(ctx, `SELECT semantic_key FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY semantic_key`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return domain.MergeIntent{}, false, err
	}
	var key string
	count := 0
	for rows.Next() {
		if err := rows.Scan(&key); err != nil {
			return domain.MergeIntent{}, false, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.MergeIntent{}, false, err
	}
	rows.Close()
	if count == 0 {
		return domain.MergeIntent{}, false, nil
	}
	if count != 1 {
		return domain.MergeIntent{}, false, ErrPublicationEvidence
	}
	intent, found, err := mergeIntentFrom(ctx, conn, key)
	if err != nil || !found || intent.Ref != ref || validMergeIntent(intent) != nil {
		return domain.MergeIntent{}, false, ErrPublicationEvidence
	}
	publication, err := loadCIPublicationBase(ctx, conn, ref)
	if err != nil {
		return domain.MergeIntent{}, false, ErrPublicationEvidence
	}
	pr := publication.PullRequest
	if intent.RepositoryHost != pr.Repository.Host || intent.RepositoryOwner != pr.Repository.Owner || intent.RepositoryName != pr.Repository.Name || intent.PullRequestNumber != pr.Number || intent.HeadOwner != pr.HeadOwner || intent.HeadRepository != pr.HeadRepository || intent.HeadRef != pr.HeadRef || intent.HeadOID != pr.HeadOID || intent.BaseRef != pr.BaseRef || intent.OriginalBaseOID != pr.BaseOID {
		return domain.MergeIntent{}, false, ErrPublicationEvidence
	}
	return intent, true, nil
}

func (s *Store) authenticateMergingRecoveryEffect(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, approval, current normalRecoveryEndpoint, newLeader uint64, intent domain.MergeIntent) error {
	intentEndpoint := normalRecoveryEndpoint{version: intent.TicketVersion, runner: intent.RunnerEpoch, leader: intent.LeaderEpoch}
	if intentEndpoint.version < approval.version || intentEndpoint.runner < approval.runner {
		return ErrPublicationEvidence
	}
	intentLeader, err := normalRecoveryLeaderAt(ctx, conn, ref, approval, intentEndpoint.version, intentEndpoint.runner)
	if err != nil || intentLeader != intentEndpoint.leader {
		return ErrPublicationEvidence
	}
	effect, err := effectFrom(ctx, conn, intent.SemanticKey)
	if err != nil || effect.Ref != ref || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest {
		return ErrPublicationEvidence
	}
	switch effect.State {
	case EffectUncertain:
		if effect.ObservedIdentity != "" || effect.TicketVersion != intent.TicketVersion || effect.RunnerEpoch != intent.RunnerEpoch || effect.LeaderEpoch != newLeader || current.version < intent.TicketVersion || current.runner < intent.RunnerEpoch {
			return ErrPublicationEvidence
		}
		delta := current.version - intent.TicketVersion
		if delta != current.runner-intent.RunnerEpoch || intent.ClaimEpoch > ^uint64(0)-delta {
			return ErrPublicationEvidence
		}
		// ReconcileEffects revokes the external claimant once per leader change.
		// FenceRecoveredRunners then advances the ticket/runner; that fence does
		// not claim the effect a second time. The first complete recovery is
		// therefore intent+1, not intent+2. Interrupted recovery attempts may
		// add further claim epochs, bounded below by the ticket delta and above
		// by the authenticated leader delta.
		minimumClaim := intent.ClaimEpoch + delta
		if effect.ClaimEpoch < minimumClaim || newLeader <= intent.LeaderEpoch || effect.ClaimEpoch-intent.ClaimEpoch > newLeader-intent.LeaderEpoch {
			return ErrPublicationEvidence
		}
	case EffectConfirmed, EffectFailed:
		effectEndpoint := normalRecoveryEndpoint{version: effect.TicketVersion, runner: effect.RunnerEpoch, leader: effect.LeaderEpoch}
		leader, err := normalRecoveryLeaderAt(ctx, conn, ref, intentEndpoint, effectEndpoint.version, effectEndpoint.runner)
		normal := err == nil && leader == effectEndpoint.leader && validRecoveredMergeClaimAdvance(intent, effectEndpoint, effect.ClaimEpoch)
		promotedAncestor := false
		if !normal && effect.State == EffectConfirmed {
			if effectEndpoint.version > current.version || effectEndpoint.runner > current.runner {
				normal = s.authenticatePromotedConfirmedMergeEffect(ctx, conn, ref, intent, effect, current) == nil
				promotedAncestor = normal
			} else {
				normal = s.authenticatePromotedConfirmedMergeEffect(ctx, conn, ref, intent, effect) == nil
			}
		}
		if !normal || effectEndpoint.version < intentEndpoint.version || (!promotedAncestor && (effectEndpoint.version > current.version || effectEndpoint.runner > current.runner)) {
			return ErrPublicationEvidence
		}
		if (effect.State == EffectConfirmed && effect.ObservedIdentity == "") || (effect.State == EffectFailed && effect.ObservedIdentity != "") {
			return ErrPublicationEvidence
		}
		if promotedAncestor {
			return nil
		}
		if err := authenticatePostPublicationEndpointBridge(ctx, conn, ref, domain.StateMerging, effectEndpoint, current); err != nil {
			return ErrPublicationEvidence
		}
	default:
		return ErrPublicationEvidence
	}
	return nil
}

func (s *Store) confirmedMergeRecoveryEndpoint(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (normalRecoveryEndpoint, error) {
	approval, err := s.approvalRecoveryEndpoint(ctx, conn, ref)
	if err != nil {
		return normalRecoveryEndpoint{}, err
	}
	intent, found, err := singleRecoveryMergeIntent(ctx, conn, ref)
	if err != nil || !found {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	intentEndpoint := normalRecoveryEndpoint{version: intent.TicketVersion, runner: intent.RunnerEpoch, leader: intent.LeaderEpoch}
	leader, err := normalRecoveryLeaderAt(ctx, conn, ref, approval, intentEndpoint.version, intentEndpoint.runner)
	if err != nil || leader != intentEndpoint.leader {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	effect, err := effectFrom(ctx, conn, intent.SemanticKey)
	if err != nil || effect.Ref != ref || effect.Kind != "merge" || effect.RequestDigest != intent.RequestDigest || effect.State != EffectConfirmed || effect.ObservedIdentity == "" || effect.TicketVersion < intent.TicketVersion || effect.RunnerEpoch < intent.RunnerEpoch {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	effectEndpoint := normalRecoveryEndpoint{version: effect.TicketVersion, runner: effect.RunnerEpoch, leader: effect.LeaderEpoch}
	leader, err = normalRecoveryLeaderAt(ctx, conn, ref, intentEndpoint, effectEndpoint.version, effectEndpoint.runner)
	if err == nil && leader == effectEndpoint.leader && validRecoveredMergeClaimAdvance(intent, effectEndpoint, effect.ClaimEpoch) {
		return effectEndpoint, nil
	}
	if err := s.authenticatePromotedConfirmedMergeEffect(ctx, conn, ref, intent, effect); err != nil {
		return normalRecoveryEndpoint{}, ErrPublicationEvidence
	}
	return effectEndpoint, nil
}

func validRecoveredMergeClaimAdvance(intent domain.MergeIntent, endpoint normalRecoveryEndpoint, claimEpoch uint64) bool {
	if endpoint.version < intent.TicketVersion || endpoint.runner < intent.RunnerEpoch || endpoint.leader < intent.LeaderEpoch {
		return false
	}
	delta := endpoint.version - intent.TicketVersion
	return intent.ClaimEpoch <= ^uint64(0)-delta && claimEpoch >= intent.ClaimEpoch+delta && claimEpoch-intent.ClaimEpoch <= endpoint.leader-intent.LeaderEpoch
}

// authenticatePromotedConfirmedMergeEffect walks the bounded append-only
// audit chain produced when an already-confirmed merge is independently
// re-observed after one or more pause/resume cycles. Each hop must bind the
// exact prior/current fences and be explained by either ordinary +1/+1 runner
// recovery, the guarded operator control triplet, or the semantic retry pair.
func (s *Store) authenticatePromotedConfirmedMergeEffect(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, intent domain.MergeIntent, effect Effect, required ...normalRecoveryEndpoint) error {
	if effect.State != EffectConfirmed || effect.ObservedIdentity == "" || effect.ClaimEpoch <= intent.ClaimEpoch {
		return ErrPublicationEvidence
	}
	var intentCreatedRaw string
	if err := conn.QueryRowContext(ctx, `SELECT created_at FROM merge_intents WHERE semantic_key=?`, intent.SemanticKey).Scan(&intentCreatedRaw); err != nil {
		return ErrPublicationEvidence
	}
	intentCreated, err := parseRunnerRecoveryTime(intentCreatedRaw)
	if err != nil {
		return ErrPublicationEvidence
	}
	type promotion struct {
		version uint64
		created time.Time
		value   invalidatedEffectReconciliationPayload
	}
	rows, err := conn.QueryContext(ctx, `SELECT ticket_version,from_state,to_state,payload,created_at FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='invalidated_effect_reconciled' ORDER BY id`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return err
	}
	var promotions []promotion
	for rows.Next() {
		var step promotion
		var from, to domain.State
		var raw string
		var createdRaw string
		if err := rows.Scan(&step.version, &from, &to, &raw, &createdRaw); err != nil {
			rows.Close()
			return err
		}
		step.created, err = parseRunnerRecoveryTime(createdRaw)
		if err != nil {
			rows.Close()
			return ErrPublicationEvidence
		}
		var value invalidatedEffectReconciliationPayload
		if len(raw) == 0 || len(raw) > maxEvidenceJSON || json.Unmarshal([]byte(raw), &value) != nil {
			rows.Close()
			return ErrPublicationEvidence
		}
		canonical, err := json.Marshal(value)
		if err != nil || string(canonical) != raw {
			rows.Close()
			return ErrPublicationEvidence
		}
		if value.SemanticKey == intent.SemanticKey {
			if from != domain.StateMerging || to != domain.StateMerging {
				rows.Close()
				return ErrPublicationEvidence
			}
			if !value.Present {
				// The same deterministic key may have one safely retired
				// pre-handoff claim before the immutable merge intent is issued.
				// Authenticate that historical absence independently and exclude
				// it from the later confirmed-promotion chain. An absence at or
				// after intent creation, or one overlapping its claim epoch, is a
				// contradiction and remains fail-closed.
				if value.ObservedIdentity != "" || value.PriorTicketVersion == 0 || value.PriorLeaderEpoch == 0 || value.PriorRunnerEpoch == 0 || value.PriorClaimEpoch == 0 || value.CurrentTicket != step.version || value.CurrentLeader == 0 || value.CurrentRunner == 0 || value.CurrentClaim != value.PriorClaimEpoch || value.CurrentTicket != intent.TicketVersion || value.CurrentRunner != intent.RunnerEpoch || value.CurrentLeader != intent.LeaderEpoch || value.CurrentClaim == ^uint64(0) || value.CurrentClaim+1 != intent.ClaimEpoch || value.PriorTicketVersion == ^uint64(0) || value.PriorTicketVersion+1 != value.CurrentTicket || value.PriorRunnerEpoch == ^uint64(0) || value.PriorRunnerEpoch+1 != value.CurrentRunner || value.PriorLeaderEpoch != value.CurrentLeader || !step.created.Before(intentCreated) {
					rows.Close()
					return ErrPublicationEvidence
				}
				recovery, found, err := loadRunnerRecoveryAt(ctx, conn, ref, value.CurrentTicket)
				if err != nil || !found || recovery.PriorTicketVersion != value.PriorTicketVersion || recovery.PriorRunnerEpoch != value.PriorRunnerEpoch || recovery.TicketVersion != value.CurrentTicket || recovery.RunnerEpoch != value.CurrentRunner || recovery.LeaderEpoch != value.CurrentLeader || !validRunnerRecovery(recovery) {
					rows.Close()
					return ErrPublicationEvidence
				}
				continue
			}
			if !step.created.After(intentCreated) {
				rows.Close()
				return ErrPublicationEvidence
			}
			step.value = value
			promotions = append(promotions, step)
			if len(promotions) > 64 {
				rows.Close()
				return ErrPublicationEvidence
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(promotions) == 0 {
		return ErrPublicationEvidence
	}

	intentEndpoint := normalRecoveryEndpoint{version: intent.TicketVersion, runner: intent.RunnerEpoch, leader: intent.LeaderEpoch}
	var previous normalRecoveryEndpoint
	var previousClaim uint64
	requiredFound := len(required) == 0 || required[0] == intentEndpoint
	for index, step := range promotions {
		value := step.value
		if !value.Present || value.PriorTicketVersion == 0 || value.PriorLeaderEpoch == 0 || value.PriorRunnerEpoch == 0 || value.PriorClaimEpoch == 0 || value.PriorClaimEpoch == ^uint64(0) || value.CurrentClaim != value.PriorClaimEpoch+1 || value.CurrentTicket != step.version || value.CurrentTicket == 0 || value.CurrentLeader == 0 || value.CurrentRunner == 0 || value.ObservedIdentity == "" || value.ObservedIdentity != effect.ObservedIdentity {
			return ErrPublicationEvidence
		}
		prior := normalRecoveryEndpoint{version: value.PriorTicketVersion, runner: value.PriorRunnerEpoch, leader: value.PriorLeaderEpoch}
		current := normalRecoveryEndpoint{version: value.CurrentTicket, runner: value.CurrentRunner, leader: value.CurrentLeader}
		if index == 0 {
			leader, err := normalRecoveryLeaderAt(ctx, conn, ref, intentEndpoint, prior.version, prior.runner)
			if err != nil || leader != prior.leader || !validRecoveredMergeClaimAdvance(intent, prior, value.PriorClaimEpoch) {
				return ErrPublicationEvidence
			}
		} else if prior != previous || value.PriorClaimEpoch != previousClaim {
			return ErrPublicationEvidence
		}
		if err := authenticateMergePromotionBridge(ctx, conn, ref, prior, current); err != nil {
			return ErrPublicationEvidence
		}
		if len(required) > 0 {
			checkpoint := required[0]
			if checkpoint == prior || checkpoint == current {
				requiredFound = true
			} else if checkpoint.version > prior.version && checkpoint.version < current.version &&
				validatePostPublicationEndpointAdvance(ctx, conn, ref, domain.StateMerging, prior, checkpoint) == nil &&
				validatePostPublicationEndpointAdvance(ctx, conn, ref, domain.StateMerging, checkpoint, current) == nil {
				// A single promotion can span more than one authenticated
				// pause/recovery segment. A later crash fences from the latest
				// durable stop endpoint, which may sit inside that hop; prove
				// both bounded halves rather than requiring an artificial
				// intermediate promotion event.
				requiredFound = true
			}
		}
		previous, previousClaim = current, value.CurrentClaim
	}
	last := promotions[len(promotions)-1].value
	if !requiredFound || previous.version != effect.TicketVersion || previous.runner != effect.RunnerEpoch || previous.leader != effect.LeaderEpoch || previousClaim != effect.ClaimEpoch || last.ObservedIdentity != effect.ObservedIdentity {
		return ErrPublicationEvidence
	}
	return nil
}

func authenticateMergePromotionBridge(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, prior, current normalRecoveryEndpoint) error {
	return authenticatePostPublicationEndpointBridge(ctx, conn, ref, domain.StateMerging, prior, current)
}

func authenticatePostPublicationEndpointBridge(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, state domain.State, prior, current normalRecoveryEndpoint) error {
	if (state != domain.StateReviewing && state != domain.StateWaitingApproval && state != domain.StateWaitingManualMerge && state != domain.StateMerging && state != domain.StateReconciling) || prior.version == 0 || prior.runner == 0 || prior.leader == 0 || current.version < prior.version || current.runner < prior.runner || current.leader < prior.leader {
		return ErrPublicationEvidence
	}
	return validatePostPublicationEndpointAdvance(ctx, q, ref, state, prior, current)
}

func authenticateCurrentPostPublicationEndpointBridge(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, state domain.State, prior, current normalRecoveryEndpoint) error {
	if err := authenticatePostPublicationEndpointBridge(ctx, q, ref, state, prior, current); err != nil {
		return err
	}
	var future int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?`, ref.Channel, ref.Project, ref.Ticket, current.version).Scan(&future); err != nil || future != 0 {
		return ErrPublicationEvidence
	}
	return nil
}

func currentPostPublicationLeader(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, current Ticket) (uint64, error) {
	if current.Ref != ref || !postPublicationState(current.State) || current.Version == 0 || current.RunnerEpoch == 0 {
		return 0, ErrPublicationEvidence
	}
	var state domain.State
	var version, runner, leader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil || state != current.State || version != current.Version || runner != current.RunnerEpoch || leader == 0 {
		return 0, ErrPublicationEvidence
	}
	return leader, nil
}

// validatePostPublicationEndpointAdvance authenticates a bounded same-state
// lineage through any interleaving of signed +1/+1 daemon recoveries, exact
// operator pause/drain/resume triplets, and (for guarded merging/reconciling)
// exact retry-exhaustion/retry pairs. It is deliberately separate from the
// generic runner helpers: phase transitions cannot carry publication/merge
// authority, and a counter gap without one of these rows must fail closed.
func validatePostPublicationEndpointAdvance(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, state domain.State, source, target normalRecoveryEndpoint) error {
	if ref.Validate() != nil || (state != domain.StateReviewing && state != domain.StateWaitingApproval && state != domain.StateWaitingManualMerge && state != domain.StateMerging && state != domain.StateReconciling) || source.version == 0 || source.runner == 0 || source.leader == 0 || target.version < source.version || target.runner < source.runner || target.leader < source.leader || target.version-source.version > 64 {
		return ErrPublicationEvidence
	}
	if source == target {
		return nil
	}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, source.version, target.version)
	if err != nil {
		return err
	}
	var recoveries []RunnerRecoveryLedger
	for rows.Next() {
		var step RunnerRecoveryLedger
		var createdAt string
		if err := rows.Scan(&step.PriorTicketVersion, &step.PriorRunnerEpoch, &step.PriorLeaderEpoch, &step.TicketVersion, &step.RunnerEpoch, &step.LeaderEpoch, &step.RecoveryDigest, &createdAt); err != nil {
			rows.Close()
			return err
		}
		step.Ref = ref
		step.CreatedAt, err = parseRunnerRecoveryTime(createdAt)
		if err != nil || !validRunnerRecovery(step) {
			rows.Close()
			return ErrPublicationEvidence
		}
		recoveries = append(recoveries, step)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	expected := source
	var previousCreated time.Time
	for _, step := range recoveries {
		predecessor := normalRecoveryEndpoint{version: step.PriorTicketVersion, runner: step.PriorRunnerEpoch, leader: step.PriorLeaderEpoch}
		if err := validatePostPublicationControlAdvance(ctx, q, ref, state, expected, predecessor); err != nil {
			return ErrPublicationEvidence
		}
		if !previousCreated.IsZero() && !step.CreatedAt.After(previousCreated) {
			return ErrPublicationEvidence
		}
		var transitions int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, step.TicketVersion).Scan(&transitions); err != nil || transitions != 0 {
			return ErrPublicationEvidence
		}
		expected = normalRecoveryEndpoint{version: step.TicketVersion, runner: step.RunnerEpoch, leader: step.LeaderEpoch}
		previousCreated = step.CreatedAt
	}
	return validatePostPublicationControlAdvance(ctx, q, ref, state, expected, target)
}

func validatePostPublicationControlAdvance(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, state domain.State, source, target normalRecoveryEndpoint) error {
	if source.version == 0 || source.runner == 0 || source.leader == 0 || target.version < source.version || target.runner < source.runner || target.leader < source.leader {
		return ErrPublicationEvidence
	}
	if source.version == target.version {
		if source.runner != target.runner || source.leader != target.leader {
			return ErrPublicationEvidence
		}
		return nil
	}
	rows, err := q.QueryContext(ctx, `SELECT ticket_version,trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND from_state<>to_state ORDER BY ticket_version,id`, ref.Channel, ref.Project, ref.Ticket, source.version, target.version)
	if err != nil {
		return err
	}
	type transition struct {
		version uint64
		trigger string
		from    domain.State
		to      domain.State
		payload string
	}
	var transitions []transition
	for rows.Next() {
		var value transition
		if err := rows.Scan(&value.version, &value.trigger, &value.from, &value.to, &value.payload); err != nil {
			rows.Close()
			return err
		}
		if len(value.payload) == 0 || len(value.payload) > maxEvidenceJSON || !json.Valid([]byte(value.payload)) {
			rows.Close()
			return ErrPublicationEvidence
		}
		transitions = append(transitions, value)
		if len(transitions) > 64 {
			rows.Close()
			return ErrPublicationEvidence
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(transitions) == 0 {
		return ErrPublicationEvidence
	}

	expectedVersion, expectedRunner := source.version, source.runner
	for index := 0; index < len(transitions); {
		first := transitions[index]
		if first.version != expectedVersion+1 || first.from != state {
			return ErrPublicationEvidence
		}
		switch {
		case first.trigger == "operator_pause_or_take" && first.to == domain.StateStopping:
			if index+2 >= len(transitions) || expectedVersion > ^uint64(0)-3 || expectedRunner == ^uint64(0) {
				return ErrPublicationEvidence
			}
			drained, resumed := transitions[index+1], transitions[index+2]
			// Both explicit resume and retry are valid ways to leave a sealed
			// pause/take control sequence.  authenticatePostPublicationResume
			// already enforces the same documented parity for rearm; keeping this
			// lineage reader narrower would strand a reviewing or approval ticket
			// that resumed with operator_retry.
			if drained.version != expectedVersion+2 || drained.trigger != "process_and_effects_drained" || drained.from != domain.StateStopping || drained.to != domain.StatePaused || resumed.version != expectedVersion+3 || (resumed.trigger != "operator_resume" && resumed.trigger != "operator_retry") || resumed.from != domain.StatePaused || resumed.to != state {
				return ErrPublicationEvidence
			}
			expectedVersion += 3
			expectedRunner++
			index += 3
		case (state == domain.StateMerging || state == domain.StateReconciling) && first.trigger == "retry_or_correction_exhausted" && first.to == domain.StatePaused:
			if index+1 >= len(transitions) || expectedVersion > ^uint64(0)-2 {
				return ErrPublicationEvidence
			}
			resumed := transitions[index+1]
			if resumed.version != expectedVersion+2 || resumed.trigger != "operator_retry" || resumed.from != domain.StatePaused || resumed.to != state {
				return ErrPublicationEvidence
			}
			expectedVersion += 2
			index += 2
		default:
			return ErrPublicationEvidence
		}
	}
	if expectedVersion != target.version || expectedRunner != target.runner {
		return ErrPublicationEvidence
	}
	// Paused tickets are intentionally not fenced. An authenticated operator
	// may therefore resume under a newer daemon leader; the exact control
	// sequence is the durable authority for that monotonic handoff. A leader
	// change without at least one control sequence was rejected above.
	return nil
}

func exactStateChangeEvent(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, trigger string, from, to domain.State) error {
	var matching, stateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN trigger=? AND from_state=? AND to_state=? THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, trigger, from, to, ref.Channel, ref.Project, ref.Ticket, version).Scan(&matching, &stateChanges); err != nil || matching != 1 || stateChanges != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

func noRunnerRecoveryRows(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, afterVersion, throughVersion uint64) error {
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=?`, ref.Channel, ref.Project, ref.Ticket, afterVersion, throughVersion).Scan(&count); err != nil || count != 0 {
		return ErrPublicationEvidence
	}
	return nil
}

func promotionBridgeLeader(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, resumed, current normalRecoveryEndpoint) (uint64, error) {
	if current.version < resumed.version || current.runner < resumed.runner || current.version-resumed.version != current.runner-resumed.runner {
		return 0, ErrPublicationEvidence
	}
	if current.version == resumed.version {
		resumed.leader = current.leader
		return resumed.leader, nil
	}
	first, found, err := loadRunnerRecoveryAt(ctx, conn, ref, resumed.version+1)
	if err != nil || !found || first.PriorTicketVersion != resumed.version || first.PriorRunnerEpoch != resumed.runner || first.PriorLeaderEpoch == 0 {
		return 0, ErrPublicationEvidence
	}
	resumed.leader = first.PriorLeaderEpoch
	if err := validateRunnerRecoveryLedger(ctx, conn, ref, resumed.version, resumed.runner, resumed.leader, current.version, current.runner, current.leader); err != nil {
		return 0, ErrPublicationEvidence
	}
	return resumed.leader, nil
}

func postPublicationControlTripletPresent(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version uint64) (bool, error) {
	if !postPublicationState(state) || version < 3 {
		return false, nil
	}
	checks := []struct {
		version uint64
		trigger string
		from    domain.State
		to      domain.State
	}{
		{version - 2, "operator_pause_or_take", state, domain.StateStopping},
		{version - 1, "process_and_effects_drained", domain.StateStopping, domain.StatePaused},
		{version, "operator_resume|operator_retry", domain.StatePaused, state},
	}
	for _, check := range checks {
		var matching, total int
		triggerClause := "trigger=?"
		args := []any{ref.Channel, ref.Project, ref.Ticket, check.version, ref.Channel, ref.Project, ref.Ticket, check.version, check.trigger, check.from, check.to}
		if check.trigger == "operator_resume|operator_retry" {
			triggerClause = "trigger IN ('operator_resume','operator_retry')"
			args = []any{ref.Channel, ref.Project, ref.Ticket, check.version, ref.Channel, ref.Project, ref.Ticket, check.version, check.from, check.to}
		}
		query := `SELECT COALESCE((SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?),0),COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND ` + triggerClause + ` AND from_state=? AND to_state=?`
		if err := conn.QueryRowContext(ctx, query, args...).Scan(&total, &matching); err != nil {
			return false, err
		}
		if total != 1 || matching != 1 {
			return false, nil
		}
	}
	return true, nil
}
