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
	if baselineVersion == 0 || baselineRunner == 0 || baselineLeader == 0 || liveVersion == 0 || liveRunner == 0 || liveLeader == 0 {
		return ErrPublicationEvidence
	}
	if err := validateRunnerRecoveryCardinality(ctx, q, ref); err != nil {
		return err
	}
	// A future row remains ticket authority. It cannot be ignored merely
	// because this caller is authenticating an earlier live fence.
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, baselineVersion)
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
		if step.TicketVersion > liveVersion || !validRunnerRecovery(step) || step.PriorTicketVersion < expectedVersion || step.PriorRunnerEpoch < expectedRunner || step.PriorLeaderEpoch < expectedLeader {
			return ErrPublicationEvidence
		}
		// A complete operator pause/take may advance the ticket version and
		// runner epoch between two daemon recovery rows. Authenticate that
		// control-only segment before consuming the next +1 recovery row; a
		// counter gap without its stopping/drained/resume event triplet remains
		// invalid.
		if err := validateRunnerControlAdvance(ctx, q, ref, expectedVersion, expectedRunner, expectedLeader, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch); err != nil {
			if err := validateRunnerPhaseChain(ctx, q, ref, expectedVersion, expectedRunner, step.PriorTicketVersion, step.PriorRunnerEpoch); err != nil {
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
		if err := validateRunnerPhaseAdvance(ctx, q, ref, expectedVersion, expectedRunner, liveVersion, liveRunner); err != nil {
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
			if err := validateRunnerPhaseChain(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, step.PriorTicketVersion, step.PriorRunnerEpoch); err != nil {
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
			if err := validateRunnerPhaseChain(ctx, q, ref, previous.TicketVersion, previous.RunnerEpoch, liveVersion, liveFence.RunnerEpoch); err != nil {
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
		if !validInitialLifecycleTransition(trigger, from, to) {
			return ErrPublicationEvidence
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
	if err := validateRunnerRecoveryAuthority(ctx, s.db, key.Ref, expected, fence); err != nil {
		return ErrStaleFence
	}
	if err := providerResultReachesFence(ctx, s.db, key, result, expected, fence); err != nil {
		return ErrStaleFence
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
	if requireLiveAuthority {
		if err := validateRunnerRecoveryAuthority(ctx, q, key.Ref, expected, fence); err != nil {
			return ErrStaleFence
		}
	}
	if claim.ExpectedVersion == expected && claim.RunnerEpoch == fence.RunnerEpoch && claim.LeaderEpoch == fence.LeaderEpoch {
		return nil
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
