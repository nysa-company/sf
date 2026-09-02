package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

const providerExhaustionSchema = "sf.provider-exhaustion/v1"

type providerExhaustionPayload struct {
	Schema             string       `json:"schema"`
	Phase              domain.Phase `json:"phase"`
	EntryTicketVersion uint64       `json:"entry_ticket_version"`
	RetryEpoch         int          `json:"retry_epoch"`
	Attempts           []int        `json:"attempts"`
	// Reason is Store-derived from the authenticated terminal pair.  Its empty
	// value preserves the generic failed/cancelled payload used before typed
	// invalid-artifact repair was introduced.
	Reason string `json:"reason,omitempty"`
}

const providerExhaustionReasonInvalidArtifact = "invalid_artifact"

// ProviderRetryDisposition separates generic correction pauses from the two
// Store-owned provider exhaustion generations. Only Eligible opens capacity;
// Exhausted is terminal for v1 and NotProvider keeps the existing generic
// correction/CI retry behavior intact.
type ProviderRetryDisposition uint8

const (
	ProviderRetryNotProvider ProviderRetryDisposition = iota
	ProviderRetryEligible
	ProviderRetryExhausted
)

type providerRetryEpoch struct {
	Phase                                                 domain.Phase
	EntryVersion                                          uint64
	InitialFirst, InitialLast, RetryFirst, RetryLast      int
	ExhaustionVersion, ExhaustionLeader, ExhaustionRunner uint64
	RetryVersion, RetryLeader, RetryRunner                uint64
	Digest                                                string
}

type providerPhaseEntry struct {
	Phase                   domain.Phase
	Version, Leader, Runner uint64
	EventID                 int64
	EventCreated            string
	From, State             domain.State
	Trigger, Digest         string
}

func providerPhaseForState(state domain.State) (domain.Phase, bool) {
	switch state {
	case domain.StatePlanning:
		return domain.PhasePlanning, true
	case domain.StateVerifying:
		return domain.PhaseVerification, true
	case domain.StateBuilding:
		return domain.PhaseBuild, true
	case domain.StateReviewing:
		return domain.PhaseReview, true
	default:
		return "", false
	}
}

func providerStateForPhaseTransition(state domain.State) bool {
	_, ok := providerPhaseForState(state)
	return ok
}

func phaseForProviderState(state domain.State) domain.Phase {
	phase, _ := providerPhaseForState(state)
	return phase
}

func providerPhaseEntryForTransition(from, to domain.State, trigger string) (domain.Phase, bool) {
	switch {
	case from == domain.StatePlanning && to == domain.StateVerifying && trigger == "phase_pass":
		return domain.PhaseVerification, true
	case from == domain.StateVerifying && to == domain.StateBuilding && trigger == "phase_pass":
		return domain.PhaseBuild, true
	case from == domain.StateBuilding && to == domain.StateVerifying && trigger == "verification_amendment_requested":
		return domain.PhaseVerification, true
	case from == domain.StateVerifying && to == domain.StateBuilding && (trigger == "amendment_accepted" || trigger == "amendment_rejected"):
		return domain.PhaseBuild, true
	case from == domain.StateReviewing && to == domain.StateBuilding && trigger == "review_repair":
		return domain.PhaseBuild, true
	case from == domain.StateReviewing && to == domain.StateVerifying && trigger == "review_repair":
		return domain.PhaseVerification, true
	default:
		return "", false
	}
}

func providerPhaseEntryDigest(ref domain.TicketRef, entry providerPhaseEntry) (string, error) {
	payload := struct {
		Channel, Project, Ticket string
		Phase                    domain.Phase
		Version, Leader, Runner  uint64
		EventID                  int64
		EventCreated             string
		From, State              domain.State
		Trigger                  string
	}{string(ref.Channel), string(ref.Project), string(ref.Ticket), entry.Phase, entry.Version, entry.Leader, entry.Runner, entry.EventID, entry.EventCreated, entry.From, entry.State, entry.Trigger}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// recordProviderPhaseEntry is only called from Store-owned semantic
// transitions. Controls and leader recovery deliberately never create an
// entry: they retain the same bounded failure window.
func recordProviderPhaseEntry(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, version, leader, runner uint64, eventID int64, eventCreated string, from, state domain.State, trigger string) error {
	if providerStateForPhase(phase) != state || version == 0 || leader == 0 || runner == 0 || eventID <= 0 || eventCreated == "" || !from.Valid() || trigger == "" {
		return ErrEvidenceConflict
	}
	entry := providerPhaseEntry{Phase: phase, Version: version, Leader: leader, Runner: runner, EventID: eventID, EventCreated: eventCreated, From: from, State: state, Trigger: trigger}
	digest, err := providerPhaseEntryDigest(ref, entry)
	if err != nil {
		return err
	}
	entry.Digest = digest
	_, err = conn.ExecContext(ctx, `INSERT INTO provider_phase_entries(channel,project_id,ticket_id,phase,entry_ticket_version,entry_event_id,entry_event_created_at,entry_leader_epoch,entry_runner_epoch,entry_from_state,entry_state,entry_trigger,entry_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(channel,project_id,ticket_id,phase,entry_ticket_version) DO NOTHING`, ref.Channel, ref.Project, ref.Ticket, phase, version, eventID, eventCreated, leader, runner, from, state, trigger, digest, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	var stored providerPhaseEntry
	err = conn.QueryRowContext(ctx, `SELECT entry_event_id,entry_event_created_at,entry_leader_epoch,entry_runner_epoch,entry_from_state,entry_state,entry_trigger,entry_digest FROM provider_phase_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, phase, version).Scan(&stored.EventID, &stored.EventCreated, &stored.Leader, &stored.Runner, &stored.From, &stored.State, &stored.Trigger, &stored.Digest)
	if err != nil || stored.EventID != eventID || stored.EventCreated != eventCreated || stored.Leader != leader || stored.Runner != runner || stored.From != from || stored.State != state || stored.Trigger != trigger || stored.Digest != digest {
		return ErrEvidenceConflict
	}
	return nil
}

func loadCurrentProviderPhaseEntry(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, ref domain.TicketRef, phase domain.Phase, version, runner, leader uint64) (providerPhaseEntry, error) {
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, phase, version)
	if err != nil {
		return providerPhaseEntry{}, err
	}
	if entry.Version == version && entry.Runner == runner && entry.Leader == leader {
		return entry, nil
	}
	// Provider exhaustion followed by the single typed operator retry is a
	// same-runner, two-version semantic bridge.  It is not a runner-recovery
	// row, so it must be authenticated before the ordinary recovery-ledger
	// fallback.  The validator binds the immutable entry, both terminal
	// attempts, the exhaustion event, and the retry epoch endpoints.
	if epoch, found, err := loadProviderRetryEpochForEntry(ctx, q, ref, phase, entry.Version); err == nil && found {
		if err := validateProviderRetryAdvance(ctx, q, ref, phase, epoch.ExhaustionVersion-1, epoch.ExhaustionRunner, epoch.ExhaustionLeader, version, runner, leader); err == nil {
			return entry, nil
		}
	}
	// A typed provider blocker is an intentionally narrow, same-runner
	// interruption.  Its paired operator_recover event is the only non-ledger
	// provider gap admitted here; generic controls and arbitrary blocked rows
	// remain unable to renew a phase entry.
	if err := validateProviderBlockedRecoveryAdvance(ctx, q, ref, entry, version, runner, leader); err == nil {
		return entry, nil
	}
	if err := validateRunnerRecoveryLedger(ctx, q, ref, entry.Version, entry.Runner, entry.Leader, version, runner, leader); err != nil {
		return providerPhaseEntry{}, ErrEvidenceConflict
	}
	return entry, nil
}

// validateProviderPausedResumePrefix authenticates the already-committed
// operator pause/take -> drained prefix immediately before a generic resume.
// It runs before the resume event exists, so loadCurrentProviderPhaseEntry
// cannot be used yet.  The runtime seal supplies the exact control endpoint;
// the immutable entry supplies the only admissible provider predecessor.
func validateProviderPausedResumePrefix(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, version, runner, leader uint64, target domain.State) error {
	if version < 2 || runner < 2 || leader == 0 || providerStateForPhase(phase) != target {
		return ErrEvidenceConflict
	}
	stopVersion := version - 1
	// A ticket may remain paused across daemon replacement. No runner-recovery
	// row is minted while paused, so the resume transition is fenced by the new
	// daemon leader while the retained phase entry still reaches the sealed
	// pre-stop leader. The exact durable control row is the bridge; never
	// substitute the live leader into the historical phase proof.
	control, err := runtimeControlFrom(ctx, conn, ref)
	if err != nil || control.state != "sealed" || control.stop.version != stopVersion || control.stop.runner != runner || control.stop.leader == 0 || control.stop.leader > leader || control.authority != control.stop {
		return ErrEvidenceConflict
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, conn, ref, phase, stopVersion-1, runner-1, control.stop.leader)
	if err != nil || entry.State != target {
		return ErrEvidenceConflict
	}
	if err := exactStateChangeEvent(ctx, conn, ref, stopVersion, "operator_pause_or_take", target, domain.StateStopping); err != nil {
		return ErrEvidenceConflict
	}
	if err := exactStateChangeEvent(ctx, conn, ref, version, "process_and_effects_drained", domain.StateStopping, domain.StatePaused); err != nil {
		return ErrEvidenceConflict
	}
	return validateProviderPhaseEntryBindings(ctx, conn, ref, entry)
}

// validateProviderBlockedRecoveryPrefix is the pre-transition counterpart to
// validateProviderBlockedRecoveryAdvance.  It proves that the current blocked
// row is the exact typed interruption of a retained provider entry before an
// operator_recover appends its resumption event.
func validateProviderBlockedRecoveryPrefix(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, phase domain.Phase, version, runner, leader uint64, target domain.State, code string) error {
	if version < 2 || runner == 0 || leader == 0 || !providerStateForPhaseTransition(target) || providerStateForPhase(phase) != target || !validBlockedCode(code) || code == "legacy_provider_phase_entry_unverifiable" {
		return ErrEvidenceConflict
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, conn, ref, phase, version-1, runner, leader)
	if err != nil || entry.State != target {
		return ErrEvidenceConflict
	}
	var trigger, raw string
	var from, to domain.State
	if err := conn.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&trigger, &from, &to, &raw); err != nil {
		return ErrEvidenceConflict
	}
	var payload struct {
		Code string `json:"code"`
	}
	if trigger != "typed_blocker" || from != target || to != domain.StateBlocked || len(raw) > maxEvidenceJSON || json.Unmarshal([]byte(raw), &payload) != nil || payload.Code != code {
		return ErrEvidenceConflict
	}
	if err := exactStateChangeEvent(ctx, conn, ref, version, "typed_blocker", target, domain.StateBlocked); err != nil {
		return ErrEvidenceConflict
	}
	return validateProviderPhaseEntryBindings(ctx, conn, ref, entry)
}

// validateProviderBlockedRecoveryAdvance is used only after the recovery event
// was atomically appended.  No generic state gap or leader change is accepted.
func validateProviderBlockedRecoveryAdvance(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, ref domain.TicketRef, entry providerPhaseEntry, version, runner, leader uint64) error {
	if version < 2 || runner == 0 || leader == 0 {
		return ErrEvidenceConflict
	}
	// The typed block can follow a legitimate +1/+1 daemon recovery. Resolve
	// the phase entry at the exact pre-block endpoint first; requiring its
	// creation version here would strand a recovered phase on the next operator
	// action.
	preBlock, err := loadCurrentProviderPhaseEntry(ctx, q, ref, entry.Phase, version-2, runner, leader)
	if err != nil || preBlock.Version != entry.Version || preBlock.Digest != entry.Digest || preBlock.State != entry.State {
		return ErrEvidenceConflict
	}
	var blockTrigger, recoverTrigger, blockRaw string
	var blockFrom, blockTo, recoverFrom, recoverTo domain.State
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version-1).Scan(&blockTrigger, &blockFrom, &blockTo, &blockRaw); err != nil {
		return ErrEvidenceConflict
	}
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&recoverTrigger, &recoverFrom, &recoverTo); err != nil {
		return ErrEvidenceConflict
	}
	var payload struct {
		Code string `json:"code"`
	}
	if blockTrigger != "typed_blocker" || blockFrom != entry.State || blockTo != domain.StateBlocked || len(blockRaw) > maxEvidenceJSON || json.Unmarshal([]byte(blockRaw), &payload) != nil || !validBlockedCode(payload.Code) || payload.Code == "legacy_provider_phase_entry_unverifiable" || recoverTrigger != "operator_recover" || recoverFrom != domain.StateBlocked || recoverTo != entry.State {
		return ErrEvidenceConflict
	}
	if err := exactStateChangeEvent(ctx, q, ref, version-1, "typed_blocker", entry.State, domain.StateBlocked); err != nil {
		return ErrEvidenceConflict
	}
	if err := exactStateChangeEvent(ctx, q, ref, version, "operator_recover", domain.StateBlocked, entry.State); err != nil {
		return ErrEvidenceConflict
	}
	return nil
}

func loadProviderPhaseEntryAt(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, phase domain.Phase, version uint64) (providerPhaseEntry, error) {
	var entry providerPhaseEntry
	err := q.QueryRowContext(ctx, `SELECT entry_ticket_version,entry_event_id,entry_event_created_at,entry_leader_epoch,entry_runner_epoch,entry_from_state,entry_state,entry_trigger,entry_digest FROM provider_phase_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version<=? ORDER BY entry_ticket_version DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket, phase, version).Scan(&entry.Version, &entry.EventID, &entry.EventCreated, &entry.Leader, &entry.Runner, &entry.From, &entry.State, &entry.Trigger, &entry.Digest)
	if errors.Is(err, sql.ErrNoRows) {
		return providerPhaseEntry{}, ErrEvidenceConflict
	}
	if err != nil || providerStateForPhase(phase) != entry.State {
		return providerPhaseEntry{}, ErrEvidenceConflict
	}
	entry.Phase = phase
	want, err := providerPhaseEntryDigest(ref, entry)
	if err != nil || want != entry.Digest {
		return providerPhaseEntry{}, ErrEvidenceConflict
	}
	return entry, nil
}

// validateProviderPhaseEntryBindings makes the binding table an authority,
// not merely a counter index. Any provider attempt whose durable endpoint is
// reachable from this entry must carry exactly this immutable entry binding;
// an unbound row is ambiguous and blocks admission rather than resetting a
// failure window.
func validateProviderPhaseEntryBindings(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, entry providerPhaseEntry) error {
	// Do not issue further statements on conn until this cursor is closed. SQLite
	// connections are deliberately used as the authority boundary throughout this
	// package, and nested readers here can otherwise hide a malformed binding.
	rows, err := conn.QueryContext(ctx, `SELECT a.id,a.attempt,a.expected_ticket_version,a.leader_epoch,a.runner_epoch,COUNT(pe.provider_attempt_id)
		FROM provider_attempts a LEFT JOIN provider_phase_attempt_entries pe ON pe.provider_attempt_id=a.id
		AND pe.channel=a.channel AND pe.project_id=a.project_id AND pe.ticket_id=a.ticket_id AND pe.phase=a.phase AND pe.attempt=a.attempt AND pe.entry_ticket_version=?
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=?
		GROUP BY a.id,a.attempt,a.expected_ticket_version,a.leader_epoch,a.runner_epoch`, entry.Version, ref.Channel, ref.Project, ref.Ticket, entry.Phase)
	if err != nil {
		return err
	}
	type attemptBinding struct {
		id                      int64
		attempt                 int
		version, leader, runner uint64
		bindings                int
	}
	var attempts []attemptBinding
	for rows.Next() {
		var value attemptBinding
		if err := rows.Scan(&value.id, &value.attempt, &value.version, &value.leader, &value.runner, &value.bindings); err != nil {
			rows.Close()
			return err
		}
		attempts = append(attempts, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, value := range attempts {
		if value.version < entry.Version {
			continue
		}
		// Provider entry lineage may legitimately include the narrowly
		// authenticated retry, control, or typed-block/recover bridges.  Resolve
		// the attempt endpoint through the same entry loader used for admission
		// instead of applying the generic recovery ledger directly.
		resolved, err := loadCurrentProviderPhaseEntry(ctx, conn, ref, entry.Phase, value.version, value.runner, value.leader)
		if err != nil || resolved.Version != entry.Version || resolved.Digest != entry.Digest {
			continue
		}
		if value.bindings != 1 {
			return ErrEvidenceConflict
		}
	}
	return nil
}

type providerRetryAttemptPair struct {
	Attempts [2]int
	Reason   string
}

// authenticateProviderRetryAttemptPair is the single authority for a
// terminal pair that may create or cross a provider-retry epoch.  It starts
// at the immutable entry binding, then rehydrates each Store-issued claim so
// the canonical input and every duplicated endpoint fact are checked before
// a typed invalid-artifact repair marker is trusted.
func authenticateProviderRetryAttemptPair(ctx context.Context, q rowQueryer, ref domain.TicketRef, phase domain.Phase, entry providerPhaseEntry, first, last int) (providerRetryAttemptPair, error) {
	if ref.Validate() != nil || phase == "" || entry.Phase != phase || entry.Version == 0 || first <= 0 || last != first+1 {
		return providerRetryAttemptPair{}, ErrEvidenceConflict
	}
	type boundAttempt struct {
		id      int64
		role    string
		attempt int
	}
	rows, err := q.QueryContext(ctx, `SELECT provider_attempt_id,role,attempt
		FROM provider_phase_attempt_entries
		WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=? AND attempt BETWEEN ? AND ?
		ORDER BY attempt`, ref.Channel, ref.Project, ref.Ticket, phase, entry.Version, first, last)
	if err != nil {
		return providerRetryAttemptPair{}, err
	}
	var bound []boundAttempt
	for rows.Next() {
		var value boundAttempt
		if err := rows.Scan(&value.id, &value.role, &value.attempt); err != nil {
			rows.Close()
			return providerRetryAttemptPair{}, err
		}
		bound = append(bound, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return providerRetryAttemptPair{}, err
	}
	if err := rows.Close(); err != nil {
		return providerRetryAttemptPair{}, err
	}
	if len(bound) != 2 || bound[0].id <= 0 || bound[1].id <= 0 || bound[0].attempt != first || bound[1].attempt != last || bound[0].role == "" || bound[1].role == "" {
		return providerRetryAttemptPair{}, ErrEvidenceConflict
	}

	claims := make([]ProviderAttemptClaim, 2)
	invalid := make([]bool, 2)
	for i, value := range bound {
		claim, err := loadAuthenticatedProviderAttemptClaim(ctx, q, value.id)
		if err != nil || claim.Ref != ref || claim.Phase != phase || claim.Role != value.role || claim.Attempt != value.attempt || claim.BindingDigest != bindingDigest(claim.Binding) {
			return providerRetryAttemptPair{}, ErrEvidenceConflict
		}
		var attemptState, attemptOutcome, launchState, finishedAt string
		if err := q.QueryRowContext(ctx, `SELECT state,outcome,launch_state,finished_at FROM provider_attempts WHERE id=?`, claim.ID).Scan(&attemptState, &attemptOutcome, &launchState, &finishedAt); err != nil {
			return providerRetryAttemptPair{}, err
		}
		phaseState, phaseOutcome := "", ""
		var phaseFinished string
		if err := q.QueryRowContext(ctx, `SELECT state,outcome,completed_at FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND provider=? AND model=? AND family=? AND provider_version=? AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND worktree_identity=? AND base_sha=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.WorktreeIdentity, claim.BaseSHA).Scan(&phaseState, &phaseOutcome, &phaseFinished); err != nil {
			return providerRetryAttemptPair{}, err
		}
		invalid[i] = attemptState == "failed" && attemptOutcome == providerExhaustionReasonInvalidArtifact && phaseState == "failed" && phaseOutcome == providerExhaustionReasonInvalidArtifact
		if invalid[i] && (launchState != "drained" || finishedAt == "" || phaseFinished == "") {
			return providerRetryAttemptPair{}, ErrEvidenceConflict
		}
		if !invalid[i] && ((attemptState != "failed" && attemptState != "cancelled") || (attemptOutcome != "failed" && attemptOutcome != "cancelled") || (phaseState != "failed" && phaseState != "cancelled") || (phaseOutcome != "failed" && phaseOutcome != "cancelled")) {
			return providerRetryAttemptPair{}, ErrEvidenceConflict
		}
		claims[i] = claim
	}
	var results int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id IN (?,?)`, claims[0].ID, claims[1].ID).Scan(&results); err != nil {
		return providerRetryAttemptPair{}, err
	}
	if results != 0 {
		return providerRetryAttemptPair{}, ErrEvidenceConflict
	}
	pair := providerRetryAttemptPair{Attempts: [2]int{first, last}}
	if invalid[0] || invalid[1] {
		if !invalid[0] || !invalid[1] || claims[0].Binding != claims[1].Binding || claims[0].BindingDigest != claims[1].BindingDigest || claims[0].Role != claims[1].Role || claims[0].Ref != claims[1].Ref || claims[0].Phase != claims[1].Phase || claims[0].LeaderEpoch != claims[1].LeaderEpoch || claims[0].RunnerEpoch != claims[1].RunnerEpoch || claims[0].ExpectedVersion != claims[1].ExpectedVersion || claims[0].Repository != claims[1].Repository || claims[0].Worktree != claims[1].Worktree || claims[0].WorktreeIdentity != claims[1].WorktreeIdentity || claims[0].BaseSHA != claims[1].BaseSHA || claims[0].Input.Repair != nil || claims[1].Input.Repair == nil || claims[1].Input.Repair.PriorAttempt != claims[0].Attempt || claims[1].Input.Repair.PriorRequestDigest != claims[0].RequestDigest {
			return providerRetryAttemptPair{}, ErrEvidenceConflict
		}
		pair.Reason = providerExhaustionReasonInvalidArtifact
	}
	return pair, nil
}

func providerRetryDigest(ref domain.TicketRef, epoch providerRetryEpoch) (string, error) {
	payload := struct {
		Channel, Project, Ticket  string
		Phase                     domain.Phase
		EntryVersion              uint64
		Epoch                     int
		InitialFirst, InitialLast int
		RetryFirst, RetryLast     int
		ExhaustionVersion         uint64
		ExhaustionLeader          uint64
		ExhaustionRunner          uint64
		RetryVersion              uint64
		RetryLeader               uint64
		RetryRunner               uint64
	}{string(ref.Channel), string(ref.Project), string(ref.Ticket), epoch.Phase, epoch.EntryVersion, 1, epoch.InitialFirst, epoch.InitialLast, epoch.RetryFirst, epoch.RetryLast, epoch.ExhaustionVersion, epoch.ExhaustionLeader, epoch.ExhaustionRunner, epoch.RetryVersion, epoch.RetryLeader, epoch.RetryRunner}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func loadProviderRetryEpoch(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, phase domain.Phase) (providerRetryEpoch, bool, error) {
	return loadProviderRetryEpochForEntry(ctx, q, ref, phase, 0)
}

func loadProviderRetryEpochForEntry(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, phase domain.Phase, entryVersion uint64) (providerRetryEpoch, bool, error) {
	var epoch providerRetryEpoch
	query := `SELECT entry_ticket_version,initial_first_attempt,initial_last_attempt,retry_first_attempt,retry_last_attempt,exhaustion_ticket_version,exhaustion_leader_epoch,exhaustion_runner_epoch,retry_ticket_version,retry_leader_epoch,retry_runner_epoch,retry_digest FROM provider_retry_epochs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND epoch=1`
	args := []any{ref.Channel, ref.Project, ref.Ticket, phase}
	if entryVersion != 0 {
		query += ` AND entry_ticket_version=?`
		args = append(args, entryVersion)
	}
	query += ` ORDER BY entry_ticket_version DESC LIMIT 1`
	err := q.QueryRowContext(ctx, query, args...).Scan(&epoch.EntryVersion, &epoch.InitialFirst, &epoch.InitialLast, &epoch.RetryFirst, &epoch.RetryLast, &epoch.ExhaustionVersion, &epoch.ExhaustionLeader, &epoch.ExhaustionRunner, &epoch.RetryVersion, &epoch.RetryLeader, &epoch.RetryRunner, &epoch.Digest)
	if errors.Is(err, sql.ErrNoRows) {
		return providerRetryEpoch{}, false, nil
	}
	if err != nil {
		return providerRetryEpoch{}, false, err
	}
	epoch.Phase = phase
	want, err := providerRetryDigest(ref, epoch)
	if err != nil || epoch.Digest != want || epoch.EntryVersion == 0 || epoch.InitialFirst <= 0 || epoch.InitialLast != epoch.InitialFirst+1 || epoch.RetryFirst != epoch.InitialLast+1 || epoch.RetryLast != epoch.RetryFirst+1 || epoch.ExhaustionVersion == 0 || epoch.ExhaustionLeader == 0 || epoch.ExhaustionRunner == 0 || epoch.RetryVersion != epoch.ExhaustionVersion+1 || epoch.RetryLeader == 0 || epoch.RetryRunner == 0 {
		return providerRetryEpoch{}, false, ErrEvidenceConflict
	}
	return epoch, true, nil
}

// ProviderRetryPause reports whether paused is exactly a Store-authenticated
// provider-exhaustion pause. It is deliberately narrower than RetryablePause:
// publication and CI exhaustion cannot open provider capacity.
func (s *Store) ProviderRetryPause(ctx context.Context, ticket Ticket) (bool, error) {
	disposition, err := s.ProviderRetryDisposition(ctx, ticket)
	return disposition == ProviderRetryEligible, err
}

func (s *Store) ProviderRetryDisposition(ctx context.Context, ticket Ticket) (ProviderRetryDisposition, error) {
	phase, ok := providerPhaseForState(ticket.ResumeState)
	if !ok || ticket.State != domain.StatePaused || ticket.Version == 0 {
		return ProviderRetryNotProvider, nil
	}
	var candidates int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='retry_or_correction_exhausted' AND from_state=? AND to_state='paused'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version, ticket.ResumeState).Scan(&candidates); err != nil {
		return ProviderRetryNotProvider, err
	}
	if candidates == 0 {
		return ProviderRetryNotProvider, nil
	}
	if err := exactStateChangeEvent(ctx, s.db, ticket.Ref, ticket.Version, "retry_or_correction_exhausted", ticket.ResumeState, domain.StatePaused); err != nil {
		return ProviderRetryNotProvider, ErrEvidenceConflict
	}
	var trigger string
	var from, to domain.State
	var payload string
	err := s.db.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version).Scan(&trigger, &from, &to, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return ProviderRetryNotProvider, nil
	}
	if err != nil {
		return ProviderRetryNotProvider, err
	}
	var proof providerExhaustionPayload
	if trigger != "retry_or_correction_exhausted" || from != ticket.ResumeState || to != domain.StatePaused || len(payload) > maxEvidenceJSON || json.Unmarshal([]byte(payload), &proof) != nil || proof.Schema != providerExhaustionSchema {
		return ProviderRetryNotProvider, nil
	}
	if proof.Phase != phase || proof.EntryTicketVersion == 0 || len(proof.Attempts) != 2 || proof.Attempts[1] != proof.Attempts[0]+1 {
		return ProviderRetryNotProvider, ErrEvidenceConflict
	}
	entry, entryErr := loadProviderPhaseEntryAt(ctx, s.db, ticket.Ref, phase, proof.EntryTicketVersion)
	if entryErr != nil || entry.Version != proof.EntryTicketVersion || entry.State != ticket.ResumeState {
		return ProviderRetryNotProvider, ErrEvidenceConflict
	}
	pair, pairErr := authenticateProviderRetryAttemptPair(ctx, s.db, ticket.Ref, phase, entry, proof.Attempts[0], proof.Attempts[1])
	if pairErr != nil || pair.Reason != proof.Reason {
		return ProviderRetryNotProvider, ErrEvidenceConflict
	}
	if proof.RetryEpoch == 0 {
		return ProviderRetryEligible, nil
	}
	if proof.RetryEpoch == 1 {
		epoch, found, err := loadProviderRetryEpochForEntry(ctx, s.db, ticket.Ref, phase, proof.EntryTicketVersion)
		if err != nil || !found || epoch.RetryFirst != proof.Attempts[0] || epoch.RetryLast != proof.Attempts[1] {
			return ProviderRetryNotProvider, ErrEvidenceConflict
		}
		return ProviderRetryExhausted, nil
	}
	return ProviderRetryNotProvider, ErrEvidenceConflict
}

// TransitionProviderExhausted is the only authority that can turn two
// terminal provider failures into a retry-exhausted pause. Generic transition
// callers cannot supply its proof payload.
func (s *Store) TransitionProviderExhausted(ctx context.Context, transition Transition) (TransitionResult, error) {
	phase, ok := providerPhaseForState(transition.From)
	if !ok || transition.To != domain.StatePaused || transition.ResumeState != transition.From || transition.Trigger != "retry_or_correction_exhausted" || transition.Ref.Validate() != nil || transition.ExpectedVersion == 0 || transition.Fence.LeaderEpoch == 0 || transition.Fence.RunnerEpoch == 0 {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var actual domain.State
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &version, &runner); err != nil {
			return err
		}
		if actual != transition.From || version != transition.ExpectedVersion || s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence) != nil {
			return ErrStaleFence
		}
		entry, err := loadCurrentProviderPhaseEntry(ctx, conn, transition.Ref, phase, version, runner, transition.Fence.LeaderEpoch)
		if err != nil {
			return err
		}
		if err := validateProviderPhaseEntryBindings(ctx, conn, transition.Ref, entry); err != nil {
			return err
		}
		epoch, hasEpoch, err := loadProviderRetryEpochForEntry(ctx, conn, transition.Ref, phase, entry.Version)
		if err != nil {
			return err
		}
		expectedBindings := 2
		if hasEpoch {
			expectedBindings = 4
		}
		var entryBindings int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, phase, entry.Version).Scan(&entryBindings); err != nil {
			return err
		}
		if entryBindings != expectedBindings {
			return ErrEvidenceConflict
		}
		lower, upper, retryEpoch := 0, 0, 0
		if hasEpoch {
			lower, upper, retryEpoch = epoch.RetryFirst, epoch.RetryLast, 1
		}
		query := `SELECT attempt FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version=?`
		args := []any{transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, phase, entry.Version}
		if hasEpoch {
			query += ` AND attempt BETWEEN ? AND ?`
			args = append(args, lower, upper)
		}
		query += ` ORDER BY attempt`
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		attempts := make([]int, 0, 2)
		for rows.Next() {
			var attempt int
			if err := rows.Scan(&attempt); err != nil {
				rows.Close()
				return err
			}
			attempts = append(attempts, attempt)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(attempts) != 2 || attempts[0] != lower || attempts[1] != upper {
			if !hasEpoch && len(attempts) == 2 && attempts[1] == attempts[0]+1 {
				lower, upper = attempts[0], attempts[1]
			} else {
				return ErrEvidenceConflict
			}
		}
		pair, err := authenticateProviderRetryAttemptPair(ctx, conn, transition.Ref, phase, entry, lower, upper)
		if err != nil {
			return err
		}
		var active, activeRuns, leases, gitWriters, commandWriters int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&active); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&activeRuns); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope='provider'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&leases); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&gitWriters); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&commandWriters); err != nil {
			return err
		}
		if active != 0 || activeRuns != 0 || leases != 0 || gitWriters != 0 || commandWriters != 0 {
			return ErrEvidenceConflict
		}
		payload, err := json.Marshal(providerExhaustionPayload{Schema: providerExhaustionSchema, Phase: phase, EntryTicketVersion: entry.Version, RetryEpoch: retryEpoch, Attempts: pair.Attempts[:], Reason: pair.Reason})
		if err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=?,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, domain.StatePaused, transition.From, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, transition.Trigger, transition.From, domain.StatePaused, string(payload), time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

// TransitionProviderRetry records the sole operator-approved extension for
// this immutable phase entry before restoring the paused provider phase. It is
// deliberately independent of the ticket-wide fallback budget: a later,
// separately authenticated phase entry gets its own bounded extension.
func (s *Store) TransitionProviderRetry(ctx context.Context, transition Transition) (TransitionResult, error) {
	phase, ok := providerPhaseForState(transition.To)
	if !ok || transition.From != domain.StatePaused || transition.ResumeState != transition.To || transition.Trigger != "operator_retry" || transition.Ref.Validate() != nil || transition.ExpectedVersion == 0 || transition.Fence.LeaderEpoch == 0 || transition.Fence.RunnerEpoch == 0 {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var actual domain.State
		var resume sql.NullString
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state,resume_state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &resume, &version, &runner); err != nil {
			return err
		}
		if actual != domain.StatePaused || !resume.Valid || domain.State(resume.String) != transition.To || version != transition.ExpectedVersion || s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence) != nil {
			return ErrStaleFence
		}
		if err := exactStateChangeEvent(ctx, conn, transition.Ref, version, "retry_or_correction_exhausted", transition.To, domain.StatePaused); err != nil {
			return ErrEvidenceConflict
		}
		var trigger string
		var from, to domain.State
		var raw string
		if err := conn.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version).Scan(&trigger, &from, &to, &raw); err != nil {
			return err
		}
		var exhaustion providerExhaustionPayload
		if trigger != "retry_or_correction_exhausted" || from != transition.To || to != domain.StatePaused || json.Unmarshal([]byte(raw), &exhaustion) != nil || exhaustion.Schema != providerExhaustionSchema || exhaustion.Phase != phase || len(exhaustion.Attempts) != 2 {
			return ErrEvidenceConflict
		}
		if exhaustion.RetryEpoch == 1 {
			return ErrBudgetExhausted
		}
		if exhaustion.RetryEpoch != 0 || exhaustion.EntryTicketVersion == 0 || exhaustion.Attempts[0] <= 0 || exhaustion.Attempts[1] != exhaustion.Attempts[0]+1 {
			return ErrEvidenceConflict
		}
		entry, err := loadProviderPhaseEntryAt(ctx, conn, transition.Ref, phase, exhaustion.EntryTicketVersion)
		if err != nil || entry.Version != exhaustion.EntryTicketVersion || entry.State != transition.To {
			return ErrEvidenceConflict
		}
		pair, err := authenticateProviderRetryAttemptPair(ctx, conn, transition.Ref, phase, entry, exhaustion.Attempts[0], exhaustion.Attempts[1])
		if err != nil || pair.Reason != exhaustion.Reason {
			return ErrEvidenceConflict
		}
		var active, activeRuns, leases, gitWriters, commandWriters int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&active); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&activeRuns); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope='provider'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&leases); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&gitWriters); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&commandWriters); err != nil {
			return err
		}
		if active != 0 || activeRuns != 0 || leases != 0 || gitWriters != 0 || commandWriters != 0 {
			return ErrEvidenceConflict
		}
		if _, found, err := loadProviderRetryEpochForEntry(ctx, conn, transition.Ref, phase, entry.Version); err != nil || found {
			if err != nil {
				return err
			}
			return ErrBudgetExhausted
		}
		epoch := providerRetryEpoch{Phase: phase, EntryVersion: entry.Version, InitialFirst: exhaustion.Attempts[0], InitialLast: exhaustion.Attempts[1], RetryFirst: exhaustion.Attempts[1] + 1, RetryLast: exhaustion.Attempts[1] + 2, ExhaustionVersion: version, ExhaustionLeader: transition.Fence.LeaderEpoch, ExhaustionRunner: runner, RetryVersion: version + 1, RetryLeader: transition.Fence.LeaderEpoch, RetryRunner: runner}
		digest, err := providerRetryDigest(transition.Ref, epoch)
		if err != nil {
			return err
		}
		epoch.Digest = digest
		if _, err := conn.ExecContext(ctx, `INSERT INTO provider_retry_epochs(channel,project_id,ticket_id,phase,entry_ticket_version,epoch,initial_first_attempt,initial_last_attempt,retry_first_attempt,retry_last_attempt,exhaustion_ticket_version,exhaustion_leader_epoch,exhaustion_runner_epoch,retry_ticket_version,retry_leader_epoch,retry_runner_epoch,retry_digest,created_at) VALUES(?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, phase, entry.Version, epoch.InitialFirst, epoch.InitialLast, epoch.RetryFirst, epoch.RetryLast, epoch.ExhaustionVersion, epoch.ExhaustionLeader, epoch.ExhaustionRunner, epoch.RetryVersion, epoch.RetryLeader, epoch.RetryRunner, epoch.Digest, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=NULL,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, transition.To, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, domain.StatePaused, version, runner)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		payload, _ := json.Marshal(map[string]any{"schema": providerExhaustionSchema, "phase": phase, "entry_ticket_version": entry.Version, "retry_epoch": 1})
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, transition.Trigger, domain.StatePaused, transition.To, string(payload), time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

// ProviderRetryReplay recognizes the exact already-committed retry response
// window. It is intentionally a narrow daemon lost-response aid.
func (s *Store) ProviderRetryReplay(ctx context.Context, ticket Ticket) (bool, error) {
	phase, ok := providerPhaseForState(ticket.State)
	if !ok {
		return false, nil
	}
	var leader uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ticket.Ref.Channel).Scan(&leader); err != nil {
		return false, err
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, s.db, ticket.Ref, phase, ticket.Version, ticket.RunnerEpoch, leader)
	if err != nil {
		if errors.Is(err, ErrEvidenceConflict) {
			return false, nil
		}
		return false, err
	}
	epoch, found, err := loadProviderRetryEpochForEntry(ctx, s.db, ticket.Ref, phase, entry.Version)
	if err != nil || !found {
		return false, err
	}
	if epoch.RetryVersion != ticket.Version || epoch.RetryRunner != ticket.RunnerEpoch {
		return false, nil
	}
	if leader != epoch.RetryLeader || epoch.ExhaustionVersion == 0 {
		return false, nil
	}
	if err := validateProviderRetryAdvance(ctx, s.db, ticket.Ref, phase, epoch.ExhaustionVersion-1, epoch.ExhaustionRunner, epoch.ExhaustionLeader, epoch.RetryVersion, epoch.RetryRunner, epoch.RetryLeader); err != nil {
		return false, nil
	}
	return true, nil
}

// validateProviderRetryAdvance authenticates the sole no-runner-change gap
// that provider exhaustion and its one operator retry introduce. Recovery may
// traverse this pair, but may never treat an arbitrary version gap as renewed
// provider capacity.
func validateProviderRetryAdvance(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, phase domain.Phase, fromVersion, fromRunner, fromLeader, toVersion, toRunner, toLeader uint64) error {
	if fromVersion == 0 || fromRunner == 0 || fromLeader == 0 || toVersion != fromVersion+2 || toRunner != fromRunner || toLeader != fromLeader {
		return ErrPublicationEvidence
	}
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, phase, fromVersion)
	if err != nil {
		return ErrPublicationEvidence
	}
	epoch, found, err := loadProviderRetryEpochForEntry(ctx, q, ref, phase, entry.Version)
	if err != nil || !found || epoch.EntryVersion != entry.Version || epoch.ExhaustionVersion != fromVersion+1 || epoch.ExhaustionLeader != fromLeader || epoch.ExhaustionRunner != fromRunner || epoch.RetryVersion != toVersion || epoch.RetryLeader != toLeader || epoch.RetryRunner != toRunner {
		if err != nil {
			return err
		}
		return ErrPublicationEvidence
	}
	if err := exactStateChangeEvent(ctx, q, ref, fromVersion+1, "retry_or_correction_exhausted", providerStateForPhase(phase), domain.StatePaused); err != nil {
		return ErrPublicationEvidence
	}
	var exhaustionTrigger string
	var exhaustionFrom, exhaustionTo domain.State
	var exhaustionRaw string
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, fromVersion+1).Scan(&exhaustionTrigger, &exhaustionFrom, &exhaustionTo, &exhaustionRaw); err != nil {
		return ErrPublicationEvidence
	}
	var exhaustion providerExhaustionPayload
	if exhaustionTrigger != "retry_or_correction_exhausted" || exhaustionFrom != providerStateForPhase(phase) || exhaustionTo != domain.StatePaused || json.Unmarshal([]byte(exhaustionRaw), &exhaustion) != nil || exhaustion.Schema != providerExhaustionSchema || exhaustion.Phase != phase || exhaustion.EntryTicketVersion != entry.Version || exhaustion.RetryEpoch != 0 || len(exhaustion.Attempts) != 2 || exhaustion.Attempts[0] != epoch.InitialFirst || exhaustion.Attempts[1] != epoch.InitialLast {
		return ErrPublicationEvidence
	}
	query, ok := q.(rowQueryer)
	if !ok {
		return ErrPublicationEvidence
	}
	pair, err := authenticateProviderRetryAttemptPair(ctx, query, ref, phase, entry, epoch.InitialFirst, epoch.InitialLast)
	if err != nil || pair.Reason != exhaustion.Reason {
		return ErrPublicationEvidence
	}
	if err := exactStateChangeEvent(ctx, q, ref, toVersion, "operator_retry", domain.StatePaused, providerStateForPhase(phase)); err != nil {
		return ErrPublicationEvidence
	}
	var retryTrigger string
	var retryFrom, retryTo domain.State
	var retryRaw string
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, toVersion).Scan(&retryTrigger, &retryFrom, &retryTo, &retryRaw); err != nil {
		return ErrPublicationEvidence
	}
	var retry struct {
		Schema     string       `json:"schema"`
		Phase      domain.Phase `json:"phase"`
		RetryEpoch int          `json:"retry_epoch"`
	}
	if retryTrigger != "operator_retry" || retryFrom != domain.StatePaused || retryTo != providerStateForPhase(phase) || json.Unmarshal([]byte(retryRaw), &retry) != nil || retry.Schema != providerExhaustionSchema || retry.Phase != phase || retry.RetryEpoch != 1 {
		return ErrPublicationEvidence
	}
	return nil
}

// providerRetryRecoveryPredecessor supplies the exact pre-fence leader for a
// ticket resumed from provider exhaustion before it has produced a reusable
// completed result. This is deliberately narrower than a worktree fallback:
// it accepts only the immutable epoch's v→v+2 pause/retry pair.
func providerRetryRecoveryPredecessor(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	phase, ok := providerPhaseForState(state)
	if !ok {
		return 0, false, nil
	}
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, phase, version)
	if err != nil {
		return 0, false, nil
	}
	epoch, found, err := loadProviderRetryEpochForEntry(ctx, q, ref, phase, entry.Version)
	if err != nil || !found {
		return 0, false, err
	}
	if epoch.ExhaustionVersion < 2 || epoch.RetryVersion != version || epoch.RetryRunner != runner || epoch.RetryLeader == 0 || epoch.RetryLeader >= newLeader {
		return 0, false, ErrPublicationEvidence
	}
	if err := validateProviderRetryAdvance(ctx, q, ref, phase, epoch.ExhaustionVersion-1, epoch.ExhaustionRunner, epoch.ExhaustionLeader, epoch.RetryVersion, epoch.RetryRunner, epoch.RetryLeader); err != nil {
		return 0, false, err
	}
	return epoch.RetryLeader, true, nil
}

// providerPausedRecoveryPredecessor supplies the retained pre-fence leader
// after an ordinary pause/take has been resumed, but before Controller.Rearm
// has opened the new runtime admission.  This is not a generic control-gap
// bridge: it recognizes only the complete sealed provider control triplet and
// retains the immutable provider entry created before the stop.  A current
// provider state without that recognizable resume shape deliberately falls
// through to the ordinary phase/recovery baselines below.
func providerPausedRecoveryPredecessor(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	phase, ok := providerPhaseForState(state)
	// Reviewing is post-publication and has its own publication-bound recovery
	// baseline.  Letting this pre-publication bridge inspect it would bypass or
	// reject that stronger authority before FenceRecoveredRunners reaches it.
	if !ok || state == domain.StateReviewing || version < 3 || runner < 2 || newLeader == 0 {
		return 0, false, nil
	}

	// Only a paused -> provider-state operator resume is this authority's
	// recognizable prefix.  In particular, operator_retry belongs to the
	// V51 provider-exhaustion boundary and must never borrow a generic pause.
	var resumeCount int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND from_state='paused' AND to_state=? AND trigger IN ('operator_resume','operator_retry')`,
		ref.Channel, ref.Project, ref.Ticket, version, state).Scan(&resumeCount)
	if err != nil {
		return 0, false, err
	}
	if resumeCount == 0 {
		return 0, false, nil
	}
	if resumeCount != 1 {
		return 0, false, ErrPublicationEvidence
	}
	if err := exactStateChangeEvent(ctx, conn, ref, version, "operator_resume", domain.StatePaused, state); err != nil {
		return 0, false, ErrPublicationEvidence
	}

	var currentState domain.State
	var currentVersion, currentRunner, currentLeader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch
		FROM tickets t JOIN daemon_instances d ON d.channel=t.channel
		WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).
		Scan(&currentState, &currentVersion, &currentRunner, &currentLeader); err != nil ||
		currentState != state || currentVersion != version || currentRunner != runner || currentLeader != newLeader {
		return 0, false, ErrPublicationEvidence
	}

	control, err := runtimeControlFrom(ctx, conn, ref)
	if err != nil {
		return 0, false, ErrPublicationEvidence
	}
	// A successful Controller.Rearm has already opened a new durable runtime
	// admission at this resumed endpoint. It is no longer the crash window this
	// helper owns; normal phase/control recovery validates that open authority.
	if control.state == "open" || control.state == "armed" {
		return 0, false, nil
	}
	if control.state != "sealed" || control.authority != control.stop ||
		control.stop.version == 0 || control.stop.runner < 2 || control.stop.leader == 0 ||
		control.stop.version+2 != version || control.stop.runner != runner ||
		control.stop.leader >= newLeader {
		return 0, false, ErrPublicationEvidence
	}
	stopVersion := control.stop.version
	entry, err := loadProviderPhaseEntryAt(ctx, conn, ref, phase, stopVersion-1)
	if err != nil || entry.State != state || entry.Version != stopVersion-1 ||
		entry.Runner+1 != runner || entry.Leader != control.stop.leader {
		return 0, false, ErrPublicationEvidence
	}
	if err := validateProviderPhaseEntryBindings(ctx, conn, ref, entry); err != nil {
		return 0, false, ErrPublicationEvidence
	}
	if err := exactStateChangeEvent(ctx, conn, ref, stopVersion, "operator_pause_or_take", state, domain.StateStopping); err != nil {
		return 0, false, ErrPublicationEvidence
	}
	if err := exactStateChangeEvent(ctx, conn, ref, stopVersion+1, "process_and_effects_drained", domain.StateStopping, domain.StatePaused); err != nil {
		return 0, false, ErrPublicationEvidence
	}
	for _, query := range []string{
		`SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`,
		`SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`,
		`SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`,
		`SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`,
		`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('executing','uncertain')`,
	} {
		var writers int
		if err := conn.QueryRowContext(ctx, query, ref.Channel, ref.Project, ref.Ticket).Scan(&writers); err != nil || writers != 0 {
			return 0, false, ErrPublicationEvidence
		}
	}
	return control.stop.leader, true, nil
}

// providerBlockedRecoveryPredecessor supplies the signed endpoint after an
// exact provider typed_blocker -> operator_recover pair.  This is deliberately
// separate from generic blocked recovery: it retains the phase entry and does
// not create capacity or accept a leader-only gap.
func providerBlockedRecoveryPredecessor(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, ref domain.TicketRef, state domain.State, version, runner, newLeader uint64) (uint64, bool, error) {
	phase, ok := providerPhaseForState(state)
	if !ok || version < 3 || runner == 0 {
		return 0, false, nil
	}
	// This helper is reached for every active provider state during startup.
	// It is an authority only for the exact typed_blocker -> operator_recover
	// pair; an older phase entry plus a routine runner-recovery row must fall
	// through to the ordinary recovery-ledger proof below.  Treating every
	// non-matching current endpoint as malformed made a healthy third daemon
	// takeover indistinguishable from a forged provider-recovery pair.
	var recoveries, blockers int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_recover' AND from_state='blocked' AND to_state=?`, ref.Channel, ref.Project, ref.Ticket, version, state).Scan(&recoveries); err != nil {
		return 0, false, err
	}
	if recoveries == 0 {
		return 0, false, nil
	}
	if recoveries != 1 {
		return 0, false, ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='typed_blocker' AND from_state=? AND to_state='blocked'`, ref.Channel, ref.Project, ref.Ticket, version-1, state).Scan(&blockers); err != nil {
		return 0, false, err
	}
	if blockers != 1 {
		return 0, false, ErrPublicationEvidence
	}
	preVersion := version - 2
	var prior uint64
	if recovery, found, err := loadRunnerRecoveryAt(ctx, q, ref, preVersion); err != nil {
		return 0, false, err
	} else if found && recovery.RunnerEpoch == runner {
		prior = recovery.LeaderEpoch
	}
	entry, err := loadProviderPhaseEntryAt(ctx, q, ref, phase, preVersion)
	if err != nil {
		return 0, false, nil
	}
	if prior == 0 && entry.Version == preVersion && entry.Runner == runner {
		prior = entry.Leader
	}
	if prior == 0 {
		if epoch, found, err := loadProviderRetryEpochForEntry(ctx, q, ref, phase, entry.Version); err != nil {
			return 0, false, err
		} else if found && epoch.RetryVersion == preVersion && epoch.RetryRunner == runner {
			prior = epoch.RetryLeader
		}
	}
	if prior == 0 || prior >= newLeader || validateProviderBlockedRecoveryAdvance(ctx, q, ref, entry, version, runner, prior) != nil {
		return 0, false, ErrPublicationEvidence
	}
	return prior, true, nil
}

func providerStateForPhase(phase domain.Phase) domain.State {
	switch phase {
	case domain.PhasePlanning:
		return domain.StatePlanning
	case domain.PhaseVerification:
		return domain.StateVerifying
	case domain.PhaseBuild:
		return domain.StateBuilding
	case domain.PhaseReview:
		return domain.StateReviewing
	default:
		return ""
	}
}

// backfillV51ProviderPhaseEntries recognizes only the v50 Relay recovery
// shape: a canonical start endpoint and exactly two terminal, resultless
// planning attempts. Every other legacy provider-state ticket is blocked by
// the companion migration helper rather than receiving fabricated capacity.
func backfillV51ProviderPhaseEntries(ctx context.Context, conn *sql.Conn) error {
	// Buffer candidates before performing their individual authority checks.
	// A *sql.Conn is the transaction boundary here; interleaving a QueryRow or
	// Exec with an open cursor is not a safe SQLite reader pattern.
	rows, err := conn.QueryContext(ctx, `SELECT t.channel,t.project_id,t.id,a.leader_epoch,t.runner_epoch,e.id,e.created_at,e.trigger
		FROM tickets t JOIN runner_start_authorities a ON a.channel=t.channel AND a.project_id=t.project_id AND a.ticket_id=t.id AND a.start_ticket_version=2 AND a.runner_epoch=1
		JOIN events e ON e.channel=t.channel AND e.project_id=t.project_id AND e.ticket_id=t.id AND e.ticket_version=2
		WHERE t.state='planning' AND t.version>=2 AND t.runner_epoch=1
		AND e.trigger IN ('operator_start','start_or_adopt') AND e.from_state='queued' AND e.to_state='planning' AND e.payload='{}'`)
	if err != nil {
		return err
	}
	type candidate struct {
		ref                   domain.TicketRef
		leader, runner        uint64
		eventID               int64
		eventCreated, trigger string
	}
	var candidates []candidate
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.ref.Channel, &value.ref.Project, &value.ref.Ticket, &value.leader, &value.runner, &value.eventID, &value.eventCreated, &value.trigger); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, candidate := range candidates {
		ref, leader, runner := candidate.ref, candidate.leader, candidate.runner
		var starts int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=2 AND trigger IN ('operator_start','start_or_adopt') AND from_state='queued' AND to_state='planning' AND payload='{}'`, ref.Channel, ref.Project, ref.Ticket).Scan(&starts); err != nil || starts != 1 || leader == 0 || runner != 1 {
			continue
		}
		var authority int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_start_authorities WHERE channel=? AND project_id=? AND ticket_id=? AND start_ticket_version=2 AND runner_epoch=1 AND leader_epoch=?`, ref.Channel, ref.Project, ref.Ticket, leader).Scan(&authority); err != nil || authority != 1 {
			continue
		}
		var phaseRuns, attempts, terminal, results int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase='planning'`, ref.Channel, ref.Project, ref.Ticket).Scan(&phaseRuns); err != nil || phaseRuns != 2 {
			continue
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='planning'`, ref.Channel, ref.Project, ref.Ticket).Scan(&attempts); err != nil || attempts != 2 {
			continue
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs r JOIN provider_attempts a ON a.channel=r.channel AND a.project_id=r.project_id AND a.ticket_id=r.ticket_id AND a.phase=r.phase AND a.attempt=r.attempt AND a.provider=r.provider AND a.model=r.model AND a.family=r.family AND a.version=r.provider_version AND a.expected_ticket_version=r.expected_ticket_version AND a.leader_epoch=r.leader_epoch AND a.runner_epoch=r.runner_epoch AND a.worktree_identity=r.worktree_identity AND a.base_sha=r.base_sha WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.phase='planning' AND r.attempt IN (1,2) AND r.state IN ('failed','cancelled') AND r.outcome IN ('failed','cancelled') AND a.role='planner' AND a.state IN ('failed','cancelled') AND a.outcome IN ('failed','cancelled') AND a.expected_ticket_version=2 AND a.leader_epoch=? AND a.runner_epoch=1`, ref.Channel, ref.Project, ref.Ticket, leader).Scan(&terminal); err != nil || terminal != 2 {
			continue
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase='planning' AND a.attempt IN (1,2)`, ref.Channel, ref.Project, ref.Ticket).Scan(&results); err != nil || results != 0 {
			continue
		}
		var writers int
		for _, query := range []string{
			`SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`,
			`SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND state='active'`,
			`SELECT COUNT(*) FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope='provider'`,
			`SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`,
			`SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`,
		} {
			if err := conn.QueryRowContext(ctx, query, ref.Channel, ref.Project, ref.Ticket).Scan(&writers); err != nil || writers != 0 {
				writers = -1
				break
			}
		}
		if writers != 0 {
			continue
		}
		if err := recordProviderPhaseEntry(ctx, conn, ref, domain.PhasePlanning, 2, leader, 1, candidate.eventID, candidate.eventCreated, domain.StateQueued, domain.StatePlanning, candidate.trigger); err != nil {
			return err
		}
		attemptRows, err := conn.QueryContext(ctx, `SELECT id,role,attempt FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='planning' AND attempt IN (1,2) ORDER BY attempt`, ref.Channel, ref.Project, ref.Ticket)
		if err != nil {
			return err
		}
		type attempt struct {
			id     int64
			role   string
			number int
		}
		var bound []attempt
		for attemptRows.Next() {
			var value attempt
			if err := attemptRows.Scan(&value.id, &value.role, &value.number); err != nil {
				attemptRows.Close()
				return err
			}
			bound = append(bound, value)
		}
		if err := attemptRows.Err(); err != nil {
			attemptRows.Close()
			return err
		}
		if err := attemptRows.Close(); err != nil {
			return err
		}
		for _, value := range bound {
			if _, err := conn.ExecContext(ctx, `INSERT INTO provider_phase_attempt_entries(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,entry_ticket_version,created_at) VALUES(?,?,?,?,?,?,?,2,?)`, value.id, ref.Channel, ref.Project, ref.Ticket, domain.PhasePlanning, value.role, value.number, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
	}
	return nil
}

// legacyProviderMigrationRecoveryPredecessor is the one startup-fencing
// exception for V51's fail-closed migration blocker. A V50 provider could have
// crossed the launch gate before V51 found that its phase-entry lineage was
// unverifiable. The ticket is deliberately not schedulable, but its exact
// launched process must still be fenced before a replacement daemon can prove
// it drained. Nothing other than the migration's canonical blocker endpoint,
// one exact old claim/phase/lease tuple, and a newer leader can supply this
// predecessor. A preexisting V50 typed blocker whose code V51 later rewrote
// to the legacy classification is accepted by the same exact endpoint proof.
func legacyProviderMigrationRecoveryPredecessor(ctx context.Context, q rowQueryer, ref domain.TicketRef, version, runner, newLeader uint64) (uint64, error) {
	if ref.Validate() != nil || version < 2 || runner == 0 || newLeader == 0 {
		return 0, ErrPublicationEvidence
	}
	var state, resume domain.State
	var code string
	if err := q.QueryRowContext(ctx, `SELECT state,COALESCE(resume_state,''),blocked_code FROM tickets WHERE channel=? AND project_id=? AND id=? AND version=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, version, runner).Scan(&state, &resume, &code); err != nil || state != domain.StateBlocked || code != "legacy_provider_phase_entry_unverifiable" {
		return 0, ErrPublicationEvidence
	}
	phase, role, ok := recoveryProviderPhase(resume)
	if !ok {
		return 0, ErrPublicationEvidence
	}
	var matching, stateChanges int
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(SUM(CASE WHEN trigger='typed_blocker' AND to_state='blocked' THEN 1 ELSE 0 END),0),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&matching, &stateChanges); err != nil || matching != 1 || stateChanges != 1 {
		return 0, ErrPublicationEvidence
	}
	var trigger, payload string
	var from, to domain.State
	var blocker struct {
		Code string `json:"code"`
	}
	if err := q.QueryRowContext(ctx, `SELECT trigger,from_state,to_state,payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&trigger, &from, &to, &payload); err != nil || trigger != "typed_blocker" || to != domain.StateBlocked || json.Unmarshal([]byte(payload), &blocker) != nil || !validBlockedCode(blocker.Code) || (from != resume && from != domain.StateStopping) {
		return 0, ErrPublicationEvidence
	}
	rows, err := q.QueryContext(ctx, `SELECT id FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND role=? AND expected_ticket_version=? AND runner_epoch=? AND leader_epoch>0 AND leader_epoch<? AND state IN ('active','quarantined') ORDER BY id`, ref.Channel, ref.Project, ref.Ticket, phase, role, version-1, runner, newLeader)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var id int64
	count := 0
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil || count != 1 {
		return 0, ErrPublicationEvidence
	}
	claim, err := loadAuthenticatedProviderAttemptClaim(ctx, q, id)
	if err != nil || claim.Ref != ref || claim.Phase != phase || claim.Role != role || claim.ExpectedVersion+1 != version || claim.RunnerEpoch != runner || claim.LeaderEpoch >= newLeader {
		return 0, ErrPublicationEvidence
	}
	var phaseRuns, leases int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM phase_runs WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND state='active' AND leader_epoch=? AND runner_epoch=? AND expected_ticket_version=? AND provider=? AND model=? AND family=? AND provider_version=? AND worktree_identity=? AND base_sha=?`, ref.Channel, ref.Project, ref.Ticket, claim.Phase, claim.Attempt, claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion, claim.Binding.Identity.Provider, claim.Binding.Identity.Model, claim.Binding.Identity.Family, claim.Binding.Identity.Version, claim.WorktreeIdentity, claim.BaseSHA).Scan(&phaseRuns); err != nil || phaseRuns != 1 {
		return 0, ErrPublicationEvidence
	}
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND scope='provider' AND scope_key=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, claim.LeaseKey, claim.RunnerEpoch).Scan(&leases); err != nil || leases != 1 {
		return 0, ErrPublicationEvidence
	}
	return claim.LeaderEpoch, nil
}

func blockUnverifiableV51ProviderEntries(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `SELECT channel,project_id,id,state,COALESCE(resume_state,''),version,runner_epoch FROM tickets WHERE state IN ('planning','verifying','building','reviewing') OR (state IN ('paused','blocked','stopping') AND resume_state IN ('planning','verifying','building','reviewing'))`)
	if err != nil {
		return err
	}
	type candidate struct {
		ref             domain.TicketRef
		state, resume   domain.State
		version, runner uint64
	}
	var candidates []candidate
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.ref.Channel, &value.ref.Project, &value.ref.Ticket, &value.state, &value.resume, &value.version, &value.runner); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, candidate := range candidates {
		state := candidate.state
		if state == domain.StatePaused || state == domain.StateBlocked || state == domain.StateStopping {
			state = candidate.resume
		}
		phase, ok := providerPhaseForState(state)
		if !ok {
			continue
		}
		var count int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase=? AND entry_ticket_version<=?`, candidate.ref.Channel, candidate.ref.Project, candidate.ref.Ticket, phase, candidate.version).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			continue
		}
		query := `UPDATE tickets SET state='blocked',resume_state=?,blocked_code='legacy_provider_phase_entry_unverifiable',version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`
		args := []any{state, candidate.ref.Channel, candidate.ref.Project, candidate.ref.Ticket, candidate.state, candidate.version, candidate.runner}
		if candidate.state == domain.StatePaused || candidate.state == domain.StateBlocked {
			// No listed state-machine transition can turn a paused or already
			// blocked legacy ticket into this new fail-closed classification. Keep
			// its historical version and emit no synthetic typed_blocker event: the
			// migration is recording an incompatibility, not replaying a workflow
			// action that never happened.
			query = `UPDATE tickets SET state='blocked',resume_state=?,blocked_code='legacy_provider_phase_entry_unverifiable' WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`
		} else {
			payload := `{"code":"legacy_provider_phase_entry_unverifiable"}`
			if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?, 'typed_blocker', ?, 'blocked', ?, ?)`, candidate.ref.Channel, candidate.ref.Project, candidate.ref.Ticket, candidate.version+1, candidate.state, payload, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
		result, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return ErrEvidenceConflict
		}
	}
	return nil
}
