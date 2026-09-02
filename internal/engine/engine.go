// Package engine implements the selected single custom workflow runtime. It
// translates authenticated guard results into the normative state-machine spec;
// store remains the sole authority for the resulting transaction and event.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
)

type Engine struct {
	store *store.Store
	spec  statemachine.Spec
}

func Open(ctx context.Context, databasePath, stateMachinePath string) (*Engine, error) {
	file, err := os.Open(stateMachinePath)
	if err != nil {
		return nil, fmt.Errorf("open normative state machine: %w", err)
	}
	defer file.Close()
	spec, err := statemachine.LoadApproved(file)
	if err != nil {
		return nil, err
	}
	database, err := store.Open(ctx, databasePath)
	if err != nil {
		return nil, err
	}
	return &Engine{store: database, spec: spec}, nil
}

func New(database *store.Store, spec statemachine.Spec) *Engine {
	return &Engine{store: database, spec: spec}
}

func (e *Engine) Close() error { return e.store.Close() }

func (e *Engine) Start(ctx context.Context, request contracts.StartRequest) error {
	_, err := e.store.StartOrAdopt(ctx, request.Ticket, request.TicketVersion, request.WorkflowID, request.Fence)
	return err
}

func (e *Engine) StartOrAdopt(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, stableWorkflowID string, fence domain.Fence) (store.Ticket, error) {
	return e.store.StartOrAdopt(ctx, ref, expectedVersion, stableWorkflowID, fence)
}

func (e *Engine) Transition(ctx context.Context, request contracts.TransitionRequest) (contracts.TransitionResult, error) {
	// External merge observations are evidence-bearing boundaries, not generic
	// state-machine transitions. Dedicated Store paths authenticate manual and
	// guarded observations before changing durable lifecycle state.
	if request.Trigger == "external_merge_observed" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	// The source_changes branch is deliberately not a generic state-machine
	// persistence path. Its event binds a sealed operator-take lineage to the
	// immutable plan, verification checkpoint, and registered worktree; only
	// SignalOperatorSourceResume may mint it.
	if genericOperatorSourceResume(request) {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	if genericVerificationAmendmentTransition(request.Trigger) {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	if request.Trigger == "phase_pass" && (request.From == domain.StatePlanning || request.From == domain.StateVerifying || request.From == domain.StateBuilding) {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, request, e.store.Transition)
}

// genericVerificationAmendmentTransition rejects every generic entry to the
// Builder/Reviewer amendment boundary. Each trigger carries Store-authenticated
// authority that the generic state-machine persistence path cannot construct.
func genericVerificationAmendmentTransition(trigger string) bool {
	switch trigger {
	case "verification_amendment_requested", "amendment_accepted", "amendment_rejected", "review_repair":
		return true
	default:
		return false
	}
}

func genericOperatorSourceResume(request contracts.TransitionRequest) bool {
	if request.From != domain.StatePaused || request.Trigger != "operator_resume" {
		return false
	}
	if request.Attributes["takeover_source_commit_valid"] == "true" || request.Attributes["takeover_source_diff_valid"] == "true" || request.Attributes["verification_files_unchanged"] == "true" {
		return true
	}
	var payload struct {
		ChangeKind string `json:"change_kind"`
	}
	return json.Unmarshal([]byte(request.EventPayload), &payload) == nil && (payload.ChangeKind == "source_commit" || payload.ChangeKind == "source_changes")
}

// SignalOperatorSourceResume is the sole Engine entry point for a retained,
// source-only operator diff. Store independently authenticates the complete
// control/evidence lineage and builds the durable event payload itself.
func (e *Engine) SignalOperatorSourceResume(ctx context.Context, request store.OperatorSourceResume) (contracts.TransitionResult, error) {
	if request.Ref.Validate() != nil || request.ExpectedVersion == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	transition, err := e.spec.Select(string(domain.StatePaused), "operator_resume", map[string]bool{
		"operator_identity_authenticated": true,
		"takeover_source_commit_valid":    true,
		"verification_files_unchanged":    true,
		"branch_remote_identity_exact":    true,
	})
	if err != nil || transition.ID != "resume_source_changes" || transition.To != string(domain.StateVerifying) {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	ticket, err := e.store.Ticket(ctx, request.Ref)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	if ticket.State != domain.StatePaused || ticket.Version != request.ExpectedVersion {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	result, err := e.store.TransitionOperatorSourceResume(ctx, request)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	return contracts.TransitionResult{To: domain.StateVerifying, TicketVersion: result.Version, Invalidated: invalidations(e.spec, transition.Invalidates), EventID: fmt.Sprint(result.EventID)}, nil
}

func (e *Engine) transition(ctx context.Context, request contracts.TransitionRequest, persist func(context.Context, store.Transition) (store.TransitionResult, error)) (contracts.TransitionResult, error) {
	guards := make(map[string]bool, len(request.Attributes))
	for key, value := range request.Attributes {
		if value == "true" {
			guards[key] = true
		}
	}
	transition, err := e.spec.Select(string(request.From), request.Trigger, guards)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	ticket, err := e.store.Ticket(ctx, request.Ticket)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	if ticket.State != request.From || ticket.Version != request.TicketVersion {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	target, err := statemachine.ResolveTarget(transition.To, string(request.From), string(ticket.ResumeState), string(ticket.ResumeState))
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	resume := domain.State(transition.ResumeState)
	if transition.ResumeState == "$from" {
		resume = request.From
	}
	if transition.ResumeState == "$resume_state" {
		resume = ticket.ResumeState
	}
	if transition.ResumeState == "$stored" {
		resume = ticket.ResumeState
	}
	persisted := store.Transition{
		Ref: request.Ticket, ExpectedVersion: request.TicketVersion, From: request.From,
		To: target, ResumeState: resume, Trigger: request.Trigger, Fence: request.Fence,
		EventPayload: request.EventPayload,
	}
	if persisted.EventPayload == "" {
		persisted.EventPayload = "{}"
	}
	var result store.TransitionResult
	if transition.ID == "stopped" || transition.ID == "cancelled" {
		result, err = e.store.CompleteControlTransition(ctx, persisted)
	} else if strings.HasPrefix(transition.PhaseDisposition, "invalidate_runner_epoch") {
		result, err = e.store.TransitionAndInvalidateRunner(ctx, persisted)
	} else if ((persisted.From == domain.StatePaused && (persisted.Trigger == "operator_resume" || persisted.Trigger == "operator_retry")) || (persisted.From == domain.StateBlocked && persisted.Trigger == "operator_recover")) && (persisted.To == domain.StatePublishing || persisted.To == domain.StateWaitingCI) {
		result, err = e.store.TransitionPublishedResume(ctx, persisted)
	} else if persisted.From == domain.StatePaused && persisted.To == domain.StateMerging && (persisted.Trigger == "operator_resume" || persisted.Trigger == "operator_retry") {
		result, err = e.store.TransitionGuardedMergeResume(ctx, persisted)
	} else if persisted.From == domain.StatePaused && persisted.To == domain.StateReconciling && (persisted.Trigger == "operator_resume" || persisted.Trigger == "operator_retry") {
		result, err = e.store.TransitionPostPublicationReconcileResume(ctx, persisted)
	} else if persisted.From == domain.StateMerging && persisted.To == domain.StateReconciling && persisted.Trigger == "merge_observed" {
		result, err = e.store.TransitionGuardedMergeObserved(ctx, persisted)
	} else {
		result, err = persist(ctx, persisted)
	}
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	return contracts.TransitionResult{To: target, TicketVersion: result.Version, Invalidated: invalidations(e.spec, transition.Invalidates), EventID: fmt.Sprint(result.EventID)}, nil
}

func invalidations(spec statemachine.Spec, sets []string) []string {
	seen := make(map[string]bool)
	var result []string
	for _, set := range sets {
		values, ok := spec.InvalidationSets[set]
		if !ok {
			values = []string{set}
		}
		for _, value := range values {
			if !seen[value] {
				seen[value] = true
				result = append(result, value)
			}
		}
	}
	return result
}

func (e *Engine) Signal(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	return e.Transition(ctx, contracts.TransitionRequest{
		Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From,
		Trigger: request.Trigger, Fence: request.Fence, Attributes: request.Attributes, EventPayload: request.EventPayload,
	})
}

// SignalPlan is the only planning phase-pass entry point. Store consumes the
// current planner binding atomically with the state transition.
func (e *Engine) SignalPlan(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From != domain.StatePlanning || request.Trigger != "phase_pass" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: request.Trigger, Fence: request.Fence, Attributes: map[string]string{"typed_plan_valid": "true"}, EventPayload: request.EventPayload}, e.store.TransitionPlan)
}

// SignalVerification is the only verification phase-pass entry point.
func (e *Engine) SignalVerification(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From != domain.StateVerifying || request.Trigger != "phase_pass" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: request.Trigger, Fence: request.Fence, Attributes: map[string]string{"independent_intent_valid": "true", "prebuild_proof_valid": "true", "verification_checkpoint_committed": "true"}, EventPayload: request.EventPayload}, e.store.TransitionVerification)
}

// SignalVerificationAmendment is the only lifecycle entry for a Builder
// request to replace protected pre-build verification. Store binds the request
// and the fresh Reviewer decision; caller attributes are only the normative
// selector inputs.
func (e *Engine) SignalVerificationAmendment(ctx context.Context, request contracts.SignalRequest, decision store.VerificationAmendmentDecision, key store.ProviderAttemptResultKey) (contracts.TransitionResult, error) {
	if request.From != domain.StateVerifying || key.Ref != request.Ticket || key.Phase != domain.PhaseVerification {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	var trigger string
	var persist func(context.Context, store.Transition) (store.TransitionResult, error)
	switch decision {
	case store.VerificationAmendmentAccepted:
		trigger = "amendment_accepted"
		persist = func(ctx context.Context, transition store.Transition) (store.TransitionResult, error) {
			return e.store.TransitionVerificationAmendmentAccepted(ctx, transition, key)
		}
	case store.VerificationAmendmentRejected:
		trigger = "amendment_rejected"
		persist = func(ctx context.Context, transition store.Transition) (store.TransitionResult, error) {
			return e.store.TransitionVerificationAmendmentRejected(ctx, transition, key)
		}
	default:
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: trigger, Fence: request.Fence, Attributes: map[string]string{"fresh_reviewer": "true", "old_and_new_digest_bound": "true", "correction_available": "true", "original_verification_intent_current": "true"}, EventPayload: request.EventPayload}, persist)
}

// SignalVerificationAmendmentRequest consumes a completed Builder request in
// the same lifecycle transaction as building -> verifying.
func (e *Engine) SignalVerificationAmendmentRequest(ctx context.Context, request contracts.SignalRequest, key store.ProviderAttemptResultKey) (contracts.TransitionResult, error) {
	if request.From != domain.StateBuilding || key.Ref != request.Ticket || key.Phase != domain.PhaseBuild {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: "verification_amendment_requested", Fence: request.Fence, Attributes: map[string]string{"amendment_request_valid": "true", "correction_available": "true"}, EventPayload: request.EventPayload}, func(ctx context.Context, transition store.Transition) (store.TransitionResult, error) {
		return e.store.TransitionVerificationAmendmentRequest(ctx, transition, key)
	})
}

// SignalVerificationAmendmentBlocked is the typed fail-closed exit for a
// malformed Builder request or Reviewer response. It deliberately does not
// reinterpret the response as a rejection: only the original proof is a
// valid rejection, and only the Builder-bound proposed proof is acceptance.
func (e *Engine) SignalVerificationAmendmentBlocked(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From != domain.StateBuilding && request.From != domain.StateVerifying {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: "typed_blocker", Fence: request.Fence, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"verification_amendment_invalid"}`}, e.store.Transition)
}

// SignalCandidate consumes the exact candidate in the same SQLite write as
// the state transition.  It is intentionally separate from Signal so no
// caller can accidentally turn a candidate-bound build pass into a generic
// transition after a stale read.
func (e *Engine) SignalCandidate(ctx context.Context, request contracts.SignalRequest, candidate domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	if request.From != domain.StateBuilding || request.Trigger != "phase_pass" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	ticket, err := e.store.Ticket(ctx, request.Ticket)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	if ticket.State != request.From || ticket.Version != request.TicketVersion {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	attributes := map[string]string{"proof_green": "true", "diff_valid": "true", "git_control_plane_valid": "true", "candidate_checkpoint_committed": "true"}
	if ticket.Type == domain.TicketSpike {
		attributes["ticket_type_spike"] = "true"
	} else {
		attributes["ticket_type_not_spike"] = "true"
	}
	guards := make(map[string]bool, len(attributes))
	for key, value := range attributes {
		guards[key] = value == "true"
	}
	transition, err := e.spec.Select(string(request.From), request.Trigger, guards)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	target, err := statemachine.ResolveTarget(transition.To, string(request.From), string(ticket.ResumeState), string(ticket.ResumeState))
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	resume := domain.State(transition.ResumeState)
	if transition.ResumeState == "$from" {
		resume = request.From
	}
	if transition.ResumeState == "$stored" {
		resume = ticket.ResumeState
	}
	result, err := e.store.TransitionCandidate(ctx, store.Transition{Ref: request.Ticket, ExpectedVersion: request.TicketVersion, From: request.From, To: target, ResumeState: resume, Trigger: request.Trigger, Fence: request.Fence, EventPayload: request.EventPayload}, candidate)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	return contracts.TransitionResult{To: target, TicketVersion: result.Version, Invalidated: invalidations(e.spec, transition.Invalidates), EventID: fmt.Sprint(result.EventID)}, nil
}

// SignalFinalReview is the sole final-review pass entry point.  The Store
// consumes the completed immutable Reviewer result with the reviewing exit,
// so a stale provider response can never advance a newer candidate.
func (e *Engine) SignalFinalReview(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From != domain.StateReviewing || request.Trigger != "review_pass" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	ticket, err := e.store.Ticket(ctx, request.Ticket)
	if err != nil || ticket.State != domain.StateReviewing || ticket.Version != request.TicketVersion {
		return contracts.TransitionResult{}, store.ErrStaleFence
	}
	attributes := map[string]string{}
	if ticket.Type == domain.TicketSpike {
		attributes["ticket_type_spike"] = "true"
		attributes["report_present"] = "true"  // Store independently proves it.
		attributes["no_merge_effect"] = "true" // Store independently proves it.
	} else if ticket.MergeMode == domain.MergeGuarded {
		attributes["ticket_type_not_spike"] = "true"
		attributes["merge_mode_guarded"] = "true"
		attributes["all_nonapproval_gates_green"] = "true"
	} else if ticket.MergeMode == domain.MergeManual {
		attributes["ticket_type_not_spike"] = "true"
		attributes["merge_mode_manual"] = "true"
		attributes["all_nonapproval_gates_green"] = "true"
	} else {
		// Autonomous merging remains deliberately unavailable until its separate
		// qualified merge authority is composed.
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: request.Trigger, Fence: request.Fence, Attributes: attributes, EventPayload: request.EventPayload}, e.store.TransitionFinalReview)
}

// SignalAutonomyBlocked consumes the same authenticated final-review pass as
// a normal review exit, but records the normative closed autonomy prerequisite
// rather than offering an approval shortcut.
func (e *Engine) SignalAutonomyBlocked(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From != domain.StateReviewing || request.Trigger != "review_pass" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: "review_pass", Fence: request.Fence, Attributes: map[string]string{"ticket_type_not_spike": "true", "merge_mode_autonomous": "true", "autonomy_ineligible": "true"}, EventPayload: `{"code":"autonomy_ineligible"}`}, e.store.TransitionFinalReview)
}

// SignalFinalReviewRepair derives the state-machine branch from the durable
// reviewer owner, then lets Store consume that same immutable result/budget.
func (e *Engine) SignalFinalReviewRepair(ctx context.Context, request contracts.SignalRequest, owner string) (contracts.TransitionResult, error) {
	if request.From != domain.StateReviewing || request.Trigger != "review_repair" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	attributes := map[string]string{"correction_available": "true"}
	if owner == "builder" {
		attributes["repair_owner_builder"] = "true"
	} else if owner == "reviewer" {
		attributes["repair_owner_verification"] = "true"
	} else {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: request.Trigger, Fence: request.Fence, Attributes: attributes, EventPayload: request.EventPayload}, e.store.TransitionReviewRepair)
}

func (e *Engine) SignalFinalReviewNeedsOperator(ctx context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	if request.From != domain.StateReviewing {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, contracts.TransitionRequest{Ticket: request.Ticket, TicketVersion: request.TicketVersion, From: request.From, Trigger: "typed_blocker", Fence: request.Fence, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"review_needs_operator"}`}, e.store.TransitionReviewNeedsOperator)
}

func (e *Engine) Recover(ctx context.Context, request contracts.RecoveryRequest) error {
	return e.store.BlockOrphanedWorkflows(ctx, request.Channel, request.LeaderEpoch)
}

func (e *Engine) RecoverChannel(ctx context.Context, channel domain.Channel, leaderEpoch uint64) error {
	return e.store.BlockOrphanedWorkflows(ctx, channel, leaderEpoch)
}

var _ contracts.WorkflowEngine = (*Engine)(nil)
