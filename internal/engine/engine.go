// Package engine implements the selected single custom workflow runtime. It
// translates authenticated guard results into the normative state-machine spec;
// store remains the sole authority for the resulting transaction and event.
package engine

import (
	"context"
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
	if request.Trigger == "phase_pass" && (request.From == domain.StatePlanning || request.From == domain.StateVerifying || request.From == domain.StateBuilding) {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	return e.transition(ctx, request, e.store.Transition)
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

// SignalCandidate consumes the exact candidate in the same SQLite write as
// the state transition.  It is intentionally separate from Signal so no
// caller can accidentally turn a candidate-bound build pass into a generic
// transition after a stale read.
func (e *Engine) SignalCandidate(ctx context.Context, request contracts.SignalRequest, candidate domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	if request.From != domain.StateBuilding || request.Trigger != "phase_pass" {
		return contracts.TransitionResult{}, store.ErrEvidenceConflict
	}
	attributes := map[string]string{"proof_green": "true", "diff_valid": "true", "git_control_plane_valid": "true", "candidate_checkpoint_committed": "true"}
	guards := make(map[string]bool, len(attributes))
	for key, value := range attributes {
		guards[key] = value == "true"
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
	if transition.ResumeState == "$stored" {
		resume = ticket.ResumeState
	}
	result, err := e.store.TransitionCandidate(ctx, store.Transition{Ref: request.Ticket, ExpectedVersion: request.TicketVersion, From: request.From, To: target, ResumeState: resume, Trigger: request.Trigger, Fence: request.Fence, EventPayload: request.EventPayload}, candidate)
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	return contracts.TransitionResult{To: target, TicketVersion: result.Version, Invalidated: invalidations(e.spec, transition.Invalidates), EventID: fmt.Sprint(result.EventID)}, nil
}

func (e *Engine) Recover(ctx context.Context, request contracts.RecoveryRequest) error {
	return e.store.BlockOrphanedWorkflows(ctx, request.Channel, request.LeaderEpoch)
}

func (e *Engine) RecoverChannel(ctx context.Context, channel domain.Channel, leaderEpoch uint64) error {
	return e.store.BlockOrphanedWorkflows(ctx, channel, leaderEpoch)
}

var _ contracts.WorkflowEngine = (*Engine)(nil)
