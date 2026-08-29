// Package engine implements the selected single custom workflow runtime. It
// translates authenticated guard results into the normative state-machine spec;
// store remains the sole authority for the resulting transaction and event.
package engine

import (
	"context"
	"errors"
	"fmt"
	"os"

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
	spec, err := statemachine.Load(file)
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

func (e *Engine) Start(ctx context.Context, ref domain.TicketRef, stableWorkflowID string) error {
	ticket, err := e.store.Ticket(ctx, ref)
	if err != nil {
		return err
	}
	if ticket.State != domain.StateQueued {
		return fmt.Errorf("start ticket: expected queued, got %s", ticket.State)
	}
	// The leader epoch is acquired by the daemon. It is intentionally explicit:
	// an engine cannot invent a fence after a stale daemon restart.
	return fmt.Errorf("start ticket requires daemon fence; use StartOrAdopt")
}

func (e *Engine) StartOrAdopt(ctx context.Context, ref domain.TicketRef, stableWorkflowID string, fence domain.Fence) (store.Ticket, error) {
	return e.store.StartOrAdopt(ctx, ref, stableWorkflowID, fence)
}

func (e *Engine) Transition(ctx context.Context, request contracts.TransitionRequest) (contracts.TransitionResult, error) {
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
	result, err := e.store.Transition(ctx, store.Transition{
		Ref: request.Ticket, ExpectedVersion: request.TicketVersion, From: request.From,
		To: target, ResumeState: resume, Trigger: request.Trigger, Fence: request.Fence,
		EventPayload: "{}",
	})
	if err != nil {
		return contracts.TransitionResult{}, err
	}
	return contracts.TransitionResult{To: target, TicketVersion: result.Version, EventID: fmt.Sprint(result.EventID)}, nil
}

func (e *Engine) Signal(context.Context, domain.TicketRef, string, map[string]string) error {
	return errors.New("signals require daemon authentication and are not implemented in the storage lane")
}

func (e *Engine) Recover(ctx context.Context) error {
	return errors.New("recovery requires daemon leader epoch; use RecoverChannel")
}

func (e *Engine) RecoverChannel(ctx context.Context, channel domain.Channel, leaderEpoch uint64) error {
	return e.store.ReconcileOrphans(ctx, channel, leaderEpoch)
}

var _ contracts.WorkflowEngine = (*Engine)(nil)
