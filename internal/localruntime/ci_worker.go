package localruntime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

var ErrCIUnavailable = errors.New("CI observation capability is unavailable")

// CIObserver is deliberately read-only.  Store owns the authoritative
// candidate binding, policy witness, observation, budget use, and transition;
// this boundary merely reads GitHub's current required-check facts.
type CIObserver interface {
	contracts.CIRequiredCheckPolicyObserver
	contracts.CIRequiredChecksObserver
}

// CIWorker performs one bounded, durable waiting_ci observation.  It never
// waits for GitHub, creates a worktree, starts a provider, or retries an
// external action.  A later scheduler tick is the only poll retry.
type CIWorker struct {
	Store    *store.Store
	Observer CIObserver
	Now      func() time.Time
}

func (w CIWorker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Store == nil || w.Observer == nil {
		return workflowworker.RunResult{Ref: ref}, ErrCIUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticket, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	result := workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}
	if ticket.State != domain.StateWaitingCI {
		return result, nil
	}
	if fence.LeaderEpoch == 0 || fence.RunnerEpoch != ticket.RunnerEpoch {
		return result, store.ErrStaleFence
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	admission, err := w.Store.AdmitCIPoll(ctx, ref, fence, now().UTC())
	if err != nil {
		return result, err
	}
	if !admission.Due {
		current, ticketErr := w.Store.Ticket(ctx, ref)
		if ticketErr != nil {
			return result, ticketErr
		}
		return workflowworker.RunResult{Ref: ref, State: current.State, Version: current.Version}, nil
	}

	// The required set is server-defined. Persist it before accepting current
	// states so the Store can reject a changed, omitted, or injected set.
	if err := w.Store.RecordCIRequiredCheckPolicyFromObserver(ctx, ref, w.Observer); err != nil {
		return result, err
	}
	if err := w.Store.RecordCIObservationFromObserver(ctx, ref, ticket.Version, fence, w.Observer); err != nil {
		return result, err
	}
	stored, err := w.Store.LoadCIObservation(ctx, ref)
	if err != nil {
		return result, err
	}
	transition := store.CIObservationTransition{Ref: ref, ObservationDigest: stored.ObservationDigest, ExpectedVersion: ticket.Version, Fence: fence}
	if stored.Classification == "red" {
		requestID := "ci-red/" + strings.TrimPrefix(stored.ObservationDigest, "sha256:")
		// The Store allocates this request atomically with checks_red, repair
		// binding, and the ticket advance. No budget mutation occurs here.
		transition.CorrectionBudget = &store.CorrectionBudgetAuthority{Ref: ref, RequestID: requestID, TicketVersion: ticket.Version, Fence: fence}
	}
	transitioned, err := w.Store.ConsumeAuthenticatedCIObservation(ctx, transition)
	if err != nil {
		return result, err
	}
	current, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return result, err
	}
	return workflowworker.RunResult{Ref: ref, State: current.State, Version: transitioned.Version, Transitioned: transitioned.Version == ticket.Version+1}, nil
}
