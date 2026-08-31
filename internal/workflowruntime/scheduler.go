// Package workflowruntime contains the deliberately small composition used by
// a future daemon.  It is not a daemon: a tick is finite, admits one already
// started ticket, and never starts a queued ticket.
package workflowruntime

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

var (
	ErrInvalidScheduler = errors.New("workflow scheduler is not configured")
	ErrCanceled         = errors.New("workflow scheduler canceled")
	ErrStale            = errors.New("workflow scheduler observed stale authority")
	ErrBusy             = errors.New("workflow scheduler observed a busy authority")
	ErrInProgress       = errors.New("workflow scheduler observed work already in progress")
	ErrReadiness        = errors.New("workflow scheduler readiness failed")
	ErrWorker           = errors.New("workflow scheduler worker failed")
)

// TicketSource is intentionally narrower than Store. Implementations return a
// snapshot; the scheduler sorts and filters it before doing any external work.
// StoreTicketSource is provided for the production Store composition.
type TicketSource interface {
	ListTickets(context.Context, domain.Channel) ([]store.Ticket, error)
	Ticket(context.Context, domain.TicketRef) (store.Ticket, error)
}

// StoreTicketSource adapts Store.Tickets without exposing SQL to the runtime.
type StoreTicketSource struct {
	Store *store.Store
	Limit int
}

func (s StoreTicketSource) ListTickets(ctx context.Context, channel domain.Channel) ([]store.Ticket, error) {
	if s.Store == nil {
		return nil, ErrInvalidScheduler
	}
	limit := s.Limit
	if limit == 0 {
		limit = 10_000
	}
	return s.Store.Tickets(ctx, channel, "", limit)
}

func (s StoreTicketSource) Ticket(ctx context.Context, ref domain.TicketRef) (store.Ticket, error) {
	if s.Store == nil {
		return store.Ticket{}, ErrInvalidScheduler
	}
	return s.Store.Ticket(ctx, ref)
}

// WorktreeEnsurer is compatible with worktreecoord.Coordinator. Keeping the
// request and result concrete prevents later adapters from dropping fence or
// identity fields while still permitting deterministic fakes in tests.
type WorktreeEnsurer interface {
	Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error)
}

type Worker interface {
	Run(context.Context, domain.TicketRef, domain.Fence) (workflowworker.RunResult, error)
}

type Outcome string

const (
	OutcomeIdle       Outcome = "idle"
	OutcomeInvoked    Outcome = "invoked"
	OutcomeCanceled   Outcome = "canceled"
	OutcomeStale      Outcome = "stale"
	OutcomeBusy       Outcome = "busy"
	OutcomeInProgress Outcome = "in_progress"
	OutcomeReadiness  Outcome = "readiness_failed"
	OutcomeWorker     Outcome = "worker_failed"
)

// TickResult intentionally carries no provider error or transcript. Err is a
// stable package sentinel wrapped with a safe phase boundary, suitable for a
// later daemon policy decision.
type TickResult struct {
	Outcome  Outcome
	Ref      domain.TicketRef
	Ticket   store.Ticket
	Fence    domain.Fence
	Worktree store.StoredWorktree
	Worker   workflowworker.RunResult
	Err      error
}

// Scheduler is a one-tick core. It invokes Worker at most once and performs no
// state transition itself. In particular, this type has no operator_start
// implementation and cannot turn queued rows into active rows.
type Scheduler struct {
	Channel   domain.Channel
	Tickets   TicketSource
	Worktrees WorktreeEnsurer
	Worker    Worker
	// AdmitPublishing records whether the publication worker is configured. A
	// false value still admits a publishing ticket exactly once so the worker
	// can durably block it with an actionable operator reason; it must not be
	// filtered into an invisible forever-waiting state.
	AdmitPublishing bool
	admission       *admission
}

func NewScheduler(channel domain.Channel, tickets TicketSource, worktrees WorktreeEnsurer, worker Worker) *Scheduler {
	return newScheduler(channel, tickets, worktrees, worker, newAdmission())
}

func newScheduler(channel domain.Channel, tickets TicketSource, worktrees WorktreeEnsurer, worker Worker, admission *admission) *Scheduler {
	return &Scheduler{Channel: channel, Tickets: tickets, Worktrees: worktrees, Worker: worker, AdmitPublishing: true, admission: admission}
}

func (s Scheduler) validate() error {
	if !s.Channel.Valid() || s.Tickets == nil || s.Worktrees == nil || s.Worker == nil || s.admission == nil {
		return ErrInvalidScheduler
	}
	return nil
}

// Tick takes one immutable leader fence snapshot and binds each ticket's
// runner epoch to its candidate fence. Stale, busy, and
// in-progress candidates are benign and allow a later candidate to be tried;
// the first admitted candidate ends the tick. There is never a second phase
// invocation in this call.
func (s Scheduler) Tick(ctx context.Context, fence domain.Fence) TickResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.validate(); err != nil {
		return TickResult{Outcome: OutcomeReadiness, Fence: fence, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return TickResult{Outcome: OutcomeCanceled, Fence: fence, Err: ErrCanceled}
	}
	if fence.LeaderEpoch == 0 {
		return TickResult{Outcome: OutcomeReadiness, Fence: fence, Err: ErrReadiness}
	}
	tickets, err := s.Tickets.ListTickets(ctx, s.Channel)
	if err != nil {
		return classify(err, fence, domain.TicketRef{})
	}
	// A source may reuse a backing slice across calls. Runtime pool loops tick
	// concurrently, so sort an owned snapshot rather than mutating the source.
	tickets = append([]store.Ticket(nil), tickets...)
	// Store's historical query order is intentionally not scheduler order.
	// Sorting all rows before filtering makes ordering stable across restarts
	// and independent of SQLite rowid allocation.
	sort.SliceStable(tickets, func(i, j int) bool {
		left, right := tickets[i].Ref, tickets[j].Ref
		if left.Project != right.Project {
			return left.Project < right.Project
		}
		return left.Ticket < right.Ticket
	})
	var lastBenign *TickResult
	for _, ticket := range tickets {
		if ticket.Ref.Channel != s.Channel || !s.activeState(ticket.State) {
			continue
		}
		// Runner epochs are ticket-scoped. Preserve the one leader snapshot,
		// then bind this candidate's own runner epoch into its fence.
		if ticket.Version == 0 || ticket.RunnerEpoch == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return TickResult{Outcome: OutcomeCanceled, Ref: ticket.Ref, Ticket: ticket, Fence: fence, Err: ErrCanceled}
		}
		candidateFence := fence
		candidateFence.RunnerEpoch = ticket.RunnerEpoch
		result := TickResult{Ref: ticket.Ref, Ticket: ticket, Fence: candidateFence}
		runCtx, end, admitted := s.admission.Begin(ctx, ticket.Ref, ticket.Version, candidateFence.LeaderEpoch, ticket.RunnerEpoch)
		if !admitted {
			lastBenign = &TickResult{Outcome: OutcomeCanceled, Ref: ticket.Ref, Ticket: ticket, Fence: candidateFence, Err: ErrCanceled}
			continue
		}
		// Register before the durable current-row check and every external
		// boundary, so control can cancel this exact activity first.
		current, currentErr := s.Tickets.Ticket(runCtx, ticket.Ref)
		if currentErr != nil {
			end()
			return classify(currentErr, candidateFence, ticket.Ref)
		}
		if current.State != ticket.State || current.Version != ticket.Version || current.RunnerEpoch != ticket.RunnerEpoch || !s.activeState(current.State) {
			end()
			lastBenign = &TickResult{Outcome: OutcomeStale, Ref: ticket.Ref, Ticket: current, Fence: candidateFence, Err: ErrStale}
			continue
		}
		if ticket.State == domain.StatePublishing && !s.AdmitPublishing {
			// The explicit pre-publishing composition must block before it asks
			// for a worktree. This keeps a missing local checkout from stranding
			// an already-publishing ticket outside the publication capability.
			workerResult, workerErr := s.Worker.Run(runCtx, ticket.Ref, candidateFence)
			end()
			result.Worker = workerResult
			if workerErr != nil {
				result.Outcome, result.Err = classifyWorker(workerErr)
				return result
			}
			result.Outcome = OutcomeInvoked
			return result
		}
		worktree, ensureErr := s.Worktrees.Ensure(runCtx, worktreecoord.EnsureRequest{Ref: ticket.Ref, Version: ticket.Version, Fence: candidateFence})
		if ensureErr != nil {
			end()
			result.Outcome, result.Err = classifyEnsure(ensureErr)
			if result.Outcome == OutcomeStale || result.Outcome == OutcomeBusy || result.Outcome == OutcomeInProgress {
				// Keep the most recent benign outcome so a one-candidate tick
				// reports why it did no work, while still allowing a later
				// candidate to be admitted in this same tick.
				lastBenign = &result
				continue
			}
			return result
		}
		result.Worktree = worktree
		workerResult, workerErr := s.Worker.Run(runCtx, ticket.Ref, candidateFence)
		end()
		result.Worker = workerResult
		if workerErr != nil {
			result.Outcome, result.Err = classifyWorker(workerErr)
			return result
		}
		result.Outcome = OutcomeInvoked
		return result
	}
	if lastBenign != nil {
		return *lastBenign
	}
	return TickResult{Outcome: OutcomeIdle, Fence: fence}
}

func (s Scheduler) activeState(state domain.State) bool {
	// Publishing is an active, single-shot boundary: publication.Worker owns
	// its durable effect replay and the publishing -> waiting_ci transition,
	// while a pre-publishing runtime uses the same admission to durably block.
	// waiting_ci is intentionally absent. It is a passive observation state and
	// must never cause a scheduler retry or external command.
	return state == domain.StatePlanning || state == domain.StateVerifying || state == domain.StateBuilding || state == domain.StatePublishing
}

func classify(err error, fence domain.Fence, ref domain.TicketRef) TickResult {
	result := TickResult{Ref: ref, Fence: fence, Err: ErrReadiness}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		result.Outcome, result.Err = OutcomeCanceled, ErrCanceled
	} else if errors.Is(err, store.ErrBusy) {
		result.Outcome, result.Err = OutcomeBusy, ErrBusy
	}
	return result
}

func classifyEnsure(err error) (Outcome, error) {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return OutcomeCanceled, ErrCanceled
	case errors.Is(err, store.ErrStaleFence), errors.Is(err, worktreecoord.ErrAuthentication):
		return OutcomeStale, ErrStale
	case errors.Is(err, store.ErrBusy):
		return OutcomeBusy, ErrBusy
	case errors.Is(err, worktreecoord.ErrInProgress):
		return OutcomeInProgress, ErrInProgress
	default:
		return OutcomeReadiness, ErrReadiness
	}
}

func classifyWorker(err error) (Outcome, error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, workflowworker.ErrCanceled) {
		return OutcomeCanceled, ErrCanceled
	}
	if errors.Is(err, store.ErrStaleFence) {
		return OutcomeStale, ErrStale
	}
	return OutcomeWorker, ErrWorker
}

// Compile-time documentation that the concrete coordinator remains usable by
// this package. No daemon wiring is performed here.
var _ WorktreeEnsurer = worktreecoord.Coordinator{}

func (r TickResult) String() string {
	if r.Err == nil {
		return string(r.Outcome)
	}
	return fmt.Sprintf("%s: %v", r.Outcome, r.Err)
}
