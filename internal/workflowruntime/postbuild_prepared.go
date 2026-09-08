package workflowruntime

import (
	"context"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

func (s StoreTicketSource) PostbuildAmendmentPreparedCheckpoint(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.CommitObservation, bool, error) {
	if s.Store == nil {
		return store.CommitObservation{}, false, ErrInvalidScheduler
	}
	return s.Store.PostbuildAmendmentPreparedCheckpoint(ctx, ref, version, fence)
}

type preparedPostbuildSource interface {
	PostbuildAmendmentPreparedCheckpoint(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CommitObservation, bool, error)
}

var _ preparedPostbuildSource = StoreTicketSource{}

type preparedPostbuildAuthenticator interface {
	AuthenticatePreparedPostbuildAmendment(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error)
}
type preparedPostbuildResumer interface {
	ResumePreparedPostbuildAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (workflowworker.RunResult, error)
}

// A found prepared operation must never fall back to provider-capable Run,
// even if HEAD is still the parent or a required optional adapter is absent.
func (s *Scheduler) resumePreparedPostbuild(ctx context.Context, ticket store.Ticket, fence domain.Fence) (TickResult, bool) {
	result := TickResult{Outcome: OutcomeReadiness, Ref: ticket.Ref, Ticket: ticket, Fence: fence, Err: ErrReadiness}
	source, ok := s.Tickets.(preparedPostbuildSource)
	if !ok {
		return TickResult{}, false
	}
	_, found, err := source.PostbuildAmendmentPreparedCheckpoint(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		result.Err = err
		return result, true
	}
	if !found {
		return TickResult{}, false
	}
	authenticator, authOK := s.Worktrees.(preparedPostbuildAuthenticator)
	resumer, resumeOK := s.Worker.(preparedPostbuildResumer)
	if !authOK || !resumeOK {
		return result, true
	}
	result.Worktree, err = authenticator.AuthenticatePreparedPostbuildAmendment(ctx, worktreecoord.EnsureRequest{Ref: ticket.Ref, Version: ticket.Version, Fence: fence})
	if err != nil {
		result.Outcome, result.Err = classifyEnsure(err)
		return result, true
	}
	if err := ctx.Err(); err != nil {
		result.Outcome, result.Err = OutcomeCanceled, ErrCanceled
		return result, true
	}
	result.Worker, err = resumer.ResumePreparedPostbuildAmendment(ctx, ticket.Ref, ticket.Version, fence)
	if err != nil {
		result.Outcome, result.Err = classifyWorker(err)
		return result, true
	}
	result.Outcome, result.Err = OutcomeInvoked, nil
	return result, true
}
