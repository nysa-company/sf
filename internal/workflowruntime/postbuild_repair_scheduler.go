package workflowruntime

import (
	"context"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type postbuildRepairContextSource interface {
	PostbuildRepairContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildRepairBuildContext, error)
}

type postbuildRepairAuthenticator interface {
	AuthenticatePostbuildRepair(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error)
}

type completedPostbuildRepairSource interface {
	PostbuildRepairCompletedBuildContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildRepairBuildContext, error)
}

type completedPostbuildRepairAuthenticator interface {
	AuthenticateCompletedPostbuildRepair(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error)
}

type pendingPostbuildAmendmentSource interface {
	PostbuildRepairPendingAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (store.ProviderAttemptResultKey, error)
}

type pendingPostbuildAmendmentDispatcher interface {
	DispatchPostbuildRepairAmendment(context.Context, domain.TicketRef, uint64, domain.Fence, store.ProviderAttemptResultKey) (workflowworker.RunResult, error)
}

type postbuildVerificationContextSource interface {
	PostbuildVerificationAmendmentContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildVerificationAmendmentContext, error)
}

type postbuildVerificationAuthenticator interface {
	AuthenticatePostbuildVerificationAmendment(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error)
}

type rejectedPostbuildAmendmentStopper interface {
	StopRejectedPostbuildAmendment(context.Context, domain.TicketRef, uint64, domain.Fence) (workflowworker.RunResult, error)
}

func (s StoreTicketSource) PostbuildVerificationAmendmentContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.PostbuildVerificationAmendmentContext, error) {
	if s.Store == nil {
		return store.PostbuildVerificationAmendmentContext{}, ErrInvalidScheduler
	}
	return s.Store.PostbuildVerificationAmendmentContext(ctx, ref, version, fence)
}

func (s StoreTicketSource) PostbuildRepairPendingAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.ProviderAttemptResultKey, error) {
	if s.Store == nil {
		return store.ProviderAttemptResultKey{}, ErrInvalidScheduler
	}
	return s.Store.PostbuildRepairPendingAmendment(ctx, ref, version, fence)
}

func (s StoreTicketSource) PostbuildRepairCompletedBuildContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.PostbuildRepairBuildContext, error) {
	if s.Store == nil {
		return store.PostbuildRepairBuildContext{}, ErrInvalidScheduler
	}
	return s.Store.PostbuildRepairCompletedBuildContext(ctx, ref, version, fence)
}

func (s StoreTicketSource) PostbuildRepairContext(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.PostbuildRepairBuildContext, error) {
	if s.Store == nil {
		return store.PostbuildRepairBuildContext{}, ErrInvalidScheduler
	}
	return s.Store.PostbuildRepairContext(ctx, ref, version, fence)
}
