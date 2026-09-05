package workflowruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type pendingBaseRefreshTickets struct {
	fakeTickets
	pending bool
	err     error
}

func (f pendingBaseRefreshTickets) PendingProtectedBaseRefresh(context.Context, domain.TicketRef, uint64, domain.Fence) (store.ProtectedBaseRefresh, bool, error) {
	return store.ProtectedBaseRefresh{}, f.pending, f.err
}

func TestSchedulerDispatchesPendingBaseRefreshWithoutPristineEnsure(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "refresh", Ticket: "SF-refresh-scheduler"}
	for _, tc := range []struct {
		name    string
		pending bool
		err     error
	}{
		{name: "pending", pending: true},
		{name: "pending error", err: errors.New("refresh proof unavailable")},
		{name: "ordinary", pending: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ensurer := &fakeEnsure{}
			worker := &fakeWorker{}
			source := pendingBaseRefreshTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePublishing)}}, pending: tc.pending, err: tc.err}
			result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
			if tc.err != nil {
				if result.Outcome == OutcomeInvoked || len(worker.calls) != 0 || len(ensurer.calls) != 0 {
					t.Fatalf("pending error dispatched work: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
				}
				return
			}
			if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 {
				t.Fatalf("result=%+v worker=%v", result, worker.calls)
			}
			if tc.pending && len(ensurer.calls) != 0 {
				t.Fatalf("pending refresh used pristine Ensure: %v", ensurer.calls)
			}
			if !tc.pending && len(ensurer.calls) != 1 {
				t.Fatalf("ordinary path Ensure calls=%v", ensurer.calls)
			}
		})
	}
}
