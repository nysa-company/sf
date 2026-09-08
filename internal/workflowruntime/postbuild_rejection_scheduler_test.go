package workflowruntime

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type rejectionSchedulerTickets struct {
	fakeTickets
	decision store.VerificationAmendmentDecision
	err      error
}

func (s *rejectionSchedulerTickets) PostbuildVerificationAmendmentContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildVerificationAmendmentContext, error) {
	return store.PostbuildVerificationAmendmentContext{Decision: s.decision}, s.err
}

type rejectionSchedulerWorker struct {
	fakeWorker
	stops   []worktreecoord.EnsureRequest
	stopErr error
}

func (w *rejectionSchedulerWorker) StopRejectedPostbuildAmendment(_ context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (workflowworker.RunResult, error) {
	w.stops = append(w.stops, worktreecoord.EnsureRequest{Ref: ref, Version: version, Fence: fence})
	return workflowworker.RunResult{Ref: ref, State: domain.StateBlocked, Version: version + 1, Transitioned: w.stopErr == nil}, w.stopErr
}

func TestSchedulerReplaysRejectedAmendmentWithoutWorktreeOrFullWorker(t *testing.T) {
	for _, mode := range []string{"decision before block", "missing stopper", "stale decision", "corrupt decision", "stop refusal", "accepted"} {
		t.Run(mode, func(t *testing.T) {
			building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-rejection"}, domain.StateBuilding)
			source := &rejectionSchedulerTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, decision: store.VerificationAmendmentRejected}
			physical := &fakeEnsure{}
			stopper := &rejectionSchedulerWorker{}
			var worker Worker = stopper
			switch mode {
			case "missing stopper":
				worker = &stopper.fakeWorker
			case "stale decision":
				source.err = store.ErrStaleFence
			case "corrupt decision":
				source.err = store.ErrEvidenceConflict
			case "stop refusal":
				stopper.stopErr = store.ErrStaleFence
			case "accepted":
				source.decision = store.VerificationAmendmentAccepted
			}
			result := NewScheduler(domain.ChannelDev, source, physical, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
			if len(physical.calls) != 0 || len(stopper.calls) != 0 {
				t.Fatalf("rejection dispatched broad work: ensure=%v worker=%v", physical.calls, stopper.calls)
			}
			if mode == "decision before block" || mode == "stop refusal" {
				want := worktreecoord.EnsureRequest{Ref: building.Ref, Version: building.Version, Fence: domain.Fence{LeaderEpoch: 9, RunnerEpoch: building.RunnerEpoch}}
				if len(stopper.stops) != 1 || stopper.stops[0] != want {
					t.Fatalf("stop calls=%+v", stopper.stops)
				}
				if mode == "decision before block" && (result.Outcome != OutcomeInvoked || result.Worker.State != domain.StateBlocked) {
					t.Fatalf("decision-before-block replay=%+v", result)
				}
			} else if len(stopper.stops) != 0 {
				t.Fatal("non-rejection/missing authority dispatched stop")
			}
			if mode != "decision before block" && result.Outcome == OutcomeInvoked {
				t.Fatalf("invalid authority invoked: %+v", result)
			}
		})
	}
}
