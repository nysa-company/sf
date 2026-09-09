package workflowruntime

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

var _ preparedPostbuildSource = StoreTicketSource{}

type preparedSchedulerSource struct {
	rejectionSchedulerTickets
	found       bool
	preparedErr error
}

func (s *preparedSchedulerSource) PostbuildAmendmentPreparedCheckpoint(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CommitObservation, bool, error) {
	return store.CommitObservation{}, s.found, s.preparedErr
}

type preparedSchedulerWorktrees struct {
	fakeEnsure
	admissions int
	authErr    error
	cancel     context.CancelFunc
}

func (w *preparedSchedulerWorktrees) AuthenticatePreparedPostbuildAmendment(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	w.admissions++
	if w.cancel != nil {
		w.cancel()
	}
	return store.StoredWorktree{Path: "/prepared"}, w.authErr
}

type preparedSchedulerWorker struct {
	fakeWorker
	resumes   []worktreecoord.EnsureRequest
	resumeErr error
}

func (w *preparedSchedulerWorker) ResumePreparedPostbuildAmendment(_ context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (workflowworker.RunResult, error) {
	w.resumes = append(w.resumes, worktreecoord.EnsureRequest{Ref: ref, Version: version, Fence: fence})
	return workflowworker.RunResult{Ref: ref, State: domain.StateBuilding}, w.resumeErr
}

func TestSchedulerPreparedPostbuildCannotFallBackToProviderCapableWorker(t *testing.T) {
	for _, mode := range []string{"valid", "missing authenticator", "missing resumer", "corrupt prepared", "physical refusal", "canceled", "resume refusal"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			verifying := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-prepared"}, domain.StateVerifying)
			source := &preparedSchedulerSource{rejectionSchedulerTickets: rejectionSchedulerTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{verifying}}}, found: true}
			physical, resumer := &preparedSchedulerWorktrees{}, &preparedSchedulerWorker{}
			var worktrees WorktreeEnsurer = physical
			var worker Worker = resumer
			switch mode {
			case "missing authenticator":
				worktrees = &physical.fakeEnsure
			case "missing resumer":
				worker = &resumer.fakeWorker
			case "corrupt prepared":
				source.preparedErr = store.ErrEvidenceConflict
			case "physical refusal":
				physical.authErr = worktreecoord.ErrUnready
			case "canceled":
				physical.cancel = cancel
			case "resume refusal":
				resumer.resumeErr = store.ErrStaleFence
			}
			result := NewScheduler(domain.ChannelDev, source, worktrees, worker).Tick(ctx, domain.Fence{LeaderEpoch: 9})
			if len(physical.calls) != 0 || len(resumer.calls) != 0 {
				t.Fatal("prepared operation fell back to general Ensure/Worker")
			}
			if mode == "valid" || mode == "resume refusal" {
				want := worktreecoord.EnsureRequest{Ref: verifying.Ref, Version: verifying.Version, Fence: domain.Fence{LeaderEpoch: 9, RunnerEpoch: verifying.RunnerEpoch}}
				if len(resumer.resumes) != 1 || resumer.resumes[0] != want {
					t.Fatalf("resumes=%+v", resumer.resumes)
				}
			} else if len(resumer.resumes) != 0 {
				t.Fatal("unready prepared operation dispatched")
			}
			if (result.Outcome == OutcomeInvoked) != (mode == "valid") {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}
