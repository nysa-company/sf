package localruntime

import (
	"context"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func (w Worker) ResumePreparedPostbuildAmendment(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Store == nil {
		return workflowworker.RunResult{Ref: ref}, workflowworker.ErrUnsupportedState
	}
	ready, err := w.Store.RuntimeAdmissionReady(ctx, ref, version, fence)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	if !ready {
		return workflowworker.RunResult{Ref: ref}, store.ErrControlNotDrained
	}
	if err := ctx.Err(); err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	return w.Workflow.ResumePreparedPostbuildAmendment(ctx, ref, version, fence)
}
