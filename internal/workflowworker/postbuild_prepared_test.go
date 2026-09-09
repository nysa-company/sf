package workflowworker

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type preparedResumeEvidence struct {
	*rejectionEvidence
	receipt    store.PostbuildAmendmentCheckpointSnapshot
	prepared   store.CommitObservation
	found      bool
	receiptErr error
}

func (e *preparedResumeEvidence) PostbuildAmendmentCheckpointSnapshot(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildAmendmentCheckpointSnapshot, error) {
	return e.receipt, e.receiptErr
}
func (e *preparedResumeEvidence) PostbuildAmendmentPreparedCheckpoint(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CommitObservation, bool, error) {
	return e.prepared, e.found, nil
}

func TestResumePreparedPostbuildRefusesIncompleteAuthorityWithoutProvider(t *testing.T) {
	for _, mode := range []string{"no authority", "no engine", "canceled", "wrong state", "wrong version", "wrong runner", "missing receipt", "mismatched receipt", "missing prepared", "wrong parent"} {
		t.Run(mode, func(t *testing.T) {
			evidence := &preparedResumeEvidence{rejectionEvidence: &rejectionEvidence{fakeEvidence: &fakeEvidence{ticket: store.Ticket{Ref: testRef, State: domain.StateVerifying, Version: 8, RunnerEpoch: testFence.RunnerEpoch}}}}
			engine := &fakeEngine{state: &evidence.ticket}
			worker := Worker{Evidence: evidence, Engine: engine}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "no authority":
				worker.Evidence = evidence.fakeEvidence
			case "no engine":
				worker.Engine = nil
			case "canceled":
				cancel()
			case "wrong state":
				evidence.ticket.State = domain.StateBuilding
			case "wrong version":
				evidence.ticket.Version++
			case "wrong runner":
				evidence.ticket.RunnerEpoch++
			case "missing receipt":
				evidence.receiptErr = store.ErrNotFound
			case "mismatched receipt":
				evidence.receipt.CompanionBindingDigest = "foreign"
			case "wrong parent":
				evidence.found = true
				evidence.prepared.ParentOID = "foreign"
			}
			result, err := worker.ResumePreparedPostbuildAmendment(ctx, testRef, 8, testFence)
			if err == nil || result.Transitioned || engine.signals != 0 {
				t.Fatalf("unsafe resume=%+v err=%v signals=%d", result, err, engine.signals)
			}
		})
	}
}
