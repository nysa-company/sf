package workflowworker

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type rejectionEvidence struct {
	*fakeEvidence
	decision store.VerificationAmendmentDecision
	err      error
	calls    int
	ref      domain.TicketRef
	version  uint64
	fence    domain.Fence
}

func (e *rejectionEvidence) PostbuildVerificationAmendmentContext(_ context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.PostbuildVerificationAmendmentContext, error) {
	e.calls++
	e.ref, e.version, e.fence = ref, version, fence
	return store.PostbuildVerificationAmendmentContext{Decision: e.decision}, e.err
}

// This narrow test treats Store's authenticated context as the authority;
// immutable decision lineage itself is covered by Store's integration suite.
func TestStopRejectedPostbuildAmendmentUsesOnlyTypedSignal(t *testing.T) {
	for _, mode := range []string{"valid", "accepted", "pending", "stale context", "absent context", "wrong version", "wrong runner", "wrong state", "no authority", "no engine", "late fence"} {
		t.Run(mode, func(t *testing.T) {
			evidence := &rejectionEvidence{fakeEvidence: &fakeEvidence{ticket: store.Ticket{Ref: testRef, State: domain.StateBuilding, Version: 8, RunnerEpoch: testFence.RunnerEpoch}}, decision: store.VerificationAmendmentRejected}
			engine := &fakeEngine{state: &evidence.ticket}
			// Nil provider/materializer dependencies make any full-worker or Git
			// dispatch unusable: rejection must need neither.
			worker := Worker{Evidence: evidence, Engine: engine}
			switch mode {
			case "accepted":
				evidence.decision = store.VerificationAmendmentAccepted
			case "pending":
				evidence.decision = ""
			case "stale context":
				evidence.err = store.ErrStaleFence
			case "absent context":
				evidence.err = store.ErrNotFound
			case "wrong version":
				evidence.ticket.Version++
			case "wrong runner":
				evidence.ticket.RunnerEpoch++
			case "wrong state":
				evidence.ticket.State = domain.StateVerifying
			case "no authority":
				worker.Evidence = evidence.fakeEvidence
			case "no engine":
				worker.Engine = nil
			case "late fence":
				engine.stale = true
			}
			result, err := worker.StopRejectedPostbuildAmendment(context.Background(), testRef, 8, testFence)
			if mode == "valid" {
				if err != nil || !result.Transitioned || result.State != domain.StateBlocked || engine.signals != 1 || evidence.calls != 1 || evidence.ref != testRef || evidence.version != 8 || evidence.fence != testFence {
					t.Fatalf("result=%+v err=%v engine=%+v", result, err, engine)
				}
				if engine.last.Trigger != "typed_blocker" || engine.last.EventPayload != `{"code":"postbuild_amendment_rejected"}` || engine.last.Ticket != testRef || engine.last.TicketVersion != 8 || engine.last.Fence != testFence {
					t.Fatalf("wrong blocker signal: %+v", engine.last)
				}
				return
			}
			if err == nil || result.Transitioned {
				t.Fatalf("unsafe stop=%+v err=%v", result, err)
			}
			if mode != "late fence" && engine.signals != 0 {
				t.Fatal("invalid authority reached signal")
			}
		})
	}
}
