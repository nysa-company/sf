package workflowruntime

import (
	"context"

	"github.com/nysa-company/sf/internal/store"
)

// ApplyOperatorDecision consumes an exact pending admission as a runtime
// activity before executing the Store-owned decision. Waiting states do not
// necessarily receive a scheduler tick, so installing a rearm token alone is
// insufficient. The activity excludes a duplicate tick until the decision ends.
// admitted distinguishes admission refusal from Store's exact-head refusal.
func (b *ControlBundle) ApplyOperatorDecision(ctx context.Context, database *store.Store, request store.OperatorDecisionRequest) (result store.TransitionResult, admitted bool, err error) {
	if !b.Valid() || database == nil {
		return result, false, ErrRuntimeRearm
	}
	// Only the production Store-backed source can authorize this operation;
	// a caller cannot pair another database with this runtime's capability.
	var bound *store.Store
	switch source := b.runtime.Scheduler.Tickets.(type) {
	case StoreTicketSource:
		bound = source.Store
	case *StoreTicketSource:
		if source != nil {
			bound = source.Store
		}
	}
	if bound != database {
		return result, false, ErrRuntimeRearm
	}
	decision := request.OperatorDecision
	if decision.Ref.Validate() != nil || decision.ExpectedVersion == 0 || decision.Fence.LeaderEpoch == 0 || decision.Fence.RunnerEpoch == 0 || decision.Fence.ClaimEpoch != 0 {
		return result, false, ErrRuntimeRearm
	}
	version, fence, err := database.CurrentTicketFence(ctx, decision.Ref)
	if err != nil || version != decision.ExpectedVersion || fence != decision.Fence {
		return result, false, ErrRuntimeRearm
	}
	run, end, admitted := b.runtime.Scheduler.admission.BeginDecision(ctx, decision.Ref, decision.ExpectedVersion, decision.Fence.LeaderEpoch, decision.Fence.RunnerEpoch)
	if !admitted {
		return result, false, ErrRuntimeRearm
	}
	defer end()
	version, fence, err = database.CurrentTicketFence(run, decision.Ref)
	if err != nil || version != decision.ExpectedVersion || fence != decision.Fence {
		return result, false, ErrRuntimeRearm
	}
	ready, err := database.RuntimeAdmissionReady(run, decision.Ref, decision.ExpectedVersion, decision.Fence)
	if err != nil || !ready {
		return result, false, ErrRuntimeRearm
	}
	result, err = database.ApplyOperatorDecision(run, request)
	return result, true, err
}
