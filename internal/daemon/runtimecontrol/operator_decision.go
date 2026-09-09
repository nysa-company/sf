package runtimecontrol

import (
	"context"

	"github.com/nysa-company/sf/internal/store"
)

// ApplyOperatorDecision joins a recovered ticket before installing its typed
// Store admission. It does not manufacture an open control row or replace the
// original durable stop. The final exact runtime Begin opens admission, then
// the unchanged Store decision transaction advances that open authority.
func (c *Controller) ApplyOperatorDecision(ctx context.Context, request store.OperatorDecisionRequest) (result store.TransitionResult, admitted bool, err error) {
	if c == nil || c.store == nil || c.runtime == nil {
		return result, false, store.ErrStaleFence
	}
	ref := request.OperatorDecision.Ref
	if ref.Validate() != nil {
		return result, false, store.ErrStaleFence
	}
	entry := c.acquireTicket(ref)
	defer c.releaseTicket(ref, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	version, fence, err := c.store.CurrentTicketFence(ctx, ref)
	if err != nil || version != request.OperatorDecision.ExpectedVersion || fence != request.OperatorDecision.Fence {
		return result, false, store.ErrStaleFence
	}
	needed, err := c.store.RuntimeRearmNeeded(ctx, ref)
	if err != nil {
		return result, false, err
	}
	if needed {
		// Recovered rejection needs its own post-publication Building lineage;
		// do not consume admission then create an unsupported successor.
		if request.OperatorDecision.Decision != "approved" {
			return result, false, store.ErrEvidenceConflict
		}
		stopped, err := c.store.StoppedRuntimeTicket(ctx, ref)
		if err != nil {
			return result, false, err
		}
		// Never call Controller.Drain here: it reseals Store and would erase
		// the historical retry/control identity needed by the exact proof.
		if err := c.runtime.Drain(ctx, ref); err != nil {
			return result, false, err
		}
		entry.stopped, entry.hasStop = stopped, true
		capability, err := c.store.PostPublicationRearmProof(ctx, ref, stopped)
		if err != nil {
			return result, false, err
		}
		if err := c.store.ActivateRearm(ctx, capability, c.runtime.ApplyRearm); err != nil {
			return result, false, err
		}
	}
	return c.runtime.ApplyOperatorDecision(ctx, c.store, request)
}
