package store

import (
	"context"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// RecoverMergeIntent is the restart-reconciliation hook: it loads the
// structured witness, lets an injected observer prove the result, then
// confirms the original durable effect using its recorded fence.
func (s *Store) RecoverMergeIntent(ctx context.Context, semanticKey string, observer contracts.MergeIntentObserver) (Effect, error) {
	intent, found, err := s.MergeIntent(ctx, semanticKey)
	if err != nil || !found || observer == nil {
		return Effect{}, ErrNotFound
	}
	identity, err := observer.ObserveMergeIntent(ctx, intent)
	if err != nil {
		return Effect{}, err
	}
	return s.ConfirmEffect(ctx, EffectFence{Ref: intent.Ref, SemanticKey: intent.SemanticKey, TicketVersion: intent.TicketVersion, Fence: domain.Fence{LeaderEpoch: intent.LeaderEpoch, RunnerEpoch: intent.RunnerEpoch, ClaimEpoch: intent.ClaimEpoch}}, identity)
}
