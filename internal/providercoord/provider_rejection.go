package providercoord

import (
	"context"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/store"
)

type serverRejectionDrainer interface {
	DrainServerRejection(context.Context, contracts.DrainRequest) (contracts.DrainProof, contracts.ServerRejectionAttestation, error)
}

// Optional receipt failure preserves ordinary conservative drain. A returned
// but invalid receipt is not downgraded: its run may already have been consumed.
func drainWithServerRejection(ctx context.Context, supervisor contracts.ProcessSupervisor, claim store.ProviderAttemptClaim, eligible bool) (contracts.DrainProof, *contracts.ServerRejectionAttestation, error) {
	request := drainRequest(claim)
	if eligible && claim.Binding.Identity.Provider == "claude" && claim.Binding.AuthMode == "claude_subscription" {
		if specialized, ok := supervisor.(serverRejectionDrainer); ok {
			drain, receipt, err := specialized.DrainServerRejection(ctx, request)
			if err == nil {
				if !contracts.VerifyDrainProof(supervisor.PublicKey(), request, drain) || !contracts.VerifyServerRejection(supervisor.PublicKey(), request, receipt) {
					return contracts.DrainProof{}, nil, store.ErrProviderServerRejection
				}
				return drain, &receipt, nil
			}
		}
	}
	drain, err := supervisor.Drain(ctx, request)
	return drain, nil, err
}

func waitServerRejectionBackoff(ctx context.Context, clock Clock, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := deadline.Sub(clock.Now())
	if delay > 3*time.Second {
		return store.ErrProviderServerRejection
	}
	// Not-before can elapse between Store's check and this clock read.
	// Return to Store admission; this does not authorize a launch itself.
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Coordinator) inspectRejectionRetryBeforeLaunch(ctx context.Context, claim store.ProviderAttemptClaim) error {
	checkpoint, required, err := c.store.ProviderRejectionRetryCheckpoint(ctx, claim)
	if err != nil || !required {
		return err
	}
	c.checkpointMu.RLock()
	inspector := c.checkpoint
	c.checkpointMu.RUnlock()
	if inspector == nil {
		return errors.New("provider rejection retry checkpoint unavailable")
	}
	head, digest, err := inspector.InspectRejectionCheckpoint(ctx, drainRequest(claim))
	want, digestErr := store.ProviderAttemptCheckpointDigest(checkpoint, claim.RequestDigest)
	if err != nil || digestErr != nil || ctx.Err() != nil || head != checkpoint.ExpectedHead || digest != want {
		return store.ErrProviderServerRejection
	}
	after, required, err := c.store.ProviderRejectionRetryCheckpoint(ctx, claim)
	afterDigest, digestErr := store.ProviderAttemptCheckpointDigest(after, claim.RequestDigest)
	if err != nil || !required || digestErr != nil || afterDigest != want {
		return store.ErrProviderServerRejection
	}
	return nil
}
