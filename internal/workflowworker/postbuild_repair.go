package workflowworker

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/store"
)

var errPostbuildRepairStarted = errors.New("bounded postbuild diagnosis entered")

type postbuildRepairPreparer interface {
	PreparePostbuildRepair(context.Context, PhaseRequest, store.ProviderAttemptResultKey, contracts.RepositoryCommandResultKey) (store.PostbuildRepairRequest, error)
}

type postbuildRepairEngine interface {
	SignalPostbuildRepair(context.Context, store.PostbuildRepairRequest) (contracts.TransitionResult, error)
}

func (w Worker) tryPostbuildRepair(ctx context.Context, request PhaseRequest, key store.ProviderAttemptResultKey, original error) error {
	var failure *PostbuildFailure
	if !errors.As(original, &failure) {
		return original
	}
	preparer, physicalOK := w.CandidateMaterializer.(postbuildRepairPreparer)
	engine, transitionOK := w.Engine.(postbuildRepairEngine)
	if !physicalOK || !transitionOK {
		return original
	}
	repair, err := preparer.PreparePostbuildRepair(ctx, request, key, failure.CommandResult)
	if err != nil {
		return original
	}
	if _, err := engine.SignalPostbuildRepair(ctx, repair); err != nil {
		return original
	}
	return errPostbuildRepairStarted
}
