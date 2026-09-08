package workflowruntime

import (
	"context"
	"reflect"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// PreparePostbuildRepair observes retained bytes; it does not run a command,
// reset files, or launch a provider. The Store transition consumes its exact
// failure identity and the next admission rechecks this physical fingerprint.
func (m RepositoryMaterializer) PreparePostbuildRepair(ctx context.Context, request workflowworker.PhaseRequest, builder store.ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey) (store.PostbuildRepairRequest, error) {
	if m.Store == nil {
		return store.PostbuildRepairRequest{}, ErrRepositoryMaterialization
	}
	failureRequest := store.PostbuildFailureRequest{Ref: request.Ticket.Ref, ExpectedVersion: request.Ticket.Version, Fence: request.Fence, BuilderResult: builder, CommandResult: command}
	failure, err := m.Store.AuthenticatePostbuildFailure(ctx, failureRequest)
	if err != nil {
		return store.PostbuildRepairRequest{}, err
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, builder)
	if err != nil || parsed.Builder == nil || provider.TypedSHA256 != failure.BuilderTypedDigest || !providerMatches(request, provider, builder, domain.PhaseBuild, "builder") || request.Plan == nil || request.Plan.Document.Planner == nil {
		return store.PostbuildRepairRequest{}, ErrRepositoryMaterialization
	}
	plan, err := m.Store.Plan(ctx, request.Ticket.Ref)
	if err != nil || !reflect.DeepEqual(plan, *request.Plan) {
		return store.PostbuildRepairRequest{}, ErrRepositoryMaterialization
	}
	worktree, err := m.worktree(request)
	if err != nil {
		return store.PostbuildRepairRequest{}, err
	}
	inspection, err := m.Git.InspectRetainedWorktree(ctx, worktree)
	if err != nil || inspection.Changes.Head != failure.Verification.Checkpoint.CommitOID {
		return store.PostbuildRepairRequest{}, ErrRepositoryMaterialization
	}
	for _, path := range inspection.Changes.Paths {
		declared := false
		for _, item := range parsed.Builder.ChangedFiles {
			declared = declared || item == path
		}
		if !declared || !containsPath(plan.Document.Planner.Paths, path) || containsPath(failure.Verification.Revision.OwnedFiles, path) {
			return store.PostbuildRepairRequest{}, ErrRepositoryMaterialization
		}
	}
	checked, err := m.Store.AuthenticatePostbuildFailure(ctx, failureRequest)
	if err != nil || !reflect.DeepEqual(checked, failure) {
		return store.PostbuildRepairRequest{}, ErrRepositoryMaterialization
	}
	return store.PostbuildRepairRequest{PostbuildFailureRequest: failureRequest, RetainedWorktreeDigest: inspection.Digest}, nil
}
