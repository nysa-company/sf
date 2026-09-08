package workflowruntime

import (
	"context"
	"reflect"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// An already committed post-amendment candidate is observation-only replay.
// In particular its historical command must not be reissued at a new fence.
func (m RepositoryMaterializer) replayPostbuildAmendmentPreparedCandidate(ctx context.Context, request workflowworker.PhaseRequest, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, key store.ProviderAttemptResultKey) (workflowworker.CandidateWitness, bool, error) {
	var empty workflowworker.CandidateWitness
	// Store selects the exact current accepted-amendment entry. Historical
	// companions cannot shadow a separately authenticated later lifecycle.
	recovered, found, err := m.Store.PostbuildAmendmentPreparedCandidateWitness(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence, key)
	if err != nil || !found {
		return empty, found, err
	}
	if recovered.Ref != request.Ticket.Ref || recovered.Version != request.Ticket.Version || recovered.Fence != request.Fence || recovered.Builder != key || (recovered.EffectState != store.EffectConfirmed && recovered.EffectState != store.EffectExecuting && recovered.EffectState != store.EffectUncertain) || !sameJSON(recovered.BuilderArtifact, builder) || !reflect.DeepEqual(recovered.Worktree, request.Worktree) || recovered.Commit.ParentOID != verification.CheckpointID || recovered.Verification.Revision.IntentDigest != verification.IntentDigest || recovered.Verification.Revision.ProofDigest != verification.ProofDigest || recovered.Verification.Revision.CheckpointID != verification.CheckpointID || !reflect.DeepEqual(recovered.Verification.Revision.OwnedFiles, verification.OwnedFiles) || recovered.Plan.Document.Planner == nil {
		return empty, true, ErrRepositoryMaterialization
	}
	identity, err := workflowprompt.NewPlanIdentity(*recovered.Plan.Document.Planner)
	if err != nil || !reflect.DeepEqual(identity, plan) {
		return empty, true, ErrRepositoryMaterialization
	}
	command, err := m.Store.LoadRepositoryCommandResult(ctx, recovered.Command.Key)
	if err != nil || !reflect.DeepEqual(command, recovered.Command) || command.Result.ExitCode != 0 || !strings.HasPrefix(command.Claim.PolicyDigest, "sha256:") {
		return empty, true, ErrRepositoryMaterialization
	}
	worktree, err := m.worktree(request)
	if err != nil {
		return empty, true, err
	}
	head, err := m.Git.StrictCleanWorktreeHead(ctx, worktree)
	if err != nil || head != recovered.Commit.CommitOID {
		return empty, true, ErrRepositoryMaterialization
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != recovered.Commit {
		return empty, true, ErrRepositoryMaterialization
	}
	if recovered.EffectState != store.EffectConfirmed {
		if _, err := m.Store.ConfirmPreparedCommit(ctx, recovered.Claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); err != nil {
			return empty, true, err
		}
		recovered.EffectState = store.EffectConfirmed
	}
	checked, found, err := m.Store.PostbuildAmendmentPreparedCandidateWitness(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence, key)
	if err != nil || !found || !reflect.DeepEqual(checked, recovered) {
		return empty, true, ErrRepositoryMaterialization
	}
	if head, err := m.Git.StrictCleanWorktreeHead(ctx, worktree); err != nil || head != recovered.Commit.CommitOID {
		return empty, true, ErrRepositoryMaterialization
	}
	return workflowworker.CandidateWitness{Commit: recovered.Commit, CommandPolicyDigest: strings.TrimPrefix(command.Claim.PolicyDigest, "sha256:"), Reason: "recovered authenticated postbuild amendment candidate", CommandResult: command.Key}, true, nil
}
