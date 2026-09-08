package workflowruntime

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func (m RepositoryMaterializer) materializePostbuildAmendmentCheckpoint(ctx context.Context, request workflowworker.PhaseRequest, reviewer store.ProviderAttemptResultKey, artifact phaseartifact.Verification) (workflowworker.VerificationCheckpoint, bool, error) {
	var empty workflowworker.VerificationCheckpoint
	if request.Amendment == nil {
		return empty, false, nil
	}
	value, err := m.Store.PostbuildVerificationAmendmentContext(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if errors.Is(err, store.ErrNotFound) {
		return empty, false, nil
	}
	if err != nil {
		return empty, true, err
	}
	if request.Ticket.State != domain.StateVerifying || !reflect.DeepEqual(value.Amendment, *request.Amendment) || request.Worktree.Path != value.Worktree.Path || request.Worktree.Branch != value.Worktree.Branch || request.Worktree.BaseSHA != value.Worktree.BaseSHA || string(request.Worktree.IdentityJSON) != string(value.Worktree.IdentityJSON) {
		return empty, true, ErrRepositoryMaterialization
	}
	decision, err := m.Store.VerificationAmendmentDecision(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence, reviewer)
	if err != nil || decision != store.VerificationAmendmentAccepted {
		return empty, true, ErrRepositoryMaterialization
	}
	worktree, err := m.worktree(request)
	if err != nil {
		return empty, true, err
	}
	parent, protected := value.Verification.Checkpoint.CommitOID, value.Verification.Revision.OwnedFiles
	implementation, err := m.Git.InspectRetainedImplementation(ctx, worktree, parent, protected)
	if err != nil || implementation != value.Snapshot.ImplementationDigest {
		return empty, true, ErrRepositoryMaterialization
	}
	receipt, receiptErr := m.Store.PostbuildAmendmentCheckpointSnapshot(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if receiptErr != nil && !errors.Is(receiptErr, store.ErrNotFound) {
		return empty, true, receiptErr
	}
	if receiptErr == nil {
		if receipt.Reviewer != reviewer || receipt.ImplementationDigest != implementation || receipt.CompanionBindingDigest != value.Binding.BindingDigest {
			return empty, true, ErrRepositoryMaterialization
		}
		prepared, found, err := m.Store.PostbuildAmendmentPreparedCheckpoint(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
		if err != nil {
			return empty, true, err
		}
		if found {
			synced := m.proveProtectedCheckpointSynced(ctx, worktree, parent, protected, implementation, prepared) == nil
			checked, err := m.Store.PostbuildAmendmentCheckpointSnapshot(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
			if err != nil || !reflect.DeepEqual(checked, receipt) {
				return empty, true, ErrRepositoryMaterialization
			}
			command, err := m.Store.LoadRepositoryCommandResult(ctx, receipt.Command)
			if err != nil {
				return empty, true, err
			}
			evidence := verificationAmendmentCheckpointCommitDigest(request, reviewer, receipt.Command, command.ResultDigest, artifact)
			stable := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, Fence: request.Fence}, RequestDigest: evidence, Repository: worktree.Identity.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: worktree.Identity.BaseRef, ExpectedBaseOID: worktree.Identity.BaseHead, ExpectedHeadOID: parent}
			facts, err := m.Store.GitMutationIntentFacts(ctx, store.CanonicalGitMutationSemanticKey(stable))
			if err != nil || facts.PreparedCommitOID != prepared.CommitOID || facts.PreparedTreeOID != prepared.TreeOID || facts.Claim.RequestDigest != evidence {
				return empty, true, ErrRepositoryMaterialization
			}
			claim := facts.Claim
			if !synced {
				// A prepared object is not proof that the branch CAS or protected
				// index sync finished. Admit only its exact parent/full snapshot or
				// exact child; Git revalidates the deterministic tree under its lease.
				physical, err := m.Git.InspectRetainedWorktree(ctx, worktree)
				if err != nil {
					return empty, true, err
				}
				if physical.Changes.Head == parent {
					if physical.Digest != receipt.FullSnapshotDigest {
						return empty, true, ErrRepositoryMaterialization
					}
				} else if observed, err := m.Git.ObserveCommit(ctx, worktree); err != nil || observed.CommitOID != prepared.CommitOID || observed.ParentOID != prepared.ParentOID || observed.TreeOID != prepared.TreeOID {
					return empty, true, ErrRepositoryMaterialization
				}
				claim, err = m.Store.ReclaimPostbuildAmendmentCheckpoint(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
				if err != nil {
					return empty, true, err
				}
				if claim.SemanticKey != facts.Claim.SemanticKey || claim.RequestDigest != evidence {
					return empty, true, ErrRepositoryMaterialization
				}
				runner := m.Git
				if runner.MutationAuthority == nil {
					runner.MutationAuthority = m.Store
				}
				if _, err := runner.CommitProtectedCheckpoint(ctx, worktree, git.ProtectedCheckpointRequest{EvidenceDigest: evidence, Timestamp: time.Unix(0, 0).UTC(), OriginalCheckpoint: parent, ProtectedPaths: protected, FullSnapshotDigest: receipt.FullSnapshotDigest, ImplementationDigest: receipt.ImplementationDigest, MutationClaim: claim}); err != nil {
					return empty, true, errors.Join(err, m.settleCommitFailure(ctx, claim))
				}
				if err := m.proveProtectedCheckpointSynced(ctx, worktree, parent, protected, implementation, prepared); err != nil {
					return empty, true, errors.Join(err, m.settleCommitFailure(ctx, claim))
				}
			}
			if _, err := m.Store.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: prepared.CommitOID, ParentOID: prepared.ParentOID, TreeOID: prepared.TreeOID}); err != nil {
				return empty, true, err
			}
			if err := m.proveProtectedCheckpointSynced(ctx, worktree, parent, protected, implementation, prepared); err != nil {
				return empty, true, err
			}
			return workflowworker.VerificationCheckpoint{ID: prepared.CommitOID, Commit: prepared, CommandResult: receipt.Command}, true, nil
		}
	}
	var command contracts.RepositoryCommandResultKey
	var result store.RepositoryCommandResult
	if receiptErr == nil {
		command = receipt.Command
		result, err = m.Store.LoadRepositoryCommandResult(ctx, command)
	} else {
		command, result, err = m.runCommand(ctx, request, store.RepositoryCommandPurposePrebuildVerification, reviewer, artifact, "")
	}
	if err != nil || !verificationOutcome(artifact.PrebuildOutcome, result.Result.ExitCode) {
		return empty, true, materializeErr(err)
	}
	full, err := m.Git.InspectRetainedWorktree(ctx, worktree)
	if err != nil || full.Changes.Head != parent {
		return empty, true, ErrRepositoryMaterialization
	}
	if implementation, err := m.Git.InspectRetainedImplementation(ctx, worktree, parent, protected); err != nil || implementation != value.Snapshot.ImplementationDigest {
		return empty, true, ErrRepositoryMaterialization
	}
	// This is the accepted Reviewer's physical snapshot, after its independent
	// prebuild command. The earlier Builder request snapshot is not substituted.
	if receiptErr != nil {
		receipt, err = m.Store.RecordPostbuildAmendmentCheckpointSnapshot(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence, reviewer, command, full.Digest)
		if err != nil {
			return empty, true, err
		}
	}
	if receipt.FullSnapshotDigest != full.Digest || receipt.ImplementationDigest != value.Snapshot.ImplementationDigest {
		return empty, true, ErrRepositoryMaterialization
	}
	evidence := verificationAmendmentCheckpointCommitDigest(request, reviewer, command, result.ResultDigest, artifact)
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, Fence: request.Fence}, RequestDigest: evidence, Repository: worktree.Identity.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: worktree.Identity.BaseRef, ExpectedBaseOID: worktree.Identity.BaseHead, ExpectedHeadOID: parent}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	var claim contracts.GitMutationClaim
	if _, effectErr := m.Store.Effect(ctx, intent.SemanticKey); errors.Is(effectErr, store.ErrNotFound) {
		if _, err := m.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: evidence}); err != nil {
			return empty, true, err
		}
		claim, err = m.Store.IssueGitMutationClaim(ctx, intent)
	} else if effectErr != nil {
		return empty, true, effectErr
	} else {
		// Resume the immutable receipt's existing operation. Never rerun its
		// command, replace its receipt, or mint another semantic target.
		claim, err = m.Store.ReclaimPostbuildAmendmentCheckpoint(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	}
	if err != nil {
		return empty, true, err
	}
	if claim.SemanticKey != intent.SemanticKey || claim.RequestDigest != evidence {
		return empty, true, ErrRepositoryMaterialization
	}
	runner := m.Git
	if runner.MutationAuthority == nil {
		runner.MutationAuthority = m.Store
	}
	_, err = runner.CommitProtectedCheckpoint(ctx, worktree, git.ProtectedCheckpointRequest{EvidenceDigest: evidence, Timestamp: time.Unix(0, 0).UTC(), OriginalCheckpoint: parent, ProtectedPaths: protected, FullSnapshotDigest: receipt.FullSnapshotDigest, ImplementationDigest: receipt.ImplementationDigest, MutationClaim: claim})
	if err != nil {
		return empty, true, errors.Join(err, m.settleCommitFailure(ctx, claim))
	}
	if m.BeforePreparedCommitObservation != nil {
		if err := m.BeforePreparedCommitObservation(); err != nil {
			return empty, true, errors.Join(err, m.settleCommitFailure(ctx, claim))
		}
	}
	observed, err := runner.ObserveCommit(ctx, worktree)
	if err != nil || observed.ParentOID != parent {
		return empty, true, errors.Join(materializeErr(err), m.settleCommitFailure(ctx, claim))
	}
	if _, err := m.Store.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); err != nil {
		return empty, true, errors.Join(err, m.settleCommitFailure(ctx, claim))
	}
	checkpoint := workflowworker.VerificationCheckpoint{ID: observed.CommitOID, Commit: store.CommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}, CommandResult: command}
	if m.AfterVerificationCheckpoint != nil {
		if err := m.AfterVerificationCheckpoint(checkpoint); err != nil {
			return empty, true, err
		}
	}
	return checkpoint, true, nil
}

func (m RepositoryMaterializer) proveProtectedCheckpointSynced(ctx context.Context, worktree git.Worktree, original string, protected []string, implementation string, prepared store.CommitObservation) error {
	observed, err := m.Git.ObserveCommit(ctx, worktree)
	if err != nil || observed.CommitOID != prepared.CommitOID || observed.ParentOID != original || observed.ParentOID != prepared.ParentOID || observed.TreeOID != prepared.TreeOID {
		return ErrRepositoryMaterialization
	}
	changes, err := m.Git.InspectRetainedWorktree(ctx, worktree)
	if err != nil || changes.Changes.Head != prepared.CommitOID {
		return ErrRepositoryMaterialization
	}
	for _, path := range changes.Changes.Paths {
		if containsPath(protected, path) {
			return ErrRepositoryMaterialization
		}
	}
	got, err := m.Git.InspectRetainedImplementation(ctx, worktree, original, protected)
	if err != nil || got != implementation {
		return ErrRepositoryMaterialization
	}
	return nil
}
