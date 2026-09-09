package worktreecoord

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

type postbuildAmendmentSource interface {
	PostbuildVerificationAmendmentContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildVerificationAmendmentContext, error)
	Project(context.Context, domain.Channel, domain.ProjectID) (store.Project, error)
	TicketWorktreePath(domain.TicketRef) (string, error)
	LatestReusableProviderAttempt(context.Context, store.LatestReusableProviderAttemptRequest) (store.LatestReusableProviderAttemptResult, error)
	CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error)
	PostbuildAmendmentPreparedCheckpoint(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CommitObservation, bool, error)
	PostbuildAmendmentPreparedCandidateWitness(context.Context, domain.TicketRef, uint64, domain.Fence, store.ProviderAttemptResultKey) (store.PostbuildAmendmentPreparedCandidateWitness, bool, error)
	AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error
}

type postbuildAmendmentInspector interface {
	InspectRetainedWorktree(context.Context, git.Worktree) (git.RetainedWorktreeInspection, error)
	InspectRetainedImplementation(context.Context, git.Worktree, string, []string) (string, error)
}

// Admission composes three independently bounded 15-second Git inspections
// (initial snapshot, implementation, final snapshot) and Store rechecks. Keep
// those individual limits, with 15 seconds for the bounded Store/selection work.
// context.WithTimeout still honors an earlier caller deadline or cancellation.
const postbuildAmendmentAdmissionTimeout = 60 * time.Second

// AuthenticatePostbuildVerificationAmendment is separate from pristine Ensure.
// It preserves the bound implementation while an independent Reviewer changes
// only the frozen test scope. Rejection never grants dirty Builder admission.
func (c Coordinator) AuthenticatePostbuildVerificationAmendment(ctx context.Context, request EnsureRequest) (store.StoredWorktree, error) {
	if c.Store == nil {
		return store.StoredWorktree{}, ErrAuthentication
	}
	return authenticatePostbuildVerificationAmendment(ctx, request, c.Store, c.Git)
}

func authenticatePostbuildVerificationAmendment(ctx context.Context, request EnsureRequest, source postbuildAmendmentSource, inspector postbuildAmendmentInspector) (store.StoredWorktree, error) {
	if request.Ref.Validate() != nil || request.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 {
		return store.StoredWorktree{}, ErrAuthentication
	}
	ctx, cancel := context.WithTimeout(ctx, postbuildAmendmentAdmissionTimeout)
	defer cancel()
	unready := func(stage string, cause error) error {
		// Preserve typed diagnostics, never raw subprocess output.
		causes := []error{ErrUnready, ctx.Err()}
		for _, safe := range []error{context.DeadlineExceeded, context.Canceled, git.ErrUnsafeWorktree, git.ErrIdentityMismatch, git.ErrOutputBound} {
			if errors.Is(cause, safe) {
				causes = append(causes, safe)
			}
		}
		return fmt.Errorf("postbuild amendment %s: %w", stage, errors.Join(causes...))
	}
	if err := ctx.Err(); err != nil {
		return store.StoredWorktree{}, err
	}
	proof, err := source.PostbuildVerificationAmendmentContext(ctx, request.Ref, request.Version, request.Fence)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	if proof.Decision == store.VerificationAmendmentRejected {
		return store.StoredWorktree{}, ErrUnready
	}
	project, err := source.Project(ctx, request.Ref.Channel, request.Ref.Project)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	path, err := source.TicketWorktreePath(request.Ref)
	if err != nil || proof.Worktree.Path != path || proof.Worktree.State != "registered" {
		return store.StoredWorktree{}, ErrAuthentication
	}
	worktree, identity, err := decodeWorktree(path, proof.Worktree.Branch, proof.Worktree.IdentityJSON)
	if err != nil || project.Channel != request.Ref.Channel || project.ID != request.Ref.Project || !sameIdentityJSON(proof.Worktree.IdentityJSON, identity) || identity.Worktree != path || identity.HeadRef != proof.Worktree.Branch || identity.Repository != project.Path || identity.BaseRef != project.BaseRef || identity.BaseHead != proof.Worktree.BaseSHA || proof.Builder.Claim.Repository != project.Path || proof.Builder.Claim.BaseSHA != proof.Worktree.BaseSHA || proof.Builder.Claim.Worktree != path || proof.Builder.Claim.WorktreeIdentity != string(proof.Worktree.IdentityJSON) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	phase, role := domain.PhaseVerification, "reviewer"
	head := proof.Binding.OriginalCheckpointOID
	if proof.Decision == store.VerificationAmendmentAccepted {
		phase, role = domain.PhaseBuild, "builder"
		head = proof.CurrentVerification.Checkpoint.CommitOID
		if proof.Candidate != nil {
			head = proof.Candidate.Snapshot.HeadSHA
		}
	}
	reuseRequest := store.LatestReusableProviderAttemptRequest{Ref: request.Ref, Phase: phase, Role: role, ExpectedVersion: request.Version, Fence: request.Fence}
	completed, completedErr := source.LatestReusableProviderAttempt(ctx, reuseRequest)
	if completedErr != nil && !errors.Is(completedErr, store.ErrNotFound) {
		return store.StoredWorktree{}, completedErr
	}
	var recorded *store.StoredVerification
	if completedErr == nil && phase == domain.PhaseVerification {
		if completed.Result.Claim.ExpectedVersion < proof.Amendment.TransitionTicketVersion || completed.Parsed.Verify == nil || !reflect.DeepEqual(completed.Parsed.Verify.OwnedFiles, proof.Verification.Revision.OwnedFiles) {
			return store.StoredWorktree{}, ErrAuthentication
		}
		if current, currentErr := source.CurrentVerification(ctx, request.Ref); currentErr == nil && current.ProviderResult == completed.Key {
			head = current.Checkpoint.CommitOID
			recorded = &current
		}
	}
	observed, err := inspector.InspectRetainedWorktree(ctx, worktree)
	if err != nil {
		return store.StoredWorktree{}, unready("initial physical snapshot", err)
	}
	// The Store-owned candidate handoff is only a clean finalization replay,
	// never permission to retain another dirty delta or launch a provider.
	if proof.Candidate != nil && (phase != domain.PhaseBuild || completedErr != nil || completed.Key != proof.Candidate.BuilderResult || observed.Changes.Head != proof.Candidate.Snapshot.HeadSHA || len(observed.Changes.Paths) != 0) {
		return store.StoredWorktree{}, unready("persisted candidate handoff", nil)
	}
	var preparedCandidate *store.PostbuildAmendmentPreparedCandidateWitness
	if phase == domain.PhaseBuild && proof.Candidate == nil && observed.Changes.Head != head && completedErr == nil {
		child, found, err := source.PostbuildAmendmentPreparedCandidateWitness(ctx, request.Ref, request.Version, request.Fence, completed.Key)
		if err != nil || !found || child.Builder != completed.Key || child.Commit.ParentOID != head || child.Commit.CommitOID != observed.Changes.Head || len(observed.Changes.Paths) != 0 {
			return store.StoredWorktree{}, unready("prepared candidate handoff", err)
		}
		preparedCandidate, head = &child, child.Commit.CommitOID
	}
	var prepared *store.CommitObservation
	if observed.Changes.Head != head && phase == domain.PhaseVerification && completedErr == nil && recorded == nil {
		child, found, err := source.PostbuildAmendmentPreparedCheckpoint(ctx, request.Ref, request.Version, request.Fence)
		if err != nil || !found || child.ParentOID != proof.Binding.OriginalCheckpointOID || !validFullOID(child.CommitOID) || !validFullOID(child.TreeOID) {
			return store.StoredWorktree{}, ErrAuthentication
		}
		prepared, head = &child, child.CommitOID
	}
	if observed.Changes.Head != head {
		return store.StoredWorktree{}, unready("physical head mismatch", nil)
	}
	if phase == domain.PhaseBuild || head != proof.Binding.OriginalCheckpointOID {
		protectedPaths := proof.Verification.Revision.OwnedFiles
		if phase == domain.PhaseBuild {
			protectedPaths = proof.CurrentVerification.Revision.OwnedFiles
		}
		for _, path := range observed.Changes.Paths {
			for _, protected := range protectedPaths {
				if path == protected || strings.HasPrefix(path, strings.TrimSuffix(protected, "/")+"/") {
					return store.StoredWorktree{}, unready("protected path changed", nil)
				}
			}
		}
	}
	if completedErr == nil && phase == domain.PhaseBuild {
		if completed.Result.Claim.ExpectedVersion <= proof.Amendment.TransitionTicketVersion || completed.Parsed.Builder == nil || completed.Parsed.Builder.AmendmentRequest != nil || proof.Plan.Document.Planner == nil || !refreshBuilderChangesMatch(observed.Changes, head, completed.Parsed.Builder.ChangedFiles, proof.Plan.Document.Planner.Paths, proof.CurrentVerification.Revision.OwnedFiles) {
			return store.StoredWorktree{}, ErrAuthentication
		}
	} else {
		implementation, err := inspector.InspectRetainedImplementation(ctx, worktree, proof.Binding.OriginalCheckpointOID, proof.Verification.Revision.OwnedFiles)
		if err != nil || implementation != proof.Snapshot.ImplementationDigest {
			return store.StoredWorktree{}, unready("retained implementation", err)
		}
		if phase == domain.PhaseVerification && errors.Is(completedErr, store.ErrNotFound) && observed.Digest != proof.Snapshot.FullSnapshotDigest {
			return store.StoredWorktree{}, unready("initial snapshot digest mismatch", nil)
		}
	}
	checked, err := source.PostbuildVerificationAmendmentContext(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !reflect.DeepEqual(checked, proof) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	againCompleted, againErr := source.LatestReusableProviderAttempt(ctx, reuseRequest)
	if completedErr == nil {
		again, err := againCompleted, againErr
		if err != nil || again.Key != completed.Key || again.Result.TypedSHA256 != completed.Result.TypedSHA256 {
			return store.StoredWorktree{}, ErrAuthentication
		}
	} else if !errors.Is(againErr, store.ErrNotFound) {
		return store.StoredWorktree{}, ErrAuthentication
	}
	if prepared != nil {
		again, found, err := source.PostbuildAmendmentPreparedCheckpoint(ctx, request.Ref, request.Version, request.Fence)
		if err != nil || !found || again != *prepared {
			return store.StoredWorktree{}, ErrAuthentication
		}
	}
	if preparedCandidate != nil {
		again, found, err := source.PostbuildAmendmentPreparedCandidateWitness(ctx, request.Ref, request.Version, request.Fence, completed.Key)
		if err != nil || !found || !reflect.DeepEqual(again, *preparedCandidate) {
			return store.StoredWorktree{}, ErrAuthentication
		}
	}
	if recorded != nil {
		again, err := source.CurrentVerification(ctx, request.Ref)
		if err != nil || !reflect.DeepEqual(again, *recorded) {
			return store.StoredWorktree{}, ErrAuthentication
		}
	}
	again, err := inspector.InspectRetainedWorktree(ctx, worktree)
	if err != nil || again.Digest != observed.Digest {
		return store.StoredWorktree{}, unready("final physical snapshot", err)
	}
	if err := ctx.Err(); err != nil {
		return store.StoredWorktree{}, err
	}
	if err := source.AssertTicketFence(ctx, request.Ref, request.Version, request.Fence); err != nil {
		return store.StoredWorktree{}, err
	}
	return proof.Worktree, nil
}
