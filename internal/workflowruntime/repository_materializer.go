package workflowruntime

// This file is the deliberately small production composition between the
// workflow worker and the two authenticated repository boundaries.  Provider
// bytes are never used as command or Git authority: every such value is
// recovered from Store and compared to the frozen ticket snapshot.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/repositoryexec"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

var ErrRepositoryMaterialization = errors.New("repository materialization is not authenticated")

type RepositoryMaterializer struct {
	Store    *store.Store
	Git      git.Runner
	Executor repositoryexec.Executor

	// AfterVerificationCheckpoint is a test-only crash seam. Production leaves
	// it nil. It runs only after Git's prepared commit is durably confirmed and
	// immediately before Worker can RecordVerification.
	AfterVerificationCheckpoint func(workflowworker.VerificationCheckpoint) error
	// AfterCandidateCommit is the equivalent crash seam for candidate child G.
	// It runs only after G is durably confirmed and immediately before Worker can
	// RecordCandidate. Recovery must observe and bind that exact G rather than
	// running post-build or issuing another Git mutation.
	AfterCandidateCommit func(workflowworker.CandidateWitness) error
	// BeforePreparedCommitObservation is a test-only response-loss seam. It runs
	// after Commit has made its prepared child reachable but before this
	// materializer can observe it. Production leaves it nil.
	BeforePreparedCommitObservation func() error
}

func (m RepositoryMaterializer) MaterializeVerificationCheckpoint(ctx context.Context, request workflowworker.PhaseRequest, artifact phaseartifact.Verification, key store.ProviderAttemptResultKey) (workflowworker.VerificationCheckpoint, error) {
	if m.Store == nil || request.Phase != "verification" {
		return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || !providerMatches(request, provider, key, domain.PhaseVerification, "reviewer") || m.Store.ProviderResultReachesFence(ctx, key, request.Ticket.Version, request.Fence) != nil || parsed.Verify == nil || !sameJSON(*parsed.Verify, artifact) {
		return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
	}
	if prior := request.RecoveryVerification; prior != nil {
		if prior.ProviderResult != key || prior.Checkpoint.CommitOID != prior.Revision.CheckpointID || prior.CommandBinding.Key.SemanticKey == "" {
			return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
		}
		if err := m.Store.ProviderResultReachesFence(ctx, prior.ProviderResult, request.Ticket.Version, request.Fence); err != nil {
			return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
		}
		observed, err := m.observe(ctx, request)
		if err != nil || observed != prior.Checkpoint || observed.ParentOID != request.Worktree.BaseSHA {
			return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
		}
		if _, err := m.Store.LoadRepositoryCommandResult(ctx, prior.CommandBinding.Key); err != nil {
			return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
		}
		return workflowworker.VerificationCheckpoint{ID: prior.Revision.CheckpointID, Commit: prior.Checkpoint, CommandResult: prior.CommandBinding.Key}, nil
	}
	command, result, err := m.runCommand(ctx, request, store.RepositoryCommandPurposePrebuildVerification, key, artifact, "")
	if err != nil || !verificationOutcome(artifact.PrebuildOutcome, result.Result.ExitCode) {
		return workflowworker.VerificationCheckpoint{}, materializeErr(err)
	}
	parent, err := m.verificationParent(ctx, request)
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	allowed, protected, err := m.verificationCommitPolicy(ctx, request, artifact, parent)
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	evidence, err := m.verificationCheckpointCommitDigest(ctx, request, key, command, result.ResultDigest, artifact)
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	observation, err := m.commit(ctx, request, key, parent, allowed, protected, evidence)
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	checkpoint := workflowworker.VerificationCheckpoint{ID: observation.CommitOID, Commit: observation, CommandResult: command}
	if m.AfterVerificationCheckpoint != nil {
		if err := m.AfterVerificationCheckpoint(checkpoint); err != nil {
			return workflowworker.VerificationCheckpoint{}, err
		}
	}
	return checkpoint, nil
}

func (m RepositoryMaterializer) AuthenticateVerificationCheckpoint(ctx context.Context, request workflowworker.PhaseRequest, _ phaseartifact.Verification, checkpoint workflowworker.VerificationCheckpoint) error {
	if m.Store == nil || checkpoint.ID == "" || checkpoint.ID != checkpoint.Commit.CommitOID || checkpoint.CommandResult.SemanticKey == "" || checkpoint.CommandResult.ClaimEpoch == 0 {
		return ErrRepositoryMaterialization
	}
	parent, err := m.verificationParent(ctx, request)
	if err != nil {
		return ErrRepositoryMaterialization
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != checkpoint.Commit || observed.ParentOID != parent {
		return ErrRepositoryMaterialization
	}
	_, err = m.Store.LoadRepositoryCommandResult(ctx, checkpoint.CommandResult)
	return err
}

// verificationParent preserves the ordinary base-branch contract while
// authenticating the one clean operator source-commit takeover. The retained
// verification checkpoint is the operator commit's parent; the new verifier
// executes from that operator commit and creates a fresh child checkpoint.
func (m RepositoryMaterializer) verificationParent(ctx context.Context, request workflowworker.PhaseRequest) (string, error) {
	if m.Store == nil || request.Ticket.Ref.Validate() != nil {
		return "", ErrRepositoryMaterialization
	}
	parent := request.Worktree.BaseSHA
	proof, found, err := m.Store.OperatorSourceResumeProof(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil {
		return "", ErrRepositoryMaterialization
	}
	if !found {
		return parent, nil
	}
	if request.Ticket.State != domain.StateVerifying || proof.Ref != request.Ticket.Ref || proof.Version != request.Ticket.Version || proof.Fence != request.Fence || proof.Verification.Checkpoint.CommitOID == "" || proof.Verification.Checkpoint.CommitOID != proof.Verification.Revision.CheckpointID || proof.SourceCommit.ParentOID != proof.Verification.Checkpoint.CommitOID || proof.SourceCommit.CommitOID == "" {
		return "", ErrRepositoryMaterialization
	}
	return proof.SourceCommit.CommitOID, nil
}

func (m RepositoryMaterializer) verificationCommitPolicy(ctx context.Context, request workflowworker.PhaseRequest, artifact phaseartifact.Verification, parent string) ([]string, []string, error) {
	allowed := append([]string(nil), artifact.OwnedFiles...)
	proof, found, err := m.Store.OperatorSourceResumeProof(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil {
		return nil, nil, ErrRepositoryMaterialization
	}
	if !found {
		return allowed, nil, nil
	}
	if parent != proof.SourceCommit.CommitOID || !reflect.DeepEqual(artifact.OwnedFiles, proof.Verification.Revision.OwnedFiles) {
		return nil, nil, ErrRepositoryMaterialization
	}
	protected := make([]string, 0, len(proof.SourceCommit.Changes))
	seen := make(map[string]struct{}, len(allowed)+len(proof.SourceCommit.Changes))
	for _, path := range allowed {
		seen[path] = struct{}{}
	}
	for _, change := range proof.SourceCommit.Changes {
		protected = append(protected, change.Path)
		if _, exists := seen[change.Path]; !exists {
			allowed = append(allowed, change.Path)
			seen[change.Path] = struct{}{}
		}
	}
	return allowed, protected, nil
}

func (m RepositoryMaterializer) MaterializeCandidate(ctx context.Context, request workflowworker.PhaseRequest, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, key store.ProviderAttemptResultKey) (workflowworker.CandidateWitness, error) {
	if m.Store == nil || request.Phase != "build" || request.Verification == nil {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || !providerMatches(request, provider, key, domain.PhaseBuild, "builder") || m.Store.ProviderResultReachesFence(ctx, key, request.Ticket.Version, request.Fence) != nil || parsed.Builder == nil || !sameJSON(*parsed.Builder, builder) {
		return workflowworker.CandidateWitness{}, fmt.Errorf("candidate provider binding: %w", ErrRepositoryMaterialization)
	}
	// Re-establish the durable Planner scope before starting the post-build
	// command. A lost-response replay must fail closed on a missing or modified
	// plan rather than launching a command from caller memory.
	allowed, err := m.currentCandidatePlanScope(ctx, request, plan)
	if err != nil {
		return workflowworker.CandidateWitness{}, fmt.Errorf("candidate plan scope: %w", err)
	}
	// The builder may not overwrite proof ownership outside the accepted plan.
	if err := candidateChangedFilesWithinScope(builder.ChangedFiles, allowed); err != nil {
		return workflowworker.CandidateWitness{}, fmt.Errorf("candidate changed-file scope: %w", err)
	}
	if request.Candidate != nil {
		if request.Candidate.BuilderResult != key {
			return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
		}
		// Candidate replay is observation-only. In particular, a stale-fence
		// replay may prove the old immutable witness but must not launch another
		// repository command while Store has no historical-command rebind.
		return m.replayCandidate(ctx, request, verification, builder, *request.Candidate)
	}
	// Source-resume has one additional prepared-but-not-recorded boundary.  A
	// crash after Builder's post-build command and Git child G must not turn the
	// later recovery fence into a second command/commit intent. Store returns a
	// single authenticated witness spanning source S, fresh verification F,
	// Builder, command, and G; the live Git observation below is the only
	// runtime work performed before RecordCandidate.
	if recovered, found, recoveryErr := m.Store.OperatorSourceResumeRecoverablePreparedCandidateWitness(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence); recoveryErr != nil {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	} else if found {
		if recovered.EffectState == store.EffectUncertain {
			// G crossed update-ref, but the first observation response was lost.
			// Re-observe and confirm this exact prepared tuple before replaying
			// the witness. This path never reruns post-build or issues Commit.
			confirmed, err := m.confirmSourceResumePreparedCandidate(ctx, request, recovered)
			if err != nil {
				return workflowworker.CandidateWitness{}, err
			}
			recovered = confirmed
		}
		return m.replaySourceResumePreparedCandidate(ctx, request, plan, verification, builder, key, recovered)
	}
	command, result, err := m.runCommand(ctx, request, store.RepositoryCommandPurposePostbuildCandidate, key, phaseartifact.Verification{}, verification.CheckpointID)
	if err != nil || result.Result.ExitCode != 0 {
		return workflowworker.CandidateWitness{}, fmt.Errorf("candidate post-build command (exit=%d): %w", result.Result.ExitCode, materializeErr(err))
	}
	observation, err := m.commit(ctx, request, key, verification.CheckpointID, allowed, verification.OwnedFiles, commitDigest("candidate", request, key, command, result.ResultDigest, struct {
		Plan         workflowprompt.PlanIdentity
		Verification workflowprompt.VerificationIdentity
		Builder      phaseartifact.Builder
	}{plan, verification, builder}))
	if err != nil {
		return workflowworker.CandidateWitness{}, fmt.Errorf("candidate commit: %w", err)
	}
	witness := workflowworker.CandidateWitness{Commit: observation, CommandPolicyDigest: strings.TrimPrefix(result.Claim.PolicyDigest, "sha256:"), Reason: "authenticated post-build verification", CommandResult: command}
	if m.AfterCandidateCommit != nil {
		if err := m.AfterCandidateCommit(witness); err != nil {
			return workflowworker.CandidateWitness{}, err
		}
	}
	return witness, nil
}

// confirmSourceResumePreparedCandidate settles an uncertain source-resume G
// using only its durable claim and prepared tuple. The live observation is
// required to match both the durable tuple and expected parent before Store
// confirms it under the original same-fence claim.
func (m RepositoryMaterializer) confirmSourceResumePreparedCandidate(ctx context.Context, request workflowworker.PhaseRequest, recovered store.OperatorSourceResumePreparedCandidateWitness) (store.OperatorSourceResumePreparedCandidateWitness, error) {
	if recovered.EffectState != store.EffectUncertain || recovered.Claim.Operation != "commit" || recovered.Claim.TicketRef != request.Ticket.Ref || recovered.Claim.TicketVersion != request.Ticket.Version || recovered.Claim.LeaderEpoch != request.Fence.LeaderEpoch || recovered.Claim.RunnerEpoch != request.Fence.RunnerEpoch || recovered.Commit.ParentOID != recovered.Claim.ExpectedHeadOID {
		return store.OperatorSourceResumePreparedCandidateWitness{}, fmt.Errorf("source-resume uncertain witness binding: %w", ErrRepositoryMaterialization)
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != recovered.Commit {
		return store.OperatorSourceResumePreparedCandidateWitness{}, fmt.Errorf("source-resume prepared commit observation: %w", ErrRepositoryMaterialization)
	}
	if _, err := m.Store.ConfirmPreparedCommit(ctx, recovered.Claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); err != nil {
		return store.OperatorSourceResumePreparedCandidateWitness{}, err
	}
	confirmed, found, err := m.Store.OperatorSourceResumePreparedCandidateWitness(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil || !found || confirmed.Commit != recovered.Commit || confirmed.Claim != recovered.Claim || confirmed.EffectState != store.EffectConfirmed {
		return store.OperatorSourceResumePreparedCandidateWitness{}, fmt.Errorf("source-resume confirmed witness reload: %w", ErrRepositoryMaterialization)
	}
	return confirmed, nil
}

// replaySourceResumePreparedCandidate converts Store's prepared G witness
// into the normal Worker witness.  All durable authority was checked by Store;
// this method only verifies that the current materialization request names the
// same fresh plan/F/Builder tuple and re-observes exact live G.  It must never
// execute post-build or issue a commit.
func (m RepositoryMaterializer) replaySourceResumePreparedCandidate(ctx context.Context, request workflowworker.PhaseRequest, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, key store.ProviderAttemptResultKey, recovered store.OperatorSourceResumePreparedCandidateWitness) (workflowworker.CandidateWitness, error) {
	worktree, err := m.worktree(request)
	if err != nil {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	project, projectErr := m.Store.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if projectErr != nil {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume project reload: %w", ErrRepositoryMaterialization)
	}
	if recovered.Ref != request.Ticket.Ref || recovered.Version != request.Ticket.Version || recovered.Fence != request.Fence || !reflect.DeepEqual(recovered.Project, project) || recovered.Project.Path != worktree.Identity.Repository || recovered.Project.BaseRef != worktree.Identity.BaseRef || recovered.Builder != key {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness ticket/project binding: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Ref != request.Ticket.Ref {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness source ref: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Version != request.Ticket.Version {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness source version: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Fence != request.Fence {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness source fence: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Worktree.Path != request.Worktree.Path {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness worktree path: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Worktree.Branch != request.Worktree.Branch {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness worktree branch: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Worktree.BaseSHA != request.Worktree.BaseSHA {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness worktree base: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Worktree.HeadSHA != request.Worktree.HeadSHA {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness worktree head: %w", ErrRepositoryMaterialization)
	}
	if !bytes.Equal(recovered.Source.Worktree.IdentityJSON, request.Worktree.IdentityJSON) {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness worktree identity: %w", ErrRepositoryMaterialization)
	}
	if recovered.Verification.Revision.IntentDigest != verification.IntentDigest || recovered.Verification.Revision.ProofDigest != verification.ProofDigest || recovered.Verification.Revision.CheckpointID != verification.CheckpointID || recovered.Verification.Checkpoint.CommitOID != verification.CheckpointID || !sameJSON(recovered.Verification.Revision.OwnedFiles, verification.OwnedFiles) || !sameJSON(recovered.Verification.Revision.IntentDigest, verification.IntentDigest) || recovered.Commit.ParentOID != verification.CheckpointID {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness verification binding: %w", ErrRepositoryMaterialization)
	}
	if recovered.Command.Key.SemanticKey == "" || recovered.Command.Key.ClaimEpoch == 0 || recovered.Command.Result.ExitCode != 0 || recovered.Command.Claim.TicketRef != request.Ticket.Ref || recovered.Command.Claim.Repository != worktree.Identity.Repository || recovered.Command.Claim.Worktree != request.Worktree.Path || recovered.Command.Claim.WorktreeIdentity != string(request.Worktree.IdentityJSON) || recovered.Command.Claim.Branch != request.Worktree.Branch || recovered.Command.Claim.BaseRef != worktree.Identity.BaseRef || recovered.Command.Claim.BaseSHA != request.Worktree.BaseSHA || !strings.HasPrefix(recovered.Command.Claim.PolicyDigest, "sha256:") {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume prepared witness command binding: %w", ErrRepositoryMaterialization)
	}
	if recovered.Source.Plan.Document.Planner == nil {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume stored plan: %w", ErrRepositoryMaterialization)
	}
	planDigest, err := workflowprompt.NewPlanIdentity(*recovered.Source.Plan.Document.Planner)
	if err != nil || !reflect.DeepEqual(planDigest, plan) {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume plan identity: %w", ErrRepositoryMaterialization)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil || builderDigest == "" {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume builder identity: %w", ErrRepositoryMaterialization)
	}
	command, err := m.Store.LoadRepositoryCommandResult(ctx, recovered.Command.Key)
	if err != nil || !reflect.DeepEqual(command, recovered.Command) {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume command reload: %w", ErrRepositoryMaterialization)
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != recovered.Commit {
		return workflowworker.CandidateWitness{}, fmt.Errorf("source-resume replay observation: %w", ErrRepositoryMaterialization)
	}
	return workflowworker.CandidateWitness{Commit: recovered.Commit, CommandPolicyDigest: strings.TrimPrefix(recovered.Command.Claim.PolicyDigest, "sha256:"), Reason: "recovered authenticated source-resume candidate", CommandResult: recovered.Command.Key}, nil
}

func (m RepositoryMaterializer) replayCandidate(ctx context.Context, request workflowworker.PhaseRequest, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, candidate store.StoredCandidate) (workflowworker.CandidateWitness, error) {
	if err := candidateMatches(request, verification, builder, candidate); err != nil {
		return workflowworker.CandidateWitness{}, err
	}
	if err := m.Store.ProviderResultReachesFence(ctx, candidate.BuilderResult, request.Ticket.Version, request.Fence); err != nil {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	durable, err := m.Store.RecoverableCandidate(ctx, request.Ticket.Ref)
	if err != nil || !reflect.DeepEqual(durable, candidate) {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	result, err := m.Store.LoadRepositoryCommandResult(ctx, candidate.CommandBinding.Key)
	if err != nil || result.Result.ExitCode != 0 {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != candidate.Commit {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	return workflowworker.CandidateWitness{Commit: candidate.Commit, CommandPolicyDigest: strings.TrimPrefix(result.Claim.PolicyDigest, "sha256:"), Reason: "authenticated post-build verification", CommandResult: candidate.CommandBinding.Key}, nil
}

func candidateMatches(request workflowworker.PhaseRequest, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, candidate store.StoredCandidate) error {
	digest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil || candidate.BuilderResult.AttemptID == 0 || candidate.Commit.CommitOID != candidate.Snapshot.HeadSHA || candidate.Commit.TreeOID != candidate.Snapshot.TreeSHA || candidate.Commit.ParentOID != verification.CheckpointID || candidate.Snapshot.BaseSHA != request.Worktree.BaseSHA || candidate.Snapshot.SourceDigest != request.Ticket.SourceDigest || candidate.Snapshot.VerificationIntentDigest != verification.IntentDigest || candidate.Snapshot.ProofDigest != verification.ProofDigest || candidate.Snapshot.BuilderEvidenceDigest != digest || candidate.CommandBinding.Key.SemanticKey == "" || candidate.CommandBinding.Key.ClaimEpoch == 0 {
		return ErrRepositoryMaterialization
	}
	return nil
}

// currentCandidatePlanScope re-authenticates every mutable input immediately
// before the Git commit. The builder can only narrow the persisted Planner
// scope; it can never supply an empty scope or broaden it with ChangedFiles.
func (m RepositoryMaterializer) currentCandidatePlanScope(ctx context.Context, request workflowworker.PhaseRequest, supplied workflowprompt.PlanIdentity) ([]string, error) {
	if m.Store == nil {
		return nil, ErrRepositoryMaterialization
	}
	ticket, err := m.Store.Ticket(ctx, request.Ticket.Ref)
	if err != nil || !reflect.DeepEqual(ticket, request.Ticket) || ticket.Ref != request.Ticket.Ref || ticket.State != domain.StateBuilding || ticket.RunnerEpoch != request.Fence.RunnerEpoch {
		return nil, ErrRepositoryMaterialization
	}
	if err := m.Store.AssertTicketFence(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence); err != nil {
		return nil, ErrRepositoryMaterialization
	}
	project, err := m.Store.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if err != nil || project.Path == "" {
		return nil, ErrRepositoryMaterialization
	}
	plan, err := m.Store.Plan(ctx, request.Ticket.Ref)
	if err != nil || request.Plan == nil || !sameJSON(*request.Plan, plan) || plan.TicketVersion == 0 || plan.Fence.LeaderEpoch == 0 || plan.Fence.RunnerEpoch == 0 || plan.Document.Planner == nil || plan.Document.ProviderResult == nil || plan.Digest == "" {
		return nil, ErrRepositoryMaterialization
	}
	encoded, err := json.Marshal(plan.Document)
	if err != nil {
		return nil, ErrRepositoryMaterialization
	}
	digest := sha256.Sum256(encoded)
	if hex.EncodeToString(digest[:]) != plan.Digest {
		return nil, ErrRepositoryMaterialization
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, *plan.Document.ProviderResult)
	if err != nil || provider.Claim.Ref != request.Ticket.Ref || provider.Claim.Phase != domain.PhasePlanning || provider.Claim.Role != "planner" || provider.Claim.ExpectedVersion != plan.TicketVersion || provider.Claim.LeaderEpoch != plan.Fence.LeaderEpoch || provider.Claim.RunnerEpoch != plan.Fence.RunnerEpoch || provider.Claim.Repository != project.Path || provider.Claim.Worktree != request.Worktree.Path || provider.Claim.WorktreeIdentity != string(request.Worktree.IdentityJSON) || provider.Claim.BaseSHA != request.Worktree.BaseSHA || parsed.Planner == nil || !sameJSON(*plan.Document.Planner, *parsed.Planner) {
		return nil, ErrRepositoryMaterialization
	}
	worktree, err := m.Store.Worktree(ctx, request.Ticket.Ref)
	if err != nil || worktree.Path != request.Worktree.Path || worktree.Branch != request.Worktree.Branch || worktree.BaseSHA != request.Worktree.BaseSHA || worktree.TicketVersion != request.Worktree.TicketVersion || worktree.Fence != request.Worktree.Fence || !bytes.Equal(worktree.IdentityJSON, request.Worktree.IdentityJSON) {
		return nil, ErrRepositoryMaterialization
	}
	return candidatePlanScope(*plan.Document.Planner, supplied)
}

func candidatePlanScope(stored phaseartifact.Planner, supplied workflowprompt.PlanIdentity) ([]string, error) {
	persisted, err := workflowprompt.NewPlanIdentity(stored)
	if err != nil || persisted.Digest != supplied.Digest || !sameJSON(persisted.Plan, supplied.Plan) {
		return nil, ErrRepositoryMaterialization
	}
	if len(stored.Paths) == 0 {
		return nil, ErrRepositoryMaterialization
	}
	for _, path := range stored.Paths {
		if path == "." {
			return nil, ErrRepositoryMaterialization
		}
	}
	if err := phaseartifact.ValidateMutationPaths(phaseartifact.Parsed{Phase: domain.PhasePlanning, Planner: &stored}, nil, stored.Paths); err != nil {
		return nil, ErrRepositoryMaterialization
	}
	return append([]string(nil), stored.Paths...), nil
}

func candidateChangedFilesWithinScope(changed, scope []string) error {
	for _, path := range changed {
		if !containsPath(scope, path) {
			return ErrRepositoryMaterialization
		}
	}
	return nil
}

func (m RepositoryMaterializer) AuthenticateCandidate(ctx context.Context, request workflowworker.PhaseRequest, _ workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, _ phaseartifact.Builder, witness workflowworker.CandidateWitness) error {
	if m.Store == nil || witness.CommandResult.SemanticKey == "" || witness.CommandResult.ClaimEpoch == 0 || witness.Commit.ParentOID != verification.CheckpointID {
		return ErrRepositoryMaterialization
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != witness.Commit {
		return ErrRepositoryMaterialization
	}
	result, err := m.Store.LoadRepositoryCommandResult(ctx, witness.CommandResult)
	if err != nil || result.Result.ExitCode != 0 || strings.TrimPrefix(result.Claim.PolicyDigest, "sha256:") != witness.CommandPolicyDigest {
		return ErrRepositoryMaterialization
	}
	return nil
}

func (m RepositoryMaterializer) runCommand(ctx context.Context, request workflowworker.PhaseRequest, purpose string, provider store.ProviderAttemptResultKey, verification phaseartifact.Verification, checkpoint string) (contracts.RepositoryCommandResultKey, store.RepositoryCommandResult, error) {
	if m.Store == nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, fmt.Errorf("repository command store: %w", ErrRepositoryMaterialization)
	}
	effective, err := decodeTicketConfig(request.Ticket)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	argv := append([]string(nil), effective.Commands.Verify.Argv...)
	worktree, err := m.worktree(request)
	if err != nil || effective.Repository == "" || effective.Repository != worktree.Identity.Repository || effective.BaseBranch != worktree.Identity.BaseRef {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, fmt.Errorf("repository command config/worktree binding: %w", ErrRepositoryMaterialization)
	}
	if purpose == store.RepositoryCommandPurposePrebuildVerification && !equalArgv(argv, verification.Command) {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, fmt.Errorf("repository verification command binding: %w", ErrRepositoryMaterialization)
	}
	policy, err := executionpolicy.NewCommandSnapshot(argv)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	timeout := effective.PhaseTimeout
	if timeout <= 0 || timeout > 45*time.Minute {
		timeout = 45 * time.Minute
	}
	spec := contracts.CommandSpec{Argv: argv, Directory: request.Worktree.Path, Timeout: timeout, Profile: contracts.ProfileGuarded}
	executable, executableDigest, err := processsupervisor.RepositoryCommandExecutableIdentity(argv)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	commandDigest, err := repositoryexec.CommandDigest(argv)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	empty := sha256.Sum256(nil)
	specDigest, err := repositoryexec.SpecDigest(spec, "sha256:"+hex.EncodeToString(empty[:]))
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	intent, proof := "", ""
	if request.Verification != nil {
		intent, proof = request.Verification.Revision.IntentDigest, request.Verification.Revision.ProofDigest
	}
	if purpose == store.RepositoryCommandPurposePrebuildVerification {
		intent, err = workflowprompt.VerificationIntentDigest(verification)
		if err != nil {
			return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
		}
		proof, err = workflowprompt.VerificationProofDigest(verification)
		if err != nil {
			return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
		}
	}
	evidence := store.RepositoryCommandEvidenceRequest{Purpose: purpose, Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch, ProviderResult: provider, VerificationIntentDigest: intent, ProofDigest: proof, CheckpointID: checkpoint, ConfigCommandDigest: commandDigest, Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), BaseSHA: request.Worktree.BaseSHA, PolicyDigest: policy.Digest(), SpecDigest: specDigest, ExecutablePath: executable, ExecutableDigest: executableDigest}
	if purpose == store.RepositoryCommandPurposePrebuildVerification {
		if key, recovered, found, recoveryErr := m.recoverSourceResumeVerificationCommand(ctx, request, provider, evidence); recoveryErr != nil {
			return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, recoveryErr
		} else if found {
			return key, recovered, nil
		}
		if key, recovered, found, recoveryErr := m.recoverVerificationAmendmentCommand(ctx, request, provider, evidence); recoveryErr != nil {
			return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, recoveryErr
		} else if found {
			return key, recovered, nil
		}
	}
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(evidence)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(evidence)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	if effect, effectErr := m.Store.Effect(ctx, semantic); effectErr == nil && (effect.State == store.EffectConfirmed || effect.State == store.EffectFailed) {
		result, loadErr := m.Store.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: semantic, ClaimEpoch: effect.ClaimEpoch})
		if loadErr == nil {
			return result.Key, result, nil
		}
		// A failed effect before the repository child launched has no immutable
		// result. PlanEffect may safely bind the same deterministic request for a
		// new claim; a confirmed effect without its result is tampering/ambiguity.
		if effect.State != store.EffectFailed || !errors.Is(loadErr, store.ErrNotFound) {
			return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, loadErr
		}
	} else if effectErr == nil && (effect.State == store.EffectExecuting || effect.State == store.EffectUncertain) {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, fmt.Errorf("repository command effect is unresolved: %w", ErrRepositoryMaterialization)
	}
	if _, err = m.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: semantic, Ref: request.Ticket.Ref, Kind: "repository_command", TicketVersion: request.Ticket.Version, Fence: request.Fence, RequestDigest: requestDigest}); err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	claim, err := m.Store.IssueRepositoryCommandClaim(ctx, store.RepositoryCommandIntent{EffectFence: store.EffectFence{SemanticKey: semantic, Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, Fence: request.Fence}, RequestDigest: requestDigest, Repository: effective.Repository, Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), Branch: request.Worktree.Branch, BaseRef: effective.BaseBranch, BaseSHA: request.Worktree.BaseSHA, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest(), ExecutablePath: executable, ExecutableDigest: executableDigest})
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	executor := m.Executor
	if executor.Authority == nil {
		executor.Authority = m.Store
	}
	resultRaw, runErr := executor.Run(ctx, repositoryexec.Request{Claim: claim, Spec: spec, Policy: policy})
	key := contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}
	result, loadErr := m.Store.LoadRepositoryCommandResult(ctx, key)
	if loadErr != nil {
		if runErr != nil {
			return key, store.RepositoryCommandResult{}, runErr
		}
		return key, store.RepositoryCommandResult{}, loadErr
	}
	if !resultRaw.Observed && runErr != nil {
		return key, result, runErr
	}
	return key, result, nil
}

// recoverSourceResumeVerificationCommand finds the prebuild command emitted at
// the immutable source-resume endpoint. Its original ticket fence is carried
// by the Reviewer claim, not the caller's later recovery fence; that lets a
// response-loss restart reuse the exact result without launching the command
// again. A missing result deliberately falls through to normal current-fence
// planning.
func (m RepositoryMaterializer) recoverSourceResumeVerificationCommand(ctx context.Context, request workflowworker.PhaseRequest, key store.ProviderAttemptResultKey, current store.RepositoryCommandEvidenceRequest) (contracts.RepositoryCommandResultKey, store.RepositoryCommandResult, bool, error) {
	proof, found, err := m.Store.OperatorSourceResumeProof(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil || !found {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, found, err
	}
	if key == proof.Verification.ProviderResult {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, nil
	}
	provider, _, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || provider.Claim.Ref != request.Ticket.Ref || provider.Claim.Phase != domain.PhaseVerification || provider.Claim.Role != "reviewer" || provider.Claim.ID != key.AttemptID || provider.Claim.Attempt != key.Attempt || provider.Claim.ExpectedVersion == 0 || provider.Claim.LeaderEpoch == 0 || provider.Claim.RunnerEpoch == 0 {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	stable := current
	stable.TicketVersion, stable.LeaderEpoch, stable.RunnerEpoch = provider.Claim.ExpectedVersion, provider.Claim.LeaderEpoch, provider.Claim.RunnerEpoch
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(stable)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(stable)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	effect, err := m.Store.Effect(ctx, semantic)
	if errors.Is(err, store.ErrNotFound) {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, nil
	}
	if err != nil || effect.RequestDigest != requestDigest || effect.State != store.EffectConfirmed {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	result, err := m.Store.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: semantic, ClaimEpoch: effect.ClaimEpoch})
	if err != nil || result.Claim.RequestDigest != requestDigest {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	return result.Key, result, true, nil
}

// recoverVerificationAmendmentCommand rebinds the command result created for
// an amendment Reviewer at its immutable provider endpoint. A later recovery
// changes the live fence but must neither rerun the command nor create a new
// checkpoint identity for the same completed Reviewer result.
func (m RepositoryMaterializer) recoverVerificationAmendmentCommand(ctx context.Context, request workflowworker.PhaseRequest, key store.ProviderAttemptResultKey, current store.RepositoryCommandEvidenceRequest) (contracts.RepositoryCommandResultKey, store.RepositoryCommandResult, bool, error) {
	if request.Amendment == nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, nil
	}
	amendment, err := m.Store.PendingVerificationAmendment(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil || !reflect.DeepEqual(amendment, *request.Amendment) {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	provider, _, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || provider.Claim.Ref != request.Ticket.Ref || provider.Claim.Phase != domain.PhaseVerification || provider.Claim.Role != "reviewer" || provider.Claim.ID != key.AttemptID || provider.Claim.Attempt != key.Attempt || amendment.TransitionTicketVersion == 0 || provider.Claim.ExpectedVersion < amendment.TransitionTicketVersion || provider.Claim.LeaderEpoch == 0 || provider.Claim.RunnerEpoch == 0 || m.Store.ProviderResultReachesFence(ctx, key, request.Ticket.Version, request.Fence) != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	stable := current
	stable.TicketVersion, stable.LeaderEpoch, stable.RunnerEpoch = provider.Claim.ExpectedVersion, provider.Claim.LeaderEpoch, provider.Claim.RunnerEpoch
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(stable)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(stable)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	effect, err := m.Store.Effect(ctx, semantic)
	if errors.Is(err, store.ErrNotFound) {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, nil
	}
	if err != nil || effect.RequestDigest != requestDigest || effect.State != store.EffectConfirmed {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	result, err := m.Store.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: semantic, ClaimEpoch: effect.ClaimEpoch})
	if err != nil || result.Claim.RequestDigest != requestDigest {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, false, ErrRepositoryMaterialization
	}
	return result.Key, result, true, nil
}

func (m RepositoryMaterializer) commit(ctx context.Context, request workflowworker.PhaseRequest, provider store.ProviderAttemptResultKey, parent string, paths, protected []string, evidence string) (store.CommitObservation, error) {
	worktree, err := m.worktree(request)
	if err != nil {
		return store.CommitObservation{}, err
	}
	runner := m.Git
	if runner.MutationAuthority == nil {
		runner.MutationAuthority = m.Store
	}
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, Fence: request.Fence}, RequestDigest: evidence, Repository: worktree.Identity.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: worktree.Identity.BaseRef, ExpectedBaseOID: worktree.Identity.BaseHead, ExpectedHeadOID: parent}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if observation, found, recoveryErr := m.recoverVerificationAmendmentPreparedCommit(ctx, request, provider, worktree, parent, evidence); recoveryErr != nil {
		return store.CommitObservation{}, recoveryErr
	} else if found {
		return observation, nil
	}
	// A response can be lost after update-ref.  Re-observe the exact prepared
	// tuple before considering a new mutation; an executing effect is never a
	// license to run Commit a second time.
	if facts, factsErr := m.Store.GitMutationIntentFacts(ctx, intent.SemanticKey); factsErr == nil {
		if facts.Claim != (contracts.GitMutationClaim{}) && (facts.Effect.State == store.EffectExecuting || facts.Effect.State == store.EffectUncertain || facts.Effect.State == store.EffectConfirmed) {
			observed, observeErr := runner.ObserveCommit(ctx, worktree)
			if observeErr != nil || observed.ParentOID != parent || observed.CommitOID != facts.PreparedCommitOID || observed.TreeOID != facts.PreparedTreeOID {
				return store.CommitObservation{}, ErrRepositoryMaterialization
			}
			if _, confirmErr := m.Store.ConfirmPreparedCommit(ctx, facts.Claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); confirmErr != nil {
				return store.CommitObservation{}, confirmErr
			}
			return store.CommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}, nil
		}
		return store.CommitObservation{}, ErrRepositoryMaterialization
	}
	if _, err := m.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: request.Ticket.Ref, Kind: "git/commit", TicketVersion: request.Ticket.Version, Fence: request.Fence, RequestDigest: evidence}); err != nil {
		return store.CommitObservation{}, err
	}
	claim, err := m.Store.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		return store.CommitObservation{}, err
	}
	if _, err = runner.Commit(ctx, worktree, git.CommitRequest{EvidenceDigest: evidence, Timestamp: time.Unix(0, 0).UTC(), BaseRef: worktree.Identity.BaseRef, ExpectedParent: parent, Policy: git.DiffPolicy{AllowedPaths: paths, ProtectedPaths: protected}, MutationClaim: claim}); err != nil {
		if settleErr := m.settleCommitFailure(ctx, claim); settleErr != nil {
			return store.CommitObservation{}, errors.Join(err, settleErr)
		}
		return store.CommitObservation{}, err
	}
	if m.BeforePreparedCommitObservation != nil {
		if err := m.BeforePreparedCommitObservation(); err != nil {
			if settleErr := m.settleCommitFailure(ctx, claim); settleErr != nil {
				return store.CommitObservation{}, errors.Join(err, settleErr)
			}
			return store.CommitObservation{}, err
		}
	}
	observed, err := runner.ObserveCommit(ctx, worktree)
	if err != nil {
		if settleErr := m.settleCommitFailure(ctx, claim); settleErr != nil {
			return store.CommitObservation{}, errors.Join(err, settleErr)
		}
		return store.CommitObservation{}, err
	}
	if _, err = m.Store.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); err != nil {
		if settleErr := m.settleCommitFailure(ctx, claim); settleErr != nil {
			return store.CommitObservation{}, errors.Join(err, settleErr)
		}
		return store.CommitObservation{}, err
	}
	return store.CommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}, nil
}

// recoverVerificationAmendmentPreparedCommit finds the commit prepared under
// the completed amendment Reviewer's immutable fence. The current worker fence
// authorizes the observation, not another update-ref mutation.
func (m RepositoryMaterializer) recoverVerificationAmendmentPreparedCommit(ctx context.Context, request workflowworker.PhaseRequest, key store.ProviderAttemptResultKey, worktree git.Worktree, parent, evidence string) (store.CommitObservation, bool, error) {
	if request.Amendment == nil {
		return store.CommitObservation{}, false, nil
	}
	amendment, err := m.Store.PendingVerificationAmendment(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil || !reflect.DeepEqual(amendment, *request.Amendment) {
		return store.CommitObservation{}, false, ErrRepositoryMaterialization
	}
	provider, _, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || provider.Claim.Ref != request.Ticket.Ref || provider.Claim.Phase != domain.PhaseVerification || provider.Claim.Role != "reviewer" || provider.Claim.ID != key.AttemptID || provider.Claim.Attempt != key.Attempt || provider.Claim.ExpectedVersion < amendment.TransitionTicketVersion || provider.Claim.LeaderEpoch == 0 || provider.Claim.RunnerEpoch == 0 || m.Store.ProviderResultReachesFence(ctx, key, request.Ticket.Version, request.Fence) != nil {
		return store.CommitObservation{}, false, ErrRepositoryMaterialization
	}
	// The prepared commit digest is stable; rebuild only the provider endpoint
	// semantic envelope to rediscover a commit that crossed before recovery.
	stable := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: request.Ticket.Ref, TicketVersion: provider.Claim.ExpectedVersion, Fence: domain.Fence{LeaderEpoch: provider.Claim.LeaderEpoch, RunnerEpoch: provider.Claim.RunnerEpoch}}, RequestDigest: evidence, Repository: worktree.Identity.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: worktree.Identity.BaseRef, ExpectedBaseOID: worktree.Identity.BaseHead, ExpectedHeadOID: parent}
	if stable.TicketVersion == 0 || stable.Fence.LeaderEpoch == 0 || stable.Fence.RunnerEpoch == 0 {
		return store.CommitObservation{}, false, ErrRepositoryMaterialization
	}
	stable.SemanticKey = store.CanonicalGitMutationSemanticKey(stable)
	facts, factsErr := m.Store.GitMutationIntentFacts(ctx, stable.SemanticKey)
	if errors.Is(factsErr, store.ErrNotFound) {
		return store.CommitObservation{}, false, nil
	}
	if factsErr != nil || facts.Claim == (contracts.GitMutationClaim{}) || (facts.Effect.State != store.EffectExecuting && facts.Effect.State != store.EffectUncertain && facts.Effect.State != store.EffectConfirmed) || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" {
		return store.CommitObservation{}, false, ErrRepositoryMaterialization
	}
	runner := m.Git
	if runner.MutationAuthority == nil {
		runner.MutationAuthority = m.Store
	}
	observed, observeErr := runner.ObserveCommit(ctx, worktree)
	if observeErr != nil || observed.ParentOID != parent || observed.CommitOID != facts.PreparedCommitOID || observed.TreeOID != facts.PreparedTreeOID {
		return store.CommitObservation{}, false, ErrRepositoryMaterialization
	}
	if _, confirmErr := m.Store.ConfirmPreparedCommit(ctx, facts.Claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); confirmErr != nil {
		return store.CommitObservation{}, false, confirmErr
	}
	return store.CommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}, true, nil
}

func (m RepositoryMaterializer) observe(ctx context.Context, request workflowworker.PhaseRequest) (store.CommitObservation, error) {
	w, err := m.worktree(request)
	if err != nil {
		return store.CommitObservation{}, err
	}
	got, err := m.Git.ObserveCommit(ctx, w)
	return store.CommitObservation{CommitOID: got.CommitOID, ParentOID: got.ParentOID, TreeOID: got.TreeOID}, err
}
func (m RepositoryMaterializer) worktree(request workflowworker.PhaseRequest) (git.Worktree, error) {
	var id git.Identity
	if m.Store == nil || json.Unmarshal(request.Worktree.IdentityJSON, &id) != nil {
		return git.Worktree{}, ErrRepositoryMaterialization
	}
	canonical, _ := json.Marshal(id)
	if !bytes.Equal(canonical, request.Worktree.IdentityJSON) || id.Worktree != request.Worktree.Path || id.HeadRef != request.Worktree.Branch || id.BaseHead != request.Worktree.BaseSHA {
		return git.Worktree{}, ErrRepositoryMaterialization
	}
	return git.Worktree{Path: request.Worktree.Path, Branch: request.Worktree.Branch, Identity: id}, nil
}
func verificationOutcome(outcome string, exit int) bool {
	return (exit != 0 && (outcome == "red" || outcome == "missing" || outcome == "check_failed")) || (exit == 0 && (outcome == "baseline" || outcome == "dry_run" || outcome == "report_ready"))
}
func equalArgv(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func sameJSON(left, right any) bool {
	a, ae := json.Marshal(left)
	b, be := json.Marshal(right)
	return ae == nil && be == nil && bytes.Equal(a, b)
}
func providerMatches(request workflowworker.PhaseRequest, result store.ProviderAttemptResult, key store.ProviderAttemptResultKey, phase domain.Phase, role string) bool {
	claim := result.Claim
	return key.Ref == request.Ticket.Ref && key.AttemptID == result.AttemptID && key.Attempt == claim.Attempt && key.Phase == phase && claim.Ref == request.Ticket.Ref && claim.Phase == phase && claim.Role == role && claim.Worktree == request.Worktree.Path && claim.WorktreeIdentity == string(request.Worktree.IdentityJSON) && claim.BaseSHA == request.Worktree.BaseSHA
}
func containsPath(paths []string, path string) bool {
	for _, p := range paths {
		if p == path || strings.HasPrefix(path, strings.TrimSuffix(p, "/")+"/") {
			return true
		}
	}
	return false
}
func (m RepositoryMaterializer) settleCommitFailure(_ context.Context, claim contracts.GitMutationClaim) error {
	persistCtx, cancel := materializerPersistenceContext()
	defer cancel()
	facts, err := m.Store.GitMutationIntentFacts(persistCtx, claim.SemanticKey)
	if err != nil || facts.Claim != claim {
		if err == nil {
			return ErrRepositoryMaterialization
		}
		return err
	}
	fence := store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}
	if facts.PreparedCommitOID != "" || facts.PreparedTreeOID != "" {
		// A recorded prepared tuple means update-ref may have crossed. Preserve
		// it for the startup observer rather than retrying a potentially visible
		// mutation in-process.
		_, err = m.Store.MarkEffectUncertain(persistCtx, fence)
		return err
	}
	// No visible ref mutation can precede RecordPreparedCommit. Retire both the
	// exact child intent and effect so the deterministic key can be issued
	// again; merely marking the effect failed leaves the unique intent behind.
	return m.Store.RetireUnpreparedGitCommit(persistCtx, claim)
}

func materializerPersistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func commitDigest(kind string, request workflowworker.PhaseRequest, provider store.ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey, resultDigest string, value any) string {
	return store.CanonicalRepositoryCommitDigest(kind, request.Ticket.Ref, request.Ticket.Version, request.Fence, request.Worktree, provider, command, resultDigest, value)
}

// verificationCheckpointCommitDigest freezes source-resume commit identity at
// the authenticated source endpoint. Later daemon leader/runner fences are
// intentionally excluded: they authorize observation/rebind, never a second
// checkpoint mutation.
func (m RepositoryMaterializer) verificationCheckpointCommitDigest(ctx context.Context, request workflowworker.PhaseRequest, provider store.ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey, resultDigest string, artifact phaseartifact.Verification) (string, error) {
	proof, found, err := m.Store.OperatorSourceResumeProof(ctx, request.Ticket.Ref, request.Ticket.Version, request.Fence)
	if err != nil {
		return "", ErrRepositoryMaterialization
	}
	if !found {
		if request.Amendment != nil {
			return verificationAmendmentCheckpointCommitDigest(request, provider, command, resultDigest, artifact), nil
		}
		return commitDigest("verification-checkpoint", request, provider, command, resultDigest, artifact), nil
	}
	return store.CanonicalOperatorSourceResumeCheckpointDigest(store.OperatorSourceResumeCheckpointDigestInput{
		Ref: request.Ticket.Ref, WorktreePath: proof.Worktree.Path, Branch: proof.Worktree.Branch,
		Identity: proof.Worktree.IdentityJSON, BaseSHA: proof.Worktree.BaseSHA, Source: proof.SourceCommit,
		Retained: proof.Verification.Revision, Provider: provider, Command: command,
		ResultDigest: resultDigest, Artifact: artifact,
	})
}

// verificationAmendmentCheckpointCommitDigest is stable across the live
// verifying fence. An amendment can be recovered after Git has confirmed its
// checkpoint but before RecordVerification or the decision transition. The
// durable amendment request and immutable Reviewer result identify the same
// child commit; a new daemon fence must not manufacture another mutation.
func verificationAmendmentCheckpointCommitDigest(request workflowworker.PhaseRequest, provider store.ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey, resultDigest string, artifact phaseartifact.Verification) string {
	amendment := request.Amendment
	data, _ := json.Marshal(struct {
		Kind              string
		Ref               domain.TicketRef
		TransitionVersion uint64
		ConsumedVersion   uint64
		Prior             store.VerificationRevision
		Builder           store.ProviderAttemptResultKey
		BuilderTypedSHA   string
		ProposedDigest    string
		ProposedCommand   []string
		Reason            string
		Requester         string
		BudgetRequestID   string
		WorktreePath      string
		WorktreeBranch    string
		WorktreeIdentity  []byte
		BaseSHA           string
		Provider          store.ProviderAttemptResultKey
		Command           contracts.RepositoryCommandResultKey
		ResultDigest      string
		Artifact          phaseartifact.Verification
	}{"verification-amendment-checkpoint/v1", request.Ticket.Ref, amendment.TransitionTicketVersion, amendment.ConsumedVersion, amendment.Prior, amendment.BuilderResult, amendment.BuilderTypedSHA256, amendment.ProposedDigest, amendment.ProposedCommand, amendment.Reason, amendment.Requester, amendment.BudgetRequestID, request.Worktree.Path, request.Worktree.Branch, request.Worktree.IdentityJSON, request.Worktree.BaseSHA, provider, command, resultDigest, artifact})
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}
func materializeErr(err error) error {
	if err != nil {
		return err
	}
	return ErrRepositoryMaterialization
}

var _ workflowworker.VerificationCheckpointMaterializer = RepositoryMaterializer{}
var _ workflowworker.CheckpointAuthenticator = RepositoryMaterializer{}
var _ workflowworker.CandidateMaterializer = RepositoryMaterializer{}
var _ workflowworker.CandidateAuthenticator = RepositoryMaterializer{}
