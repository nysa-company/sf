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
}

func (m RepositoryMaterializer) MaterializeVerificationCheckpoint(ctx context.Context, request workflowworker.PhaseRequest, artifact phaseartifact.Verification, key store.ProviderAttemptResultKey) (workflowworker.VerificationCheckpoint, error) {
	if m.Store == nil || request.Phase != "verification" {
		return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || !providerMatches(request, provider, key, domain.PhaseVerification, "reviewer") || parsed.Verify == nil || !sameJSON(*parsed.Verify, artifact) {
		return workflowworker.VerificationCheckpoint{}, ErrRepositoryMaterialization
	}
	command, result, err := m.runCommand(ctx, request, store.RepositoryCommandPurposePrebuildVerification, key, artifact, "")
	if err != nil || !verificationOutcome(artifact.PrebuildOutcome, result.Result.ExitCode) {
		return workflowworker.VerificationCheckpoint{}, materializeErr(err)
	}
	observation, err := m.commit(ctx, request, request.Worktree.BaseSHA, artifact.OwnedFiles, nil, commitDigest("verification-checkpoint", request, key, command, result.ResultDigest, artifact))
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	return workflowworker.VerificationCheckpoint{ID: observation.CommitOID, Commit: observation, CommandResult: command}, nil
}

func (m RepositoryMaterializer) AuthenticateVerificationCheckpoint(ctx context.Context, request workflowworker.PhaseRequest, _ phaseartifact.Verification, checkpoint workflowworker.VerificationCheckpoint) error {
	if m.Store == nil || checkpoint.ID == "" || checkpoint.ID != checkpoint.Commit.CommitOID || checkpoint.CommandResult.SemanticKey == "" || checkpoint.CommandResult.ClaimEpoch == 0 {
		return ErrRepositoryMaterialization
	}
	observed, err := m.observe(ctx, request)
	if err != nil || observed != checkpoint.Commit || observed.ParentOID != request.Worktree.BaseSHA {
		return ErrRepositoryMaterialization
	}
	_, err = m.Store.LoadRepositoryCommandResult(ctx, checkpoint.CommandResult)
	return err
}

func (m RepositoryMaterializer) MaterializeCandidate(ctx context.Context, request workflowworker.PhaseRequest, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, key store.ProviderAttemptResultKey) (workflowworker.CandidateWitness, error) {
	if m.Store == nil || request.Phase != "build" || request.Verification == nil {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	provider, parsed, err := m.Store.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || !providerMatches(request, provider, key, domain.PhaseBuild, "builder") || parsed.Builder == nil || !sameJSON(*parsed.Builder, builder) {
		return workflowworker.CandidateWitness{}, ErrRepositoryMaterialization
	}
	command, result, err := m.runCommand(ctx, request, store.RepositoryCommandPurposePostbuildCandidate, key, phaseartifact.Verification{}, verification.CheckpointID)
	if err != nil || result.Result.ExitCode != 0 {
		return workflowworker.CandidateWitness{}, materializeErr(err)
	}
	allowed, err := m.currentCandidatePlanScope(ctx, request, plan)
	if err != nil {
		return workflowworker.CandidateWitness{}, err
	}
	// The builder may not overwrite proof ownership outside the accepted plan.
	if err := candidateChangedFilesWithinScope(builder.ChangedFiles, allowed); err != nil {
		return workflowworker.CandidateWitness{}, err
	}
	observation, err := m.commit(ctx, request, verification.CheckpointID, allowed, verification.OwnedFiles, commitDigest("candidate", request, key, command, result.ResultDigest, struct {
		Plan         workflowprompt.PlanIdentity
		Verification workflowprompt.VerificationIdentity
		Builder      phaseartifact.Builder
	}{plan, verification, builder}))
	if err != nil {
		return workflowworker.CandidateWitness{}, err
	}
	return workflowworker.CandidateWitness{Commit: observation, CommandPolicyDigest: strings.TrimPrefix(result.Claim.PolicyDigest, "sha256:"), Reason: "authenticated post-build verification", CommandResult: command}, nil
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
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, ErrRepositoryMaterialization
	}
	effective, err := decodeTicketConfig(request.Ticket)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, err
	}
	argv := append([]string(nil), effective.Commands.Verify.Argv...)
	worktree, err := m.worktree(request)
	if err != nil || effective.Repository == "" || effective.Repository != worktree.Identity.Repository || effective.BaseBranch != worktree.Identity.BaseRef {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, ErrRepositoryMaterialization
	}
	if purpose == store.RepositoryCommandPurposePrebuildVerification && !equalArgv(argv, verification.Command) {
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, ErrRepositoryMaterialization
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
	executable, executableDigest, err := processsupervisor.RepositoryExecutableIdentity(argv[0])
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
		return contracts.RepositoryCommandResultKey{}, store.RepositoryCommandResult{}, ErrRepositoryMaterialization
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

func (m RepositoryMaterializer) commit(ctx context.Context, request workflowworker.PhaseRequest, parent string, paths, protected []string, evidence string) (store.CommitObservation, error) {
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
	// A response can be lost after update-ref.  Re-observe the exact prepared
	// tuple before considering a new mutation; an executing effect is never a
	// license to run Commit a second time.
	if facts, factsErr := m.Store.GitMutationIntentFacts(ctx, intent.SemanticKey); factsErr == nil {
		if facts.Claim != (contracts.GitMutationClaim{}) && (facts.Effect.State == store.EffectExecuting || facts.Effect.State == store.EffectConfirmed) {
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
	observed, err := runner.ObserveCommit(ctx, worktree)
	if err != nil {
		return store.CommitObservation{}, err
	}
	if _, err = m.Store.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}); err != nil {
		return store.CommitObservation{}, err
	}
	return store.CommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}, nil
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
	return key.Ref == request.Ticket.Ref && key.AttemptID == result.AttemptID && key.Attempt == claim.Attempt && key.Phase == phase && claim.Ref == request.Ticket.Ref && claim.Phase == phase && claim.Role == role && claim.ExpectedVersion == request.Ticket.Version && claim.LeaderEpoch == request.Fence.LeaderEpoch && claim.RunnerEpoch == request.Fence.RunnerEpoch && claim.Worktree == request.Worktree.Path && claim.WorktreeIdentity == string(request.Worktree.IdentityJSON) && claim.BaseSHA == request.Worktree.BaseSHA
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
	data, _ := json.Marshal(struct {
		Kind         string
		Ref          any
		Version      uint64
		Fence        domain.Fence
		Worktree     store.StoredWorktree
		Provider     store.ProviderAttemptResultKey
		Command      contracts.RepositoryCommandResultKey
		ResultDigest string
		Value        any
	}{kind, request.Ticket.Ref, request.Ticket.Version, request.Fence, request.Worktree, provider, command, resultDigest, value})
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
