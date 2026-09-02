package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/events"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/leader"
	"github.com/nysa-company/sf/internal/operator"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/transport"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type testIDs struct{ next int }

func (g *testIDs) NewTicketID(domain.Channel) (domain.TicketID, error) {
	g.next++
	return domain.TicketID(fmt.Sprintf("SF-test-%d", g.next)), nil
}

type testClock struct{ now time.Time }

func (c testClock) Now() time.Time { return c.now }

type testRuntimeController struct {
	drain func(context.Context, domain.TicketRef) (bool, error)
	merge func(context.Context, domain.TicketRef) (bool, error)
}

type testRuntimeRetirementController struct {
	testRuntimeController
	retire func(context.Context, domain.TicketRef) error
}

type takeoverRuntimeController struct {
	testRuntimeController
	inspection contracts.TakeoverInspection
	inspectErr error
	rearms     int
}

type retryRuntimeController struct {
	testRuntimeController
	needed    bool
	stateErr  error
	replay    store.GuardedMergeRetryReplayState
	replayErr error
	rearmErr  error
	rearms    int
}

func (controller *takeoverRuntimeController) InspectTakeover(context.Context, domain.TicketRef) (contracts.TakeoverInspection, error) {
	return controller.inspection, controller.inspectErr
}

func (controller *takeoverRuntimeController) Rearm(context.Context, domain.TicketRef) error {
	controller.rearms++
	return nil
}

func (controller *retryRuntimeController) Rearm(context.Context, domain.TicketRef) error {
	controller.rearms++
	return controller.rearmErr
}

func (controller *retryRuntimeController) RuntimeRearmNeeded(context.Context, domain.TicketRef) (bool, error) {
	return controller.needed, controller.stateErr
}

func (controller *retryRuntimeController) GuardedMergeRetryReplay(context.Context, domain.TicketRef) (store.GuardedMergeRetryReplayState, error) {
	return controller.replay, controller.replayErr
}

type daemonRuntimeEnsure struct{}

func (daemonRuntimeEnsure) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	return store.StoredWorktree{Path: "/tmp/daemon-runtime-worktree", State: "registered"}, nil
}

type daemonRuntimeWorker struct {
	mu      sync.Mutex
	calls   map[domain.TicketRef]int
	active  map[domain.TicketRef]bool
	onExit  func(domain.TicketRef)
	entered chan domain.TicketRef
	exited  chan domain.TicketRef
}

func newDaemonRuntimeWorker() *daemonRuntimeWorker {
	return &daemonRuntimeWorker{calls: make(map[domain.TicketRef]int), active: make(map[domain.TicketRef]bool), entered: make(chan domain.TicketRef, 8), exited: make(chan domain.TicketRef, 8)}
}

func (worker *daemonRuntimeWorker) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	worker.mu.Lock()
	worker.calls[ref]++
	worker.active[ref] = true
	worker.mu.Unlock()
	worker.entered <- ref
	<-ctx.Done()
	worker.mu.Lock()
	delete(worker.active, ref)
	onExit := worker.onExit
	worker.mu.Unlock()
	if onExit != nil {
		onExit(ref)
	}
	worker.exited <- ref
	return workflowworker.RunResult{Ref: ref}, ctx.Err()
}

func (worker *daemonRuntimeWorker) snapshot(ref domain.TicketRef) (calls int, active bool) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.calls[ref], worker.active[ref]
}

func (worker *daemonRuntimeWorker) setOnExit(onExit func(domain.TicketRef)) {
	worker.mu.Lock()
	worker.onExit = onExit
	worker.mu.Unlock()
}

type daemonCIPolicyObserver struct {
	observation contracts.CIRequiredCheckPolicyObservation
}

func (o daemonCIPolicyObserver) ObserveCIRequiredCheckPolicy(context.Context, contracts.PullRequestIdentity) (contracts.CIRequiredCheckPolicyObservation, error) {
	return o.observation, nil
}

var daemonFixtureCommandPID int64 = 20_000

func daemonFixtureDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}

func daemonFixtureIdentity(t *testing.T, repository, worktree, branch, base string) []byte {
	t.Helper()
	value, err := json.Marshal(gitboundary.Identity{
		Repository: repository, RepositoryDev: 1, RepositoryIno: 2,
		Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4,
		GitFile: "gitdir: " + worktree + "/.git", GitFileDev: 5, GitFileIno: 6,
		CommonDir: repository + "/.git", CommonDirDev: 7, CommonDirIno: 8,
		Origin: "https://github.com/acme/app.git", PushOrigin: "git@github.com:acme/app.git", PushOriginDev: 9, PushOriginIno: 10,
		BaseRef: base, BaseHead: strings.Repeat("a", 40), HeadRef: branch,
		ConfigHash: strings.Repeat("b", 64), HooksHash: strings.Repeat("c", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func daemonFixtureQualification(channel domain.Channel, runID, provider, family string) store.ProviderQualification {
	return store.ProviderQualification{
		Channel: channel, RunID: runID,
		Provider:     domain.ProviderIdentity{Provider: provider, Model: provider + "-model", Family: family, Version: "1.0.0"},
		BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64),
		Profile: store.QualificationGuarded, CreatedAt: time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC),
	}
}

func daemonFixtureBinding(q store.ProviderQualification) contracts.RuntimeBinding {
	return contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: daemonFixtureDigest("auth:" + q.Provider.Provider)}
}

func daemonFixtureDrainProof(t *testing.T, signer *contracts.DrainSigner, claim store.ProviderAttemptClaim) contracts.DrainProof {
	t.Helper()
	proof, err := signer.ProveDrained(contracts.DrainRequest{
		ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt,
		LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey,
		BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest,
		AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree,
		WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return proof
}

func daemonFixtureCompleteCommand(t *testing.T, database *store.Store, ref domain.TicketRef, version uint64, fence domain.Fence, purpose string, provider store.ProviderAttemptResultKey, intent, proof, checkpoint, policy string) contracts.RepositoryCommandResultKey {
	t.Helper()
	project, err := database.Project(t.Context(), ref.Channel, ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := database.Worktree(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := config.DecodeSnapshot(ticket.ConfigSnapshot, ticket.ConfigDigest)
	if err != nil || effective.Commands.Verify.Validate("verify") != nil {
		t.Fatalf("decode fixture verify command: %v", err)
	}
	argv, err := json.Marshal(effective.Commands.Verify.Argv)
	if err != nil {
		t.Fatal(err)
	}
	commandDigest := "sha256:" + daemonFixtureDigest(string(argv))
	if policy == "" {
		policy = "sha256:" + strings.Repeat("1", 64)
	}
	request := store.RepositoryCommandEvidenceRequest{
		Purpose: purpose, Ref: ref, TicketVersion: version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ProviderResult: provider,
		VerificationIntentDigest: intent, ProofDigest: proof, CheckpointID: checkpoint, ConfigCommandDigest: commandDigest,
		Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, PolicyDigest: policy,
		SpecDigest: "sha256:" + strings.Repeat("2", 64), ExecutablePath: "/usr/bin/true", ExecutableDigest: "sha256:" + strings.Repeat("3", 64),
	}
	if purpose == store.RepositoryCommandPurposePrebuildVerification {
		request.CheckpointID = ""
	}
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(request)
	if err != nil {
		t.Fatal(err)
	}
	command := store.RepositoryCommandIntent{
		EffectFence: store.EffectFence{SemanticKey: semantic, Ref: ref, TicketVersion: version, Fence: fence}, RequestDigest: requestDigest,
		Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), Branch: worktree.Branch, BaseRef: project.BaseRef, BaseSHA: worktree.BaseSHA,
		CommandDigest: commandDigest, SpecDigest: request.SpecDigest, PolicyDigest: policy, ExecutablePath: request.ExecutablePath, ExecutableDigest: request.ExecutableDigest,
	}
	if _, err := database.PlanEffect(t.Context(), store.EffectPlan{SemanticKey: semantic, Ref: ref, Kind: "repository_command", TicketVersion: version, Fence: fence, RequestDigest: requestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.IssueRepositoryCommandClaim(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireRepositoryCommand(t.Context(), claim)
	if err != nil {
		t.Fatal(err)
	}
	pid := int(atomic.AddInt64(&daemonFixtureCommandPID, 1))
	launch := contracts.RepositoryCommandLaunch{PID: pid, PGID: pid, BootIdentity: "fixture", ProcessStartIdentity: fmt.Sprintf("fixture-%d", pid)}
	if err := lease.RecordRepositoryCommandLaunch(t.Context(), launch); err != nil {
		t.Fatal(err)
	}
	if err := lease.FinishRepositoryCommandLaunch(t.Context(), launch); err != nil {
		t.Fatal(err)
	}
	if err := database.CompleteRepositoryCommand(t.Context(), claim, contracts.CommandResult{ExitCode: map[string]int{store.RepositoryCommandPurposePrebuildVerification: 1, store.RepositoryCommandPurposePostbuildCandidate: 0}[purpose], Duration: time.Millisecond, Observed: true, ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	return contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}
}

// prepareDaemonGuardedMergeRetry creates the minimum complete public-Store
// provenance required for a guarded operator retry. It never invokes a GitHub,
// Git, or provider adapter: the immutable observations are explicit fixtures.
func prepareDaemonGuardedMergeRetry(t *testing.T, daemon *Daemon, ticketID domain.TicketID) store.Ticket {
	t.Helper()
	ctx := t.Context()
	ref := domain.TicketRef{Channel: daemon.channel, Project: "demo", Ticket: ticketID}
	project, err := daemon.store.Project(ctx, ref.Channel, ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.store.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: daemonFixtureDigest("source:" + string(ticketID)), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.StartOrAdopt(ctx, ref, ticket.Version, "guarded-merge-retry-fixture", domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: ticket.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	branch := "sf/" + string(ref.Channel) + "/" + daemonFixtureDigest(string(ref.Project))[:16] + "/" + daemonFixtureDigest(string(ref.Ticket))[:16] + "-" + strings.Repeat("b", 32)
	base := strings.Repeat("a", 40)
	worktreePath := filepath.Join(project.Path, string(ticketID))
	identity := daemonFixtureIdentity(t, project.Path, worktreePath, branch, project.BaseRef)
	fence := domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: ticket.RunnerEpoch}
	branchKey := string(ref.Channel) + "\x00" + string(ref.Project) + "\x00" + string(ref.Ticket)
	if _, err := daemon.store.LoadOrStoreBranchUnderFence(ctx, branchKey, branch, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	if err := daemon.store.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Path: worktreePath, Branch: branch, IdentityJSON: identity, BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	builder, _, err := daemon.store.RecordProviderQualification(ctx, daemonFixtureQualification(daemon.channel, strings.Repeat("a", 32), "cursor", "cursor-family"))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := daemon.store.RecordProviderQualification(ctx, daemonFixtureQualification(daemon.channel, strings.Repeat("b", 32), "claude", "claude-family"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := daemon.store.SelectProviderPair(ctx, daemon.channel, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	launch := func(phase domain.Phase, role string, binding contracts.RuntimeBinding, raw []byte, validation phaseartifact.Validation, expectedHead, expectedProof string) store.ProviderAttemptClaim {
		request := store.ProviderAttemptRequest{
			Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
			ExpectedHead: expectedHead, ExpectedProof: expectedProof, Repository: project.Path, Worktree: worktreePath, WorktreeIdentity: string(identity), BaseSHA: base, SupervisorKey: signer.PublicKey(),
			Input: contracts.PhaseInput{Ticket: ref, Phase: phase, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ExpectedVersion: ticket.Version, Prompt: "guarded merge fixture", Repository: project.Path, Worktree: worktreePath, WorktreeIdentity: string(identity), BaseSHA: base, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)},
		}
		claim, err := daemon.store.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatalf("begin %s/%s: %v", phase, role, err)
		}
		if err := daemon.store.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "fixture", ProcessStartIdentity: fmt.Sprintf("fixture-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatal(err)
		}
		if _, err := daemon.store.CompleteProviderAttemptSuccess(ctx, claim, daemonFixtureDrainProof(t, signer, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
			t.Fatalf("complete %s/%s: %v", phase, role, err)
		}
		return claim
	}
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"fixture acceptance"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test"}, Details: "fixture proof"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"fixture"}}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planner := launch(domain.PhasePlanning, "planner", daemonFixtureBinding(builder), planRaw, phaseartifact.Validation{TicketType: ticket.Type}, "", "")
	planKey := store.ProviderAttemptResultKey{AttemptID: planner.ID, Ref: ref, Phase: domain.PhasePlanning, Attempt: planner.Attempt}
	if _, err := daemon.store.RecordPlan(ctx, store.PlanArtifact{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Document: store.PlanDocument{Planner: &plan, ProviderResult: &planKey, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.store.TransitionPlan(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	planIdentity, err := workflowprompt.NewPlanIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	verification := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: daemonFixtureDigest("verification")}
	verificationRaw, err := json.Marshal(verification)
	if err != nil {
		t.Fatal(err)
	}
	verified := launch(domain.PhaseVerification, "reviewer", daemonFixtureBinding(reviewer), verificationRaw, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: planIdentity.Digest}, "", "")
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(verification)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := workflowprompt.CanonicalVerificationProofBytes(verification)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := strings.Repeat("c", 40)
	verificationKey := store.ProviderAttemptResultKey{AttemptID: verified.ID, Ref: ref, Phase: domain.PhaseVerification, Attempt: verified.Attempt}
	verificationCommand := daemonFixtureCompleteCommand(t, daemon.store, ref, ticket.Version, fence, store.RepositoryCommandPurposePrebuildVerification, verificationKey, daemonFixtureDigest(string(intent)), daemonFixtureDigest(string(proof)), "", "")
	if _, err := daemon.store.RecordVerification(ctx, store.VerificationArtifact{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proof, OwnedFiles: verification.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &verificationKey, Checkpoint: store.CommitObservation{CommitOID: checkpoint, ParentOID: base, TreeOID: strings.Repeat("d", 40)}, CommandResult: verificationCommand}); err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.store.TransitionVerification(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"fixture","changed_files":["internal/x.go"],"commands":[["go","test"]]}`)
	built := launch(domain.PhaseBuild, "builder", daemonFixtureBinding(builder), builderRaw, phaseartifact.Validation{TicketType: ticket.Type}, "", "")
	builtKey := store.ProviderAttemptResultKey{AttemptID: built.ID, Ref: ref, Phase: domain.PhaseBuild, Attempt: built.Attempt}
	_, parsed, err := daemon.store.LoadHistoricalProviderAttemptResult(ctx, builtKey)
	if err != nil || parsed.Builder == nil {
		t.Fatalf("load fixture builder result: %v", err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	policy := daemonFixtureDigest("candidate-policy")
	snapshot := domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: strings.Repeat("e", 40), TreeSHA: strings.Repeat("f", 40), SourceDigest: daemonFixtureDigest("source:" + string(ticketID)), VerificationIntentDigest: daemonFixtureDigest(string(intent)), ProofDigest: daemonFixtureDigest(string(proof)), CommandPolicyDigest: policy, BuilderEvidenceDigest: builderDigest}
	candidateCommand := daemonFixtureCompleteCommand(t, daemon.store, ref, ticket.Version, fence, store.RepositoryCommandPurposePostbuildCandidate, builtKey, snapshot.VerificationIntentDigest, snapshot.ProofDigest, checkpoint, "sha256:"+policy)
	if _, err := daemon.store.RecordCandidate(ctx, store.CandidateEvidence{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Snapshot: snapshot, BuilderResult: builtKey, Commit: store.CommitObservation{CommitOID: snapshot.HeadSHA, ParentOID: checkpoint, TreeOID: snapshot.TreeSHA}, Reason: "fixture", CommandResult: candidateCommand}); err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.store.TransitionCandidate(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, snapshot); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := daemon.store.RecoverableCandidate(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := daemon.store.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	pr := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 42, HeadOwner: "acme", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: project.BaseRef, BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	publication := store.PublishedCandidateEvidence{Ref: ref, TicketVersion: ticket.Version, Fence: fence, Candidate: candidate, ConfigGeneration: ticket.ConfigGeneration, ConfigDigest: ticket.ConfigDigest, ConfigSnapshotDigest: daemonFixtureDigest(string(ticket.ConfigSnapshot)), Worktree: worktree, RemoteBranchRef: worktree.Branch, RemoteBranchOID: candidate.Snapshot.HeadSHA, RemoteBaseOID: candidate.Snapshot.BaseSHA, PullRequest: pr, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}
	publication.PushEffect = store.PublicationEffectEvidence{SemanticKey: "fixture-push-" + string(ticketID), Kind: store.PublicationPushEffectKind, RequestDigest: strings.Repeat("1", 64), ClaimEpoch: 1, ObservedIdentity: store.CanonicalPublicationPushObservation(publication.RemoteBranchRef, publication.RemoteBranchOID)}
	publication.PRCreateOrUpdateEffect = store.PublicationEffectEvidence{SemanticKey: "fixture-pr-" + string(ticketID), Kind: store.PublicationPRCreateEffectKind, RequestDigest: "sha256:" + strings.Repeat("2", 64), ClaimEpoch: 1, ObservedIdentity: store.CanonicalPublicationPRObservation(pr, "OPEN", true)}
	for _, effect := range []store.PublicationEffectEvidence{publication.PushEffect, publication.PRCreateOrUpdateEffect} {
		if _, err := daemon.store.PlanEffect(ctx, store.EffectPlan{SemanticKey: effect.SemanticKey, Ref: ref, Kind: effect.Kind, TicketVersion: ticket.Version, Fence: fence, RequestDigest: effect.RequestDigest}); err != nil {
			t.Fatal(err)
		}
		claim, err := daemon.store.ClaimEffect(ctx, store.EffectFence{SemanticKey: effect.SemanticKey, Ref: ref, TicketVersion: ticket.Version, Fence: fence})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := daemon.store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: effect.SemanticKey, Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, effect.ObservedIdentity); err != nil {
			t.Fatal(err)
		}
	}
	if err := daemon.store.RecordPublishedCandidate(ctx, publication); err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.store.TransitionPublishedCandidate(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	published, err := daemon.store.LoadPublishedCandidate(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	policyObserver := daemonCIPolicyObserver{observation: contracts.CIRequiredCheckPolicyObservation{PullRequest: pr, ProtectedBranchRef: pr.BaseRef, ProtectedBranchOID: pr.BaseOID, PolicySourceDigest: strings.Repeat("a", 64), AuthenticatedPrincipal: "fixture", RequiredChecks: []contracts.RequiredCheck{{Name: "unit", ExternalID: "run-1", State: "success"}}, ObservedAt: time.Now().UTC()}}
	if err := daemon.store.RecordCIRequiredCheckPolicyFromObserver(ctx, ref, policyObserver); err != nil {
		t.Fatal(err)
	}
	observation := store.CIObservation{Ref: ref, CandidateGeneration: candidate.Snapshot.Generation, CandidateHeadSHA: candidate.Snapshot.HeadSHA, CandidateTreeSHA: candidate.Snapshot.TreeSHA, PublicationWitnessDigest: published.WitnessDigest, PullRequest: pr, ObservedTicketVersion: ticket.Version, ObservedFence: fence, ObservedAt: time.Now().UTC(), RequiredChecks: []store.CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-1", NormalizedState: "success"}}, Classification: "green"}
	if err := daemon.store.RecordCIObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	observation, err = daemon.store.LoadCurrentCIObservation(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.store.ConsumeCIObservation(ctx, store.CIObservationTransition{Ref: ref, ObservationDigest: observation.ObservationDigest, ExpectedVersion: ticket.Version, Fence: fence}); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	reviewed := launch(domain.PhaseReview, "reviewer", daemonFixtureBinding(reviewer), []byte(fmt.Sprintf(`{"schema":"sf.reviewer/v1","decision":"pass","repair_owner":"","findings":[],"reviewed_head":"%s","proof_digest":"%s"}`, candidate.Snapshot.HeadSHA, candidate.Snapshot.ProofDigest)), phaseartifact.Validation{TicketType: ticket.Type, ExpectedReviewedHead: candidate.Snapshot.HeadSHA, ExpectedProofDigest: candidate.Snapshot.ProofDigest}, candidate.Snapshot.HeadSHA, candidate.Snapshot.ProofDigest)
	if reviewed.ID == 0 {
		t.Fatal("final review did not create a provider attempt")
	}
	if _, err := daemon.store.TransitionFinalReview(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	if _, err := daemon.store.ApplyOperatorDecision(ctx, store.OperatorDecisionRequest{OperatorDecision: store.OperatorDecision{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, ReviewedHead: candidate.Snapshot.HeadSHA, OperatorUID: 501, Decision: "approved"}}); err != nil {
		t.Fatal(err)
	}
	ticket, err = daemon.store.Ticket(ctx, ref)
	if err != nil || ticket.State != domain.StateMerging {
		t.Fatalf("guarded merge fixture=%+v err=%v", ticket, err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	publication, err = daemon.store.LoadHistoricalPublishedCandidate(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	head := publication.PullRequest.HeadOID
	readyKey := "merge-ready/" + string(ref.Channel) + "/" + string(ref.Project) + "/" + string(ref.Ticket) + "/" + head
	if _, err := daemon.store.PlanEffect(ctx, store.EffectPlan{SemanticKey: readyKey, Ref: ref, Kind: "pr_ready", TicketVersion: ticket.Version, Fence: fence, RequestDigest: "fixture-ready-request"}); err != nil {
		t.Fatal(err)
	}
	readyClaim, err := daemon.store.ClaimEffect(ctx, store.EffectFence{SemanticKey: readyKey, Ref: ref, TicketVersion: ticket.Version, Fence: fence})
	if err != nil || !readyClaim.Claimed {
		t.Fatalf("fixture pr-ready claim=%+v err=%v", readyClaim, err)
	}
	if _, err := daemon.store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: readyKey, Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: readyClaim.Effect.LeaderEpoch, RunnerEpoch: readyClaim.Effect.RunnerEpoch, ClaimEpoch: readyClaim.Effect.ClaimEpoch}}, "ready/"+head); err != nil {
		t.Fatal(err)
	}
	mergeKey := "merge/" + string(ref.Channel) + "/" + string(ref.Project) + "/" + string(ref.Ticket) + "/" + head
	if _, err := daemon.store.PlanEffect(ctx, store.EffectPlan{SemanticKey: mergeKey, Ref: ref, Kind: "merge", TicketVersion: ticket.Version, Fence: fence, RequestDigest: "fixture-merge-request"}); err != nil {
		t.Fatal(err)
	}
	mergeClaim, err := daemon.store.ClaimEffect(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: ticket.Version, Fence: fence})
	if err != nil || !mergeClaim.Claimed {
		t.Fatalf("fixture merge claim=%+v err=%v", mergeClaim, err)
	}
	if err := daemon.store.RecordMergeIntent(ctx, domain.MergeIntent{
		Ref: ref, SemanticKey: mergeKey, RequestDigest: mergeClaim.Effect.RequestDigest,
		TicketVersion: ticket.Version, LeaderEpoch: mergeClaim.Effect.LeaderEpoch, RunnerEpoch: mergeClaim.Effect.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch,
		RepositoryHost: publication.PullRequest.Repository.Host, RepositoryOwner: publication.PullRequest.Repository.Owner, RepositoryName: publication.PullRequest.Repository.Name,
		PullRequestNumber: publication.PullRequest.Number, HeadOwner: publication.PullRequest.HeadOwner, HeadRepository: publication.PullRequest.HeadRepository,
		HeadRef: publication.PullRequest.HeadRef, HeadOID: publication.PullRequest.HeadOID, BaseRef: publication.PullRequest.BaseRef, OriginalBaseOID: publication.PullRequest.BaseOID,
		ProtectionRuleID: "fixture-main", StrictStatusChecks: true, AdminEnforced: true, Method: "squash",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := daemon.store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: mergeClaim.Effect.LeaderEpoch, RunnerEpoch: mergeClaim.Effect.RunnerEpoch, ClaimEpoch: mergeClaim.Effect.ClaimEpoch}}, "merged/"+head); err != nil {
		t.Fatal(err)
	}
	return ticket
}

func (controller testRuntimeController) Drain(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if controller.drain == nil {
		return true, nil
	}
	return controller.drain(ctx, ref)
}

func (controller testRuntimeController) MergeObserved(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if controller.merge == nil {
		return false, nil
	}
	return controller.merge(ctx, ref)
}

func (controller testRuntimeRetirementController) Retire(ctx context.Context, ref domain.TicketRef) error {
	if controller.retire == nil {
		return nil
	}
	return controller.retire(ctx, ref)
}

func testDaemon(t *testing.T) (*Daemon, config.ChannelPaths, context.CancelFunc) {
	return testDaemonForChannel(t, domain.ChannelStable)
}

func testDaemonForChannel(t *testing.T, channel domain.Channel) (*Daemon, config.ChannelPaths, context.CancelFunc) {
	return testDaemonForChannelWithProjectMaximum(t, channel, domain.MergeGuarded)
}

func testDaemonForChannelWithProjectMaximum(t *testing.T, channel domain.Channel, maximum domain.MergeMode) (*Daemon, config.ChannelPaths, context.CancelFunc) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "sfv2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	paths := config.ChannelPaths{
		Root: root, Database: filepath.Join(root, "sf.sqlite"), Socket: filepath.Join(root, "run", "sf.sock"),
		Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "backups"),
	}
	_, thisFile, _, _ := runtime.Caller(0)
	stateMachine := filepath.Join(filepath.Dir(thisFile), "..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	uid := uint32(os.Getuid())
	auth := operator.Authenticator{ExpectedUID: uid, Lookup: func(got string) (operator.Account, error) {
		return operator.Account{Username: "operator", UID: strconv.FormatUint(uint64(uid), 10)}, nil
	}}
	machine := config.DefaultMachineLimits()
	projectConfig := config.DefaultProject("demo", filepath.Join(root, "repo"))
	projectConfig.MergeMode = maximum
	if maximum == domain.MergeAutonomous {
		machine.AllowAutonomous = true
	}
	effective, err := config.Resolve(machine, projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	d, err := Start(context.Background(), Config{
		Channel: channel, Paths: paths, StateMachinePath: stateMachine, DaemonIdentity: "integration-test-" + string(channel),
		Projects:  []store.Project{{Channel: channel, ID: "demo", Path: filepath.Join(root, "repo"), BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}},
		TicketIDs: &testIDs{}, Clock: testClock{now: time.Unix(100, 0).UTC()}, Operator: auth,
		StartupTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	go func() { _ = d.Serve(serveCtx) }()
	t.Cleanup(func() { cancel(); _ = d.Close() })
	return d, paths, cancel
}

func TestDaemonFailureActionsUseTheDaemonChannelExecutable(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		t.Run(string(channel), func(t *testing.T) {
			d := &Daemon{channel: channel}
			binary := "sf"
			if channel == domain.ChannelDev {
				binary = "sf-dev"
			}
			for code, want := range map[string][]string{
				"autonomous_unavailable":                  {binary, "submit", "--help"},
				"runtime_activation_failed":               {binary, "providers", "qualify", "--builder", "codex", "--reviewer", "codex"},
				"runtime_already_active":                  {binary, "daemon", "status"},
				"terminal_replay_requires_new":            {binary, "submit", "--help"},
				"unknown_project":                         {binary, "init", "--help"},
				"invalid_submit":                          {binary, "submit", "--help"},
				"ticket_policy_refused":                   {binary, "submit", "--help"},
				"invalid_logs":                            {binary, "logs", "--help"},
				"not_ready":                               {binary, "--help"},
				"takeover_inspection_failed":              {binary, "take", "--help"},
				"takeover_changes_unadopted":              {binary, "take", "--help"},
				"takeover_verification_changes_unadopted": {binary, "resume", "--help"},
				"takeover_source_out_of_scope":            {binary, "take", "--help"},
				"invalid_resume":                          {binary, "resume", "--help"},
				"invalid_retry":                           {binary, "retry", "--help"},
				"invalid_recover":                         {binary, "recover", "--help"},
				"runtime_rearm_unavailable":               {binary, "resume", "--help"},
				"runtime_rearm_failed":                    {binary, "resume", "--help"},
				"resume_state_unavailable":                {binary, "resume", "--help"},
				"resume_transition_refused":               {binary, "resume", "--help"},
				"retry_state_unavailable":                 {binary, "retry", "--help"},
				"retry_not_available":                     {binary, "retry", "--help"},
				"retry_transition_refused":                {binary, "retry", "--help"},
				"retry_required":                          {binary, "retry", "--help"},
				"recover_mode_refused":                    {binary, "recover", "--help"},
				"recover_transition_refused":              {binary, "recover", "--help"},
				"other":                                   {binary, "doctor"},
			} {
				response := d.failure(api.Request{Version: api.Version, RequestID: "failure-actions", Method: "ticket.submit"}, code, "failed", false)
				if response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join(want, "\x00") {
					t.Fatalf("%s action=%+v want=%+v", code, response.NextAction, want)
				}
				if strings.Contains(strings.Join(response.NextAction.Argv, " "), "<") {
					t.Fatalf("%s action includes a placeholder: %+v", code, response.NextAction.Argv)
				}
				if err := response.Validate(); err != nil {
					t.Fatalf("%s response failed validation: %v", code, err)
				}
			}
			for code, want := range map[string][]string{
				"invalid_control":                         {binary, "pause", "--help"},
				"invalid_ticket_reference":                {binary, "pause", "--help"},
				"invalid_transition":                      {binary, "status", "SF-action"},
				"external_merge_observed":                 {binary, "status", "SF-action"},
				"external_state_unavailable":              {binary, "status", "SF-action"},
				"blocked_process":                         {binary, "status", "SF-action"},
				"uncertain_effect":                        {binary, "status", "SF-action"},
				"takeover_changes_unadopted":              {binary, "take", "SF-action"},
				"takeover_verification_changes_unadopted": {binary, "resume", "SF-action"},
				"takeover_source_out_of_scope":            {binary, "take", "SF-action"},
				"takeover_inspection_failed":              {binary, "take", "SF-action"},
				"retry_required":                          {binary, "retry", "SF-action"},
				"invalid_resume":                          {binary, "resume", "--help"},
				"invalid_retry":                           {binary, "retry", "--help"},
				"invalid_recover":                         {binary, "recover", "--help"},
				"runtime_rearm_unavailable":               {binary, "resume", "SF-action"},
				"runtime_rearm_failed":                    {binary, "resume", "SF-action"},
				"resume_state_unavailable":                {binary, "resume", "SF-action"},
				"resume_transition_refused":               {binary, "resume", "SF-action"},
				"retry_state_unavailable":                 {binary, "retry", "SF-action"},
				"retry_not_available":                     {binary, "retry", "SF-action"},
				"retry_transition_refused":                {binary, "retry", "SF-action"},
				"recover_mode_refused":                    {binary, "recover", "SF-action"},
				"recover_transition_refused":              {binary, "recover", "SF-action"},
			} {
				request := api.Request{Version: api.Version, RequestID: "control-actions", Method: "ticket.pause", Ticket: "SF-action"}
				response := d.failure(request, code, "failed", false)
				if response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join(want, "\x00") {
					t.Fatalf("%s control action=%+v want=%+v", code, response.NextAction, want)
				}
			}
		})
	}
}

func TestLeaseCapacitiesUseAuthenticatedProjectSnapshot(t *testing.T) {
	machine := config.DefaultMachineLimits()
	machine.MaxConcurrentTickets = 4
	projectConfig := config.DefaultProject("demo", "/tmp/demo")
	projectConfig.MaxConcurrentTickets = 3
	effective, err := config.Resolve(machine, projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	project := store.Project{Channel: domain.ChannelDev, ID: "demo", Path: "/tmp/demo", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}
	global, perProject, err := leaseCapacities(project)
	if err != nil || global != 4 || perProject != 3 {
		t.Fatalf("global=%d project=%d err=%v", global, perProject, err)
	}
	project.Path = "/tmp/other"
	if _, _, err := leaseCapacities(project); err == nil {
		t.Fatal("configuration snapshot was accepted for another repository")
	}
}

func TestDaemonAdmissionUsesConfiguredTwoTicketDefault(t *testing.T) {
	d, _, _ := testDaemon(t)
	for index := 1; index <= 3; index++ {
		ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "demo", Ticket: domain.TicketID(fmt.Sprintf("SF-capacity-%d", index))}
		if err := d.store.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: fmt.Sprintf("capacity-%d", index), Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
			t.Fatal(err)
		}
		request := api.Request{Version: api.Version, RequestID: fmt.Sprintf("capacity-%d", index), Method: "ticket.start", Ticket: string(ref.Ticket), Parameters: []byte(`{"channel":"stable","project":"demo"}`)}
		response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
		if index <= 2 && !response.OK {
			t.Fatalf("ticket %d was not admitted: %+v", index, response)
		}
		if index == 3 && (response.OK || response.Error == nil || response.Error.Code != "capacity_unavailable") {
			t.Fatalf("third ticket did not hit capacity: %+v", response)
		}
	}
}

func TestDaemonStartRefusesDanglingLegacyConfigurationHistory(t *testing.T) {
	d, paths, _ := testDaemon(t)
	writer, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	// Simulate malformed generation-zero history written by a pre-v51
	// database. Current databases reject this rollback at the trigger, but the
	// daemon must still fail closed while upgrading an already inconsistent
	// legacy database.
	if _, err := writer.ExecContext(context.Background(), `DROP TRIGGER projects_config_generation_forward`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(context.Background(), `UPDATE projects SET current_config_generation=0 WHERE channel=? AND id=?`, d.channel, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(context.Background(), `CREATE TRIGGER projects_config_generation_forward BEFORE UPDATE OF current_config_generation ON projects
		WHEN NEW.current_config_generation != OLD.current_config_generation
		 AND (NEW.current_config_generation != OLD.current_config_generation + 1
		      OR NOT EXISTS (SELECT 1 FROM project_configurations
		                     WHERE channel=NEW.channel AND project_id=NEW.id
		                       AND generation=NEW.current_config_generation))
		BEGIN SELECT RAISE(ABORT,'project configuration generation must advance one step to an existing generation'); END`); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-dangling-legacy-config"}
	if err := d.store.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: "dangling-legacy-config", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	var eventsBefore int
	if err := writer.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "dangling-legacy-start", Method: "ticket.start", Ticket: string(ref.Ticket), Parameters: json.RawMessage(`{"channel":"stable","project":"demo"}`)})
	if response.OK || response.Error == nil || response.Error.Code != "invalid_configuration" || response.Mutation.Attempted {
		t.Fatalf("dangling legacy start response=%+v", response)
	}
	after, err := d.store.Ticket(context.Background(), ref)
	if err != nil || after.State != domain.StateQueued || after.ConfigGeneration != 0 || len(after.ConfigSnapshot) != 0 {
		t.Fatalf("dangling daemon admission mutated ticket=%+v err=%v", after, err)
	}
	leases, err := d.store.Leases(context.Background(), d.channel)
	if err != nil || len(leases) != 0 {
		t.Fatalf("dangling daemon admission leases=%+v err=%v", leases, err)
	}
	var owners, events int
	if err := writer.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM workflow_owners WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&owners); err != nil || owners != 0 {
		t.Fatalf("dangling daemon admission owners=%d err=%v", owners, err)
	}
	if err := writer.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&events); err != nil || events != eventsBefore {
		t.Fatalf("dangling daemon admission events before=%d after=%d err=%v", eventsBefore, events, err)
	}
}

func TestDaemonLogsReturnBoundedRedactedDurableEvents(t *testing.T) {
	d, paths, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-logs")
	_ = createAndStartControlTicket(t, d, "SF-other-logs")
	request := api.Request{Version: api.Version, RequestID: "logs", Method: "ticket.logs", Ticket: string(started.Ref.Ticket), Parameters: json.RawMessage(`{"channel":"stable","phase":"planning","follow":false,"after":0}`)}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if !response.OK {
		t.Fatalf("logs response=%+v", response)
	}
	var page struct {
		Ticket    domain.TicketID `json:"ticket"`
		NextAfter uint64          `json:"next_after"`
		Events    []events.Record `json:"events"`
	}
	if err := json.Unmarshal(response.Data, &page); err != nil {
		t.Fatal(err)
	}
	if page.Ticket != started.Ref.Ticket || page.NextAfter == 0 || len(page.Events) == 0 || len(page.Events) > maxLogItems {
		t.Fatalf("page=%+v", page)
	}
	for _, event := range page.Events {
		if event.Ticket != started.Ref.Ticket || !eventMatchesPhase(store.Event{From: event.From, To: event.To, Payload: string(event.Payload)}, "planning") {
			t.Fatalf("cross-ticket or cross-phase event=%+v", event)
		}
		if strings.Contains(string(event.Payload), paths.Root) {
			t.Fatalf("unredacted channel path in payload=%s", event.Payload)
		}
	}
	request.RequestID = "logs-replay"
	request.Parameters = json.RawMessage(fmt.Sprintf(`{"channel":"stable","phase":"planning","follow":false,"after":%d}`, page.NextAfter))
	replay := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if !replay.OK {
		t.Fatalf("replay=%+v", replay)
	}
	var replayPage struct {
		Events []events.Record `json:"events"`
	}
	if err := json.Unmarshal(replay.Data, &replayPage); err != nil || replayPage.Events == nil || len(replayPage.Events) != 0 {
		t.Fatalf("replay events=%+v err=%v", replayPage.Events, err)
	}
	request.RequestID = "logs-invalid"
	request.Parameters = json.RawMessage(`{"channel":"stable","phase":"unknown"}`)
	invalid := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if invalid.OK || invalid.Error == nil || invalid.Error.Code != "invalid_logs" {
		t.Fatalf("invalid phase response=%+v", invalid)
	}
}

func createAndStartControlTicket(t *testing.T, daemon *Daemon, id domain.TicketID) store.Ticket {
	t.Helper()
	ref := domain.TicketRef{Channel: daemon.channel, Project: "demo", Ticket: id}
	if err := daemon.store.CreateTicket(context.Background(), store.Ticket{Ref: ref, SourceDigest: "control-" + string(id), Type: domain.TicketBug, MergeMode: domain.MergeGuarded, MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	request := api.Request{Version: api.Version, RequestID: "start-" + string(id), Method: "ticket.start", Ticket: string(id), Parameters: json.RawMessage(`{"channel":"` + string(daemon.channel) + `","project":"demo"}`)}
	response := daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if !response.OK {
		t.Fatalf("start response=%+v", response)
	}
	started, err := daemon.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return started
}

func daemonControl(daemon *Daemon, id domain.TicketID, intent string) api.Response {
	return daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: intent + "-" + string(id), Method: "ticket." + intent,
		Ticket: string(id), OperatorLabel: "operator",
		Parameters: json.RawMessage(`{"channel":"` + string(daemon.channel) + `","operator":"operator"}`),
	})
}

func daemonResume(daemon *Daemon, id domain.TicketID) api.Response {
	return daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "resume-" + string(id), Method: "ticket.resume",
		Ticket: string(id), OperatorLabel: "operator",
		Parameters: json.RawMessage(`{"channel":"` + string(daemon.channel) + `","operator":"operator"}`),
	})
}

func TestDaemonFactoryTwoWorkerPauseDrainsOnlyTargetAndResumeRearms(t *testing.T) {
	worker := newDaemonRuntimeWorker()
	cfg, _ := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		runtime, err := workflowruntime.NewRuntimeWithConfig(workflowruntime.NewScheduler(
			domain.ChannelStable,
			workflowruntime.StoreTicketSource{Store: deps.Store},
			daemonRuntimeEnsure{},
			worker,
		), workflowruntime.RuntimeConfig{Interval: time.Millisecond, Workers: 2})
		if err != nil {
			return WorkflowRuntimeComponents{}, err
		}
		controller, err := runtimecontrol.New(deps.Store, runtime.ControlBundle(), runtimecontrol.MergeObserverFunc(func(context.Context, domain.TicketRef) (bool, error) {
			return false, nil
		}))
		if err != nil {
			_ = runtime.Close()
			return WorkflowRuntimeComponents{}, err
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: controller}, nil
	})
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	target := createAndStartControlTicket(t, d, "SF-runtime-target")
	sibling := createAndStartControlTicket(t, d, "SF-runtime-sibling")
	seen := map[domain.TicketRef]bool{}
	for len(seen) != 2 {
		select {
		case ref := <-worker.entered:
			seen[ref] = true
		case <-time.After(time.Second):
			t.Fatalf("runtime did not start both tickets: %v", seen)
		}
	}
	if !seen[target.Ref] || !seen[sibling.Ref] {
		t.Fatalf("worker entries=%v target=%v sibling=%v", seen, target.Ref, sibling.Ref)
	}
	if response := daemonControl(d, target.Ref.Ticket, "pause"); !response.OK {
		t.Fatalf("pause response=%+v", response)
	}
	select {
	case exited := <-worker.exited:
		if exited != target.Ref {
			t.Fatalf("pause stopped sibling %v, want %v", exited, target.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("pause did not join the target worker")
	}
	if calls, active := worker.snapshot(sibling.Ref); calls != 1 || !active {
		t.Fatalf("sibling did not continue through target pause: calls=%d active=%v", calls, active)
	}
	if response := daemonResume(d, target.Ref.Ticket); !response.OK {
		t.Fatalf("resume response=%+v", response)
	}
	select {
	case ref := <-worker.entered:
		if ref != target.Ref {
			t.Fatalf("resume ran %v, want target %v", ref, target.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not rearm and run the target")
	}
	if calls, active := worker.snapshot(target.Ref); calls != 2 || !active {
		t.Fatalf("target did not restart after resume: calls=%d active=%v", calls, active)
	}
	if response := daemonControl(d, target.Ref.Ticket, "cancel"); !response.OK {
		t.Fatalf("terminal cancel response=%+v error=%+v", response, response.Error)
	}
	select {
	case exited := <-worker.exited:
		if exited != target.Ref {
			t.Fatalf("cancel stopped sibling %v, want target %v", exited, target.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal cancel did not join the rearmed target")
	}
	if _, err := d.store.StoppedRuntimeTicket(context.Background(), target.Ref); !errors.Is(err, store.ErrStaleFence) {
		t.Fatalf("terminal cancel retained durable runtime control: %v", err)
	}
	if response := daemonControl(d, target.Ref.Ticket, "cancel"); !response.OK || !response.Mutation.Observed {
		t.Fatalf("terminal cancel retry=%+v", response)
	}
}

func TestDaemonPauseDrainsThenCommitsAndReplays(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-pause")
	var drains int
	d.control = testRuntimeController{drain: func(_ context.Context, ref domain.TicketRef) (bool, error) {
		drains++
		if ref != started.Ref {
			t.Fatalf("drain ref=%+v want=%+v", ref, started.Ref)
		}
		return true, nil
	}}
	response := daemonControl(d, started.Ref.Ticket, "pause")
	if !response.OK || !response.Mutation.Attempted || response.Mutation.Observed {
		t.Fatalf("pause response=%+v", response)
	}
	paused, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != domain.StatePaused || paused.ResumeState != domain.StatePlanning || paused.RunnerEpoch != started.RunnerEpoch+1 || paused.Version != started.Version+2 {
		t.Fatalf("paused=%+v started=%+v", paused, started)
	}
	leases, err := d.store.Leases(context.Background(), d.channel)
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
	events, err := d.store.Events(context.Background(), d.channel, 0, 20)
	if err != nil || len(events) < 3 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	intentEvent := events[len(events)-2]
	if intentEvent.Trigger != "operator_pause_or_take" || !strings.Contains(intentEvent.Payload, `"intent":"pause"`) || !strings.Contains(intentEvent.Payload, `"operator":"operator"`) || !strings.Contains(intentEvent.Payload, `"operator_uid":`) {
		t.Fatalf("control intent event=%+v", intentEvent)
	}
	if drainedEvent := events[len(events)-1]; drainedEvent.Trigger != "process_and_effects_drained" || drainedEvent.Payload != `{"drained":true,"intent":"pause","remote":{"registered":false,"candidate_present":false}}` {
		t.Fatalf("drained event=%+v", drainedEvent)
	}
	replay := daemonControl(d, started.Ref.Ticket, "pause")
	if !replay.OK || !replay.Mutation.Observed || drains != 1 {
		t.Fatalf("replay=%+v drains=%d", replay, drains)
	}
}

func TestDaemonTakeReturnsAuthenticatedWorktreeAndResumeRearmsExactlyOnce(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-take-handoff")
	project, err := d.store.Project(context.Background(), d.channel, started.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Repeat("a", 40)
	path := filepath.Join(d.paths.Worktrees, "demo", string(started.Ref.Ticket))
	branch := "sf/stable/aaaaaaaa/aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := d.store.RegisterWorktree(context.Background(), store.WorktreeRegistration{Ref: started.Ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}, Path: path, Branch: branch, IdentityJSON: daemonRecoveryWorktreeIdentity(project.Path, path, branch, project.BaseRef, base), BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	control := &takeoverRuntimeController{inspection: contracts.TakeoverInspection{Registered: true, Path: path, Branch: branch, Repository: project.Path, BaseSHA: base, HeadSHA: base, Clean: true, ChangeKind: "none", RemoteBaseSHA: base, RemoteIdentityExact: true}}
	d.control = control
	taken := daemonControl(d, started.Ref.Ticket, "take")
	if !taken.OK || taken.Mutation.Kind != "ticket_take" {
		t.Fatalf("take response=%+v", taken)
	}
	var body struct {
		Takeover contracts.TakeoverInspection `json:"takeover"`
	}
	if err := json.Unmarshal(taken.Data, &body); err != nil || body.Takeover.Path != path || body.Takeover.Branch != branch || !body.Takeover.Clean {
		t.Fatalf("take body=%s parsed=%+v err=%v", taken.Data, body, err)
	}
	events, err := d.store.Events(context.Background(), d.channel, 0, 100)
	if err != nil || !strings.Contains(events[len(events)-2].Payload, `"intent":"take"`) {
		t.Fatalf("take events=%+v err=%v", events, err)
	}
	resumed := daemonResume(d, started.Ref.Ticket)
	if !resumed.OK || resumed.Mutation.Observed || control.rearms != 1 {
		t.Fatalf("resume=%+v rearms=%d", resumed, control.rearms)
	}
	replay := daemonResume(d, started.Ref.Ticket)
	if !replay.OK || !replay.Mutation.Observed || control.rearms != 1 {
		t.Fatalf("resume replay=%+v rearms=%d", replay, control.rearms)
	}
}

func TestDaemonEarlyTakeReportsPausedWithoutInventingWorktree(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-take-before-worktree")
	d.control = testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) { return true, nil }}

	taken := daemonControl(d, started.Ref.Ticket, "take")
	if !taken.OK || !taken.Mutation.Attempted || taken.Mutation.Observed || taken.Mutation.Kind != "ticket_take" {
		t.Fatalf("early take response=%+v", taken)
	}
	var body struct {
		State       domain.State `json:"state"`
		ResumeState domain.State `json:"resume_state"`
		Takeover    struct {
			Registered      bool     `json:"registered"`
			Path            string   `json:"path"`
			Clean           bool     `json:"clean"`
			ChangeKind      string   `json:"change_kind"`
			ChangedFiles    []string `json:"changed_files"`
			SourceResumable bool     `json:"source_resumable"`
		} `json:"takeover"`
	}
	if err := json.Unmarshal(taken.Data, &body); err != nil || body.State != domain.StatePaused || body.ResumeState != domain.StatePlanning || body.Takeover.Registered || body.Takeover.Path != "" || !body.Takeover.Clean || body.Takeover.ChangeKind != "no_worktree" || len(body.Takeover.ChangedFiles) != 0 || body.Takeover.SourceResumable {
		t.Fatalf("early take body=%s parsed=%+v err=%v", taken.Data, body, err)
	}
}

func TestDaemonOperatorControlsRejectWrongChannelAndOperator(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-operator-controls-auth")
	wrongChannel := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "take-wrong-channel", Method: "ticket.take", Ticket: string(started.Ref.Ticket), OperatorLabel: "operator",
		Parameters: json.RawMessage(`{"channel":"dev","operator":"operator"}`),
	})
	if wrongChannel.OK || wrongChannel.Error == nil || wrongChannel.Error.Code != "invalid_control" {
		t.Fatalf("wrong channel=%+v", wrongChannel)
	}
	wrongOperator := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "take-wrong-operator", Method: "ticket.take", Ticket: string(started.Ref.Ticket), OperatorLabel: "intruder",
		Parameters: json.RawMessage(`{"channel":"stable","operator":"intruder"}`),
	})
	if wrongOperator.OK || wrongOperator.Error == nil || wrongOperator.Error.Code != "operator_identity_required" {
		t.Fatalf("wrong operator=%+v", wrongOperator)
	}
}

func TestDaemonResumeRefusesUnadoptedTakeoverChanges(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-take-dirty")
	project, err := d.store.Project(context.Background(), d.channel, started.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Repeat("a", 40)
	path := filepath.Join(d.paths.Worktrees, "demo", string(started.Ref.Ticket))
	branch := "sf/stable/bbbbbbbb/bbbbbbbb-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := d.store.RegisterWorktree(context.Background(), store.WorktreeRegistration{Ref: started.Ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}, Path: path, Branch: branch, IdentityJSON: daemonRecoveryWorktreeIdentity(project.Path, path, branch, project.BaseRef, base), BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	control := &takeoverRuntimeController{inspection: contracts.TakeoverInspection{Registered: true, Path: path, Branch: branch, Repository: project.Path, BaseSHA: base, RemoteBaseSHA: base, RemoteIdentityExact: true, ChangeKind: "dirty"}}
	d.control = control
	if response := daemonControl(d, started.Ref.Ticket, "take"); !response.OK {
		t.Fatalf("take=%+v", response)
	}
	response := daemonResume(d, started.Ref.Ticket)
	if response.OK || response.Error == nil || response.Error.Code != "takeover_changes_unadopted" || control.rearms != 0 {
		t.Fatalf("dirty resume=%+v rearms=%d", response, control.rearms)
	}
}

func TestDaemonResumeSourceChangesRequiresAuthenticatedEvidence(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-take-source")
	project, err := d.store.Project(context.Background(), d.channel, started.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Repeat("c", 40)
	path := filepath.Join(d.paths.Worktrees, "demo", string(started.Ref.Ticket))
	branch := "sf/stable/cccccccc/cccccccc-cccccccccccccccccccccccccccccccc"
	if err := d.store.RegisterWorktree(context.Background(), store.WorktreeRegistration{Ref: started.Ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}, Path: path, Branch: branch, IdentityJSON: daemonRecoveryWorktreeIdentity(project.Path, path, branch, project.BaseRef, base), BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	control := &takeoverRuntimeController{inspection: contracts.TakeoverInspection{
		Registered: true, Path: path, Branch: branch, Repository: project.Path, BaseSHA: base, HeadSHA: base,
		Clean: true, ChangeKind: "source_commit", ChangedFiles: []string{"src/feature.go"}, SourceResumable: true, RemoteBaseSHA: base, RemoteIdentityExact: true,
		SourceCommit: contracts.OperatorSourceCommit{CommitOID: strings.Repeat("d", 40), ParentOID: base, TreeOID: strings.Repeat("e", 40), Changes: []contracts.OperatorSourceChange{{Status: "M", Path: "src/feature.go"}}},
	}}
	d.control = control
	if response := daemonControl(d, started.Ref.Ticket, "take"); !response.OK {
		t.Fatalf("take=%+v", response)
	}
	response := daemonResume(d, started.Ref.Ticket)
	// This fixture deliberately has no authenticated plan or verification
	// checkpoint. A source-changes resume must retain the paused ticket rather
	// than minting a Builder cycle from inspection data alone. The compiled
	// takeover test covers the real evidence-backed positive path.
	if response.OK || response.Error == nil || response.Error.Code != "resume_transition_refused" || control.rearms != 0 {
		t.Fatalf("unauthenticated source resume=%+v error=%+v next=%+v rearms=%d", response, response.Error, response.NextAction, control.rearms)
	}
	current, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil || current.State != domain.StatePaused || current.ResumeState != domain.StatePlanning {
		t.Fatalf("source resume ticket=%+v err=%v", current, err)
	}
}

func TestDaemonRetryUsesOnlyARecordedRetryPause(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-operator-retry")
	seedDaemonProviderExhaustion(t, d, started)
	response := daemonControl(d, started.Ref.Ticket, "retry")
	if !response.OK || response.Mutation.Kind != "ticket_retry" {
		t.Fatalf("retry response=%+v", response)
	}
	current, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil || current.State != domain.StatePlanning || current.ResumeState != "" {
		t.Fatalf("retried=%+v err=%v", current, err)
	}
	// A normal handoff pause has a different durable predecessor and cannot be
	// smuggled through the retry trigger. Keep it separate from the real
	// provider fixture above: taking an active provider phase intentionally
	// requires its own authenticated recovery evidence.
	normal := createAndStartControlTicket(t, d, "SF-operator-retry-normal-pause")
	if response := daemonControl(d, normal.Ref.Ticket, "take"); !response.OK {
		t.Fatalf("take=%+v", response)
	}
	if response := daemonControl(d, normal.Ref.Ticket, "retry"); response.OK || response.Error == nil || response.Error.Code != "retry_not_available" {
		t.Fatalf("retry after take=%+v", response)
	}
}

func TestDaemonProviderRetryReplayRearmsAndSecondEpochIsTerminal(t *testing.T) {
	t.Run("committed provider retry replays runtime rearm", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-provider-retry-replay")
		seedDaemonProviderRetryEpoch(t, d, started, false)
		controller := &retryRuntimeController{needed: true}
		d.control = controller
		response := daemonControl(d, started.Ref.Ticket, "retry")
		if !response.OK || !response.Mutation.Observed || response.Mutation.Attempted || controller.rearms != 1 {
			t.Fatalf("provider retry replay=%+v rearms=%d", response, controller.rearms)
		}
	})

	t.Run("provider retry replay fails closed without rearm authority", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-provider-retry-no-rearm")
		seedDaemonProviderRetryEpoch(t, d, started, false)
		d.control = testRuntimeController{}
		response := daemonControl(d, started.Ref.Ticket, "retry")
		if response.OK || response.Error == nil || response.Error.Code != "runtime_rearm_unavailable" {
			t.Fatalf("provider retry without rearm=%+v", response)
		}
	})

	t.Run("second provider exhaustion has a stable non-retryable response", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-provider-retry-terminal")
		seedDaemonProviderRetryEpoch(t, d, started, true)
		response := daemonControl(d, started.Ref.Ticket, "retry")
		if response.OK || response.Error == nil || response.Error.Code != "provider_retry_exhausted" {
			t.Fatalf("second provider exhaustion=%+v", response)
		}
		current, err := d.store.Ticket(t.Context(), started.Ref)
		if err != nil || current.State != domain.StatePaused || current.Version != 5 {
			t.Fatalf("terminal provider retry mutated ticket=%+v err=%v", current, err)
		}
	})
}

// seedDaemonProviderRetryEpoch models a response that was durably committed
// immediately before the daemon process lost its reply. Store-level tests
// exercise the authority that creates these rows; these daemon tests isolate
// the replay and operator-facing behavior at the transport boundary.
func seedDaemonProviderRetryEpoch(t *testing.T, daemon *Daemon, started store.Ticket, terminal bool) {
	seedDaemonProviderRetryState(t, daemon, started, true, terminal)
}

func seedDaemonProviderExhaustion(t *testing.T, daemon *Daemon, started store.Ticket) {
	seedDaemonProviderRetryState(t, daemon, started, false, false)
}

func seedDaemonProviderRetryState(t *testing.T, daemon *Daemon, started store.Ticket, retry, terminal bool) {
	t.Helper()
	ctx := t.Context()
	project, err := daemon.store.Project(ctx, started.Ref.Channel, started.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	branch := "sf/" + string(started.Ref.Channel) + "/" + daemonFixtureDigest(string(started.Ref.Project))[:16] + "/" + daemonFixtureDigest(string(started.Ref.Ticket))[:16] + "-" + strings.Repeat("b", 32)
	worktree := filepath.Join(project.Path, string(started.Ref.Ticket))
	identity := daemonFixtureIdentity(t, project.Path, worktree, branch, project.BaseRef)
	fence := domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: started.RunnerEpoch}
	branchKey := string(started.Ref.Channel) + "\x00" + string(started.Ref.Project) + "\x00" + string(started.Ref.Ticket)
	if _, err := daemon.store.LoadOrStoreBranchUnderFence(ctx, branchKey, branch, started.Version, fence); err != nil {
		t.Fatal(err)
	}
	if err := daemon.store.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: started.Ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: branch, IdentityJSON: identity, BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	planner, _, err := daemon.store.RecordProviderQualification(ctx, daemonFixtureQualification(daemon.channel, strings.Repeat("d", 32), "retry-planner", "retry-family"))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := daemon.store.RecordProviderQualification(ctx, daemonFixtureQualification(daemon.channel, strings.Repeat("e", 32), "retry-reviewer", "retry-review-family"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := daemon.store.SelectProviderPair(ctx, daemon.channel, planner.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	fail := func(ticket store.Ticket) {
		fence := domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: ticket.RunnerEpoch}
		binding := daemonFixtureBinding(planner)
		request := store.ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: binding, ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(), Repository: project.Path, Worktree: worktree, WorktreeIdentity: string(identity), BaseSHA: strings.Repeat("a", 40), SupervisorKey: signer.PublicKey(), Input: contracts.PhaseInput{Ticket: ticket.Ref, Phase: domain.PhasePlanning, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ExpectedVersion: ticket.Version, Prompt: "retry fixture", Repository: project.Path, Worktree: worktree, WorktreeIdentity: string(identity), BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}}
		claim, err := daemon.store.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := daemon.store.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "fixture", ProcessStartIdentity: fmt.Sprintf("retry-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatal(err)
		}
		if err := daemon.store.FinishProviderAttempt(ctx, claim, daemonFixtureDrainProof(t, signer, claim), ticket.Version, fence, "failed", "failed", 0, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	fail(started)
	fail(started)
	paused, err := daemon.store.TransitionProviderExhausted(ctx, store.Transition{Ref: started.Ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	started.Version, started.State = paused.Version, domain.StatePaused
	if !retry {
		return
	}
	retried, err := daemon.store.TransitionProviderRetry(ctx, store.Transition{Ref: started.Ref, ExpectedVersion: started.Version, From: domain.StatePaused, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_retry", Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	started.Version, started.State = retried.Version, domain.StatePlanning
	if terminal {
		fail(started)
		fail(started)
		if _, err := daemon.store.TransitionProviderExhausted(ctx, store.Transition{Ref: started.Ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDaemonGuardedMergeRetryDrainsBeforeAuthorityAndReplaysCommittedRearm(t *testing.T) {
	t.Run("missing merge authority remains paused after drain", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-merge-retry-drain"}
		if err := d.store.CreateTicket(t.Context(), store.Ticket{Ref: ref, SourceDigest: "merge-retry", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging}); err != nil {
			t.Fatal(err)
		}
		merging, err := d.store.Ticket(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.engine.Signal(t.Context(), contracts.SignalRequest{Ticket: ref, TicketVersion: merging.Version, From: domain.StateMerging, Trigger: "retry_or_correction_exhausted", Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: merging.RunnerEpoch}, EventPayload: `{"reason":"fixture"}`}); err != nil {
			t.Fatal(err)
		}
		var drains int
		controller := &retryRuntimeController{testRuntimeController: testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) {
			drains++
			return true, nil
		}}}
		d.control = controller
		response := daemonControl(d, ref.Ticket, "retry")
		if response.OK || response.Error == nil || response.Error.Code != "retry_transition_refused" || drains != 1 || controller.rearms != 0 {
			t.Fatalf("unbound guarded retry response=%+v drains=%d rearms=%d", response, drains, controller.rearms)
		}
		paused, err := d.store.Ticket(t.Context(), ref)
		if err != nil || paused.State != domain.StatePaused || paused.ResumeState != domain.StateMerging || paused.Version != merging.Version+1 {
			t.Fatalf("rejected guarded retry mutated ticket=%+v err=%v", paused, err)
		}
	})

	t.Run("committed sealed retry replays rearm without transition", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-merge-retry-rearm"}
		if err := d.store.CreateTicket(t.Context(), store.Ticket{Ref: ref, SourceDigest: "merge-retry", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging}); err != nil {
			t.Fatal(err)
		}
		before, err := d.store.Events(t.Context(), d.channel, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		controller := &retryRuntimeController{replay: store.GuardedMergeRetryNeedsRearm}
		d.control = controller
		response := daemonControl(d, ref.Ticket, "retry")
		if !response.OK || response.Mutation.Attempted || !response.Mutation.Observed || response.Mutation.Kind != "ticket_retry" || controller.rearms != 1 {
			t.Fatalf("committed retry replay=%+v rearms=%d", response, controller.rearms)
		}
		after, err := d.store.Events(t.Context(), d.channel, 0, 10)
		if err != nil || len(after) != len(before) {
			t.Fatalf("rearm replay events before=%d after=%d err=%v", len(before), len(after), err)
		}
	})

	t.Run("already rearmed retry is observed without another rearm", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-merge-retry-observed"}
		if err := d.store.CreateTicket(t.Context(), store.Ticket{Ref: ref, SourceDigest: "merge-retry", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging}); err != nil {
			t.Fatal(err)
		}
		controller := &retryRuntimeController{replay: store.GuardedMergeRetryAlreadyRearmed}
		d.control = controller
		response := daemonControl(d, ref.Ticket, "retry")
		if !response.OK || response.Mutation.Attempted || !response.Mutation.Observed || controller.rearms != 0 {
			t.Fatalf("already rearmed retry=%+v rearms=%d", response, controller.rearms)
		}
	})

	t.Run("unrelated sealed runtime is not a retry replay", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-merge-retry-unrelated"}
		if err := d.store.CreateTicket(t.Context(), store.Ticket{Ref: ref, SourceDigest: "merge-retry", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StateMerging}); err != nil {
			t.Fatal(err)
		}
		controller := &retryRuntimeController{replay: store.GuardedMergeRetryNotReplay}
		d.control = controller
		response := daemonControl(d, ref.Ticket, "retry")
		if response.OK || response.Error == nil || response.Error.Code != "retry_not_available" || controller.rearms != 0 {
			t.Fatalf("unrelated sealed retry=%+v rearms=%d", response, controller.rearms)
		}
	})
}

func TestDaemonGuardedMergeRetryUsesRealControllerProofAndSchedulerAdmission(t *testing.T) {
	worker := newDaemonRuntimeWorker()
	var scheduler *workflowruntime.Scheduler
	cfg, _ := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		scheduler = workflowruntime.NewScheduler(domain.ChannelStable, workflowruntime.StoreTicketSource{Store: deps.Store}, daemonRuntimeEnsure{}, worker)
		runtime, err := workflowruntime.NewRuntimeWithConfig(scheduler, workflowruntime.RuntimeConfig{Interval: time.Hour, Workers: 1})
		if err != nil {
			return WorkflowRuntimeComponents{}, err
		}
		controller, err := runtimecontrol.New(deps.Store, runtime.ControlBundle(), runtimecontrol.MergeObserverFunc(func(context.Context, domain.TicketRef) (bool, error) {
			return false, nil
		}))
		if err != nil {
			_ = runtime.Close()
			return WorkflowRuntimeComponents{}, err
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: controller}, nil
	})
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("demo", cfg.Projects[0].Path), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Projects[0].ConfigGeneration = 1
	cfg.Projects[0].ConfigDigest = digest
	cfg.Projects[0].ConfigSnapshot = snapshot
	d, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if scheduler == nil {
		t.Fatal("runtime factory did not expose its real scheduler")
	}
	merging := prepareDaemonGuardedMergeRetry(t, d, "SF-real-guarded-retry")
	stateAtDrain := make(chan domain.State, 1)
	worker.setOnExit(func(ref domain.TicketRef) {
		if ref != merging.Ref {
			return
		}
		current, err := d.store.Ticket(context.Background(), ref)
		if err != nil {
			t.Errorf("read ticket while proving drain: %v", err)
			return
		}
		stateAtDrain <- current.State
	})
	firstTick := make(chan workflowruntime.TickResult, 1)
	go func() { firstTick <- scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch}) }()
	select {
	case entered := <-worker.entered:
		if entered != merging.Ref {
			t.Fatalf("first admission entered %v, want %v", entered, merging.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("guarded merge did not enter the real scheduler")
	}
	if _, err := d.engine.Signal(t.Context(), contracts.SignalRequest{
		Ticket: merging.Ref, TicketVersion: merging.Version, From: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: merging.RunnerEpoch}, EventPayload: `{"reason":"fixture"}`,
	}); err != nil {
		t.Fatalf("record guarded merge retry exhaustion: %v", err)
	}
	paused, err := d.store.Ticket(t.Context(), merging.Ref)
	if err != nil || paused.State != domain.StatePaused || paused.ResumeState != domain.StateMerging {
		t.Fatalf("semantic retry pause=%+v err=%v", paused, err)
	}
	if calls, active := worker.snapshot(merging.Ref); calls != 1 || !active {
		t.Fatalf("semantic retry pause unexpectedly stopped the worker: calls=%d active=%v", calls, active)
	}
	replay, err := d.store.GuardedMergeRetryReplay(t.Context(), merging.Ref)
	if err != nil || replay != store.GuardedMergeRetryNotReplay {
		t.Fatalf("paused retry replay=%v err=%v", replay, err)
	}
	response := daemonControl(d, merging.Ref.Ticket, "retry")
	if !response.OK || response.Mutation.Observed {
		t.Fatalf("guarded retry response: ok=%v mutation=%+v error=%+v data=%s next_action=%+v", response.OK, response.Mutation, response.Error, response.Data, response.NextAction)
	}
	select {
	case state := <-stateAtDrain:
		if state != domain.StatePaused {
			t.Fatalf("worker drain did not complete before operator retry commit: saw state %s", state)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not prove its drain")
	}
	select {
	case exited := <-worker.exited:
		if exited != merging.Ref {
			t.Fatalf("retry drain exited %v, want %v", exited, merging.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("retry drain did not join the real scheduler worker")
	}
	select {
	case result := <-firstTick:
		if result.Outcome != workflowruntime.OutcomeCanceled {
			t.Fatalf("drained scheduler tick=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("drained scheduler tick did not return")
	}
	retried, err := d.store.Ticket(t.Context(), merging.Ref)
	if err != nil || retried.State != domain.StateMerging || retried.ResumeState != "" {
		t.Fatalf("retried guarded merge=%+v err=%v", retried, err)
	}
	replay, err = d.store.GuardedMergeRetryReplay(t.Context(), merging.Ref)
	if err != nil || replay != store.GuardedMergeRetryAlreadyRearmed {
		t.Fatalf("activated retry replay=%v err=%v", replay, err)
	}
	ready, err := d.store.RuntimeAdmissionReady(t.Context(), merging.Ref, retried.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: retried.RunnerEpoch})
	if err != nil || ready {
		t.Fatalf("armed retry was admitted before scheduler begin: ready=%v err=%v", ready, err)
	}
	runCtx, cancelRun := context.WithCancel(t.Context())
	secondTick := make(chan workflowruntime.TickResult, 1)
	go func() { secondTick <- scheduler.Tick(runCtx, domain.Fence{LeaderEpoch: d.epoch}) }()
	select {
	case entered := <-worker.entered:
		if entered != merging.Ref {
			t.Fatalf("rearmed admission entered %v, want %v", entered, merging.Ref)
		}
	case <-time.After(time.Second):
		cancelRun()
		t.Fatal("rearmed ticket was not admitted by the real scheduler")
	}
	ready, err = d.store.RuntimeAdmissionReady(t.Context(), merging.Ref, retried.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: retried.RunnerEpoch})
	if err != nil || !ready {
		cancelRun()
		t.Fatalf("scheduler begin did not open durable admission: ready=%v err=%v", ready, err)
	}
	duplicate := scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch})
	if duplicate.Outcome != workflowruntime.OutcomeCanceled {
		cancelRun()
		t.Fatalf("active rearmed ticket was admitted twice: %+v", duplicate)
	}
	cancelRun()
	select {
	case exited := <-worker.exited:
		if exited != merging.Ref {
			t.Fatalf("rearmed cancellation exited %v, want %v", exited, merging.Ref)
		}
	case <-time.After(time.Second):
		t.Fatal("rearmed scheduler worker did not stop")
	}
	select {
	case result := <-secondTick:
		if result.Outcome != workflowruntime.OutcomeCanceled {
			t.Fatalf("rearmed scheduler result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("rearmed scheduler tick did not return")
	}
}

func TestDaemonRecoverUsesTypedBlockerAndGuardedNarrowing(t *testing.T) {
	ctx := context.Background()
	d, _, _ := testDaemonForChannelWithProjectMaximum(t, domain.ChannelStable, domain.MergeGuarded)
	started := createAndStartControlTicket(t, d, "SF-operator-recover")
	if _, err := d.engine.Signal(ctx, contracts.SignalRequest{Ticket: started.Ref, TicketVersion: started.Version, From: domain.StatePlanning, Trigger: "typed_blocker", Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"host_repair_required"}`}); err != nil {
		t.Fatal(err)
	}
	response := d.Handle(ctx, transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "recover", Method: "ticket.recover", Ticket: string(started.Ref.Ticket), OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","operator":"operator"}`)})
	if !response.OK || response.Mutation.Kind != "ticket_recover" {
		t.Fatalf("recover=%+v", response)
	}

	legacy := createAndStartControlTicket(t, d, "SF-legacy-provider-recover")
	if _, err := d.engine.Signal(ctx, contracts.SignalRequest{Ticket: legacy.Ref, TicketVersion: legacy.Version, From: domain.StatePlanning, Trigger: "typed_blocker", Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: legacy.RunnerEpoch}, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"legacy_provider_phase_entry_unverifiable"}`}); err != nil {
		t.Fatal(err)
	}
	var legacyDrains int
	d.control = testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) {
		legacyDrains++
		return true, nil
	}}
	legacyResponse := daemonControl(d, legacy.Ref.Ticket, "recover")
	if legacyResponse.OK || legacyResponse.Error == nil || legacyResponse.Error.Code != "legacy_provider_entry_unverifiable" || legacyDrains != 0 || legacyResponse.NextAction == nil || strings.Join(legacyResponse.NextAction.Argv, " ") != "sf cancel SF-legacy-provider-recover" {
		t.Fatalf("legacy recovery=%+v drains=%d", legacyResponse, legacyDrains)
	}

	ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-guarded-recover"}
	if err := d.store.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "guarded-recover", Type: domain.TicketBug, MergeMode: domain.MergeAutonomous}); err != nil {
		t.Fatal(err)
	}
	queued, err := d.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	startedGuarded, err := d.store.StartOrAdopt(ctx, ref, queued.Version, "guarded-recover", domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.engine.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: startedGuarded.Version, From: domain.StatePlanning, Trigger: "typed_blocker", Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: startedGuarded.RunnerEpoch}, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"autonomy_ineligible"}`}); err != nil {
		t.Fatal(err)
	}
	guarded := d.Handle(ctx, transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "recover-guarded", Method: "ticket.recover", Ticket: string(ref.Ticket), OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","operator":"operator","mode":"guarded"}`)})
	if !guarded.OK {
		t.Fatalf("guarded recovery=%+v", guarded)
	}
	current, err := d.store.Ticket(ctx, ref)
	if err != nil || current.State != domain.StateBuilding || current.MergeMode != domain.MergeGuarded {
		t.Fatalf("guarded current=%+v err=%v", current, err)
	}
}

func TestDaemonPauseRemainsStoppingUntilRuntimeDrains(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-blocked-pause")
	drained := false
	d.control = testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) { return drained, nil }}
	response := daemonControl(d, started.Ref.Ticket, "pause")
	if response.OK || response.Error == nil || response.Error.Code != "blocked_process" || !response.Mutation.Attempted {
		t.Fatalf("undrained response=%+v", response)
	}
	stopping, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if stopping.State != domain.StateStopping || stopping.RunnerEpoch != started.RunnerEpoch+1 {
		t.Fatalf("stopping=%+v started=%+v", stopping, started)
	}
	if leases, err := d.store.Leases(context.Background(), d.channel); err != nil || len(leases) != 2 {
		t.Fatalf("undrained leases=%+v err=%v", leases, err)
	}
	drained = true
	response = daemonControl(d, started.Ref.Ticket, "pause")
	if !response.OK {
		t.Fatalf("completed response=%+v", response)
	}
	paused, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil || paused.State != domain.StatePaused {
		t.Fatalf("paused=%+v err=%v", paused, err)
	}
}

func TestDaemonCancelChecksMergeBeforeAndAfterDrain(t *testing.T) {
	t.Run("merge already observed does not mutate", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-cancel-premerge")
		var drains int
		d.control = testRuntimeController{
			drain: func(context.Context, domain.TicketRef) (bool, error) { drains++; return true, nil },
			merge: func(context.Context, domain.TicketRef) (bool, error) { return true, nil },
		}
		response := daemonControl(d, started.Ref.Ticket, "cancel")
		if response.OK || response.Error == nil || response.Error.Code != "external_merge_observed" || response.Mutation.Attempted || drains != 0 {
			t.Fatalf("response=%+v drains=%d", response, drains)
		}
		stored, err := d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StatePlanning || stored.Version != started.Version || stored.RunnerEpoch != started.RunnerEpoch {
			t.Fatalf("stored=%+v err=%v started=%+v", stored, err, started)
		}
	})

	t.Run("merge racing drain leaves cancellation incomplete", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-cancel-race")
		observations := 0
		d.control = testRuntimeController{
			drain: func(context.Context, domain.TicketRef) (bool, error) { return true, nil },
			merge: func(context.Context, domain.TicketRef) (bool, error) {
				observations++
				return observations >= 2, nil
			},
		}
		response := daemonControl(d, started.Ref.Ticket, "cancel")
		if response.OK || response.Error == nil || response.Error.Code != "external_merge_observed" || !response.Mutation.Attempted || observations != 2 {
			t.Fatalf("response=%+v observations=%d", response, observations)
		}
		stored, err := d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StateCancelling || stored.RunnerEpoch != started.RunnerEpoch+1 {
			t.Fatalf("stored=%+v err=%v started=%+v", stored, err, started)
		}
		response = daemonControl(d, started.Ref.Ticket, "cancel")
		if response.OK || response.Error == nil || response.Error.Code != "external_merge_observed" {
			t.Fatalf("repeat after observed merge=%+v", response)
		}
		stored, err = d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StateCancelling {
			t.Fatalf("repeat stored=%+v err=%v", stored, err)
		}
	})

	t.Run("absence through drain completes cancellation", func(t *testing.T) {
		d, _, _ := testDaemon(t)
		started := createAndStartControlTicket(t, d, "SF-cancel")
		var observations int
		d.control = testRuntimeController{
			drain: func(context.Context, domain.TicketRef) (bool, error) { return true, nil },
			merge: func(context.Context, domain.TicketRef) (bool, error) { observations++; return false, nil },
		}
		response := daemonControl(d, started.Ref.Ticket, "cancel")
		if !response.OK || observations != 2 {
			t.Fatalf("response=%+v observations=%d", response, observations)
		}
		stored, err := d.store.Ticket(context.Background(), started.Ref)
		if err != nil || stored.State != domain.StateCancelled || stored.RunnerEpoch != started.RunnerEpoch+1 {
			t.Fatalf("stored=%+v err=%v started=%+v", stored, err, started)
		}
	})
}

func TestDaemonCancellationRetriesTerminalRuntimeRetirementWithoutReopeningAdmission(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-cancel-retirement")
	retireErr := errors.New("runtime retirement unavailable")
	attempts := 0
	d.control = testRuntimeRetirementController{retire: func(context.Context, domain.TicketRef) error {
		attempts++
		if attempts == 1 {
			return retireErr
		}
		return nil
	}}
	response := daemonControl(d, started.Ref.Ticket, "cancel")
	if response.OK || response.Error == nil || response.Error.Code != "runtime_retirement_failed" || !response.Error.Retryable {
		t.Fatalf("first cancellation response=%+v", response)
	}
	terminal, err := d.store.Ticket(context.Background(), started.Ref)
	if err != nil || terminal.State != domain.StateCancelled {
		t.Fatalf("terminal cancellation was not persisted: ticket=%+v err=%v", terminal, err)
	}
	response = daemonControl(d, started.Ref.Ticket, "cancel")
	if !response.OK || !response.Mutation.Observed || attempts != 2 {
		t.Fatalf("retry response=%+v attempts=%d", response, attempts)
	}
	terminal, err = d.store.Ticket(context.Background(), started.Ref)
	if err != nil || terminal.State != domain.StateCancelled || terminal.Version != started.Version+2 {
		t.Fatalf("retirement retry changed terminal admission: ticket=%+v err=%v", terminal, err)
	}
}

func TestDaemonControlPreconditionFailureReportsNoMutation(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-control-read")
	d.control = testRuntimeController{merge: func(context.Context, domain.TicketRef) (bool, error) { return false, errors.New("unavailable") }}
	response := daemonControl(d, started.Ref.Ticket, "cancel")
	if response.OK || response.Error == nil || response.Error.Code != "external_state_unavailable" || response.Mutation.Attempted {
		t.Fatalf("response=%+v", response)
	}
}

func TestDaemonShowAndStatusExposeBoundedAuthenticatedEvidence(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-evidence-view")
	digest, err := d.store.RecordPlan(context.Background(), store.PlanArtifact{
		Ref: started.Ref, ExpectedVersion: started.Version,
		Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch},
		Document: store.PlanDocument{
			Acceptance: []string{"a durable result exists"}, ProofKind: "regression",
			Paths: []string{"internal/example.go"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"stale state"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	show := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "show-evidence", Method: "ticket.show", Ticket: string(started.Ref.Ticket),
		OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"demo","ticket":"SF-evidence-view"}`),
	})
	if !show.OK || !strings.Contains(string(show.Data), `"digest":"`+digest+`"`) || !strings.Contains(string(show.Data), `"proof_kind":"regression"`) || !strings.Contains(string(show.Data), `"operator":{"label":"operator"`) || strings.Contains(string(show.Data), "identity_json") {
		t.Fatalf("show=%+v data=%s", show, show.Data)
	}
	status := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "status-evidence", Method: "ticket.status", Ticket: string(started.Ref.Ticket),
		OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"","watch":false}`),
	})
	if !status.OK || !strings.Contains(string(status.Data), `"runner_epoch":`) || !strings.Contains(string(status.Data), `"phase_attempts":[]`) || !strings.Contains(string(status.Data), `"merge_mode":"guarded"`) {
		t.Fatalf("status=%+v data=%s", status, status.Data)
	}
}

func TestDaemonRefusesToHideCorruptDurableEvidence(t *testing.T) {
	d, paths, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-evidence-conflict")
	if _, err := d.store.RecordPlan(context.Background(), store.PlanArtifact{
		Ref: started.Ref, ExpectedVersion: started.Version,
		Fence:    domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch},
		Document: store.PlanDocument{Acceptance: []string{"one"}, ProofKind: "focused", Paths: []string{"x.go"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"one"}},
	}); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(context.Background(), `UPDATE plans SET artifact_bytes='{}' WHERE channel='stable' AND project_id='demo' AND ticket_id='SF-evidence-conflict'`); err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "show-conflict", Method: "ticket.show", Ticket: string(started.Ref.Ticket),
		OperatorLabel: "operator", Parameters: json.RawMessage(`{"channel":"stable","project":"demo","ticket":"SF-evidence-conflict"}`),
	})
	if response.OK || response.Error == nil || response.Error.Code != "evidence_conflict" || response.NextAction == nil || strings.Join(response.NextAction.Argv, " ") != "sf doctor" {
		t.Fatalf("corrupt evidence response=%+v", response)
	}
}

func TestSubmitRejectsUnregisteredProjectBeforeTicketPersistence(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		t.Run(string(channel), func(t *testing.T) {
			d, _, _ := testDaemonForChannel(t, channel)
			request := api.Request{
				Version: api.Version, RequestID: "missing-project", Method: "ticket.submit",
				Parameters: []byte(`{"channel":"` + string(channel) + `","project":"missing","source":"# Missing project\n\nKeep the behavior deterministic.\n"}`),
			}
			response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
			binary := "sf"
			if channel == domain.ChannelDev {
				binary = "sf-dev"
			}
			if response.OK || response.Error == nil || response.Error.Code != "unknown_project" || response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join([]string{binary, "init", "--help"}, "\x00") {
				t.Fatalf("missing-project response=%+v", response)
			}
			tickets, err := d.store.Tickets(context.Background(), channel, "missing", 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(tickets) != 0 {
				t.Fatalf("unregistered project persisted tickets: %+v", tickets)
			}
		})
	}
}

func TestSubmitRefusesAutonomousWorkflowBeforeAdmission(t *testing.T) {
	d, _, _ := testDaemonForChannel(t, domain.ChannelDev)
	source := "---\nmerge: autonomous\n---\n# Autonomous\n\nDo not start this workflow.\n\n## Acceptance\n- It is refused before admission\n"
	request := api.Request{Version: api.Version, RequestID: "autonomous-refusal", Method: "ticket.submit", Parameters: []byte(`{"channel":"dev","project":"demo","source":` + strconv.Quote(source) + `}`)}
	response := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, request)
	if response.OK || response.Error == nil || response.Error.Code != "autonomous_unavailable" || response.Mutation.Attempted || response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join([]string{"sf-dev", "submit", "--help"}, "\x00") {
		t.Fatalf("autonomous submission response=%+v", response)
	}
	tickets, err := d.store.Tickets(context.Background(), domain.ChannelDev, "demo", 10)
	if err != nil || len(tickets) != 0 {
		t.Fatalf("autonomous submission persisted tickets=%+v err=%v", tickets, err)
	}
}

func TestSubmitResolvesOmittedMergeModeAgainstFrozenProjectPolicy(t *testing.T) {
	submit := func(t *testing.T, daemon *Daemon, source, requestID string) api.Response {
		t.Helper()
		return daemon.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
			Version: api.Version, RequestID: requestID, Method: "ticket.submit",
			Parameters: []byte(`{"channel":"dev","project":"demo","source":` + strconv.Quote(source) + `}`),
		})
	}
	manual, _, _ := testDaemonForChannelWithProjectMaximum(t, domain.ChannelDev, domain.MergeManual)
	omitted := submit(t, manual, "# Inherit manual\n\nThe project policy is the default.\n", "manual-omitted")
	if !omitted.OK {
		t.Fatalf("manual omitted response=%+v", omitted)
	}
	manualTickets, err := manual.store.Tickets(context.Background(), domain.ChannelDev, "demo", 10)
	if err != nil || len(manualTickets) != 1 || manualTickets[0].MergeMode != domain.MergeManual {
		t.Fatalf("manual omitted ticket=%+v err=%v", manualTickets, err)
	}
	explicitGuarded := submit(t, manual, "---\nmerge: guarded\n---\n# Reject wider mode\n\nThe project is manual.\n", "manual-guarded")
	if explicitGuarded.OK || explicitGuarded.Error == nil || explicitGuarded.Error.Code != "ticket_policy_refused" || explicitGuarded.NextAction == nil || strings.Join(explicitGuarded.NextAction.Argv, "\x00") != strings.Join([]string{"sf-dev", "submit", "--help"}, "\x00") {
		t.Fatalf("manual guarded override response=%+v", explicitGuarded)
	}
	guarded, _, _ := testDaemonForChannelWithProjectMaximum(t, domain.ChannelDev, domain.MergeGuarded)
	manualOverride := submit(t, guarded, "---\nmerge: manual\n---\n# Narrow merge\n\nManual is stricter.\n", "guarded-manual")
	if !manualOverride.OK {
		t.Fatalf("guarded manual response=%+v", manualOverride)
	}
	guardedTickets, err := guarded.store.Tickets(context.Background(), domain.ChannelDev, "demo", 10)
	if err != nil || len(guardedTickets) != 1 || guardedTickets[0].MergeMode != domain.MergeManual {
		t.Fatalf("guarded manual ticket=%+v err=%v", guardedTickets, err)
	}
}

func writeTicket(t *testing.T, dir, title string) string {
	t.Helper()
	path := filepath.Join(dir, strings.ToLower(strings.ReplaceAll(title, " ", "-"))+".md")
	contents := "# " + title + "\n\nKeep the behavior deterministic.\n\n## Acceptance\n- A durable result exists\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func executeCLI(t *testing.T, ctx context.Context, paths config.ChannelPaths, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut strings.Builder
	code := cli.Execute(ctx, args, &out, &errOut, cli.SocketClient{Path: paths.Socket, Timeout: 5 * time.Second})
	return code, out.String(), errOut.String()
}

func TestForegroundCLIListShowStartAndReplay(t *testing.T) {
	d, paths, _ := testDaemon(t)
	dir := t.TempDir()
	first, second := writeTicket(t, dir, "First ticket"), writeTicket(t, dir, "Second ticket")
	if code, _, errOut := executeCLI(t, context.Background(), paths, "submit", first, "--project", "demo"); code != 0 || errOut != "" {
		t.Fatalf("first submit code=%d stderr=%q", code, errOut)
	}
	if code, _, errOut := executeCLI(t, context.Background(), paths, "submit", second, "--project", "demo"); code != 0 || errOut != "" {
		t.Fatalf("second submit code=%d stderr=%q", code, errOut)
	}
	code, list, errOut := executeCLI(t, context.Background(), paths, "status", "--json")
	if code != 0 || errOut != "" || !strings.Contains(list, "SF-test-1") || !strings.Contains(list, "SF-test-2") {
		t.Fatalf("status code=%d output=%q stderr=%q", code, list, errOut)
	}
	code, shown, errOut := executeCLI(t, context.Background(), paths, "show", "SF-test-1", "--json")
	if code != 0 || errOut != "" || !strings.Contains(shown, "First ticket") || !strings.Contains(shown, "source_digest") {
		t.Fatalf("show code=%d output=%q stderr=%q", code, shown, errOut)
	}
	code, started, errOut := executeCLI(t, context.Background(), paths, "start", "SF-test-1", "--json")
	if code != 0 || errOut != "" || !strings.Contains(started, "planning") {
		t.Fatalf("start code=%d output=%q stderr=%q", code, started, errOut)
	}
	code, replay, errOut := executeCLI(t, context.Background(), paths, "start", "SF-test-1", "--json")
	if code != 0 || errOut != "" || !strings.Contains(replay, "\"observed\":true") {
		t.Fatalf("replay code=%d output=%q stderr=%q", code, replay, errOut)
	}
	events, err := d.store.Events(context.Background(), domain.ChannelStable, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	var submit, start int
	for _, event := range events {
		switch event.Trigger {
		case "submit_valid":
			submit++
		case "operator_start":
			start++
		}
	}
	if submit != 2 || start != 1 {
		t.Fatalf("normative event counts submit=%d start=%d events=%+v", submit, start, events)
	}
}

func TestDaemonRebuildsRedactedEventProjectionAfterMutations(t *testing.T) {
	_, paths, _ := testDaemon(t)
	ticketPath := writeTicket(t, t.TempDir(), "Projected ticket")
	if code, _, errOut := executeCLI(t, context.Background(), paths, "submit", ticketPath, "--project", "demo"); code != 0 || errOut != "" {
		t.Fatalf("submit code=%d stderr=%q", code, errOut)
	}
	if code, _, errOut := executeCLI(t, context.Background(), paths, "start", "SF-test-1"); code != 0 || errOut != "" {
		t.Fatalf("start code=%d stderr=%q", code, errOut)
	}
	projectionPath := filepath.Join(paths.Events, "events.ndjson")
	info, err := os.Lstat(projectionPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("projection identity info=%v err=%v", info, err)
	}
	data, err := os.ReadFile(projectionPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("projection lines=%d data=%s", len(lines), data)
	}
	for index, line := range lines {
		var record events.Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("line %d record=%+v err=%v", index, record, err)
		}
		if err := record.Validate(); err != nil {
			t.Fatalf("line %d record=%+v err=%v", index, record, err)
		}
	}
	if !strings.Contains(string(data), `"trigger":"submit_valid"`) || !strings.Contains(string(data), `"trigger":"operator_start"`) {
		t.Fatalf("projection lacks normative events: %s", data)
	}
}

func TestProjectionFailureReportsCommittedMutationAndReadRepairsIt(t *testing.T) {
	d, paths, _ := testDaemon(t)
	original := d.projectionPath
	d.projectionMu.Lock()
	d.projectionPath = filepath.Join(paths.Root, "missing-parent", "events.ndjson")
	d.projectionMu.Unlock()
	ticketPath := writeTicket(t, t.TempDir(), "Projection repair")
	code, output, errOut := executeCLI(t, context.Background(), paths, "submit", ticketPath, "--project", "demo")
	if code != int(cli.ExitWait) || errOut != "" || !strings.Contains(output, "projection_unavailable") || !strings.Contains(output, "Mutation: attempted") || !strings.Contains(output, "sf status SF-test-1") {
		t.Fatalf("submit code=%d output=%q stderr=%q", code, output, errOut)
	}
	if _, err := d.store.TicketByID(context.Background(), domain.ChannelStable, "SF-test-1"); err != nil {
		t.Fatalf("committed ticket was lost: %v", err)
	}
	d.projectionMu.Lock()
	d.projectionPath = original
	d.projectionMu.Unlock()
	request := api.Request{Version: api.Version, RequestID: "repair-projection", Method: "daemon.status", Parameters: []byte(`{}`)}
	response, err := transport.Call(context.Background(), paths.Socket, request)
	if err != nil || !response.OK || !strings.Contains(string(response.Data), `"event_projection_ready":true`) {
		t.Fatalf("status response=%+v err=%v", response, err)
	}
	data, err := os.ReadFile(original)
	if err != nil || !strings.Contains(string(data), "SF-test-1") {
		t.Fatalf("repaired projection=%q err=%v", data, err)
	}
}

func TestForegroundDaemonStatusAndNormativeEvents(t *testing.T) {
	d, paths, _ := testDaemon(t)
	request := api.Request{Version: api.Version, RequestID: "status-test", Method: "daemon.status", Parameters: []byte(`{}`)}
	response, err := transport.Call(context.Background(), paths.Socket, request)
	if err != nil || !response.OK {
		t.Fatalf("daemon status response=%+v err=%v", response, err)
	}
	events, err := d.store.Events(context.Background(), domain.ChannelStable, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("unexpected events before submit: %+v", events)
	}
}

func TestDaemonSocketFallbackRetainsTheChannelExecutable(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sfv2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	server, err := transport.ListenWithExecutable(filepath.Join(root, "sf.sock"), uint32(os.Getuid()), transport.HandlerFunc(func(context.Context, transport.Peer, api.Request) api.Response {
		return api.Response{OK: false}
	}), "sf-dev")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		<-done
	})
	response, err := transport.Call(context.Background(), filepath.Join(root, "sf.sock"), api.Request{Version: api.Version, RequestID: "invalid-response", Method: "ticket.submit"})
	if err != nil {
		t.Fatal(err)
	}
	if response.Error == nil || response.Error.Code != "internal_response_invalid" || response.NextAction == nil || strings.Join(response.NextAction.Argv, "\x00") != strings.Join([]string{"sf-dev", "doctor"}, "\x00") {
		t.Fatalf("invalid response fallback=%+v", response)
	}
}

func TestForegroundRejectsWrongOperatorAndSecondLeader(t *testing.T) {
	_, paths, _ := testDaemon(t)
	request := api.Request{Version: api.Version, RequestID: "wrong-operator", Method: "daemon.status", OperatorLabel: "intruder", Parameters: []byte(`{}`)}
	response, err := transport.Call(context.Background(), paths.Socket, request)
	if err != nil {
		t.Fatal(err)
	}
	if response.OK || response.Error == nil || response.Error.Code != "operator_identity_required" || response.NextAction == nil {
		t.Fatalf("wrong operator response=%+v", response)
	}
	second, err := leader.Acquire(filepath.Join(paths.Root, "run", "leader.lock"), domain.ChannelStable, "second")
	if err == nil {
		_ = second.Close()
		t.Fatal("second leader unexpectedly acquired")
	}
	if !errors.Is(err, leader.ErrLeaderExists) {
		t.Fatalf("second leader error=%v", err)
	}
}

func TestRestartFencesPlanningRunnerWithoutDroppingItsLease(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	dir := t.TempDir()
	ticket := writeTicket(t, dir, "Restart fence")
	if code, _, stderr := executeCLI(t, context.Background(), paths, "submit", ticket, "--project", "demo"); code != 0 || stderr != "" {
		t.Fatalf("submit code=%d stderr=%q", code, stderr)
	}
	if code, _, stderr := executeCLI(t, context.Background(), paths, "start", "SF-test-1"); code != 0 || stderr != "" {
		t.Fatalf("start code=%d stderr=%q", code, stderr)
	}
	ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "demo", Ticket: "SF-test-1"}
	before, err := d.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeLeases, err := d.store.Leases(context.Background(), domain.ChannelStable)
	if err != nil || len(beforeLeases) == 0 {
		t.Fatalf("before restart leases=%+v err=%v", beforeLeases, err)
	}
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// The production path uses the embedded reviewed artifact and may start with
	// no configured projects when the durable registration already exists.
	restarted, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "restart-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	after, err := restarted.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.RunnerEpoch != before.RunnerEpoch+1 || after.Version != before.Version+1 {
		t.Fatalf("recovery did not fence runner: before=%+v after=%+v", before, after)
	}
	leases, err := restarted.store.Leases(context.Background(), domain.ChannelStable)
	if err != nil || len(leases) != len(beforeLeases) {
		t.Fatalf("recovery changed capacity occupancy: leases=%+v err=%v", leases, err)
	}
	for index, lease := range leases {
		if lease.Ref != beforeLeases[index].Ref || lease.Scope != beforeLeases[index].Scope || lease.ScopeKey != beforeLeases[index].ScopeKey || !lease.AcquiredAt.Equal(beforeLeases[index].AcquiredAt) || lease.RunnerEpoch != after.RunnerEpoch {
			t.Fatalf("recovery did not transfer exact lease: before=%+v after=%+v", beforeLeases[index], lease)
		}
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	secondRestart, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "restart-test-second"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondRestart.Close() })
	afterSecondRestart, err := secondRestart.store.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if afterSecondRestart.RunnerEpoch != after.RunnerEpoch+1 || afterSecondRestart.Version != after.Version+1 {
		t.Fatalf("second recovery did not fence runner: first=%+v second=%+v", after, afterSecondRestart)
	}
	secondLeases, err := secondRestart.store.Leases(context.Background(), domain.ChannelStable)
	if err != nil || len(secondLeases) != len(leases) {
		t.Fatalf("second recovery changed capacity occupancy: leases=%+v err=%v", secondLeases, err)
	}
	for index, lease := range secondLeases {
		if lease.Scope != leases[index].Scope || lease.ScopeKey != leases[index].ScopeKey || !lease.AcquiredAt.Equal(leases[index].AcquiredAt) || lease.RunnerEpoch != afterSecondRestart.RunnerEpoch {
			t.Fatalf("second recovery was not idempotent transfer: first=%+v second=%+v", leases[index], lease)
		}
	}
}

func TestRestartRefusesAmbiguousLeaseAdoptionBeforeSocketExposure(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-restart-ambiguous-lease")
	if _, err := d.store.AcquireLeases(context.Background(), started.Ref, started.Version, domain.Fence{LeaderEpoch: d.Epoch(), RunnerEpoch: started.RunnerEpoch}, []store.LeaseRequest{{Scope: "provider", Resource: "recovery-provider", Capacity: 1}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "restart-ambiguous-lease"})
	if !errors.Is(err, store.ErrLeaseAdoption) {
		t.Fatalf("startup error=%v, want lease adoption refusal", err)
	}
	if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket was exposed despite ambiguous adoption: %v", err)
	}
}

func TestRecoverDrainRequestPreservesChatGPTSubscriptionAuthMode(t *testing.T) {
	claim := store.ProviderAttemptClaim{ID: 41, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-auth-recovery"}, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2, Binding: contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "codex", Model: "gpt-5.6", Family: "openai", Version: "1"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), AuthDigest: strings.Repeat("c", 64), AuthMode: "chatgpt_subscription"}, LeaseKey: "provider/codex", BindingDigest: strings.Repeat("d", 64), LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: strings.Repeat("e", 40), RequestDigest: strings.Repeat("f", 64)}
	req := drainRequestForProviderClaim(claim)
	if req.AuthMode != "chatgpt_subscription" || req.RequestDigest != claim.RequestDigest {
		t.Fatalf("recovery request lost authenticated launch fields: %+v", req)
	}
}

func TestStartupBusyHonorsConfiguredDeadline(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	locked, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = locked.Close() })
	connection, err := locked.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer connection.ExecContext(context.Background(), "ROLLBACK")
	started := time.Now()
	_, err = Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "busy-test", StartupTimeout: 40 * time.Millisecond})
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("startup error=%v, want typed busy", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("startup outlived deadline: %s", elapsed)
	}
}

func TestStartRejectsSymlinkedExactSocketParent(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	if err := os.Symlink(external, filepath.Join(root, "socket-link")); err != nil {
		t.Fatal(err)
	}
	paths := config.ChannelPaths{
		Root: root, Database: filepath.Join(root, "sf.sqlite"), Socket: filepath.Join(root, "socket-link", "sf.sock"),
		Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "backups"),
	}
	if _, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "symlink-test"}); err == nil {
		t.Fatal("daemon accepted a symlinked socket parent")
	}
}
