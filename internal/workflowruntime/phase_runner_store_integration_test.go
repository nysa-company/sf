package workflowruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// This exercises the production bridge against immutable Store evidence, not
// a hand-built phaseStore. Each phase has a newer ticket/fence than its
// predecessor, while the registered worktree and predecessor bindings retain
// their planning/verifying identities.
func TestPhaseRunnerStoreLifecyclePreservesHistoricalPredecessors(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "phase-lineage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const repository = "/tmp/phase-lineage-repository"
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("lineage", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "lineage", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "lineage", Ticket: "SF-phase-lineage"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: lineageDigest("source"), Source: []byte("source"), Title: "lineage", Problem: "prove historical predecessors", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}

	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "phase-lineage-planning")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, supervisor.PublicKey()); err != nil {
		t.Fatal(err)
	}
	initial, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, initial.Version, "dev/lineage/SF-phase-lineage", domain.Fence{LeaderEpoch: leader, RunnerEpoch: initial.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	planningFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	identity := lineageIdentity(t, repository, repository+"/worktree", "dev/lineage/SF-phase-lineage")
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: planningFence, Path: repository + "/worktree", Branch: "dev/lineage/SF-phase-lineage", IdentityJSON: identity, BaseSHA: lineageOID("a"), HeadSHA: lineageOID("b")}); err != nil {
		t.Fatal(err)
	}

	coordinator := &lineageCoordinator{db: db, configDigest: configDigest, signer: lineageSigner(t), bindings: lineageQualifications(t, db, supervisor, leader, "planning")}
	runner := NewPhaseRunner(db, coordinator)
	worktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	planningRequest := workflowworker.PhaseRequest{Ticket: started, Worktree: worktree, Phase: domain.PhasePlanning, Fence: planningFence}
	planningResult, err := runner.Run(ctx, planningRequest)
	if err != nil {
		t.Fatalf("planner: %v coordinator=%v", err, coordinator.err)
	}
	plannerResult, plannerParsed, err := db.LoadCurrentProviderAttemptResult(ctx, planningResult.ProviderResult, started.Version, planningFence)
	if err != nil || plannerParsed.Planner == nil {
		t.Fatalf("current planner=%+v parsed=%+v err=%v", plannerResult, plannerParsed, err)
	}
	if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, planningResult.ProviderResult); err != nil {
		t.Fatalf("historical planner: %v", err)
	}
	if _, err := db.RecordPlan(ctx, store.PlanArtifact{Ref: ref, ExpectedVersion: started.Version, Fence: planningFence, Document: store.PlanDocument{Planner: plannerParsed.Planner, ProviderResult: &planningResult.ProviderResult, Acceptance: plannerParsed.Planner.Acceptance, ProofKind: string(plannerParsed.Planner.Proof.Kind), Paths: plannerParsed.Planner.Paths, Commands: plannerParsed.Planner.Commands, Risks: plannerParsed.Planner.Risks}}); err != nil {
		t.Fatalf("record plan: %v", err)
	}
	plan, err := db.Plan(ctx, ref)
	if err != nil || plan.TicketVersion != started.Version || plan.Fence != planningFence {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if _, err := engineForLineage(t, db).SignalPlan(ctx, lineageSignal(started, planningFence, "phase_pass")); err != nil {
		t.Fatalf("signal plan: %v", err)
	}

	verifying, verificationFence := lineageRecover(t, db, supervisor, ref, "verifying")
	coordinator.bindings = lineageQualifications(t, db, supervisor, verificationFence.LeaderEpoch, "verifying")
	worktree, err = db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	verificationRequest := workflowworker.PhaseRequest{Ticket: verifying, Worktree: worktree, Phase: domain.PhaseVerification, Fence: verificationFence, Plan: &plan}
	if _, parsed, loadErr := db.LoadHistoricalProviderAttemptResult(ctx, *plan.Document.ProviderResult); loadErr != nil || parsed.Planner == nil {
		t.Fatalf("historical predecessor load=%v parsed=%+v", loadErr, parsed)
	}
	verificationResult, err := runner.Run(ctx, verificationRequest)
	if err != nil {
		t.Fatalf("reviewer with historical plan/worktree: %v calls=%v coordinator=%v plan=%+v worktree=%+v", err, coordinator.calls, coordinator.err, plan, worktree)
	}
	_, verificationParsed, err := db.LoadCurrentProviderAttemptResult(ctx, verificationResult.ProviderResult, verifying.Version, verificationFence)
	if err != nil || verificationParsed.Verify == nil {
		t.Fatalf("current reviewer parsed=%+v err=%v", verificationParsed, err)
	}
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(*verificationParsed.Verify)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := workflowprompt.CanonicalVerificationProofBytes(*verificationParsed.Verify)
	if err != nil {
		t.Fatal(err)
	}
	command := lineageCommandEvidence(t, db, ctx, ref, verifying.Version, verificationFence, verificationResult.ProviderResult, lineageDigestBytes(intent), lineageDigestBytes(proof))
	if _, err := db.RecordVerification(ctx, store.VerificationArtifact{Ref: ref, ExpectedVersion: verifying.Version, Fence: verificationFence, Intent: intent, Proof: proof, OwnedFiles: verificationParsed.Verify.OwnedFiles, CheckpointID: lineageOID("c"), ProviderResult: &verificationResult.ProviderResult, Checkpoint: store.CommitObservation{CommitOID: lineageOID("c"), ParentOID: lineageOID("a"), TreeOID: lineageOID("d")}, CommandResult: command}); err != nil {
		t.Fatalf("record verification: %v", err)
	}
	verification, err := db.CurrentVerification(ctx, ref)
	if err != nil || verification.TicketVersion != verifying.Version || verification.Fence != verificationFence {
		t.Fatalf("verification=%+v err=%v", verification, err)
	}
	if _, err := engineForLineage(t, db).SignalVerification(ctx, lineageSignal(verifying, verificationFence, "phase_pass")); err != nil {
		t.Fatalf("signal verification: %v", err)
	}

	building, buildFence := lineageRecover(t, db, supervisor, ref, "building")
	coordinator.bindings = lineageQualifications(t, db, supervisor, buildFence.LeaderEpoch, "building")
	worktree, err = db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	buildRequest := workflowworker.PhaseRequest{Ticket: building, Worktree: worktree, Phase: domain.PhaseBuild, Fence: buildFence, Plan: &plan, Verification: &verification}
	buildResult, err := runner.Run(ctx, buildRequest)
	if err != nil {
		t.Fatalf("builder with historical plan/verification/worktree: %v", err)
	}
	if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, buildResult.ProviderResult, building.Version, buildFence); err != nil {
		t.Fatalf("current builder: %v", err)
	}
	if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, buildResult.ProviderResult); err != nil {
		t.Fatalf("historical builder: %v", err)
	}
	if planningFence == verificationFence || verificationFence == buildFence || !(started.Version < verifying.Version && verifying.Version < building.Version) || plan.TicketVersion != started.Version || verification.TicketVersion != verifying.Version || worktree.TicketVersion != started.Version {
		t.Fatalf("lineage versions/fences plan=%+v verification=%+v tickets=%d/%d/%d fences=%+v/%+v/%+v worktree=%+v", plan, verification, started.Version, verifying.Version, building.Version, planningFence, verificationFence, buildFence, worktree)
	}
	if coordinator.calls[providercoord.RolePlanner] != 1 || coordinator.calls[providercoord.RoleReviewer] != 1 || coordinator.calls[providercoord.RoleBuilder] != 1 {
		t.Fatalf("provider calls=%v", coordinator.calls)
	}

	for name, mutate := range map[string]func(*workflowworker.PhaseRequest){
		"plan":         func(r *workflowworker.PhaseRequest) { p := *r.Plan; p.TicketVersion++; r.Plan = &p },
		"verification": func(r *workflowworker.PhaseRequest) { v := *r.Verification; v.TicketVersion++; r.Verification = &v },
		"worktree":     func(r *workflowworker.PhaseRequest) { r.Worktree.Branch = "dev/lineage/substituted" },
		"ticket":       func(r *workflowworker.PhaseRequest) { r.Ticket.SourceDigest = lineageDigest("substituted") },
	} {
		t.Run("refuses substituted "+name, func(t *testing.T) {
			request := buildRequest
			mutate(&request)
			before := coordinator.calls[providercoord.RoleBuilder]
			if _, err := runner.Run(ctx, request); err == nil || coordinator.calls[providercoord.RoleBuilder] != before {
				t.Fatalf("err=%v calls=%v", err, coordinator.calls)
			}
		})
	}
	_ = plannerResult
}

type lineageCoordinator struct {
	db           *store.Store
	configDigest string
	signer       *contracts.DrainSigner
	bindings     map[providercoord.Role]contracts.RuntimeBinding
	calls        map[providercoord.Role]int
	err          error
}

func (c *lineageCoordinator) Run(ctx context.Context, request providercoord.Request) providercoord.Result {
	if c.calls == nil {
		c.calls = map[providercoord.Role]int{}
	}
	c.calls[request.Role]++
	binding := c.bindings[request.Role]
	input := request.Input
	input.Provider, input.AuthMode = binding.Identity, binding.AuthMode
	input.LeaderEpoch, input.RunnerEpoch, input.ExpectedVersion = request.Fence.LeaderEpoch, request.Fence.RunnerEpoch, request.ExpectedVersion
	claim, err := c.db.BeginProviderAttempt(ctx, store.ProviderAttemptRequest{Ref: request.Input.Ticket, ExpectedVersion: request.ExpectedVersion, Fence: request.Fence, Phase: request.Input.Phase, Role: string(request.Role), Binding: binding, ConfigDigest: c.configDigest, Capacity: 1, At: time.Now().UTC(), Repository: request.Input.Repository, Worktree: request.Input.Worktree, WorktreeIdentity: request.Input.WorktreeIdentity, BaseSHA: request.Input.BaseSHA, SupervisorKey: c.signer.PublicKey(), Input: input})
	if err != nil {
		c.err = fmt.Errorf("%w input=%+v binding=%+v", err, input, binding)
		return providercoord.Result{Code: providercoord.Failed}
	}
	if err := c.db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: 321, PGID: 321, BootIdentity: "lineage", ProcessStartIdentity: "lineage" + fmt.Sprint(claim.ID), Worktree: claim.Worktree}); err != nil {
		c.err = err
		return providercoord.Result{Code: providercoord.Failed}
	}
	artifact, changed := lineageArtifact(request)
	proof, err := c.signer.ProveDrained(lineageDrain(claim))
	if err != nil {
		c.err = err
		return providercoord.Result{Code: providercoord.Failed}
	}
	if _, err := c.db.CompleteProviderAttemptSuccess(ctx, claim, proof, request.ExpectedVersion, request.Fence, contracts.PhaseResult{Provider: binding.Identity, Artifact: artifact, ChangedFiles: changed, UsageTrusted: true, UsageUnits: 1}, request.Validation, time.Now().UTC()); err != nil {
		c.err = err
		return providercoord.Result{Code: providercoord.Failed}
	}
	return providercoord.Result{Code: providercoord.Completed, ProviderResult: store.ProviderAttemptResultKey{AttemptID: claim.ID, Ref: claim.Ref, Phase: claim.Phase, Attempt: claim.Attempt}}
}

func lineageArtifact(request providercoord.Request) ([]byte, []string) {
	var value any
	switch request.Role {
	case providercoord.RolePlanner:
		value = phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"lineage"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "lineage"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"none"}}
	case providercoord.RoleReviewer:
		value = phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: request.Validation.AcceptanceDigest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal/phase_lineage_test.go"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: lineageDigest("verification")}
	default:
		value = phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "lineage", ChangedFiles: []string{"internal/phase_lineage.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	}
	b, _ := json.Marshal(value)
	if request.Role == providercoord.RoleBuilder {
		return b, []string{"internal/phase_lineage.go"}
	}
	return b, nil
}

func lineageQualifications(t *testing.T, db *store.Store, supervisor *processsupervisor.Supervisor, leader uint64, stage string) map[providercoord.Role]contracts.RuntimeBinding {
	t.Helper()
	roles := []providercoord.Role{providercoord.RolePlanner, providercoord.RoleReviewer, providercoord.RoleBuilder}
	qualifications := make(map[providercoord.Role]store.ProviderQualification, len(roles))
	var stageID uint64
	for _, value := range stage {
		stageID = stageID*131 + uint64(value)
	}
	for index, role := range roles {
		created := time.Now().UTC()
		identity := domain.ProviderIdentity{Provider: "codex", Model: "lineage-" + string(role) + "-" + stage, Family: "lineage-" + string(role) + "-family-" + stage, Version: "1"}
		input := store.ProviderQualification{Channel: domain.ChannelDev, RunID: fmt.Sprintf("%032x", stageID+uint64(index+1)), Provider: identity, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), ProbeDigest: strings.Repeat("d", 64), AuthMode: "chatgpt_subscription", Profile: store.QualificationGuarded, CreatedAt: created}
		attestation, err := supervisor.AttestQualification(contracts.QualificationAttestation{Channel: input.Channel, RunID: input.RunID, Identity: input.Provider, BinaryDigest: input.BinaryDigest, PolicyDigest: input.PolicyDigest, FixtureDigest: input.FixtureDigest, AuthDigest: lineageDigest("auth-" + stage + string(role)), AuthMode: input.AuthMode, ProbeDigest: input.ProbeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: input.RunID})
		if err != nil {
			t.Fatal(err)
		}
		qualification, _, err := db.RecordAttestedProviderQualification(context.Background(), input, attestation)
		if err != nil {
			t.Fatalf("qualify %s: %v", role, err)
		}
		qualifications[role] = qualification
	}
	if _, _, err := db.SelectProviderSet(context.Background(), domain.ChannelDev, qualifications[providercoord.RolePlanner].ID, qualifications[providercoord.RoleBuilder].ID, qualifications[providercoord.RoleReviewer].ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	bindings := make(map[providercoord.Role]contracts.RuntimeBinding, len(roles))
	for _, role := range roles {
		q := qualifications[role]
		bindings[role] = contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: q.AuthDigest, AuthMode: q.AuthMode}
	}
	return bindings
}

func lineageCommandEvidence(t *testing.T, db *store.Store, ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, provider store.ProviderAttemptResultKey, intent, proof string) contracts.RepositoryCommandResultKey {
	t.Helper()
	project, err := db.Project(ctx, ref.Channel, ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	argv := []string{"go", "test", "./..."}
	argvBytes, err := json.Marshal(argv)
	if err != nil {
		t.Fatal(err)
	}
	commandDigest := "sha256:" + lineageDigestBytes(argvBytes)
	policy, spec, executable := "sha256:"+strings.Repeat("1", 64), "sha256:"+strings.Repeat("2", 64), "sha256:"+strings.Repeat("3", 64)
	provisional := store.RepositoryCommandResult{Claim: contracts.RepositoryCommandClaim{TicketRef: ref, TicketVersion: version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), Branch: worktree.Branch, BaseRef: project.BaseRef, BaseSHA: worktree.BaseSHA, CommandDigest: commandDigest, SpecDigest: spec, PolicyDigest: policy, ExecutablePath: "/usr/bin/true", ExecutableDigest: executable}}
	request := store.RepositoryCommandEvidenceRequest{Purpose: store.RepositoryCommandPurposePrebuildVerification, Ref: ref, TicketVersion: version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ProviderResult: provider, VerificationIntentDigest: intent, ProofDigest: proof, ConfigCommandDigest: commandDigest, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, PolicyDigest: policy, SpecDigest: spec, ExecutablePath: "/usr/bin/true", ExecutableDigest: executable}
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(request)
	if err != nil {
		t.Fatal(err)
	}
	intentValue := store.RepositoryCommandIntent{EffectFence: store.EffectFence{SemanticKey: semantic, Ref: ref, TicketVersion: version, Fence: fence}, RequestDigest: requestDigest, Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), Branch: worktree.Branch, BaseRef: project.BaseRef, BaseSHA: worktree.BaseSHA, CommandDigest: provisional.Claim.CommandDigest, SpecDigest: spec, PolicyDigest: policy, ExecutablePath: "/usr/bin/true", ExecutableDigest: executable}
	if _, err := db.PlanEffect(ctx, store.EffectPlan{SemanticKey: semantic, Ref: ref, Kind: "repository_command", TicketVersion: version, Fence: fence, RequestDigest: requestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueRepositoryCommandClaim(ctx, intentValue)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	launch := contracts.RepositoryCommandLaunch{PID: 654, PGID: 654, BootIdentity: "lineage", ProcessStartIdentity: "lineage-command"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 1, Duration: time.Millisecond, Observed: true, ObservedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	return contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}
}

func lineageRecover(t *testing.T, db *store.Store, supervisor *processsupervisor.Supervisor, ref domain.TicketRef, name string) (store.Ticket, domain.Fence) {
	t.Helper()
	leader, err := db.AcquireLeader(context.Background(), domain.ChannelDev, "phase-lineage-"+name)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.SetRecoveryAuthority(context.Background(), domain.ChannelDev, leader, supervisor.PublicKey()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.FenceRecoveredRunners(context.Background(), domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return ticket, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
}
func engineForLineage(t *testing.T, db *store.Store) *engine.Engine {
	t.Helper()
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	return engine.New(db, spec)
}
func lineageSignal(ticket store.Ticket, fence domain.Fence, trigger string) contracts.SignalRequest {
	return contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: trigger, Fence: fence, Attributes: map[string]string{"typed_plan_valid": "true", "independent_intent_valid": "true", "prebuild_proof_valid": "true", "verification_checkpoint_committed": "true"}, EventPayload: "{}"}
}
func lineageSigner(t *testing.T) *contracts.DrainSigner {
	t.Helper()
	s, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func lineageDrain(c store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: c.ID, Identity: c.Binding.Identity, Ref: c.Ref, Phase: c.Phase, Role: c.Role, Attempt: c.Attempt, LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch, ExpectedVersion: c.ExpectedVersion, LeaseKey: c.LeaseKey, BindingDigest: c.BindingDigest, BinaryDigest: c.Binding.BinaryDigest, PolicyDigest: c.Binding.PolicyDigest, AuthDigest: c.Binding.AuthDigest, AuthMode: c.Binding.AuthMode, Repository: c.Repository, Worktree: c.Worktree, WorktreeIdentity: c.WorktreeIdentity, BaseSHA: c.BaseSHA, RequestDigest: c.RequestDigest}
}
func lineageDigest(v string) string      { return lineageDigestBytes([]byte(v)) }
func lineageDigestBytes(v []byte) string { sum := sha256.Sum256(v); return hex.EncodeToString(sum[:]) }
func lineageOID(v string) string         { return strings.Repeat(v, 40) }
func lineageIdentity(t *testing.T, repository, worktree, branch string) []byte {
	t.Helper()
	b, err := workflowprompt.MarshalCanonicalWorktreeIdentity(gitboundary.Identity{Repository: repository, RepositoryDev: 1, RepositoryIno: 2, Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4, GitFile: "gitdir: " + repository + "/.git/worktrees/lineage\n", GitFileDev: 5, GitFileIno: 6, CommonDir: repository + "/.git", CommonDirDev: 7, CommonDirIno: 8, Origin: "https://example.invalid/lineage", PushOrigin: "https://example.invalid/lineage", BaseRef: "main", BaseHead: lineageOID("a"), HeadRef: branch, ConfigHash: "sha256:" + strings.Repeat("b", 64), HooksHash: "sha256:" + strings.Repeat("c", 64)})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
