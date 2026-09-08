package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/transport"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type providerRetryRoutedProvider struct {
	*testkit.ScriptedProvider
	route string
}

func newProviderRetryRoutedProvider(route, family string) *providerRetryRoutedProvider {
	return &providerRetryRoutedProvider{
		ScriptedProvider: testkit.NewScriptedProvider(domain.ProviderIdentity{
			Provider: "codex",
			Model:    route + "-model",
			Family:   family,
			Version:  "v1",
		}),
		route: route,
	}
}

func (p *providerRetryRoutedProvider) Name() string { return p.route }

func (p *providerRetryRoutedProvider) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	binding, err := p.ScriptedProvider.Binding(ctx)
	if err != nil {
		return contracts.RuntimeBinding{}, err
	}
	binding.AuthMode = "chatgpt_subscription"
	return binding, nil
}

type providerRetryTicketSource struct {
	workflowruntime.StoreTicketSource
	once        sync.Once
	initialList chan struct{}
}

func (s *providerRetryTicketSource) ListTickets(ctx context.Context, channel domain.Channel) ([]store.Ticket, error) {
	tickets, err := s.StoreTicketSource.ListTickets(ctx, channel)
	s.once.Do(func() { close(s.initialList) })
	return tickets, err
}

func providerRetryGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", directory}, args...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func providerRetryQualification(t *testing.T, database *store.Store, supervisor *providerControlSupervisor, channel domain.Channel, leader uint64, provider *providerRetryRoutedProvider, runID, probeDigest string) store.ProviderQualification {
	t.Helper()
	binding, err := provider.Binding(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC()
	input := store.ProviderQualification{
		Channel: channel, RunID: runID, Provider: binding.Identity,
		BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest,
		AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: probeDigest,
		Profile: store.QualificationGuarded, CreatedAt: created,
	}
	attestation, err := supervisor.Signer.SignQualification(contracts.QualificationAttestation{
		Channel: channel, RunID: runID, Identity: binding.Identity,
		BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest,
		AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: probeDigest,
		Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: runID,
	})
	if err != nil {
		t.Fatal(err)
	}
	qualification, _, err := database.RecordAttestedProviderQualification(t.Context(), input, attestation)
	if err != nil {
		t.Fatal(err)
	}
	return qualification
}

type providerRetryCheckpointMaterializer struct {
	t        *testing.T
	database *store.Store
}

func (m providerRetryCheckpointMaterializer) MaterializeVerificationCheckpoint(_ context.Context, request workflowworker.PhaseRequest, artifact phaseartifact.Verification, key store.ProviderAttemptResultKey) (workflowworker.VerificationCheckpoint, error) {
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(artifact)
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	proof, err := workflowprompt.CanonicalVerificationProofBytes(artifact)
	if err != nil {
		return workflowworker.VerificationCheckpoint{}, err
	}
	command := daemonFixtureCompleteCommand(
		m.t,
		m.database,
		request.Ticket.Ref,
		request.Ticket.Version,
		request.Fence,
		store.RepositoryCommandPurposePrebuildVerification,
		key,
		daemonFixtureDigest(string(intent)),
		daemonFixtureDigest(string(proof)),
		"",
		"",
	)
	checkpoint := strings.Repeat("c", 40)
	return workflowworker.VerificationCheckpoint{
		ID:            checkpoint,
		Commit:        store.CommitObservation{CommitOID: checkpoint, ParentOID: request.Worktree.BaseSHA, TreeOID: strings.Repeat("d", 40)},
		CommandResult: command,
	}, nil
}

func (providerRetryCheckpointMaterializer) AuthenticateVerificationCheckpoint(context.Context, workflowworker.PhaseRequest, phaseartifact.Verification, workflowworker.VerificationCheckpoint) error {
	return nil
}

func TestDaemonProviderRetryRetainsProviderWrittenInvalidArtifactWorktree(t *testing.T) {
	planner := newProviderRetryRoutedProvider("retry-builder", "retry-builder-family")
	planner.Add(domain.PhasePlanning, testkit.ProviderStep{
		Artifact: []byte(`{"schema":"sf.planner/v1","acceptance":["retry retains invalid provider output"],"proof":{"kind":"acceptance","command":["go","test","./..."],"details":"provider retry composition"},"paths":["app"],"commands":[["go","test","./..."]],"risks":["none"],"questions":[]}`),
	})
	reviewer := newProviderRetryRoutedProvider("retry-reviewer", "retry-reviewer-family")
	dirtyRelative := filepath.Join("app", "tests", "job-count.test.js")
	dirtyContent := []byte("untrusted verifier output\n")
	reviewer.Add(domain.PhaseVerification, testkit.ProviderStep{
		Artifact:   []byte(`{"schema":"invalid-reviewer/v1"}`),
		WriteFiles: map[string][]byte{dirtyRelative: dirtyContent},
		UsageUnits: 1,
	})
	reviewer.Add(domain.PhaseVerification, testkit.ProviderStep{
		Artifact:   []byte(`{"schema":"still-invalid-reviewer/v1"}`),
		UsageUnits: 1,
	})

	registry := providercoord.NewRegistry()
	if err := registry.Register(t.Context(), planner); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(t.Context(), reviewer); err != nil {
		t.Fatal(err)
	}
	supervisor := &providerControlSupervisor{Supervisor: testkit.NewSupervisor(), entered: make(chan contracts.DrainRequest, 4)}

	var scheduler *workflowruntime.Scheduler
	initialList := make(chan struct{})
	var gitRunner gitboundary.Runner
	cfg, paths := lifecycleConfig(t, func(deps RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		runtimeGit := gitRunner
		runtimeGit.MutationAuthority = deps.Store
		tickets := &providerRetryTicketSource{
			StoreTicketSource: workflowruntime.StoreTicketSource{Store: deps.Store},
			initialList:       initialList,
		}
		checkpoint := providerRetryCheckpointMaterializer{t: t, database: deps.Store}
		worker := workflowworker.Worker{
			Evidence:               deps.Store,
			Engine:                 deps.Engine,
			Runner:                 workflowruntime.NewPhaseRunner(deps.Store, deps.ProviderCoordinator),
			Checkpoint:             checkpoint,
			CheckpointMaterializer: checkpoint,
		}
		scheduler = workflowruntime.NewScheduler(
			domain.ChannelStable,
			tickets,
			worktreecoord.Coordinator{Store: deps.Store, Git: runtimeGit},
			worker,
		)
		runtime, err := workflowruntime.NewRuntimeWithConfig(scheduler, workflowruntime.RuntimeConfig{Interval: time.Hour, Workers: 1})
		if err != nil {
			return WorkflowRuntimeComponents{}, err
		}
		controller, err := runtimecontrol.New(deps.Store, runtime.ControlBundle(), nil, runtimeGit)
		if err != nil {
			_ = runtime.Close()
			return WorkflowRuntimeComponents{}, err
		}
		return WorkflowRuntimeComponents{Runtime: runtime, Controller: controller}, nil
	})
	canonicalRoot, err := filepath.EvalSymlinks(paths.Root)
	if err != nil {
		t.Fatal(err)
	}
	paths = config.ChannelPaths{
		Root: canonicalRoot, Database: filepath.Join(canonicalRoot, "sf.sqlite"), Socket: filepath.Join(canonicalRoot, "run", "sf.sock"),
		Logs: filepath.Join(canonicalRoot, "logs"), Events: filepath.Join(canonicalRoot, "events"), Worktrees: filepath.Join(canonicalRoot, "worktrees"), Backups: filepath.Join(canonicalRoot, "backups"),
	}
	cfg.Paths = paths
	repository := filepath.Join(canonicalRoot, "repo")
	remote := filepath.Join(canonicalRoot, "origin.git")
	if err := os.MkdirAll(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	providerRetryGit(t, canonicalRoot, "init", "--bare", remote)
	providerRetryGit(t, repository, "init", "-b", "main")
	providerRetryGit(t, repository, "config", "user.name", "fixture")
	providerRetryGit(t, repository, "config", "user.email", "fixture@example.test")
	if err := os.MkdirAll(filepath.Join(repository, "app"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "app", "README.md"), []byte("provider retry fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	providerRetryGit(t, repository, "add", "app/README.md")
	providerRetryGit(t, repository, "commit", "-m", "provider retry baseline")
	providerRetryGit(t, repository, "remote", "add", "origin", remote)
	providerRetryGit(t, repository, "push", "origin", "main:refs/heads/main")
	cfg.Projects[0].Path = repository
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("demo", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Projects[0].ConfigGeneration, cfg.Projects[0].ConfigDigest, cfg.Projects[0].ConfigSnapshot = 1, digest, snapshot
	gitRunner = gitboundary.Runner{
		Binary: "/usr/bin/git", Home: filepath.Join(canonicalRoot, "git-home"), TestLocalTransport: true,
		Run: func(ctx context.Context, binary string, argv, env []string) ([]byte, error) {
			command := exec.CommandContext(ctx, binary, argv...)
			command.Env = env
			return command.Output()
		},
	}
	cfg.ProviderSupervisor = supervisor
	cfg.RecoveryAuthorityKey = supervisor.PublicKey()
	cfg.ProviderCoordinatorFactory = func(database *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
		return providercoord.New(registry, map[providercoord.Role]providercoord.Route{
			providercoord.RolePlanner:  {Primary: planner.Name(), Capacity: 1},
			providercoord.RoleBuilder:  {Primary: planner.Name(), Capacity: 1},
			providercoord.RoleReviewer: {Primary: reviewer.Name(), Capacity: 1},
		}, database, nil, process)
	}

	d, err := Start(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	select {
	case <-initialList:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not finish its initial empty ticket scan")
	}
	if scheduler == nil {
		t.Fatal("provider retry runtime was not composed")
	}
	plannerQualification := providerRetryQualification(t, d.store, supervisor, d.channel, d.epoch, planner, strings.Repeat("1", 32), strings.Repeat("a", 64))
	reviewerQualification := providerRetryQualification(t, d.store, supervisor, d.channel, d.epoch, reviewer, strings.Repeat("2", 32), strings.Repeat("b", 64))
	if _, _, err := d.store.SelectProviderSet(t.Context(), d.channel, plannerQualification.ID, plannerQualification.ID, reviewerQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	ref := domain.TicketRef{Channel: d.channel, Project: "demo", Ticket: "SF-provider-retry-dirty-composition"}
	source := []byte("Add a job-count API while retaining invalid Reviewer output for operator inspection.")
	sourceSum := sha256.Sum256(source)
	if _, created, err := d.store.SubmitTicketFenced(t.Context(), store.Ticket{
		Ref: ref, SourceDigest: hex.EncodeToString(sourceSum[:]), Source: source,
		Type: domain.TicketFeature, MergeMode: domain.MergeGuarded,
		Title: "Count jobs", Problem: "The API needs a job count.", Acceptance: []string{"count all jobs"},
		MaxDuration: time.Hour, MaxCostMicroUSD: 100,
	}, false, d.epoch); err != nil || !created {
		t.Fatalf("submit created=%v err=%v", created, err)
	}
	start := d.Handle(t.Context(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{
		Version: api.Version, RequestID: "start-provider-retry-composition", Method: "ticket.start", Ticket: string(ref.Ticket),
		Parameters: json.RawMessage(`{"channel":"stable","project":"demo"}`),
	})
	if !start.OK {
		t.Fatalf("start response=%+v", start)
	}
	started, err := d.store.Ticket(t.Context(), ref)
	if err != nil || started.State != domain.StatePlanning || started.ConfigGeneration != 1 || started.ConfigDigest != digest {
		t.Fatalf("started ticket=%+v err=%v", started, err)
	}

	first := scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch})
	if first.Outcome != workflowruntime.OutcomeInvoked || first.Worker.State != domain.StateVerifying || !first.Worker.Transitioned {
		t.Fatalf("planning tick=%+v", first)
	}
	verifying, err := d.store.Ticket(t.Context(), ref)
	if err != nil || verifying.State != domain.StateVerifying {
		t.Fatalf("verifying ticket=%+v err=%v", verifying, err)
	}
	plan, err := d.store.Plan(t.Context(), ref)
	if err != nil || plan.Document.Planner == nil {
		t.Fatalf("stored plan=%+v err=%v", plan, err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	validVerification, err := json.Marshal(phaseartifact.Verification{
		Schema:           "sf.verification/v1",
		AcceptanceDigest: planIdentity.Digest,
		ProofKind:        phaseartifact.ProofAcceptance,
		OwnedFiles:       []string{dirtyRelative},
		Command:          []string{"go", "test", "./..."},
		PrebuildOutcome:  "red",
		EvidenceDigest:   daemonFixtureDigest("provider-retry-verification"),
	})
	if err != nil {
		t.Fatal(err)
	}
	reviewer.Add(domain.PhaseVerification, testkit.ProviderStep{Artifact: validVerification, UsageUnits: 1})
	registered, err := d.store.Worktree(t.Context(), ref)
	if err != nil || providerRetryGit(t, registered.Path, "status", "--porcelain") != "" {
		t.Fatalf("registered worktree=%+v err=%v", registered, err)
	}

	second := scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch})
	if second.Outcome != workflowruntime.OutcomeInvoked || second.Worker.State != domain.StatePaused || !second.Worker.Transitioned {
		t.Fatalf("verification exhaustion tick=%+v", second)
	}
	paused, err := d.store.Ticket(t.Context(), ref)
	if err != nil || paused.State != domain.StatePaused || paused.ResumeState != domain.StateVerifying || paused.Version != verifying.Version+1 || paused.RunnerEpoch != verifying.RunnerEpoch {
		t.Fatalf("provider exhaustion pause=%+v verifying=%+v err=%v", paused, verifying, err)
	}
	attemptsBefore, err := d.store.ProviderAttempts(t.Context(), ref)
	if err != nil || len(attemptsBefore) != 3 || attemptsBefore[0].Phase != domain.PhasePlanning || attemptsBefore[0].State != "completed" || attemptsBefore[1].Phase != domain.PhaseVerification || attemptsBefore[1].Attempt != 1 || attemptsBefore[1].Outcome != "invalid_artifact" || attemptsBefore[2].Phase != domain.PhaseVerification || attemptsBefore[2].Attempt != 2 || attemptsBefore[2].Outcome != "invalid_artifact" {
		t.Fatalf("provider attempts=%+v err=%v", attemptsBefore, err)
	}
	failures, err := d.store.ProviderArtifactFailures(t.Context(), ref)
	if err != nil || len(failures) != 2 || failures[0].Reason != contracts.ArtifactFailureSchema || failures[1].Reason != contracts.ArtifactFailureSchema {
		t.Fatalf("artifact failures=%+v err=%v", failures, err)
	}
	if active, err := d.store.ActiveProviderAttempts(t.Context(), d.channel); err != nil || len(active) != 0 {
		t.Fatalf("active provider attempts=%+v err=%v", active, err)
	}
	dirtyPath := filepath.Join(registered.Path, dirtyRelative)
	if got, err := os.ReadFile(dirtyPath); err != nil || !slices.Equal(got, dirtyContent) {
		t.Fatalf("retained provider output=%q err=%v", got, err)
	}
	if status := providerRetryGit(t, registered.Path, "status", "--porcelain", "--untracked-files=all"); status != "?? app/tests/job-count.test.js" {
		t.Fatalf("dirty worktree status=%q", status)
	}
	if disposition, err := d.store.ProviderRetryDisposition(t.Context(), paused); err != nil || disposition != store.ProviderRetryEligible {
		t.Fatalf("retry disposition=%v err=%v", disposition, err)
	}
	eventsBefore, err := d.store.Events(t.Context(), d.channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	plannerCallsBefore := planner.CallsSnapshot()
	reviewerCallsBefore := reviewer.CallsSnapshot()

	response := daemonControl(d, ref.Ticket, "retry")
	if response.OK || response.Mutation.Attempted || response.Error == nil || response.Error.Code != "provider_retry_worktree_unready" || response.Error.Retryable || response.NextAction == nil || strings.Join(response.NextAction.Argv, " ") != "sf take "+string(ref.Ticket) {
		t.Fatalf("dirty provider retry response=%+v", response)
	}
	after, err := d.store.Ticket(t.Context(), ref)
	if err != nil || after.State != paused.State || after.ResumeState != paused.ResumeState || after.Version != paused.Version || after.RunnerEpoch != paused.RunnerEpoch {
		t.Fatalf("retry mutated ticket before=%+v after=%+v err=%v", paused, after, err)
	}
	eventsAfter, err := d.store.Events(t.Context(), d.channel, 0, 100)
	if err != nil || len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("retry events before=%d after=%d err=%v", len(eventsBefore), len(eventsAfter), err)
	}
	attemptsAfter, err := d.store.ProviderAttempts(t.Context(), ref)
	if err != nil || len(attemptsAfter) != len(attemptsBefore) {
		t.Fatalf("retry provider attempts before=%d after=%d err=%v", len(attemptsBefore), len(attemptsAfter), err)
	}
	if !slices.Equal(planner.CallsSnapshot(), plannerCallsBefore) || !slices.Equal(reviewer.CallsSnapshot(), reviewerCallsBefore) {
		t.Fatalf("retry relaunched provider planner=%v reviewer=%v", planner.CallsSnapshot(), reviewer.CallsSnapshot())
	}
	if got, err := os.ReadFile(dirtyPath); err != nil || !slices.Equal(got, dirtyContent) {
		t.Fatalf("retry did not retain provider output=%q err=%v", got, err)
	}
	if status := providerRetryGit(t, registered.Path, "status", "--porcelain", "--untracked-files=all"); status != "?? app/tests/job-count.test.js" {
		t.Fatalf("retry changed dirty worktree status=%q", status)
	}

	if err := os.Remove(dirtyPath); err != nil {
		t.Fatal(err)
	}
	if status := providerRetryGit(t, registered.Path, "status", "--porcelain", "--untracked-files=all"); status != "" {
		t.Fatalf("restored worktree is not pristine status=%q", status)
	}
	reviewerCallsBeforeRetry := reviewer.CallsSnapshot()
	retry := daemonControl(d, ref.Ticket, "retry")
	if !retry.OK || !retry.Mutation.Attempted || retry.Mutation.Observed || retry.Mutation.Kind != "ticket_retry" {
		t.Fatalf("clean provider retry response=%+v", retry)
	}
	active, err := d.store.Ticket(t.Context(), ref)
	if err != nil || active.State != domain.StateVerifying || active.Version != paused.Version+1 || active.RunnerEpoch != paused.RunnerEpoch {
		t.Fatalf("active provider retry=%+v paused=%+v err=%v", active, paused, err)
	}
	if !slices.Equal(reviewer.CallsSnapshot(), reviewerCallsBeforeRetry) {
		t.Fatalf("control request launched provider calls before=%v after=%v", reviewerCallsBeforeRetry, reviewer.CallsSnapshot())
	}
	third := scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: d.epoch})
	if third.Outcome != workflowruntime.OutcomeInvoked || third.Worker.State != domain.StateBuilding || !third.Worker.Transitioned {
		t.Fatalf("provider retry verification tick=%+v", third)
	}
	building, err := d.store.Ticket(t.Context(), ref)
	if err != nil || building.State != domain.StateBuilding || building.Version != active.Version+1 {
		t.Fatalf("provider retry building ticket=%+v active=%+v err=%v", building, active, err)
	}
	reviewerCallsAfterRetry := reviewer.CallsSnapshot()
	if len(reviewerCallsAfterRetry) != len(reviewerCallsBeforeRetry)+1 || reviewerCallsAfterRetry[len(reviewerCallsAfterRetry)-1] != domain.PhaseVerification {
		t.Fatalf("provider retry reviewer calls before=%v after=%v", reviewerCallsBeforeRetry, reviewerCallsAfterRetry)
	}
	attemptsFinal, err := d.store.ProviderAttempts(t.Context(), ref)
	if err != nil || len(attemptsFinal) != 4 || attemptsFinal[3].Phase != domain.PhaseVerification || attemptsFinal[3].Attempt != 3 || attemptsFinal[3].State != "completed" || attemptsFinal[3].Outcome != "completed" {
		t.Fatalf("provider retry attempts=%+v err=%v", attemptsFinal, err)
	}
	registeredAfter, err := d.store.Worktree(t.Context(), ref)
	if err != nil || registeredAfter.Path != registered.Path || registeredAfter.Branch != registered.Branch || registeredAfter.BaseSHA != registered.BaseSHA || registeredAfter.HeadSHA != registered.HeadSHA || !slices.Equal(registeredAfter.IdentityJSON, registered.IdentityJSON) {
		t.Fatalf("provider retry replaced worktree before=%+v after=%+v err=%v", registered, registeredAfter, err)
	}
}
