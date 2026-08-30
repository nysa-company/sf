package workflowruntime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/repositoryexec"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
)

// This is intentionally a real macOS integration boundary.  The guarded
// repository supervisor is Darwin-only and the test must not turn a Linux
// fallback into evidence that the production composition is executable.
func TestRepositoryMaterializerRealStoreGitReplay(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("guarded repository command execution is Darwin-only")
	}
	ctx := context.Background()
	repository, worktree, base := newMaterializerGitFixture(t)
	helper := filepath.Join(t.TempDir(), "sf-git-exec")
	sfBinary := filepath.Join(t.TempDir(), "sf")
	build := exec.Command("go", "build", "-o", helper, "./cmd/sf-git-exec")
	build.Dir = repoRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build git helper: %v: %s", err, output)
	}
	build = exec.Command("go", "build", "-o", sfBinary, "./cmd/sf")
	build.Dir = repoRoot(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build sf gate: %v: %s", err, output)
	}
	runner := git.Runner{Home: filepath.Join(t.TempDir(), "git-home"), ExecHelper: helper, TestLocalTransport: true}
	if err := os.MkdirAll(runner.Home, 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := runner.Snapshot(ctx, worktree, "main")
	if err != nil {
		t.Fatalf("snapshot worktree: %v", err)
	}
	base = identity.BaseHead

	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "workflow.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("real", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "real", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "real", Ticket: "SF-real-materializer"}
	source := []byte("real materializer source")
	sourceSum := sha256.Sum256(source)
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: hex.EncodeToString(sourceSum[:]), Source: source, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "real-materializer")
	if err != nil {
		t.Fatal(err)
	}
	branch := "sf/dev/0123456789abcdef/01234567-0123456789abcdef"
	started, err := db.StartOrAdopt(ctx, ref, 1, branch, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: branch, IdentityJSON: identityJSON, BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}

	planner := materializerQualification(t, db, "11111111111111111111111111111111", "planner")
	builder := materializerQualification(t, db, "22222222222222222222222222222222", "builder")
	reviewer := materializerQualification(t, db, "33333333333333333333333333333333", "reviewer")
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, planner.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	providers := &materializerProviderRunner{db: db, configDigest: configDigest, repository: repository, bindings: map[domain.Phase]contracts.RuntimeBinding{
		domain.PhasePlanning:     materializerBinding(planner),
		domain.PhaseVerification: materializerBinding(reviewer),
		domain.PhaseBuild:        materializerBinding(builder),
	}, signer: signer, worktree: worktree}
	baseEngine, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	state := &materializerFaultEngine{StateMachine: engine.New(db, baseEngine), failVerification: true, failCandidate: true}
	supervisor := processsupervisor.RepositoryCommandSupervisor{Executable: sfBinary, GitRunner: runner, SoftDrain: time.Second, HardDrain: time.Second}
	materializer := workflowruntime.RepositoryMaterializer{Store: db, Git: git.Runner{Home: runner.Home, ExecHelper: helper, TestLocalTransport: true, MutationAuthority: db}, Executor: repositoryexec.Executor{Authority: db, Supervisor: supervisor}}
	worker := workflowworker.Worker{Evidence: db, Engine: state, Runner: providers, Checkpoint: materializer, Candidate: materializer, CheckpointMaterializer: materializer, CandidateMaterializer: materializer}

	if _, err := worker.Run(ctx, ref, fence); err != nil {
		t.Fatalf("planning: %v", err)
	}
	cancel, cancellation := context.WithCancel(ctx)
	failingMaterializer := materializer
	failingAuthority := &cancelFirstGitMutationAuthority{Store: db, Cancel: cancellation}
	failingMaterializer.Git.MutationAuthority = failingAuthority
	worker.Checkpoint = failingMaterializer
	worker.CheckpointMaterializer = failingMaterializer
	if _, err := worker.Run(cancel, ref, fence); err == nil {
		t.Fatal("expected canceled pre-prepare checkpoint commit")
	}
	cancellation()
	if failingAuthority.Claim.SemanticKey == "" {
		t.Fatal("canceled Git boundary did not receive a Store-issued claim")
	}
	if effect, effectErr := db.Effect(ctx, failingAuthority.Claim.SemanticKey); effectErr != nil || effect.State != store.EffectFailed {
		t.Fatalf("canceled commit effect=%+v err=%v", effect, effectErr)
	}
	if _, factsErr := db.GitMutationIntentFacts(ctx, failingAuthority.Claim.SemanticKey); !errors.Is(factsErr, store.ErrGitMutationIntent) {
		t.Fatalf("canceled unprepared intent was not retired: %v", factsErr)
	}
	if leases, leaseErr := db.ActiveGitMutationLeases(ctx, domain.ChannelDev); leaseErr != nil || len(leases) != 0 {
		t.Fatalf("canceled commit left leases=%+v err=%v", leases, leaseErr)
	}
	if leases, leaseErr := db.ActiveRepositoryCommandLeases(ctx, domain.ChannelDev); leaseErr != nil || len(leases) != 0 {
		t.Fatalf("canceled checkpoint left repository-command leases=%+v err=%v", leases, leaseErr)
	}
	worker.Checkpoint = materializer
	worker.CheckpointMaterializer = materializer
	if _, err := worker.Run(ctx, ref, fence); err == nil {
		t.Fatal("expected injected response loss after checkpoint evidence")
	}
	if _, err := worker.Run(ctx, ref, fence); err != nil {
		t.Fatalf("verification replay: %v", err)
	}
	if providers.calls[domain.PhaseVerification] != 1 {
		t.Fatalf("reviewer reran on checkpoint replay: %d", providers.calls[domain.PhaseVerification])
	}

	if _, err := worker.Run(ctx, ref, fence); err == nil {
		t.Fatal("expected injected response loss after candidate evidence")
	}
	candidateBefore, err := db.LatestCandidate(ctx, ref)
	if err != nil || candidateBefore.Commit.CommitOID == "" || candidateBefore.CommandBinding.Key.SemanticKey == "" {
		t.Fatalf("candidate evidence=%+v err=%v", candidateBefore, err)
	}
	if _, err := worker.Run(ctx, ref, fence); err != nil {
		t.Fatalf("candidate replay: %+v", err)
	}
	candidateAfter, err := db.LatestCandidate(ctx, ref)
	if err != nil || candidateAfter.Commit != candidateBefore.Commit || candidateAfter.CommandBinding.Key != candidateBefore.CommandBinding.Key {
		t.Fatalf("candidate replay changed evidence before=%+v after=%+v err=%v", candidateBefore, candidateAfter, err)
	}
	if providers.calls[domain.PhasePlanning] != 1 || providers.calls[domain.PhaseVerification] != 1 || providers.calls[domain.PhaseBuild] != 1 {
		t.Fatalf("provider rerun counts=%v", providers.calls)
	}
	ticket, err := db.Ticket(ctx, ref)
	if err != nil || ticket.State != domain.StatePublishing {
		t.Fatalf("ticket=%+v err=%v", ticket, err)
	}
	if got := rawMaterializerGit(t, worktree, "rev-list", "--count", "HEAD"); got != "3" {
		t.Fatalf("expected base, checkpoint, candidate commits; count=%q", got)
	}
	if got := rawMaterializerGit(t, worktree, "show", "--format=%s", "--no-patch", "HEAD"); got == "" {
		t.Fatal("candidate commit was not observed")
	}
}

type cancelFirstGitMutationAuthority struct {
	Store  *store.Store
	Cancel context.CancelFunc
	once   sync.Once
	Claim  contracts.GitMutationClaim
}

func (a *cancelFirstGitMutationAuthority) AcquireGitMutation(ctx context.Context, claim contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	a.Claim = claim
	a.once.Do(a.Cancel)
	return a.Store.AcquireGitMutation(ctx, claim)
}

type materializerProviderRunner struct {
	db           *store.Store
	configDigest string
	repository   string
	worktree     string
	bindings     map[domain.Phase]contracts.RuntimeBinding
	signer       *contracts.DrainSigner
	calls        map[domain.Phase]int
}

func (r *materializerProviderRunner) Run(ctx context.Context, req workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	if r.calls == nil {
		r.calls = map[domain.Phase]int{}
	}
	r.calls[req.Phase]++
	role := map[domain.Phase]string{domain.PhasePlanning: "planner", domain.PhaseVerification: "reviewer", domain.PhaseBuild: "builder"}[req.Phase]
	binding := r.bindings[req.Phase]
	input := contracts.PhaseInput{Ticket: req.Ticket.Ref, Phase: req.Phase, LeaderEpoch: req.Fence.LeaderEpoch, RunnerEpoch: req.Fence.RunnerEpoch, ExpectedVersion: req.Ticket.Version, Prompt: "real materializer integration", Repository: r.repository, Worktree: r.worktree, WorktreeIdentity: string(req.Worktree.IdentityJSON), BaseSHA: req.Worktree.BaseSHA, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	claim, err := r.db.BeginProviderAttempt(ctx, store.ProviderAttemptRequest{Ref: req.Ticket.Ref, ExpectedVersion: req.Ticket.Version, Fence: req.Fence, Phase: req.Phase, Role: role, Binding: binding, ConfigDigest: r.configDigest, Capacity: 1, At: time.Now().UTC(), Repository: r.repository, Worktree: r.worktree, WorktreeIdentity: string(req.Worktree.IdentityJSON), BaseSHA: req.Worktree.BaseSHA, SupervisorKey: r.signer.PublicKey(), Input: input})
	if err != nil {
		return workflowworker.PhaseResult{}, fmt.Errorf("begin %s: %w", req.Phase, err)
	}
	if err := r.db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: 99, PGID: 99, BootIdentity: "real-materializer", ProcessStartIdentity: "real-materializer", Worktree: claim.Worktree}); err != nil {
		return workflowworker.PhaseResult{}, err
	}
	artifact, changed, validation, err := r.artifact(req)
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	result := contracts.PhaseResult{Outcome: "completed", Provider: binding.Identity, Artifact: artifact, ChangedFiles: changed, UsageTrusted: true, UsageUnits: 1}
	if _, err := phaseartifact.Parse(req.Phase, result, validation); err != nil {
		return workflowworker.PhaseResult{}, fmt.Errorf("parse %s: %w", req.Phase, err)
	}
	proof, err := r.signer.ProveDrained(materializerDrainRequest(claim))
	if err != nil {
		return workflowworker.PhaseResult{}, err
	}
	if _, err := r.db.CompleteProviderAttemptSuccess(ctx, claim, proof, req.Ticket.Version, req.Fence, result, validation, time.Now().UTC()); err != nil {
		return workflowworker.PhaseResult{}, err
	}
	key := store.ProviderAttemptResultKey{AttemptID: claim.ID, Ref: claim.Ref, Phase: claim.Phase, Attempt: claim.Attempt}
	return workflowworker.PhaseResult{ProviderResult: key}, nil
}

func (r *materializerProviderRunner) artifact(req workflowworker.PhaseRequest) ([]byte, []string, phaseartifact.Validation, error) {
	validation := phaseartifact.Validation{TicketType: req.Ticket.Type}
	switch req.Phase {
	case domain.PhasePlanning:
		value := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"real materializer"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "real"}, Paths: []string{"tracked_test.go"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"none"}}
		artifact, err := json.Marshal(value)
		return artifact, nil, validation, err
	case domain.PhaseVerification:
		identity, err := workflowprompt.NewPlanIdentity(*req.Plan.Document.Planner)
		if err != nil {
			return nil, nil, validation, err
		}
		value := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: identity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"proof.txt"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: materializerDigest("verification")}
		validation.AcceptanceDigest = identity.Digest
		artifact, err := json.Marshal(value)
		return artifact, nil, validation, err
	case domain.PhaseBuild:
		if err := os.WriteFile(filepath.Join(r.worktree, "tracked_test.go"), []byte("package example\n\nimport \"testing\"\n\nfunc TestFeature(t *testing.T) {}\n"), 0o600); err != nil {
			return nil, nil, validation, err
		}
		check := exec.Command("go", "test", "./...")
		check.Dir = r.worktree
		if output, checkErr := check.CombinedOutput(); checkErr != nil {
			return nil, nil, validation, fmt.Errorf("builder fixture test: %v: %s", checkErr, output)
		}
		value := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "real mutation", ChangedFiles: []string{"tracked_test.go"}, Commands: [][]string{{"go", "test", "./..."}}}
		artifact, err := json.Marshal(value)
		return artifact, []string{"tracked_test.go"}, validation, err
	default:
		return nil, nil, validation, errors.New("unexpected phase")
	}
}

type materializerFaultEngine struct {
	workflowworker.StateMachine
	failVerification bool
	failCandidate    bool
}

func (e *materializerFaultEngine) SignalVerification(ctx context.Context, req contracts.SignalRequest) (contracts.TransitionResult, error) {
	if e.failVerification {
		e.failVerification = false
		return contracts.TransitionResult{}, errors.New("injected checkpoint response loss")
	}
	return e.StateMachine.SignalVerification(ctx, req)
}
func (e *materializerFaultEngine) SignalCandidate(ctx context.Context, req contracts.SignalRequest, candidate domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	if e.failCandidate {
		e.failCandidate = false
		return contracts.TransitionResult{}, errors.New("injected candidate response loss")
	}
	return e.StateMachine.SignalCandidate(ctx, req, candidate)
}

func newMaterializerGitFixture(t *testing.T) (repository, worktree, base string) {
	t.Helper()
	root := t.TempDir()
	repository = filepath.Join(root, "repo")
	worktree = filepath.Join(root, "worktree")
	remote := filepath.Join(root, "remote.git")
	for _, args := range [][]string{{"init", "-b", "main", repository}, {"init", "--bare", remote}} {
		runMaterializerGit(t, root, args...)
	}
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test\n\ngo 1.23\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "tracked_test.go"), []byte("package example\n\nimport \"testing\"\n\nfunc TestFeature(t *testing.T) { t.Fatal(\"red\") }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "proof.txt"), []byte("proof\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runMaterializerGit(t, repository, "config", "user.email", "sf@example.test")
	runMaterializerGit(t, repository, "config", "user.name", "sf integration")
	runMaterializerGit(t, repository, "add", "--", ".")
	runMaterializerGit(t, repository, "commit", "-m", "base")
	runMaterializerGit(t, repository, "remote", "add", "origin", remote)
	runMaterializerGit(t, repository, "push", "-u", "origin", "main")
	branch := "sf/dev/0123456789abcdef/01234567-0123456789abcdef"
	runMaterializerGit(t, repository, "worktree", "add", "-b", branch, worktree, "main")
	repository, _ = filepath.EvalSymlinks(repository)
	worktree, _ = filepath.EvalSymlinks(worktree)
	remote, _ = filepath.EvalSymlinks(remote)
	base = rawMaterializerGit(t, worktree, "rev-parse", "main^{commit}")
	return repository, worktree, base
}

func runMaterializerGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, output)
	}
}
func rawMaterializerGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output))
}
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
func materializerQualification(t *testing.T, db *store.Store, run, name string) store.ProviderQualification {
	t.Helper()
	q, _, err := db.RecordProviderQualification(context.Background(), store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: name + "-family", Version: "1.0.0"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Date(2026, 8, 29, 18, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return q
}
func materializerBinding(q store.ProviderQualification) contracts.RuntimeBinding {
	return contracts.RuntimeBinding{Identity: q.Provider, BinaryDigest: q.BinaryDigest, PolicyDigest: q.PolicyDigest, FixtureDigest: q.FixtureDigest, AuthDigest: materializerDigest("auth:" + q.Provider.Model)}
}
func materializerDrainRequest(c store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: c.ID, Identity: c.Binding.Identity, Ref: c.Ref, Phase: c.Phase, Role: c.Role, Attempt: c.Attempt, LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch, ExpectedVersion: c.ExpectedVersion, LeaseKey: c.LeaseKey, BindingDigest: c.BindingDigest, BinaryDigest: c.Binding.BinaryDigest, PolicyDigest: c.Binding.PolicyDigest, AuthDigest: c.Binding.AuthDigest, AuthMode: c.Binding.AuthMode, Repository: c.Repository, Worktree: c.Worktree, WorktreeIdentity: c.WorktreeIdentity, BaseSHA: c.BaseSHA, RequestDigest: c.RequestDigest}
}
func materializerDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
