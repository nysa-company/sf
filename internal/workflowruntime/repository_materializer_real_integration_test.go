package workflowruntime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
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

	providerScript := writeMaterializerProvider(t)
	builderAuth := writeMaterializerAuthHome(t)
	reviewerAuth := writeMaterializerAuthHome(t)
	providerSupervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	providerSupervisor.Executable = sfBinary
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, providerSupervisor.PublicKey()); err != nil {
		t.Fatal(err)
	}
	builderAdapter, err := codexprovider.New(codexprovider.Config{Route: "codex-builder", Executable: providerScript, AuthHome: builderAuth, Model: "gpt-5.6-luna"})
	if err != nil {
		t.Fatal(err)
	}
	reviewerAdapter, err := codexprovider.New(codexprovider.Config{Route: "codex-reviewer", Executable: providerScript, AuthHome: reviewerAuth, Model: "gpt-5.5"})
	if err != nil {
		t.Fatal(err)
	}
	builderBinding, err := builderAdapter.Binding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reviewerBinding, err := reviewerAdapter.Binding(ctx)
	if err != nil {
		t.Fatal(err)
	}
	builder := materializerAttestedQualification(t, db, providerSupervisor, builderBinding, "11111111111111111111111111111111")
	reviewer := materializerAttestedQualification(t, db, providerSupervisor, reviewerBinding, "22222222222222222222222222222222")
	planner := builder
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, planner.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := providerSupervisor.RegisterRuntime(builderBinding, providerScript, builderAuth); err != nil {
		t.Fatal(err)
	}
	if _, err := providerSupervisor.RegisterRuntime(reviewerBinding, providerScript, reviewerAuth); err != nil {
		t.Fatal(err)
	}
	registry := providercoord.NewRegistry()
	if err := registry.Register(ctx, builderAdapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, reviewerAdapter); err != nil {
		t.Fatal(err)
	}
	coordinator, err := providercoord.New(registry, map[providercoord.Role]providercoord.Route{
		providercoord.RolePlanner:  {Primary: builderAdapter.Name()},
		providercoord.RoleBuilder:  {Primary: builderAdapter.Name()},
		providercoord.RoleReviewer: {Primary: reviewerAdapter.Name()},
	}, db, nil, providerSupervisor)
	if err != nil {
		t.Fatal(err)
	}
	providers := workflowruntime.NewPhaseRunner(db, coordinator)
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
	_, canceledRunErr := worker.Run(cancel, ref, fence)
	if canceledRunErr == nil {
		t.Fatal("expected canceled pre-prepare checkpoint commit")
	}
	cancellation()
	if failingAuthority.Claim.SemanticKey == "" {
		t.Fatalf("canceled Git boundary did not receive a Store-issued claim: %v", canceledRunErr)
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
	assertMaterializerProviderAttempts(t, db, ref, 2)

	if _, err := worker.Run(ctx, ref, fence); err == nil {
		t.Fatal("expected injected response loss after candidate evidence")
	}
	candidateBefore, err := db.LatestCandidate(ctx, ref)
	if err != nil || candidateBefore.Commit.CommitOID == "" || candidateBefore.CommandBinding.Key.SemanticKey == "" {
		t.Fatalf("candidate evidence=%+v err=%v", candidateBefore, err)
	}
	// A direct materializer caller cannot substitute an older candidate fence
	// while a runner-recovery chain is unavailable. The rejection happens before
	// command execution or Git observation is used as fresh authority.
	storedPlan, err := db.Plan(ctx, ref)
	if err != nil || storedPlan.Document.Planner == nil {
		t.Fatalf("stored plan=%+v err=%v", storedPlan, err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*storedPlan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	storedVerification, err := db.CurrentVerification(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, verificationParsed, err := db.LoadHistoricalProviderAttemptResult(ctx, storedVerification.ProviderResult)
	if err != nil || verificationParsed.Verify == nil {
		t.Fatalf("verification provider result=%+v err=%v", verificationParsed, err)
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(*verificationParsed.Verify, storedVerification.Revision.IntentDigest, storedVerification.Revision.ProofDigest, storedVerification.Revision.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	_, builderParsed, err := db.LoadHistoricalProviderAttemptResult(ctx, candidateBefore.BuilderResult)
	if err != nil || builderParsed.Builder == nil {
		t.Fatalf("builder provider result=%+v err=%v", builderParsed, err)
	}
	buildingTicket, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	storedWorktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	staleCandidate := candidateBefore
	staleCandidate.Fence.RunnerEpoch++
	if _, err := materializer.MaterializeCandidate(ctx, workflowworker.PhaseRequest{Ticket: buildingTicket, Worktree: storedWorktree, Phase: domain.PhaseBuild, Fence: fence, Plan: &storedPlan, Verification: &storedVerification, Candidate: &staleCandidate}, planIdentity, verificationIdentity, *builderParsed.Builder, candidateBefore.BuilderResult); !errors.Is(err, workflowruntime.ErrRepositoryMaterialization) {
		t.Fatalf("stale candidate fence accepted: %v", err)
	}
	verificationBefore, err := db.CurrentVerification(ctx, ref)
	if err != nil {
		t.Fatalf("checkpoint evidence: %v", err)
	}
	candidateCommandBefore, err := db.LoadRepositoryCommandResult(ctx, candidateBefore.CommandBinding.Key)
	if err != nil {
		t.Fatalf("candidate command evidence: %v", err)
	}
	checkpointCommandBefore, err := db.LoadRepositoryCommandResult(ctx, verificationBefore.CommandBinding.Key)
	if err != nil {
		t.Fatalf("checkpoint command evidence: %v", err)
	}
	commitCountBefore := rawMaterializerGit(t, worktree, "rev-list", "--count", "HEAD")
	if _, err := worker.Run(ctx, ref, fence); err != nil {
		t.Fatalf("candidate replay: %+v", err)
	}
	candidateAfter, err := db.LatestCandidate(ctx, ref)
	if err != nil || candidateAfter.Commit != candidateBefore.Commit || candidateAfter.CommandBinding.Key != candidateBefore.CommandBinding.Key {
		t.Fatalf("candidate replay changed evidence before=%+v after=%+v err=%v", candidateBefore, candidateAfter, err)
	}
	verificationAfter, err := db.CurrentVerification(ctx, ref)
	if err != nil || verificationAfter.Checkpoint != verificationBefore.Checkpoint || verificationAfter.CommandBinding.Key != verificationBefore.CommandBinding.Key {
		t.Fatalf("candidate replay changed checkpoint evidence before=%+v after=%+v err=%v", verificationBefore, verificationAfter, err)
	}
	candidateCommandAfter, err := db.LoadRepositoryCommandResult(ctx, candidateAfter.CommandBinding.Key)
	if err != nil || candidateCommandAfter.Key != candidateCommandBefore.Key || candidateCommandAfter.ResultDigest != candidateCommandBefore.ResultDigest {
		t.Fatalf("candidate replay changed command evidence before=%+v after=%+v err=%v", candidateCommandBefore, candidateCommandAfter, err)
	}
	checkpointCommandAfter, err := db.LoadRepositoryCommandResult(ctx, verificationAfter.CommandBinding.Key)
	if err != nil || checkpointCommandAfter.Key != checkpointCommandBefore.Key || checkpointCommandAfter.ResultDigest != checkpointCommandBefore.ResultDigest {
		t.Fatalf("candidate replay changed checkpoint command before=%+v after=%+v err=%v", checkpointCommandBefore, checkpointCommandAfter, err)
	}
	assertMaterializerProviderAttempts(t, db, ref, 3)
	ticket, err := db.Ticket(ctx, ref)
	if err != nil || ticket.State != domain.StatePublishing {
		t.Fatalf("ticket=%+v err=%v", ticket, err)
	}
	if got := rawMaterializerGit(t, worktree, "rev-list", "--count", "HEAD"); got != commitCountBefore || got != "3" {
		t.Fatalf("candidate replay changed commit count before=%q after=%q", commitCountBefore, got)
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
func materializerAttestedQualification(t *testing.T, db *store.Store, supervisor *processsupervisor.Supervisor, binding contracts.RuntimeBinding, run string) store.ProviderQualification {
	t.Helper()
	created := time.Now().UTC()
	leader, err := db.LeaderEpoch(context.Background(), domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	probeDigest := materializerDigest("probe:" + run)
	attestation, err := supervisor.AttestQualification(contracts.QualificationAttestation{Channel: domain.ChannelDev, RunID: run, Identity: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: probeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: run})
	if err != nil {
		t.Fatal(err)
	}
	qualification, _, err := db.RecordAttestedProviderQualification(context.Background(), store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, Profile: store.QualificationGuarded, ProbeDigest: probeDigest, CreatedAt: created}, attestation)
	if err != nil {
		t.Fatal(err)
	}
	return qualification
}

func writeMaterializerAuthHome(t *testing.T) string {
	t.Helper()
	home := filepath.Join(t.TempDir(), "codex-home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "auth.json"), []byte(`{"fixture":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func writeMaterializerProvider(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "codex-fixture")
	script := `#!/bin/sh
set -eu
case "${1:-} ${2:-}" in
  "--version ") printf '%s\n' 'Codex 1.2.3'; exit 0 ;;
  "exec --help") printf '%s\n' '--json --output-schema --output-last-message --ephemeral --ignore-user-config --ignore-rules --config --model -C'; exit 0 ;;
  "login status") printf '%s\n' 'Logged in using ChatGPT'; exit 0 ;;
esac
last=''
previous=''
for arg in "$@"; do
  if [ "$previous" = '--output-last-message' ]; then last="$arg"; fi
  previous="$arg"
done
[ -n "$last" ]
prompt=$(cat)
model=''
previous=''
for arg in "$@"; do
  if [ "$previous" = '--model' ]; then model="$arg"; fi
  previous="$arg"
done
if [ "$model" = 'gpt-5.5' ] || printf '%s' "$prompt" | grep -qi 'independent pre-build reviewer'; then
	plan=${prompt#*PLAN=}
	plan=${plan%%WORKSPACE=*}
	digest=$(printf '%s' "$plan" | grep -Eo '"digest":"[0-9a-f]+"' | grep -Eo '[0-9a-f]{64}' | head -1 || true)
  printf '%s\n' '{"schema":"sf.verification/v1","acceptance_digest":"'"$digest"'","proof_kind":"acceptance","owned_files":["proof.txt"],"command":["go","test","./..."],"prebuild_outcome":"red","evidence_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' > "$last"
elif printf '%s' "$prompt" | grep -qi 'builder'; then
  printf '%s\n' 'package example

import "testing"

func TestFeature(t *testing.T) {}
' > tracked_test.go
  printf '%s\n' '{"schema":"sf.builder/v1","summary":"real mutation","changed_files":["tracked_test.go"],"commands":[["go","test","./..."]]}' > "$last"
else
  printf '%s\n' '{"schema":"sf.planner/v1","acceptance":["real materializer"],"proof":{"kind":"acceptance","command":["go","test","./..."],"details":"real"},"paths":["proof.txt","tracked_test.go"],"commands":[["go","test","./..."]],"risks":["none"]}' > "$last"
fi
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}'
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertMaterializerProviderAttempts(t *testing.T, db *store.Store, ref domain.TicketRef, want int) {
	t.Helper()
	attempts, err := db.ProviderAttempts(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != want {
		t.Fatalf("provider attempt count=%d, want %d", len(attempts), want)
	}
	for _, attempt := range attempts {
		if attempt.State != "completed" || attempt.Outcome != "completed" || attempt.FinishedAt.IsZero() {
			t.Fatalf("provider attempt did not durably complete: %+v", attempt)
		}
	}
	active, err := db.ActiveProviderAttempts(context.Background(), ref.Channel)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active provider attempts=%+v", active)
	}
}
func materializerDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
