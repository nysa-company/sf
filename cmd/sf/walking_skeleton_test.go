package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	githubboundary "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/localruntime"
	"github.com/nysa-company/sf/internal/mergeproof"
	"github.com/nysa-company/sf/internal/operator"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/publication"
	"github.com/nysa-company/sf/internal/repositoryexec"
	"github.com/nysa-company/sf/internal/runtimeassets"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

// TestWalkingSkeletonCompiledCLIOverSocket exercises the real development
// executable as a socket client while leaving all host/network boundaries
// hermetic. It is intentionally Darwin-only: the guarded repository command
// supervisor is a macOS execution boundary, not a portable test double.
func TestWalkingSkeletonCompiledCLIOverSocket(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("guarded repository command execution is Darwin-only")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	binary := buildDevRuntimeBundle(t)
	home, err := os.MkdirTemp("/tmp", "sf-ws-")
	if err != nil {
		t.Fatal(err)
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	repository, bare, base := walkingSkeletonRepository(t)
	gitHome := filepath.Join(home, "git-home")
	if err := os.Mkdir(gitHome, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := walkingSkeletonRunner(t, bare)
	runner.Home, runner.MutationAuthority = gitHome, nil // Store owns this after daemon startup.

	trustedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	authHome := filepath.Join(trustedRoot, ".codex")
	if err := os.MkdirAll(authHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), []byte(`{"fixture":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(trustedRoot, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	codexBinary := filepath.Join(bin, "codex")
	writeWalkingSkeletonCodex(t, codexBinary)
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", authHome)
	t.Setenv("PATH", bin+":"+filepath.Dir(binary)+":"+filepath.Dir(goBinary)+":/usr/bin:/bin:/usr/sbin:/sbin")

	github, err := testkit.NewFakeGH(filepath.Join(home, "fake-gh.json"), contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := github.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := github.SetBaseHeadOIDForTest(base); err != nil {
		t.Fatal(err)
	}
	if err := github.SetRequiredStatusCheckContextsForTest("unit"); err != nil {
		t.Fatal(err)
	}
	if err := github.SetChecks(1, contracts.RequiredCheck{Name: "unit", ExternalID: "unit-1", State: "pending"}); err != nil {
		t.Fatal(err)
	}
	if err := github.UseBareRepositoryForTest(bare); err != nil {
		t.Fatal(err)
	}

	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("app", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor.Executable = binary
	t.Cleanup(func() { _ = supervisor.Close() })

	var mergeHead string
	var runtimeStore *store.Store
	factory := walkingSkeletonRuntimeFactory(t, binary, runner, github, &runtimeStore, func() error {
		if mergeHead == "" {
			return fmt.Errorf("merge invoked before the exact squash head was prepared")
		}
		return walkingSkeletonGitRun(bare, "update-ref", "refs/heads/main", mergeHead, base)
	})
	d, err := daemon.Start(ctx, daemon.Config{
		Channel:                  domain.ChannelDev,
		Paths:                    paths,
		DaemonIdentity:           "walking-skeleton",
		Projects:                 []store.Project{{Channel: domain.ChannelDev, ID: "app", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}},
		StartupTimeout:           30 * time.Second,
		RecoveryAuthorityKey:     supervisor.PublicKey(),
		ProviderSupervisor:       supervisor,
		RecoveryDrainer:          supervisor,
		GitMutationDrainer:       gitboundary.MutationDrainer{},
		RepositoryCommandDrainer: processsupervisor.RepositoryCommandDrainer{},
		Operator: operator.Authenticator{ExpectedUID: uint32(os.Getuid()), Lookup: func(string) (operator.Account, error) {
			return operator.Account{Username: account.Username, UID: strconv.Itoa(os.Getuid())}, nil
		}},
		ProviderCoordinatorFactory: func(database *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
			return codexprovider.ComposeProfiles(context.Background(), domain.ChannelDev, database, process, []codexprovider.Config{
				{Route: "codex-builder", Executable: codexBinary, AuthHome: authHome, Model: "gpt-5.6-luna"},
				{Route: "codex-reviewer", Executable: codexBinary, AuthHome: authHome, Model: "gpt-5.5"},
			})
		},
		ProviderQualifier: func(qualifyCtx context.Context, database *store.Store, channel domain.Channel, builder, reviewer string) (any, error) {
			return codexprovider.QualifyLocalPair(qualifyCtx, database, channel, builder, reviewer, supervisor)
		},
		WorkflowRuntimeFactory: factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- d.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case serveErr := <-serveDone:
			if serveErr != nil {
				t.Errorf("serve compiled CLI fixture: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("compiled CLI fixture did not stop serving")
		}
		if closeErr := d.Close(); closeErr != nil {
			t.Errorf("close compiled CLI fixture: %v", closeErr)
		}
	})
	if _, err := os.Lstat(paths.Socket); err != nil {
		t.Fatalf("in-process daemon did not expose its Unix socket: %v", err)
	}

	walkingSkeletonCLI(t, binary, home, "providers", "qualify", "--builder", "codex", "--reviewer", "codex", "--json")
	if runtimeStore == nil {
		t.Fatal("qualified runtime did not expose its Store composition")
	}
	ticketPath := filepath.Join(home, "ticket.md")
	if err := os.WriteFile(ticketPath, []byte("---\ntype: feature\nmerge: guarded\nmax_duration: 30m\nmax_cost_usd: 10\n---\n# Repair the red test\n\nThe focused test intentionally starts red.\n\n## Acceptance\n- The focused test passes after the builder change.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	submit := walkingSkeletonCLI(t, binary, home, "submit", ticketPath, "--project", "app", "--json")
	ref := walkingSkeletonSubmittedRef(t, submit)
	walkingSkeletonCLI(t, binary, home, "start", string(ref.Ticket), "--json")

	// Planner, red pre-build verifier, builder, green candidate materialization,
	// publication, and the first (pending) CI observation all cross the actual
	// scheduler rather than direct Store transitions.
	waitingCI := walkingSkeletonWaitState(t, runtimeStore, ref, domain.StateWaitingCI, github, bare)
	verification, err := runtimeStore.RecoverableVerification(ctx, ref)
	if err != nil {
		t.Fatalf("recover pre-build verification: %v", err)
	}
	_, verificationArtifact, err := runtimeStore.LoadHistoricalProviderAttemptResult(ctx, verification.ProviderResult)
	if err != nil || verificationArtifact.Verify == nil || verificationArtifact.Verify.PrebuildOutcome != "red" {
		t.Fatalf("pre-build verification=%+v err=%v", verificationArtifact, err)
	}
	if github.MutationCount("pr_create") != 1 {
		t.Fatalf("draft PR mutation count=%d, want 1", github.MutationCount("pr_create"))
	}
	if err := github.SetChecks(1, contracts.RequiredCheck{Name: "unit", ExternalID: "unit-1", State: "success"}); err != nil {
		t.Fatal(err)
	}
	waitingApproval := walkingSkeletonWaitState(t, runtimeStore, ref, domain.StateWaitingApproval, github, bare)
	if waitingApproval.Version <= waitingCI.Version {
		t.Fatalf("CI did not advance ticket: waiting_ci=%+v waiting_approval=%+v", waitingCI, waitingApproval)
	}

	candidate, err := runtimeStore.RecoverableCandidate(ctx, ref)
	if err != nil {
		t.Fatalf("recover candidate: %v", err)
	}
	if candidate.Snapshot.HeadSHA == "" || candidate.Snapshot.BaseSHA != base {
		t.Fatalf("candidate does not bind the expected base/head: %+v", candidate.Snapshot)
	}
	postbuild, err := runtimeStore.LoadRepositoryCommandResult(ctx, candidate.CommandBinding.Key)
	if err != nil || postbuild.Result.ExitCode != 0 || !postbuild.Result.Observed {
		t.Fatalf("post-build verification result=%+v err=%v", postbuild, err)
	}
	mergeHead = walkingSkeletonSquashCommit(t, bare, candidate.Snapshot.BaseSHA, candidate.Snapshot.HeadSHA)
	if err := github.SetMergeCommitForTest(mergeHead); err != nil {
		t.Fatal(err)
	}
	if github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
		t.Fatalf("guarded runtime mutated ready/merge before human approval: ready=%d merge=%d", github.MutationCount("pr_ready"), github.MutationCount("pr_merge"))
	}

	walkingSkeletonCLI(t, binary, home, "approve", string(ref.Ticket), "--operator", account.Username, "--json")
	done := walkingSkeletonWaitState(t, runtimeStore, ref, domain.StateDone, github, bare)
	if done.MergeMode != domain.MergeGuarded || done.RunnerEpoch == 0 {
		t.Fatalf("terminal ticket lost guarded runtime identity: %+v", done)
	}
	if got := walkingSkeletonGitOutput(t, bare, "rev-parse", "refs/heads/main"); got != mergeHead {
		t.Fatalf("protected main=%s, want exact squash merge=%s", got, mergeHead)
	}
	if github.MutationCount("pr_create") != 1 || github.MutationCount("pr_ready") != 1 || github.MutationCount("pr_merge") != 1 {
		t.Fatalf("GitHub mutation counts create=%d ready=%d merge=%d, want exactly one each", github.MutationCount("pr_create"), github.MutationCount("pr_ready"), github.MutationCount("pr_merge"))
	}
	attempts, err := runtimeStore.ProviderAttempts(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 4 {
		t.Fatalf("provider attempts=%d, want planner + prebuild verifier + builder + final reviewer", len(attempts))
	}
	for _, attempt := range attempts {
		if attempt.State != "completed" || attempt.Outcome != "completed" || attempt.FinishedAt.IsZero() {
			t.Fatalf("provider attempt did not complete: %+v", attempt)
		}
	}
}

type walkingSkeletonGHSupervisor struct {
	github     *testkit.FakeGH
	afterMerge func() error
}

type walkingSkeletonRuntimeWorker struct {
	delegate localruntime.Worker
	t        *testing.T
	mu       sync.Mutex
	last     string
}

func (w *walkingSkeletonRuntimeWorker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	result, err := w.delegate.Run(ctx, ref, fence)
	if err != nil {
		w.mu.Lock()
		message := err.Error()
		if message != w.last {
			w.t.Logf("walking-skeleton runtime boundary: %v", err)
			w.last = message
		}
		w.mu.Unlock()
	}
	return result, err
}

func (s walkingSkeletonGHSupervisor) Run(_ context.Context, _ string, args, _ []string) ([]byte, error) {
	output, err := s.github.Run(args)
	if len(args) >= 2 && args[0] == "pr" && args[1] == "merge" && s.afterMerge != nil {
		if advanceErr := s.afterMerge(); advanceErr != nil {
			return output, advanceErr
		}
	}
	return output, err
}

func (walkingSkeletonGHSupervisor) Cleanup(context.Context) (githubboundary.CleanupProof, error) {
	return githubboundary.CleanupProof{Drained: true}, nil
}

func walkingSkeletonRuntimeFactory(t *testing.T, binary string, runner gitboundary.Runner, fake *testkit.FakeGH, runtimeStore **store.Store, afterMerge func() error) daemon.WorkflowRuntimeFactory {
	t.Helper()
	var ciMu sync.Mutex
	ciNow := time.Now().UTC()
	return func(deps daemon.RuntimeDependencies) (daemon.WorkflowRuntimeComponents, error) {
		if deps.ProviderCoordinator == nil || deps.ProviderCoordinator.ReadyForPrePublishing() != nil {
			return daemon.WorkflowRuntimeComponents{}, nil
		}
		*runtimeStore = deps.Store
		core, err := runtimeassets.ResolveCore(domain.ChannelDev, binary)
		if err != nil {
			return daemon.WorkflowRuntimeComponents{}, err
		}
		runner.ExecHelper, runner.MutationAuthority = core.GitExec, deps.Store
		repositorySupervisor := processsupervisor.RepositoryCommandSupervisor{Executable: core.Executable, GitRunner: runner, SoftDrain: time.Second, HardDrain: time.Second}
		materializer := workflowruntime.RepositoryMaterializer{Store: deps.Store, Git: runner, Executor: repositoryexec.Executor{Authority: deps.Store, Supervisor: repositorySupervisor}}
		providers := workflowruntime.NewPhaseRunner(deps.Store, deps.ProviderCoordinator)
		workflow := workflowworker.Worker{Evidence: deps.Store, Engine: deps.Engine, Runner: providers, Checkpoint: materializer, Candidate: materializer, CheckpointMaterializer: materializer, CandidateMaterializer: materializer}
		client, err := githubboundary.NewStoreClient("/usr/bin/fake-gh", runner.Home, runner.Home, walkingSkeletonGHSupervisor{github: fake, afterMerge: afterMerge}, deps.Store, mergeproof.Coordinator{Store: deps.Store, Git: runner})
		if err != nil {
			return daemon.WorkflowRuntimeComponents{}, err
		}
		worker := localruntime.Worker{Store: deps.Store, Engine: deps.Engine, Workflow: workflow, Publication: publication.Worker{Store: deps.Store, Git: runner, GitHub: client}, CI: localruntime.CIWorker{Store: deps.Store, Observer: client, Now: func() time.Time {
			ciMu.Lock()
			defer ciMu.Unlock()
			ciNow = ciNow.Add(time.Minute)
			return ciNow
		}}, PublicationEnabled: true}
		loggedWorker := &walkingSkeletonRuntimeWorker{delegate: worker, t: t}
		scheduler := workflowruntime.NewScheduler(domain.ChannelDev, workflowruntime.StoreTicketSource{Store: deps.Store}, worktreecoord.Coordinator{Store: deps.Store, Git: runner}, loggedWorker)
		runtime, err := workflowruntime.NewRuntimeWithConfig(scheduler, workflowruntime.RuntimeConfig{Interval: 20 * time.Millisecond, Workers: 1})
		if err != nil {
			return daemon.WorkflowRuntimeComponents{}, err
		}
		observer := runtimecontrol.MergeObserverFunc(func(ctx context.Context, ref domain.TicketRef) (bool, error) {
			published, err := deps.Store.LoadHistoricalPublishedCandidate(ctx, ref)
			if err != nil {
				return false, err
			}
			observed, err := client.ObservePublishedPullRequest(ctx, published.PullRequest)
			return err == nil && observed.Merged, err
		})
		controller, err := runtimecontrol.New(deps.Store, runtime.ControlBundle(), observer, runner)
		if err != nil {
			_ = runtime.Close()
			return daemon.WorkflowRuntimeComponents{}, err
		}
		return daemon.WorkflowRuntimeComponents{Runtime: runtime, Controller: controller}, nil
	}
}

func walkingSkeletonRepository(t *testing.T) (repository, bare, base string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repository, bare = filepath.Join(root, "repo"), filepath.Join(root, "origin.git")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	walkingSkeletonGit(t, repository, "init", "-b", "main")
	walkingSkeletonGit(t, repository, "config", "user.name", "fixture")
	walkingSkeletonGit(t, repository, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test/walking\n\ngo 1.23\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "tracked_test.go"), []byte("package walking\n\nimport \"testing\"\n\nfunc TestFocused(t *testing.T) { t.Fatal(\"red\") }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "proof.txt"), []byte("proof\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	walkingSkeletonGit(t, repository, "add", ".")
	walkingSkeletonGit(t, repository, "commit", "-m", "base")
	base = walkingSkeletonGitOutput(t, repository, "rev-parse", "HEAD")
	walkingSkeletonGit(t, root, "init", "--bare", bare)
	if err := os.Chmod(bare, 0o700); err != nil {
		t.Fatal(err)
	}
	walkingSkeletonGit(t, repository, "remote", "add", "origin", bare)
	walkingSkeletonGit(t, repository, "push", "origin", "main")
	walkingSkeletonGit(t, repository, "remote", "set-url", "origin", "https://github.com/acme/app.git")
	walkingSkeletonGit(t, repository, "remote", "set-url", "--push", "origin", "https://github.com/acme/app.git")
	canonical, err := filepath.EvalSymlinks(repository)
	if err != nil {
		t.Fatal(err)
	}
	return canonical, bare, base
}

func walkingSkeletonRunner(t *testing.T, bare string) gitboundary.Runner {
	t.Helper()
	home := filepath.Join(t.TempDir(), "git-home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	runner := gitboundary.Runner{
		Binary:             "/usr/bin/git",
		Home:               home,
		TestLocalTransport: true,
		CredentialHelper:   "/usr/bin/true",
		GHBinary:           "/usr/bin/true",
		GHBinaryDigest:     "sha256:" + strings.Repeat("a", 64),
		GHConfigDir:        home,
	}
	runner.Run = func(ctx context.Context, binary string, args []string, env []string) ([]byte, error) {
		for i := range args {
			if args[i] == "https://github.com/acme/app.git" {
				args[i] = bare
			}
		}
		command := exec.CommandContext(ctx, binary, args...)
		command.Env = env
		return command.CombinedOutput()
	}
	return runner
}

func writeWalkingSkeletonCodex(t *testing.T, path string) {
	t.Helper()
	const script = `#!/bin/sh
set -eu
case "${1:-} ${2:-}" in
  "--version ") printf '%s\n' 'Codex 1.2.3'; exit 0 ;;
  "exec --help") printf '%s\n' '--json --output-schema --output-last-message --ephemeral --ignore-user-config --ignore-rules --config --model -C'; exit 0 ;;
  "login status") printf '%s\n' 'Logged in using ChatGPT'; exit 0 ;;
esac
if [ "${1:-}" = sandbox ]; then
  case "$*" in
    *'test -r /etc/hosts'*) exit 0 ;;
    *'test -w /etc/hosts'*) exit 1 ;;
    *'test -w .'*) exit 0 ;;
    *curl*) exit 1 ;;
    *CODEX_HOME*) exit 1 ;;
  esac
  exit 0
fi
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
if printf '%s' "$prompt" | grep -q '^You are the fresh, independent final Reviewer\.'; then
  candidate=${prompt#*CANDIDATE=}
  candidate=${candidate%%CHECKS=*}
  head=$(printf '%s' "$candidate" | grep -Eo '"head_sha":"[0-9a-f]+"' | grep -Eo '[0-9a-f]{40,64}' | head -1)
  proof=$(printf '%s' "$candidate" | grep -Eo '"proof_digest":"[0-9a-f]+"' | grep -Eo '[0-9a-f]{64}' | head -1)
  printf '%s\n' '{"schema":"sf.reviewer/v1","decision":"pass","findings":[],"reviewed_head":"'"$head"'","proof_digest":"'"$proof"'"}' > "$last"
elif printf '%s' "$prompt" | grep -q '^You are the independent pre-build Reviewer and verification author\.'; then
  plan=${prompt#*PLAN=}
  plan=${plan%%WORKSPACE=*}
  digest=$(printf '%s' "$plan" | grep -Eo '"digest":"[0-9a-f]+"' | grep -Eo '[0-9a-f]{64}' | head -1 || true)
  printf '%s\n' '{"schema":"sf.verification/v1","acceptance_digest":"'"$digest"'","proof_kind":"acceptance","owned_files":["proof.txt"],"command":["go","test","./..."],"prebuild_outcome":"red","evidence_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' > "$last"
elif printf '%s' "$prompt" | grep -q '^You are the implementation Builder\.'; then
  printf '%s\n' 'package walking

import "testing"

func TestFocused(t *testing.T) {}' > tracked_test.go
  printf '%s\n' '{"schema":"sf.builder/v1","summary":"repair focused test","changed_files":["tracked_test.go"],"commands":[["go","test","./..."]]}' > "$last"
elif printf '%s' "$prompt" | grep -q '^You are the Planner for a software ticket\.'; then
  printf '%s\n' '{"schema":"sf.planner/v1","acceptance":["focused test passes"],"proof":{"kind":"acceptance","command":["go","test","./..."],"details":"red before build"},"paths":["proof.txt","tracked_test.go"],"commands":[["go","test","./..."]],"risks":["none"],"questions":[]}' > "$last"
else
  exit 64
fi
printf '%s\n' '{"type":"turn.completed","usage":{"input_tokens":1,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}'
`
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func walkingSkeletonCLI(t *testing.T, binary, home string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Env = append(os.Environ(), "HOME="+home)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled sf-dev %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return output
}

func walkingSkeletonSubmittedRef(t *testing.T, output []byte) domain.TicketRef {
	t.Helper()
	var response struct {
		Data struct {
			Ticket string `json:"ticket"`
		} `json:"data"`
	}
	if err := json.Unmarshal(output, &response); err != nil || response.Data.Ticket == "" {
		t.Fatalf("submit response=%s err=%v", output, err)
	}
	return domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: domain.TicketID(response.Data.Ticket)}
}

func walkingSkeletonWaitState(t *testing.T, database *store.Store, ref domain.TicketRef, want domain.State, github *testkit.FakeGH, bare string, diagnostics ...fmt.Stringer) store.Ticket {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		ticket, err := database.Ticket(context.Background(), ref)
		if err == nil && ticket.State == want {
			return ticket
		}
		time.Sleep(20 * time.Millisecond)
	}
	ticket, err := database.Ticket(context.Background(), ref)
	attempts, attemptsErr := database.ProviderAttempts(context.Background(), ref)
	worktree, worktreeErr := database.Worktree(context.Background(), ref)
	type attemptSummary struct {
		Phase, Role, State, Outcome string
		Attempt                     int
	}
	summaries := make([]attemptSummary, 0, len(attempts))
	for _, attempt := range attempts {
		summaries = append(summaries, attemptSummary{Phase: string(attempt.Phase), Role: attempt.Role, State: attempt.State, Outcome: attempt.Outcome, Attempt: attempt.Attempt})
	}
	events, eventsErr := database.Events(context.Background(), ref.Channel, 0, 1_000)
	eventTypes := make([]string, 0, 20)
	for _, event := range events {
		if event.Ref == ref {
			eventTypes = append(eventTypes, event.Trigger)
		}
	}
	if len(eventTypes) > 20 {
		eventTypes = eventTypes[len(eventTypes)-20:]
	}
	gitStatus := exec.Command("/usr/bin/git", "-C", worktree.Path, "status", "--short")
	gitStatus.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	statusOutput, statusErr := gitStatus.CombinedOutput()
	goTest := exec.Command("go", "test", "./...")
	goTest.Dir = worktree.Path
	diagnosticRoot := t.TempDir()
	goTest.Env = append(os.Environ(), "HOME="+diagnosticRoot, "GOCACHE="+filepath.Join(diagnosticRoot, "cache"))
	testOutput, testErr := goTest.CombinedOutput()
	remoteRefs := walkingSkeletonGitOutput(t, bare, "for-each-ref", "--format=%(refname):%(objectname)", "refs/heads")
	diagnosticText := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic != nil {
			diagnosticText = append(diagnosticText, diagnostic.String())
		}
	}
	t.Fatalf("ticket did not reach %s: state=%s version=%d runner=%d err=%v attempts=%+v attempts_err=%v worktree_state=%s worktree_head=%s worktree_err=%v events=%v events_err=%v git_status=%q git_status_err=%v go_test=%q go_test_err=%v gh_mutations=create:%d ready:%d merge:%d remote_refs=%q diagnostics=%q", want, ticket.State, ticket.Version, ticket.RunnerEpoch, err, summaries, attemptsErr, worktree.State, worktree.HeadSHA, worktreeErr, eventTypes, eventsErr, statusOutput, statusErr, testOutput, testErr, github.MutationCount("pr_create"), github.MutationCount("pr_ready"), github.MutationCount("pr_merge"), remoteRefs, diagnosticText)
	return store.Ticket{}
}

func walkingSkeletonSquashCommit(t *testing.T, bare, base, head string) string {
	t.Helper()
	tree := walkingSkeletonGitOutput(t, bare, "rev-parse", head+"^{tree}")
	command := exec.Command("/usr/bin/git", "--git-dir", bare, "commit-tree", tree, "-p", base, "-m", "guarded hosted squash")
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=sf", "GIT_AUTHOR_EMAIL=sf@localhost", "GIT_COMMITTER_NAME=sf", "GIT_COMMITTER_EMAIL=sf@localhost")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("prebuild squash merge: %v", err)
	}
	return strings.TrimSpace(string(output))
}

func walkingSkeletonGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	if err := walkingSkeletonGitRun(directory, args...); err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
}

func walkingSkeletonGitRun(directory string, args ...string) error {
	command := exec.Command("/usr/bin/git", append([]string{"-C", directory}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func walkingSkeletonGitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("/usr/bin/git", append([]string{"-C", directory}, args...)...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output))
}
