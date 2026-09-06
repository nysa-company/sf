//go:build sf_e2e

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

// TestCompiledDevGuardedWalkingSkeleton runs the production dev binary and its
// daemon across the real Unix socket. Only the tagged Git URL bridge and the
// process-boundary codex/gh fixtures are substituted, so no real remote runs.
func TestCompiledDevGuardedWalkingSkeleton(t *testing.T) {
	compiledDevWalkingSkeleton(t, domain.MergeGuarded)
}

// TestCompiledDevManualWalkingSkeleton proves manual mode stops after the
// draft PR and green review, then advances only after an independently
// simulated human merge. The factory must never mark ready or call merge.
func TestCompiledDevManualWalkingSkeleton(t *testing.T) {
	compiledDevWalkingSkeleton(t, domain.MergeManual)
}

func compiledDevWalkingSkeleton(t *testing.T, mergeMode domain.MergeMode) {
	compiledDevWalkingSkeletonProfile(t, mergeMode, false)
}

// Real CLI/daemon/Python execution with process-boundary provider/GitHub fixtures.
// Public downloads are opt-in; no real provider credentials or remote mutation.
func TestCompiledPythonGuardedWalkingSkeleton(t *testing.T) {
	if os.Getenv("SF_TEST_PYTHON_CLI_DOWNLOAD") != "1" || runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Skip("requires explicit pinned Python download acceptance on macOS ARM64")
	}
	compiledDevWalkingSkeletonProfile(t, domain.MergeGuarded, true)
}

func compiledDevWalkingSkeletonProfile(t *testing.T, mergeMode domain.MergeMode, python bool) {
	compiledDevWalkingSkeletonScenario(t, mergeMode, python, false)
}

func TestCompiledDevConcurrentTicketsReachIndependentPRs(t *testing.T) {
	compiledDevWalkingSkeletonScenario(t, domain.MergeGuarded, false, true)
}

func compiledDevWalkingSkeletonScenario(t *testing.T, mergeMode domain.MergeMode, python, concurrent bool) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("guarded repository command execution is Darwin-only")
	}
	if mergeMode != domain.MergeGuarded && mergeMode != domain.MergeManual {
		t.Fatalf("unsupported compiled merge mode %q", mergeMode)
	}

	binary, fixtureBin := compiledWalkingSkeletonBundle(t)
	home := compiledWalkingSkeletonHome(t)
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}

	codexHome := filepath.Join(home, "codex")
	ghConfig := filepath.Join(home, ".config", "gh")
	if err := os.Mkdir(codexHome, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(home, ".config"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(ghConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), []byte("{\"fixture\":true}"), 0o600); err != nil {
		t.Fatal(err)
	}

	bareRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repository, bare, base := compiledWalkingSkeletonRepository(t, bareRoot)
	if python {
		// Only this newly created disposable fixture is changed.
		if err := os.Remove(filepath.Join(repository, "go.mod")); err != nil {
			t.Fatal(err)
		}
		for name, content := range map[string]string{
			"pyproject.toml":     "[project]\nname='sf-python-fixture'\nversion='0.1.0'\n",
			"test_sf_fixture.py": "# Independent Reviewer will author the proof.\n",
		} {
			if err := os.WriteFile(filepath.Join(repository, name), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
		}
		walkingSkeletonGit(t, repository, "add", ".")
		walkingSkeletonGit(t, repository, "commit", "-m", "Python fixture baseline")
		walkingSkeletonGit(t, repository, "push", bare, "main")
		base = walkingSkeletonGitOutput(t, repository, "rev-parse", "HEAD")
	}
	if got := walkingSkeletonGitOutput(t, repository, "config", "--get", "remote.origin.url"); got != "https://github.com/acme/app.git" {
		t.Fatalf("origin URL=%q, want exact GitHub fixture URL", got)
	}
	if got := walkingSkeletonGitOutput(t, repository, "config", "--get", "remote.origin.pushurl"); got != "https://github.com/acme/app.git" {
		t.Fatalf("origin push URL=%q, want exact GitHub fixture URL", got)
	}
	info, statErr := os.Stat(bare)
	if statErr != nil {
		t.Fatalf("stat private bare repository: %v", statErr)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("private bare repository mode=%v, want 0700", info.Mode())
	}
	if err := os.WriteFile(filepath.Join(ghConfig, "sf-fake-gh-bare"), []byte(bare+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	github, err := testkit.NewFakeGH(filepath.Join(ghConfig, "sf-fake-gh.json"), contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"})
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

	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", codexHome)
	t.Setenv("GH_CONFIG_DIR", ghConfig)
	t.Setenv("SF_E2E_GIT_BARE", bare)
	if concurrent {
		t.Setenv("SF_CODEX_PROVIDER_CAPACITY", "2")
		if err := github.SetChecks(2, contracts.RequiredCheck{Name: "unit", ExternalID: "unit-2", State: "pending"}); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", fixtureBin+":"+filepath.Dir(binary)+":"+filepath.Dir(goBinary)+":/usr/bin:/bin:/usr/sbin:/sbin")

	if python {
		compiledWalkingSkeletonCLI(t, binary, home, "runtimes", "prepare", "python", "--download", "--json")
		compiledWalkingSkeletonCLI(t, binary, home, "init", "--project", "app", "--repo", repository, "--profile", "python-pytest-v1", "--test", "test_sf_fixture.py", "--json")
	} else {
		compiledWalkingSkeletonCLI(t, binary, home, "init", "--project", "app", "--repo", repository, "--json")
	}
	var daemonOutput compiledSafeBuffer
	daemonCommand := exec.Command(binary, "daemon", "run")
	daemonCommand.Env = os.Environ()
	daemonCommand.Stdout, daemonCommand.Stderr = &daemonOutput, &daemonOutput
	if err := daemonCommand.Start(); err != nil {
		t.Fatalf("start compiled sf-dev daemon: %v", err)
	}
	daemonDone := make(chan error, 1)
	go func() { daemonDone <- daemonCommand.Wait() }()
	daemonStopped := false
	stopDaemon := func() {
		t.Helper()
		if daemonStopped {
			return
		}
		daemonStopped = true
		if daemonCommand.Process != nil && daemonCommand.ProcessState == nil {
			if err := daemonCommand.Process.Signal(os.Interrupt); err != nil {
				t.Errorf("SIGINT compiled daemon: %v", err)
			}
		}
		select {
		case err := <-daemonDone:
			if err != nil {
				t.Errorf("compiled daemon graceful exit: %v\n%s", err, daemonOutput.String())
			}
		case <-time.After(30 * time.Second):
			_ = daemonCommand.Process.Kill()
			select {
			case <-daemonDone:
			case <-time.After(5 * time.Second):
			}
			t.Errorf("compiled daemon did not stop within 30s: %s", daemonOutput.String())
		}
		if _, err := os.Lstat(paths.Socket); !os.IsNotExist(err) {
			t.Errorf("socket remained after compiled daemon SIGINT: %v", err)
		}
	}
	t.Cleanup(stopDaemon)
	compiledWalkingSkeletonWaitSocket(t, paths.Socket, daemonDone, &daemonOutput, &daemonStopped)

	compiledWalkingSkeletonCLI(t, binary, home, "providers", "qualify", "--builder", "codex", "--reviewer", "codex", "--json")
	if concurrent {
		compiledConcurrentTicketsReachPRs(t, binary, home, paths.Database, bare, github, &daemonOutput)
		return
	}
	ticketPath := filepath.Join(home, "ticket.md")
	ticketSource := fmt.Sprintf("---\ntype: feature\nmerge: %s\nmax_duration: 30m\nmax_cost_usd: 10\n---\n# Implement the fixture\n\nThe verification fixture deliberately begins without its implementation.\n\n## Acceptance\n- The fixture workflow completes.\n", mergeMode)
	if python {
		ticketSource = strings.Replace(ticketSource, "The verification fixture", "SF_E2E_PYTHON_PYTEST: The verification fixture", 1)
	}
	if err := os.WriteFile(ticketPath, []byte(ticketSource), 0o600); err != nil {
		t.Fatal(err)
	}
	submit := compiledWalkingSkeletonCLI(t, binary, home, "submit", ticketPath, "--project", "app", "--json")
	ref := walkingSkeletonSubmittedRef(t, submit)
	compiledWalkingSkeletonCLI(t, binary, home, "start", string(ref.Ticket), "--json")

	readOnly, err := store.OpenReadOnly(context.Background(), paths.Database)
	if err != nil {
		t.Fatalf("open compiled daemon Store for observation: %v", err)
	}
	defer readOnly.Close()
	waitingCI := walkingSkeletonWaitState(t, readOnly, ref, domain.StateWaitingCI, github, bare, &daemonOutput)
	verification, err := readOnly.RecoverableVerification(context.Background(), ref)
	if err != nil {
		t.Fatalf("recover pre-build verification: %v", err)
	}
	_, verificationArtifact, err := readOnly.LoadHistoricalProviderAttemptResult(context.Background(), verification.ProviderResult)
	if err != nil || verificationArtifact.Verify == nil || verificationArtifact.Verify.PrebuildOutcome != "missing" {
		t.Fatalf("pre-build verification=%+v err=%v", verificationArtifact, err)
	}
	prebuild, err := readOnly.LoadRepositoryCommandResult(context.Background(), verification.CommandBinding.Key)
	if err != nil || !prebuild.Result.Observed || prebuild.Result.ExitCode == 0 {
		t.Fatalf("pre-build verification result=%+v artifact=%+v err=%v", prebuild, verificationArtifact, err)
	}
	if github.MutationCount("pr_create") != 1 {
		t.Fatalf("draft PR mutation count=%d, want one", github.MutationCount("pr_create"))
	}
	candidate, err := readOnly.RecoverableCandidate(context.Background(), ref)
	if err != nil {
		t.Fatalf("recover candidate: %v", err)
	}
	postbuild, err := readOnly.LoadRepositoryCommandResult(context.Background(), candidate.CommandBinding.Key)
	if err != nil || postbuild.Result.ExitCode != 0 || !postbuild.Result.Observed {
		t.Fatalf("post-build verification result=%+v err=%v", postbuild, err)
	}
	if python {
		if prebuild.Claim.ExecutableDigest != postbuild.Claim.ExecutableDigest || !strings.Contains(postbuild.Claim.ExecutablePath, "python3.13") {
			t.Fatal("workflow did not execute the same prepared Python interpreter")
		}
		if got := walkingSkeletonGitOutput(t, bare, "show", candidate.Snapshot.HeadSHA+":sf_fixture.py"); !strings.Contains(got, "def software_factory_fixture") {
			t.Fatal("published candidate lacks Python implementation")
		}
	}

	if err := github.SetChecks(1, contracts.RequiredCheck{Name: "unit", ExternalID: "unit-1", State: "success"}); err != nil {
		t.Fatal(err)
	}
	waitingState := domain.StateWaitingApproval
	if mergeMode == domain.MergeManual {
		waitingState = domain.StateWaitingManualMerge
	}
	waitingReview := walkingSkeletonWaitState(t, readOnly, ref, waitingState, github, bare, &daemonOutput)
	if waitingReview.Version <= waitingCI.Version {
		t.Fatalf("CI did not advance ticket: waiting_ci=%+v waiting_review=%+v", waitingCI, waitingReview)
	}
	mergeHead := walkingSkeletonSquashCommit(t, bare, candidate.Snapshot.BaseSHA, candidate.Snapshot.HeadSHA)
	var manualPublished store.PublishedCandidateEvidence
	switch mergeMode {
	case domain.MergeGuarded:
		if err := github.SetMergeCommitForTest(mergeHead); err != nil {
			t.Fatal(err)
		}
		if github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
			t.Fatalf("guarded runtime mutated before approval: ready=%d merge=%d", github.MutationCount("pr_ready"), github.MutationCount("pr_merge"))
		}
		account, err := user.Current()
		if err != nil {
			t.Fatal(err)
		}
		compiledWalkingSkeletonCLI(t, binary, home, "approve", string(ref.Ticket), "--operator", account.Username, "--json")
	case domain.MergeManual:
		manualPublished, err = readOnly.LoadHistoricalPublishedCandidate(context.Background(), ref)
		if err != nil || manualPublished.PullRequest.Number <= 0 || manualPublished.PullRequest.HeadOID != candidate.Snapshot.HeadSHA {
			t.Fatalf("load exact manual PR: evidence=%+v err=%v", manualPublished, err)
		}
		if github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
			t.Fatalf("manual runtime mutated before external merge: ready=%d merge=%d", github.MutationCount("pr_ready"), github.MutationCount("pr_merge"))
		}
		walkingSkeletonGitOutput(t, bare, "update-ref", "refs/heads/main", mergeHead, candidate.Snapshot.BaseSHA)
		if err := github.SetPullRequestMergedForTest(manualPublished.PullRequest.Number, mergeHead); err != nil {
			t.Fatal(err)
		}
	}
	done := walkingSkeletonWaitState(t, readOnly, ref, domain.StateDone, github, bare, &daemonOutput)
	if done.MergeMode != mergeMode || done.RunnerEpoch == 0 {
		t.Fatalf("terminal ticket lost %s runtime identity: %+v", mergeMode, done)
	}
	if got := walkingSkeletonGitOutput(t, bare, "rev-parse", "refs/heads/main"); got != mergeHead {
		t.Fatalf("protected main=%s, want exact squash merge=%s", got, mergeHead)
	}
	if mergeMode == domain.MergeManual {
		observation, err := readOnly.LoadManualMergeObservation(context.Background(), ref)
		if err != nil || observation.Publication != manualPublished.PullRequest || observation.CandidateGeneration != candidate.Snapshot.Generation || observation.CandidateHeadSHA != candidate.Snapshot.HeadSHA || observation.CandidateBaseSHA != candidate.Snapshot.BaseSHA || observation.CandidateTreeSHA != candidate.Snapshot.TreeSHA || observation.Observed.State != "MERGED" || !observation.Observed.Merged || observation.Observed.Draft || observation.MergeCommit != mergeHead || observation.Observed.MergeCommit != mergeHead || observation.Observed.Identity.BaseOID != manualPublished.PullRequest.BaseOID || observation.Observed.BaseHeadOID != manualPublished.PullRequest.BaseOID || observation.ObservedProtectedBase != manualPublished.PullRequest.BaseOID {
			t.Fatalf("durable manual merge observation=%+v publication=%+v candidate=%+v err=%v", observation, manualPublished.PullRequest, candidate.Snapshot, err)
		}
	}
	wantReady, wantMerge := 1, 1
	if mergeMode == domain.MergeManual {
		wantReady, wantMerge = 0, 0
	}
	if github.MutationCount("pr_create") != 1 || github.MutationCount("pr_ready") != wantReady || github.MutationCount("pr_merge") != wantMerge {
		t.Fatalf("%s GitHub mutations create=%d ready=%d merge=%d, want create=1 ready=%d merge=%d", mergeMode, github.MutationCount("pr_create"), github.MutationCount("pr_ready"), github.MutationCount("pr_merge"), wantReady, wantMerge)
	}

	stopDaemon()
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err = store.OpenReadOnly(context.Background(), paths.Database)
	if err != nil {
		t.Fatalf("reopen stopped daemon Store: %v", err)
	}
	defer readOnly.Close()
	compiledWalkingSkeletonAssertStore(t, readOnly, ref)
	// TerminalControlProof is the Store's public atomic observation for
	// unreconciled effects. Retiring the one-shot capability removes the
	// transient runtime-control row created for that proof.
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}
	writable, err := store.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatalf("open stopped daemon Store for terminal proof: %v", err)
	}
	defer writable.Close()
	retirement, err := writable.TerminalControlProof(context.Background(), ref)
	if err != nil {
		t.Fatalf("terminal Store proof (including unresolved effects): %v", err)
	}
	if err := retirement.RetireRuntime(context.Background(), func(domain.TicketRef) error { return nil }); err != nil {
		t.Fatalf("retire terminal Store proof: %v", err)
	}
}

// Full compiled CLI/daemon/runtime and native red-to-green commands, with
// controlled provider/GitHub processes. Approval is deliberately not granted.
func compiledConcurrentTicketsReachPRs(t *testing.T, binary, home, databasePath, bare string, github *testkit.FakeGH, output *compiledSafeBuffer) {
	t.Helper()
	refs := make([]domain.TicketRef, 2)
	for index := range refs {
		path := filepath.Join(home, fmt.Sprintf("concurrent-%d.md", index))
		source := fmt.Sprintf("---\ntype: feature\nmerge: guarded\nmax_duration: 30m\n---\n# Concurrent fixture %d\n\nImplement the fixture in this ticket's independent worktree.\n\n## Acceptance\n- The verification fixture passes.\n", index)
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		refs[index] = walkingSkeletonSubmittedRef(t, compiledWalkingSkeletonCLI(t, binary, home, "submit", path, "--project", "app", "--json"))
	}
	for _, ref := range refs {
		compiledWalkingSkeletonCLI(t, binary, home, "start", string(ref.Ticket), "--json")
	}
	database, err := store.OpenReadOnly(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var attempts [2][]store.ProviderAttempt
	worktrees, numbers := map[string]bool{}, map[int]bool{}
	for index, ref := range refs {
		walkingSkeletonWaitState(t, database, ref, domain.StateWaitingCI, github, bare, output)
		published, err := database.LoadHistoricalPublishedCandidate(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if numbers[published.PullRequest.Number] {
			t.Fatal("tickets shared a PR")
		}
		numbers[published.PullRequest.Number] = true
		candidate, err := database.RecoverableCandidate(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := database.LoadRepositoryCommandResult(t.Context(), candidate.CommandBinding.Key)
		if err != nil || !proof.Result.Observed || proof.Result.ExitCode != 0 {
			t.Fatalf("candidate proof: %+v err=%v", proof, err)
		}
		worktree, err := database.Worktree(t.Context(), ref)
		if err != nil || worktrees[worktree.Path] {
			t.Fatalf("worktree isolation: %+v err=%v", worktree, err)
		}
		worktrees[worktree.Path] = true
		attempts[index], err = database.ProviderAttempts(t.Context(), ref)
		if err != nil || len(attempts[index]) != 3 {
			t.Fatalf("attempts=%+v err=%v", attempts[index], err)
		}
		for _, attempt := range attempts[index] {
			if attempt.State != "completed" || attempt.Outcome != "completed" {
				t.Fatalf("incomplete attempt: %+v", attempt)
			}
		}
	}
	// The two real ticket pipelines must overlap, even when a short repository
	// writer boundary serializes a particular provider or command interval.
	if !attempts[0][0].StartedAt.Before(attempts[1][2].FinishedAt) || !attempts[1][0].StartedAt.Before(attempts[0][2].FinishedAt) {
		t.Fatal("ticket pipelines did not overlap")
	}
	if github.MutationCount("pr_create") != 2 || github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
		t.Fatal("unexpected or duplicate publication mutations")
	}
	if active, err := database.ActiveProviderAttempts(t.Context(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active provider residue: %+v err=%v", active, err)
	}
	if leases, err := database.ActiveRepositoryCommandLeases(t.Context(), domain.ChannelDev); err != nil || len(leases) != 0 {
		t.Fatalf("command residue: %+v err=%v", leases, err)
	}
}

func compiledWalkingSkeletonAssertStore(t *testing.T, database *store.Store, ref domain.TicketRef) {
	t.Helper()
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 4 {
		t.Fatalf("provider attempts=%d, want planner + pre-build verifier + builder + final reviewer", len(attempts))
	}
	want := map[string]bool{
		"planning/planner":      false,
		"verification/reviewer": false,
		"build/builder":         false,
		"review/reviewer":       false,
	}
	for _, attempt := range attempts {
		key := string(attempt.Phase) + "/" + attempt.Role
		if _, expected := want[key]; expected {
			want[key] = true
		}
		if attempt.State != "completed" || attempt.Outcome != "completed" || attempt.FinishedAt.IsZero() {
			t.Fatalf("provider attempt did not complete: %+v", attempt)
		}
	}
	for key, found := range want {
		if !found {
			t.Fatalf("missing required provider attempt %s: %+v", key, attempts)
		}
	}
	if active, err := database.ActiveProviderAttempts(context.Background(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active provider attempts=%+v err=%v", active, err)
	}
	if active, err := database.ActiveRepositoryCommandLeases(context.Background(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active repository command leases=%+v err=%v", active, err)
	}
	if active, err := database.ActiveGitMutationLeases(context.Background(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active Git mutation leases=%+v err=%v", active, err)
	}
	if leases, err := database.Leases(context.Background(), domain.ChannelDev); err != nil || len(leases) != 0 {
		t.Fatalf("active Store leases=%+v err=%v", leases, err)
	}
}

func compiledWalkingSkeletonBundle(t *testing.T) (binary, fixtureBin string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixtureBin = filepath.Join(root, "bin")
	if err := os.Mkdir(fixtureBin, 0o700); err != nil {
		t.Fatal(err)
	}
	goBinary, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	ldflags := "-X github.com/nysa-company/sf/internal/version.Channel=dev"
	for _, target := range []struct{ path, packagePath string }{
		{filepath.Join(root, "sf-dev"), "."},
		{filepath.Join(root, "sf-ssh-dev"), "../../cmd/sf-ssh"},
		{filepath.Join(root, "sf-git-exec-dev"), "../../cmd/sf-git-exec"},
		{filepath.Join(root, "sf-git-credential-dev"), "../../cmd/sf-git-credential"},
		{filepath.Join(fixtureBin, "codex"), "../../cmd/fake-provider"},
		{filepath.Join(fixtureBin, "codex-code-mode-host"), "../../cmd/fake-provider"},
		{filepath.Join(fixtureBin, "gh"), "../../cmd/fake-gh"},
	} {
		command := exec.Command(goBinary, "build", "-tags", "sf_e2e", "-ldflags", ldflags, "-o", target.path, target.packagePath)
		command.Dir = "."
		command.Env = append(os.Environ(), "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			t.Fatalf("build %s: %v\n%s", target.packagePath, buildErr, output)
		}
	}
	knownHosts, err := os.ReadFile(filepath.Join("..", "..", "internal", "gitssh", "github_known_hosts"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "github_known_hosts"), knownHosts, 0o644); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "sf-dev"), fixtureBin
}

func compiledWalkingSkeletonHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "sfh-")
	if err != nil {
		t.Fatal(err)
	}
	home, err = filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(home)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("short canonical private HOME is unsafe: path=%q info=%v err=%v", home, info, err)
	}
	return home
}

func compiledWalkingSkeletonRepository(t *testing.T, root string) (repository, bare, base string) {
	t.Helper()
	repository, bare = filepath.Join(root, "repo"), filepath.Join(root, "origin.git")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	walkingSkeletonGit(t, repository, "init", "-b", "main")
	walkingSkeletonGit(t, repository, "config", "user.name", "fixture")
	walkingSkeletonGit(t, repository, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test/app\n\ngo 1.23\n"), 0o600); err != nil {
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
	canonicalRepository, err := filepath.EvalSymlinks(repository)
	if err != nil {
		t.Fatal(err)
	}
	canonicalBare, err := filepath.EvalSymlinks(bare)
	if err != nil || canonicalBare != bare {
		t.Fatalf("bare repository is not canonical: bare=%q canonical=%q err=%v", bare, canonicalBare, err)
	}
	return canonicalRepository, bare, base
}

func compiledWalkingSkeletonCLI(t *testing.T, binary, home string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Env = append(os.Environ(), "HOME="+home)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compiled sf-dev %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	return output
}

func compiledWalkingSkeletonWaitSocket(t *testing.T, socket string, processDone <-chan error, output *compiledSafeBuffer, stopped *bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(socket); err == nil {
			return
		}
		select {
		case err := <-processDone:
			*stopped = true
			t.Fatalf("compiled daemon exited before socket: err=%v output=%s", err, output.String())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("compiled daemon did not expose socket %q: %s", socket, output.String())
}

type compiledSafeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *compiledSafeBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(value)
}

func (b *compiledSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
