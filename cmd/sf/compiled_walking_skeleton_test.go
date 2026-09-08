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
	"reflect"
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

func TestCompiledDevFinalReviewVerificationRepair(t *testing.T) {
	t.Setenv("SF_TEST_REVIEW_REPAIR_FIXTURE", "1")
	compiledDevWalkingSkeletonScenario(t, domain.MergeGuarded, false, false)
}

func compiledDevWalkingSkeletonScenario(t *testing.T, mergeMode domain.MergeMode, python, concurrent bool) {
	compiledDevWalkingSkeletonProviders(t, mergeMode, python, concurrent, false, false)
}

// Real models, compiled SF and Store; GitHub and hosted Git remain local
// fixtures. This is not a live hosted delivery and never opens channel state.
func TestCompiledLiveClaudeCodexTicket(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_MIXED_TICKET") != "1" {
		t.Skip("explicit paid live-provider ticket acceptance")
	}
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, false, true, false)
}

func TestCompiledLiveCodexClaudeTicket(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_MIXED_TICKET") != "1" {
		t.Skip("explicit paid live-provider ticket acceptance")
	}
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, false, true, true)
}

func TestCompiledLiveCursorClaudeTicket(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_CURSOR_TICKET") != "1" {
		t.Skip("explicit paid Cursor ticket acceptance")
	}
	compiledDevWalkingSkeletonConfigured(t, domain.MergeGuarded, false, false, true, false, true)
}

func TestCompiledLiveClaudeCursorTicket(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_CURSOR_TICKET") != "1" {
		t.Skip("explicit paid Cursor ticket acceptance")
	}
	compiledDevWalkingSkeletonConfigured(t, domain.MergeGuarded, false, false, true, true, true)
}

func TestCompiledLiveCursorConcurrentTicketsRestartBeforeSeparateApprovals(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_CURSOR_CONCURRENT") != "1" {
		t.Skip("explicit paid Cursor concurrent restart and two-delivery acceptance")
	}
	compiledDevWalkingSkeletonConfigured(t, domain.MergeGuarded, false, true, true, false, true, true, true, true, true)
}

func TestCompiledLiveMixedConcurrentTicketsReachPRs(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_MIXED_CONCURRENT") != "1" {
		t.Skip("explicit paid two-ticket mixed-provider acceptance")
	}
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, true, false)
}

func TestCompiledConcurrentApprovalDoesNotAuthorizeSibling(t *testing.T) {
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, false, false, true)
}

func TestCompiledConcurrentSiblingRefreshesAfterApprovedMerge(t *testing.T) {
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, false, false, true, true)
}

func TestCompiledConcurrentTicketsDeliverWithSeparateApprovals(t *testing.T) {
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, false, false, true, true, true)
}

func TestCompiledConcurrentTicketsRestartBeforeSeparateApprovals(t *testing.T) {
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, false, false, true, true, true, true)
}

func TestCompiledLiveMixedConcurrentTicketsRestartBeforeSeparateApprovals(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_MIXED_CONCURRENT") != "1" {
		t.Skip("explicit paid concurrent restart and two-delivery acceptance")
	}
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, true, false, true, true, true, true)
}

func TestCompiledLiveMixedConcurrentTicketsDeliverWithSeparateApprovals(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_MIXED_CONCURRENT") != "1" {
		t.Skip("explicit paid concurrent two-delivery acceptance")
	}
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, true, false, true, true, true)
}

func TestCompiledLiveMixedConcurrentApprovalDoesNotAuthorizeSibling(t *testing.T) {
	if os.Getenv("SF_TEST_LIVE_MIXED_CONCURRENT") != "1" {
		t.Skip("explicit paid concurrent review and approval acceptance")
	}
	compiledDevWalkingSkeletonProviders(t, domain.MergeGuarded, false, true, true, false, true)
}

func compiledDevWalkingSkeletonProviders(t *testing.T, mergeMode domain.MergeMode, python, concurrent, live, reverse bool, approveFirst ...bool) {
	t.Helper()
	compiledDevWalkingSkeletonConfigured(t, mergeMode, python, concurrent, live, reverse, false, approveFirst...)
}

func compiledDevWalkingSkeletonConfigured(t *testing.T, mergeMode domain.MergeMode, python, concurrent, live, reverse, cursor bool, approveFirst ...bool) {
	t.Helper()
	if runtime.GOOS != "darwin" {
		t.Skip("guarded repository command execution is Darwin-only")
	}
	if mergeMode != domain.MergeGuarded && mergeMode != domain.MergeManual {
		t.Fatalf("unsupported compiled merge mode %q", mergeMode)
	}

	binary, fixtureBin := compiledWalkingSkeletonBundle(t)
	builderProvider, reviewerProvider := "codex", "codex"
	if live {
		builderProvider = "claude"
		if reverse {
			builderProvider, reviewerProvider = "codex", "claude"
		}
	}
	builderModel, reviewerModel := "claude-sonnet-5", "gpt-5.6-luna"
	if reverse {
		builderModel, reviewerModel = reviewerModel, builderModel
	}
	if cursor {
		builderProvider, reviewerProvider = "cursor", "claude"
		builderModel, reviewerModel = "gpt-5.6-luna-low", "claude-sonnet-5"
		if reverse {
			builderProvider, reviewerProvider = reviewerProvider, builderProvider
			builderModel, reviewerModel = reviewerModel, builderModel
		}
	}
	realCodexHome := os.Getenv("CODEX_HOME")
	if live {
		if realCodexHome == "" {
			ownerHome, err := os.UserHomeDir()
			if err != nil {
				t.Fatal(err)
			}
			realCodexHome = filepath.Join(ownerHome, ".codex")
		}
		cliNames := []string{"codex", "claude"}
		if cursor {
			cliNames = append(cliNames, "cursor-agent")
		}
		for _, name := range cliNames {
			original, err := exec.LookPath(name)
			if err != nil {
				t.Fatalf("live %s unavailable", name)
			}
			original, err = filepath.EvalSymlinks(original)
			if err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(fixtureBin, name)
			// Only freshly built test binaries are replaced, never installed CLIs.
			if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if err := os.Symlink(original, target); err != nil {
				t.Fatal(err)
			}
		}
	}
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
	if live {
		t.Setenv("CODEX_HOME", realCodexHome)
		t.Setenv("SF_CODEX_BUILDER_MODEL", "gpt-5.6-luna")
		t.Setenv("SF_CODEX_REVIEWER_MODEL", "gpt-5.6-luna")
	}
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
		args := []string{"init", "--project", "app", "--repo", repository, "--json"}
		if live {
			args = append(args, "--providers", builderProvider+"-"+reviewerProvider)
		}
		compiledWalkingSkeletonCLI(t, binary, home, args...)
	}
	var daemonOutput compiledSafeBuffer
	var daemonCommand *exec.Cmd
	var daemonDone chan error
	startDaemon := func() {
		t.Helper()
		command := exec.Command(binary, "daemon", "run")
		command.Env = os.Environ()
		command.Stdout, command.Stderr = &daemonOutput, &daemonOutput
		if err := command.Start(); err != nil {
			t.Fatalf("start compiled sf-dev daemon: %v", err)
		}
		done := make(chan error, 1)
		daemonCommand, daemonDone = command, done
		go func() { done <- command.Wait() }()
	}
	startDaemon()
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

	qualificationArgs := []string{"providers", "qualify", "--builder", builderProvider, "--reviewer", reviewerProvider, "--json"}
	if live {
		qualificationArgs = append(qualificationArgs, "--builder-model", builderModel, "--reviewer-model", reviewerModel)
	}
	compiledWalkingSkeletonCLI(t, binary, home, qualificationArgs...)
	if concurrent {
		var restart func()
		if len(approveFirst) > 3 && approveFirst[3] {
			restart = func() {
				stopDaemon()
				if t.Failed() {
					t.Fatal("refusing restart after unproven daemon shutdown")
				}
				startDaemon()
				daemonStopped = false
				compiledWalkingSkeletonWaitSocket(t, paths.Socket, daemonDone, &daemonOutput, &daemonStopped)
				// Qualification is leader-bound. Requalify the exact pair through
				// the CLI; never copy or fabricate its persisted authority.
				compiledWalkingSkeletonCLI(t, binary, home, qualificationArgs...)
			}
		}
		compiledConcurrentTicketsReachPRs(t, binary, home, paths.Database, bare, github, &daemonOutput, builderProvider, reviewerProvider, live, len(approveFirst) > 0 && approveFirst[0], len(approveFirst) > 1 && approveFirst[1], len(approveFirst) > 2 && approveFirst[2], restart)
		return
	}
	ticketPath := filepath.Join(home, "ticket.md")
	ticketSource := fmt.Sprintf("---\ntype: feature\nmerge: %s\nmax_duration: 30m\nmax_cost_usd: 10\n---\n# Implement the fixture\n\nThe verification fixture deliberately begins without its implementation.\n\n## Acceptance\n- The fixture workflow completes.\n", mergeMode)
	if python {
		ticketSource = strings.Replace(ticketSource, "The verification fixture", "SF_E2E_PYTHON_PYTEST: The verification fixture", 1)
	}
	if live {
		ticketSource = "---\ntype: feature\nmerge: guarded\nmax_duration: 20m\nmax_cost_usd: 10\n---\n# Add integer addition\n\nIn this empty dependency-free Go module, create package app with exported function Add(a, b int) int in add.go. Keep scope to add.go and add_test.go. The independent Reviewer authors add_test.go before Builder writes add.go. Use go test ./... as the verification command; SF runs the command itself. Do not commit or change configuration.\n\n## Acceptance\n- Add(2,3) equals 5.\n- Add(-2,2) equals 0.\n- Add(0,0) equals 0.\n"
	}
	reviewRepairFixture := os.Getenv("SF_TEST_REVIEW_REPAIR_FIXTURE") == "1"
	if reviewRepairFixture {
		if live || python || concurrent {
			t.Fatal("review repair fixture requires isolated fake-provider Go ticket")
		}
		ticketSource = strings.Replace(ticketSource, "The verification fixture", "SF_E2E_REVIEW_REPAIR: The verification fixture", 1)
	}
	if err := os.WriteFile(ticketPath, []byte(ticketSource), 0o600); err != nil {
		t.Fatal(err)
	}
	submit := compiledWalkingSkeletonCLI(t, binary, home, "submit", ticketPath, "--project", "app", "--json")
	ref := walkingSkeletonSubmittedRef(t, submit)
	startArgs := []string{"start", string(ref.Ticket), "--json"}
	if live {
		startArgs = append(startArgs, "--accept-cost-estimates")
	}
	compiledWalkingSkeletonCLI(t, binary, home, startArgs...)

	readOnly, err := store.OpenReadOnly(context.Background(), paths.Database)
	if err != nil {
		t.Fatalf("open compiled daemon Store for observation: %v", err)
	}
	defer readOnly.Close()
	wait := func(want domain.State) store.Ticket {
		limit := 2 * time.Minute
		if live {
			limit = 8 * time.Minute
		}
		return walkingSkeletonWaitStateBounded(t, readOnly, ref, want, github, bare, limit, &daemonOutput)
	}
	waitingCI := wait(domain.StateWaitingCI)
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
	if reviewRepairFixture {
		_ = wait(domain.StateBuilding)
		repaired, err := readOnly.CurrentVerification(context.Background(), ref)
		if err != nil || repaired.Revision.Revision <= verification.Revision.Revision || repaired.ProviderResult == verification.ProviderResult {
			t.Fatal("final review repair did not record a fresh verification", err)
		}
		if github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
			t.Fatal("repair authorized external approval or merge")
		}
		return
	}
	waitingState := domain.StateWaitingApproval
	if mergeMode == domain.MergeManual {
		waitingState = domain.StateWaitingManualMerge
	}
	waitingReview := wait(waitingState)
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
	done := wait(domain.StateDone)
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
	if live {
		attempts, err := readOnly.ProviderAttempts(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		for _, attempt := range attempts {
			want := builderProvider
			if attempt.Role == "reviewer" {
				want = reviewerProvider
			}
			identity := attempt.Binding.Identity
			if !compiledLiveIdentityMatches(identity, want) {
				t.Fatalf("wrong live role identity: phase=%s role=%s identity=%+v", attempt.Phase, attempt.Role, identity)
			}
		}
	}
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
// controlled GitHub processes. Approval is granted only to the first ticket
// when explicitly requested by the scenario; live providers are opt-in.
func compiledConcurrentTicketsReachPRs(t *testing.T, binary, home, databasePath, bare string, github *testkit.FakeGH, output *compiledSafeBuffer, builderProvider, reviewerProvider string, live, approveFirst, refreshSibling, deliverSibling bool, restart func()) {
	t.Helper()
	refs := make([]domain.TicketRef, 2)
	for index := range refs {
		path := filepath.Join(home, fmt.Sprintf("concurrent-%d.md", index))
		source := fmt.Sprintf("---\ntype: feature\nmerge: guarded\nmax_duration: 30m\n---\n# Concurrent fixture %d\n\nImplement the fixture in this ticket's independent worktree.\n\n## Acceptance\n- The verification fixture passes.\n", index)
		if live {
			source = fmt.Sprintf("---\ntype: feature\nmerge: guarded\nmax_duration: 20m\nmax_cost_usd: 10\n---\n# Concurrent addition %d\n\nIn this dependency-free Go module implement package app function Add%d(a, b int) int in add%d.go. Limit scope to add%d.go and add%d_test.go. Independent Reviewer writes the test before Builder writes the implementation. Use go test ./...; SF executes it. Do not change config or run Git.\n\n## Acceptance\n- Add%d(2, 3) returns 5.\n- Add%d(-2, 2) returns 0.\n", index, index, index, index, index, index, index)
		}
		if err := os.WriteFile(path, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		refs[index] = walkingSkeletonSubmittedRef(t, compiledWalkingSkeletonCLI(t, binary, home, "submit", path, "--project", "app", "--json"))
	}
	for _, ref := range refs {
		args := []string{"start", string(ref.Ticket), "--json"}
		if live {
			args = append(args, "--accept-cost-estimates")
		}
		compiledWalkingSkeletonCLI(t, binary, home, args...)
	}
	database, err := store.OpenReadOnly(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var attempts [2][]store.ProviderAttempt
	var publications [2]store.PublishedCandidateEvidence
	var candidates [2]store.StoredCandidate
	worktrees, numbers := map[string]bool{}, map[int]bool{}
	for index, ref := range refs {
		limit := 2 * time.Minute
		if live {
			limit = 8 * time.Minute
		}
		walkingSkeletonWaitStateBounded(t, database, ref, domain.StateWaitingCI, github, bare, limit, output)
		published, err := database.LoadHistoricalPublishedCandidate(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		publications[index] = published
		if numbers[published.PullRequest.Number] {
			t.Fatal("tickets shared a PR")
		}
		numbers[published.PullRequest.Number] = true
		candidate, err := database.RecoverableCandidate(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		candidates[index] = candidate
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
		if err != nil {
			t.Fatal("read concurrent attempt history", err)
		}
		if err := validateCompiledAttemptHistory(attempts[index], false); err != nil {
			t.Fatal(err)
		}
		for _, attempt := range attempts[index] {
			if live {
				provider := builderProvider
				if attempt.Role == "reviewer" {
					provider = reviewerProvider
				}
				identity := attempt.Binding.Identity
				if !compiledLiveIdentityMatches(identity, provider) {
					t.Fatalf("wrong concurrent role identity: %+v", identity)
				}
			}
		}
	}
	// The two real ticket pipelines must overlap, even when a short repository
	// writer boundary serializes a particular provider or command interval.
	if !attempts[0][0].StartedAt.Before(attempts[1][len(attempts[1])-1].FinishedAt) || !attempts[1][0].StartedAt.Before(attempts[0][len(attempts[0])-1].FinishedAt) {
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
	if restart != nil {
		var before [2]store.Ticket
		for index, ref := range refs {
			before[index], err = database.Ticket(t.Context(), ref)
			if err != nil || before[index].State != domain.StateWaitingCI {
				t.Fatal("restart requires two settled waiting-CI tickets")
			}
		}
		restart()
		for index, ref := range refs {
			after, err := database.Ticket(t.Context(), ref)
			if err != nil || after.State != domain.StateWaitingCI || after.Version != before[index].Version+1 || after.RunnerEpoch != before[index].RunnerEpoch+1 {
				t.Fatal("restart did not fence the waiting-CI ticket exactly once")
			}
			published, err := database.LoadPublishedCandidate(t.Context(), ref)
			if err != nil || !reflect.DeepEqual(published.PullRequest, publications[index].PullRequest) {
				t.Fatal("restart lost authenticated publication continuity", err)
			}
			history, err := database.ProviderAttempts(t.Context(), ref)
			if err != nil || !reflect.DeepEqual(history, attempts[index]) {
				t.Fatal("restart replayed or changed completed provider attempts")
			}
		}
		if github.MutationCount("pr_create") != 2 || github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
			t.Fatal("restart duplicated publication or bypassed approval")
		}
	}
	if approveFirst {
		for _, published := range publications {
			number := published.PullRequest.Number
			if err := github.SetChecks(number, contracts.RequiredCheck{Name: "unit", ExternalID: fmt.Sprintf("unit-%d", number), State: "success"}); err != nil {
				t.Fatal(err)
			}
		}
		limit := 2 * time.Minute
		if live {
			limit = 8 * time.Minute
		}
		for index, ref := range refs {
			walkingSkeletonWaitStateBounded(t, database, ref, domain.StateWaitingApproval, github, bare, limit, output)
			completed, err := database.ProviderAttempts(t.Context(), ref)
			if err != nil {
				t.Fatal("fresh final review missing", err)
			}
			if err := validateCompiledAttemptHistory(completed, true); err != nil {
				t.Fatal(err)
			}
			attempts[index] = completed
			for _, attempt := range completed {
				if live && attempt.Role == "reviewer" && !compiledLiveIdentityMatches(attempt.Binding.Identity, reviewerProvider) {
					t.Fatal("final review changed provider/model")
				}
			}
		}
		if github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
			t.Fatal("concurrent review bypassed human approval")
		}
		candidate := candidates[0]
		mergeHead := walkingSkeletonSquashCommit(t, bare, candidate.Snapshot.BaseSHA, candidate.Snapshot.HeadSHA)
		if err := github.SetMergeCommitForTest(mergeHead); err != nil {
			t.Fatal(err)
		}
		account, err := user.Current()
		if err != nil {
			t.Fatal(err)
		}
		compiledWalkingSkeletonCLI(t, binary, home, "approve", string(refs[0].Ticket), "--operator", account.Username, "--json")
		walkingSkeletonWaitStateBounded(t, database, refs[0], domain.StateDone, github, bare, limit, output)
		sibling, err := database.Ticket(t.Context(), refs[1])
		if err != nil || sibling.State == domain.StateDone || sibling.State == domain.StateMerging || sibling.State == domain.StateReconciling {
			t.Fatal("approval leaked to sibling ticket", err)
		}
		if github.MutationCount("pr_create") != 2 || github.MutationCount("pr_ready") != 1 || github.MutationCount("pr_merge") != 1 {
			t.Fatal("approval produced duplicate/sibling mutations")
		}
		if got := walkingSkeletonGitOutput(t, bare, "rev-parse", "refs/heads/main"); got != mergeHead {
			t.Fatal("wrong approved protected head")
		}
		if active, err := database.ActiveProviderAttempts(t.Context(), domain.ChannelDev); err != nil || len(active) != 0 {
			t.Fatal("provider lease survived completed reviews", err)
		}
		if refreshSibling {
			// The first merge advances the protected base. The unapproved sibling
			// must use the production refresh boundary, not retain its old base or
			// inherit the first ticket's approval. Do not synthesize Store rows.
			deadline := time.NewTimer(limit)
			defer deadline.Stop()
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			for {
				worktree, err := database.Worktree(t.Context(), refs[1])
				if err != nil {
					t.Fatal("read sibling refresh worktree", err)
				}
				if worktree.BaseSHA == mergeHead {
					if worktree.TicketVersion <= candidates[1].TicketVersion || worktree.HeadSHA == candidates[1].Snapshot.HeadSHA {
						t.Fatal("refresh did not advance sibling worktree authority")
					}
					if github.MutationCount("pr_ready") != 1 || github.MutationCount("pr_merge") != 1 {
						t.Fatal("base refresh authorized sibling publication")
					}
					break
				}
				select {
				case <-tick.C:
				case <-deadline.C:
					current, _ := database.Ticket(t.Context(), refs[1])
					t.Fatalf("sibling refresh timed out: state=%s version=%d", current.State, current.Version)
				case <-t.Context().Done():
					t.Fatal("sibling refresh cancelled")
				}
			}
			if deliverSibling {
				// Git push updates GitHub's existing PR refs independently of PR
				// text edits. The fake intentionally keeps snapshots until told to
				// observe its actual bare repository. Do not inject desired OIDs.
				loggedAuthority := false
				for {
					current, err := database.Ticket(t.Context(), refs[1])
					if err != nil {
						t.Fatal("read sibling push state", err)
					}
					if restart != nil && !loggedAuthority && current.State == domain.StateBuilding {
						history, historyErr := database.ProviderAttempts(t.Context(), refs[1])
						if historyErr == nil {
							for _, attempt := range history {
								if attempt.Phase != domain.PhaseBuild || attempt.Attempt < 2 || attempt.State != "completed" {
									continue
								}
								key := store.ProviderAttemptResultKey{Ref: refs[1], Phase: domain.PhaseBuild, AttemptID: attempt.ID, Attempt: attempt.Attempt}
								fence := domain.Fence{LeaderEpoch: attempt.LeaderEpoch, RunnerEpoch: attempt.RunnerEpoch}
								_, _, loadErr := database.LoadCurrentProviderAttemptResult(t.Context(), key, current.Version, fence)
								reachErr := database.ProviderResultReachesFence(t.Context(), key, current.Version, fence)
								_, refreshErr := database.ProtectedBaseRefreshBuildContext(t.Context(), refs[1], current.Version, fence)
								_, verificationErr := database.CurrentVerification(t.Context(), refs[1])
								_, reusableErr := database.LatestReusableProviderAttempt(t.Context(), store.LatestReusableProviderAttemptRequest{Ref: refs[1], Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: current.Version, Fence: fence})
								t.Logf("refreshed authority probes: current_result=%t result_fence=%t refresh_context=%t verification=%t reusable=%t", loadErr == nil, reachErr == nil, refreshErr == nil, verificationErr == nil, reusableErr == nil)
								loggedAuthority = true
								if loadErr != nil {
									t.Fatal("fresh refreshed Builder result failed current authority after restart")
								}
								break
							}
						}
					}
					if current.State == domain.StatePaused || current.State == domain.StateBlocked || current.State == domain.StateCancelled {
						// Surface the existing bounded, transcript-free diagnostics
						// immediately; no operator resumes this acceptance fixture.
						walkingSkeletonWaitStateBounded(t, database, refs[1], domain.StateWaitingCI, github, bare, time.Nanosecond, output)
					}
					observed, err := github.SyncPullRequestRefsFromBareForTest(publications[1].PullRequest.Number)
					if err != nil {
						t.Fatal("sync private hosted PR refs", err)
					}
					if observed.HeadOID != publications[1].PullRequest.HeadOID && observed.BaseOID == mergeHead {
						break
					}
					select {
					case <-tick.C:
					case <-deadline.C:
						walkingSkeletonWaitStateBounded(t, database, refs[1], domain.StateWaitingCI, github, bare, time.Nanosecond, output)
						t.Fatal("refreshed sibling did not push its source ref")
					case <-t.Context().Done():
						t.Fatal("sibling push wait cancelled")
					}
				}
				walkingSkeletonWaitStateBounded(t, database, refs[1], domain.StateWaitingCI, github, bare, limit, output)
				updated, err := database.LoadHistoricalPublishedCandidate(t.Context(), refs[1])
				if err != nil || updated.PullRequest.Number != publications[1].PullRequest.Number || updated.Candidate.Snapshot.Generation <= candidates[1].Snapshot.Generation || updated.Candidate.Snapshot.BaseSHA != mergeHead {
					t.Fatal("sibling did not republish a fresh candidate to its existing PR", err)
				}
				if err := github.SetChecks(updated.PullRequest.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "unit-refreshed", State: "success"}); err != nil {
					t.Fatal(err)
				}
				walkingSkeletonWaitStateBounded(t, database, refs[1], domain.StateWaitingApproval, github, bare, limit, output)
				after, err := database.ProviderAttempts(t.Context(), refs[1])
				if err != nil {
					t.Fatal("read refreshed role history", err)
				}
				if err := validateCompiledRefreshedHistory(attempts[1], after, mergeHead); err != nil {
					t.Fatal(err)
				}
				if github.MutationCount("pr_create") != 2 || github.MutationCount("pr_ready") != 1 || github.MutationCount("pr_merge") != 1 {
					t.Fatal("refreshed candidate reused approval or created another PR")
				}
				secondMerge := walkingSkeletonSquashCommit(t, bare, mergeHead, updated.Candidate.Snapshot.HeadSHA)
				if err := github.SetMergeCommitForTest(secondMerge); err != nil {
					t.Fatal(err)
				}
				compiledWalkingSkeletonCLI(t, binary, home, "approve", string(refs[1].Ticket), "--operator", account.Username, "--json")
				walkingSkeletonWaitStateBounded(t, database, refs[1], domain.StateDone, github, bare, limit, output)
				if github.MutationCount("pr_create") != 2 || github.MutationCount("pr_ready") != 2 || github.MutationCount("pr_merge") != 2 || walkingSkeletonGitOutput(t, bare, "rev-parse", "refs/heads/main") != secondMerge {
					t.Fatal("second approval did not deliver exactly once")
				}
			}
		}
	}
}

// A base refresh starts only fresh Builder/final-review entries. Prior rows
// remain immutable; the selected runtime must not change or silently fallback.
func validateCompiledRefreshedHistory(before, after []store.ProviderAttempt, base string) error {
	old := map[int64]store.ProviderAttempt{}
	prior := map[domain.Phase]store.ProviderAttempt{}
	for _, attempt := range before {
		old[attempt.ID], prior[attempt.Phase] = attempt, attempt
	}
	fresh := map[domain.Phase][]store.ProviderAttempt{}
	for _, attempt := range after {
		if previous, ok := old[attempt.ID]; ok {
			if !reflect.DeepEqual(previous, attempt) {
				return fmt.Errorf("refresh changed historical provider evidence")
			}
			delete(old, attempt.ID)
			continue
		}
		previous, ok := prior[attempt.Phase]
		if !ok || (attempt.Phase != domain.PhaseBuild && attempt.Phase != domain.PhaseReview) || attempt.Binding != previous.Binding || attempt.Role != previous.Role || attempt.BaseSHA != base || attempt.Input.BaseSHA != base || attempt.ExpectedVersion <= previous.ExpectedVersion || attempt.FinishedAt.IsZero() {
			return fmt.Errorf("refresh reused an old phase or changed runtime/base authority")
		}
		fresh[attempt.Phase] = append(fresh[attempt.Phase], attempt)
	}
	if len(old) != 0 {
		return fmt.Errorf("refresh lost historical provider attempts")
	}
	for _, phase := range []domain.Phase{domain.PhaseBuild, domain.PhaseReview} {
		group := fresh[phase]
		if len(group) < 1 || len(group) > 2 {
			return fmt.Errorf("missing fresh role or exceeded bounded repair")
		}
		for index, attempt := range group {
			if attempt.Attempt != prior[phase].Attempt+index+1 || attempt.ExpectedVersion != group[0].ExpectedVersion || attempt.LeaderEpoch != group[0].LeaderEpoch || attempt.RunnerEpoch != group[0].RunnerEpoch {
				return fmt.Errorf("refresh reset durable attempt sequence")
			}
			if index == len(group)-1 {
				if attempt.State != "completed" || attempt.Outcome != "completed" {
					return fmt.Errorf("refreshed role did not complete")
				}
			} else if attempt.State != "failed" || attempt.Outcome != "invalid_artifact" {
				return fmt.Errorf("refresh blindly retried an uncertain role")
			}
		}
	}
	return nil
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
