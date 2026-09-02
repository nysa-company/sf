//go:build sf_e2e

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

const compiledTakeoverEnteredFile = ".sf-e2e-takeover-entered"

// TestCompiledDevFriendlyOperatorTakeover proves that a real operator can
// drain an in-flight Builder, make one planned source change, and resume a
// fresh guarded Builder cycle through the compiled daemon socket.
func TestCompiledDevFriendlyOperatorTakeover(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("guarded repository command execution is Darwin-only")
	}

	binary, fixtureBin := compiledWalkingSkeletonBundle(t)
	home := compiledWalkingSkeletonHome(t)
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	codexHome, ghConfig := compiledTakeoverCredentialHomes(t, home)

	bareRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repository, bare, base := compiledWalkingSkeletonRepository(t, bareRoot)
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
	t.Setenv("PATH", fixtureBin+":"+filepath.Dir(binary)+":"+filepath.Dir(goBinary)+":/usr/bin:/bin:/usr/sbin:/sbin")
	if resolved, err := exec.LookPath("sf-dev"); err != nil || resolved != binary {
		t.Fatalf("channel-correct next-action executable is unavailable: resolved=%q binary=%q err=%v", resolved, binary, err)
	}

	compiledWalkingSkeletonCLI(t, binary, home, "init", "--project", "app", "--repo", repository, "--json")
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
	ticketPath := filepath.Join(home, "takeover.md")
	ticketSource := "---\ntype: feature\nmerge: guarded\nmax_duration: 30m\nmax_cost_usd: 10\n---\n# Implement the takeover fixture\n\nSF_E2E_TAKEOVER\n\n## Acceptance\n- The fixture workflow completes.\n"
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
	firstBuilder := compiledTakeoverWaitActiveBuilder(t, readOnly, ref)
	firstLaunch, err := readOnly.ProviderLaunchIdentity(context.Background(), firstBuilder.ProviderAttemptClaim)
	if err != nil || firstLaunch.PID <= 1 || firstLaunch.PGID <= 1 {
		t.Fatalf("load live Builder launch: launch=%+v err=%v", firstLaunch, err)
	}
	entryMarker := filepath.Join(firstBuilder.Worktree, compiledTakeoverEnteredFile)
	compiledTakeoverWaitProviderEntry(t, entryMarker)
	if err := os.Remove(entryMarker); err != nil {
		t.Fatalf("remove fixture-only provider entry marker: %v", err)
	}
	if got := walkingSkeletonGitOutput(t, firstBuilder.Worktree, "status", "--porcelain"); got != "" {
		t.Fatalf("provider entry barrier dirtied the takeover worktree: %q", got)
	}
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	firstTake := compiledWalkingSkeletonCLI(t, binary, home, "take", string(ref.Ticket), "--operator", account.Username, "--json")
	firstTakeData := compiledTakeoverAssertTakeResponse(t, firstTake, false)
	compiledTakeoverWaitProcessGroupGone(t, firstLaunch.PGID)
	paused := compiledTakeoverWaitState(t, readOnly, ref, domain.StatePaused)
	if paused.ResumeState != domain.StateBuilding || firstTakeData.Version != paused.Version || firstTakeData.RunnerEpoch != paused.RunnerEpoch || paused.Version != firstBuilder.ExpectedVersion+2 || paused.RunnerEpoch != firstBuilder.RunnerEpoch+1 {
		t.Fatalf("take paused ticket=%+v, want building resume state", paused)
	}
	secondTake := compiledWalkingSkeletonCLI(t, binary, home, "take", string(ref.Ticket), "--operator", account.Username, "--json")
	secondTakeData := compiledTakeoverAssertTakeResponse(t, secondTake, true)
	if secondTakeData.Version != firstTakeData.Version || secondTakeData.RunnerEpoch != firstTakeData.RunnerEpoch || !reflect.DeepEqual(secondTakeData.Takeover, firstTakeData.Takeover) {
		t.Fatalf("idempotent take changed durable handoff: first=%+v second=%+v", firstTakeData, secondTakeData)
	}

	worktree, err := readOnly.Worktree(context.Background(), ref)
	if err != nil {
		t.Fatalf("load registered takeover worktree: %v", err)
	}
	canonicalWorktree, err := filepath.EvalSymlinks(worktree.Path)
	if err != nil || canonicalWorktree != worktree.Path {
		t.Fatalf("registered worktree is not canonical: path=%q canonical=%q err=%v", worktree.Path, canonicalWorktree, err)
	}
	info, statErr := os.Stat(worktree.Path)
	if statErr != nil {
		t.Fatalf("stat registered worktree: %v", statErr)
	}
	if !info.IsDir() || !firstTakeData.Takeover.Registered || firstTakeData.Takeover.Path != worktree.Path || firstTakeData.Takeover.Branch != worktree.Branch || firstTakeData.Takeover.Repository != repository || firstTakeData.Takeover.BaseSHA != worktree.BaseSHA || worktree.HeadSHA != worktree.BaseSHA || firstTakeData.Takeover.ChangeKind != "none" || len(firstTakeData.Takeover.ChangedFiles) != 0 || firstTakeData.Takeover.SourceResumable || !firstTakeData.Takeover.Clean {
		t.Fatalf("CLI handoff=%+v does not match registered worktree=%+v", firstTakeData.Takeover, worktree)
	}
	for _, check := range []struct {
		name string
		argv []string
		want string
	}{
		{name: "top-level", argv: []string{"rev-parse", "--show-toplevel"}, want: worktree.Path},
		{name: "branch", argv: []string{"symbolic-ref", "--short", "HEAD"}, want: worktree.Branch},
		{name: "head", argv: []string{"rev-parse", "HEAD"}, want: firstTakeData.Takeover.HeadSHA},
		{name: "base", argv: []string{"rev-parse", "main"}, want: firstTakeData.Takeover.BaseSHA},
		{name: "origin", argv: []string{"config", "--get", "remote.origin.url"}, want: "https://github.com/acme/app.git"},
		{name: "push origin", argv: []string{"config", "--get", "remote.origin.pushurl"}, want: "https://github.com/acme/app.git"},
	} {
		if got := walkingSkeletonGitOutput(t, worktree.Path, check.argv...); got != check.want {
			t.Fatalf("physical takeover %s=%q, want %q", check.name, got, check.want)
		}
	}
	humanTake := string(compiledWalkingSkeletonCLI(t, binary, home, "take", string(ref.Ticket), "--operator", account.Username))
	for _, want := range []string{"Takeover worktree: " + worktree.Path, "State: paused", "Resume state: building", "Branch: " + worktree.Branch, "Repository: " + repository, "Local base: " + firstTakeData.Takeover.BaseSHA, "Local head: " + firstTakeData.Takeover.HeadSHA, "Remote base: " + firstTakeData.Takeover.RemoteBaseSHA, "Remote candidate: absent", "Remote identity: exact", "Retained proof: " + firstTakeData.Takeover.RetainedProofDigest, "Retained policy: " + firstTakeData.Takeover.RetainedPolicyDigest, "Change kind: none", "Next: sf-dev resume " + string(ref.Ticket)} {
		if !strings.Contains(humanTake, want) {
			t.Fatalf("human take output=%q missing %q", humanTake, want)
		}
	}
	sourcePath := filepath.Join(worktree.Path, "sf_fixture.go")
	operatorSource := "package app\n\n// operator takeover preserved\nfunc SoftwareFactoryFixture() string { return \"ready\" }\n"
	completedSource := operatorSource + "\n// fresh reviewer follow-up\nfunc SoftwareFactoryTakeoverReviewed() string { return \"verified\" }\n"
	if err := os.WriteFile(sourcePath, []byte(operatorSource), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := walkingSkeletonGitOutput(t, worktree.Path, "status", "--porcelain"); got != "?? sf_fixture.go" {
		t.Fatalf("operator changed files=%q, want only planned sf_fixture.go", got)
	}
	dirtyResume := compiledTakeoverCLIFailure(t, binary, home, "resume", string(ref.Ticket), "--operator", account.Username, "--json")
	compiledTakeoverAssertFailureCode(t, dirtyResume, "source_commit_required")
	compiledTakeoverAssertFailureNextAction(t, dirtyResume, []string{"sf-dev", "resume", string(ref.Ticket)})
	if current := compiledTakeoverWaitState(t, readOnly, ref, domain.StatePaused); current.Version != paused.Version || current.RunnerEpoch != paused.RunnerEpoch {
		t.Fatalf("dirty resume changed paused authority: paused=%+v current=%+v", paused, current)
	}
	walkingSkeletonGitOutput(t, worktree.Path, "add", "--", "sf_fixture.go")
	walkingSkeletonGitOutput(t, worktree.Path, "commit", "-m", "operator: preserve takeover source")
	sourceCommit := contracts.OperatorSourceCommit{
		CommitOID: walkingSkeletonGitOutput(t, worktree.Path, "rev-parse", "HEAD^{commit}"),
		ParentOID: walkingSkeletonGitOutput(t, worktree.Path, "rev-parse", "HEAD^1"),
		TreeOID:   walkingSkeletonGitOutput(t, worktree.Path, "rev-parse", "HEAD^{tree}"),
		Changes:   []contracts.OperatorSourceChange{{Status: "A", Path: "sf_fixture.go"}},
	}
	if sourceCommit.ParentOID != firstTakeData.Takeover.HeadSHA {
		t.Fatalf("operator commit=%+v is not the exact checkpoint successor", sourceCommit)
	}
	committedTake := compiledWalkingSkeletonCLI(t, binary, home, "take", string(ref.Ticket), "--operator", account.Username, "--json")
	committedTakeData := compiledTakeoverAssertTakeResponse(t, committedTake, true)
	if !committedTakeData.Takeover.Clean || !committedTakeData.Takeover.SourceResumable || committedTakeData.Takeover.ChangeKind != "source_commit" || !reflect.DeepEqual(committedTakeData.Takeover.SourceCommit, sourceCommit) || !committedTakeData.Takeover.RemoteIdentityExact {
		t.Fatalf("clean operator commit was not authenticated: %+v want=%+v", committedTakeData.Takeover, sourceCommit)
	}

	// Candidate-branch movement after take must refuse before a new Reviewer or
	// Builder is launched. This push is confined to the disposable bare fixture.
	walkingSkeletonGitOutput(t, worktree.Path, "push", bare, sourceCommit.CommitOID+":refs/heads/"+worktree.Branch)
	branchDrift := compiledTakeoverCLIFailure(t, binary, home, "resume", string(ref.Ticket), "--operator", account.Username, "--json")
	compiledTakeoverAssertFailureCode(t, branchDrift, "takeover_remote_drift")
	walkingSkeletonGitOutput(t, bare, "update-ref", "-d", "refs/heads/"+worktree.Branch)
	// Protected-base movement is independently bound to the same take witness.
	walkingSkeletonGitOutput(t, bare, "update-ref", "refs/heads/main", sourceCommit.CommitOID, base)
	baseDrift := compiledTakeoverCLIFailure(t, binary, home, "resume", string(ref.Ticket), "--operator", account.Username, "--json")
	compiledTakeoverAssertFailureCode(t, baseDrift, "takeover_remote_drift")
	walkingSkeletonGitOutput(t, bare, "update-ref", "refs/heads/main", base, sourceCommit.CommitOID)

	resumeOutput := compiledWalkingSkeletonCLI(t, binary, home, "resume", string(ref.Ticket), "--operator", account.Username, "--json")
	resumed := compiledTakeoverAssertResume(t, resumeOutput, paused)
	compiledTakeoverWaitResumeVerification(t, readOnly, ref, resumed, sourceCommit)
	takeoverDiagnostic := compiledTakeoverStateDiagnostic{database: readOnly, ref: ref, resumed: resumed}
	compiledTakeoverWaitFreshBuilderAdmission(t, readOnly, ref, resumed, takeoverDiagnostic)
	waitingCI := walkingSkeletonWaitState(t, readOnly, ref, domain.StateWaitingCI, github, bare, &daemonOutput, takeoverDiagnostic)
	if err := github.SetChecks(1, contracts.RequiredCheck{Name: "unit", ExternalID: "unit-1", State: "success"}); err != nil {
		t.Fatal(err)
	}
	waitingApproval := walkingSkeletonWaitState(t, readOnly, ref, domain.StateWaitingApproval, github, bare, &daemonOutput)
	if waitingApproval.Version <= waitingCI.Version {
		t.Fatalf("CI did not advance ticket: waiting_ci=%+v waiting_approval=%+v", waitingCI, waitingApproval)
	}
	candidate, err := readOnly.RecoverableCandidate(context.Background(), ref)
	if err != nil {
		t.Fatalf("recover candidate after resumed Builder: %v", err)
	}
	mergeHead := walkingSkeletonSquashCommit(t, bare, candidate.Snapshot.BaseSHA, candidate.Snapshot.HeadSHA)
	if err := github.SetMergeCommitForTest(mergeHead); err != nil {
		t.Fatal(err)
	}
	if github.MutationCount("pr_create") != 1 || github.MutationCount("pr_ready") != 0 || github.MutationCount("pr_merge") != 0 {
		t.Fatalf("mutations before approval create=%d ready=%d merge=%d", github.MutationCount("pr_create"), github.MutationCount("pr_ready"), github.MutationCount("pr_merge"))
	}
	compiledWalkingSkeletonCLI(t, binary, home, "approve", string(ref.Ticket), "--operator", account.Username, "--json")
	done := walkingSkeletonWaitState(t, readOnly, ref, domain.StateDone, github, bare, &daemonOutput)
	if done.MergeMode != domain.MergeGuarded {
		t.Fatalf("takeover terminal merge mode=%q, want guarded", done.MergeMode)
	}
	if got := walkingSkeletonGitOutput(t, bare, "rev-parse", "refs/heads/main"); got != mergeHead {
		t.Fatalf("protected main=%s, want exact squash merge=%s", got, mergeHead)
	}
	if got := walkingSkeletonGitOutput(t, worktree.Path, "show", candidate.Snapshot.HeadSHA+":sf_fixture.go"); got != strings.TrimSpace(completedSource) {
		t.Fatalf("candidate did not preserve operator-authored source:\n%s", got)
	}
	if github.MutationCount("pr_create") != 1 || github.MutationCount("pr_ready") != 1 || github.MutationCount("pr_merge") != 1 {
		t.Fatalf("mutations after approval create=%d ready=%d merge=%d", github.MutationCount("pr_create"), github.MutationCount("pr_ready"), github.MutationCount("pr_merge"))
	}

	stopDaemon()
	compiledTakeoverAssertEvidence(t, readOnly, github, ref, worktree.Path, firstBuilder.ID, resumed, sourceCommit, candidate)
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
		t.Fatalf("terminal takeover Store proof: %v", err)
	}
	if err := retirement.RetireRuntime(context.Background(), func(domain.TicketRef) error { return nil }); err != nil {
		t.Fatalf("retire terminal takeover runtime: %v", err)
	}
}

type compiledTakeoverStateDiagnostic struct {
	database *store.Store
	ref      domain.TicketRef
	resumed  store.Ticket
}

func (d compiledTakeoverStateDiagnostic) String() string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	current, ticketErr := d.database.Ticket(ctx, d.ref)
	leader, leaderErr := d.database.LeaderEpoch(ctx, d.ref.Channel)
	resumedFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: d.resumed.RunnerEpoch}
	resumedProof, resumedProofFound, resumedProofErr := d.database.OperatorSourceResumeProof(ctx, d.ref, d.resumed.Version, resumedFence)
	currentFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	currentProof, currentProofFound, currentProofErr := d.database.OperatorSourceResumeProof(ctx, d.ref, current.Version, currentFence)
	prepared, preparedFound, preparedErr := d.database.OperatorSourceResumePreparedCheckpoint(ctx, d.ref, d.resumed.Version, resumedFence)
	recoverable, recoverableErr := d.database.RecoverableVerification(ctx, d.ref)
	currentVerification, currentVerificationErr := d.database.CurrentVerification(ctx, d.ref)
	attempts, attemptsErr := d.database.ProviderAttempts(ctx, d.ref)
	attemptSummary := make([]string, 0, len(attempts))
	for _, attempt := range attempts {
		attemptSummary = append(attemptSummary, fmt.Sprintf("%s/%s#%d:%s/%s@v%d/L%d/R%d", attempt.Phase, attempt.Role, attempt.Attempt, attempt.State, attempt.Outcome, attempt.ExpectedVersion, attempt.LeaderEpoch, attempt.RunnerEpoch))
	}
	return fmt.Sprintf("ticket=%s/v%d/R%d ticket_err=%v leader=%d leader_err=%v resumed_source_proof_found=%v resumed_source_proof_v=%d resumed_source_proof_err=%v current_source_proof_found=%v current_source_proof_v=%d current_source_proof_err=%v prepared_found=%v prepared=%+v prepared_err=%v recoverable=rev%d/amends%d/v%d/%+v/checkpoint=%+v recoverable_err=%v current_verification=rev%d/amends%d/v%d/%+v/checkpoint=%+v current_verification_err=%v attempts=%v attempts_err=%v", current.State, current.Version, current.RunnerEpoch, ticketErr, leader, leaderErr, resumedProofFound, resumedProof.Version, resumedProofErr, currentProofFound, currentProof.Version, currentProofErr, preparedFound, prepared, preparedErr, recoverable.Revision.Revision, recoverable.Revision.Amends, recoverable.TicketVersion, recoverable.Fence, recoverable.Checkpoint, recoverableErr, currentVerification.Revision.Revision, currentVerification.Revision.Amends, currentVerification.TicketVersion, currentVerification.Fence, currentVerification.Checkpoint, currentVerificationErr, attemptSummary, attemptsErr)
}

func compiledTakeoverCredentialHomes(t *testing.T, home string) (codexHome, ghConfig string) {
	t.Helper()
	codexHome = filepath.Join(home, "codex")
	ghConfig = filepath.Join(home, ".config", "gh")
	for _, directory := range []string{codexHome, filepath.Join(home, ".config"), ghConfig} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(codexHome, "auth.json"), []byte("{\"fixture\":true}"), 0o600); err != nil {
		t.Fatal(err)
	}
	return codexHome, ghConfig
}

func compiledTakeoverCLIFailure(t *testing.T, binary, home string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	command.Env = append(os.Environ(), "HOME="+home)
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("compiled sf-dev %s unexpectedly succeeded:\n%s", strings.Join(args, " "), output)
	}
	return output
}

func compiledTakeoverAssertFailureCode(t *testing.T, output []byte, want string) {
	t.Helper()
	var response struct {
		OK    bool `json:"ok"`
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Mutation struct {
			Attempted bool `json:"attempted"`
		} `json:"mutation"`
	}
	if json.Unmarshal(output, &response) != nil || response.OK || response.Error.Code != want || response.Mutation.Attempted {
		t.Fatalf("failure response=%s want code=%s and no mutation", output, want)
	}
}

// A compiled dev client must receive a command it can execute against its own
// channel root; do not let an operator-facing response accidentally point at
// the stable sf binary after a failed takeover inspection.
func compiledTakeoverAssertFailureNextAction(t *testing.T, output []byte, want []string) {
	t.Helper()
	var response struct {
		NextAction struct {
			Argv []string `json:"argv"`
		} `json:"next_action"`
	}
	if json.Unmarshal(output, &response) != nil || !reflect.DeepEqual(response.NextAction.Argv, want) {
		t.Fatalf("failure response=%s next_action=%v want=%v", output, response.NextAction.Argv, want)
	}
}

type compiledTakeResponse struct {
	State       domain.State `json:"state"`
	ResumeState domain.State `json:"resume_state"`
	Version     uint64       `json:"version"`
	RunnerEpoch uint64       `json:"runner_epoch"`
	Takeover    struct {
		Registered             bool                           `json:"registered"`
		Path                   string                         `json:"path"`
		Branch                 string                         `json:"branch"`
		Repository             string                         `json:"repository"`
		BaseSHA                string                         `json:"base_sha"`
		HeadSHA                string                         `json:"head_sha"`
		Clean                  bool                           `json:"clean"`
		ChangeKind             string                         `json:"change_kind"`
		ChangedFiles           []string                       `json:"changed_files"`
		SourceResumable        bool                           `json:"source_resumable"`
		Origin                 string                         `json:"origin"`
		PushOrigin             string                         `json:"push_origin"`
		RemoteCandidatePresent bool                           `json:"remote_candidate_present"`
		RemoteCandidateSHA     string                         `json:"remote_candidate_sha"`
		RemoteBaseSHA          string                         `json:"remote_base_sha"`
		RemoteIdentityExact    bool                           `json:"remote_identity_exact"`
		RetainedProofDigest    string                         `json:"retained_proof_digest"`
		RetainedPolicyDigest   string                         `json:"retained_policy_digest"`
		RetainedVersion        uint64                         `json:"retained_version"`
		RetainedLeaderEpoch    uint64                         `json:"retained_leader_epoch"`
		RetainedRunnerEpoch    uint64                         `json:"retained_runner_epoch"`
		SourceCommit           contracts.OperatorSourceCommit `json:"source_commit"`
	} `json:"takeover"`
}

func compiledTakeoverAssertTakeResponse(t *testing.T, output []byte, observed bool) compiledTakeResponse {
	t.Helper()
	var response struct {
		OK       bool `json:"ok"`
		Mutation struct {
			Attempted bool   `json:"attempted"`
			Kind      string `json:"kind"`
			Observed  bool   `json:"observed"`
		} `json:"mutation"`
		Data compiledTakeResponse `json:"data"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode take response=%s err=%v", output, err)
	}
	if !response.OK || !response.Mutation.Attempted || response.Mutation.Kind != "ticket_take" || response.Mutation.Observed != observed || response.Data.State != domain.StatePaused || response.Data.ResumeState != domain.StateBuilding || response.Data.Version == 0 || response.Data.RunnerEpoch == 0 || !response.Data.Takeover.Registered || response.Data.Takeover.Path == "" || response.Data.Takeover.Branch == "" || response.Data.Takeover.Repository == "" || response.Data.Takeover.Origin == "" || response.Data.Takeover.PushOrigin == "" || response.Data.Takeover.BaseSHA == "" || response.Data.Takeover.HeadSHA == "" || response.Data.Takeover.RemoteBaseSHA == "" || !response.Data.Takeover.RemoteIdentityExact || response.Data.Takeover.RetainedProofDigest == "" || response.Data.Takeover.RetainedPolicyDigest == "" || response.Data.Takeover.RetainedVersion == 0 || response.Data.Takeover.RetainedLeaderEpoch == 0 || response.Data.Takeover.RetainedRunnerEpoch == 0 {
		t.Fatalf("take response=%s, want ticket_take observed=%t", output, observed)
	}
	return response.Data
}

func compiledTakeoverAssertResume(t *testing.T, output []byte, paused store.Ticket) store.Ticket {
	t.Helper()
	var response struct {
		OK       bool `json:"ok"`
		Mutation struct {
			Attempted bool   `json:"attempted"`
			Kind      string `json:"kind"`
			Observed  bool   `json:"observed"`
		} `json:"mutation"`
		Data struct {
			State       domain.State `json:"state"`
			ResumeState domain.State `json:"resume_state"`
			Version     uint64       `json:"version"`
			RunnerEpoch uint64       `json:"runner_epoch"`
		} `json:"data"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode resume response=%s err=%v", output, err)
	}
	if !response.OK || !response.Mutation.Attempted || response.Mutation.Kind != "ticket_resume" || response.Mutation.Observed || response.Data.State != domain.StateVerifying || response.Data.ResumeState != "" || response.Data.Version != paused.Version+1 || response.Data.RunnerEpoch != paused.RunnerEpoch {
		t.Fatalf("resume response=%s does not prove paused -> fresh verification", output)
	}
	return store.Ticket{Ref: paused.Ref, State: response.Data.State, Version: response.Data.Version, RunnerEpoch: response.Data.RunnerEpoch}
}

func compiledTakeoverWaitActiveBuilder(t *testing.T, database *store.Store, ref domain.TicketRef) store.ProviderAttempt {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		attempts, err := database.ProviderAttempts(context.Background(), ref)
		if err == nil {
			for _, attempt := range attempts {
				if attempt.Phase == domain.PhaseBuild && attempt.Role == "builder" && attempt.State == "active" {
					launch, launchErr := database.ProviderLaunchIdentity(context.Background(), attempt.ProviderAttemptClaim)
					if launchErr == nil && launch.PID > 0 && launch.PGID > 0 && launch.Worktree == attempt.Worktree {
						return attempt
					}
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	t.Fatalf("active Builder claim did not appear: attempts=%+v err=%v", attempts, err)
	return store.ProviderAttempt{}
}

func compiledTakeoverWaitProviderEntry(t *testing.T, marker string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		info, err := os.Lstat(marker)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm() == 0o600 && info.Size() == int64(len("provider-entered\n")) {
			data, readErr := os.ReadFile(marker)
			if readErr == nil && string(data) == "provider-entered\n" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("compiled provider never entered the takeover fixture: marker=%q", marker)
}

func compiledTakeoverWaitProcessGroupGone(t *testing.T, pgid int) {
	t.Helper()
	if pgid <= 1 {
		t.Fatalf("unsafe provider process group %d", pgid)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(-pgid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			t.Fatalf("probe provider process group %d: %v", pgid, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("provider process group %d survived the completed take", pgid)
}

func compiledTakeoverWaitState(t *testing.T, database *store.Store, ref domain.TicketRef, want domain.State) store.Ticket {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		ticket, err := database.Ticket(context.Background(), ref)
		if err == nil && ticket.State == want {
			return ticket
		}
		time.Sleep(20 * time.Millisecond)
	}
	ticket, err := database.Ticket(context.Background(), ref)
	t.Fatalf("ticket did not reach %s: ticket=%+v err=%v", want, ticket, err)
	return store.Ticket{}
}

func compiledTakeoverWaitResumeVerification(t *testing.T, database *store.Store, ref domain.TicketRef, resumed store.Ticket, source contracts.OperatorSourceCommit) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		events, err := database.Events(context.Background(), domain.ChannelDev, 0, 1_000)
		if err == nil {
			var matches []store.Event
			for _, event := range events {
				if event.Ref == ref && event.TicketVersion == resumed.Version && event.Trigger == "operator_resume" && event.From == domain.StatePaused && event.To == domain.StateVerifying {
					matches = append(matches, event)
				}
			}
			if len(matches) > 1 {
				t.Fatalf("resume transition was recorded %d times: %+v", len(matches), matches)
			}
			if len(matches) == 1 {
				var payload struct {
					Intent       string                         `json:"intent"`
					ChangeKind   string                         `json:"change_kind"`
					SourceCommit contracts.OperatorSourceCommit `json:"source_commit"`
					Remote       store.TakeoverRemoteBaseline   `json:"remote"`
				}
				if json.Unmarshal([]byte(matches[0].Payload), &payload) != nil || payload.Intent != "resume" || payload.ChangeKind != "source_commit" || !reflect.DeepEqual(payload.SourceCommit, source) || !payload.Remote.Registered || payload.Remote.CandidatePresent || payload.Remote.CandidateOID != "" || payload.Remote.BaseOID == "" {
					t.Fatalf("resume event did not authenticate the exact operator source edit: %+v payload=%s", matches[0], matches[0].Payload)
				}
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	events, err := database.Events(context.Background(), domain.ChannelDev, 0, 1_000)
	t.Fatalf("resume did not reach fresh verification: events=%+v err=%v", events, err)
}

func compiledTakeoverWaitFreshBuilderAdmission(t *testing.T, database *store.Store, ref domain.TicketRef, resumed store.Ticket, diagnostic fmt.Stringer) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		attempts, err := database.ProviderAttempts(context.Background(), ref)
		if err == nil {
			for _, attempt := range attempts {
				if attempt.Phase == domain.PhaseBuild && attempt.Role == "builder" && attempt.Attempt == 2 {
					if attempt.ExpectedVersion != resumed.Version+1 || attempt.RunnerEpoch != resumed.RunnerEpoch {
						t.Fatalf("fresh Builder claim is not bound to the resumed source authority: attempt=%+v resumed=%+v", attempt, resumed)
					}
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fresh Builder was not admitted after source-resume verification: %s", diagnostic.String())
}

func compiledTakeoverAssertEvidence(t *testing.T, database *store.Store, github *testkit.FakeGH, ref domain.TicketRef, worktree string, firstBuilderID int64, resumed store.Ticket, source contracts.OperatorSourceCommit, candidate store.StoredCandidate) {
	t.Helper()
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 6 {
		t.Fatalf("provider attempts=%d, want planner + two verifiers + two builders + final reviewer: %+v", len(attempts), attempts)
	}
	completedRoles := map[string]int{}
	var cancelled, completed, verifications int
	for _, attempt := range attempts {
		if attempt.ID == firstBuilderID {
			if attempt.Phase != domain.PhaseBuild || attempt.Role != "builder" || attempt.State != "cancelled" || attempt.Outcome != "cancelled" || attempt.FinishedAt.IsZero() {
				t.Fatalf("drained first Builder attempt is not durably cancelled: %+v", attempt)
			}
			key := store.ProviderAttemptResultKey{AttemptID: attempt.ID, Ref: ref, Phase: attempt.Phase, Attempt: attempt.Attempt}
			if _, _, loadErr := database.LoadHistoricalProviderAttemptResult(context.Background(), key); !errors.Is(loadErr, store.ErrNotFound) {
				t.Fatalf("cancelled Builder unexpectedly has a provider result: key=%+v err=%v", key, loadErr)
			}
			cancelled++
			continue
		}
		if attempt.State != "completed" || attempt.Outcome != "completed" || attempt.FinishedAt.IsZero() {
			t.Fatalf("provider attempt is not immutable completed evidence: %+v", attempt)
		}
		key := store.ProviderAttemptResultKey{AttemptID: attempt.ID, Ref: ref, Phase: attempt.Phase, Attempt: attempt.Attempt}
		result, parsed, loadErr := database.LoadHistoricalProviderAttemptResult(context.Background(), key)
		if loadErr != nil || result.AttemptID != attempt.ID || result.Claim.ID != attempt.ID || parsed.Phase != attempt.Phase {
			t.Fatalf("provider result does not authenticate attempt=%+v result=%+v parsed=%+v err=%v", attempt, result, parsed, loadErr)
		}
		roleKey := string(attempt.Phase) + "/" + attempt.Role
		completedRoles[roleKey]++
		switch attempt.Phase {
		case domain.PhasePlanning:
			if attempt.Role != "planner" || parsed.Planner == nil {
				t.Fatalf("planning result has wrong role/artifact: %+v %+v", attempt, parsed)
			}
		case domain.PhaseVerification:
			if attempt.Role != "reviewer" || parsed.Verify == nil {
				t.Fatalf("verification result has wrong role/artifact: %+v %+v", attempt, parsed)
			}
			verifications++
			if verifications == 2 && (attempt.ExpectedVersion != resumed.Version || attempt.RunnerEpoch != resumed.RunnerEpoch) {
				t.Fatalf("fresh verification was not bound to source-resume fence: %+v resumed=%+v firstBuilderID=%d", attempt, resumed, firstBuilderID)
			}
		case domain.PhaseBuild:
			if attempt.Role != "builder" || parsed.Builder == nil || attempt.ExpectedVersion != candidate.TicketVersion || attempt.ExpectedVersion <= resumed.Version || attempt.RunnerEpoch != resumed.RunnerEpoch || candidate.BuilderResult != key || candidate.Fence.RunnerEpoch != resumed.RunnerEpoch {
				t.Fatalf("candidate is not bound to the fresh resumed Builder: attempt=%+v candidate=%+v", attempt, candidate)
			}
			completed++
		case domain.PhaseReview:
			if attempt.Role != "reviewer" || parsed.Reviewer == nil || string(parsed.Reviewer.Decision) != "pass" || parsed.Reviewer.ReviewedHead != candidate.Snapshot.HeadSHA || parsed.Reviewer.ProofDigest != candidate.Snapshot.ProofDigest {
				t.Fatalf("final review result has wrong role/artifact: %+v %+v", attempt, parsed)
			}
		default:
			t.Fatalf("unexpected provider phase after takeover: %+v", attempt)
		}
	}
	wantRoles := map[string]int{"planning/planner": 1, "verification/reviewer": 2, "build/builder": 1, "review/reviewer": 1}
	if cancelled != 1 || completed != 1 || verifications != 2 || len(completedRoles) != len(wantRoles) {
		t.Fatalf("Builder attempts cancelled=%d completed=%d, want one each: %+v", cancelled, completed, attempts)
	}
	for role, want := range wantRoles {
		if completedRoles[role] != want {
			t.Fatalf("completed provider role %s=%d, want %d: %+v", role, completedRoles[role], want, attempts)
		}
	}
	verification, err := database.RecoverableVerification(context.Background(), ref)
	if err != nil {
		t.Fatalf("load recoverable verification: %v", err)
	}
	if verification.Checkpoint.ParentOID != source.CommitOID || candidate.Commit.ParentOID != verification.Checkpoint.CommitOID {
		t.Fatalf("source -> fresh verification -> candidate ancestry is not exact: source=%+v verification=%+v candidate=%+v", source, verification.Checkpoint, candidate.Commit)
	}
	prebuild, err := database.LoadRepositoryCommandResult(context.Background(), verification.CommandBinding.Key)
	if err != nil || !prebuild.Result.Observed || prebuild.Result.ExitCode == 0 || !strings.Contains(prebuild.Key.SemanticKey, store.RepositoryCommandPurposePrebuildVerification) || prebuild.Claim.TicketRef != ref || prebuild.Claim.Worktree != worktree || prebuild.Claim.TicketVersion != verification.CommandBinding.TicketVersion || prebuild.Claim.LeaderEpoch != verification.CommandBinding.LeaderEpoch || prebuild.Claim.RunnerEpoch != verification.CommandBinding.RunnerEpoch || verification.CommandBinding.Key != prebuild.Key {
		t.Fatalf("pre-build regression command is not observed red evidence: result=%+v verification=%+v worktree=%q err=%v", prebuild, verification, worktree, err)
	}
	postbuild, err := database.LoadRepositoryCommandResult(context.Background(), candidate.CommandBinding.Key)
	if err != nil || !postbuild.Result.Observed || postbuild.Result.ExitCode != 0 || !strings.Contains(postbuild.Key.SemanticKey, store.RepositoryCommandPurposePostbuildCandidate) || postbuild.Claim.TicketRef != ref || postbuild.Claim.Worktree != prebuild.Claim.Worktree || postbuild.Claim.TicketVersion != candidate.TicketVersion || postbuild.Claim.LeaderEpoch != candidate.Fence.LeaderEpoch || postbuild.Claim.RunnerEpoch != candidate.Fence.RunnerEpoch || candidate.CommandBinding.Key != postbuild.Key {
		t.Fatalf("post-build candidate command is not observed green evidence: result=%+v err=%v", postbuild, err)
	}
	if active, err := database.ActiveProviderAttempts(context.Background(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active/quarantined provider claims=%+v err=%v", active, err)
	}
	if active, err := database.ActiveRepositoryCommandLeases(context.Background(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active/quarantined repository leases=%+v err=%v", active, err)
	}
	if active, err := database.ActiveGitMutationLeases(context.Background(), domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatalf("active/quarantined Git mutation leases=%+v err=%v", active, err)
	}
	if leases, err := database.Leases(context.Background(), domain.ChannelDev); err != nil || len(leases) != 0 {
		t.Fatalf("active Store leases=%+v err=%v", leases, err)
	}
	for _, operation := range []string{"pr_create", "pr_ready", "pr_merge"} {
		if got := github.DeliveryCount(operation); got != 1 {
			t.Fatalf("delivery count for %s=%d, want one external handoff", operation, got)
		}
	}
}
