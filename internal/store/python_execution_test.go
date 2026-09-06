package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/executionpolicy"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/pythonclosure"
	"github.com/nysa-company/sf/internal/repositoryexec"
)

// This explicitly provisioned acceptance never discovers or installs a host
// Python. Missing fixture inputs are reported as skipped, not runtime support.
// The digests are supplied independently of the prepared manifest.
func TestPreparedPythonStoreExecution(t *testing.T) {
	if goruntime.GOOS != "darwin" {
		t.Skip("macOS prepared Python acceptance")
	}
	root, envDigest, lockDigest := os.Getenv("SF_TEST_PYTHON_SNAPSHOTS"), os.Getenv("SF_TEST_PYTHON_DIGEST"), os.Getenv("SF_TEST_PYTHON_LOCK")
	if root == "" && envDigest == "" && lockDigest == "" {
		t.Skip("explicit prepared Python fixture is not provisioned")
	}
	argv := []string{"python3", pythonclosure.RecipeFlag, envDigest, lockDigest, "test_proof.py"}
	if _, err := pythonclosure.ParseRecipe(argv); err != nil || !filepath.IsAbs(root) {
		t.Fatal("invalid explicit Python fixture")
	}
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	command := func(dir, name string, args ...string) {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fixture %s failed: %v: %s", name, err, out)
		}
	}
	self, helper := filepath.Join(bin, "sf"), filepath.Join(bin, "sf-git-exec")
	command(repoRoot, "go", "build", "-o", self, "./cmd/sf")
	command(repoRoot, "go", "build", "-o", helper, "./cmd/sf-git-exec")
	for _, test := range []struct {
		name, source string
		timeout      time.Duration
		exit         int
		abort        error
	}{
		{"pass", "import os, socket, subprocess, pytest\ndef test_bounds(tmp_path):\n    assert (1+1)==2\n    (tmp_path/'allowed').write_text('ok')\n    with pytest.raises(OSError): open('forbidden','w')\n    with pytest.raises(OSError): subprocess.run(['/bin/echo','no'])\n    with pytest.raises(OSError): socket.create_connection(('127.0.0.1',9))\n", 30 * time.Second, 0, nil},
		{"red", "def test_red():\n    assert False\n", 30 * time.Second, 1, nil},
		{"timeout", "import time\ndef test_wait():\n    time.sleep(30)\n", 5 * time.Second, -1, context.DeadlineExceeded},
		{"cancel", "import time\ndef test_wait():\n    time.sleep(30)\n", 30 * time.Second, -1, context.Canceled},
		{"quota", "import os, time, tempfile\ndef test_quota():\n    for i in range(9):\n        with open(os.path.join(tempfile.gettempdir(),str(i)), 'wb') as f: f.truncate(15*1024*1024)\n    time.sleep(30)\n", 30 * time.Second, -1, contracts.ErrRepositoryCommandResourceLimit},
		{"filelimit", "import os, tempfile\ndef test_file_limit():\n    with open(os.path.join(tempfile.gettempdir(),'oversize'), 'wb') as f: f.truncate(17*1024*1024)\n", 30 * time.Second, -1, contracts.ErrRepositoryCommandResourceLimit},
		{"restart", "import time\ndef test_wait():\n    time.sleep(30)\n", 30 * time.Second, -1, nil},
		{"restart-unclear", "import time\ndef test_wait():\n    time.sleep(30)\n", 30 * time.Second, -1, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			repo := t.TempDir()
			command(repo, "git", "init", "-b", "main")
			command(repo, "git", "config", "user.email", "sf@example.test")
			command(repo, "git", "config", "user.name", "SF")
			command(repo, "git", "remote", "add", "origin", "https://github.com/example/prepared-python-fixture.git")
			if err := os.WriteFile(filepath.Join(repo, "test_proof.py"), []byte(test.source), 0600); err != nil {
				t.Fatal(err)
			}
			command(repo, "git", "add", ".")
			command(repo, "git", "commit", "-m", "fixture")
			worktree := filepath.Join(t.TempDir(), "worktree")
			branch := "sf/dev/python/" + test.name
			command(repo, "git", "worktree", "add", "-b", branch, worktree)
			home := filepath.Join(t.TempDir(), "git-home")
			if err := os.Mkdir(home, 0700); err != nil {
				t.Fatal(err)
			}
			runner := gitboundary.Runner{Binary: "/usr/bin/git", ExecHelper: helper, Home: home}
			identity, err := runner.Snapshot(ctx, worktree, "main")
			if err != nil {
				t.Fatal(err)
			}
			identityJSON, err := json.Marshal(identity)
			if err != nil {
				t.Fatal(err)
			}
			dbPath := filepath.Join(t.TempDir(), "sf.sqlite")
			db, err := Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("python", identity.Repository), config.TicketOverride{})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, digest, err := config.Snapshot(effective)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.CreateProject(ctx, Project{Channel: domain.ChannelDev, ID: "python", Path: identity.Repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}); err != nil {
				t.Fatal(err)
			}
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "python", Ticket: domain.TicketID("SF-python-" + test.name)}
			if err := db.CreateTicket(ctx, ticket(ref, "source-"+test.name)); err != nil {
				t.Fatal(err)
			}
			leader, err := db.AcquireLeader(ctx, ref.Channel, "python-acceptance")
			if err != nil {
				t.Fatal(err)
			}
			current, err := db.Ticket(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			started, err := db.StartOrAdopt(ctx, ref, current.Version, branch, domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
			if err != nil {
				t.Fatal(err)
			}
			fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
			if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: identity.Worktree, Branch: branch, IdentityJSON: identityJSON, BaseSHA: identity.BaseHead, HeadSHA: identity.BaseHead}); err != nil {
				t.Fatal(err)
			}
			supervisor := processsupervisor.RepositoryCommandSupervisor{Executable: self, GitRunner: runner, PythonSnapshots: root, SoftDrain: 100 * time.Millisecond, HardDrain: time.Second}
			executable, exeDigest, err := supervisor.CommandExecutableIdentity(ctx, argv)
			if err != nil {
				t.Fatal(err)
			}
			policy, err := executionpolicy.NewCommandSnapshot(argv)
			if err != nil {
				t.Fatal(err)
			}
			spec := contracts.CommandSpec{Argv: argv, Directory: identity.Worktree, Timeout: test.timeout, Profile: contracts.ProfileGuarded}
			commandDigest, err := repositoryexec.CommandDigest(argv)
			if err != nil {
				t.Fatal(err)
			}
			empty := sha256.Sum256(nil)
			specDigest, err := repositoryexec.SpecDigest(spec, "sha256:"+hex.EncodeToString(empty[:]))
			if err != nil {
				t.Fatal(err)
			}
			intent := RepositoryCommandIntent{EffectFence: EffectFence{SemanticKey: "repository-command/python-" + test.name, Ref: ref, TicketVersion: started.Version, Fence: fence}, RequestDigest: repositoryCommandDigest("d"), Repository: identity.Repository, Worktree: identity.Worktree, WorktreeIdentity: string(identityJSON), Branch: branch, BaseRef: "main", BaseSHA: identity.BaseHead, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest(), ExecutablePath: executable, ExecutableDigest: exeDigest}
			if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "repository_command", TicketVersion: started.Version, Fence: fence, RequestDigest: intent.RequestDigest}); err != nil {
				t.Fatal(err)
			}
			claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(test.name, "restart") {
				assertPythonCrashRecovery(t, db, dbPath, supervisor, claim, spec, test.name == "restart-unclear")
				return
			}
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()
			cancelDone := make(chan struct{})
			if test.name == "cancel" {
				go func() {
					defer close(cancelDone)
					ticker := time.NewTicker(10 * time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-runCtx.Done():
							return
						case <-ticker.C:
						}
						var released int
						if err := db.db.QueryRowContext(runCtx, `SELECT COUNT(*) FROM repository_command_leases WHERE semantic_key=? AND launch_state='released'`, claim.SemanticKey).Scan(&released); err != nil || released != 1 {
							continue
						}
						// Give the recorded child time to cross the gate. Observed
						// below distinguishes this from pre-gate cancellation.
						timer := time.NewTimer(2 * time.Second)
						select {
						case <-runCtx.Done():
							timer.Stop()
							return
						case <-timer.C:
							cancelRun()
							return
						}
					}
				}()
			} else {
				close(cancelDone)
			}
			result, runErr := (repositoryexec.Executor{Authority: db, Supervisor: supervisor}).Run(runCtx, repositoryexec.Request{Claim: claim, Spec: spec, Policy: policy})
			cancelRun()
			<-cancelDone
			if !result.Observed || result.ExitCode != test.exit {
				t.Fatalf("observed=%v exit=%d err=%v", result.Observed, result.ExitCode, runErr)
			}
			if test.abort != nil && !errors.Is(runErr, test.abort) {
				t.Fatalf("abort=%v", runErr)
			}
			if test.exit == 0 && runErr != nil {
				t.Fatal(runErr)
			}
			_, loadErr := db.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch})
			if test.abort != nil {
				if !errors.Is(loadErr, ErrNotFound) {
					t.Fatalf("abort minted result: %v", loadErr)
				}
			} else if loadErr != nil {
				t.Fatal(loadErr)
			}
			var leases int
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE semantic_key=?`, claim.SemanticKey).Scan(&leases); err != nil || leases != 0 {
				t.Fatalf("leases=%d err=%v", leases, err)
			}
		})
	}
}
