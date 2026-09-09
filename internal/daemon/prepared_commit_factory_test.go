package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
)

func TestDaemonPreparedCommitRunnerFactoryIsLazyAndPrecedesRuntime(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		name := "empty"
		if prepared {
			name = "prepared"
		}
		t.Run(name, func(t *testing.T) {
			previous, paths, cancel := testDaemon(t)
			if prepared {
				seedPreparedCommit(t, previous, "SF-lazy-prepared")
			}
			closeDaemonForPreparedRecovery(t, previous, cancel)
			calls, runtimeCalls := 0, 0
			assetError := errors.New("test trusted asset unavailable")
			restarted, err := Start(context.Background(), Config{
				Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "lazy-prepared-restart",
				PreparedCommitRunnerFactory: func() (git.Runner, error) {
					calls++
					return git.Runner{}, assetError
				},
				WorkflowRuntimeFactory: func(RuntimeDependencies) (WorkflowRuntimeComponents, error) {
					runtimeCalls++
					return WorkflowRuntimeComponents{}, nil
				},
			})
			if restarted != nil {
				t.Cleanup(func() { _ = restarted.Close() })
			}
			if prepared {
				if !errors.Is(err, assetError) || calls != 1 || runtimeCalls != 0 {
					t.Fatalf("prepared startup err=%v asset calls=%d runtime calls=%d", err, calls, runtimeCalls)
				}
				if _, statErr := os.Lstat(paths.Socket); !os.IsNotExist(statErr) {
					t.Fatalf("socket exposed before prepared recovery: %v", statErr)
				}
			} else if err != nil || calls != 0 || runtimeCalls != 1 {
				t.Fatalf("idle startup err=%v asset calls=%d runtime calls=%d", err, calls, runtimeCalls)
			}
		})
	}
}

func TestDaemonPreparedCommitResolverRejectsForgedAndFutureRegistration(t *testing.T) {
	for _, mode := range []string{"forged_claim", "missing_prepared", "future_version", "future_leader", "future_runner", "identity"} {
		t.Run(mode, func(t *testing.T) {
			daemon, paths, _ := testDaemon(t)
			_, claim, _, _ := seedPreparedCommit(t, daemon, "SF-resolver-negative", mode != "missing_prepared")
			if mode == "forged_claim" {
				claim.TicketVersion++
			} else if mode != "missing_prepared" {
				// No normal API can move immutable registration provenance into
				// the future or replace its identity. Corrupt only negative fixtures.
				assignment := "ticket_version=ticket_version+1"
				switch mode {
				case "future_leader":
					assignment = "leader_epoch=leader_epoch+1"
				case "future_runner":
					assignment = "runner_epoch=runner_epoch+1"
				case "identity":
					assignment = "identity_json=json_set(identity_json,'$.Repository','/foreign')"
				}
				writer, err := sql.Open("sqlite", paths.Database)
				if err != nil {
					t.Fatal(err)
				}
				result, err := writer.ExecContext(t.Context(), `UPDATE worktrees SET `+assignment+` WHERE channel=? AND project_id=? AND ticket_id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket)
				closeErr := writer.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("negative fixture: %v %v", err, closeErr)
				}
				if count, err := result.RowsAffected(); err != nil || count != 1 {
					t.Fatalf("negative fixture rows=%d err=%v", count, err)
				}
			}
			if _, err := registeredWorktreeResolver(daemon.store)(t.Context(), claim); !errors.Is(err, git.ErrIdentityMismatch) {
				t.Fatalf("%s accepted: %v", mode, err)
			}
		})
	}
}

func TestDaemonPreparedCommitRunnerFactoryRecoversRealGitBeforeRuntime(t *testing.T) {
	ctx := t.Context()
	temporary, err := os.MkdirTemp("/tmp", "sfpg-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporary) })
	root, err := filepath.EvalSymlinks(temporary)
	if err != nil {
		t.Fatal(err)
	}
	paths := config.ChannelPaths{Root: root, Database: filepath.Join(root, "sf.sqlite"), Socket: filepath.Join(root, "run", "sf.sock"), Logs: filepath.Join(root, "logs"), Events: filepath.Join(root, "events"), Worktrees: filepath.Join(root, "worktrees"), Backups: filepath.Join(root, "backups")}
	repository, home := filepath.Join(root, "repo"), filepath.Join(root, "git-home")
	for _, path := range []string{repository, home} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	rawGit := func(directory string, args ...string) string {
		t.Helper()
		command := exec.CommandContext(ctx, "/usr/bin/git", append([]string{"-C", directory, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false"}, args...)...)
		command.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=fixture", "GIT_AUTHOR_EMAIL=fixture@example.test", "GIT_COMMITTER_NAME=fixture", "GIT_COMMITTER_EMAIL=fixture@example.test"}
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("fixture Git: %v: %s", err, output)
		}
		return strings.TrimSpace(string(output))
	}
	rawGit(repository, "init", "-b", "main")
	rawGit(repository, "commit", "--allow-empty", "-m", "base")
	rawGit(repository, "remote", "add", "origin", "https://github.com/example/prepared-fixture.git")
	base := rawGit(repository, "rev-parse", "HEAD")
	helperRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(helperRoot, "sf-git-exec")
	build := exec.CommandContext(ctx, "go", "build", "-o", helper, "../../cmd/sf-git-exec")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build trusted Git gate: %v: %s", err, output)
	}
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("demo", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	configuration := Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "real-prepared-initial", Projects: []store.Project{{Channel: domain.ChannelStable, ID: "demo", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}}}
	initial, err := Start(ctx, configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = initial.Close() })
	ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "demo", Ticket: "SF-real-prepared"}
	if err := initial.store.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "real-prepared", Type: domain.TicketBug, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	queued, err := initial.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := initial.store.StartOrAdopt(ctx, ref, queued.Version, "real-prepared/planning", domain.Fence{LeaderEpoch: initial.epoch, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: initial.epoch, RunnerEpoch: started.RunnerEpoch}
	branch := "sf/stable/aaaaaaaa/aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	worktree := filepath.Join(paths.Worktrees, "prepared")
	rawGit(repository, "worktree", "add", "-b", branch, worktree, "main")
	runner := git.Runner{Binary: "/usr/bin/git", Home: home, ExecHelper: helper}
	identity, err := runner.Snapshot(ctx, worktree, "main")
	if err != nil {
		t.Fatal(err)
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.store.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Path: worktree, Branch: branch, IdentityJSON: identityJSON, BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	// Match production: registration belongs to Planning, but the prepared
	// checkpoint belongs to the later Verifying phase. Advance using the
	// typed Planner result and TransitionPlan, never a fabricated event row.
	builder, _, err := initial.store.RecordProviderQualification(ctx, daemonFixtureQualification(initial.channel, strings.Repeat("a", 32), "fixture-cursor", "cursor-family"))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := initial.store.RecordProviderQualification(ctx, daemonFixtureQualification(initial.channel, strings.Repeat("b", 32), "fixture-claude", "claude-family"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := initial.store.SelectProviderPair(ctx, initial.channel, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	binding := daemonFixtureBinding(builder)
	planner, err := initial.store.BeginProviderAttempt(ctx, store.ProviderAttemptRequest{
		Ref: ref, ExpectedVersion: started.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: binding, ConfigDigest: started.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
		Repository: repository, Worktree: worktree, WorktreeIdentity: string(identityJSON), BaseSHA: base, SupervisorKey: signer.PublicKey(),
		Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ExpectedVersion: started.Version, Prompt: "prepared checkpoint fixture", Repository: repository, Worktree: worktree, WorktreeIdentity: string(identityJSON), BaseSHA: base, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := initial.store.RecordProviderLaunch(ctx, planner, contracts.ProviderLaunch{PID: int(planner.ID), PGID: int(planner.ID), BootIdentity: "fixture", ProcessStartIdentity: "fixture-planner", Worktree: worktree}); err != nil {
		t.Fatal(err)
	}
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"fixture acceptance"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofRegression, Command: []string{"go", "test"}, Details: "fixture regression proof"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"fixture"}}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planResult := contracts.PhaseResult{Provider: binding.Identity, Artifact: planRaw, UsageTrusted: true, UsageUnits: 1}
	planValidation := phaseartifact.Validation{TicketType: started.Type}
	if _, err := phaseartifact.Parse(domain.PhasePlanning, planResult, planValidation); err != nil {
		t.Fatalf("fixture Planner artifact: %v", err)
	}
	if _, err := initial.store.CompleteProviderAttemptSuccess(ctx, planner, daemonFixtureDrainProof(t, signer, planner), started.Version, fence, planResult, planValidation, time.Now().UTC()); err != nil {
		t.Fatalf("complete fixture Planner: %v", err)
	}
	planKey := store.ProviderAttemptResultKey{AttemptID: planner.ID, Ref: ref, Phase: domain.PhasePlanning, Attempt: planner.Attempt}
	if _, err := initial.store.RecordPlan(ctx, store.PlanArtifact{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Document: store.PlanDocument{Planner: &plan, ProviderResult: &planKey, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); err != nil {
		t.Fatal(err)
	}
	if _, err := initial.store.TransitionPlan(ctx, store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	started, err = initial.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = started.RunnerEpoch
	registered, err := initial.store.Worktree(ctx, ref)
	if err != nil || registered.TicketVersion+1 != started.Version || started.State != domain.StateVerifying {
		t.Fatalf("fixture did not cross registration/commit phase boundary: %v", err)
	}
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: ref, TicketVersion: started.Version, Fence: fence}, RequestDigest: "sha256:" + strings.Repeat("d", 64), Repository: repository, Worktree: worktree, Branch: branch, Operation: "commit", BaseRef: "main", ExpectedBaseOID: base, ExpectedHeadOID: base}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if _, err := initial.store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "git/commit", TicketVersion: started.Version, Fence: fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := initial.store.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := initial.store.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	rawGit(worktree, "commit", "--allow-empty", "-m", "prepared child")
	child, tree := rawGit(worktree, "rev-parse", "HEAD"), rawGit(worktree, "rev-parse", "HEAD^{tree}")
	prepared, ok := lease.(contracts.GitMutationRecoveryFactsLease)
	if !ok {
		t.Fatal("missing prepared recorder")
	}
	if err := prepared.RecordPreparedCommit(ctx, child, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	calls, runtimeCalls := 0, 0
	configuration.DaemonIdentity = "real-prepared-restart"
	configuration.PreparedCommitRunnerFactory = func() (git.Runner, error) { calls++; return runner, nil }
	configuration.WorkflowRuntimeFactory = func(dependencies RuntimeDependencies) (WorkflowRuntimeComponents, error) {
		runtimeCalls++
		effect, err := dependencies.Store.Effect(ctx, intent.SemanticKey)
		if err != nil || effect.State != store.EffectConfirmed || effect.ObservedIdentity != child {
			t.Fatalf("runtime preceded exact prepared confirmation: %+v %v", effect, err)
		}
		return WorkflowRuntimeComponents{}, nil
	}
	for restart := 0; restart < 2; restart++ {
		daemon, err := Start(ctx, configuration)
		if err != nil {
			t.Fatal(err)
		}
		if err := daemon.Close(); err != nil {
			t.Fatal(err)
		}
		if calls != 1 || runtimeCalls != restart+1 || rawGit(worktree, "rev-parse", "HEAD") != child || rawGit(worktree, "rev-list", "--count", base+"..HEAD") != "1" {
			t.Fatal("prepared recovery repeated observation or replaced the existing commit")
		}
	}
}
