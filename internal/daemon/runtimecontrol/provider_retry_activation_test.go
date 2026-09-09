package runtimecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
)

type retryActivationWorker struct{ calls int }

func (w *retryActivationWorker) Run(context.Context, domain.TicketRef, domain.Fence) (workflowworker.RunResult, error) {
	w.calls++
	return workflowworker.RunResult{}, nil
}

func TestProviderRetryRestoredControllerActivatesRuntime(t *testing.T) {
	for _, failedInstall := range []bool{false, true} {
		t.Run(fmt.Sprintf("prior_failed_install_%t", failedInstall), func(t *testing.T) {
			db, ticket, fence, runner := retryActivationFixture(t)
			worker := &retryActivationWorker{}
			scheduler := workflowruntime.NewScheduler(ticket.Ref.Channel, workflowruntime.StoreTicketSource{Store: db}, controlEnsure{}, worker)
			runtime, err := workflowruntime.NewRuntime(scheduler, time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			stopped, err := db.StoppedRuntimeTicket(t.Context(), ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if failedInstall {
				// Reproduce the old installation boundary against a fresh bundle:
				// Store issues proof, but no runtime stop latch exists yet.
				capability, err := db.ProviderRetryRearmProof(t.Context(), ticket.Ref, stopped)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.ActivateRearm(t.Context(), capability, runtime.ControlBundle().ApplyRearm); !errors.Is(err, workflowruntime.ErrRuntimeRearm) {
					t.Fatalf("fresh unjoined installation=%v", err)
				}
				if ready, err := db.RuntimeAdmissionReady(t.Context(), ticket.Ref, ticket.Version, fence); err != nil || ready {
					t.Fatalf("failed install opened Store=%v err=%v", ready, err)
				}
			}
			controller, err := New(db, runtime.ControlBundle(), nil, runner)
			if err != nil {
				t.Fatal(err)
			}
			ready, err := controller.RearmProviderRetry(t.Context(), ticket.Ref, ticket.Version, fence)
			if err != nil || !ready {
				t.Fatalf("restored provider retry activation ready=%v err=%v", ready, err)
			}
			if ready, err := db.RuntimeAdmissionReady(t.Context(), ticket.Ref, ticket.Version, fence); err != nil || ready {
				t.Fatalf("rearm bypassed Begin=%v err=%v", ready, err)
			}
			result := scheduler.Tick(t.Context(), fence)
			if worker.calls != 1 {
				t.Fatalf("exact Begin did not run one worker: calls=%d result=%+v", worker.calls, result)
			}
			if ready, err := db.RuntimeAdmissionReady(t.Context(), ticket.Ref, ticket.Version, fence); err != nil || !ready {
				t.Fatalf("matching Begin did not open Store=%v err=%v", ready, err)
			}
			current, err := db.Ticket(t.Context(), ticket.Ref)
			if err != nil || current.Version != ticket.Version || current.RunnerEpoch != ticket.RunnerEpoch {
				t.Fatalf("rearm minted lifecycle step: %+v %v", current, err)
			}
			entry := controller.tickets[ticket.Ref]
			if entry == nil || !entry.hasStop || entry.stopped.Version != stopped.Version || entry.stopped.RunnerEpoch != stopped.RunnerEpoch {
				t.Fatal("rearm replaced historical stop")
			}
		})
	}
}

func retryActivationFixture(t *testing.T) (*store.Store, store.Ticket, domain.Fence, git.Runner) {
	t.Helper()
	ctx := t.Context()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(root, "repository")
	if err := os.MkdirAll(repository, 0700); err != nil {
		t.Fatal(err)
	}
	rawGit := func(dir string, args ...string) {
		t.Helper()
		cmd := exec.Command("/usr/bin/git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "TMPDIR="+root)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("fixture git: %v %s", err, out)
		}
	}
	rawGit(repository, "init", "-b", "main")
	rawGit(repository, "config", "user.name", "fixture")
	rawGit(repository, "config", "user.email", "fixture@example.test")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	rawGit(repository, "add", "README.md")
	rawGit(repository, "commit", "-m", "fixture")
	rawGit(root, "init", "--bare", filepath.Join(root, "origin.git"))
	rawGit(repository, "remote", "add", "origin", filepath.Join(root, "origin.git"))
	rawGit(repository, "push", "origin", "main")
	db, err := store.Open(ctx, filepath.Join(root, "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("retry", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "retry", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "retry", Ticket: "SF-retry-activation"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: fmt.Sprintf("%x", sha256.Sum256([]byte("source"))), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, ref.Channel, "retry-activation")
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "retry-activation", fence)
	if err != nil {
		t.Fatal(err)
	}
	path, err := db.TicketWorktreePath(ref)
	if err != nil {
		t.Fatal(err)
	}
	projectHash, ticketHash := sha256.Sum256([]byte(ref.Project)), sha256.Sum256([]byte(ref.Ticket))
	branch := fmt.Sprintf("sf/dev/%x/%x-%s", projectHash[:8], ticketHash[:8], strings.Repeat("b", 32))
	if _, err := db.LoadOrStoreBranchUnderFence(ctx, string(ref.Channel)+"\x00"+string(ref.Project)+"\x00"+string(ref.Ticket), branch, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	rawGit(repository, "worktree", "add", "-b", branch, path, "main")
	runner := git.Runner{Binary: "/usr/bin/git", Home: filepath.Join(root, "git-home"), TestLocalTransport: true, Run: func(ctx context.Context, binary string, args, env []string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = env
		return cmd.Output()
	}}
	identity, err := runner.Snapshot(ctx, path, "main")
	if err != nil {
		t.Fatal(err)
	}
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	creation := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: ref, TicketVersion: ticket.Version, Fence: fence}, Operation: "create-worktree", Repository: repository, Worktree: path, Branch: branch, BaseRef: "main", ExpectedBaseOID: identity.BaseHead, ExpectedHeadOID: identity.BaseHead, RequestDigest: "sha256:" + strings.Repeat("a", 64)}
	creation.SemanticKey = store.CanonicalGitMutationSemanticKey(creation)
	if _, err := db.PlanEffect(ctx, store.EffectPlan{SemanticKey: creation.SemanticKey, Ref: ref, Kind: "git/create-worktree", TicketVersion: ticket.Version, Fence: fence, RequestDigest: creation.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	creationClaim, err := db.IssueGitMutationClaim(ctx, creation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, store.EffectFence{SemanticKey: creationClaim.SemanticKey, Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1, ClaimEpoch: creationClaim.ClaimEpoch}}, string(identityJSON)); err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Path: path, Branch: branch, IdentityJSON: identityJSON, BaseSHA: identity.BaseHead, HeadSHA: identity.BaseHead}); err != nil {
		t.Fatal(err)
	}
	qualify := func(name, family, id string) store.ProviderQualification {
		q, _, err := db.RecordProviderQualification(ctx, store.ProviderQualification{Channel: ref.Channel, RunID: id, Provider: domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: family, Version: "1.0.0"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	builder, reviewer := qualify("fixture-cursor", "cursor-family", strings.Repeat("a", 32)), qualify("fixture-claude", "claude-family", strings.Repeat("b", 32))
	if _, _, err := db.SelectProviderPair(ctx, ref.Channel, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	binding := contracts.RuntimeBinding{Identity: builder.Provider, BinaryDigest: builder.BinaryDigest, PolicyDigest: builder.PolicyDigest, FixtureDigest: builder.FixtureDigest, AuthDigest: strings.Repeat("d", 64)}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		request := store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: binding, ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(), Repository: repository, Worktree: path, WorktreeIdentity: string(identityJSON), BaseSHA: identity.BaseHead, SupervisorKey: signer.PublicKey(), Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, LeaderEpoch: leader, RunnerEpoch: 1, ExpectedVersion: ticket.Version, Prompt: "fixture", Repository: repository, Worktree: path, WorktreeIdentity: string(identityJSON), BaseSHA: identity.BaseHead, AllowedPaths: []string{"."}, Provider: binding.Identity, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}}
		claim, err := db.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "fixture", ProcessStartIdentity: fmt.Sprint(claim.ID), Worktree: path}); err != nil {
			t.Fatal(err)
		}
		drain, err := signer.ProveDrained(contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, drain, ticket.Version, fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.TransitionProviderExhausted(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
		t.Fatal(err)
	}
	paused, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SealRuntimeControl(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionProviderRetry(ctx, store.Transition{Ref: ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_retry", Fence: fence}); err != nil {
		t.Fatal(err)
	}
	leader, err = db.AcquireLeader(ctx, ref.Channel, "retry-activation-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("signed restart changed=%d err=%v", changed, err)
	}
	ticket, err = db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return db, ticket, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, runner
}
