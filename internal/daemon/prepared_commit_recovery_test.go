package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

func daemonRecoveryWorktreeIdentity(repository, worktree, branch, baseRef, baseSHA string) []byte {
	identity, _ := json.Marshal(git.Identity{
		Repository: repository, RepositoryDev: 1, RepositoryIno: 2,
		Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4,
		GitFile: "gitdir: " + filepath.Join(worktree, ".git"), GitFileDev: 5, GitFileIno: 6,
		CommonDir: filepath.Join(repository, ".git"), CommonDirDev: 7, CommonDirIno: 8,
		Origin: "git@example.test:nysa.git", PushOrigin: "git@example.test:nysa.git",
		BaseRef: baseRef, BaseHead: baseSHA, HeadRef: branch,
		ConfigHash: strings.Repeat("b", 64), HooksHash: strings.Repeat("c", 64),
	})
	return identity
}

type preparedCommitObserverFunc func(context.Context, contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error)

func (f preparedCommitObserverFunc) ObservePreparedCommit(ctx context.Context, claim contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error) {
	return f(ctx, claim)
}

// seedPreparedCommit leaves the durable state at the exact crash window: the
// immutable intent has its commit/tree tuple, the lease is gone, and the
// effect is still executing under the old runner. Start's recovery pass then
// supplies the new leader/claim fence before asking the observer anything.
func seedPreparedCommit(t *testing.T, daemon *Daemon, ticketID domain.TicketID) (store.GitMutationIntent, contracts.GitMutationClaim, string, string) {
	t.Helper()
	ctx := context.Background()
	ref := domain.TicketRef{Channel: daemon.channel, Project: "demo", Ticket: ticketID}
	if err := daemon.store.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "prepared-" + string(ticketID), Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	current, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := daemon.store.StartOrAdopt(ctx, ref, current.Version, "prepared/"+string(ticketID), domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: current.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	project, err := daemon.store.Project(ctx, daemon.channel, "demo")
	if err != nil {
		t.Fatal(err)
	}
	base := strings.Repeat("a", 40)
	commit := strings.Repeat("b", 40)
	tree := strings.Repeat("c", 40)
	path := filepath.Join(daemon.paths.Worktrees, "demo", string(ticketID))
	branch := fmt.Sprintf("sf/%s/aaaaaaaa/aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", daemon.channel)
	identity := daemonRecoveryWorktreeIdentity(project.Path, path, branch, project.BaseRef, base)
	if err := daemon.store.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: started.RunnerEpoch}, Path: path, Branch: branch, IdentityJSON: identity, BaseSHA: base, HeadSHA: base}); err != nil {
		t.Fatal(err)
	}
	intent := store.GitMutationIntent{
		EffectFence:     store.EffectFence{Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: started.RunnerEpoch}},
		RequestDigest:   "sha256:" + strings.Repeat("d", 64),
		Repository:      project.Path,
		Worktree:        path,
		Branch:          branch,
		Operation:       "commit",
		BaseRef:         project.BaseRef,
		ExpectedBaseOID: base,
		ExpectedHeadOID: base,
	}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if _, err := daemon.store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "git/commit", TicketVersion: started.Version, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := daemon.store.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := daemon.store.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	prepared, ok := lease.(contracts.GitMutationRecoveryFactsLease)
	if !ok {
		t.Fatal("Git lease does not expose prepared-commit recording")
	}
	if err := prepared.RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	return intent, claim, commit, tree
}

func closeDaemonForPreparedRecovery(t *testing.T, daemon *Daemon, cancel context.CancelFunc) {
	t.Helper()
	cancel()
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonReconcilesPreparedCommitBeforeRunnerFenceAndSecondRestartIsIdempotent(t *testing.T) {
	daemon, paths, cancel := testDaemon(t)
	intent, claim, commit, tree := seedPreparedCommit(t, daemon, "SF-prepared-daemon-order")
	closeDaemonForPreparedRecovery(t, daemon, cancel)

	// ConfirmRecoveredPreparedCommit's final CAS requires the old ticket
	// version/runner, so a successful startup proves this callback completed
	// between ReconcileEffects and FenceRecoveredRunners. The ticket assertion
	// below then proves the later runner fence occurred exactly once.
	calls := 0
	observer := preparedCommitObserverFunc(func(_ context.Context, got contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error) {
		calls++
		if got != claim {
			t.Fatalf("observer claim=%+v want immutable claim=%+v", got, claim)
		}
		return contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}, nil
	})
	restarted, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "prepared-daemon-restart", PreparedCommitObserver: observer})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("prepared observer calls=%d want one", calls)
	}
	effect, err := restarted.store.Effect(context.Background(), intent.SemanticKey)
	if err != nil || effect.State != store.EffectConfirmed || effect.ObservedIdentity != commit {
		t.Fatalf("recovered effect=%+v err=%v", effect, err)
	}
	fencedTicket, err := restarted.store.Ticket(context.Background(), claim.TicketRef)
	if err != nil {
		t.Fatal(err)
	}
	if fencedTicket.Version != claim.TicketVersion+1 || fencedTicket.RunnerEpoch != claim.RunnerEpoch+1 {
		t.Fatalf("runner fence=%+v claim=%+v", fencedTicket, claim)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "prepared-daemon-second-restart", PreparedCommitObserver: observer})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if calls != 1 {
		t.Fatalf("confirmed effect was observed again on second restart: calls=%d", calls)
	}
	secondEffect, err := second.store.Effect(context.Background(), intent.SemanticKey)
	if err != nil || secondEffect.State != store.EffectConfirmed || secondEffect.ObservedIdentity != commit {
		t.Fatalf("second restart effect=%+v err=%v", secondEffect, err)
	}
}

func TestDaemonPreparedCommitMismatchPreventsSocketExposure(t *testing.T) {
	daemon, paths, cancel := testDaemon(t)
	_, claim, commit, tree := seedPreparedCommit(t, daemon, "SF-prepared-daemon-mismatch")
	closeDaemonForPreparedRecovery(t, daemon, cancel)
	observer := preparedCommitObserverFunc(func(context.Context, contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error) {
		return contracts.PreparedCommitObservation{CommitOID: strings.Repeat("e", len(commit)), ParentOID: claim.ExpectedHeadOID, TreeOID: tree}, nil
	})
	_, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "prepared-daemon-mismatch", PreparedCommitObserver: observer})
	if !errors.Is(err, store.ErrPreparedCommitRecovery) {
		t.Fatalf("startup error=%v, want prepared recovery refusal", err)
	}
	if _, statErr := os.Lstat(paths.Socket); !os.IsNotExist(statErr) {
		t.Fatalf("socket was exposed after prepared mismatch: %v", statErr)
	}
}

func TestDaemonMissingPreparedCommitObserverPreventsSocketExposure(t *testing.T) {
	daemon, paths, cancel := testDaemon(t)
	seedPreparedCommit(t, daemon, "SF-prepared-daemon-missing")
	closeDaemonForPreparedRecovery(t, daemon, cancel)
	_, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "prepared-daemon-missing"})
	if !errors.Is(err, store.ErrPreparedCommitRecovery) {
		t.Fatalf("startup error=%v, want prepared recovery refusal", err)
	}
	if _, statErr := os.Lstat(paths.Socket); !os.IsNotExist(statErr) {
		t.Fatalf("socket was exposed without prepared observer: %v", statErr)
	}
}
