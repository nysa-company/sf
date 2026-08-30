package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type gitDrainerFunc func(context.Context, contracts.GitMutationLaunch) error

func (f gitDrainerFunc) DrainGitMutation(ctx context.Context, launch contracts.GitMutationLaunch) error {
	return f(ctx, launch)
}

func gitDigest(ch string) string { return "sha256:" + strings.Repeat(ch, 64) }

func gitIntentFixture(t *testing.T, db *Store, ctx context.Context, ticketID string) GitMutationIntent {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID(ticketID)}
	if err := db.CreateTicket(ctx, ticket(ref, "source-"+ticketID)); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "git-authority")
	if err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, current.Version, "dev/nysa/"+ticketID+"/git", domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return GitMutationIntent{EffectFence: EffectFence{SemanticKey: "git/" + ticketID + "/commit", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}, RequestDigest: gitDigest("a"), Repository: "/tmp/nysa", Worktree: "/tmp/nysa/" + ticketID, Branch: "sf/dev/aaaaaaaa/aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Operation: "commit", BaseRef: "main", ExpectedBaseOID: strings.Repeat("a", 40), ExpectedHeadOID: strings.Repeat("a", 40)}
}

func TestGitMutationClaimIsIssuedOnlyFromExactDurableIntent(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-intent")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if claim.ClaimEpoch == 0 || claim.SemanticKey != intent.SemanticKey {
		t.Fatalf("claim=%+v", claim)
	}
	tampered := claim
	tampered.ExpectedHeadOID = strings.Repeat("b", 40)
	if _, err := db.AcquireGitMutation(ctx, tampered); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("tampered acquisition=%v", err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IssueGitMutationClaim(ctx, intent); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("reissued executing effect=%v", err)
	}
}

func TestGitMutationLeaseExcludesRepositoryWriterAndSurvivesFenceChange(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-lease")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireRepositoryWriter(ctx, RepositoryWriterIntent{EffectFence: intent.EffectFence, RequestDigest: intent.RequestDigest, Repository: intent.Repository, Worktree: intent.Worktree, Operation: "repo-check"}); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("writer admitted beside git=%v", err)
	}
	if _, err := db.ReconcileEffects(ctx, domain.ChannelDev, intent.Fence.LeaderEpoch); err != nil {
		t.Fatal(err)
	}
	if err := lease.Check(ctx); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("fenced lease check=%v", err)
	}
	if err := lease.Release(); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("stale release must retain crash lease=%v", err)
	}
	var retained int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, intent.Repository).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("retained=%d err=%v", retained, err)
	}
}

func TestRepositoryWriterRequiresExecutingEffectAndExcludesGit(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-repo-writer")
	if _, err := db.AcquireRepositoryWriter(ctx, RepositoryWriterIntent{EffectFence: intent.EffectFence, RequestDigest: intent.RequestDigest, Repository: intent.Repository, Worktree: intent.Worktree, Operation: "repo-check"}); !errors.Is(err, ErrRepositoryWriter) {
		t.Fatalf("unissued writer=%v", err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := db.AcquireRepositoryWriter(ctx, RepositoryWriterIntent{EffectFence: intent.EffectFence, RequestDigest: intent.RequestDigest, Repository: intent.Repository, Worktree: intent.Worktree, Operation: "repo-check"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireGitMutation(ctx, claim); !errors.Is(err, ErrRepositoryWriter) {
		t.Fatalf("git admitted beside writer=%v", err)
	}
	if err := writer.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestGitMutationRecoveryRequiresExactRecordedDrain(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-recovery")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecoverGitMutationLeases(ctx, domain.ChannelDev, intent.Fence.LeaderEpoch, nil); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("unrecorded recovery=%v", err)
	}
	if _, err := db.AcquireGitMutation(ctx, claim); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("quarantined repository admitted=%v", err)
	}

	// A separate authority with a recorded identity can clear only after
	// the injected OS drainer accepts that exact tuple.
	db2, ctx2 := openTestStore(t)
	intent2 := gitIntentFixture(t, db2, ctx2, "SF-git-recovery-2")
	intent2.Repository = "/tmp/nysa-2"
	claim2, err := db2.IssueGitMutationClaim(ctx2, intent2)
	if err != nil {
		t.Fatal(err)
	}
	lease2, err := db2.AcquireGitMutation(ctx2, claim2)
	if err != nil {
		t.Fatal(err)
	}
	recorded, ok := lease2.(contracts.GitMutationLaunchLease)
	if !ok {
		t.Fatal("store lease does not record launches")
	}
	launch := contracts.GitMutationLaunch{PID: 42, PGID: 42, BootIdentity: "boot-a", ProcessStartIdentity: "start-a"}
	if err := recorded.RecordGitMutationLaunch(ctx2, launch); err != nil {
		t.Fatal(err)
	}
	if err := db2.RecoverGitMutationLeases(ctx2, domain.ChannelDev, intent2.Fence.LeaderEpoch, gitDrainerFunc(func(_ context.Context, got contracts.GitMutationLaunch) error {
		if got != launch {
			t.Fatalf("launch=%+v", got)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db2.db.QueryRowContext(ctx2, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, intent2.Repository).Scan(&n); err != nil || n != 0 {
		t.Fatalf("drained lease remains=%d err=%v", n, err)
	}
	_ = lease
}

var _ contracts.GitMutationAuthority = (*Store)(nil)
