package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

func repositoryCommandDigest(ch string) string { return "sha256:" + strings.Repeat(ch, 64) }

func repositoryCommandIdentity(t *testing.T, repository, worktree, branch, base string) string {
	t.Helper()
	identity := gitboundary.Identity{
		Repository: repository, RepositoryDev: 1, RepositoryIno: 2,
		Worktree: worktree, WorktreeDev: 3, WorktreeIno: 4,
		GitFile: "gitdir: " + worktree + "/.git", GitFileDev: 5, GitFileIno: 6,
		CommonDir: repository + "/.git", CommonDirDev: 7, CommonDirIno: 8,
		Origin: "git@example.test:nysa.git", PushOrigin: "/tmp/nysa-origin", PushOriginDev: 9, PushOriginIno: 10,
		BaseRef: base, BaseHead: strings.Repeat("a", 40), HeadRef: branch,
		ConfigHash: strings.Repeat("b", 64), HooksHash: strings.Repeat("c", 64),
	}
	b, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func repositoryCommandIntentFixture(t *testing.T, db *Store, ctx context.Context, key string) RepositoryCommandIntent {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID("SF-command-" + key)}
	if err := db.CreateTicket(ctx, ticket(ref, "source-"+key)); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repository-command-test")
	if err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	branch := "sf/dev/aaaaaaaa/" + strings.ToLower(key)
	started, err := db.StartOrAdopt(ctx, ref, current.Version, branch, domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	repository, worktree, base := "/tmp/nysa", "/tmp/nysa/worktree", "main"
	identity := repositoryCommandIdentity(t, repository, worktree, branch, base)
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Path: worktree, Branch: branch, IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	intent := RepositoryCommandIntent{EffectFence: EffectFence{SemanticKey: "repository-command/" + key, Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}, RequestDigest: repositoryCommandDigest("d"), Repository: repository, Worktree: worktree, WorktreeIdentity: identity, Branch: branch, BaseRef: base, BaseSHA: strings.Repeat("a", 40), CommandDigest: repositoryCommandDigest("e"), SpecDigest: repositoryCommandDigest("f"), PolicyDigest: repositoryCommandDigest("1"), ExecutablePath: "/usr/bin/true", ExecutableDigest: repositoryCommandDigest("2")}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "repository_command", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestRepositoryCommandCompleteReleaseThenNextAcquire(t *testing.T) {
	db, ctx := openTestStore(t)
	first := repositoryCommandIntentFixture(t, db, ctx, "first")
	claim, err := db.IssueRepositoryCommandClaim(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 0, Observed: true}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	var residue int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE repository_path=?`, first.Repository).Scan(&residue); err != nil || residue != 0 {
		t.Fatalf("lease residue=%d err=%v", residue, err)
	}
	// A distinct subsequent semantic effect for the same repository must be
	// admitted after the completed effect releases its exact nonce.
	second := first
	second.SemanticKey += "/second"
	second.RequestDigest = repositoryCommandDigest("3")
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: second.SemanticKey, Ref: second.Ref, Kind: "repository_command", TicketVersion: second.TicketVersion, Fence: second.Fence, RequestDigest: second.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	secondClaim, err := db.IssueRepositoryCommandClaim(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	secondLease, err := db.AcquireRepositoryCommand(ctx, secondClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, secondClaim, contracts.CommandResult{ExitCode: 1, Observed: true}); err != nil {
		t.Fatal(err)
	}
	if err := secondLease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryCommandRejectsOpaqueWorktreeIdentity(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "opaque")
	intent.WorktreeIdentity = `{"repository":"/tmp/nysa"}`
	if _, err := db.IssueRepositoryCommandClaim(ctx, intent); err == nil {
		t.Fatal("opaque worktree identity was accepted")
	}
}

func TestRepositoryCommandRefusesUnobservedCompletion(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "unobserved")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireRepositoryCommand(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 0}); err == nil {
		t.Fatal("zero-value command result was accepted as success")
	}
}

func TestRepositoryCommandStaleObservedResultRetiresExactExecutingEffect(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "stale-observation")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.InvalidateRunner(ctx, claim.TicketRef, claim.TicketVersion, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	result := contracts.CommandResult{ExitCode: 0, Observed: true, Stdout: []byte("observed after cancellation")}
	if err := db.CompleteRepositoryCommand(ctx, claim, result); err == nil {
		t.Fatal("stale claim was accepted as completion")
	}
	if err := db.ReconcileStaleRepositoryCommandObservation(ctx, claim, result); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM effects WHERE semantic_key=?`, claim.SemanticKey).Scan(&state); err != nil || state != string(EffectFailed) {
		t.Fatalf("effect state=%q err=%v", state, err)
	}
	if err := lease.Quarantine(); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryCommandPersistsTrackedGoTestGroups(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "tracked-groups")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	primary := contracts.RepositoryCommandLaunch{PID: 101, PGID: 101, BootIdentity: "boot", ProcessStartIdentity: "start-primary"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, primary); err != nil {
		t.Fatal(err)
	}
	recorder, ok := lease.(contracts.RepositoryCommandGroupRecorder)
	if !ok {
		t.Fatal("production repository lease does not record test groups")
	}
	group := contracts.RepositoryCommandLaunch{PID: 202, PGID: 202, BootIdentity: "boot", ProcessStartIdentity: "start-test"}
	if err := recorder.RecordRepositoryCommandProcessGroup(ctx, group); err != nil {
		t.Fatal(err)
	}
	active, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel)
	if err != nil || len(active) != 1 || len(active[0].Groups) != 1 || active[0].Groups[0] != group {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 0, Observed: true}); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	var residue int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_process_groups WHERE repository_path=?`, claim.Repository).Scan(&residue); err != nil || residue != 0 {
		t.Fatalf("tracked group residue=%d err=%v", residue, err)
	}
}
