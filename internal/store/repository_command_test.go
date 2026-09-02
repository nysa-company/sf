package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

type repositoryRecoveryDrainer struct{ err error }

var repositoryCommandTestPID int64 = 10_000

func (d repositoryRecoveryDrainer) DrainRepositoryCommand(context.Context, contracts.RepositoryCommandLaunch) error {
	return d.err
}
func (d repositoryRecoveryDrainer) DrainRepositoryCommandTree(context.Context, contracts.RepositoryCommandLaunch, []contracts.RepositoryCommandLaunch) error {
	return d.err
}

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

// completeDrainedRepositoryCommand models the supervisor ordering: launch is
// recorded, the exact child is drained, then the Store receives its observed
// result.  Completion intentionally has no no-launch shortcut in production.
func completeDrainedRepositoryCommand(t *testing.T, db *Store, ctx context.Context, lease contracts.RepositoryCommandLease, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) {
	t.Helper()
	pid := int(atomic.AddInt64(&repositoryCommandTestPID, 1))
	launch := contracts.RepositoryCommandLaunch{PID: pid, PGID: pid, BootIdentity: "boot", ProcessStartIdentity: "command-result"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if result.ObservedAt.IsZero() {
		result.ObservedAt = time.Now().UTC()
	}
	result.Observed = true
	if err := db.CompleteRepositoryCommand(ctx, claim, result); err != nil {
		t.Fatal(err)
	}
}

// completeEvidenceRepositoryCommand issues the exact canonical command claim
// a future workflow runtime will issue. Tests use it to exercise Store-side
// authentication without giving tests (or providers) a weaker authority path.
func completeEvidenceRepositoryCommand(t *testing.T, db *Store, ctx context.Context, purpose string, ref domain.TicketRef, version uint64, fence domain.Fence, provider ProviderAttemptResultKey, intent, proof, checkpoint, policyDigest string, exit int) contracts.RepositoryCommandResultKey {
	t.Helper()
	project, err := db.Project(ctx, ref.Channel, ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := frozenVerifyArgv(ticket.ConfigSnapshot, ticket.ConfigDigest)
	if err != nil {
		t.Fatal(err)
	}
	commandDigest, err := exactRepositoryCommandDigest(argv)
	if err != nil {
		t.Fatal(err)
	}
	if policyDigest == "" {
		policyDigest = repositoryCommandDigest("1")
	}
	provisional := RepositoryCommandResult{Claim: contracts.RepositoryCommandClaim{
		TicketRef: ref, TicketVersion: version, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch,
		Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), Branch: worktree.Branch, BaseRef: project.BaseRef, BaseSHA: worktree.BaseSHA,
		CommandDigest: commandDigest, SpecDigest: repositoryCommandDigest("2"), PolicyDigest: policyDigest, ExecutablePath: "/usr/bin/true", ExecutableDigest: repositoryCommandDigest("3"),
	}}
	if purpose == RepositoryCommandPurposePrebuildVerification {
		checkpoint = ""
	}
	request := commandEvidenceRequest(purpose, ref, version, fence, provider, intent, proof, checkpoint, commandDigest, provisional)
	_, requestDigest, err := CanonicalRepositoryCommandEvidenceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := RepositoryCommandEvidenceSemanticKey(request)
	if err != nil {
		t.Fatal(err)
	}
	command := RepositoryCommandIntent{EffectFence: EffectFence{SemanticKey: semantic, Ref: ref, TicketVersion: version, Fence: fence}, RequestDigest: requestDigest,
		Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), Branch: worktree.Branch, BaseRef: project.BaseRef, BaseSHA: worktree.BaseSHA,
		CommandDigest: commandDigest, SpecDigest: provisional.Claim.SpecDigest, PolicyDigest: policyDigest, ExecutablePath: provisional.Claim.ExecutablePath, ExecutableDigest: provisional.Claim.ExecutableDigest}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: semantic, Ref: ref, Kind: "repository_command", TicketVersion: version, Fence: fence, RequestDigest: requestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueRepositoryCommandClaim(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	completeDrainedRepositoryCommand(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: exit, Duration: time.Millisecond})
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	return contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}
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
	completeDrainedRepositoryCommand(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 0})
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
	completeDrainedRepositoryCommand(t, db, ctx, secondLease, secondClaim, contracts.CommandResult{ExitCode: 1})
	if err := secondLease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryCommandIssueResponseLossReturnsExactClaim(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "issue-replay")
	first, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil || second != first {
		t.Fatalf("replayed claim=%+v first=%+v err=%v", second, first, err)
	}
	if _, err := db.AcquireRepositoryCommand(ctx, second); err != nil {
		t.Fatalf("lost issue response could not acquire exact claim: %v", err)
	}
}

func TestRecoverUnleasedRepositoryCommandRetiresGateClosedClaimForRetry(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "unleased-restart")
	if _, err := db.IssueRepositoryCommandClaim(ctx, intent); err != nil {
		t.Fatal(err)
	}
	if err := db.RecoverUnleasedRepositoryCommands(ctx, intent.Ref.Channel, intent.Fence.LeaderEpoch); err != nil {
		t.Fatal(err)
	}
	if err := db.RecoverUnleasedRepositoryCommands(ctx, intent.Ref.Channel, intent.Fence.LeaderEpoch); err != nil {
		t.Fatalf("repeat recovery was not idempotent: %v", err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "repository_command", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatalf("fresh issue after gate-closed recovery: %v", err)
	}
	if _, err := db.AcquireRepositoryCommand(ctx, claim); err != nil {
		t.Fatalf("fresh acquire after gate-closed recovery: %v", err)
	}
}

func TestRecoverRepositoryCommandQuarantineDoesNotAbortAndLaterProofClears(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "quarantine-retry")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	launch := contracts.RepositoryCommandLaunch{PID: 101, PGID: 101, BootIdentity: "boot", ProcessStartIdentity: "start"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if err := db.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, claim.LeaderEpoch, repositoryRecoveryDrainer{err: errors.New("ambiguous")}); err != nil {
		t.Fatalf("ambiguous recovery must retain quarantine without aborting startup: %v", err)
	}
	active, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel)
	if err != nil || len(active) != 1 || active[0].State != "quarantined" {
		t.Fatalf("quarantine=%+v err=%v", active, err)
	}
	if _, err := db.AcquireRepositoryCommand(ctx, claim); err == nil {
		t.Fatal("quarantined repository lease admitted another writer")
	}
	newLeader, err := db.AcquireLeader(ctx, claim.TicketRef.Channel, "repository-command-restart")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileEffects(ctx, claim.TicketRef.Channel, newLeader); err != nil {
		t.Fatal(err)
	}
	if err := db.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, newLeader, repositoryRecoveryDrainer{}); err != nil {
		t.Fatalf("later exact proof did not clear quarantine: %v", err)
	}
	active, err = db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel)
	if err != nil || len(active) != 0 {
		t.Fatalf("stale lease after proof=%+v err=%v", active, err)
	}
	effect, err := db.Effect(ctx, claim.SemanticKey)
	if err != nil || effect.State != EffectFailed {
		t.Fatalf("recovered effect=%+v err=%v", effect, err)
	}
}

func TestRecoverRepositoryCommandPreservesTerminalResultBeforeRelease(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "terminal-restart")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	completeDrainedRepositoryCommand(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 0})
	if err := db.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, claim.LeaderEpoch, repositoryRecoveryDrainer{}); err != nil {
		t.Fatal(err)
	}
	effect, err := db.Effect(ctx, claim.SemanticKey)
	if err != nil || effect.State != EffectConfirmed {
		t.Fatalf("terminal result overwritten effect=%+v err=%v", effect, err)
	}
	if active, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel); err != nil || len(active) != 0 {
		t.Fatalf("terminal lease not retired=%+v err=%v", active, err)
	}
}

func TestRecoverRepositoryCommandKeepsUnrecordedAndDrainedQuarantineProofs(t *testing.T) {
	for _, state := range []string{"unrecorded", "drained"} {
		t.Run(state, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := repositoryCommandIntentFixture(t, db, ctx, "quarantine-"+state)
			claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := db.AcquireRepositoryCommand(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			if state == "drained" {
				launch := contracts.RepositoryCommandLaunch{PID: 777, PGID: 777, BootIdentity: "boot", ProcessStartIdentity: "start"}
				if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
					t.Fatal(err)
				}
				if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
					t.Fatal(err)
				}
			}
			if err := lease.Quarantine(); err != nil {
				t.Fatal(err)
			}
			active, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel)
			if err != nil || len(active) != 1 || active[0].State != "quarantined" || active[0].LaunchState != state {
				t.Fatalf("quarantine state=%+v err=%v", active, err)
			}
			if err := db.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, claim.LeaderEpoch, nil); err != nil {
				t.Fatal(err)
			}
			if active, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel); err != nil || len(active) != 0 {
				t.Fatalf("proof row not cleared=%+v err=%v", active, err)
			}
		})
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
	control, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: claim.TicketRef, ExpectedVersion: claim.TicketVersion, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	result := contracts.CommandResult{ExitCode: 0, Observed: true, Stdout: []byte("observed after cancellation")}
	if err := db.CompleteRepositoryCommand(ctx, claim, result); err == nil {
		t.Fatal("stale claim was accepted as completion")
	}
	if err := db.ReconcileStaleRepositoryCommandObservation(ctx, claim, result); err != nil {
		t.Fatal(err)
	}
	var resultRows int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_results WHERE semantic_key=?`, claim.SemanticKey).Scan(&resultRows); err != nil || resultRows != 0 {
		t.Fatalf("stale observation minted result rows=%d err=%v", resultRows, err)
	}
	var state string
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM effects WHERE semantic_key=?`, claim.SemanticKey).Scan(&state); err != nil || state != string(EffectFailed) {
		t.Fatalf("effect state=%q err=%v", state, err)
	}
	// No child was launched in this Store race fixture, so the exact stale
	// claim can release its unopened lease before control completion.
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	stopping, err := db.Ticket(ctx, claim.TicketRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteControlTransition(ctx, Transition{Ref: claim.TicketRef, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatalf("control stayed blocked after stale result reconciliation: %v", err)
	}
}

func TestRepositoryCommandLeaseBlocksControlCompletionAfterTerminalResult(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "control-lease")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	completeDrainedRepositoryCommand(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 0})
	control, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: claim.TicketRef, ExpectedVersion: claim.TicketVersion, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := db.Ticket(ctx, claim.TicketRef)
	if err != nil {
		t.Fatal(err)
	}
	transition := Transition{Ref: claim.TicketRef, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}"}
	if _, err := db.CompleteControlTransition(ctx, transition); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("terminal result bypassed live repository lease: %v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteControlTransition(ctx, transition); err != nil {
		t.Fatalf("control did not finish after exact lease release: %v", err)
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
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 0, Observed: true, ObservedAt: time.Now().UTC()}); err != nil {
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

func TestRepositoryCommandRejectsMoreThanBoundedTrackedGroups(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "group-limit")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.RecordRepositoryCommandLaunch(ctx, contracts.RepositoryCommandLaunch{PID: 1, PGID: 1, BootIdentity: "boot", ProcessStartIdentity: "primary"}); err != nil {
		t.Fatal(err)
	}
	recorder := lease.(contracts.RepositoryCommandGroupRecorder)
	for i := 0; i < repositoryCommandProcessGroupLimit; i++ {
		v := contracts.RepositoryCommandLaunch{PID: 1000 + i, PGID: 1000 + i, BootIdentity: "boot", ProcessStartIdentity: "group-" + string(rune('a'+i))}
		if err := recorder.RecordRepositoryCommandProcessGroup(ctx, v); err != nil {
			t.Fatalf("record group %d: %v", i, err)
		}
	}
	if err := recorder.RecordRepositoryCommandProcessGroup(ctx, contracts.RepositoryCommandLaunch{PID: 2000, PGID: 2000, BootIdentity: "boot", ProcessStartIdentity: "group-over"}); !errors.Is(err, ErrRepositoryCommandLease) {
		t.Fatalf("unbounded group accepted: %v", err)
	}
	if active, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel); err != nil || len(active) != 1 || len(active[0].Groups) != repositoryCommandProcessGroupLimit {
		t.Fatalf("bounded recovery groups=%+v err=%v", active, err)
	}
}

func TestRepositoryCommandPrimaryLaunchOverflowQuarantinesDuringRecovery(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "primary-overflow")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	overflow := contracts.RepositoryCommandLaunch{PID: 80, PGID: 80, BootIdentity: strings.Repeat("x", repositoryCommandProcessGroupByteLimit), ProcessStartIdentity: "start"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, overflow); !errors.Is(err, ErrRepositoryCommandLease) {
		t.Fatalf("oversized primary launch persisted: %v", err)
	}
	launch := contracts.RepositoryCommandLaunch{PID: 81, PGID: 81, BootIdentity: "boot", ProcessStartIdentity: "start"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE repository_command_leases SET process_boot_identity=? WHERE repository_path=?`, strings.Repeat("x", repositoryCommandProcessGroupByteLimit+1), claim.Repository); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ActiveRepositoryCommandLeases(ctx, claim.TicketRef.Channel); !errors.Is(err, ErrRepositoryCommandLease) {
		t.Fatalf("oversized primary launch loaded: %v", err)
	}
	if err := db.RecoverRepositoryCommandLeases(ctx, claim.TicketRef.Channel, claim.LeaderEpoch, repositoryRecoveryDrainer{}); err != nil {
		t.Fatal(err)
	}
	var state, launchState string
	if err := db.db.QueryRowContext(ctx, `SELECT state,launch_state FROM repository_command_leases WHERE repository_path=?`, claim.Repository).Scan(&state, &launchState); err != nil {
		t.Fatal(err)
	}
	if state != "quarantined" || launchState != "quarantined" {
		t.Fatalf("overflow lease state=%q launch_state=%q", state, launchState)
	}
}

func TestRepositoryCommandRejectsOversizedTrackedGroupBeforePersistence(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "tracked-bytes")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.RecordRepositoryCommandLaunch(ctx, contracts.RepositoryCommandLaunch{PID: 91, PGID: 91, BootIdentity: "boot", ProcessStartIdentity: "start"}); err != nil {
		t.Fatal(err)
	}
	recorder := lease.(contracts.RepositoryCommandGroupRecorder)
	overflow := contracts.RepositoryCommandLaunch{PID: 92, PGID: 92, BootIdentity: strings.Repeat("x", repositoryCommandProcessGroupByteLimit), ProcessStartIdentity: "start"}
	if err := recorder.RecordRepositoryCommandProcessGroup(ctx, overflow); !errors.Is(err, ErrRepositoryCommandLease) {
		t.Fatalf("oversized tracked group persisted: %v", err)
	}
}

func TestRepositoryExecutableDigestDomainValidation(t *testing.T) {
	plain := "sha256:" + strings.Repeat("a", 64)
	node := "node22-closure-v1:sha256:" + strings.Repeat("b", 64)
	nysa := "nysa-api-pure-v1:sha256:" + strings.Repeat("c", 64)
	if !validRepositoryExecutableDigest("/usr/local/go/bin/go", plain) {
		t.Fatal("ordinary Go executable digest rejected")
	}
	if !validRepositoryExecutableDigest("/opt/homebrew/Cellar/node/22/bin/node", node) {
		t.Fatal("Node closure identity rejected")
	}
	if !validRepositoryExecutableDigest("/opt/homebrew/Cellar/node/22/bin/node", nysa) {
		t.Fatal("Nysa pure Node runtime identity rejected")
	}
	if validRepositoryExecutableDigest("/opt/homebrew/Cellar/node/22/bin/node", plain) {
		t.Fatal("plain digest accepted for Node")
	}
	if validRepositoryExecutableDigest("/usr/local/go/bin/go", node) {
		t.Fatal("Node closure identity accepted for Go")
	}
	if validRepositoryExecutableDigest("/usr/local/go/bin/go", nysa) {
		t.Fatal("Nysa runtime identity accepted for Go")
	}
}

func TestNodeClosureDigestIssuesAcquiresAndCompletesRepositoryCommand(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "node-closure")
	intent.ExecutablePath = "/opt/homebrew/Cellar/node@22/22.23.2/bin/node"
	intent.ExecutableDigest = "node22-closure-v1:sha256:" + strings.Repeat("c", 64)
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	completeDrainedRepositoryCommand(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 0})
	result, err := db.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch})
	if err != nil || result.Claim.ExecutableDigest != intent.ExecutableDigest {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
