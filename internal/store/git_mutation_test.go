package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type gitDrainerFunc func(context.Context, contracts.GitMutationLaunch) error

func (f gitDrainerFunc) DrainGitMutation(ctx context.Context, launch contracts.GitMutationLaunch) error {
	return f(ctx, launch)
}

func gitDigest(ch string) string { return "sha256:" + strings.Repeat(ch, 64) }

func unplannedGitIntentFixture(t *testing.T, db *Store, ctx context.Context, ticketID string) GitMutationIntent {
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
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}, RequestDigest: gitDigest("a"), Repository: "/tmp/nysa", Worktree: "/tmp/nysa/" + ticketID, Branch: "sf/dev/aaaaaaaa/aaaaaaaa-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Operation: "commit", BaseRef: "main", ExpectedBaseOID: strings.Repeat("a", 40), ExpectedHeadOID: strings.Repeat("a", 40)}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: intent.Fence, Path: intent.Worktree, Branch: intent.Branch, IdentityJSON: []byte(`{"repository":"/tmp/nysa"}`), BaseSHA: intent.ExpectedBaseOID, HeadSHA: intent.ExpectedHeadOID}); err != nil {
		t.Fatal(err)
	}
	return intent
}

func gitIntentFixture(t *testing.T, db *Store, ctx context.Context, ticketID string) GitMutationIntent {
	t.Helper()
	intent := unplannedGitIntentFixture(t, db, ctx, ticketID)
	if _, err := db.PlanEffect(ctx, EffectPlan{
		SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/" + intent.Operation,
		TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest,
	}); err != nil {
		t.Fatal(err)
	}
	return intent
}

func createWorktreeGitIntentFixture(t *testing.T, db *Store, ctx context.Context, ticketID string) GitMutationIntent {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID(ticketID)}
	if err := db.CreateTicket(ctx, ticket(ref, "source-"+ticketID)); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "git-create-authority")
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
	branch := "sf/dev/" + branchDigestPart("nysa") + "/" + branchDigestPart(ticketID) + "-" + strings.Repeat("b", 32)
	key := "dev\x00nysa\x00" + ticketID
	if stored, err := db.LoadOrStoreBranch(ctx, key, branch); err != nil || stored != branch {
		t.Fatalf("allocate branch=%q err=%v", stored, err)
	}
	worktree, err := db.TicketWorktreePath(ref)
	if err != nil {
		t.Fatal(err)
	}
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}, RequestDigest: gitDigest("c"), Repository: "/tmp/nysa", Worktree: worktree, Branch: branch, Operation: "create-worktree", BaseRef: "main", ExpectedBaseOID: strings.Repeat("a", 40), ExpectedHeadOID: strings.Repeat("a", 40)}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "git/create-worktree", TicketVersion: started.Version, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	return intent
}

func TestGitMutationClaimBootstrapsWorktreeFromDurableBranchAllocation(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := createWorktreeGitIntentFixture(t, db, ctx, "SF-git-create")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Operation != "create-worktree" || claim.Repository != intent.Repository || claim.Worktree != intent.Worktree || claim.Branch != intent.Branch {
		t.Fatalf("claim=%+v", claim)
	}
	var registered int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM worktrees WHERE channel=? AND project_id=? AND ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).Scan(&registered); err != nil || registered != 0 {
		t.Fatalf("bootstrap unexpectedly required worktree row: count=%d err=%v", registered, err)
	}
}

func TestWorktreeCreationIntentRecoversExecutingAndConfirmedIdentity(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := createWorktreeGitIntentFixture(t, db, ctx, "SF-git-create-recovery")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := db.WorktreeCreationIntent(ctx, intent.Ref)
	if err != nil || facts.Claim != claim || facts.ObservedIdentity != "" {
		t.Fatalf("executing facts=%+v err=%v", facts, err)
	}
	identity := `{"worktree":"created"}`
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, identity); err != nil {
		t.Fatal(err)
	}
	facts, err = db.WorktreeCreationIntent(ctx, intent.Ref)
	if err != nil || facts.Claim != claim || facts.ObservedIdentity != identity {
		t.Fatalf("confirmed facts=%+v err=%v", facts, err)
	}
}

func TestPublicationPushIntentRecoversOnlyOneAuthenticatedPush(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-push-recovery")
	intent.Operation, intent.ExpectedHeadOID = "push", strings.Repeat("b", 40)
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/push", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := db.PublicationPushIntent(ctx, intent.Ref)
	if err != nil || facts.Claim != claim || facts.Effect.State != EffectExecuting {
		t.Fatalf("executing=%+v err=%v", facts, err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, "push-observed"); err != nil {
		t.Fatal(err)
	}
	if facts, err = db.PublicationPushIntent(ctx, intent.Ref); err != nil || facts.Effect.State != EffectConfirmed {
		t.Fatalf("confirmed=%+v err=%v", facts, err)
	}
	if _, err := db.PublicationPushIntent(ctx, domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-none"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("none=%v", err)
	}
}

func TestPublicationPushIntentFailsClosedForUncertainDuplicateAndMalformedRows(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-push-intent-edges")
	intent.Operation, intent.ExpectedHeadOID = "push", strings.Repeat("b", 40)
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/push", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	if facts, err := db.PublicationPushIntent(ctx, intent.Ref); err != nil || facts.Effect.State != EffectUncertain {
		t.Fatalf("uncertain facts=%+v err=%v", facts, err)
	}

	second := intent
	second.RequestDigest = "sha256:" + strings.Repeat("c", 64)
	second.SemanticKey = CanonicalGitMutationSemanticKey(second)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: second.SemanticKey, Ref: second.Ref, Kind: "git/push", TicketVersion: second.TicketVersion, Fence: second.Fence, RequestDigest: second.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IssueGitMutationClaim(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublicationPushIntent(ctx, intent.Ref); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("duplicate=%v", err)
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM git_mutation_intents WHERE semantic_key=?`, second.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_intents SET expected_head_oid='bad' WHERE semantic_key=?`, intent.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublicationPushIntent(ctx, intent.Ref); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("malformed=%v", err)
	}
}

func TestGitMutationLeaseReleaseIsBoundedWhenSQLiteIsBusy(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-release-busy")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	started := time.Now()
	err = lease.Release()
	if err == nil || time.Since(started) > 3*time.Second {
		t.Fatalf("release err=%v elapsed=%s", err, time.Since(started))
	}
	if _, err := conn.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireGitMutation(ctx, claim); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("failed release admitted another writer: %v", err)
	}
	var retained int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=? AND state='active'`, intent.Repository).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("failed release did not retain active exclusion count=%d err=%v", retained, err)
	}
}

func TestGitMutationClaimRefusesCreateWithUnallocatedIdentity(t *testing.T) {
	db, ctx := openTestStore(t)
	for name, mutate := range map[string]func(*GitMutationIntent){
		"repository": func(intent *GitMutationIntent) { intent.Repository = "/tmp/other" },
		"worktree":   func(intent *GitMutationIntent) { intent.Worktree = "/tmp/other" },
		"branch": func(intent *GitMutationIntent) {
			intent.Branch = "sf/dev/aaaaaaaa/aaaaaaaa-cccccccccccccccccccccccccccccccc"
		},
		"base ref": func(intent *GitMutationIntent) { intent.BaseRef = "trunk" },
	} {
		t.Run(name, func(t *testing.T) {
			intent := createWorktreeGitIntentFixture(t, db, ctx, "SF-git-create-"+strings.ReplaceAll(name, " ", "-"))
			mutate(&intent)
			if _, err := db.IssueGitMutationClaim(ctx, intent); !errors.Is(err, ErrGitMutationIntent) {
				t.Fatalf("unallocated create identity accepted: %v", err)
			}
		})
	}
}

func TestGitMutationClaimRefusesUnknownOperation(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := unplannedGitIntentFixture(t, db, ctx, "SF-git-unknown-operation")
	intent.Operation = "arbitrary-git"
	if _, err := db.PlanEffect(ctx, EffectPlan{
		SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/" + intent.Operation,
		TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IssueGitMutationClaim(ctx, intent); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("unknown Git operation accepted: %v", err)
	}
}

func TestGitMutationClaimRefusesUnplannedEffect(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := unplannedGitIntentFixture(t, db, ctx, "SF-git-unplanned")
	if _, err := db.IssueGitMutationClaim(ctx, intent); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("unplanned claim issuance=%v", err)
	}
	var effects, intents int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE semantic_key=?`, intent.SemanticKey).Scan(&effects); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_intents WHERE semantic_key=?`, intent.SemanticKey).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if effects != 0 || intents != 0 {
		t.Fatalf("unplanned issuance persisted effects=%d intents=%d", effects, intents)
	}
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

func TestCanonicalGitMutationSemanticKeyBindsEveryStableInput(t *testing.T) {
	db, ctx := openTestStore(t)
	base := unplannedGitIntentFixture(t, db, ctx, "SF-git-semantic-key")
	if base.SemanticKey != CanonicalGitMutationSemanticKey(base) {
		t.Fatalf("fixture key is not canonical: %q", base.SemanticKey)
	}
	for name, mutate := range map[string]func(*GitMutationIntent){
		"channel":        func(i *GitMutationIntent) { i.Ref.Channel = domain.ChannelStable },
		"project":        func(i *GitMutationIntent) { i.Ref.Project = "other" },
		"ticket":         func(i *GitMutationIntent) { i.Ref.Ticket = "SF-other" },
		"request digest": func(i *GitMutationIntent) { i.RequestDigest = gitDigest("b") },
		"repository":     func(i *GitMutationIntent) { i.Repository = "/tmp/other" },
		"worktree":       func(i *GitMutationIntent) { i.Worktree = "/tmp/other-worktree" },
		"branch":         func(i *GitMutationIntent) { i.Branch = "sf/dev/aaaaaaaa/aaaaaaaa-cccccccccccccccccccccccccccccccc" },
		"operation":      func(i *GitMutationIntent) { i.Operation = "push" },
		"base ref":       func(i *GitMutationIntent) { i.BaseRef = "trunk" },
		"expected base":  func(i *GitMutationIntent) { i.ExpectedBaseOID = strings.Repeat("b", 40) },
		"expected head":  func(i *GitMutationIntent) { i.ExpectedHeadOID = strings.Repeat("b", 40) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if got := CanonicalGitMutationSemanticKey(changed); got == base.SemanticKey {
				t.Fatalf("%s did not change canonical key", name)
			}
		})
	}
}

func TestGitMutationClaimRefusesArbitrarySemanticKeyBeforeEffectMutation(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := unplannedGitIntentFixture(t, db, ctx, "SF-git-arbitrary-key")
	intent.SemanticKey = "arbitrary-key"
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/" + intent.Operation, TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IssueGitMutationClaim(ctx, intent); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("arbitrary key issued: %v", err)
	}
	var state string
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM effects WHERE semantic_key=?`, intent.SemanticKey).Scan(&state); err != nil || state != string(EffectPlanned) {
		t.Fatalf("effect changed before key refusal: state=%q err=%v", state, err)
	}
	var rows int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_intents WHERE semantic_key=?`, intent.SemanticKey).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("intent persisted for arbitrary key: rows=%d err=%v", rows, err)
	}
}

func TestGitMutationRecoveryFactsAreOneWayAndVisibleToRecovery(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-facts-commit")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	facts := lease.(contracts.GitMutationRecoveryFactsLease)
	commit, tree := strings.Repeat("b", 40), strings.Repeat("c", 40)
	if err := facts.RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := facts.RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if err := facts.RecordPreparedCommit(ctx, strings.Repeat("d", 40), tree); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("conflicting replay=%v", err)
	}
	if err := facts.RecordPreparedCommit(ctx, strings.Repeat("d", 64), strings.Repeat("e", 64)); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("mixed-width commit fact=%v", err)
	}
	recovery, err := db.ActiveGitMutationLeases(ctx, domain.ChannelDev)
	if err != nil || len(recovery) != 1 || recovery[0].PreparedCommitOID != commit || recovery[0].PreparedTreeOID != tree {
		t.Fatalf("recovery facts=%+v err=%v", recovery, err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.GitMutationIntentFacts(ctx, intent.SemanticKey)
	if err != nil || loaded.PreparedCommitOID != commit || loaded.PreparedTreeOID != tree {
		t.Fatalf("released intent facts=%+v err=%v", loaded, err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_intents SET prepared_tree_oid='' WHERE semantic_key=?`, intent.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GitMutationIntentFacts(ctx, intent.SemanticKey); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("partial intent commit fact accepted: %v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_intents SET prepared_tree_oid='not-an-oid' WHERE semantic_key=?`, intent.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GitMutationIntentFacts(ctx, intent.SemanticKey); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("tampered recovery fact accepted: %v", err)
	}
}

func TestGitMutationPushFactsDistinguishUnrecordedAbsentAndPresent(t *testing.T) {
	newPush := func(ticket string) (*Store, context.Context, contracts.GitMutationRecoveryFactsLease) {
		db, ctx := openTestStore(t)
		intent := unplannedGitIntentFixture(t, db, ctx, ticket)
		intent.Operation = "push"
		intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
		if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/push", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
			t.Fatal(err)
		}
		claim, err := db.IssueGitMutationClaim(ctx, intent)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := db.AcquireGitMutation(ctx, claim)
		if err != nil {
			t.Fatal(err)
		}
		return db, ctx, lease.(contracts.GitMutationRecoveryFactsLease)
	}
	db, ctx, absent := newPush("SF-git-facts-absent")
	var observed int
	var oid string
	if err := db.db.QueryRowContext(ctx, `SELECT prior_remote_observed,prior_remote_oid FROM git_mutation_leases`).Scan(&observed, &oid); err != nil || observed != 0 || oid != "" {
		t.Fatalf("unrecorded fact observed=%d oid=%q err=%v", observed, oid, err)
	}
	if err := absent.RecordPushPriorRemote(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT prior_remote_observed,prior_remote_oid FROM git_mutation_leases`).Scan(&observed, &oid); err != nil || observed != 1 || oid != "" {
		t.Fatalf("recorded absence observed=%d oid=%q err=%v", observed, oid, err)
	}
	if err := absent.RecordPushPriorRemote(ctx, strings.Repeat("a", 40)); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("absence overwrite=%v", err)
	}
	db, ctx, mixed := newPush("SF-git-facts-mixed-width")
	if err := mixed.RecordPushPriorRemote(ctx, strings.Repeat("a", 64)); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("mixed-width prior remote=%v", err)
	}
	db, ctx, present := newPush("SF-git-facts-present")
	want := strings.Repeat("a", 40)
	if err := present.RecordPushPriorRemote(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT prior_remote_observed,prior_remote_oid FROM git_mutation_leases`).Scan(&observed, &oid); err != nil || observed != 1 || oid != want {
		t.Fatalf("recorded presence observed=%d oid=%q err=%v", observed, oid, err)
	}
}

func TestActiveGitMutationLeasesQuarantinesInvalidRecoveryFacts(t *testing.T) {
	for name, tamper := range map[string]func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent){
		"intent lease mismatch": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_intents SET prepared_commit_oid=?,prepared_tree_oid=? WHERE semantic_key=?`, strings.Repeat("b", 40), strings.Repeat("c", 40), intent.SemanticKey); err != nil {
				t.Fatal(err)
			}
		},
		"partial commit": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET prepared_commit_oid=? WHERE repository_path=?`, strings.Repeat("b", 40), intent.Repository); err != nil {
				t.Fatal(err)
			}
		},
		"invalid remote oid": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET prior_remote_observed=1,prior_remote_oid='not-an-oid' WHERE repository_path=?`, intent.Repository); err != nil {
				t.Fatal(err)
			}
		},
		"invalid observed flag": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `PRAGMA ignore_check_constraints=ON`); err != nil {
				t.Fatal(err)
			}
			defer func() { _, _ = db.db.ExecContext(context.Background(), `PRAGMA ignore_check_constraints=OFF`) }()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET prior_remote_observed=2 WHERE repository_path=?`, intent.Repository); err != nil {
				t.Fatal(err)
			}
		},
		"mixed oid width": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET prepared_commit_oid=?,prepared_tree_oid=? WHERE repository_path=?`, strings.Repeat("b", 64), strings.Repeat("c", 64), intent.Repository); err != nil {
				t.Fatal(err)
			}
		},
		"wrong operation": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET operation='push' WHERE repository_path=?`, intent.Repository); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := gitIntentFixture(t, db, ctx, "SF-git-recovery-"+strings.ReplaceAll(name, " ", "-"))
			claim, err := db.IssueGitMutationClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.AcquireGitMutation(ctx, claim); err != nil {
				t.Fatal(err)
			}
			tamper(t, db, ctx, intent)
			if _, err := db.ActiveGitMutationLeases(ctx, domain.ChannelDev); !errors.Is(err, ErrGitMutationLease) {
				t.Fatalf("invalid recovery facts accepted: %v", err)
			}
			var state, launch string
			if err := db.db.QueryRowContext(ctx, `SELECT state,launch_state FROM git_mutation_leases WHERE repository_path=?`, intent.Repository).Scan(&state, &launch); err != nil || state != "quarantined" || launch != "quarantined" {
				t.Fatalf("lease was not quarantined: state=%q launch=%q err=%v", state, launch, err)
			}
		})
	}
}

func TestGitMutationContractClaimRequiresCanonicalSemanticKeyAndOIDWidth(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-contract-key")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if !validContractClaim(claim) {
		t.Fatal("canonical claim refused")
	}
	tampered := claim
	tampered.SemanticKey = "arbitrary-key"
	if validContractClaim(tampered) {
		t.Fatal("arbitrary semantic key accepted")
	}
	tampered = claim
	tampered.ExpectedHeadOID = strings.Repeat("b", 64)
	if validContractClaim(tampered) {
		t.Fatal("mixed-width claim accepted")
	}
}

func TestAdoptInvalidatedLeasesRefusesActiveOrQuarantinedGitWriter(t *testing.T) {
	for _, state := range []string{"active", "quarantined"} {
		t.Run(state, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := gitIntentFixture(t, db, ctx, "SF-git-adopt-"+state)
			if _, err := db.AcquireLeases(ctx, intent.Ref, intent.TicketVersion, intent.Fence, []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}}, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			claim, err := db.IssueGitMutationClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.AcquireGitMutation(ctx, claim); err != nil {
				t.Fatal(err)
			}
			if state == "quarantined" {
				if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET state='quarantined',launch_state='quarantined' WHERE repository_path=?`, intent.Repository); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.InvalidateRunner(ctx, intent.Ref, intent.TicketVersion, intent.Fence); err != nil {
				t.Fatal(err)
			}
			if _, err := db.AdoptInvalidatedLeases(ctx, intent.Ref, intent.Fence.RunnerEpoch, intent.Fence.LeaderEpoch); !errors.Is(err, ErrLeaseAdoption) {
				t.Fatalf("%s Git writer adoption=%v", state, err)
			}
		})
	}
}

func TestGitMutationClaimRejectsCallerChosenRepositoryIdentity(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-git-repository-identity")
	for name, mutate := range map[string]func(*GitMutationIntent){
		"repository alias": func(value *GitMutationIntent) { value.Repository = "/private/tmp/nysa" },
		"worktree alias":   func(value *GitMutationIntent) { value.Worktree = "/tmp/nysa/other" },
		"branch":           func(value *GitMutationIntent) { value.Branch = "sf/dev/other" },
		"base ref":         func(value *GitMutationIntent) { value.BaseRef = "trunk" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := intent
			mutate(&changed)
			if _, err := db.IssueGitMutationClaim(ctx, changed); !errors.Is(err, ErrGitMutationIntent) {
				t.Fatalf("changed identity claim=%v", err)
			}
		})
	}
	var state string
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM effects WHERE semantic_key=?`, intent.SemanticKey).Scan(&state); err != nil || state != string(EffectPlanned) {
		t.Fatalf("rejected identities changed effect state=%q err=%v", state, err)
	}
}

func TestGitMutationLeaseSurvivesFenceChangeUntilRecovery(t *testing.T) {
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

func TestGitMutationRecoveryQuarantinesFactChangesDuringDrain(t *testing.T) {
	for name, mutate := range map[string]func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent){
		"lease": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_leases SET prepared_commit_oid=?,prepared_tree_oid=? WHERE repository_path=?`, strings.Repeat("b", 40), strings.Repeat("c", 40), intent.Repository); err != nil {
				t.Fatal(err)
			}
		},
		"intent": func(t *testing.T, db *Store, ctx context.Context, intent GitMutationIntent) {
			t.Helper()
			if _, err := db.db.ExecContext(ctx, `UPDATE git_mutation_intents SET prepared_commit_oid=?,prepared_tree_oid=? WHERE semantic_key=?`, strings.Repeat("b", 40), strings.Repeat("c", 40), intent.SemanticKey); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := gitIntentFixture(t, db, ctx, "SF-git-drain-race-"+name)
			claim, err := db.IssueGitMutationClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := db.AcquireGitMutation(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			launchLease, ok := lease.(contracts.GitMutationLaunchLease)
			if !ok {
				t.Fatal("store lease does not record launches")
			}
			launch := contracts.GitMutationLaunch{PID: 77, PGID: 77, BootIdentity: "boot-race", ProcessStartIdentity: "start-race"}
			if err := launchLease.RecordGitMutationLaunch(ctx, launch); err != nil {
				t.Fatal(err)
			}
			err = db.RecoverGitMutationLeases(ctx, domain.ChannelDev, intent.Fence.LeaderEpoch, gitDrainerFunc(func(_ context.Context, got contracts.GitMutationLaunch) error {
				if got != launch {
					t.Fatalf("launch=%+v", got)
				}
				mutate(t, db, ctx, intent)
				return nil
			}))
			if !errors.Is(err, ErrGitMutationLease) {
				t.Fatalf("fact mutation during drain was accepted: %v", err)
			}
			var state, launchState string
			if err := db.db.QueryRowContext(ctx, `SELECT state,launch_state FROM git_mutation_leases WHERE repository_path=?`, intent.Repository).Scan(&state, &launchState); err != nil || state != "quarantined" || launchState != "quarantined" {
				t.Fatalf("post-drain fact race lease not retained/quarantined: state=%q launch=%q err=%v", state, launchState, err)
			}
		})
	}
}

var _ contracts.GitMutationAuthority = (*Store)(nil)
