package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func preparedCommitRecoveryFixture(t *testing.T, ticketID string) (*Store, context.Context, GitMutationIntent, contracts.GitMutationClaim, string, string, uint64) {
	t.Helper()
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, ticketID)
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("b", 40), strings.Repeat("c", 40)
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "prepared-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileEffects(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	return db, ctx, intent, claim, commit, tree, leader
}

func TestConfirmPreparedCommitAuthenticatesCurrentPreparedTupleAndIsIdempotent(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-prepared-normal")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("b", 40), strings.Repeat("c", 40)
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	observation := contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}
	confirmed, err := db.ConfirmPreparedCommit(ctx, claim, observation)
	if err != nil || confirmed.State != EffectConfirmed || confirmed.ObservedIdentity != commit {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
	again, err := db.ConfirmPreparedCommit(ctx, claim, observation)
	if err != nil || again != confirmed {
		t.Fatalf("replay=%+v err=%v", again, err)
	}
}

func TestConfirmPreparedCommitSettlesOnlyExactUncertainPreparedTuple(t *testing.T) {
	for name, observation := range map[string]func(GitMutationIntent, string, string) contracts.PreparedCommitObservation{
		"exact": func(intent GitMutationIntent, commit, tree string) contracts.PreparedCommitObservation {
			return contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}
		},
		"mismatched tree": func(intent GitMutationIntent, commit, _ string) contracts.PreparedCommitObservation {
			return contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: strings.Repeat("d", 40)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := gitIntentFixture(t, db, ctx, "SF-prepared-uncertain-"+strings.ReplaceAll(name, " ", "-"))
			claim, err := db.IssueGitMutationClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := db.AcquireGitMutation(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			commit, tree := strings.Repeat("b", 40), strings.Repeat("c", 40)
			if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit, tree); err != nil {
				t.Fatal(err)
			}
			if err := lease.Release(); err != nil {
				t.Fatal(err)
			}
			if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
				t.Fatal(err)
			}
			before, err := db.Effect(ctx, claim.SemanticKey)
			if err != nil || before.State != EffectUncertain {
				t.Fatalf("uncertain effect=%+v err=%v", before, err)
			}

			confirmed, err := db.ConfirmPreparedCommit(ctx, claim, observation(intent, commit, tree))
			if name == "exact" {
				if err != nil || confirmed.State != EffectConfirmed || confirmed.ObservedIdentity != commit {
					t.Fatalf("confirmed=%+v err=%v", confirmed, err)
				}
				return
			}
			if !errors.Is(err, ErrGitMutationIntent) {
				t.Fatalf("mismatch err=%v", err)
			}
			after, readErr := db.Effect(ctx, claim.SemanticKey)
			if readErr != nil || after != before {
				t.Fatalf("mismatch changed uncertain effect: before=%+v after=%+v err=%v", before, after, readErr)
			}
		})
	}
}

func TestConfirmPreparedCommitRefusesActiveLeaseAndMismatch(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-prepared-normal-reject")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("b", 40), strings.Repeat("c", 40)
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}); !errors.Is(err, ErrGitMutationLease) {
		t.Fatalf("active lease err=%v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: strings.Repeat("d", 40)}); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("mismatch err=%v", err)
	}
	got, err := db.Effect(ctx, claim.SemanticKey)
	if err != nil || got.State != EffectExecuting {
		t.Fatalf("effect=%+v err=%v", got, err)
	}
}

func TestRetireUnpreparedGitCommitMakesDeterministicRetryPossible(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := gitIntentFixture(t, db, ctx, "SF-prepared-retire")
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := db.RetireUnpreparedGitCommit(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GitMutationIntentFacts(ctx, intent.SemanticKey); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("retired facts err=%v", err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	retry, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil || retry.ClaimEpoch != claim.ClaimEpoch+1 {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
}

func TestConfirmRecoveredPreparedCommitIsExactAndIdempotentAcrossRunnerFence(t *testing.T) {
	db, ctx, intent, claim, commit, tree, leader := preparedCommitRecoveryFixture(t, "SF-prepared-confirm")
	observation := contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}
	confirmed, err := db.ConfirmRecoveredPreparedCommit(ctx, claim, observation)
	if err != nil || confirmed.State != EffectConfirmed || confirmed.ObservedIdentity != commit {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
	again, err := db.ConfirmRecoveredPreparedCommit(ctx, claim, observation)
	if err != nil || again != confirmed {
		t.Fatalf("same-fence replay=%+v err=%v want=%+v", again, err, confirmed)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	// A later startup observes the already confirmed immutable result without
	// asking Git to observe or mutate it again.
	third, err := db.ConfirmRecoveredPreparedCommit(ctx, claim, observation)
	if err != nil || third.State != EffectConfirmed || third.ObservedIdentity != commit {
		t.Fatalf("post-fence replay=%+v err=%v", third, err)
	}
	var prepared string
	if err := db.db.QueryRowContext(ctx, `SELECT prepared_commit_oid FROM git_mutation_intents WHERE semantic_key=?`, intent.SemanticKey).Scan(&prepared); err != nil || prepared != commit {
		t.Fatalf("immutable prepared commit=%q err=%v", prepared, err)
	}
	var activeLeases int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE semantic_key=? AND state='active'`, intent.SemanticKey).Scan(&activeLeases); err != nil || activeLeases != 0 {
		t.Fatalf("confirmation minted or retained a lease: count=%d err=%v", activeLeases, err)
	}
}

func TestConfirmRecoveredPreparedCommitRejectsMismatchedObservationWithoutStateChange(t *testing.T) {
	db, ctx, _, claim, commit, tree, _ := preparedCommitRecoveryFixture(t, "SF-prepared-mismatch")
	before, err := db.Effect(ctx, claim.SemanticKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, observation := range map[string]contracts.PreparedCommitObservation{
		"parent": {CommitOID: commit, ParentOID: strings.Repeat("d", 40), TreeOID: tree},
		"tree":   {CommitOID: commit, ParentOID: strings.Repeat("a", 40), TreeOID: strings.Repeat("d", 40)},
		"commit": {CommitOID: strings.Repeat("d", 40), ParentOID: strings.Repeat("a", 40), TreeOID: tree},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := db.ConfirmRecoveredPreparedCommit(ctx, claim, observation)
			if !errors.Is(err, ErrPreparedCommitRecovery) {
				t.Fatalf("err=%v want prepared recovery error", err)
			}
			after, readErr := db.Effect(ctx, claim.SemanticKey)
			if readErr != nil || after.State != EffectUncertain || after != before {
				t.Fatalf("effect changed after mismatch: before=%+v after=%+v err=%v", before, after, readErr)
			}
		})
	}
}

func TestConfirmRecoveredPreparedCommitRejectsMissingOrPartialPreparedTuple(t *testing.T) {
	cases := map[string]func(*testing.T, *Store, GitMutationIntent){
		"missing commit": func(t *testing.T, db *Store, intent GitMutationIntent) {
			_, err := db.db.ExecContext(context.Background(), `UPDATE git_mutation_intents SET prepared_commit_oid='' WHERE semantic_key=?`, intent.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
		},
		"missing tree": func(t *testing.T, db *Store, intent GitMutationIntent) {
			_, err := db.db.ExecContext(context.Background(), `UPDATE git_mutation_intents SET prepared_tree_oid='' WHERE semantic_key=?`, intent.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
		},
		"malformed commit": func(t *testing.T, db *Store, intent GitMutationIntent) {
			_, err := db.db.ExecContext(context.Background(), `UPDATE git_mutation_intents SET prepared_commit_oid=? WHERE semantic_key=?`, strings.Repeat("z", 40), intent.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
		},
		"malformed tree": func(t *testing.T, db *Store, intent GitMutationIntent) {
			_, err := db.db.ExecContext(context.Background(), `UPDATE git_mutation_intents SET prepared_tree_oid=? WHERE semantic_key=?`, strings.Repeat("z", 40), intent.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			db, ctx, intent, claim, commit, tree, _ := preparedCommitRecoveryFixture(t, "SF-prepared-tuple-"+strings.ReplaceAll(name, " ", "-"))
			mutate(t, db, intent)
			before, err := db.Effect(ctx, claim.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.ConfirmRecoveredPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree})
			if !errors.Is(err, ErrPreparedCommitRecovery) {
				t.Fatalf("err=%v want prepared recovery error", err)
			}
			after, readErr := db.Effect(ctx, claim.SemanticKey)
			if readErr != nil || after.State != EffectUncertain || after != before {
				t.Fatalf("effect changed after tuple tamper: before=%+v after=%+v err=%v", before, after, readErr)
			}
		})
	}
}

func TestConfirmRecoveredPreparedCommitRejectsExpectedParentAndForeignHead(t *testing.T) {
	for name, observation := range map[string]contracts.PreparedCommitObservation{
		"head still expected parent": {CommitOID: strings.Repeat("a", 40), ParentOID: strings.Repeat("a", 40), TreeOID: strings.Repeat("c", 40)},
		"foreign head":               {CommitOID: strings.Repeat("e", 40), ParentOID: strings.Repeat("a", 40), TreeOID: strings.Repeat("c", 40)},
		"wrong parent":               {CommitOID: strings.Repeat("b", 40), ParentOID: strings.Repeat("e", 40), TreeOID: strings.Repeat("c", 40)},
	} {
		t.Run(name, func(t *testing.T) {
			db, ctx, _, claim, _, _, _ := preparedCommitRecoveryFixture(t, "SF-prepared-parent-"+strings.ReplaceAll(name, " ", "-"))
			before, err := db.Effect(ctx, claim.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.ConfirmRecoveredPreparedCommit(ctx, claim, observation)
			if !errors.Is(err, ErrPreparedCommitRecovery) {
				t.Fatalf("err=%v want prepared recovery error", err)
			}
			after, readErr := db.Effect(ctx, claim.SemanticKey)
			if readErr != nil || after.State != EffectUncertain || after != before {
				t.Fatalf("effect changed after parent/head mismatch: before=%+v after=%+v err=%v", before, after, readErr)
			}
		})
	}
}
