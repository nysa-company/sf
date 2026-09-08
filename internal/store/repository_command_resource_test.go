package store

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestRepositoryCommandResourceRetirementRequiresExactDrainedLaunch(t *testing.T) {
	for _, state := range []string{"unrecorded", "active", "quarantined", "wrong-claim", "drained", "rollback"} {
		t.Run(state, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := repositoryCommandIntentFixture(t, db, ctx, "resource-"+state)
			claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := db.AcquireRepositoryCommand(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			if state != "unrecorded" {
				pid := int(atomic.AddInt64(&repositoryCommandTestPID, 1))
				launch := contracts.RepositoryCommandLaunch{PID: pid, PGID: pid, BootIdentity: "boot", ProcessStartIdentity: "resource-test"}
				if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
					t.Fatal(err)
				}
				if state != "active" {
					if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
						t.Fatal(err)
					}
				}
			}
			if state == "quarantined" {
				if err := lease.Quarantine(); err != nil {
					t.Fatal(err)
				}
			}
			request := claim
			if state == "wrong-claim" {
				request.ClaimEpoch++
			}
			if state == "rollback" {
				if _, err := db.db.ExecContext(ctx, `CREATE TRIGGER resource_retire_fault BEFORE DELETE ON repository_command_intents BEGIN SELECT RAISE(ABORT,'injected retirement failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			err = db.RetireObservedResourceLimitedRepositoryCommand(ctx, request)
			if (err == nil) != (state == "drained") {
				t.Fatalf("retirement: %v", err)
			}
			if _, err := db.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("resource abort minted test evidence: %v", err)
			}
			var effectState, observed string
			var leases, intents int
			if err := db.db.QueryRowContext(ctx, `SELECT state,COALESCE(observed_identity,'') FROM effects WHERE semantic_key=?`, claim.SemanticKey).Scan(&effectState, &observed); err != nil {
				t.Fatal(err)
			}
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE semantic_key=?`, claim.SemanticKey).Scan(&leases); err != nil {
				t.Fatal(err)
			}
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_intents WHERE semantic_key=?`, claim.SemanticKey).Scan(&intents); err != nil {
				t.Fatal(err)
			}
			if state != "drained" {
				if effectState != string(EffectExecuting) || leases != 1 || intents != 1 {
					t.Fatalf("refusal changed authority: %s leases=%d intents=%d", effectState, leases, intents)
				}
				return
			}
			if effectState != string(EffectFailed) || observed != "resource-limit:repository-command-observed" || leases != 0 || intents != 0 {
				t.Fatalf("incorrect terminal authority: %s %q leases=%d intents=%d", effectState, observed, leases, intents)
			}
			retry, err := db.IssueRepositoryCommandClaim(ctx, intent)
			if err != nil || retry.ClaimEpoch <= claim.ClaimEpoch {
				t.Fatalf("fresh retry refused: %+v %v", retry, err)
			}
		})
	}
}
