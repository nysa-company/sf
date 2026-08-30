package store

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

func drainedResult(t *testing.T, db *Store, ctx context.Context, lease contracts.RepositoryCommandLease, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) contracts.RepositoryCommandResultKey {
	t.Helper()
	completeDrainedRepositoryCommand(t, db, ctx, lease, claim, result)
	return contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}
}

func TestRepositoryCommandResultExactReplayAndAtomicRollback(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "result-replay")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	pid := int(atomic.AddInt64(&repositoryCommandTestPID, 1))
	launch := contracts.RepositoryCommandLaunch{PID: pid, PGID: pid, BootIdentity: "boot", ProcessStartIdentity: "atomic"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
		t.Fatal(err)
	}
	result := contracts.CommandResult{ExitCode: 1, Stdout: []byte("out"), Stderr: []byte("err"), OutputLastMessage: []byte("last"), StdoutTruncated: true, Duration: time.Millisecond, Observed: true, ObservedAt: time.Now().UTC()}
	if _, err := db.db.ExecContext(ctx, `CREATE TRIGGER repository_result_fault AFTER INSERT ON repository_command_results BEGIN SELECT RAISE(ABORT,'injected result failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, result); err == nil {
		t.Fatal("result insertion fault committed terminal effect")
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER repository_result_fault`); err != nil {
		t.Fatal(err)
	}
	var state string
	var rows int
	if err := db.db.QueryRowContext(ctx, `SELECT state FROM effects WHERE semantic_key=?`, claim.SemanticKey).Scan(&state); err != nil || state != string(EffectExecuting) {
		t.Fatalf("rollback effect state=%q err=%v", state, err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_results WHERE semantic_key=?`, claim.SemanticKey).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("rollback results=%d err=%v", rows, err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, result); err != nil {
		t.Fatal(err)
	}
	if _, err := db.IssueRepositoryCommandClaim(ctx, intent); !errors.Is(err, ErrRepositoryCommandIntent) {
		t.Fatalf("observed semantic request was re-claimed: %v", err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, result); err != nil {
		t.Fatalf("exact completion replay=%v", err)
	}
	changed := result
	changed.Stderr = []byte("different")
	if err := db.CompleteRepositoryCommand(ctx, claim, changed); !errors.Is(err, ErrRepositoryCommandResult) {
		t.Fatalf("non-exact completion replay=%v", err)
	}
}

func TestRepositoryCommandResultAuthenticatedHistoricalLoadAndTampering(t *testing.T) {
	for _, tamper := range []struct {
		name string
		sql  string
	}{
		{"claim-request", `request_digest='sha256:` + strings.Repeat("a", 64) + `'`},
		{"claim-command", `command_digest='sha256:` + strings.Repeat("b", 64) + `'`},
		{"exit", `exit_code=2`},
		{"stdout", `stdout=X'78'`},
		{"stderr", `stderr=X'79'`},
		{"last-message", `output_last_message=X'7A'`},
		{"stdout-flag", `stdout_truncated=1`},
		{"stderr-flag", `stderr_truncated=1`},
		{"last-flag", `output_last_message_truncated=1`},
		{"duration", `duration_ns=2`},
		{"observed-at", `observed_at=replace(observed_at,'Z','+00:00')`},
		{"created-at", `created_at=replace(created_at,'Z','+00:00')`},
		{"stdout-digest", `stdout_digest='sha256:` + strings.Repeat("c", 64) + `'`},
		{"stderr-digest", `stderr_digest='sha256:` + strings.Repeat("d", 64) + `'`},
		{"last-digest", `output_last_message_digest='sha256:` + strings.Repeat("e", 64) + `'`},
		{"result-digest", `result_digest='sha256:` + strings.Repeat("f", 64) + `'`},
	} {
		t.Run(tamper.name, func(t *testing.T) {
			db, ctx := openTestStore(t)
			intent := repositoryCommandIntentFixture(t, db, ctx, "tamper-"+tamper.name)
			claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := db.AcquireRepositoryCommand(ctx, claim)
			if err != nil {
				t.Fatal(err)
			}
			key := drainedResult(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 1, Stdout: []byte("out"), Stderr: []byte("err"), OutputLastMessage: []byte("last"), Duration: time.Nanosecond})
			if _, err := db.db.ExecContext(ctx, `DROP TRIGGER repository_command_results_immutable_update`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.db.ExecContext(ctx, `UPDATE repository_command_results SET `+tamper.sql+` WHERE semantic_key=? AND claim_epoch=?`, key.SemanticKey, key.ClaimEpoch); err != nil {
				t.Fatal(err)
			}
			if _, err := db.LoadRepositoryCommandResult(ctx, key); !errors.Is(err, ErrRepositoryCommandResult) {
				t.Fatalf("tampered %s loaded: %v", tamper.name, err)
			}
		})
	}
}

func TestRepositoryCommandResultRejectsSameRowPayloadAndDigestRewrite(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "same-row-rewrite")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	key := drainedResult(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 1, Stdout: []byte("before"), Duration: time.Millisecond})
	stored, err := db.LoadRepositoryCommandResult(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM repository_command_intents WHERE semantic_key=?`, claim.SemanticKey); err != nil {
		t.Fatal(err)
	}
	forged := stored
	forged.Result.Stdout = []byte("after")
	stdout, stderr, last, full, err := resultDigests(forged.Claim, forged.Result, forged.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER repository_command_results_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE repository_command_results SET stdout=?,stdout_digest=?,stderr_digest=?,output_last_message_digest=?,result_digest=? WHERE semantic_key=? AND claim_epoch=?`, forged.Result.Stdout, stdout, stderr, last, full, key.SemanticKey, key.ClaimEpoch); err != nil {
		t.Fatal(err)
	}
	// The matching full digest in effects is an independent immutable-completion
	// witness. A one-row payload+digest rewrite cannot replace authority after
	// the lease and intent are gone.
	if _, err := db.LoadRepositoryCommandResult(ctx, key); !errors.Is(err, ErrRepositoryCommandResult) {
		t.Fatalf("same-row recomputed rewrite loaded: %v", err)
	}
}

func TestRepositoryCommandResultIsHistoricalAfterLeaseAndIntentCleanup(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "historical-result")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	key := drainedResult(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 0, Duration: time.Millisecond})
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM repository_command_intents WHERE semantic_key=?`, claim.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if got, err := db.LoadRepositoryCommandResult(ctx, key); err != nil || got.Key != key || got.Claim != claim || got.Result.ExitCode != 0 {
		t.Fatalf("historical result=%+v err=%v", got, err)
	}
	if _, err := db.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: key.SemanticKey, ClaimEpoch: key.ClaimEpoch + 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong result epoch=%v", err)
	}
	if _, err := db.LoadRepositoryCommandResult(ctx, contracts.RepositoryCommandResultKey{SemanticKey: "other", ClaimEpoch: key.ClaimEpoch}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong result key=%v", err)
	}
}

func TestRepositoryCommandResultHistoricalTimestampDoesNotExpire(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "old-result")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	key := drainedResult(t, db, ctx, lease, claim, contracts.CommandResult{ExitCode: 0, Duration: time.Millisecond})
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	stored, err := db.LoadRepositoryCommandResult(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-72 * time.Hour)
	stored.Result.ObservedAt = old
	created := old.Add(time.Minute)
	_, _, _, digest, err := resultDigests(stored.Claim, stored.Result, created)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER repository_command_results_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE repository_command_results SET observed_at=?,result_digest=?,created_at=? WHERE semantic_key=? AND claim_epoch=?`, old.Format(time.RFC3339Nano), digest, created.Format(time.RFC3339Nano), key.SemanticKey, key.ClaimEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE effects SET observed_identity=? WHERE semantic_key=?`, "repository-command-result:"+digest, key.SemanticKey); err != nil {
		t.Fatal(err)
	}
	if got, err := db.LoadRepositoryCommandResult(ctx, key); err != nil || !got.Result.ObservedAt.Equal(old) {
		t.Fatalf("old durable result=%+v err=%v", got, err)
	}
}

func TestRepositoryCommandUncertainAndStalePathsNeverMintResult(t *testing.T) {
	db, ctx := openTestStore(t)
	intent := repositoryCommandIntentFixture(t, db, ctx, "uncertain-no-result")
	claim, err := db.IssueRepositoryCommandClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkRepositoryCommandUncertain(ctx, claim, "process identity unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 0, Observed: true, ObservedAt: time.Now().UTC()}); err == nil {
		t.Fatal("uncertain command minted a result")
	}
	var n int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_results WHERE semantic_key=?`, claim.SemanticKey).Scan(&n); err != nil || n != 0 {
		t.Fatalf("uncertain result rows=%d err=%v", n, err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}
