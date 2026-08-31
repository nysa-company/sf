package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestRunnerStartAuthorityIsCanonicalImmutableAndUnique(t *testing.T) {
	newStarted := func(t *testing.T, id string) (*Store, context.Context, Ticket, uint64) {
		t.Helper()
		db, ctx := openTestStore(t)
		setupProviderProject(t, db, ctx)
		leader, err := db.AcquireLeader(ctx, domain.ChannelDev, id+"-leader")
		if err != nil {
			t.Fatal(err)
		}
		ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: domain.TicketID(id)}
		if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: "source-" + id, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
			t.Fatal(err)
		}
		started, err := db.StartOrAdopt(ctx, ref, 1, "dev/provider/"+id, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
		if err != nil {
			t.Fatal(err)
		}
		return db, ctx, started, leader
	}

	t.Run("canonical row and duplicate key", func(t *testing.T) {
		db, ctx, started, leader := newStarted(t, "SF-start-authority-canonical")
		defer db.Close()
		var workflowID, workflowDigest, createdAt, authorityDigest string
		var startVersion, runner, rowLeader uint64
		if err := db.db.QueryRowContext(ctx, `SELECT start_ticket_version,runner_epoch,leader_epoch,workflow_id,workflow_digest,created_at,authority_digest FROM runner_start_authorities WHERE channel=? AND project_id=? AND ticket_id=?`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket).Scan(&startVersion, &runner, &rowLeader, &workflowID, &workflowDigest, &createdAt, &authorityDigest); err != nil {
			t.Fatal(err)
		}
		payload, payloadErr := runnerStartAuthorityPayload(started.Ref, startVersion, runner, rowLeader, workflowID, createdAt)
		if startVersion != started.Version || runner != 1 || rowLeader != leader || workflowID == "" || workflowDigest != publicationIdentityDigest([]byte(workflowID)) || payloadErr != nil || authorityDigest != publicationIdentityDigest(payload) {
			t.Fatalf("invalid start authority version=%d runner=%d leader=%d workflow=%q workflow_digest=%q authority_digest=%q", startVersion, runner, rowLeader, workflowID, workflowDigest, authorityDigest)
		}
		var eventCount int
		if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='runner_start_authority'`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket).Scan(&eventCount); err != nil || eventCount != 0 {
			t.Fatalf("start authority leaked into event projection: count=%d err=%v", eventCount, err)
		}
		if _, err := db.db.ExecContext(ctx, `INSERT INTO runner_start_authorities(channel,project_id,ticket_id,start_ticket_version,runner_epoch,leader_epoch,workflow_id,workflow_digest,created_at,authority_digest) SELECT channel,project_id,ticket_id,start_ticket_version,runner_epoch,leader_epoch,workflow_id,workflow_digest,created_at,authority_digest FROM runner_start_authorities WHERE channel=? AND project_id=? AND ticket_id=?`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket); err == nil {
			t.Fatal("duplicate start authority row accepted")
		}
		if _, err := db.db.ExecContext(ctx, `UPDATE runner_start_authorities SET leader_epoch=leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket); err == nil {
			t.Fatal("immutable start authority update accepted")
		}
		if _, err := db.db.ExecContext(ctx, `DELETE FROM runner_start_authorities WHERE channel=? AND project_id=? AND ticket_id=?`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket); err == nil {
			t.Fatal("immutable start authority delete accepted")
		}
	})

	t.Run("digest tamper rejects first recovery", func(t *testing.T) {
		db, ctx, started, _ := newStarted(t, "SF-start-authority-tamper")
		defer db.Close()
		if _, err := db.db.ExecContext(ctx, `DROP TRIGGER runner_start_authorities_immutable_update; UPDATE runner_start_authorities SET leader_epoch=leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, started.Ref.Channel, started.Ref.Project, started.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "SF-start-authority-tamper-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("tampered start authority allowed recovery: %v", err)
		}
	})
}

func TestRunnerStartAuthorityCannotBridgeExistingRecoveryGap(t *testing.T) {
	db, ctx := openTestStore(t)
	defer db.Close()
	setupProviderProject(t, db, ctx)
	firstLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "start-authority-gap-first")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-start-authority-gap"}
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: "source-start-authority-gap", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, 1, "dev/provider/start-authority-gap", domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	secondLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "start-authority-gap-second")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil || changed != 1 {
		t.Fatalf("initial authority recovery changed=%d err=%v", changed, err)
	}
	current, err := db.Ticket(ctx, started.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET version=version+1,runner_epoch=runner_epoch+1 WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	thirdLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "start-authority-gap-third")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, thirdLeader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("bootstrap authority bridged existing-ledger gap from v%d/r%d: %v", current.Version, current.RunnerEpoch, err)
	}
}

func TestRunnerRecoveryLedgerRejectsWholeTicketTampering(t *testing.T) {
	newLedger := func(t *testing.T) (*Store, context.Context, domain.TicketRef) {
		t.Helper()
		db, ctx := openTestStore(t)
		setupProviderProject(t, db, ctx)
		leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-ledger-audit")
		if err != nil {
			t.Fatal(err)
		}
		ticket := setupProviderTicket(t, db, ctx, "SF-runner-ledger-audit", leader)
		return db, ctx, ticket.Ref
	}
	appendStep := func(t *testing.T, db *Store, ctx context.Context, ref domain.TicketRef, priorVersion, priorRunner, priorLeader, version, runner, leader uint64) {
		t.Helper()
		step := RunnerRecoveryLedger{Ref: ref, PriorTicketVersion: priorVersion, PriorRunnerEpoch: priorRunner, PriorLeaderEpoch: priorLeader, TicketVersion: version, RunnerEpoch: runner, LeaderEpoch: leader, CreatedAt: time.Date(2026, 8, 30, 18, 0, int(version), 0, time.UTC)}
		step.RecoveryDigest = runnerRecoveryDigest(step)
		if _, err := db.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch, step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch, step.RecoveryDigest, step.CreatedAt.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("rejects a row beyond the requested fence", func(t *testing.T) {
		db, ctx, ref := newLedger(t)
		appendStep(t, db, ctx, ref, 1, 1, 1, 2, 2, 2)
		appendStep(t, db, ctx, ref, 2, 2, 2, 3, 3, 3)
		if err := validateRunnerRecoveryLedger(ctx, db.db, ref, 1, 1, 1, 2, 2, 2); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("future ledger row accepted: %v", err)
		}
	})

	t.Run("rejects a wrong predecessor", func(t *testing.T) {
		db, ctx, ref := newLedger(t)
		appendStep(t, db, ctx, ref, 2, 2, 2, 3, 3, 3)
		if err := validateRunnerRecoveryLedger(ctx, db.db, ref, 1, 1, 1, 3, 3, 3); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("wrong predecessor accepted: %v", err)
		}
	})

	t.Run("exact source and current fence still reject a ticket over the cap", func(t *testing.T) {
		db, ctx, ref := newLedger(t)
		for version := uint64(2); version <= 66; version++ {
			appendStep(t, db, ctx, ref, version-1, version-1, version-1, version, version, version)
		}
		if err := validateRunnerRecoveryLedger(ctx, db.db, ref, 1, 1, 1, 1, 1, 1); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("exact-fence cap tamper accepted: %v", err)
		}
		if _, found, err := loadLatestRunnerRecovery(ctx, db.db, ref); found || !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("latest reader ignored whole-ticket cap: found=%v err=%v", found, err)
		}
	})
}

func TestRunnerRecoveryAuthorityAuthenticatesControlGaps(t *testing.T) {
	newPlanning := func(t *testing.T, id string) (*Store, context.Context, string, Ticket, ProviderQualification, domain.Fence) {
		t.Helper()
		db, ctx := openTestStore(t)
		digest := setupProviderProject(t, db, ctx)
		leader, err := db.AcquireLeader(ctx, domain.ChannelDev, id+"-first")
		if err != nil {
			t.Fatal(err)
		}
		ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, id, leader), leader, domain.StatePlanning)
		planner, _ := setupProviderPair(t, db, ctx)
		return db, ctx, digest, ticket, planner, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	}
	completePlanner := func(t *testing.T, db *Store, ctx context.Context, digest string, ticket Ticket, fence domain.Fence, planner ProviderQualification) ProviderAttemptResultKey {
		t.Helper()
		claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
		if err != nil {
			t.Fatal(err)
		}
		raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"proof"},"paths":["main.go"],"commands":[["go","test"]],"risks":["risk"]}`), UsageTrusted: true, UsageUnits: 1}
		if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: ticket.Type}, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		return ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: claim.Attempt}
	}
	pauseAndResume := func(t *testing.T, db *Store, ctx context.Context, ticket Ticket, fence domain.Fence) Ticket {
		t.Helper()
		stopping, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: fence, EventPayload: `{"intent":"take"}`})
		if err != nil {
			t.Fatal(err)
		}
		current, err := db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		paused, err := db.CompleteControlTransition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{"drained":true}`})
		if err != nil {
			t.Fatal(err)
		}
		current, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{"operator":"test"}`}); err != nil {
			t.Fatal(err)
		}
		current, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		openExactRuntimeAdmission(t, db, ticket.Ref)
		return current
	}

	t.Run("recovery then pause take resume and later recovery remain authoritative", func(t *testing.T) {
		db, ctx, digest, ticket, planner, _ := newPlanning(t, "SF-runner-control-gap")
		firstLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-control-gap-first-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, firstLeader); err != nil {
			t.Fatal(err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		ticket = pauseAndResume(t, db, ctx, ticket, domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: ticket.RunnerEpoch})
		fence := domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: ticket.RunnerEpoch}
		key := completePlanner(t, db, ctx, digest, ticket, fence, planner)
		if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, key, ticket.Version, fence); err != nil {
			t.Fatalf("current provider after operator handoff=%v", err)
		}

		secondLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-control-gap-second-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil {
			t.Fatalf("later recovery after operator handoff=%v", err)
		}
		var priorLeader, recoveryLeader uint64
		if err := db.db.QueryRowContext(ctx, `SELECT prior_leader_epoch,leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&priorLeader, &recoveryLeader); err != nil || priorLeader != firstLeader || priorLeader >= recoveryLeader {
			t.Fatalf("second recovery leader lineage prior=%d recovery=%d expected_prior=%d err=%v", priorLeader, recoveryLeader, firstLeader, err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence = domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: ticket.RunnerEpoch}
		reusable, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence})
		if err != nil || !reusable.Recovered || reusable.Key != key {
			t.Fatalf("completed planner was rerun instead of reused: reusable=%+v err=%v original=%+v", reusable, err, key)
		}
		thirdLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-control-gap-third-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, thirdLeader); err != nil {
			t.Fatalf("third recovery after operator handoff=%v", err)
		}
		if err := db.db.QueryRowContext(ctx, `SELECT prior_leader_epoch,leader_epoch FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY ticket_version DESC LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&priorLeader, &recoveryLeader); err != nil || priorLeader != secondLeader || priorLeader >= recoveryLeader {
			t.Fatalf("third recovery leader lineage prior=%d recovery=%d expected_prior=%d err=%v", priorLeader, recoveryLeader, secondLeader, err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence = domain.Fence{LeaderEpoch: thirdLeader, RunnerEpoch: ticket.RunnerEpoch}
		reusable, err = db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence})
		if err != nil || !reusable.Recovered || reusable.Key != key {
			t.Fatalf("completed planner was not replayable on third restart: reusable=%+v err=%v original=%+v", reusable, err, key)
		}
	})

	t.Run("unexplained runner gap is rejected", func(t *testing.T) {
		db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-unexplained-gap")
		leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-unexplained-gap-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
			t.Fatal(err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
		if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET version=version+1,runner_epoch=runner_epoch+1 WHERE channel=? AND project_id=? AND id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence.RunnerEpoch = ticket.RunnerEpoch
		key := completePlanner(t, db, ctx, digest, ticket, fence, planner)
		if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, key, ticket.Version, fence); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("unexplained runner gap accepted: %v", err)
		}
	})

	t.Run("leader acquisition alone does not reauthorize a generic provider reader", func(t *testing.T) {
		db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-leader-only")
		key := completePlanner(t, db, ctx, digest, ticket, fence, planner)
		newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-leader-only-restart")
		if err != nil {
			t.Fatal(err)
		}
		newFence := domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: ticket.RunnerEpoch}
		if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, key, ticket.Version, newFence); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("old provider result survived leader-only takeover: %v", err)
		}
		if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: newFence}); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("old reusable provider result survived leader-only takeover: %v", err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil {
			t.Fatalf("fence after leader-only takeover: %v", err)
		}
		current, err := db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		newFence.RunnerEpoch = current.RunnerEpoch
		if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, key, current.Version, newFence); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("current provider reader accepted historical claim after recovery: %v", err)
		}
		reusable, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: current.Version, Fence: newFence})
		if err != nil || !reusable.Recovered || reusable.Key != key {
			t.Fatalf("fenced provider result was not reusable through recovery ledger: reusable=%+v err=%v", reusable, err)
		}
	})

	t.Run("first recovery rejects duplicate lifecycle evidence and rolls back", func(t *testing.T) {
		db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-duplicate-lifecycle")
		completePlanner(t, db, ctx, digest, ticket, fence, planner)
		if _, err := db.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version, "forged_duplicate", domain.StateQueued, domain.StatePlanning, `{}`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-duplicate-lifecycle-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("duplicate initial lifecycle was fenced: %v", err)
		}
		current, err := db.Ticket(ctx, ticket.Ref)
		if err != nil || current.Version != ticket.Version || current.RunnerEpoch != ticket.RunnerEpoch {
			t.Fatalf("failed fence mutated ticket: current=%+v original=%+v err=%v", current, ticket, err)
		}
		var rows int
		if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("failed fence wrote recovery evidence rows=%d err=%v", rows, err)
		}
	})

	for _, tamper := range []struct {
		id      string
		name    string
		version uint64
		trigger string
	}{
		{"v1", "wrong v1 submission trigger", 1, "forged_submission"},
		{"v2", "wrong v2 start trigger", 2, "forged_start"},
	} {
		t.Run(tamper.name, func(t *testing.T) {
			db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-start-anchor-"+tamper.id)
			completePlanner(t, db, ctx, digest, ticket, fence, planner)
			if _, err := db.db.ExecContext(ctx, `UPDATE events SET trigger=? WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, tamper.trigger, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, tamper.version); err != nil {
				t.Fatal(err)
			}
			leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-start-anchor-"+tamper.id)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("forged lifecycle anchor fenced: %v", err)
			}
			current, err := db.Ticket(ctx, ticket.Ref)
			if err != nil || current.Version != ticket.Version || current.RunnerEpoch != ticket.RunnerEpoch {
				t.Fatalf("forged lifecycle anchor mutated ticket: current=%+v original=%+v err=%v", current, ticket, err)
			}
			var rows int
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("forged lifecycle anchor wrote recovery evidence rows=%d err=%v", rows, err)
			}
		})
	}

	t.Run("control endpoint tampering is rejected after a pause handoff", func(t *testing.T) {
		db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-control-endpoint-tamper")
		completePlanner(t, db, ctx, digest, ticket, fence, planner)
		firstLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-control-endpoint-first-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, firstLeader); err != nil {
			t.Fatal(err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		pauseAndResume(t, db, ctx, ticket, domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: ticket.RunnerEpoch})
		if _, err := db.db.ExecContext(ctx, `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		secondLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-control-endpoint-tamper-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("missing control endpoint accepted: %v", err)
		}
	})

	t.Run("paused restart may advance the leader without a recovery row", func(t *testing.T) {
		db, ctx, digest, ticket, planner, _ := newPlanning(t, "SF-runner-paused-leader")
		firstLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-paused-leader-first-restart")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, firstLeader); err != nil {
			t.Fatal(err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		stopping, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: ticket.RunnerEpoch}, EventPayload: `{}`})
		if err != nil {
			t.Fatal(err)
		}
		paused, err := db.CompleteControlTransition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`})
		if err != nil {
			t.Fatal(err)
		}
		secondLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "runner-paused-leader-restart")
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil || changed != 0 {
			t.Fatalf("paused ticket fenced changed=%d err=%v", changed, err)
		}
		if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`}); err != nil {
			t.Fatal(err)
		}
		openExactRuntimeAdmission(t, db, ticket.Ref)
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence := domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: ticket.RunnerEpoch}
		key := completePlanner(t, db, ctx, digest, ticket, fence, planner)
		if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, key, ticket.Version, fence); err != nil {
			t.Fatalf("current provider after paused restart=%v", err)
		}
	})

	t.Run("exact-current provider readers audit a malformed earlier row", func(t *testing.T) {
		db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-whole-ledger-reader")
		key := completePlanner(t, db, ctx, digest, ticket, fence, planner)
		// This row is individually well-signed and at/before the live fence,
		// but overlays the durable start transition rather than a recovery.
		step := RunnerRecoveryLedger{Ref: ticket.Ref, PriorTicketVersion: 1, PriorRunnerEpoch: 1, PriorLeaderEpoch: 0, TicketVersion: 2, RunnerEpoch: 2, LeaderEpoch: fence.LeaderEpoch, CreatedAt: time.Date(2026, 8, 30, 19, 0, 0, 0, time.UTC)}
		step.RecoveryDigest = runnerRecoveryDigest(step)
		if _, err := db.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, step.Ref.Channel, step.Ref.Project, step.Ref.Ticket, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch, step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch, step.RecoveryDigest, step.CreatedAt.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.LoadCurrentProviderAttemptResult(ctx, key, ticket.Version, fence); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("current provider reader accepted malformed ledger: %v", err)
		}
		historical, _, err := db.LoadHistoricalProviderAttemptResult(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.LoadProviderAttemptResult(ctx, historical.Claim, ticket.Version, fence); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("claim provider reader accepted malformed ledger: %v", err)
		}
		if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhasePlanning, Role: "planner", ExpectedVersion: ticket.Version, Fence: fence}); !errors.Is(err, ErrStaleFence) {
			t.Fatalf("reusable selector accepted malformed ledger: %v", err)
		}
	})

	t.Run("phase baseline stays on the recovery write connection", func(t *testing.T) {
		db, ctx, digest, ticket, planner, fence := newPlanning(t, "SF-runner-baseline-conn")
		key := completePlanner(t, db, ctx, digest, ticket, fence, planner)
		if key.AttemptID == 0 {
			t.Fatal("missing completed planner")
		}
		if err := db.write(ctx, func(conn *sql.Conn) error {
			baseline, found, err := db.loadPhaseRecoveryBaseline(ctx, conn, ticket.Ref, domain.PhasePlanning, "planner")
			if err != nil || !found || baseline.version != ticket.Version || baseline.runner != ticket.RunnerEpoch || baseline.leader != fence.LeaderEpoch {
				t.Fatalf("conn-scoped baseline=%+v found=%v err=%v", baseline, found, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCompletedBuilderIsNotReusedAfterChecksRedReturnsToBuilding(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "checks-red-baseline")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-checks-red-baseline", leader), leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"done","changed_files":["main.go"],"commands":[["go","test"]]}`), UsageTrusted: true, UsageUnits: 1}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: ticket.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	returnedVersion := ticket.Version + 1
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET state='building',version=? WHERE channel=? AND project_id=? AND id=?`, returnedVersion, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, returnedVersion, "checks_red", "waiting_ci", "building", "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "checks-red-baseline-restart")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("completed builder crossed invalidating checks_red return: %v", err)
	}
}

func TestRunnerControlAdvanceRejectsSemanticSelfTransition(t *testing.T) {
	db, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-self-transition"}
	if err := db.CreateTicket(ctx, ticket(ref, "self-transition")); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "self-transition")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, 2, "checks_pending", domain.StateQueued, domain.StateQueued, `{}`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := validateRunnerControlAdvance(ctx, db.db, ref, 1, 1, leader, 2, 1, leader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("semantic self-transition accepted: %v", err)
	}
}

func TestRunnerControlAdvanceRejectsLeaderOnlyAndRecoverPauseTriplets(t *testing.T) {
	db, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-leader-only"}
	if err := db.CreateTicket(ctx, ticket(ref, "control-leader-only")); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "control-leader-only")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRunnerControlAdvance(ctx, db.db, ref, 1, 1, leader, 1, 1, leader+1); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("leader-only takeover accepted as control advance: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, event := range []struct {
		version uint64
		trigger string
		from    domain.State
		to      domain.State
	}{
		{2, "operator_pause_or_take", domain.StateQueued, domain.StateStopping},
		{3, "process_and_effects_drained", domain.StateStopping, domain.StatePaused},
		{4, "operator_recover", domain.StatePaused, domain.StatePlanning},
	} {
		if _, err := db.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, event.version, event.trigger, event.from, event.to, `{}`, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateRunnerControlAdvance(ctx, db.db, ref, 1, 1, leader, 4, 2, leader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("operator_recover paused control triplet accepted: %v", err)
	}
}

func TestInitialLifecycleAdvanceRejectsDuplicateAndChainedEventsAtOneVersion(t *testing.T) {
	newTicket := func(t *testing.T, id string) (*Store, context.Context, domain.TicketRef) {
		t.Helper()
		db, ctx := openTestStore(t)
		ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID(id)}
		if err := db.CreateTicket(ctx, ticket(ref, id)); err != nil {
			t.Fatal(err)
		}
		return db, ctx, ref
	}
	insert := func(t *testing.T, db *Store, ctx context.Context, ref domain.TicketRef, version uint64, trigger string, from, to domain.State) {
		t.Helper()
		if _, err := db.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, version, trigger, from, to, `{}`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("duplicate same state", func(t *testing.T) {
		db, ctx, ref := newTicket(t, "SF-initial-duplicate")
		insert(t, db, ctx, ref, 2, "start_or_adopt", domain.StateQueued, domain.StatePlanning)
		insert(t, db, ctx, ref, 2, "duplicate", domain.StateQueued, domain.StatePlanning)
		if err := validateInitialLifecycleAdvance(ctx, db.db, ref, 2); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("duplicate lifecycle event accepted: %v", err)
		}
	})
	t.Run("chained state changes", func(t *testing.T) {
		db, ctx, ref := newTicket(t, "SF-initial-chain")
		insert(t, db, ctx, ref, 2, "start_or_adopt", domain.StateQueued, domain.StatePlanning)
		insert(t, db, ctx, ref, 2, "chain", domain.StatePlanning, domain.StateVerifying)
		if err := validateInitialLifecycleAdvance(ctx, db.db, ref, 2); !errors.Is(err, ErrPublicationEvidence) {
			t.Fatalf("chained lifecycle events accepted: %v", err)
		}
	})
}
