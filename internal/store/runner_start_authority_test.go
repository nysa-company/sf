package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
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
		ownershipRef := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-start-authority-ownership"}
		if err := db.CreateTicket(ctx, Ticket{Ref: ownershipRef, SourceDigest: "source-ownership", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.StartWithOwnership(ctx, ownershipRef, 1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}, "dev/provider/ownership", []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}}, time.Now().UTC()); err != nil {
			t.Fatalf("start-with-ownership authority: %v", err)
		}
		var ownershipCount int
		if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_start_authorities WHERE channel=? AND project_id=? AND ticket_id=?`, ownershipRef.Channel, ownershipRef.Project, ownershipRef.Ticket).Scan(&ownershipCount); err != nil || ownershipCount != 1 {
			t.Fatalf("start-with-ownership authority count=%d err=%v", ownershipCount, err)
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
