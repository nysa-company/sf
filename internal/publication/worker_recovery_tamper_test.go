package publication_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/publication"
	"github.com/nysa-company/sf/internal/store"
)

func TestWorkerRejectsFutureRecoveryRowBeforeFirstPublicationMutation(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}

	mutantDir := t.TempDir()
	if err := os.Chmod(mutantDir, 0o700); err != nil {
		t.Fatal(err)
	}
	mutantPath := filepath.Join(mutantDir, "future-recovery.sqlite")
	if err := f.db.Backup(f.ctx, mutantPath); err != nil {
		t.Fatal(err)
	}
	raw, err := sql.Open("sqlite", mutantPath)
	if err != nil {
		t.Fatal(err)
	}
	createdAt := time.Now().UTC()
	payload, err := json.Marshal(struct {
		Channel            domain.Channel   `json:"channel"`
		Project            domain.ProjectID `json:"project"`
		Ticket             domain.TicketID  `json:"ticket"`
		PriorTicketVersion uint64           `json:"prior_ticket_version"`
		PriorRunnerEpoch   uint64           `json:"prior_runner_epoch"`
		PriorLeaderEpoch   uint64           `json:"prior_leader_epoch"`
		TicketVersion      uint64           `json:"ticket_version"`
		RunnerEpoch        uint64           `json:"runner_epoch"`
		LeaderEpoch        uint64           `json:"leader_epoch"`
		CreatedAt          string           `json:"created_at"`
	}{f.ref.Channel, f.ref.Project, f.ref.Ticket, ticket.Version, ticket.RunnerEpoch, f.fence.LeaderEpoch, ticket.Version + 1, ticket.RunnerEpoch + 1, f.fence.LeaderEpoch + 1, createdAt.Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(f.ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, f.ref.Channel, f.ref.Project, f.ref.Ticket, ticket.Version, ticket.RunnerEpoch, f.fence.LeaderEpoch, ticket.Version+1, ticket.RunnerEpoch+1, f.fence.LeaderEpoch+1, "sha256:"+digestHex(payload), createdAt.Format(time.RFC3339Nano)); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	mutant, err := store.Open(f.ctx, mutantPath)
	if err != nil {
		t.Fatal(err)
	}
	defer mutant.Close()

	runner := f.runner
	runner.MutationAuthority = mutant
	if _, err := (publication.Worker{Store: mutant, Git: runner, GitHub: f.github}).Run(f.ctx, f.ref, f.fence); !errors.Is(err, publication.ErrPublicationDrift) {
		t.Fatalf("worker accepted future recovery row: %v", err)
	}
	if f.gitPushCount != 0 || f.github.MutationCount("pr_create") != 0 || f.github.MutationCount("pr_update") != 0 {
		t.Fatalf("future recovery mutated push=%d pr_create=%d pr_update=%d", f.gitPushCount, f.github.MutationCount("pr_create"), f.github.MutationCount("pr_update"))
	}
}

func TestWorkerRejectsCandidateOnlyPublishingRecoveryWithMissingIntermediateLedgerRow(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()

	var leader uint64
	for recovery := 1; recovery <= 2; recovery++ {
		var err error
		leader, err = f.db.AcquireLeader(f.ctx, domain.ChannelDev, "publication-ledger-tamper")
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := f.db.FenceRecoveredRunners(f.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
			t.Fatalf("recovery %d changed=%d err=%v", recovery, changed, err)
		}
	}

	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	if ticket.Version != candidate.TicketVersion+3 {
		t.Fatalf("recovered ticket version=%d candidate version=%d", ticket.Version, candidate.TicketVersion)
	}
	if err := f.db.AuthenticatePublishingRecovery(f.ctx, f.ref, candidate, ticket.Version, fence); err != nil {
		t.Fatalf("intact recovery chain rejected: %v", err)
	}

	mutantDir := t.TempDir()
	if err := os.Chmod(mutantDir, 0o700); err != nil {
		t.Fatal(err)
	}
	mutantPath := filepath.Join(mutantDir, "missing-intermediate.sqlite")
	if err := f.db.Backup(f.ctx, mutantPath); err != nil {
		t.Fatal(err)
	}
	mutant, err := store.Open(f.ctx, mutantPath)
	if err != nil {
		t.Fatal(err)
	}
	defer mutant.Close()

	raw, err := sql.Open("sqlite", mutantPath)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(f.ctx, `DROP TRIGGER runner_recovery_ledger_immutable_delete`); err != nil {
		t.Fatal(err)
	}
	var firstRecoveryVersion uint64
	if err := raw.QueryRowContext(f.ctx, `SELECT MIN(ticket_version) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, f.ref.Channel, f.ref.Project, f.ref.Ticket).Scan(&firstRecoveryVersion); err != nil {
		t.Fatal(err)
	}
	deleted, err := raw.ExecContext(f.ctx, `DELETE FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, f.ref.Channel, f.ref.Project, f.ref.Ticket, firstRecoveryVersion)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := deleted.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("deleted intermediate recovery rows=%d err=%v", rows, err)
	}

	if err := mutant.AuthenticatePublishingRecovery(f.ctx, f.ref, candidate, ticket.Version, fence); !errors.Is(err, store.ErrPublicationEvidence) {
		t.Fatalf("missing intermediate recovery row authenticated: %v", err)
	}
	runner := f.runner
	runner.MutationAuthority = mutant
	if _, err := (publication.Worker{Store: mutant, Git: runner, GitHub: f.github}).Run(f.ctx, f.ref, fence); !errors.Is(err, publication.ErrPublicationDrift) {
		t.Fatalf("worker accepted missing intermediate recovery row: %v", err)
	}
	if f.gitPushCount != 0 || f.github.MutationCount("pr_create") != 0 || f.github.MutationCount("pr_update") != 0 {
		t.Fatalf("tampered recovery mutated push=%d pr_create=%d pr_update=%d", f.gitPushCount, f.github.MutationCount("pr_create"), f.github.MutationCount("pr_update"))
	}
}
