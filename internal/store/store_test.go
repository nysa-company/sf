package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	_ "modernc.org/sqlite"
)

func openTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	ctx := context.Background()
	database, err := Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	project := Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}
	if err := database.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	return database, ctx
}

func ticket(ref domain.TicketRef, digest string) Ticket {
	return Ticket{Ref: ref, SourceDigest: digest, Type: domain.TicketBug, MergeMode: domain.MergeGuarded}
}

func TestMigrationAndActiveTicketConstraint(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest")); err != nil {
		t.Fatal(err)
	}
	second := ref
	second.Ticket = "SF-2"
	if err := database.CreateTicket(ctx, ticket(second, "digest")); err == nil {
		t.Fatal("expected active ticket digest uniqueness violation")
	}
	var migrations int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, schemaVersion).Scan(&migrations); err != nil {
		t.Fatal(err)
	}
	if migrations != 1 {
		t.Fatalf("migrations=%d want=1", migrations)
	}
}

func TestConcurrentActiveTicketConstraint(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.sqlite")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.CreateProject(ctx, Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	refs := []domain.TicketRef{
		{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-concurrent-a"},
		{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-concurrent-b"},
	}
	stores := []*Store{first, second}
	errs := make(chan error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range refs {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			errs <- stores[index].CreateTicket(ctx, ticket(refs[index], "same-digest"))
		}(index)
	}
	close(start)
	wait.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("active ticket constraint allowed %d writers", successes)
	}
}

func TestMigrationTransactionRollsBackOnInterruption(t *testing.T) {
	database, ctx := openTestStore(t)
	err := database.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `CREATE TABLE interrupted_migration_probe (id INTEGER PRIMARY KEY)`); err != nil {
			return err
		}
		return errors.New("injected migration interruption")
	})
	if err == nil {
		t.Fatal("expected injected migration interruption")
	}
	var count int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='interrupted_migration_probe'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("interrupted migration left partial schema")
	}
}

func TestBusyDeadlineIsApplicationOwned(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "busy.sqlite")
	busyStore, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer busyStore.Close()
	lock, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := lock.Exec(`BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	defer lock.Exec(`ROLLBACK`)
	deadline, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = busyStore.CreateProject(deadline, Project{Channel: domain.ChannelDev, ID: "busy", Path: "/tmp/busy", BaseRef: "main"})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("error=%v want ErrBusy", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("write outlived deadline: %s", elapsed)
	}
}

func TestStartOrAdoptRepairsStateWithoutOwner(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-3"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-3")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
	started, err := database.StartOrAdopt(ctx, ref, "dev/nysa/SF-3/planning", fence)
	if err != nil {
		t.Fatal(err)
	}
	if started.State != domain.StatePlanning {
		t.Fatalf("state=%s", started.State)
	}
	// Simulate a process that committed state but lost durable ownership.
	if _, err := database.db.Exec(`DELETE FROM workflow_owners WHERE channel='dev' AND project_id='nysa' AND ticket_id='SF-3'`); err != nil {
		t.Fatal(err)
	}
	if err := database.ReconcileOrphans(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	var owners int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM workflow_owners WHERE channel='dev' AND project_id='nysa' AND ticket_id='SF-3'`).Scan(&owners); err != nil {
		t.Fatal(err)
	}
	if owners != 1 {
		t.Fatalf("workflow owners=%d want=1", owners)
	}
	adopted, err := database.StartOrAdopt(ctx, ref, "dev/nysa/SF-3/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if adopted.WorkflowID != "dev/nysa/SF-3/planning" {
		t.Fatalf("workflow id=%q", adopted.WorkflowID)
	}
	var startEvents int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM events WHERE channel='dev' AND project_id='nysa' AND ticket_id='SF-3' AND trigger='start_or_adopt'`).Scan(&startEvents); err != nil {
		t.Fatal(err)
	}
	if startEvents != 1 {
		t.Fatalf("start events=%d want=1", startEvents)
	}
	if _, err := database.StartOrAdopt(ctx, ref, "dev/nysa/SF-3/other", domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("changed workflow identity error=%v", err)
	}
}

func TestStaleRunnerCannotTransition(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-4"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-4")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, "dev/nysa/SF-4/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	invalidated, err := database.InvalidateRunner(ctx, ref, started.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"})
	if !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale runner transition=%v", err)
	}
	if invalidated.RunnerEpoch == started.RunnerEpoch {
		t.Fatal("runner epoch did not advance")
	}
}

func TestCompletedPhaseDoesNotReplay(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-5"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-5")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, "dev/nysa/SF-5/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	first, err := database.RecordPhaseCompletion(ctx, ref, domain.PhasePlanning, 1, fence)
	if err != nil || !first {
		t.Fatalf("first completion=%v err=%v", first, err)
	}
	second, err := database.RecordPhaseCompletion(ctx, ref, domain.PhasePlanning, 1, fence)
	if err != nil || second {
		t.Fatalf("second completion=%v err=%v", second, err)
	}
}
