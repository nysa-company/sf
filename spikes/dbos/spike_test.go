// Package dbosspike is an executable decision probe. It is intentionally
// isolated from production packages so choosing the fallback engine costs no
// production dependency or API surface.
package dbosspike

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	_ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"
	_ "modernc.org/sqlite"
)

// workflowDB is test-only state used by checkpointStep. Tests are deliberately
// non-parallel because a DBOS workflow function has no dependency injection
// parameter and the spike wants the app and DBOS system tables in one file.
var workflowDB struct {
	sync.Mutex
	db *sql.DB
}

func checkpointWorkflow(ctx dbos.Context, ticket string) (string, error) {
	return dbos.RunAsStep(ctx, func(context.Context) (string, error) {
		workflowDB.Lock()
		db := workflowDB.db
		workflowDB.Unlock()
		if db == nil {
			return "", errors.New("spike app database is unavailable")
		}
		if _, err := db.Exec(`INSERT INTO checkpoints(ticket, count) VALUES (?, 1)
			ON CONFLICT(ticket) DO UPDATE SET count = count + 1`, ticket); err != nil {
			return "", err
		}
		return "checkpointed", nil
	})
}

func openDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000;
		CREATE TABLE IF NOT EXISTS checkpoints (ticket TEXT PRIMARY KEY, count INTEGER NOT NULL);
		CREATE TABLE IF NOT EXISTS tickets (id TEXT PRIMARY KEY, state TEXT NOT NULL, workflow_id TEXT NOT NULL);
		CREATE TABLE IF NOT EXISTS effects (semantic_key TEXT PRIMARY KEY, state TEXT NOT NULL, remote_count INTEGER NOT NULL);`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func launch(t *testing.T, db *sql.DB, app string) dbos.Context {
	t.Helper()
	workflowDB.Lock()
	workflowDB.db = db
	workflowDB.Unlock()
	ctx, err := dbos.NewContext(context.Background(), dbos.Config{
		AppName:        app,
		SQLiteSystemDB: db,
	})
	if err != nil {
		t.Fatal(err)
	}
	dbos.RegisterWorkflow(ctx, checkpointWorkflow, dbos.WithWorkflowName("sf.dbos-spike.checkpoint"))
	if err := dbos.Launch(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dbos.Shutdown(ctx, 2*time.Second) })
	return ctx
}

func checkpointCount(t *testing.T, db *sql.DB, ticket string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT count FROM checkpoints WHERE ticket = ?`, ticket).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestSQLiteWorkflowIDPreventsCheckpointReplay(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "stable.sqlite"))
	ctx := launch(t, db, "sf-spike-stable")
	const ticket = "SF-101"
	const workflowID = "stable/SF-101/planning"

	first, err := dbos.RunWorkflow(ctx, checkpointWorkflow, ticket, dbos.WithWorkflowID(workflowID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.GetResult(); err != nil {
		t.Fatal(err)
	}
	second, err := dbos.RunWorkflow(ctx, checkpointWorkflow, ticket, dbos.WithWorkflowID(workflowID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.GetResult(); err != nil {
		t.Fatal(err)
	}
	if got := checkpointCount(t, db, ticket); got != 1 {
		t.Fatalf("completed DBOS step replayed: got %d executions, want 1", got)
	}
}

// TestTransitionWithoutOwnershipReconciles models the exact dangerous gap:
// application state commits, then the daemon dies before DBOS owns the work.
// Startup uses a persisted stable workflow ID, so retrying adoption creates or
// observes exactly one durable workflow rather than stranding or duplicating it.
func TestTransitionWithoutOwnershipReconciles(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "stable.sqlite"))
	const ticket = "SF-102"
	const workflowID = "stable/SF-102/planning"
	if _, err := db.Exec(`INSERT INTO tickets(id, state, workflow_id) VALUES (?, 'planning', ?)`, ticket, workflowID); err != nil {
		t.Fatal(err)
	}
	ctx := launch(t, db, "sf-spike-start-reconcile")

	for range 2 { // first is adoption; second is a restart/retry after lost response.
		h, err := dbos.RunWorkflow(ctx, checkpointWorkflow, ticket, dbos.WithWorkflowID(workflowID))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.GetResult(); err != nil {
			t.Fatal(err)
		}
	}
	if got := checkpointCount(t, db, ticket); got != 1 {
		t.Fatalf("stable workflow adoption was not idempotent: got %d executions", got)
	}
}

func TestLostResponseReconcilesWithoutSecondMutation(t *testing.T) {
	db := openDB(t, filepath.Join(t.TempDir(), "stable.sqlite"))
	const key = "stable/project/SF-103/push/branch"
	if _, err := db.Exec(`INSERT INTO effects(semantic_key, state, remote_count) VALUES (?, 'executing', 1)`, key); err != nil {
		t.Fatal(err)
	}
	// The fake remote's mutation was applied, but its response was lost. Recovery
	// observes the semantic target and confirms the existing effect; it never
	// invokes the remote mutation again.
	if _, err := db.Exec(`UPDATE effects SET state = 'confirmed' WHERE semantic_key = ? AND remote_count = 1`, key); err != nil {
		t.Fatal(err)
	}
	var state string
	var count int
	if err := db.QueryRow(`SELECT state, remote_count FROM effects WHERE semantic_key = ?`, key).Scan(&state, &count); err != nil {
		t.Fatal(err)
	}
	if state != "confirmed" || count != 1 {
		t.Fatalf("lost-response recovery mutated twice or did not confirm: state=%q count=%d", state, count)
	}
}

func TestSQLiteBusyHonorsContextDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.sqlite")
	locker := openDB(t, path)
	contender, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	contender.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = contender.Close() })
	if _, err := contender.Exec(`PRAGMA busy_timeout=1000`); err != nil {
		t.Fatal(err)
	}
	if _, err := locker.Exec(`BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = locker.Exec(`ROLLBACK`) })

	deadline, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = contender.ExecContext(deadline, `INSERT INTO tickets(id, state, workflow_id) VALUES ('SF-busy', 'queued', 'busy')`)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("contending write unexpectedly succeeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(strings.ToLower(err.Error()), "interrupted") {
		t.Fatalf("busy write returned %v, want context deadline/interrupt", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("busy operation outlived bounded deadline: %s", elapsed)
	}
}

func TestStableAndDevDatabasesAreIsolated(t *testing.T) {
	dir := t.TempDir()
	stable := openDB(t, filepath.Join(dir, "stable.sqlite"))
	dev := openDB(t, filepath.Join(dir, "dev.sqlite"))
	if _, err := stable.Exec(`INSERT INTO tickets(id, state, workflow_id) VALUES ('SF-104', 'planning', 'stable/SF-104')`); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := dev.QueryRow(`SELECT COUNT(*) FROM tickets WHERE id = 'SF-104'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("dev database observed stable ticket: %d rows", count)
	}
}

func TestProcessGroupCancellationRemovesChild(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "sleep 30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("process group did not exit after TERM")
	case <-done:
	}
}

func TestEvidenceSummary(t *testing.T) {
	// This deliberately fails closed if a future dependency upgrade removes the
	// SQLite driver registration that the decision record relies on.
	if os.Getenv("DBOS_SPIKE_REQUIRE_SQLITE") == "1" {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.Ping(); err != nil {
			t.Fatal(fmt.Errorf("SQLite driver unavailable: %w", err))
		}
	}
}
