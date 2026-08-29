package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestSubmitPersistsImmutableSourceAndRequiresNewAfterTerminal(t *testing.T) {
	database, ctx := openTestStore(t)
	source := []byte("# Fix reminders\n\nDuplicates occur.\n\n## Acceptance\n- One reminder\n")
	sum := sha256.Sum256(source)
	digest := fmt.Sprintf("%x", sum[:])
	acceptance := []string{"One reminder"}
	firstRef := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-source-1"}
	input := Ticket{
		Ref: firstRef, SourceDigest: digest, Type: domain.TicketBug, MergeMode: domain.MergeGuarded,
		Title: "Fix reminders", Problem: "Duplicates occur.", Acceptance: acceptance,
		Source: source, Priority: "high", MaxDuration: 90 * time.Minute, MaxCostMicroUSD: 20_125_000,
	}
	created, wasCreated, err := database.SubmitTicket(ctx, input, false)
	if err != nil || !wasCreated || created.Ref != firstRef {
		t.Fatalf("created=%+v wasCreated=%v err=%v", created, wasCreated, err)
	}
	source[0] = 'X'
	acceptance[0] = "mutated"
	loaded, err := database.Ticket(ctx, firstRef)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.Source) == string(source) || !reflect.DeepEqual(loaded.Acceptance, []string{"One reminder"}) || loaded.Title != "Fix reminders" || loaded.Priority != "high" || loaded.MaxDuration != 90*time.Minute || loaded.MaxCostMicroUSD != 20_125_000 {
		t.Fatalf("immutable ticket changed: %+v source=%q", loaded, loaded.Source)
	}

	duplicate := input
	duplicate.Ref.Ticket = "SF-source-duplicate"
	duplicate.Source = loaded.Source
	duplicate.Acceptance = loaded.Acceptance
	existing, wasCreated, err := database.SubmitTicket(ctx, duplicate, false)
	if err != nil || wasCreated || existing.Ref != firstRef {
		t.Fatalf("active duplicate=%+v wasCreated=%v err=%v", existing, wasCreated, err)
	}

	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-submit")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Transition(ctx, Transition{
		Ref: firstRef, ExpectedVersion: loaded.Version, From: domain.StateQueued, To: domain.StateDone,
		Trigger: "test_terminal", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: loaded.RunnerEpoch}, EventPayload: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	terminalReplay := duplicate
	terminalReplay.Ref.Ticket = "SF-source-2"
	if _, _, err := database.SubmitTicket(ctx, terminalReplay, false); !errors.Is(err, ErrTerminalReplay) {
		t.Fatalf("terminal replay error=%v", err)
	}
	second, wasCreated, err := database.SubmitTicket(ctx, terminalReplay, true)
	if err != nil || !wasCreated || second.Ref.Ticket != "SF-source-2" {
		t.Fatalf("new terminal replay=%+v wasCreated=%v err=%v", second, wasCreated, err)
	}
	tickets, err := database.Tickets(ctx, domain.ChannelDev, "nysa", 10)
	if err != nil || len(tickets) != 2 {
		t.Fatalf("tickets=%d err=%v", len(tickets), err)
	}
	events, err := database.Events(ctx, domain.ChannelDev, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 || events[0].Trigger != "ticket_submitted" {
		t.Fatalf("events=%+v", events)
	}
	for index := 1; index < len(events); index++ {
		if events[index].ID <= events[index-1].ID {
			t.Fatalf("events out of order: %+v", events)
		}
	}
}

func TestSubmitRejectsSourceDigestMismatch(t *testing.T) {
	database, ctx := openTestStore(t)
	input := Ticket{
		Ref:          domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-bad-source"},
		SourceDigest: "not-the-digest", Source: []byte("ticket"), Type: domain.TicketBug, MergeMode: domain.MergeGuarded,
	}
	if _, _, err := database.SubmitTicket(ctx, input, false); err == nil {
		t.Fatal("mismatched immutable source accepted")
	}
	input.Source = nil
	if _, _, err := database.SubmitTicket(ctx, input, false); err == nil {
		t.Fatal("missing immutable source accepted")
	}
	input.Source = []byte("ticket")
	input.SourceDigest = strings.Repeat("A", 64)
	if _, _, err := database.SubmitTicket(ctx, input, false); err == nil {
		t.Fatal("non-canonical source digest accepted")
	}
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

func TestTicketIdentityIsChannelUniqueAndProjectLookupIsDurable(t *testing.T) {
	database, ctx := openTestStore(t)
	if err := database.CreateProject(ctx, Project{Channel: domain.ChannelDev, ID: "other", Path: "/tmp/other", BaseRef: "trunk"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-one-id"}
	if err := database.CreateTicket(ctx, ticket(ref, "identity-a")); err != nil {
		t.Fatal(err)
	}
	duplicate := domain.TicketRef{Channel: domain.ChannelDev, Project: "other", Ticket: ref.Ticket}
	if err := database.CreateTicket(ctx, ticket(duplicate, "identity-b")); err == nil {
		t.Fatal("same channel ticket ID was accepted for a second project")
	}
	loaded, err := database.TicketByID(ctx, domain.ChannelDev, ref.Ticket)
	if err != nil || loaded.Ref != ref {
		t.Fatalf("ticket=%+v err=%v", loaded, err)
	}
	project, err := database.Project(ctx, domain.ChannelDev, "other")
	if err != nil || project.Path != "/tmp/other" || project.BaseRef != "trunk" {
		t.Fatalf("project=%+v err=%v", project, err)
	}
	projects, err := database.Projects(ctx, domain.ChannelDev)
	if err != nil || len(projects) != 2 || projects[0].ID != "nysa" || projects[1].ID != "other" {
		t.Fatalf("projects=%+v err=%v", projects, err)
	}
}

func TestRegisterProjectIsExactIdempotentAndSnapshotsOnStart(t *testing.T) {
	database, ctx := openTestStore(t)
	snapshot := []byte(`{"name":"configured"}`)
	digestBytes := sha256.Sum256(snapshot)
	project := Project{
		Channel: domain.ChannelDev, ID: "configured", Path: "/tmp/configured", BaseRef: "main",
		ConfigGeneration: 1, ConfigDigest: fmt.Sprintf("%x", digestBytes[:]), ConfigSnapshot: snapshot,
	}
	created, err := database.RegisterProject(ctx, project)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	created, err = database.RegisterProject(ctx, project)
	if err != nil || created {
		t.Fatalf("idempotent created=%v err=%v", created, err)
	}

	conflict := project
	conflict.BaseRef = "trunk"
	if _, err := database.RegisterProject(ctx, conflict); !errors.Is(err, ErrProjectConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	badDigest := project
	badDigest.ID = "bad-digest"
	badDigest.ConfigDigest = strings.Repeat("0", 64)
	if _, err := database.RegisterProject(ctx, badDigest); err == nil {
		t.Fatal("mismatched project snapshot digest was accepted")
	}

	loadedProject, err := database.Project(ctx, domain.ChannelDev, project.ID)
	if err != nil || loadedProject.ConfigGeneration != 1 || loadedProject.ConfigDigest != project.ConfigDigest || !bytes.Equal(loadedProject.ConfigSnapshot, snapshot) {
		t.Fatalf("project=%+v err=%v", loadedProject, err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: project.ID, Ticket: "SF-config-snapshot"}
	if err := database.CreateTicket(ctx, ticket(ref, "config-snapshot")); err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil || queued.ConfigGeneration != 0 || len(queued.ConfigSnapshot) != 0 {
		t.Fatalf("queued=%+v err=%v", queued, err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "config-daemon")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "sf/dev/configured/SF-config-snapshot", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if started.ConfigGeneration != 1 || started.ConfigDigest != project.ConfigDigest || !bytes.Equal(started.ConfigSnapshot, snapshot) {
		t.Fatalf("started config=%+v", started)
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

func TestInterruptedV2MigrationLeavesV1SchemaUntouched(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v1.sqlite")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA foreign_keys=ON; CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range migrationV1 {
		if _, err := raw.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (1, 'now'); INSERT INTO plans(channel, project_id, ticket_id, digest, body) VALUES ('dev', 'missing', 'ticket', 'digest', 'body')`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, path); err == nil {
		t.Fatal("migration with invalid legacy artifact succeeded")
	}
	raw, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var migrations int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migrations); err != nil || migrations != 1 {
		t.Fatalf("migration rows=%d err=%v", migrations, err)
	}
	var oldPlans, partialPlans int
	if err := raw.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='plans'`).Scan(&oldPlans); err != nil {
		t.Fatal(err)
	}
	if err := raw.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='plans_v2'`).Scan(&partialPlans); err != nil {
		t.Fatal(err)
	}
	if oldPlans != 1 || partialPlans != 0 {
		t.Fatalf("migration rollback plans=%d partial=%d", oldPlans, partialPlans)
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

func TestCommitFailureRollsBackBeforePoolReuse(t *testing.T) {
	database, ctx := openTestStore(t)
	injected := errors.New("injected commit failure")
	database.commit = func(context.Context, *sql.Conn) error { return injected }
	err := database.CreateProject(ctx, Project{Channel: domain.ChannelDev, ID: "commit-fail", Path: "/tmp/commit-fail", BaseRef: "main"})
	if !errors.Is(err, injected) {
		t.Fatalf("commit error=%v", err)
	}
	database.commit = commitTransaction
	if err := database.CreateProject(ctx, Project{Channel: domain.ChannelDev, ID: "after-rollback", Path: "/tmp/after-rollback", BaseRef: "main"}); err != nil {
		t.Fatalf("pooled connection retained failed transaction: %v", err)
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
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-3/planning", fence)
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
	adopted, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-3/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if adopted.WorkflowID != "dev/nysa/SF-3/planning" {
		t.Fatalf("workflow id=%q", adopted.WorkflowID)
	}
	if _, err := database.StartOrAdopt(ctx, ref, started.Version, "dev/nysa/SF-3/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("start with post-transition version=%v", err)
	}
	var startEvents int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM events WHERE channel='dev' AND project_id='nysa' AND ticket_id='SF-3' AND trigger='start_or_adopt'`).Scan(&startEvents); err != nil {
		t.Fatal(err)
	}
	if startEvents != 1 {
		t.Fatalf("start events=%d want=1", startEvents)
	}
	if _, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-3/other", domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}); !errors.Is(err, ErrStaleFence) {
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
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-4/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
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
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-5/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	first, err := database.RecordPhaseCompletion(ctx, ref, domain.PhasePlanning, 1, started.Version, fence)
	if err != nil || !first {
		t.Fatalf("first completion=%v err=%v", first, err)
	}
	second, err := database.RecordPhaseCompletion(ctx, ref, domain.PhasePlanning, 1, started.Version, fence)
	if err != nil || second {
		t.Fatalf("second completion=%v err=%v", second, err)
	}
}

func TestLatePhaseCompletionIsFencedByTicketVersion(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-6"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-6")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-6/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.RecordPhaseCompletion(ctx, ref, domain.PhasePlanning, 1, started.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch})
	if !errors.Is(err, ErrStaleFence) || advanced.Version == started.Version {
		t.Fatalf("late phase completion=%v", err)
	}
}

func TestEffectClaimsReconcileAndFenceLateResponse(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-7"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-7")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-7/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	base := EffectFence{SemanticKey: "dev/nysa/SF-7/publish/1/branch_push/main", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: base.SemanticKey, Ref: ref, Kind: "branch_push", TicketVersion: base.TicketVersion, Fence: base.Fence, RequestDigest: "request-1"}); err != nil {
		t.Fatal(err)
	}
	claims := make(chan EffectClaim, 2)
	errs := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			claim, err := database.ClaimEffect(ctx, base)
			claims <- claim
			errs <- err
		}()
	}
	close(start)
	var claimed EffectClaim
	for range 2 {
		claim := <-claims
		err := <-errs
		if err == nil && claim.Claimed {
			claimed = claim
		} else if !errors.Is(err, ErrEffectBusy) {
			t.Fatalf("claim error=%v", err)
		}
	}
	if !claimed.Claimed || claimed.Effect.ClaimEpoch != 1 {
		t.Fatalf("claim=%+v", claimed)
	}
	oldFence := base
	oldFence.Fence.ClaimEpoch = claimed.Effect.ClaimEpoch
	newLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-b")
	if err != nil {
		t.Fatal(err)
	}
	uncertain, err := database.ReconcileEffects(ctx, domain.ChannelDev, newLeader)
	if err != nil || len(uncertain) != 1 || uncertain[0].State != EffectUncertain || uncertain[0].ClaimEpoch <= oldFence.Fence.ClaimEpoch {
		t.Fatalf("reconcile=%+v err=%v", uncertain, err)
	}
	if _, err := database.ConfirmEffect(ctx, oldFence, "late-response"); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("late response=%v", err)
	}
	recovered := EffectFence{SemanticKey: base.SemanticKey, Ref: ref, TicketVersion: uncertain[0].TicketVersion, Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: uncertain[0].RunnerEpoch, ClaimEpoch: uncertain[0].ClaimEpoch}}
	confirmed, err := database.ConfirmEffect(ctx, recovered, "remote-main@abc")
	if err != nil || confirmed.State != EffectConfirmed || confirmed.ObservedIdentity != "remote-main@abc" {
		t.Fatalf("confirm=%+v err=%v", confirmed, err)
	}
}

func TestEffectSemanticKeyCannotBeReplannedDifferently(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-8"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-8")); err != nil {
		t.Fatal(err)
	}
	leader, _ := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-8/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	plan := EffectPlan{SemanticKey: "key-8", Ref: ref, Kind: "worktree_create", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "one"}
	if _, err := database.PlanEffect(ctx, plan); err != nil {
		t.Fatal(err)
	}
	plan.RequestDigest = "two"
	if _, err := database.PlanEffect(ctx, plan); !errors.Is(err, ErrEffectKey) {
		t.Fatalf("semantic conflict=%v", err)
	}
}

func TestStaleEffectObservationRetainsOriginalTicketIdentity(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-8-stale"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-8-stale")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/nysa/SF-8-stale/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	base := EffectFence{SemanticKey: "key-8-stale", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: base.SemanticKey, Ref: ref, Kind: "branch_push", TicketVersion: started.Version, Fence: base.Fence, RequestDigest: "request"}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.ClaimEffect(ctx, base)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	if _, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: base.Fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	newLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-b")
	if err != nil {
		t.Fatal(err)
	}
	uncertain, err := database.ReconcileEffects(ctx, domain.ChannelDev, newLeader)
	if err != nil || len(uncertain) != 1 || uncertain[0].TicketVersion != started.Version || uncertain[0].RunnerEpoch != started.RunnerEpoch {
		t.Fatalf("recovery rewrote effect identity: %+v err=%v", uncertain, err)
	}
	recovered := EffectFence{SemanticKey: base.SemanticKey, Ref: ref, TicketVersion: uncertain[0].TicketVersion, Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: uncertain[0].RunnerEpoch, ClaimEpoch: uncertain[0].ClaimEpoch}}
	if _, err := database.ConfirmEffect(ctx, recovered, "remote@abc"); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale confirmation=%v", err)
	}
	evidence, err := database.RecordStaleObservation(ctx, EffectObservation{EffectFence: recovered, Present: true, Identity: "remote@abc"})
	if !errors.Is(err, ErrStaleObservation) || evidence.State != EffectUncertain || evidence.ObservedIdentity != "remote@abc" {
		t.Fatalf("stale evidence=%+v err=%v", evidence, err)
	}
	if _, err := database.ConfirmEffect(ctx, recovered, ""); err == nil {
		t.Fatal("empty confirmation identity succeeded")
	}
}

func TestCurrentApprovalDecisionIsMutuallyExclusive(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-approval"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-approval")); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO approvals(channel, project_id, ticket_id, reviewed_head, operator_uid, decision, created_at) VALUES ('dev', 'nysa', 'SF-approval', 'head', 501, 'approved', 'now')`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO approvals(channel, project_id, ticket_id, reviewed_head, operator_uid, decision, created_at) VALUES ('dev', 'nysa', 'SF-approval', 'head', 501, 'rejected', 'now')`); err == nil {
		t.Fatal("conflicting current approval decision succeeded")
	}
}

func TestRecoveryBlockWritesEventAndSchemaGuardsStartup(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-9"}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-9")); err != nil {
		t.Fatal(err)
	}
	leader, _ := database.AcquireLeader(ctx, domain.ChannelDev, "daemon-a")
	if _, err := database.db.Exec(`UPDATE tickets SET state='planning' WHERE channel='dev' AND project_id='nysa' AND id='SF-9'`); err != nil {
		t.Fatal(err)
	}
	if err := database.ReconcileOrphans(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	blocked, err := database.Ticket(ctx, ref)
	if err != nil || blocked.State != domain.StateBlocked || blocked.BlockedCode != "workflow_ownership_unknown" {
		t.Fatalf("blocked=%+v err=%v", blocked, err)
	}
	if _, err := database.StartOrAdopt(ctx, ref, blocked.Version, "dev/nysa/SF-9/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: blocked.RunnerEpoch}); !errors.Is(err, ErrBlocked) {
		t.Fatalf("blocked start=%v", err)
	}
	var events int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM events WHERE ticket_id='SF-9' AND trigger='workflow_ownership_unknown'`).Scan(&events); err != nil || events != 1 {
		t.Fatalf("recovery events=%d err=%v", events, err)
	}

	corrupt := filepath.Join(t.TempDir(), "corrupt.sqlite")
	if err := os.WriteFile(corrupt, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, corrupt); err == nil {
		t.Fatal("corrupt sqlite opened")
	}
	missing := filepath.Join(t.TempDir(), "missing.sqlite")
	raw, err := sql.Open("sqlite", missing)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL, checksum TEXT NOT NULL); INSERT INTO schema_migrations VALUES (1, 'now', '` + migrationChecksums[1] + `'); INSERT INTO schema_migrations VALUES (2, 'now', '` + migrationChecksums[2] + `'); INSERT INTO schema_migrations VALUES (3, 'now', '` + migrationChecksums[3] + `'); INSERT INTO schema_migrations VALUES (4, 'now', '` + migrationChecksums[4] + `'); INSERT INTO schema_migrations VALUES (5, 'now', '` + migrationChecksums[5] + `'); INSERT INTO schema_migrations VALUES (6, 'now', '` + migrationChecksums[6] + `')`); err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	if _, err := Open(ctx, missing); err == nil {
		t.Fatal("missing schema shape opened")
	}
}

func TestOnlineBackupCanBeOpened(t *testing.T) {
	database, ctx := openTestStore(t)
	path := filepath.Join(t.TempDir(), "backup.sqlite")
	if err := database.Backup(ctx, path); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
}
