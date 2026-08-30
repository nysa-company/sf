package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestLoadOrStoreBranchIsDurableAndTicketBound(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-branch"}
	if err := database.CreateTicket(ctx, ticket(ref, "branch-ticket")); err != nil {
		t.Fatal(err)
	}
	key := "dev\x00nysa\x00SF-branch"
	first := testAllocatedBranch(ref, strings.Repeat("01", 16))
	if loaded, err := database.LoadBranch(ctx, key); err != nil || loaded != "" {
		t.Fatalf("empty load=%q err=%v", loaded, err)
	}
	stored, err := database.LoadOrStoreBranch(ctx, key, first)
	if err != nil || stored != first {
		t.Fatalf("stored=%q err=%v", stored, err)
	}
	if loaded, err := database.LoadBranch(ctx, key); err != nil || loaded != first {
		t.Fatalf("durable load=%q err=%v", loaded, err)
	}
	otherProposal := testAllocatedBranch(ref, "ffffffffffffffffffffffffffffffff")
	stored, err = database.LoadOrStoreBranch(ctx, key, otherProposal)
	if err != nil || stored != first {
		t.Fatalf("replay stored=%q err=%v", stored, err)
	}
	for _, invalid := range []struct{ key, branch string }{
		{"dev\x00nysa\x00missing", first},
		{"dev\x00nysa", first},
		{key, "sf/stable/wrong/channel"},
		{key, "sf/dev/../escape"},
		{key, "sf/dev/" + branchDigestPart("wrong-project") + "/" + branchDigestPart(string(ref.Ticket)) + "-" + strings.Repeat("01", 16)},
		{key, "sf/dev/" + branchDigestPart(string(ref.Project)) + "/" + branchDigestPart(string(ref.Ticket)) + "-not-random"},
	} {
		if _, err := database.LoadOrStoreBranch(ctx, invalid.key, invalid.branch); err == nil {
			t.Fatalf("invalid allocation accepted: %+v", invalid)
		}
	}
}

func TestConcurrentBranchReplayChoosesExactlyOneDurableValue(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "branches.sqlite")
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
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-race-branch"}
	if err := first.CreateTicket(ctx, ticket(ref, "race-branch")); err != nil {
		t.Fatal(err)
	}
	proposals := []string{testAllocatedBranch(ref, strings.Repeat("1", 32)), testAllocatedBranch(ref, strings.Repeat("2", 32))}
	stores := []*Store{first, second}
	results := make(chan string, 2)
	errorsOut := make(chan error, 2)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for index := range stores {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			branch, err := stores[index].LoadOrStoreBranch(ctx, "dev\x00nysa\x00SF-race-branch", proposals[index])
			results <- branch
			errorsOut <- err
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		if err != nil && !errors.Is(err, ErrBranchConflict) {
			t.Fatal(err)
		}
	}
	var chosen string
	for result := range results {
		if result == "" {
			continue
		}
		if chosen == "" {
			chosen = result
		} else if result != chosen {
			t.Fatalf("two branches chosen: %q %q", chosen, result)
		}
	}
	if chosen == "" {
		t.Fatal("no durable branch was chosen")
	}
}

func TestLoadBranchFailsClosedOnMismatchedDurableAuthority(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-corrupt-branch"}
	if err := database.CreateTicket(ctx, ticket(ref, "corrupt-branch")); err != nil {
		t.Fatal(err)
	}
	key := "dev\x00nysa\x00SF-corrupt-branch"
	wrong := "sf/dev/" + branchDigestPart("another-project") + "/" + branchDigestPart(string(ref.Ticket)) + "-" + strings.Repeat("a", 32)
	if _, err := database.db.ExecContext(ctx, `INSERT INTO branch_allocations(authority_key, channel, project_id, ticket_id, branch_ref, created_at) VALUES (?, ?, ?, ?, ?, ?)`, key, ref.Channel, ref.Project, ref.Ticket, wrong, "2026-08-29T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.LoadBranch(ctx, key); !errors.Is(err, ErrBranchConflict) {
		t.Fatalf("corrupt branch load err=%v", err)
	}
}

func testAllocatedBranch(ref domain.TicketRef, suffix string) string {
	return "sf/" + string(ref.Channel) + "/" + branchDigestPart(string(ref.Project)) + "/" + branchDigestPart(string(ref.Ticket)) + "-" + suffix
}
