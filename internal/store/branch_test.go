package store

import (
	"context"
	"errors"
	"path/filepath"
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
	first := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	stored, err := database.LoadOrStoreBranch(ctx, key, first)
	if err != nil || stored != first {
		t.Fatalf("stored=%q err=%v", stored, err)
	}
	otherProposal := "sf/dev/0123456789abcdef/0123456789abcdef-ffffffffffffffffffffffffffffffff"
	stored, err = database.LoadOrStoreBranch(ctx, key, otherProposal)
	if err != nil || stored != first {
		t.Fatalf("replay stored=%q err=%v", stored, err)
	}
	for _, invalid := range []struct{ key, branch string }{
		{"dev\x00nysa\x00missing", first},
		{"dev\x00nysa", first},
		{key, "sf/stable/wrong/channel"},
		{key, "sf/dev/../escape"},
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
	proposals := []string{
		"sf/dev/0123456789abcdef/0123456789abcdef-11111111111111111111111111111111",
		"sf/dev/0123456789abcdef/0123456789abcdef-22222222222222222222222222222222",
	}
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
