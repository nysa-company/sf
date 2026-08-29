package testkit

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestFakeClockSleepOnlyAdvancesWhenTold(t *testing.T) {
	clock := NewFakeClock(time.Unix(100, 0))
	done := make(chan error, 1)
	go func() { done <- clock.Sleep(context.Background(), time.Minute) }()
	select {
	case <-done:
		t.Fatal("fake sleep returned before the clock advanced")
	case <-time.After(10 * time.Millisecond):
	}
	clock.Advance(time.Minute)
	if err := <-done; err != nil {
		t.Fatalf("fake sleep: %v", err)
	}
}

func TestIDsAreDeterministicAndChannelReferencesRemainDistinct(t *testing.T) {
	ids := NewIDs(7)
	stable := ids.Ticket(domain.ChannelStable, "nysa")
	dev := domain.TicketRef{Channel: domain.ChannelDev, Project: stable.Project, Ticket: stable.Ticket}
	if stable.Ticket != "SF-00000001" {
		t.Fatalf("stable ticket = %q", stable.Ticket)
	}
	if stable.Ticket != dev.Ticket || stable.Channel == dev.Channel {
		t.Fatal("fixture should model same generated ID as distinct channel-scoped references")
	}
	if got := ids.BranchSuffix(); got != "00000009" {
		t.Fatalf("branch suffix = %q", got)
	}
}

func TestFakeGHSeparatesMutationFromResponseDeliveryAndBindsIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetResponse("pr_create", ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	identity := contracts.PullRequestIdentity{
		Repository:     contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"},
		HeadOwner:      "example",
		HeadRepository: "app",
		HeadRef:        "sf/dev/nysa/SF-00000001-abc123",
		HeadOID:        "0123456789012345678901234567890123456789",
		BaseRef:        "main",
	}
	if _, err := remote.CreateDraftPullRequest(context.Background(), identity, "title", "body", ""); err == nil {
		t.Fatal("expected dropped response")
	}
	if got := remote.MutationCount("pr_create"); got != 1 {
		t.Fatalf("mutation count = %d, want 1", got)
	}
	if got := remote.DeliveryCount("pr_create"); got != 0 {
		t.Fatalf("delivery count = %d, want 0", got)
	}
	identity.Number = 1
	found, ok, err := remote.FindPullRequest(context.Background(), identity)
	if err != nil || !ok || found.HeadOID != identity.HeadOID || !found.FactoryOwned {
		t.Fatalf("full identity lookup = %#v, found=%v, err=%v", found, ok, err)
	}
	wrongHead := identity
	wrongHead.HeadRepository = "fork"
	if _, ok, err := remote.FindPullRequest(context.Background(), wrongHead); err != nil || ok {
		t.Fatalf("fork identity unexpectedly adopted: found=%v err=%v", ok, err)
	}
}

func TestCrashControllerIsOneShot(t *testing.T) {
	crash := NewCrashController()
	crash.Arm(AfterRemoteMutationBeforeResp)
	if err := crash.Hit(AfterRemoteMutationBeforeResp); err == nil {
		t.Fatal("expected injected crash")
	}
	if err := crash.Hit(AfterRemoteMutationBeforeResp); err != nil {
		t.Fatalf("one-shot crash fired twice: %v", err)
	}
	if got := crash.Hits(); len(got) != 1 || got[0] != AfterRemoteMutationBeforeResp {
		t.Fatalf("hits = %#v", got)
	}
}
