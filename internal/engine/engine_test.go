package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
)

func TestTransitionUsesNormativeStateMachineAndFencedStore(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-engine"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "digest", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "test-daemon")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, "dev/nysa/SF-engine/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join("..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	spec, err := statemachine.Load(file)
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	result, err := runtime.Transition(ctx, contracts.TransitionRequest{
		Ticket: ref, TicketVersion: started.Version, From: domain.StatePlanning,
		Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch},
		Attributes: map[string]string{"typed_plan_valid": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.To != domain.StateVerifying || result.TicketVersion != started.Version+1 || result.EventID == "" {
		t.Fatalf("unexpected transition result: %+v", result)
	}
}
