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
	var workflow contracts.WorkflowEngine = runtime
	if err := workflow.Start(ctx, contracts.StartRequest{Ticket: ref, TicketVersion: 1, WorkflowID: "dev/nysa/SF-engine/planning", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}}); err != nil {
		t.Fatal(err)
	}
	started, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	result, err := workflow.Transition(ctx, contracts.TransitionRequest{
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
	signal, err := workflow.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: result.TicketVersion, From: domain.StateVerifying, Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Attributes: map[string]string{"independent_intent_valid": "true", "prebuild_proof_valid": "true", "verification_checkpoint_committed": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	if signal.EventID == "" || signal.To != domain.StateBuilding {
		t.Fatalf("signal receipt=%+v", signal)
	}
	built, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	published, err := workflow.Transition(ctx, contracts.TransitionRequest{Ticket: ref, TicketVersion: built.Version, From: domain.StateBuilding, Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: built.RunnerEpoch}, Attributes: map[string]string{"proof_green": "true", "diff_valid": "true", "git_control_plane_valid": "true", "candidate_checkpoint_committed": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(published.Invalidated) != 4 || published.Invalidated[0] != "proof_result" {
		t.Fatalf("normative invalidations=%v", published.Invalidated)
	}
	if err := workflow.Recover(ctx, contracts.RecoveryRequest{Channel: domain.ChannelDev, LeaderEpoch: leader}); err != nil {
		t.Fatal(err)
	}
}

func TestControlDispositionInvalidatesRunnerAtomically(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "digest", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-engine")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/SF-control/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	result, err := runtime.Signal(ctx, contracts.SignalRequest{
		Ticket: ref, TicketVersion: started.Version, From: domain.StatePlanning, Trigger: "operator_pause_or_take",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Attributes: map[string]string{"operator_identity_authenticated": "true"},
	})
	if err != nil || result.To != domain.StateStopping {
		t.Fatalf("control result=%+v err=%v", result, err)
	}
	after, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.StateStopping || after.ResumeState != domain.StatePlanning || after.RunnerEpoch != started.RunnerEpoch+1 || after.Version != result.TicketVersion {
		t.Fatalf("after=%+v result=%+v", after, result)
	}
}
