package engine

import (
	"context"
	"errors"
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
	for _, tc := range []struct{ from, to domain.State }{{domain.StatePlanning, domain.StateVerifying}, {domain.StateVerifying, domain.StateBuilding}, {domain.StateBuilding, domain.StatePublishing}} {
		if _, err := runtime.Transition(ctx, contracts.TransitionRequest{Ticket: ref, TicketVersion: 1, From: tc.from, Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}, Attributes: map[string]string{"forged": "true"}}); !errors.Is(err, store.ErrEvidenceConflict) {
			t.Fatalf("generic %s->%s bypass=%v", tc.from, tc.to, err)
		}
	}
	var workflow contracts.WorkflowEngine = runtime
	if err := workflow.Start(ctx, contracts.StartRequest{Ticket: ref, TicketVersion: 1, WorkflowID: "dev/nysa/SF-engine/planning", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}}); err != nil {
		t.Fatal(err)
	}
	started, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = workflow.Transition(ctx, contracts.TransitionRequest{
		Ticket: ref, TicketVersion: started.Version, From: domain.StatePlanning,
		Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch},
		Attributes: map[string]string{"typed_plan_valid": "true"},
	})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("generic phase pass bypass=%v", err)
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

func TestPausedMergingSignalRequiresGuardedMergeResumeAuthority(t *testing.T) {
	ctx := t.Context()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "merge-resume.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-merge-resume"}
	if err := database.CreateTicket(ctx, store.Ticket{
		Ref: ref, SourceDigest: "digest", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded,
		State: domain.StatePaused, ResumeState: domain.StateMerging,
	}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "merge-resume-engine")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	paused, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Signal(ctx, contracts.SignalRequest{
		Ticket: ref, TicketVersion: paused.Version, From: domain.StatePaused, Trigger: "operator_resume",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch},
		Attributes: map[string]string{
			"operator_identity_authenticated": "true", "takeover_diff_none": "true",
			"branch_remote_identity_exact": "true", "prerequisites_green": "true",
		},
		EventPayload: `{}`,
	})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("unbound merging resume signal=%v", err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StatePaused || current.Version != paused.Version || current.ResumeState != domain.StateMerging {
		t.Fatalf("rejected engine signal mutated ticket=%+v err=%v", current, err)
	}
}

func TestPausedReconcilingSignalRequiresPostPublicationResumeAuthority(t *testing.T) {
	ctx := t.Context()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "reconcile-resume.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-reconcile-resume"}
	if err := database.CreateTicket(ctx, store.Ticket{
		Ref: ref, SourceDigest: "digest", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded,
		State: domain.StatePaused, ResumeState: domain.StateReconciling,
	}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "reconcile-resume-engine")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	paused, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runtime.Signal(ctx, contracts.SignalRequest{
		Ticket: ref, TicketVersion: paused.Version, From: domain.StatePaused, Trigger: "operator_retry",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch},
		Attributes: map[string]string{
			"operator_identity_authenticated": "true", "pause_reason_retryable": "true",
			"typed_prerequisites_satisfied": "true", "runner_epoch_current": "true",
		},
		EventPayload: `{}`,
	})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("unbound reconciling retry signal=%v", err)
	}
	current, err := database.Ticket(ctx, ref)
	if err != nil || current.State != domain.StatePaused || current.Version != paused.Version || current.ResumeState != domain.StateReconciling {
		t.Fatalf("rejected engine signal mutated ticket=%+v err=%v", current, err)
	}
}
