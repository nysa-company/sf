package engine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
)

type amendmentAuthorityCounts struct {
	budgets, requests, revisions, bindings int
}

func TestGenericExternalMergeObservationRejectedByEngine(t *testing.T) {
	runtime := &Engine{}
	_, err := runtime.Transition(t.Context(), contracts.TransitionRequest{Trigger: "external_merge_observed"})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("generic external merge observation err=%v, want evidence conflict", err)
	}
}

func snapshotAmendmentAuthorityCounts(t *testing.T, databasePath string, ref domain.TicketRef) amendmentAuthorityCounts {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	counts := amendmentAuthorityCounts{}
	for table, target := range map[string]*int{
		"ticket_budget_uses":              &counts.budgets,
		"verification_amendment_requests": &counts.requests,
		"verification_revisions":          &counts.revisions,
		"verification_result_bindings":    &counts.bindings,
	} {
		if err := database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM `+table+` WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

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

func TestGenericTransitionCannotForgeVerificationAmendmentOrReviewRepair(t *testing.T) {
	ctx := t.Context()
	databasePath := filepath.Join(t.TempDir(), "amendment.sqlite")
	database, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-engine-amendment"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "digest", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "amendment-engine")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	beforeTicket, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeEvents, err := database.Events(ctx, domain.ChannelDev, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	beforeRevisions, err := database.VerificationRevisions(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeCounts := snapshotAmendmentAuthorityCounts(t, databasePath, ref)
	for _, tc := range []struct {
		trigger string
		from    domain.State
		to      domain.State
	}{
		{trigger: "verification_amendment_requested", from: domain.StateBuilding, to: domain.StateVerifying},
		{trigger: "amendment_accepted", from: domain.StateVerifying, to: domain.StateBuilding},
		{trigger: "amendment_rejected", from: domain.StateVerifying, to: domain.StateBuilding},
		{trigger: "review_repair", from: domain.StateReviewing, to: domain.StateBuilding},
		{trigger: "review_repair", from: domain.StateReviewing, to: domain.StateVerifying},
	} {
		if _, err := runtime.Transition(ctx, contracts.TransitionRequest{Ticket: ref, TicketVersion: beforeTicket.Version, From: tc.from, Trigger: tc.trigger, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: beforeTicket.RunnerEpoch}, Attributes: map[string]string{"amendment_request_valid": "true", "fresh_reviewer": "true", "old_and_new_digest_bound": "true", "correction_available": "true", "original_verification_intent_current": "true"}, EventPayload: "{}"}); !errors.Is(err, store.ErrEvidenceConflict) {
			t.Fatalf("generic %s transition=%v, want evidence conflict", tc.trigger, err)
		}
		afterTicket, err := database.Ticket(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		afterEvents, err := database.Events(ctx, domain.ChannelDev, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		afterRevisions, err := database.VerificationRevisions(ctx, ref)
		if err != nil {
			t.Fatal(err)
		}
		afterCounts := snapshotAmendmentAuthorityCounts(t, databasePath, ref)
		if !reflect.DeepEqual(afterTicket, beforeTicket) || !reflect.DeepEqual(afterEvents, beforeEvents) || !reflect.DeepEqual(afterRevisions, beforeRevisions) || afterCounts != beforeCounts {
			t.Fatalf("generic %s mutated ticket, events, budget, or evidence", tc.trigger)
		}
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

func TestGenericSignalCannotForgeOperatorSourceResume(t *testing.T) {
	ctx := t.Context()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "source-resume.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-forged-source-resume"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "digest", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: domain.StatePaused, ResumeState: domain.StateBuilding}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-engine")
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	_, err = runtime.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: ticket.Version, From: domain.StatePaused, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Attributes: map[string]string{"operator_identity_authenticated": "true", "takeover_source_diff_valid": "true", "verification_files_unchanged": "true", "branch_remote_identity_exact": "true"}, EventPayload: `{"intent":"resume","change_kind":"source_changes"}`})
	if !errors.Is(err, store.ErrEvidenceConflict) {
		t.Fatalf("generic source resume=%v, want evidence conflict", err)
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
