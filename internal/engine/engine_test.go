package engine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
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

func TestProviderExhaustionAndRetryUseTypedEngineRoundTrip(t *testing.T) {
	ctx := t.Context()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "provider-round-trip.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	projectConfig := config.DefaultProject("nysa", "/tmp/engine-provider")
	effective, err := config.Resolve(config.DefaultMachineLimits(), projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/engine-provider", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-engine-provider-round-trip"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "engine-provider-round-trip", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "engine-provider-round-trip")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/SF-engine-provider-round-trip/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	repository, worktree, base := "/tmp/engine-provider", "/tmp/engine-provider/worktree", strings.Repeat("a", 40)
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Path: worktree, Branch: "sf/dev/nysa/engine-provider", IdentityJSON: []byte(`{}`), BaseSHA: base, HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	planner, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: strings.Repeat("1", 32), Provider: domain.ProviderIdentity{Provider: "cursor", Model: "cursor-model", Family: "cursor-family", Version: "1"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: strings.Repeat("2", 32), Provider: domain.ProviderIdentity{Provider: "claude", Model: "claude-model", Family: "claude-family", Version: "1"}, BinaryDigest: strings.Repeat("d", 64), PolicyDigest: strings.Repeat("e", 64), FixtureDigest: strings.Repeat("f", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, planner.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	binding := contracts.RuntimeBinding{Identity: planner.Provider, BinaryDigest: planner.BinaryDigest, PolicyDigest: planner.PolicyDigest, FixtureDigest: planner.FixtureDigest, AuthDigest: strings.Repeat("d", 64), AuthMode: "test"}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	for range 2 {
		request := store.ProviderAttemptRequest{Ref: ref, ExpectedVersion: started.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: binding, ConfigDigest: started.ConfigDigest, Capacity: 1, At: time.Now().UTC(), Repository: repository, Worktree: worktree, WorktreeIdentity: "{}", BaseSHA: base, SupervisorKey: signer.PublicKey(), Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch, ExpectedVersion: started.Version, Prompt: "engine provider retry fixture", Repository: repository, Worktree: worktree, WorktreeIdentity: "{}", BaseSHA: base, AllowedPaths: []string{"."}, Provider: binding.Identity, AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}}
		claim, err := database.BeginProviderAttempt(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := database.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "engine-fixture", ProcessStartIdentity: "engine-fixture-start", Worktree: worktree}); err != nil {
			t.Fatal(err)
		}
		proof, err := signer.ProveDrained(contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest})
		if err != nil {
			t.Fatal(err)
		}
		if err := database.FinishProviderAttempt(ctx, claim, proof, started.Version, fence, "failed", "failed", 0, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	paused, err := runtime.SignalProviderExhausted(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: started.Version, From: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence})
	if err != nil || paused.To != domain.StatePaused {
		t.Fatalf("provider exhaustion result=%+v err=%v", paused, err)
	}
	retried, err := runtime.SignalProviderRetry(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: paused.TicketVersion, From: domain.StatePaused, Trigger: "operator_retry", Fence: fence})
	if err != nil || retried.To != domain.StatePlanning {
		t.Fatalf("provider retry result=%+v err=%v", retried, err)
	}
}

func TestEngineTypedProviderBlockAndRecoverPreservesPhase(t *testing.T) {
	ctx := t.Context()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "provider-block-recover.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-engine-provider-block"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "engine-provider-block", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "engine-provider-block")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/SF-engine-provider-block/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	blocked, err := runtime.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: started.Version, From: domain.StatePlanning, Trigger: "typed_blocker", Fence: fence, Attributes: map[string]string{"no_unreconciled_external_mutation": "true"}, EventPayload: `{"code":"host_repair_required"}`})
	if err != nil || blocked.To != domain.StateBlocked {
		t.Fatalf("typed block result=%+v err=%v", blocked, err)
	}
	recovered, err := runtime.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: blocked.TicketVersion, From: domain.StateBlocked, Trigger: "operator_recover", Fence: fence, Attributes: map[string]string{"operator_identity_authenticated": "true", "typed_prerequisites_satisfied": "true", "no_live_writer": "true", "runner_epoch_current": "true"}, EventPayload: `{"intent":"recover"}`})
	if err != nil || recovered.To != domain.StatePlanning {
		t.Fatalf("typed recovery result=%+v err=%v", recovered, err)
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

func TestGenericSignalCannotMintProviderExhaustionOrRetry(t *testing.T) {
	ctx := t.Context()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "provider-generic.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "provider-generic-engine")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(database, spec)
	for _, tc := range []struct {
		id            string
		state, resume domain.State
		trigger       string
	}{
		{"exhaust", domain.StatePlanning, "", "retry_or_correction_exhausted"},
		{"retry", domain.StatePaused, domain.StatePlanning, "operator_retry"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID("SF-provider-" + tc.id)}
			if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: tc.id, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, State: tc.state, ResumeState: tc.resume}); err != nil {
				t.Fatal(err)
			}
			ticket, err := database.Ticket(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: ticket.Version, From: ticket.State, Trigger: tc.trigger, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Attributes: map[string]string{"operator_identity_authenticated": "true", "pause_reason_retryable": "true", "typed_prerequisites_satisfied": "true", "runner_epoch_current": "true"}, EventPayload: `{}`}); !errors.Is(err, store.ErrEvidenceConflict) {
				t.Fatalf("generic %s=%v", tc.id, err)
			}
		})
	}
}
