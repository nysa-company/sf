package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestTransitionAndInvalidateRunnerIsAtomicAndFenced(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
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
	oldFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	result, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping,
		ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: oldFence, EventPayload: `{"operator_uid":501}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if stopping.State != domain.StateStopping || stopping.ResumeState != domain.StatePlanning || stopping.Version != result.Version || stopping.RunnerEpoch != started.RunnerEpoch+1 {
		t.Fatalf("stopping=%+v result=%+v", stopping, result)
	}
	if _, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, Trigger: "process_and_effects_drained", Fence: oldFence, EventPayload: "{}"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old runner completed after control transition: %v", err)
	}
	events, err := database.Events(ctx, domain.ChannelDev, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := events[len(events)-1]; got.Trigger != "operator_pause_or_take" || got.From != domain.StatePlanning || got.To != domain.StateStopping || got.TicketVersion != stopping.Version {
		t.Fatalf("control event=%+v", got)
	}
}

func TestTransitionAndInvalidateRunnerRejectsPublicationAndNonControlPairs(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-publication-bypass"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-publication-bypass")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/SF-control-publication-bypass/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	for name, transition := range map[string]Transition{
		"building-to-publishing": {Ref: ref, ExpectedVersion: started.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence},
		"publishing-to-waiting":  {Ref: ref, ExpectedVersion: started.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence},
		"blocked-to-publishing":  {Ref: ref, ExpectedVersion: started.Version, From: domain.StateBlocked, To: domain.StatePublishing, Trigger: "operator_resume", Fence: fence},
		"pause-wrong-resume":     {Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StateBuilding, Trigger: "operator_pause_or_take", Fence: fence},
		"cancel-wrong-resume":    {Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateCancelling, ResumeState: domain.StatePlanning, Trigger: "operator_cancel", Fence: fence},
		"cancel-wrong-trigger":   {Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateCancelling, Trigger: "operator_pause_or_take", Fence: fence},
	} {
		if _, err := database.TransitionAndInvalidateRunner(ctx, transition); err == nil {
			t.Fatalf("%s control bypass was accepted", name)
		}
		after, err := database.Ticket(ctx, ref)
		if err != nil || after.State != started.State || after.Version != started.Version || after.RunnerEpoch != started.RunnerEpoch {
			t.Fatalf("%s mutated ticket=%+v err=%v", name, after, err)
		}
	}
}

func TestCompleteControlTransitionClosesPhaseAndReleasesCapacity(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-complete"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-complete-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := database.StartWithOwnership(ctx, ref, queued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}, "dev/nysa/SF-control-complete/planning", []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 2}}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	phase := PhaseAttempt{Ref: ref, Phase: domain.PhasePlanning, Attempt: 1, ExpectedVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, Provider: domain.ProviderIdentity{Provider: "cursor", Model: "model", Family: "family", Version: "1"}, WorktreeID: "worktree", BaseSHA: strings.Repeat("a", 40)}
	if err := database.StartPhaseAttempt(ctx, phase); err != nil {
		t.Fatal(err)
	}
	control, err := database.TransitionAndInvalidateRunner(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: phase.Fence, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	currentFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}
	completed, err := database.CompleteControlTransition(ctx, Transition{Ref: ref, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: currentFence, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	paused, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != domain.StatePaused || paused.ResumeState != domain.StatePlanning || paused.RunnerEpoch != stopping.RunnerEpoch || paused.Version != completed.Version {
		t.Fatalf("paused=%+v completed=%+v", paused, completed)
	}
	leases, err := database.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 0 {
		t.Fatalf("leases=%+v err=%v", leases, err)
	}
	attempts, err := database.PhaseAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "cancelled" || attempts[0].FinishedAt.IsZero() {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	var ownerState string
	if err := database.db.QueryRowContext(ctx, `SELECT state FROM workflow_owners WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ownerState); err != nil || ownerState != string(domain.StatePaused) {
		t.Fatalf("owner state=%q err=%v", ownerState, err)
	}
}

func providerControlFixture(t *testing.T) (*Store, context.Context, uint64, Ticket, ProviderAttemptClaim) {
	t.Helper()
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "provider-control")
	if err != nil {
		t.Fatal(err)
	}
	ticket := setupProviderTicket(t, db, ctx, "SF-provider-control", leader)
	ticket = providerState(t, db, ctx, ticket, leader, domain.StateBuilding)
	builder, _ := setupProviderPair(t, db, ctx)
	claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version,
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch},
		Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder),
		ConfigDigest: digest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return db, ctx, leader, ticket, claim
}

func stopProviderControl(t *testing.T, db *Store, ctx context.Context, leader uint64, ticket Ticket) (TransitionResult, Ticket) {
	t.Helper()
	result, err := db.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: ticket.State, To: domain.StateStopping,
		ResumeState: ticket.State, Trigger: "operator_pause_or_take",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, EventPayload: "{}",
	})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	return result, stopping
}

func TestCompleteControlTransitionPreservesActiveProviderClaim(t *testing.T) {
	db, ctx, leader, ticket, claim := providerControlFixture(t)
	control, stopping := stopProviderControl(t, db, ctx, leader, ticket)
	_, err := db.CompleteControlTransition(ctx, Transition{
		Ref: stopping.Ref, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: ticket.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}",
	})
	if !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("active provider claim was accepted: %v", err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].ID != claim.ID || attempts[0].State != "active" {
		t.Fatalf("active provider claim changed: %+v err=%v", attempts, err)
	}
	phases, err := db.PhaseAttempts(ctx, ticket.Ref)
	if err != nil || len(phases) != 1 || phases[0].State != "active" {
		t.Fatalf("active phase changed: %+v err=%v", phases, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 1 {
		t.Fatalf("provider lease changed: %+v err=%v", leases, err)
	}
}

func TestDrainedProviderControlInvalidationCancelsExactAttemptAndAdmitsRetry(t *testing.T) {
	db, ctx, leader, ticket, claim := providerControlFixture(t)
	oldFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: 4242, PGID: 4242, BootIdentity: "boot-provider-control", ProcessStartIdentity: "start-provider-control", Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	control, stopping := stopProviderControl(t, db, ctx, leader, ticket)
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(`{"schema":"sf.builder/v1","summary":"late output","changed_files":["main.go"],"commands":[["go","test","./..."]]}`), UsageTrusted: true, UsageUnits: 1}
	validation := phaseartifact.Validation{TicketType: ticket.Type}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, oldFence, raw, validation, time.Now().UTC()); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("revoked provider output was admitted: %v", err)
	}
	if err := db.RetireProviderAttemptAfterControlInvalidation(ctx, claim, proof(t, claim), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, oldFence, raw, validation, time.Now().UTC()); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("late completion for retired claim was admitted: %v", err)
	}
	var results int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE provider_attempt_id=?`, claim.ID).Scan(&results); err != nil || results != 0 {
		t.Fatalf("revoked output result count=%d err=%v", results, err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "cancelled" || attempts[0].Outcome != "cancelled" || attempts[0].UsageUnits != 0 {
		t.Fatalf("retired provider attempt=%+v err=%v", attempts, err)
	}
	phases, err := db.PhaseAttempts(ctx, ticket.Ref)
	if err != nil || len(phases) != 1 || phases[0].State != "cancelled" || phases[0].Outcome != "cancelled" {
		t.Fatalf("retired phase=%+v err=%v", phases, err)
	}
	active, err := db.ActiveProviderAttempts(ctx, ticket.Ref.Channel)
	if err != nil || len(active) != 0 {
		t.Fatalf("active provider attempts=%+v err=%v", active, err)
	}
	leases, err := db.Leases(ctx, ticket.Ref.Channel)
	if err != nil || len(leases) != 0 {
		t.Fatalf("provider lease survived retirement=%+v err=%v", leases, err)
	}
	pausedResult, err := db.CompleteControlTransition(ctx, Transition{
		Ref: stopping.Ref, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: ticket.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}",
	})
	if err != nil {
		t.Fatal(err)
	}
	paused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || paused.State != domain.StatePaused || paused.Version != pausedResult.Version {
		t.Fatalf("paused ticket=%+v result=%+v err=%v", paused, pausedResult, err)
	}
	if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateBuilding, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	resumed, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	openExactRuntimeAdmission(t, db, ticket.Ref)
	second, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version,
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch},
		Phase: claim.Phase, Role: claim.Role, Binding: claim.Binding,
		ConfigDigest: resumed.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
	}))
	if err != nil || second.Attempt != 2 {
		t.Fatalf("second attempt=%+v err=%v", second, err)
	}
}

func TestCompleteControlTransitionPreservesQuarantinedProviderClaim(t *testing.T) {
	db, ctx, leader, ticket, claim := providerControlFixture(t)
	oldFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	if err := db.QuarantineProviderAttempt(ctx, claim, ticket.Version, oldFence, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	control, stopping := stopProviderControl(t, db, ctx, leader, ticket)
	_, err := db.CompleteControlTransition(ctx, Transition{
		Ref: stopping.Ref, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: ticket.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}",
	})
	if !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("quarantined provider claim was accepted: %v", err)
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].ID != claim.ID || attempts[0].State != "quarantined" || attempts[0].Outcome != "undrained" {
		t.Fatalf("quarantined provider claim changed: %+v err=%v", attempts, err)
	}
	phases, err := db.PhaseAttempts(ctx, ticket.Ref)
	if err != nil || len(phases) != 1 || phases[0].State != "active" {
		t.Fatalf("quarantined phase changed: %+v err=%v", phases, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 1 {
		t.Fatalf("quarantined provider lease changed: %+v err=%v", leases, err)
	}
}

func TestCompleteControlTransitionAfterProviderFinalization(t *testing.T) {
	db, ctx, leader, ticket, claim := providerControlFixture(t)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "cancelled", "cancelled", 0, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	control, stopping := stopProviderControl(t, db, ctx, leader, ticket)
	completed, err := db.CompleteControlTransition(ctx, Transition{
		Ref: stopping.Ref, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: ticket.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}",
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Version == 0 {
		t.Fatal("control completion did not advance the ticket")
	}
	attempts, err := db.ProviderAttempts(ctx, ticket.Ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "cancelled" || attempts[0].Outcome != "cancelled" {
		t.Fatalf("finalized provider claim=%+v err=%v", attempts, err)
	}
	var launchState string
	if err := db.db.QueryRowContext(ctx, `SELECT launch_state FROM provider_attempts WHERE id=?`, claim.ID).Scan(&launchState); err != nil || launchState != "drained" {
		t.Fatalf("finalized launch state=%q err=%v", launchState, err)
	}
	leases, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leases) != 0 {
		t.Fatalf("finalized provider lease=%+v err=%v", leases, err)
	}
}

func TestCompleteControlTransitionWaitsForEffectReconciliation(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-effect"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-effect-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := database.StartWithOwnership(ctx, ref, queued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch}, "dev/nysa/SF-control-effect/planning", []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	prior := EffectFence{SemanticKey: "control-effect", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: prior.SemanticKey, Ref: ref, Kind: "branch_push", TicketVersion: prior.TicketVersion, Fence: prior.Fence, RequestDigest: "request"}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.ClaimEffect(ctx, prior)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	prior.Fence.ClaimEpoch = claim.Effect.ClaimEpoch
	_, err = database.TransitionAndInvalidateRunner(ctx, Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	current := EffectFence{SemanticKey: prior.SemanticKey, Ref: ref, TicketVersion: stopping.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}}
	transition := Transition{Ref: ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: current.Fence, EventPayload: "{}"}
	if _, err := database.CompleteControlTransition(ctx, transition); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("undrained completion error=%v", err)
	}
	if leases, err := database.Leases(ctx, domain.ChannelDev); err != nil || len(leases) != 1 {
		t.Fatalf("undrained control released leases=%+v err=%v", leases, err)
	}
	if _, err := database.ReconcileInvalidatedEffect(ctx, InvalidatedEffectObservation{Prior: EffectObservation{EffectFence: prior, Present: false}, Current: current}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteControlTransition(ctx, transition); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentControlTransitionInvalidatesRunnerOnce(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-race"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-race-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	before, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	request := Transition{
		Ref: ref, ExpectedVersion: before.Version, From: before.State, To: domain.StateCancelling,
		Trigger: "operator_cancel", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: before.RunnerEpoch}, EventPayload: "{}",
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := database.TransitionAndInvalidateRunner(ctx, request)
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	success, stale := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrStaleFence):
			stale++
		default:
			t.Fatalf("unexpected control race error: %v", err)
		}
	}
	if success != 1 || stale != 1 {
		t.Fatalf("success=%d stale=%d", success, stale)
	}
	after, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != domain.StateCancelling || after.Version != before.Version+1 || after.RunnerEpoch != before.RunnerEpoch+1 {
		t.Fatalf("after=%+v before=%+v", after, before)
	}
}

func TestTransitionAndInvalidateRunnerRejectsInvalidPayloadWithoutMutation(t *testing.T) {
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-control-invalid"}
	if err := database.CreateTicket(ctx, ticket(ref, "control-invalid-digest")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "control-daemon")
	if err != nil {
		t.Fatal(err)
	}
	before, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: before.Version, From: before.State, To: domain.StateCancelling,
		Trigger: "operator_cancel", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: before.RunnerEpoch}, EventPayload: "not-json",
	})
	if err == nil {
		t.Fatal("invalid event payload was accepted")
	}
	after, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != before.State || after.Version != before.Version || after.RunnerEpoch != before.RunnerEpoch {
		t.Fatalf("invalid transition mutated ticket: before=%+v after=%+v", before, after)
	}
}
