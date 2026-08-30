package store

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
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
