package daemon

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestPauseDrainsQuestionPausedTicketBeforeReleasingCapacity(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-question-capacity")
	ctx := context.Background()
	paused, err := d.store.Transition(ctx, store.Transition{Ref: started.Ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "needs_operator_input", Fence: domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	ready, drains := false, 0
	d.control = testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) {
		drains++
		return ready, nil
	}}
	if response := daemonControl(d, started.Ref.Ticket, "pause"); response.OK {
		t.Fatal("already-paused ticket reported success without joining its runtime")
	}
	leases, err := d.store.Leases(ctx, d.channel)
	if err != nil || len(leases) != 2 {
		t.Fatalf("undrained capacity was released: %+v %v", leases, err)
	}
	ready = true
	if response := daemonControl(d, started.Ref.Ticket, "pause"); !response.OK {
		t.Fatalf("drained paused ticket: %+v", response)
	}
	leases, err = d.store.Leases(ctx, d.channel)
	if err != nil || len(leases) != 0 {
		t.Fatalf("drained capacity retained: %+v %v", leases, err)
	}
	current, err := d.store.Ticket(ctx, started.Ref)
	if err != nil || current.State != domain.StatePaused || current.Version != paused.Version || current.RunnerEpoch != started.RunnerEpoch {
		t.Fatalf("pause rewrote semantic question state: %+v %v", current, err)
	}
	if response := daemonControl(d, started.Ref.Ticket, "pause"); !response.OK || drains != 2 {
		t.Fatalf("release replay was not idempotent: %+v drains=%d", response, drains)
	}
}

func TestPauseQuestionCapacityRefusesOutstandingEffect(t *testing.T) {
	d, _, _ := testDaemon(t)
	started := createAndStartControlTicket(t, d, "SF-question-effect")
	ctx := context.Background()
	fence := domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: started.RunnerEpoch}
	key := "question-capacity-outstanding"
	if _, err := d.store.PlanEffect(ctx, store.EffectPlan{SemanticKey: key, Ref: started.Ref, Kind: "fixture", TicketVersion: started.Version, Fence: fence, RequestDigest: "request"}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.store.ClaimEffect(ctx, store.EffectFence{SemanticKey: key, Ref: started.Ref, TicketVersion: started.Version, Fence: fence}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.store.Transition(ctx, store.Transition{Ref: started.Ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "needs_operator_input", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	d.control = testRuntimeController{drain: func(context.Context, domain.TicketRef) (bool, error) { return true, nil }}
	if response := daemonControl(d, started.Ref.Ticket, "pause"); response.OK {
		t.Fatal("runtime claim hid an outstanding durable effect")
	}
	leases, err := d.store.Leases(ctx, d.channel)
	if err != nil || len(leases) != 2 {
		t.Fatalf("outstanding effect lost capacity protection: %+v %v", leases, err)
	}
}
