package workflowruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestOperatorDecisionActivityConsumesOnlyExactPendingAdmission(t *testing.T) {
	database, ref, leader, started := runtimeControlStore(t)
	if err := database.SealRuntimeControl(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	stopped, err := database.StoppedRuntimeTicket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Transition(t.Context(), store.Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_rearm", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	current, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := database.RearmProof(t.Context(), ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	scheduler := NewScheduler(domain.ChannelDev, StoreTicketSource{Store: database}, &fakeEnsure{}, &fakeWorker{})
	runtime, err := NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bundle := runtime.ControlBundle()
	if err := bundle.Drain(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateRearm(t.Context(), capability, bundle.ApplyRearm); err != nil {
		t.Fatal(err)
	}
	request := store.OperatorDecisionRequest{OperatorDecision: store.OperatorDecision{Ref: ref, ExpectedVersion: current.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, ReviewedHead: strings.Repeat("a", 40), OperatorUID: 501, Decision: "approved"}}
	stale := request
	stale.ExpectedVersion--
	if _, admitted, err := bundle.ApplyOperatorDecision(t.Context(), database, stale); err == nil || admitted {
		t.Fatal("stale decision consumed admission")
	}
	other, _, _, _ := runtimeControlStore(t)
	if _, admitted, err := bundle.ApplyOperatorDecision(t.Context(), other, request); err == nil || admitted {
		t.Fatal("foreign Store consumed admission")
	}
	ctx, cancel := context.WithCancel(t.Context())
	scheduler.admission.afterOpen = cancel
	if _, admitted, err := bundle.ApplyOperatorDecision(ctx, database, request); err == nil || admitted {
		t.Fatal("canceled open admitted decision")
	}
	scheduler.admission.afterOpen = nil
	if ready, err := database.RuntimeAdmissionReady(t.Context(), ref, current.Version, request.Fence); err != nil || ready {
		t.Fatalf("canceled admission ready=%v err=%v", ready, err)
	}
	// This fixture deliberately has no published candidate. The exact pending
	// activity may open, but the unchanged Store decision still refuses it.
	if _, admitted, err := bundle.ApplyOperatorDecision(t.Context(), database, request); err == nil || !admitted {
		t.Fatalf("exact activity admitted=%v Store refusal=%v", admitted, err)
	}
	decisions, err := database.OperatorDecisions(t.Context(), ref)
	if err != nil || len(decisions) != 0 {
		t.Fatalf("invented decision=%+v err=%v", decisions, err)
	}
	if scheduler.admission.Stopped(ref) {
		t.Fatal("exact activity did not consume pending admission")
	}
}

func TestOperatorDecisionReservationJoinsPollAndExcludesTicks(t *testing.T) {
	for _, outcome := range []string{"join", "cancel", "stop"} {
		t.Run(outcome, func(t *testing.T) {
			a := newAdmission()
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-decision-poll"}
			poll, endPoll, ok := a.Begin(t.Context(), ref, 12, 2, 3)
			if !ok {
				t.Fatal("poll admission")
			}
			defer endPoll()
			reserved := make(chan struct{})
			a.afterDecisionReserve = func() { close(reserved) }
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan bool, 1)
			go func() {
				_, end, admitted := a.BeginDecision(ctx, ref, 13, 2, 3)
				if admitted {
					end()
				}
				result <- admitted
			}()
			select {
			case <-reserved:
			case <-time.After(time.Second):
				t.Fatal("reservation timeout")
			}
			if _, end, ok := a.Begin(t.Context(), ref, 13, 2, 3); ok {
				end()
				t.Fatal("poll stole reserved activity")
			}
			if _, end, ok := a.BeginDecision(t.Context(), ref, 13, 2, 3); ok {
				end()
				t.Fatal("duplicate decision admitted")
			}
			select {
			case <-result:
				t.Fatal("decision did not join poll")
			default:
			}
			switch outcome {
			case "join":
				endPoll()
			case "cancel":
				cancel()
			case "stop":
				stopCtx, stopCancel := context.WithCancel(t.Context())
				stopCancel()
				_ = a.Stop(stopCtx, ref)
				endPoll()
			}
			select {
			case admitted := <-result:
				if admitted != (outcome == "join") {
					t.Fatalf("outcome %s admitted=%v", outcome, admitted)
				}
			case <-time.After(time.Second):
				t.Fatal("decision join did not finish")
			}
			if outcome == "cancel" {
				if poll.Err() != nil {
					t.Fatal("decision timeout canceled existing poll")
				}
			}
			a.mu.Lock()
			remaining := len(a.decisions)
			a.mu.Unlock()
			if remaining != 0 {
				t.Fatal("decision reservation leaked")
			}
		})
	}
}

func TestOperatorDecisionJoinPreservesEarlierDeadline(t *testing.T) {
	a := newAdmission()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-decision-deadline"}
	poll, endPoll, ok := a.Begin(t.Context(), ref, 12, 2, 3)
	if !ok {
		t.Fatal("poll admission")
	}
	defer endPoll()
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	if _, end, admitted := a.BeginDecision(ctx, ref, 13, 2, 3); admitted {
		end()
		t.Fatal("expired deadline admitted")
	}
	if poll.Err() != nil {
		t.Fatal("expired decision canceled poll")
	}
	if len(a.decisions) != 0 {
		t.Fatal("expired decision retained reservation")
	}
}
