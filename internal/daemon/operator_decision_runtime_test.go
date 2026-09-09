package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func TestOperatorDecisionUsesRuntimeActivityAndReplaysOnce(t *testing.T) {
	for _, decision := range []string{"approved", "rejected"} {
		t.Run(decision, func(t *testing.T) {
			d, _, stop := testDaemon(t)
			defer stop()
			ticket := prepareDaemonGuardedLifecycle(t, d, "SF-runtime-decision", domain.StateWaitingApproval)
			worker := newDaemonRuntimeWorker()
			runtime, err := workflowruntime.NewRuntime(workflowruntime.NewScheduler(d.channel, workflowruntime.StoreTicketSource{Store: d.store}, daemonRuntimeEnsure{}, worker), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			controller, err := runtimecontrol.New(d.store, runtime.ControlBundle(), nil)
			if err != nil {
				t.Fatal(err)
			}
			d.control = controller
			candidate, err := d.store.RecoverableCandidate(t.Context(), ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			parameters := map[string]any{"channel": d.channel, "reviewed_head": candidate.Snapshot.HeadSHA}
			if decision == "rejected" {
				parameters["reason"] = "independent operator requests revision"
			}
			body, _ := json.Marshal(parameters)
			request := api.Request{Ticket: string(ticket.Ref.Ticket), Parameters: body}
			for attempt := 0; attempt < 2; attempt++ {
				response := d.operatorDecision(t.Context(), request, domain.OperatorIdentity{UID: 501}, decision)
				if !response.OK {
					t.Fatalf("attempt %d: %+v", attempt, response)
				}
			}
			current, err := d.store.Ticket(t.Context(), ticket.Ref)
			if err != nil || current.Version != ticket.Version+1 {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
			decisions, err := d.store.OperatorDecisions(t.Context(), ticket.Ref)
			if err != nil || len(decisions) != 1 {
				t.Fatalf("decisions=%+v err=%v", decisions, err)
			}
			ready, err := d.store.RuntimeAdmissionReady(t.Context(), ticket.Ref, current.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: current.RunnerEpoch})
			if err != nil || !ready {
				t.Fatalf("successor admission=%v err=%v", ready, err)
			}
			if calls, active := worker.snapshot(ticket.Ref); calls != 0 || active {
				t.Fatal("decision launched a worker")
			}
		})
	}
}

type refusingDecisionController struct{ testRuntimeController }

func (refusingDecisionController) ApplyOperatorDecision(context.Context, store.OperatorDecisionRequest) (store.TransitionResult, bool, error) {
	return store.TransitionResult{}, false, errors.New("private failure marker must not reach output")
}

func TestOperatorDecisionAdmissionFailureIsNotHeadChange(t *testing.T) {
	d, _, stop := testDaemon(t)
	defer stop()
	ticket := prepareDaemonGuardedLifecycle(t, d, "SF-decision-admission-refusal", domain.StateWaitingApproval)
	d.control = refusingDecisionController{}
	body, _ := json.Marshal(map[string]any{"channel": d.channel})
	response := d.operatorDecision(t.Context(), api.Request{Ticket: string(ticket.Ref.Ticket), Parameters: body}, domain.OperatorIdentity{UID: 501}, "approved")
	if response.OK || response.Mutation.Attempted || response.Error == nil || response.Error.Code != "decision_recovery_unavailable" || !response.Error.Retryable {
		t.Fatalf("response=%+v", response)
	}
	if response.NextAction == nil || !reflect.DeepEqual(response.NextAction.Argv, []string{d.executable(), "status", string(ticket.Ref.Ticket)}) {
		t.Fatalf("next action=%+v", response.NextAction)
	}
	current, err := d.store.Ticket(t.Context(), ticket.Ref)
	if err != nil || current.Version != ticket.Version {
		t.Fatalf("ticket changed: %+v %v", current, err)
	}
	decisions, err := d.store.OperatorDecisions(t.Context(), ticket.Ref)
	if err != nil || len(decisions) != 0 {
		t.Fatalf("decisions=%+v err=%v", decisions, err)
	}
}

func TestOperatorDecisionMissingRecoveryControllerKeepsSeal(t *testing.T) {
	d, _, stop := testDaemon(t)
	defer stop()
	ticket := prepareDaemonGuardedLifecycle(t, d, "SF-decision-missing-controller", domain.StateWaitingApproval)
	if err := d.store.SealRuntimeControl(t.Context(), ticket.Ref); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{"channel": d.channel})
	response := d.operatorDecision(t.Context(), api.Request{Ticket: string(ticket.Ref.Ticket), Parameters: body}, domain.OperatorIdentity{UID: 501}, "approved")
	if response.OK || response.Mutation.Attempted || response.Error == nil || response.Error.Code != "decision_recovery_unavailable" {
		t.Fatalf("response=%+v", response)
	}
	current, err := d.store.Ticket(t.Context(), ticket.Ref)
	if err != nil || current.Version != ticket.Version {
		t.Fatalf("ticket changed: %+v %v", current, err)
	}
}

type operatorDecisionHeldPoll struct {
	entered chan struct{}
	release chan struct{}
}

func (p *operatorDecisionHeldPoll) Run(ctx context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	close(p.entered)
	select {
	case <-p.release:
		return workflowworker.RunResult{Ref: ref}, nil
	case <-ctx.Done():
		return workflowworker.RunResult{Ref: ref}, ctx.Err()
	}
}

func TestOperatorDecisionJoinsActiveWaitingApprovalPoll(t *testing.T) {
	d, _, stop := testDaemon(t)
	defer stop()
	ticket := prepareDaemonGuardedLifecycle(t, d, "SF-decision-active-poll", domain.StateWaitingApproval)
	poll := &operatorDecisionHeldPoll{entered: make(chan struct{}), release: make(chan struct{})}
	scheduler := workflowruntime.NewScheduler(d.channel, workflowruntime.StoreTicketSource{Store: d.store}, daemonRuntimeEnsure{}, poll)
	r, err := workflowruntime.NewRuntime(scheduler, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := runtimecontrol.New(d.store, r.ControlBundle(), nil)
	if err != nil {
		t.Fatal(err)
	}
	d.control = controller
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tickDone := make(chan workflowruntime.TickResult, 1)
	go func() { tickDone <- scheduler.Tick(ctx, domain.Fence{LeaderEpoch: d.epoch}) }()
	select {
	case <-poll.entered:
	case result := <-tickDone:
		t.Fatalf("poll never entered: %+v", result)
	case <-ctx.Done():
		t.Fatal("poll entry timeout")
	}
	body, _ := json.Marshal(map[string]any{"channel": d.channel})
	responses := make(chan api.Response, 1)
	go func() {
		responses <- d.operatorDecision(ctx, api.Request{Ticket: string(ticket.Ref.Ticket), Parameters: body}, domain.OperatorIdentity{UID: 501}, "approved")
	}()
	// The held poll makes premature refusal observable. Reservation ordering
	// and Stop/cancellation interleavings are deterministic admission tests.
	select {
	case response := <-responses:
		t.Fatalf("decision did not join active poll: %+v", response)
	case <-time.After(20 * time.Millisecond):
	}
	close(poll.release)
	select {
	case response := <-responses:
		if !response.OK {
			t.Fatalf("joined decision: %+v", response)
		}
	case <-ctx.Done():
		t.Fatal("decision timeout")
	}
	select {
	case <-tickDone:
	case <-ctx.Done():
		t.Fatal("poll did not finish")
	}
	current, err := d.store.Ticket(t.Context(), ticket.Ref)
	if err != nil || current.State != domain.StateMerging || current.Version != ticket.Version+1 {
		t.Fatalf("current=%+v err=%v", current, err)
	}
	decisions, err := d.store.OperatorDecisions(t.Context(), ticket.Ref)
	if err != nil || len(decisions) != 1 {
		t.Fatalf("decisions=%+v err=%v", decisions, err)
	}
	if ready, err := d.store.RuntimeAdmissionReady(t.Context(), ticket.Ref, current.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: current.RunnerEpoch}); err != nil || !ready {
		t.Fatalf("merge readiness=%v err=%v", ready, err)
	}
}
