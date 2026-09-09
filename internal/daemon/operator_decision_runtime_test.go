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
