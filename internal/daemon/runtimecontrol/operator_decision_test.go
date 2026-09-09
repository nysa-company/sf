package runtimecontrol

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestOperatorDecisionRecoveredRejectionRetainsSeal(t *testing.T) {
	database, ref, leader, ticket := controllerFixture(t)
	controller, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SealRuntimeControl(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	request := store.OperatorDecisionRequest{OperatorDecision: store.OperatorDecision{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ReviewedHead: strings.Repeat("a", 40), OperatorUID: 501, Decision: "rejected"}}
	if _, admitted, err := controller.ApplyOperatorDecision(t.Context(), request); err == nil || admitted {
		t.Fatalf("recovered rejection admitted=%v err=%v", admitted, err)
	}
	if needed, err := database.RuntimeRearmNeeded(t.Context(), ref); err != nil || !needed {
		t.Fatalf("seal changed needed=%v err=%v", needed, err)
	}
	current, err := database.Ticket(t.Context(), ref)
	if err != nil || current.Version != ticket.Version {
		t.Fatalf("ticket changed=%+v %v", current, err)
	}
}

func TestOperatorDecisionInvalidRefDoesNotRetainControllerEntry(t *testing.T) {
	database, _, _, _ := controllerFixture(t)
	controller, err := New(database, controllerBundle(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, admitted, err := controller.ApplyOperatorDecision(t.Context(), store.OperatorDecisionRequest{}); err == nil || admitted {
		t.Fatal("invalid request admitted")
	}
	if len(controller.tickets) != 0 {
		t.Fatal("invalid ref retained controller state")
	}
}
