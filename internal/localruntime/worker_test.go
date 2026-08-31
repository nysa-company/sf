package localruntime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

type ciBlockEngine struct {
	request contracts.SignalRequest
}

func (e *ciBlockEngine) Signal(_ context.Context, request contracts.SignalRequest) (contracts.TransitionResult, error) {
	e.request = request
	return contracts.TransitionResult{To: domain.StateBlocked, TicketVersion: request.TicketVersion + 1}, nil
}

func (e *ciBlockEngine) SignalPlan(context.Context, contracts.SignalRequest) (contracts.TransitionResult, error) {
	return contracts.TransitionResult{}, errors.New("unexpected plan signal")
}
func (e *ciBlockEngine) SignalVerification(context.Context, contracts.SignalRequest) (contracts.TransitionResult, error) {
	return contracts.TransitionResult{}, errors.New("unexpected verification signal")
}
func (e *ciBlockEngine) SignalCandidate(context.Context, contracts.SignalRequest, domain.CandidateSnapshot) (contracts.TransitionResult, error) {
	return contracts.TransitionResult{}, errors.New("unexpected candidate signal")
}

var _ workflowworker.StateMachine = (*ciBlockEngine)(nil)

func TestPrePublishingWorkerBlocksPublishingWithActionableChannelGuidance(t *testing.T) {
	database := openStore(t)
	if err := database.CreateProject(t.Context(), store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-prepublishing"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: ref, State: domain.StatePublishing, SourceDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(t.Context(), ref.Channel, "prepublishing")
	if err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	worker := Worker{Store: database, Engine: engine.New(database, spec), PublicationEnabled: false}
	result, err := worker.Run(t.Context(), ref, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil || result.State != domain.StateBlocked || !result.Transitioned {
		t.Fatalf("pre-publishing result=%+v err=%v", result, err)
	}
	blocked, err := database.Ticket(t.Context(), ref)
	if err != nil || blocked.State != domain.StateBlocked || blocked.BlockedCode != publishingUnavailableCode {
		t.Fatalf("blocked ticket=%+v err=%v", blocked, err)
	}
	events, err := database.Events(t.Context(), ref.Channel, 0, 10)
	if err != nil || len(events) != 2 || events[1].Trigger != "typed_blocker" {
		t.Fatalf("block events=%+v err=%v", events, err)
	}
	var payload struct {
		NextAction string `json:"next_action"`
		Guidance   string `json:"guidance"`
	}
	if json.Unmarshal([]byte(events[1].Payload), &payload) != nil || payload.NextAction != "sf-dev doctor" || payload.Guidance == "" {
		t.Fatalf("block payload=%s", events[1].Payload)
	}
}

func TestWaitingCIWorkerBlocksWhenObserverIsUnavailable(t *testing.T) {
	database := openStore(t)
	if err := database.CreateProject(t.Context(), store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-ci-observer-missing"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: ref, State: domain.StateWaitingCI, SourceDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(t.Context(), ref.Channel, "ci-observer-missing")
	if err != nil {
		t.Fatal(err)
	}
	engine := &ciBlockEngine{}
	worker := Worker{Store: database, Engine: engine, CI: CIWorker{Store: database}}
	result, err := worker.Run(t.Context(), ref, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil || !result.Transitioned || result.State != domain.StateBlocked {
		t.Fatalf("missing CI observer result=%+v err=%v", result, err)
	}
	if engine.request.Trigger != "typed_blocker" || engine.request.From != domain.StateWaitingCI || engine.request.Attributes["no_unreconciled_external_mutation"] != "true" {
		t.Fatalf("missing CI observer request=%+v", engine.request)
	}
	var payload struct {
		Code       string `json:"code"`
		NextAction string `json:"next_action"`
		Guidance   string `json:"guidance"`
	}
	if err := json.Unmarshal([]byte(engine.request.EventPayload), &payload); err != nil || payload.Code != ciUnavailableCode || payload.NextAction != "sf-dev doctor" || payload.Guidance == "" {
		t.Fatalf("missing CI observer payload=%q err=%v", engine.request.EventPayload, err)
	}
}

func TestPrePublishingBlockRejectsUnreconciledPublicationEffect(t *testing.T) {
	database := openStore(t)
	if err := database.CreateProject(t.Context(), store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-prepublishing-effect"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: ref, State: domain.StatePublishing, SourceDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(t.Context(), ref.Channel, "prepublishing-effect")
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1}
	if _, err := database.PlanEffect(t.Context(), store.EffectPlan{SemanticKey: "prepublishing/effect", Ref: ref, Kind: store.PublicationPushEffectKind, TicketVersion: 1, Fence: fence, RequestDigest: "request"}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ClaimEffect(t.Context(), store.EffectFence{SemanticKey: "prepublishing/effect", Ref: ref, TicketVersion: 1, Fence: fence}); err != nil {
		t.Fatal(err)
	}
	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	worker := Worker{Store: database, Engine: engine.New(database, spec), PublicationEnabled: false}
	result, err := worker.Run(t.Context(), ref, fence)
	if !errors.Is(err, store.ErrPublicationBlockUnsafe) || result.State != domain.StatePublishing {
		t.Fatalf("unreconciled publication blocker result=%+v err=%v", result, err)
	}
	ticket, err := database.Ticket(t.Context(), ref)
	if err != nil || ticket.State != domain.StatePublishing || ticket.BlockedCode != "" {
		t.Fatalf("unreconciled publication blocker mutated ticket=%+v err=%v", ticket, err)
	}
}
