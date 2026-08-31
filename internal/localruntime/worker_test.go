package localruntime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type reviewingEnsurer struct{ calls int }

func (e *reviewingEnsurer) Ensure(context.Context, worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	e.calls++
	return store.StoredWorktree{Path: "/tmp/reviewing-worktree", State: "registered"}, nil
}

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

func TestProductionSchedulerDispatchesReviewingTicketToWorkflowWorker(t *testing.T) {
	database := openStore(t)
	if err := database.CreateProject(t.Context(), store.Project{Channel: domain.ChannelDev, ID: "nysa", Path: "/tmp/nysa", BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-reviewing-dispatch"}
	if err := database.CreateTicket(t.Context(), store.Ticket{Ref: ref, State: domain.StateReviewing, SourceDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(t.Context(), ref.Channel, "reviewing-dispatch")
	if err != nil {
		t.Fatal(err)
	}
	ensurer := &reviewingEnsurer{}
	dispatcher := Worker{Store: database, Workflow: workflowworker.Worker{Evidence: database, Engine: engine.New(database, statemachine.Spec{})}}
	scheduler := workflowruntime.NewScheduler(domain.ChannelDev, workflowruntime.StoreTicketSource{Store: database}, ensurer, dispatcher)
	result := scheduler.Tick(t.Context(), domain.Fence{LeaderEpoch: leader})
	if result.Outcome != workflowruntime.OutcomeWorker || result.Ref != ref || result.Worker.Phase != domain.PhaseReview || ensurer.calls != 1 {
		t.Fatalf("reviewing scheduler dispatch=%+v worktree_calls=%d", result, ensurer.calls)
	}
}
