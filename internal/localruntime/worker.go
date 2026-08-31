package localruntime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	pub "github.com/nysa-company/sf/internal/publication"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

var ErrPublishingUnavailable = errors.New("publishing capability is unavailable")

const publishingUnavailableCode = "publication_runtime_unavailable"

// Worker is the state dispatch boundary for the local runtime. It performs a
// fresh Store read for every scheduler admission, then delegates the three
// provider phases to workflowworker and the publication phase to publication.
// Keeping this adapter at the runtime edge avoids widening either worker's
// result interface merely to compose the two lifecycles.
type Worker struct {
	Store       *store.Store
	Engine      workflowworker.StateMachine
	Workflow    workflowworker.Worker
	Publication pub.Worker
	CI          CIWorker
	// PublicationEnabled is explicit so a pre-publishing composition cannot
	// accidentally treat a zero publication worker as a successful phase.
	PublicationEnabled bool
}

// NewWorker constructs the runtime dispatcher while retaining concrete phase
// workers. The value form makes the adapter easy to use in hermetic tests and
// does not add another authority or lifecycle.
func NewWorker(database *store.Store, workflow workflowworker.Worker, publication pub.Worker) Worker {
	return Worker{Store: database, Workflow: workflow, Publication: publication, PublicationEnabled: true}
}

func (w Worker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Store == nil {
		return workflowworker.RunResult{Ref: ref}, workflowworker.ErrUnsupportedState
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticket, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	switch ticket.State {
	case domain.StatePlanning, domain.StateVerifying, domain.StateBuilding:
		return w.Workflow.Run(ctx, ref, fence)
	case domain.StatePublishing:
		if !w.PublicationEnabled {
			return w.blockPublishing(ctx, ticket, fence)
		}
		result, err := w.Publication.Run(ctx, ref, fence)
		return workflowworker.RunResult{
			Ref:          result.Ref,
			State:        result.State,
			Version:      result.Version,
			Phase:        domain.PhasePublish,
			Transitioned: result.Transitioned,
			Replayed:     result.Replayed,
		}, err
	case domain.StateWaitingCI:
		if w.CI.Observer == nil {
			return workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}, nil
		}
		return w.CI.Run(ctx, ref, fence)
	default:
		// Control and terminal states are inert. The scheduler normally filters
		// them, but keeping this dispatch boundary inert makes direct calls safe.
		return workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}, nil
	}
}

func (w Worker) blockPublishing(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Engine == nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhasePublish}, ErrPublishingUnavailable
	}
	binary := "sf"
	if ticket.Ref.Channel == domain.ChannelDev {
		binary = "sf-dev"
	}
	payload, err := json.Marshal(struct {
		Code       string `json:"code"`
		Reason     string `json:"reason"`
		NextAction string `json:"next_action"`
		Guidance   string `json:"guidance"`
	}{
		Code:       publishingUnavailableCode,
		Reason:     "publishing is unavailable because the GitHub CLI or its authentication is not configured",
		NextAction: binary + " doctor",
		Guidance:   "install gh and authenticate GitHub; sf never creates or copies credentials",
	})
	if err != nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhasePublish}, err
	}
	transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{
		Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State,
		Trigger: "typed_blocker", Fence: fence,
		Attributes:   map[string]string{"no_unreconciled_external_mutation": "true"},
		EventPayload: string(payload),
	})
	if err != nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhasePublish}, err
	}
	return workflowworker.RunResult{Ref: ticket.Ref, State: domain.StateBlocked, Version: transition.TicketVersion, Phase: domain.PhasePublish, Transitioned: true}, nil
}

var _ interface {
	Run(context.Context, domain.TicketRef, domain.Fence) (workflowworker.RunResult, error)
} = Worker{}
