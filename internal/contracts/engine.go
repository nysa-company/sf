package contracts

import (
	"context"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type TransitionRequest struct {
	Ticket        domain.TicketRef
	TicketVersion uint64
	From          domain.State
	Trigger       string
	Fence         domain.Fence
	ObservedAt    time.Time
	Attributes    map[string]string
}

type TransitionResult struct {
	To            domain.State
	TicketVersion uint64
	Invalidated   []string
	EventID       string
}

type WorkflowEngine interface {
	Start(context.Context, domain.TicketRef, string) error
	Transition(context.Context, TransitionRequest) (TransitionResult, error)
	Signal(context.Context, domain.TicketRef, string, map[string]string) error
	Recover(context.Context) error
	Close() error
}
