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

// StartRequest carries a daemon-acquired durable fence. Runtime entry points
// never invent it, so a replaced daemon cannot start stale work.
type StartRequest struct {
	Ticket     domain.TicketRef
	WorkflowID string
	Fence      domain.Fence
}

// SignalRequest is a fenced state-machine signal.
type SignalRequest struct {
	Ticket        domain.TicketRef
	TicketVersion uint64
	From          domain.State
	Trigger       string
	Fence         domain.Fence
	Attributes    map[string]string
}

// RecoveryRequest limits reconciliation to a leader-owned channel.
type RecoveryRequest struct {
	Channel     domain.Channel
	LeaderEpoch uint64
}

type TransitionResult struct {
	To            domain.State
	TicketVersion uint64
	Invalidated   []string
	EventID       string
}

type WorkflowEngine interface {
	Start(context.Context, StartRequest) error
	Transition(context.Context, TransitionRequest) (TransitionResult, error)
	Signal(context.Context, SignalRequest) error
	Recover(context.Context, RecoveryRequest) error
	Close() error
}
