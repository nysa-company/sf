package contracts

import (
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// RuntimeDiagnostic is a bounded process-local observation, not durable
// lifecycle or execution authority. It never contains a tool error or output.
// Repository preflight and worktree identity failures are distinct from stale
// scheduler authority; neither outcome establishes a GitHub credential failure.
type RuntimeDiagnostic struct {
	Ref           domain.TicketRef `json:"ref"`
	TicketVersion uint64           `json:"ticket_version"`
	Fence         domain.Fence     `json:"fence"`
	Outcome       string           `json:"outcome"`
	ObservedAt    time.Time        `json:"observed_at"`
}
