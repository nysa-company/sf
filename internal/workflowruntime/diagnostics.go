package workflowruntime

import (
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

const maxRuntimeDiagnostics = 64

func (r *Runtime) recordDiagnostic(result TickResult, at time.Time) {
	// Ignore pool contention/idle ticks: another loop failing to acquire a
	// busy ticket must not erase that ticket's meaningful last observation.
	switch result.Outcome {
	case OutcomeInvoked, OutcomeReadiness, OutcomeWorker, OutcomeBusy, OutcomeStale, OutcomeRepositoryPreflight, OutcomeWorktreeIdentity:
	default:
		return
	}
	value := contracts.RuntimeDiagnostic{Ref: result.Ref, TicketVersion: result.Ticket.Version, Fence: result.Fence, Outcome: string(result.Outcome), ObservedAt: at.UTC()}
	value.Reason = diagnosticReason(result.Err)
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, old := range r.diagnostics {
		if old.Ref == value.Ref {
			r.diagnostics = append(r.diagnostics[:i], r.diagnostics[i+1:]...)
			break
		}
	}
	if len(r.diagnostics) == maxRuntimeDiagnostics {
		r.diagnostics = r.diagnostics[1:]
	}
	r.diagnostics = append(r.diagnostics, value)
}

// RuntimeDiagnostics returns an owned snapshot of recent completed ticks,
// oldest first. Restart intentionally clears it. Store remains authoritative.
func (r *Runtime) RuntimeDiagnostics() []contracts.RuntimeDiagnostic {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]contracts.RuntimeDiagnostic(nil), r.diagnostics...)
}
