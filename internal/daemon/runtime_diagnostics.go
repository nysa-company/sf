package daemon

import (
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func (d *Daemon) runtimeActivity(ref *domain.TicketRef) map[string]any {
	if !d.runtimeMu.TryLock() {
		return map[string]any{"available": false, "scope": "Runtime composition is changing; no diagnostic snapshot was taken."}
	}
	runtime := d.runtime
	d.runtimeMu.Unlock()
	reader, ok := runtime.(interface {
		RuntimeDiagnostics() []contracts.RuntimeDiagnostic
	})
	view := map[string]any{"available": ok, "scope": "Recent completed scheduler checks in this daemon only; not current ticket state or execution authority. Restart clears this history."}
	if !ok {
		return view
	}
	items := make([]map[string]any, 0)
	for i, observation := range reader.RuntimeDiagnostics() {
		if i >= 64 {
			break
		}
		if observation.Ref.Channel != d.channel && observation.Ref != (domain.TicketRef{}) {
			continue
		}
		if ref != nil && observation.Ref != *ref {
			continue
		}
		switch observation.Outcome {
		case "invoked", "readiness_failed", "worker_failed", "busy", "stale", "repository_preflight_failed", "worktree_identity_failed":
		default:
			continue
		}
		items = append(items, map[string]any{
			"ticket": observation.Ref.Ticket, "project": observation.Ref.Project,
			"outcome": observation.Outcome, "observed_at": observation.ObservedAt,
			"observed_ticket_version": observation.TicketVersion,
			"leader_epoch":            observation.Fence.LeaderEpoch, "runner_epoch": observation.Fence.RunnerEpoch,
		})
	}
	view["observations"] = items
	return view
}
