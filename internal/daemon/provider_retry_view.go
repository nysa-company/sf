package daemon

import (
	"context"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// This read-only projection is not launch or drain authority. Retry performs
// its own current-fence, capacity, worktree and writer checks when invoked.
func (daemon *Daemon) projectProviderRetry(ctx context.Context, ticket store.Ticket, view map[string]any) {
	projectProviderRetry(ctx, ticket, view, daemon.executable(), daemon.store.ProviderRetryDisposition)
}

func projectProviderRetry(ctx context.Context, ticket store.Ticket, view map[string]any, executable string, read func(context.Context, store.Ticket) (store.ProviderRetryDisposition, error)) {
	if ticket.State != domain.StatePaused {
		return
	}
	disposition, err := read(ctx, ticket)
	if err == nil && disposition == store.ProviderRetryNotProvider {
		return
	}
	recovery, ok := view["recovery"].(map[string]any)
	if !ok {
		recovery = recoveryView(ticket, nil)
		view["recovery"] = recovery
	}
	recovery["writer_safety"] = "not_checked"
	// Never substitute guessed resume/retry authority for a failed lookup.
	delete(view, "next_action")
	if err != nil {
		recovery["cause"] = "Provider retry eligibility could not be authenticated. The next action is unknown; no retry or worktree edit is authorized by status."
		return
	}
	var code, command, cause string
	switch disposition {
	case store.ProviderRetryEligible:
		code, command = "provider_retry", "retry"
		cause = "The provider exhausted its current attempt window. One bounded retry is available; the retry command must still authenticate capacity, the worktree and drained writers."
	case store.ProviderRetryExhausted:
		code, command = "provider_retry_exhausted", "cancel"
		cause = "The provider exhausted its one permitted retry window. Cancel this ticket before submitting a new bounded ticket; retained work is not deleted."
	case store.ProviderRetryResubmissionRequired:
		code, command = "provider_retry_resubmit_required", "cancel"
		cause = "This provider retry crosses an unsupported retained-evidence boundary. Cancel this ticket before resubmitting; retained work is not deleted."
	default:
		recovery["cause"] = "Provider retry eligibility is unknown. No retry or worktree edit is authorized by status."
		return
	}
	recovery["cause"] = cause
	view["next_action"] = domain.NextAction{Code: code, Argv: []string{executable, command, string(ticket.Ref.Ticket)}}
}
