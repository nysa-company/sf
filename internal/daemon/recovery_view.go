package daemon

import (
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// recoveryView is explanatory only. A status snapshot is never a drain proof
// or permission to edit: those remain the authenticated control boundary's job.
func recoveryView(ticket store.Ticket, evidence map[string]any) map[string]any {
	switch ticket.State {
	case domain.StateBlocked, domain.StatePaused, domain.StateStopping, domain.StateCancelling, domain.StateCancelled:
	default:
		return nil
	}
	cause := "The ticket is stopped. Follow the reported next action; status does not authorize a retry or worktree edit."
	switch ticket.State {
	case domain.StateStopping, domain.StateCancelling:
		cause = "SF is stopping or reconciling work. Do not edit the worktree until the control command confirms a safe handoff."
	case domain.StateCancelled:
		cause = "This ticket is cancelled. Cancellation does not delete its recorded worktree or evidence."
	case domain.StateBlocked:
		switch ticket.BlockedCode {
		case "postbuild_command_failed":
			cause = "The factory-run post-build proof failed. This does not establish whether the implementation or the test is wrong. No passing candidate was accepted from that result."
		case "provider_result_indeterminate":
			cause = "The provider outcome is uncertain. SF will not replay it blindly; retained files are not accepted implementation evidence."
		case "provider_repair_unavailable":
			cause = "The provider result could not enter a supported repair path. Retained files are not accepted implementation evidence."
		case "ticket_budget_exhausted":
			cause = "The ticket reached its execution budget. Status does not extend that budget or start another paid attempt."
		case "verification_amendment_invalid":
			cause = "SF could not authenticate the verification amendment. The proposed replacement is not permission to weaken the existing proof."
		}
	}
	disposition := "No worktree registration is present in this status snapshot."
	_, registered := evidence["worktree"].(map[string]any)
	if registered {
		disposition = "A worktree registration is retained. Its current disk contents and writer safety were not checked by status."
	}
	return map[string]any{
		"cause": cause, "worktree_registered": registered,
		"work_disposition": disposition, "writer_safety": "not_checked",
		"safety_note": "Use the supported control action for a fresh safety check; never infer safe-to-edit from a retained worktree.",
	}
}
