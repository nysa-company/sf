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
	Reason        string           `json:"reason,omitempty"`
	ObservedAt    time.Time        `json:"observed_at"`
}

// RuntimeDiagnosticSummary is also the projection allowlist. Unknown values
// must not be rendered: an error string is never a diagnostic code.
func RuntimeDiagnosticSummary(code string) string {
	switch code {
	case "execution_base_dependencies":
		return "The execution worktree lacks supported Go dependency markers. Commit the required dependency closure to the configured base; primary-checkout-only files are not copied into tickets."
	case "config_snapshot_invalid":
		return "The ticket's frozen configuration could not be authenticated."
	case "config_digest_mismatch":
		return "The ticket configuration digest does not match its frozen bytes."
	case "workflow_identity_mismatch":
		return "The ticket, project and registered worktree identities do not agree."
	case "workflow_mode_unsupported":
		return "This runtime does not support the ticket's configured mode."
	case "provider_route_invalid", "provider_route_unavailable":
		return "The configured provider route is unavailable; check provider qualification and the frozen ticket configuration."
	case "provider_binding_unavailable":
		return "The provider's executable or isolated authentication binding is unavailable."
	case "provider_qualification_refused":
		return "Current provider qualification was refused; qualify the selected providers for this daemon."
	case "provider_request_invalid":
		return "The provider request failed validation before admission."
	case "provider_ticket_mismatch":
		return "The provider request no longer matches the current ticket configuration or fence."
	case "provider_evidence_unavailable":
		return "Required durable provider evidence could not be authenticated."
	case "provider_mismatch":
		return "The selected provider does not match the ticket's frozen route."
	case "provider_capacity_busy":
		return "Provider capacity is occupied; no new attempt was admitted."
	case "provider_store_busy":
		return "The authority database is busy; no new attempt was admitted."
	case "provider_stale_fence":
		return "Provider admission rejected an obsolete runner fence."
	case "provider_admission_refused":
		return "Store refused provider admission; this does not establish a credential or dependency failure."
	case "provider_persistence_unavailable":
		return "Provider persistence is unavailable; preserve the current ticket for recovery."
	case "provider_canceled":
		return "Provider admission was cancelled."
	case "provider_budget_exhausted":
		return "The ticket's provider budget is exhausted."
	case "provider_attempt_exhausted":
		return "The allowed provider attempts are exhausted."
	case "provider_result_indeterminate":
		return "A prior provider result remains indeterminate; do not retry blindly."
	case "provider_repair_unavailable":
		return "The authenticated provider repair route is unavailable."
	case "provider_result_invalid":
		return "The provider result could not be authenticated by Store."
	case "planner_unavailable":
		return "The planner boundary is unavailable; inspect current provider qualification."
	case "git_local_identity":
		return "Local repository identity preflight failed before remote access."
	case "git_transport_setup":
		return "The trusted Git transport could not be configured; inspect packaged helpers and authentication."
	case "git_remote_read":
		return "The protected-base remote read failed; helper success alone does not prove repository access."
	case "git_missing_base":
		return "The configured base ref was not found."
	case "git_malformed_remote_response":
		return "Git returned an invalid protected-base response."
	default:
		return ""
	}
}
