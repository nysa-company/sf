package workflowruntime

import (
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

// configuredProvider resolves one explicit portable role. Qualifications,
// model/family identities and launch authority remain coordinator/Store-owned.
// A portable selection never bypasses exact native qualification.
func configuredProvider(effective config.Effective, role providercoord.Role) (string, error) {
	var names []string
	switch role {
	case providercoord.RolePlanner:
		names = effective.Providers.Planner
	case providercoord.RoleBuilder:
		names = effective.Providers.Builder
	case providercoord.RoleReviewer:
		names = effective.Providers.Reviewer
	default:
		return "", ErrProviderOrder
	}
	if len(names) != 1 || (names[0] != "codex" && names[0] != "claude" && names[0] != "cursor") {
		return "", ErrProviderOrder
	}
	return names[0], nil
}

func ticketProvider(ticket store.Ticket, role providercoord.Role) (string, error) {
	effective, err := decodeTicketConfig(ticket)
	if err != nil {
		return "", err
	}
	return configuredProvider(effective, role)
}
