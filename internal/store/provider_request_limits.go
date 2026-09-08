package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/contracts"
)

// Enforced inside BeginProviderAttempt's IMMEDIATE transaction, before the
// attempt insert. Existing attempts themselves reserve capacity permanently,
// including failed, cancelled, and quarantined launches. No in-memory counter
// or new recovery journal is involved. This is not estimated-cost admission.
func admitProviderRequestLimit(ctx context.Context, conn *sql.Conn, r ProviderAttemptRequest) error {
	if !contracts.UsesMultiCLIRequestLimit(r.Binding.Identity.Provider) {
		return nil
	}
	if r.Input.Timeout <= 0 || r.Input.Timeout > contracts.MultiCLIRequestTimeout {
		return ErrBudgetExhausted
	}
	var attempts int64
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=?`, r.Ref.Channel, r.Ref.Project, r.Ref.Ticket).Scan(&attempts); err != nil {
		return err
	}
	if attempts >= contracts.MultiCLIRequestLimit {
		return ErrBudgetExhausted
	}
	return nil
}
