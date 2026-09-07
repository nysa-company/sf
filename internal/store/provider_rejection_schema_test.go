package store

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

// Schema-only synthetic rows are not valid signed retry receipts. This tests
// storage constraints, not admission or cryptographic authenticity.
func TestProviderServerRejectionSchemaIsBoundedAndImmutable(t *testing.T) {
	f := newProviderRetryWorktreeFixture(t, "rejection-schema", 40)
	claim, err := f.db.BeginProviderAttempt(f.ctx, f.request(t, domain.PhasePlanning, "planner", runtime(f.builder)))
	if err != nil {
		t.Fatal(err)
	}
	insert := func(ticket domain.TicketID, receipt []byte, digest string, deadline int64) error {
		_, err := f.db.db.ExecContext(f.ctx, `INSERT INTO provider_server_rejections(provider_attempt_id,channel,project_id,ticket_id,phase,role,attempt,canonical_receipt,receipt_sha256,observed_unix_nanos,not_before_unix_nanos) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, claim.ID, claim.Ref.Channel, claim.Ref.Project, ticket, claim.Phase, claim.Role, claim.Attempt, receipt, digest, 1, deadline)
		return err
	}
	digest := strings.Repeat("a", 64)
	for name, attempt := range map[string]func() error{
		"foreign ticket": func() error { return insert("SF-foreign", []byte(`{}`), digest, 2) },
		"invalid json":   func() error { return insert(claim.Ref.Ticket, []byte(`{`), digest, 2) },
		"oversized": func() error {
			return insert(claim.Ref.Ticket, []byte(`{"a":"`+strings.Repeat("a", 131072)+`"}`), digest, 2)
		},
		"nonhex":            func() error { return insert(claim.Ref.Ticket, []byte(`{}`), strings.Repeat("x", 64), 2) },
		"no backoff":        func() error { return insert(claim.Ref.Ticket, []byte(`{}`), digest, 1) },
		"unbounded backoff": func() error { return insert(claim.Ref.Ticket, []byte(`{}`), digest, 30000000002) },
	} {
		t.Run(name, func(t *testing.T) {
			if attempt() == nil {
				t.Fatal("malformed schema evidence accepted")
			}
		})
	}
	if err := insert(claim.Ref.Ticket, []byte(`{}`), digest, 2); err != nil {
		t.Fatal(err)
	}
	if err := insert(claim.Ref.Ticket, []byte(`{}`), digest, 2); err == nil {
		t.Fatal("second receipt accepted")
	}
	for _, statement := range []string{`UPDATE provider_server_rejections SET not_before_unix_nanos=3`, `DELETE FROM provider_server_rejections`} {
		if _, err := f.db.db.ExecContext(f.ctx, statement); err == nil {
			t.Fatal("immutable receipt changed")
		}
	}
	var deadline int64
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT not_before_unix_nanos FROM provider_server_rejections WHERE provider_attempt_id=?`, claim.ID).Scan(&deadline); err != nil || deadline != 2 {
		t.Fatal("deadline changed")
	}
}
