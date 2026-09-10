package providercoord

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestAdmissionErrorDiagnosticNeverCopiesErrorText(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want AdmissionDiagnostic
	}{
		{errors.New("credential=https://secret.example/token"), DiagnosticAdmissionRefused},
		{fmt.Errorf("secret: %w", store.ErrBusy), DiagnosticStoreBusy},
		{fmt.Errorf("secret: %w", store.ErrStaleFence), DiagnosticStaleFence},
		{fmt.Errorf("secret: %w", store.ErrProviderPairRefused), DiagnosticQualificationRefused},
		{store.ErrProviderCapacity, DiagnosticCapacityBusy},
		{context.Canceled, DiagnosticCanceled},
		{context.DeadlineExceeded, DiagnosticCanceled},
		{store.ErrBudgetExhausted, DiagnosticBudgetExhausted},
		{store.ErrProviderAttemptLimit, DiagnosticAttemptExhausted},
		{store.ErrProviderResultIndeterminate, DiagnosticResultIndeterminate},
		{store.ErrProviderServerRejection, DiagnosticResultIndeterminate},
		{store.ErrProviderRepairUnavailable, DiagnosticRepairUnavailable},
	} {
		if got := admissionErrorDiagnostic(tc.err); got != tc.want || !got.Valid() {
			t.Fatalf("diagnostic=%q want=%q", got, tc.want)
		}
	}
	for _, value := range []AdmissionDiagnostic{"", "secret", "provider_arbitrary"} {
		if value.Valid() {
			t.Fatalf("untrusted diagnostic accepted: %q", value)
		}
	}
}

type admissionBindingFailure struct{ contracts.Provider }

func (p admissionBindingFailure) Binding(context.Context) (contracts.RuntimeBinding, error) {
	return contracts.RuntimeBinding{}, errors.New("credential=https://secret.example/token")
}

func TestAdmissionDiagnosticsWithoutClaimOrInvocation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		want   AdmissionDiagnostic
		change func(*Request, *Coordinator)
	}{
		{"invalid_request", DiagnosticRequestInvalid, func(r *Request, _ *Coordinator) { r.Role = "unknown" }},
		{"ticket_mismatch", DiagnosticTicketMismatch, func(r *Request, _ *Coordinator) { r.ExpectedVersion++ }},
		{"route_missing", DiagnosticRouteUnavailable, func(r *Request, c *Coordinator) { delete(c.routes, r.Role) }},
		{"provider_mismatch", DiagnosticProviderMismatch, func(r *Request, _ *Coordinator) { r.ExpectedProvider = "claude" }},
		{"binding_failure", DiagnosticBindingUnavailable, func(_ *Request, c *Coordinator) {
			for name, p := range c.registry.providers {
				c.registry.providers[name] = admissionBindingFailure{p}
			}
		}},
		{"persistence_failure", DiagnosticPersistenceUnavailable, func(_ *Request, c *Coordinator) { c.markPersistenceFailure(errors.New("secret")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
			tc.change(&request, coordinator)
			result := coordinator.Run(context.Background(), request)
			if result.Diagnostic != tc.want || len(result.Attempts) != 0 || len(primary.CallsSnapshot()) != 0 {
				t.Fatalf("admission result=%+v", result)
			}
			attempts, err := db.ProviderAttempts(context.Background(), ref)
			if err != nil || len(attempts) != 0 {
				t.Fatalf("admission reserved attempts: count=%d err=%v", len(attempts), err)
			}
		})
	}
}
