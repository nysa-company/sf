package providercoord

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/store"
)

// AdmissionDiagnostic is code-owned metadata, never provider or error text.
// It describes a refused admission, not permission to retry or change state.
type AdmissionDiagnostic string

const (
	DiagnosticRequestInvalid         AdmissionDiagnostic = "provider_request_invalid"
	DiagnosticTicketMismatch         AdmissionDiagnostic = "provider_ticket_mismatch"
	DiagnosticEvidenceUnavailable    AdmissionDiagnostic = "provider_evidence_unavailable"
	DiagnosticRouteUnavailable       AdmissionDiagnostic = "provider_route_unavailable"
	DiagnosticBindingUnavailable     AdmissionDiagnostic = "provider_binding_unavailable"
	DiagnosticProviderMismatch       AdmissionDiagnostic = "provider_mismatch"
	DiagnosticQualificationRefused   AdmissionDiagnostic = "provider_qualification_refused"
	DiagnosticCapacityBusy           AdmissionDiagnostic = "provider_capacity_busy"
	DiagnosticStoreBusy              AdmissionDiagnostic = "provider_store_busy"
	DiagnosticStaleFence             AdmissionDiagnostic = "provider_stale_fence"
	DiagnosticAdmissionRefused       AdmissionDiagnostic = "provider_admission_refused"
	DiagnosticPersistenceUnavailable AdmissionDiagnostic = "provider_persistence_unavailable"
	DiagnosticCanceled               AdmissionDiagnostic = "provider_canceled"
	DiagnosticBudgetExhausted        AdmissionDiagnostic = "provider_budget_exhausted"
	DiagnosticAttemptExhausted       AdmissionDiagnostic = "provider_attempt_exhausted"
	DiagnosticResultIndeterminate    AdmissionDiagnostic = "provider_result_indeterminate"
	DiagnosticRepairUnavailable      AdmissionDiagnostic = "provider_repair_unavailable"
)

func (d AdmissionDiagnostic) Valid() bool {
	switch d {
	case DiagnosticRequestInvalid, DiagnosticTicketMismatch, DiagnosticEvidenceUnavailable,
		DiagnosticRouteUnavailable, DiagnosticBindingUnavailable, DiagnosticProviderMismatch,
		DiagnosticQualificationRefused, DiagnosticCapacityBusy, DiagnosticStoreBusy,
		DiagnosticStaleFence, DiagnosticAdmissionRefused, DiagnosticPersistenceUnavailable,
		DiagnosticCanceled, DiagnosticBudgetExhausted, DiagnosticAttemptExhausted,
		DiagnosticResultIndeterminate, DiagnosticRepairUnavailable:
		return true
	}
	return false
}

func admissionErrorDiagnostic(err error) AdmissionDiagnostic {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return DiagnosticCanceled
	case errors.Is(err, store.ErrStaleFence):
		return DiagnosticStaleFence
	case errors.Is(err, store.ErrBusy):
		return DiagnosticStoreBusy
	case errors.Is(err, store.ErrProviderPairRefused):
		return DiagnosticQualificationRefused
	case errors.Is(err, store.ErrProviderCapacity):
		return DiagnosticCapacityBusy
	case errors.Is(err, store.ErrBudgetExhausted):
		return DiagnosticBudgetExhausted
	case errors.Is(err, store.ErrProviderAttemptLimit):
		return DiagnosticAttemptExhausted
	case errors.Is(err, store.ErrProviderResultIndeterminate), errors.Is(err, store.ErrProviderServerRejection):
		return DiagnosticResultIndeterminate
	case errors.Is(err, store.ErrProviderRepairUnavailable):
		return DiagnosticRepairUnavailable
	default:
		return DiagnosticAdmissionRefused
	}
}
