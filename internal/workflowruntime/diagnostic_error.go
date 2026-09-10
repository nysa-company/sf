package workflowruntime

import (
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
)

type diagnosticError struct {
	code string
	err  error
}

func (e diagnosticError) Error() string                 { return e.err.Error() }
func (e diagnosticError) Unwrap() error                 { return e.err }
func (e diagnosticError) RuntimeDiagnosticCode() string { return e.code }

func diagnosticReason(err error) string {
	var coded interface{ RuntimeDiagnosticCode() string }
	if errors.As(err, &coded) {
		if code := coded.RuntimeDiagnosticCode(); contracts.RuntimeDiagnosticSummary(code) != "" {
			return code
		}
	}
	for _, item := range []struct {
		err  error
		code string
	}{
		{ErrConfigSnapshotInvalid, "config_snapshot_invalid"},
		{ErrConfigDigestMismatch, "config_digest_mismatch"},
		{ErrIdentityMismatch, "workflow_identity_mismatch"},
		{ErrUnsupportedMode, "workflow_mode_unsupported"},
		{ErrProviderOrder, "provider_route_invalid"},
		{ErrProviderResultInvalid, "provider_result_invalid"},
		{ErrPlannerNotReady, "planner_unavailable"},
	} {
		if errors.Is(err, item.err) {
			return item.code
		}
	}
	return ""
}

func preserveDiagnostic(source, safe error) error {
	if code := diagnosticReason(source); code != "" {
		return diagnosticError{code: code, err: safe}
	}
	return safe
}
