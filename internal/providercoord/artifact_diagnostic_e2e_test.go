//go:build sf_e2e

package providercoord

import (
	"errors"
	"testing"
)

func TestSuccessStreamDiagnosticIsClosed(t *testing.T) {
	for _, tc := range []struct{ message, want string }{
		{"Claude success stream is incomplete or inconsistent: tool_result event 7", "tool_result/7"},
		{"Claude success stream is incomplete or inconsistent: PRIVATE_SECRET event 7", "other"},
		{"Claude success stream is incomplete or inconsistent: terminal event PRIVATE_SECRET", "other"},
		{"Claude success stream is incomplete or inconsistent: terminal event -1", "other"},
		{"Claude success stream is incomplete or inconsistent: terminal event 4097", "other"},
		{"PRIVATE_SECRET terminal event 1", "other"},
	} {
		if got := successStreamCategory(errors.New(tc.message)); got != tc.want {
			t.Fatal("closed diagnostic changed")
		}
	}
}

func TestPlannerValidationDiagnosticDoesNotExposeProviderText(t *testing.T) {
	for _, tc := range []struct{ message, want string }{
		{"validate planning artifact: proof kind \"PRIVATE_SECRET\" does not satisfy ticket type \"feature\"", "proof_kind"},
		{"validate planning artifact: planner paths: unsafe path \"PRIVATE_SECRET\"", "paths"},
		{"validate planning artifact: json: unknown field \"PRIVATE_SECRET\"", "json_shape"},
		{"validate planning artifact: risks requires 1 to 20 entries", "risks"},
		{"PRIVATE_SECRET", "other_validation"},
	} {
		if got := plannerValidationCategory(errors.New(tc.message)); got != tc.want {
			t.Errorf("closed category: got %q, want %q", got, tc.want)
		}
	}
	if plannerValidationCategory(nil) != "unknown" {
		t.Fatal("nil diagnostic must be unknown")
	}
}

func TestBuilderValidationDiagnosticDoesNotExposeProviderText(t *testing.T) {
	for _, tc := range []struct{ message, want string }{
		{"validate build artifact: builder changed files are required", "changed_files_missing"},
		{"validate build artifact: builder changed files: unsafe path \"PRIVATE_SECRET\"", "changed_files_invalid"},
		{"validate build artifact: json: unknown field \"PRIVATE_SECRET\"", "json_shape"},
		{"validate build artifact: unsupported schema \"PRIVATE_SECRET\"", "schema_version"},
		{"validate build artifact: builder changed protected verification without an amendment request", "protected_verification_changed"},
		{"PRIVATE_SECRET", "other_validation"},
	} {
		if got := builderValidationCategory(errors.New(tc.message)); got != tc.want {
			t.Errorf("closed category: got %q, want %q", got, tc.want)
		}
	}
	if builderValidationCategory(nil) != "unknown" {
		t.Fatal("nil diagnostic must be unknown")
	}
}
