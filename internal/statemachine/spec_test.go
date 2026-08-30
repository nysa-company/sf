package statemachine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func loadApprovedSpec(t *testing.T) Spec {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open normative spec: %v", err)
	}
	defer file.Close()
	spec, err := Load(file)
	if err != nil {
		t.Fatalf("load normative spec: %v", err)
	}
	return spec
}

func TestApprovedSpecMatchesDomain(t *testing.T) {
	spec := loadApprovedSpec(t)
	if got, want := len(spec.States), len(domain.AllStates()); got != want {
		t.Fatalf("states=%d want=%d", got, want)
	}
	if got, want := len(spec.Transitions), 42; got != want {
		t.Fatalf("transitions=%d want=%d", got, want)
	}
}

func TestBuilderVerificationAmendmentPathPreservesProofUntilAccepted(t *testing.T) {
	spec := loadApprovedSpec(t)
	requested, err := spec.Select(string(domain.StateBuilding), "verification_amendment_requested", map[string]bool{
		"amendment_request_valid": true,
		"correction_available":    true,
	})
	if err != nil {
		t.Fatalf("select amendment request: %v", err)
	}
	if requested.ID != "build_requests_verification_amendment" || requested.To != string(domain.StateVerifying) || len(requested.Invalidates) != 0 {
		t.Fatalf("request transition=%+v", requested)
	}

	rejected, err := spec.Select(string(domain.StateVerifying), "amendment_rejected", map[string]bool{
		"fresh_reviewer":                       true,
		"original_verification_intent_current": true,
		"correction_available":                 true,
	})
	if err != nil {
		t.Fatalf("select amendment rejection: %v", err)
	}
	if rejected.ID != "verification_amendment_rejected" || rejected.To != string(domain.StateBuilding) || len(rejected.Invalidates) != 0 {
		t.Fatalf("rejection transition=%+v", rejected)
	}

	accepted, err := spec.Select(string(domain.StateVerifying), "amendment_accepted", map[string]bool{
		"fresh_reviewer":           true,
		"old_and_new_digest_bound": true,
		"correction_available":     true,
	})
	if err != nil {
		t.Fatalf("select amendment acceptance: %v", err)
	}
	if accepted.ID != "verification_amended" || accepted.To != string(domain.StateBuilding) || len(accepted.Invalidates) != 1 || accepted.Invalidates[0] != "proof" {
		t.Fatalf("acceptance transition=%+v", accepted)
	}
}

func TestApprovedArtifactDigestAndBoundedDecoder(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "plans", "2026-08-29-software-factory-v1-state-machine.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadApproved(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	mutated := append([]byte(nil), data...)
	mutated[len(mutated)-2] ^= 1
	if _, err := LoadApproved(bytes.NewReader(mutated)); err == nil {
		t.Fatal("edited normative artifact was accepted")
	}
	if _, err := Load(bytes.NewReader(append(data, []byte(` {}`)...))); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}
	if _, err := Load(bytes.NewReader(bytes.Repeat([]byte("x"), MaxSpecBytes+1))); err == nil {
		t.Fatal("oversized state machine was accepted")
	}
}

func TestReviewPassSelectionIsExclusive(t *testing.T) {
	spec := loadApprovedSpec(t)
	transition, err := spec.Select(string(domain.StateReviewing), "review_pass", map[string]bool{
		"ticket_type_not_spike":       true,
		"merge_mode_guarded":          true,
		"all_nonapproval_gates_green": true,
	})
	if err != nil {
		t.Fatalf("select guarded review: %v", err)
	}
	if transition.ID != "review_pass_guarded" {
		t.Fatalf("transition=%s", transition.ID)
	}
}

func TestSelectionRejectsZeroAndMultipleMatches(t *testing.T) {
	spec := loadApprovedSpec(t)
	_, err := spec.Select(string(domain.StateWaitingApproval), "operator_approve", nil)
	if !errors.Is(err, ErrNoTransition) {
		t.Fatalf("expected no transition, got %v", err)
	}

	ambiguous := Spec{Transitions: []Transition{
		{ID: "one", From: []string{"queued"}, Trigger: "go", To: "planning"},
		{ID: "two", From: []string{"queued"}, Trigger: "go", To: "planning"},
	}}
	_, err = ambiguous.Select("queued", "go", nil)
	if !errors.Is(err, ErrAmbiguousTransition) {
		t.Fatalf("expected ambiguous transition, got %v", err)
	}
}

func TestResolveDynamicTarget(t *testing.T) {
	state, err := ResolveTarget("$resume_state", "paused", "building", "planning")
	if err != nil {
		t.Fatalf("resolve target: %v", err)
	}
	if state != domain.StateBuilding {
		t.Fatalf("state=%s", state)
	}
}
