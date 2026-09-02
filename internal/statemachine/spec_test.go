package statemachine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
	if got, want := len(spec.Transitions), 44; got != want {
		t.Fatalf("transitions=%d want=%d", got, want)
	}
}

func TestApprovedSpecExpandsEveryTransitionAndForbiddenComplement(t *testing.T) {
	spec := loadApprovedSpec(t)
	states := []string{"none"}
	for _, state := range domain.AllStates() {
		states = append(states, string(state))
	}
	sort.Strings(states)

	triggerSet := make(map[string]struct{})
	byPair := make(map[string][]Transition)
	for _, transition := range spec.Transitions {
		triggerSet[transition.Trigger] = struct{}{}
		for _, from := range transition.From {
			key := from + "\x00" + transition.Trigger
			byPair[key] = append(byPair[key], transition)
		}
	}
	triggers := make([]string, 0, len(triggerSet))
	for trigger := range triggerSet {
		triggers = append(triggers, trigger)
	}
	sort.Strings(triggers)

	for _, from := range states {
		for _, trigger := range triggers {
			from, trigger := from, trigger
			t.Run(from+"/"+trigger, func(t *testing.T) {
				candidates := byPair[from+"\x00"+trigger]
				if len(candidates) == 0 {
					if _, err := spec.Select(from, trigger, nil); !errors.Is(err, ErrNoTransition) {
						t.Fatalf("forbidden complement selected transition: %v", err)
					}
					return
				}

				guardSet := make(map[string]struct{})
				for _, candidate := range candidates {
					for _, guard := range candidate.Guards {
						guardSet[guard] = struct{}{}
					}
				}
				guardNames := make([]string, 0, len(guardSet))
				for guard := range guardSet {
					guardNames = append(guardNames, guard)
				}
				sort.Strings(guardNames)
				if len(guardNames) > 20 {
					t.Fatalf("guard complement is unexpectedly large: %d", len(guardNames))
				}

				selected := make(map[string]bool, len(candidates))
				for mask := 0; mask < 1<<len(guardNames); mask++ {
					guards := make(map[string]bool, len(guardNames))
					for index, guard := range guardNames {
						guards[guard] = mask&(1<<index) != 0
					}
					var expected []Transition
					for _, candidate := range candidates {
						matches := true
						for _, guard := range candidate.Guards {
							if !guards[guard] {
								matches = false
								break
							}
						}
						if matches {
							expected = append(expected, candidate)
						}
					}
					got, err := spec.Select(from, trigger, guards)
					switch len(expected) {
					case 0:
						if !errors.Is(err, ErrNoTransition) {
							t.Fatalf("mask=%d selected forbidden transition %+v: %v", mask, got, err)
						}
					case 1:
						if err != nil {
							t.Fatalf("mask=%d select %s: %v", mask, expected[0].ID, err)
						}
						if !reflect.DeepEqual(got, expected[0]) {
							t.Fatalf("mask=%d selected transition mismatch\ngot:  %+v\nwant: %+v", mask, got, expected[0])
						}
						selected[got.ID] = true
					default:
						if !errors.Is(err, ErrAmbiguousTransition) {
							t.Fatalf("mask=%d must fail ambiguous, got %+v err=%v", mask, got, err)
						}
					}
				}
				for _, candidate := range candidates {
					if !selected[candidate.ID] {
						t.Fatalf("transition %s has no unambiguous guard assignment", candidate.ID)
					}
				}
			})
		}
	}
}

func TestCIRedSelectionExhaustionIsExclusive(t *testing.T) {
	spec := loadApprovedSpec(t)
	available, err := spec.Select(string(domain.StateWaitingCI), "checks_red", map[string]bool{"correction_available": true})
	if err != nil || available.ID != "ci_red_repair" || available.To != string(domain.StateBuilding) {
		t.Fatalf("available transition=%+v err=%v", available, err)
	}
	exhausted, err := spec.Select(string(domain.StateWaitingCI), "checks_red", map[string]bool{"correction_exhausted": true})
	if err != nil || exhausted.ID != "ci_red_exhausted" || exhausted.To != string(domain.StatePaused) || exhausted.ResumeState != string(domain.StateWaitingCI) || exhausted.PhaseDisposition != "pause" || len(exhausted.AllowedEffects) != 0 || len(exhausted.Invalidates) != 0 {
		t.Fatalf("exhausted transition=%+v err=%v", exhausted, err)
	}
	if _, err := spec.Select(string(domain.StateWaitingCI), "checks_red", map[string]bool{"correction_available": true, "correction_exhausted": true}); !errors.Is(err, ErrAmbiguousTransition) {
		t.Fatalf("both correction guards should be ambiguous, got %v", err)
	}
	if _, err := spec.Select(string(domain.StateWaitingCI), "checks_red", nil); !errors.Is(err, ErrNoTransition) {
		t.Fatalf("missing correction guard should refuse, got %v", err)
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
