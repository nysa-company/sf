package workflowprompt

import (
	"strings"
	"testing"
)

// Prompt assertions are contract tests, not evidence that a real model detects
// every contradiction. The hosted/native eval must exercise the decision too.
func TestVerificationChecksJointSatisfiabilityWithoutWeakeningAcceptance(t *testing.T) {
	input := VerificationInput{Ticket: testTicket(), Workspace: testWorkspace(), Plan: testPlan(), Runtime: testRuntime(), Command: []string{"go", "test", "./..."}}
	got, err := Verification(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"jointly satisfiable",
		"exact expected public diagnostic",
		"unique synthetic secret markers",
		"An expected pre-build failure does not establish that the tests themselves are correct",
		"Preserve all acceptance coverage and do not skip assertions",
	} {
		if !strings.Contains(got.Prompt, want) {
			t.Errorf("verification prompt missing %q", want)
		}
	}
}

func TestBuilderRequestsIndependentDecisionForContradictoryProof(t *testing.T) {
	got, err := Builder(BuilderInput{Ticket: testTicket(), Workspace: testWorkspace(), Plan: testPlan(), Verification: testVerification(), Runtime: testRuntime()})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"identify the exact file and conflicting assertions",
		"smallest correction preserving that acceptance",
		"Do not edit the protected files yourself",
		"test failure alone as amendment justification",
		"a fresh independent Reviewer decides",
	} {
		if !strings.Contains(got.Prompt, want) {
			t.Errorf("Builder prompt missing %q", want)
		}
	}
}

func TestAmendmentReviewerMustPreserveOriginalAcceptance(t *testing.T) {
	got, err := Verification(VerificationInput{
		Ticket: testTicket(), Workspace: testWorkspace(), Plan: testPlan(), Runtime: testRuntime(), Command: []string{"go", "test", "./..."},
		Amendment: &AmendmentReview{PriorProofDigest: strings.Repeat("a", 64), ProposedDigest: strings.Repeat("b", 64), ProposedCommand: []string{"go", "test", "./..."}, Reason: "Conflicting assertions require independent inspection", Requester: "builder"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a Builder claim or failing implementation is not evidence", "Reject a proposal that removes acceptance coverage", "changes the frozen command merely to obtain green", "Preserve every original acceptance criterion", "Reject it by returning the existing proof unchanged"} {
		if !strings.Contains(got.Prompt, want) {
			t.Errorf("amendment prompt missing %q", want)
		}
	}
}
