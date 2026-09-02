package phaseartifact

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

var provider = domain.ProviderIdentity{Provider: "fixture", Model: "m", Family: "test", Version: "1.0.0"}

func result(t *testing.T, value any) contracts.PhaseResult {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return contracts.PhaseResult{Artifact: data, Provider: provider}
}

func planner() Planner {
	return Planner{
		Schema: "sf.planner/v1", Acceptance: []string{"duplicate is prevented"},
		Proof: ProofPlan{Kind: ProofRegression, Command: []string{"go", "test", "./..."}, Details: "red duplicate regression"},
		Paths: []string{"internal/reminder"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"idempotency"},
	}
}

func TestPlannerSchemaIsStrictAndCannotSelectState(t *testing.T) {
	parsed, err := Parse(domain.PhasePlanning, result(t, planner()), Validation{TicketType: domain.TicketBug})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Planner == nil || !strings.HasPrefix(parsed.Digest, "sha256:") || parsed.Provider != provider {
		t.Fatalf("parsed=%+v", parsed)
	}

	data := []byte(`{"schema":"sf.planner/v1","acceptance":["a"],"proof":{"kind":"regression","command":["go","test"],"details":"red"},"paths":["x"],"commands":[["go","test"]],"risks":["r"],"questions":[],"state":"done"}`)
	_, err = Parse(domain.PhasePlanning, contracts.PhaseResult{Artifact: data, Provider: provider}, Validation{TicketType: domain.TicketBug})
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("state injection error=%v", err)
	}
}

func TestMalformedOversizedAndIncompleteIdentityFail(t *testing.T) {
	_, err := Parse(domain.PhasePlanning, contracts.PhaseResult{Artifact: []byte("not-json"), Provider: provider}, Validation{TicketType: domain.TicketBug})
	if err == nil {
		t.Fatal("malformed artifact accepted")
	}
	_, err = Parse(domain.PhasePlanning, contracts.PhaseResult{Artifact: []byte(strings.Repeat("x", MaxBytes+1)), Provider: provider}, Validation{TicketType: domain.TicketBug})
	if err == nil {
		t.Fatal("oversized artifact accepted")
	}
	_, err = Parse(domain.PhasePlanning, result(t, planner()), Validation{TicketType: domain.TicketBug})
	if err != nil {
		t.Fatal(err)
	}
	incomplete := result(t, planner())
	incomplete.Provider.Version = ""
	if _, err := Parse(domain.PhasePlanning, incomplete, Validation{TicketType: domain.TicketBug}); err == nil {
		t.Fatal("incomplete provider identity accepted")
	}
}

func TestVerificationTypeRulesAndAcceptanceSeal(t *testing.T) {
	value := Verification{
		Schema: "sf.verification/v1", AcceptanceDigest: "accepted", ProofKind: ProofRegression,
		OwnedFiles: []string{"internal/reminder/regression_test.go"}, Command: []string{"go", "test", "./internal/reminder"},
		PrebuildOutcome: "red", EvidenceDigest: "evidence",
	}
	if _, err := Parse(domain.PhaseVerification, result(t, value), Validation{TicketType: domain.TicketBug, AcceptanceDigest: "accepted"}); err != nil {
		t.Fatal(err)
	}
	value.AcceptanceDigest = "changed"
	if _, err := Parse(domain.PhaseVerification, result(t, value), Validation{TicketType: domain.TicketBug, AcceptanceDigest: "accepted"}); err == nil {
		t.Fatal("verification changed accepted intent")
	}
	value.AcceptanceDigest = "accepted"
	value.PrebuildOutcome = "green"
	if _, err := Parse(domain.PhaseVerification, result(t, value), Validation{TicketType: domain.TicketBug, AcceptanceDigest: "accepted"}); err == nil {
		t.Fatal("green bug proof accepted before build")
	}
}

func TestBuilderCannotSilentlyChangeProtectedVerification(t *testing.T) {
	value := Builder{Schema: "sf.builder/v1", Summary: "fix", ChangedFiles: []string{"proof_test.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	validation := Validation{ProtectedVerification: []string{"proof_test.go"}}
	if _, err := Parse(domain.PhaseBuild, result(t, value), validation); err == nil {
		t.Fatal("protected proof edit accepted without amendment")
	}
	value.AmendmentRequest = &AmendmentRequest{OldProofDigest: strings.Repeat("a", 64), ProposedDigest: strings.Repeat("b", 64), ProposedCommand: []string{"go", "test", "./..."}, Reason: "fixture assertion wrong"}
	validation.ApprovedAmendmentDigest = strings.Repeat("b", 64)
	if _, err := Parse(domain.PhaseBuild, result(t, value), validation); err != nil {
		t.Fatal(err)
	}
	value.ChangedFiles = []string{"../escape"}
	if _, err := Parse(domain.PhaseBuild, result(t, value), validation); err == nil {
		t.Fatal("escaping changed-file path accepted")
	}
}

func TestFinalReviewerBindsExactHeadAndProof(t *testing.T) {
	value := Reviewer{Schema: "sf.reviewer/v1", Decision: ReviewPass, ReviewedHead: "head", ProofDigest: "proof"}
	validation := Validation{ExpectedReviewedHead: "head", ExpectedProofDigest: "proof"}
	if _, err := Parse(domain.PhaseReview, result(t, value), validation); err != nil {
		t.Fatal(err)
	}
	value.ReviewedHead = "stale"
	_, err := Parse(domain.PhaseReview, result(t, value), validation)
	if err == nil {
		t.Fatal("stale reviewed head accepted")
	}
}

func TestCanonicalTypedArtifactUsesExactlyOneParsedRoleValue(t *testing.T) {
	cases := []struct {
		phase      domain.Phase
		result     contracts.PhaseResult
		validation Validation
	}{
		{domain.PhasePlanning, result(t, planner()), Validation{TicketType: domain.TicketBug}},
		{domain.PhaseVerification, result(t, Verification{Schema: "sf.verification/v1", AcceptanceDigest: "accepted", ProofKind: ProofRegression, OwnedFiles: []string{"proof_test.go"}, Command: []string{"go", "test"}, PrebuildOutcome: "red", EvidenceDigest: "evidence"}), Validation{TicketType: domain.TicketBug, AcceptanceDigest: "accepted"}},
		{domain.PhaseBuild, result(t, Builder{Schema: "sf.builder/v1", Summary: "done", ChangedFiles: []string{"main.go"}, Commands: [][]string{{"go", "test"}}}), Validation{}},
		{domain.PhaseReview, result(t, Reviewer{Schema: "sf.reviewer/v1", Decision: ReviewPass, ReviewedHead: "head", ProofDigest: "proof"}), Validation{ExpectedReviewedHead: "head", ExpectedProofDigest: "proof"}},
	}
	for _, tc := range cases {
		parsed, err := Parse(tc.phase, tc.result, tc.validation)
		if err != nil {
			t.Fatalf("%s parse: %v", tc.phase, err)
		}
		data, _, err := CanonicalTypedArtifact(parsed)
		if err != nil {
			t.Fatalf("%s canonical: %v", tc.phase, err)
		}
		got, err := DecodeCanonicalTypedArtifact(data)
		if err != nil {
			t.Fatalf("%s decode: %v", tc.phase, err)
		}
		if got.Phase != tc.phase || got.Provider != provider {
			t.Fatalf("%s decoded %+v", tc.phase, got)
		}
		if strings.Contains(string(data), `"artifact"`) {
			t.Fatalf("%s wrapped raw artifact", tc.phase)
		}
	}
}

func TestValidateMutationPathsUsesTypedArtifactOverAdapterInventory(t *testing.T) {
	builderResult := result(t, Builder{Schema: "sf.builder/v1", Summary: "done", ChangedFiles: []string{"src/main.go"}, Commands: [][]string{{"go", "test"}}})
	builder, err := Parse(domain.PhaseBuild, builderResult, Validation{})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateMutationPaths(builder, nil, []string{"src"}); err != nil {
		t.Fatalf("nil Codex-style inventory rejected: %v", err)
	}
	if err := ValidateMutationPaths(builder, []string{}, []string{"src"}); err == nil {
		t.Fatal("explicit empty inventory bypassed builder declaration")
	}
	if err := ValidateMutationPaths(builder, []string{"src/main.go"}, []string{"test"}); err == nil {
		t.Fatal("out-of-scope builder declaration accepted")
	}
	if err := ValidateMutationPaths(builder, []string{"src/main.go"}, []string{"src"}); err != nil {
		t.Fatal(err)
	}
	verifyResult := result(t, Verification{Schema: "sf.verification/v1", AcceptanceDigest: "accepted", ProofKind: ProofRegression, OwnedFiles: []string{"proof/check_test.go"}, Command: []string{"go", "test"}, PrebuildOutcome: "red", EvidenceDigest: "evidence"})
	verify, err := Parse(domain.PhaseVerification, verifyResult, Validation{TicketType: domain.TicketBug, AcceptanceDigest: "accepted"})
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateMutationPaths(verify, nil, []string{"proof"}); err != nil {
		t.Fatalf("nil Codex-style verification inventory rejected: %v", err)
	}
	if err := ValidateMutationPaths(verify, []string{}, []string{"proof"}); err == nil {
		t.Fatal("explicit empty verification inventory bypassed declaration")
	}
	if err := ValidateMutationPaths(verify, []string{"proof/check_test.go"}, []string{"src"}); err == nil {
		t.Fatal("out-of-scope verification declaration accepted")
	}
	if err := ValidateMutationPaths(verify, []string{"proof/check_test.go"}, []string{"proof"}); err != nil {
		t.Fatal(err)
	}
}
