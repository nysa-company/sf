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
	value.AmendmentRequest = &AmendmentRequest{OldProofDigest: "old", ProposedDigest: "new", Reason: "fixture assertion wrong"}
	validation.ApprovedAmendmentDigest = "new"
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
