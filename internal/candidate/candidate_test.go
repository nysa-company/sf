package candidate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func snapshot() domain.CandidateSnapshot {
	return domain.CandidateSnapshot{
		Generation: 1, BaseSHA: "base", HeadSHA: "head", TreeSHA: "tree",
		SourceDigest: "source", VerificationIntentDigest: "intent", ProofDigest: "proof",
		CommandPolicyDigest: "policy", BuilderEvidenceDigest: "builder",
	}
}

func TestEveryCandidateIdentityChangeInvalidatesDownstreamGates(t *testing.T) {
	base := snapshot()
	tests := map[string]func(*domain.CandidateSnapshot){
		"generation": func(value *domain.CandidateSnapshot) { value.Generation++ },
		"base":       func(value *domain.CandidateSnapshot) { value.BaseSHA = "new-base" },
		"head":       func(value *domain.CandidateSnapshot) { value.HeadSHA = "new-head" },
		"tree":       func(value *domain.CandidateSnapshot) { value.TreeSHA = "new-tree" },
		"source":     func(value *domain.CandidateSnapshot) { value.SourceDigest = "new-source" },
		"proof":      func(value *domain.CandidateSnapshot) { value.ProofDigest = "new-proof" },
		"policy":     func(value *domain.CandidateSnapshot) { value.CommandPolicyDigest = "new-policy" },
		"builder":    func(value *domain.CandidateSnapshot) { value.BuilderEvidenceDigest = "new-builder" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			next := base
			mutate(&next)
			if got := Invalidated(base, next, false); !reflect.DeepEqual(got, downstreamGates) {
				t.Fatalf("invalidated=%v want=%v", got, downstreamGates)
			}
		})
	}
}

func TestVerificationChangeInvalidatesIntentAndAllDownstreamGates(t *testing.T) {
	base := snapshot()
	next := base
	next.VerificationIntentDigest = "amended"
	if got := Invalidated(base, next, false); !reflect.DeepEqual(got, allGates) {
		t.Fatalf("invalidated=%v want=%v", got, allGates)
	}
	if got := Invalidated(base, base, true); !reflect.DeepEqual(got, allGates) {
		t.Fatalf("owned-file invalidated=%v want=%v", got, allGates)
	}
}

func TestStaleGateCannotAuthorizeBoundary(t *testing.T) {
	current := snapshot()
	var evidence Evidence
	for _, gate := range allGates {
		var err error
		evidence, err = Bind(current, evidence, gate)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := RequireCurrent(current, evidence, allGates...); err != nil {
		t.Fatal(err)
	}

	next := current
	next.HeadSHA = "new-head"
	next.Generation++
	if err := RequireCurrent(next, evidence, GateProofResult, GateChecks, GateFinalReview, GateApproval); !errors.Is(err, ErrStaleEvidence) {
		t.Fatalf("stale evidence error=%v", err)
	}

	evidence = ApplyInvalidation(evidence, Invalidated(current, next, false))
	if evidence.ProofCandidateDigest != "" || evidence.ChecksCandidateDigest != "" || evidence.ReviewCandidateDigest != "" || evidence.ApprovalCandidateDigest != "" {
		t.Fatalf("downstream evidence survived: %+v", evidence)
	}
	if evidence.VerificationIntentDigest == "" {
		t.Fatal("unchanged verification intent was incorrectly cleared")
	}
}

func TestSnapshotDigestIsDeterministicAndRequiresEveryIdentity(t *testing.T) {
	first, err := Digest(snapshot())
	if err != nil {
		t.Fatal(err)
	}
	second, err := Digest(snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("digest changed: %q != %q", first, second)
	}
	invalid := snapshot()
	invalid.CommandPolicyDigest = ""
	if _, err := Digest(invalid); err == nil {
		t.Fatal("expected incomplete candidate rejection")
	}
}
