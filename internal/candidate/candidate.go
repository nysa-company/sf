// Package candidate owns immutable candidate identity and the universal gate
// invalidation rules. Callers cannot selectively preserve evidence after the
// source, Git identity, verification intent, proof, or command policy changes.
package candidate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/domain"
)

type Gate string

const (
	GateVerificationIntent Gate = "verification_intent"
	GateProofResult        Gate = "proof_result"
	GateChecks             Gate = "github_checks"
	GateFinalReview        Gate = "final_review"
	GateApproval           Gate = "approval"
)

var downstreamGates = []Gate{GateProofResult, GateChecks, GateFinalReview, GateApproval}
var allGates = []Gate{GateVerificationIntent, GateProofResult, GateChecks, GateFinalReview, GateApproval}

var ErrStaleEvidence = errors.New("candidate evidence is stale")

// Evidence records the candidate digest against which each gate completed.
// The verification intent has its own digest because its owned files can be
// invalidated independently of an ordinary candidate refresh.
type Evidence struct {
	VerificationIntentDigest string
	ProofCandidateDigest     string
	ChecksCandidateDigest    string
	ReviewCandidateDigest    string
	ApprovalCandidateDigest  string
}

func Validate(snapshot domain.CandidateSnapshot) error {
	if snapshot.Generation == 0 {
		return errors.New("candidate generation must be positive")
	}
	for _, field := range [][2]string{
		{"base SHA", snapshot.BaseSHA},
		{"head SHA", snapshot.HeadSHA},
		{"tree SHA", snapshot.TreeSHA},
		{"source digest", snapshot.SourceDigest},
		{"verification intent digest", snapshot.VerificationIntentDigest},
		{"proof digest", snapshot.ProofDigest},
		{"command policy digest", snapshot.CommandPolicyDigest},
		{"builder evidence digest", snapshot.BuilderEvidenceDigest},
	} {
		if field[1] == "" {
			return fmt.Errorf("candidate %s is required", field[0])
		}
	}
	return nil
}

// Digest produces the stable identity persisted alongside every gate result.
func Digest(snapshot domain.CandidateSnapshot) (string, error) {
	if err := Validate(snapshot); err != nil {
		return "", err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", fmt.Errorf("encode candidate identity: %w", err)
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// Invalidated returns a stable, duplicate-free list of gates that must be
// discarded. Any candidate identity change invalidates every downstream gate.
// A change to sf-owned verification files additionally invalidates the intent.
func Invalidated(previous, next domain.CandidateSnapshot, verificationFilesChanged bool) []Gate {
	changed := previous != next
	if !changed && !verificationFilesChanged {
		return nil
	}
	if verificationFilesChanged || previous.VerificationIntentDigest != next.VerificationIntentDigest {
		return append([]Gate(nil), allGates...)
	}
	return append([]Gate(nil), downstreamGates...)
}

func ApplyInvalidation(evidence Evidence, invalidated []Gate) Evidence {
	for _, gate := range invalidated {
		switch gate {
		case GateVerificationIntent:
			evidence.VerificationIntentDigest = ""
		case GateProofResult:
			evidence.ProofCandidateDigest = ""
		case GateChecks:
			evidence.ChecksCandidateDigest = ""
		case GateFinalReview:
			evidence.ReviewCandidateDigest = ""
		case GateApproval:
			evidence.ApprovalCandidateDigest = ""
		}
	}
	return evidence
}

// RequireCurrent fails closed when a requested operation depends on evidence
// from another candidate. It is used immediately before commit, push, PR,
// review, approval, and merge boundaries.
func RequireCurrent(snapshot domain.CandidateSnapshot, evidence Evidence, gates ...Gate) error {
	digest, err := Digest(snapshot)
	if err != nil {
		return err
	}
	for _, gate := range gates {
		var actual string
		switch gate {
		case GateVerificationIntent:
			actual = evidence.VerificationIntentDigest
			if actual != snapshot.VerificationIntentDigest {
				return fmt.Errorf("%w: %s", ErrStaleEvidence, gate)
			}
		case GateProofResult:
			actual = evidence.ProofCandidateDigest
		case GateChecks:
			actual = evidence.ChecksCandidateDigest
		case GateFinalReview:
			actual = evidence.ReviewCandidateDigest
		case GateApproval:
			actual = evidence.ApprovalCandidateDigest
		default:
			return fmt.Errorf("unknown candidate gate %q", gate)
		}
		if gate != GateVerificationIntent && actual != digest {
			return fmt.Errorf("%w: %s", ErrStaleEvidence, gate)
		}
	}
	return nil
}

func Bind(snapshot domain.CandidateSnapshot, evidence Evidence, gate Gate) (Evidence, error) {
	digest, err := Digest(snapshot)
	if err != nil {
		return Evidence{}, err
	}
	switch gate {
	case GateVerificationIntent:
		evidence.VerificationIntentDigest = snapshot.VerificationIntentDigest
	case GateProofResult:
		evidence.ProofCandidateDigest = digest
	case GateChecks:
		evidence.ChecksCandidateDigest = digest
	case GateFinalReview:
		evidence.ReviewCandidateDigest = digest
	case GateApproval:
		evidence.ApprovalCandidateDigest = digest
	default:
		return Evidence{}, fmt.Errorf("unknown candidate gate %q", gate)
	}
	return evidence, nil
}
