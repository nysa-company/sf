package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func evidenceRequestFixture(purpose string) RepositoryCommandEvidenceRequest {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-command-evidence"}
	phase := domain.PhaseVerification
	if purpose == RepositoryCommandPurposePostbuildCandidate {
		phase = domain.PhaseBuild
	}
	return RepositoryCommandEvidenceRequest{
		Purpose: purpose, Ref: ref, TicketVersion: 7, LeaderEpoch: 3, RunnerEpoch: 2,
		ProviderResult:           ProviderAttemptResultKey{AttemptID: 11, Ref: ref, Phase: phase, Attempt: 2},
		VerificationIntentDigest: strings.Repeat("a", 64),
		ProofDigest:              strings.Repeat("b", 64),
		ConfigCommandDigest:      "sha256:" + strings.Repeat("c", 64),
		Worktree:                 "/tmp/nysa/command-evidence",
		WorktreeIdentity:         `{"identity":"immutable"}`,
		BaseSHA:                  strings.Repeat("d", 40),
		PolicyDigest:             "sha256:" + strings.Repeat("e", 64),
		SpecDigest:               "sha256:" + strings.Repeat("f", 64),
		ExecutablePath:           "/usr/bin/true",
		ExecutableDigest:         "sha256:" + strings.Repeat("1", 64),
	}
}

func TestPrebuildEvidenceRequestCanBeIssuedBeforeCheckpointExists(t *testing.T) {
	request := evidenceRequestFixture(RepositoryCommandPurposePrebuildVerification)
	// A pre-build command is the thing that establishes red/baseline evidence;
	// no checkpoint commit exists yet. Runtime can derive both immutable ids now.
	payload, digest, err := CanonicalRepositoryCommandEvidenceRequest(request)
	if err != nil || len(payload) == 0 || digest == "" {
		t.Fatalf("prebuild issuance request payload=%q digest=%q err=%v", payload, digest, err)
	}
	if semantic, err := RepositoryCommandEvidenceSemanticKey(request); err != nil || !strings.Contains(semantic, RepositoryCommandPurposePrebuildVerification) {
		t.Fatalf("prebuild semantic=%q err=%v", semantic, err)
	}
	request.CheckpointID = strings.Repeat("a", 40)
	if _, _, err := CanonicalRepositoryCommandEvidenceRequest(request); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("prebuild request silently bound a future checkpoint: %v", err)
	}
	postbuild := evidenceRequestFixture(RepositoryCommandPurposePostbuildCandidate)
	if _, _, err := CanonicalRepositoryCommandEvidenceRequest(postbuild); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("post-build request accepted without its current checkpoint: %v", err)
	}
	postbuild.CheckpointID = strings.Repeat("a", 40)
	if _, _, err := CanonicalRepositoryCommandEvidenceRequest(postbuild); err != nil {
		t.Fatalf("post-build checkpoint request=%v", err)
	}
}

func TestEvidenceRequestRefusesCrossAttemptTicketAndRevisionReuse(t *testing.T) {
	request := evidenceRequestFixture(RepositoryCommandPurposePrebuildVerification)
	_, digest, err := CanonicalRepositoryCommandEvidenceRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	semantic, err := RepositoryCommandEvidenceSemanticKey(request)
	if err != nil {
		t.Fatal(err)
	}
	result := RepositoryCommandResult{Claim: contracts.RepositoryCommandClaim{TicketRef: request.Ref, SemanticKey: semantic, RequestDigest: digest}}
	if err := assertCommandEvidenceRequest(request, result); err != nil {
		t.Fatalf("exact request rejected: %v", err)
	}
	changedAttempt := request
	changedAttempt.ProviderResult.Attempt++
	if err := assertCommandEvidenceRequest(changedAttempt, result); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("cross-attempt result reuse=%v", err)
	}
	changedRevision := request
	changedRevision.VerificationIntentDigest = strings.Repeat("9", 64)
	if err := assertCommandEvidenceRequest(changedRevision, result); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("cross-revision result reuse=%v", err)
	}
	changedTicket := request
	changedTicket.Ref.Ticket = "SF-other"
	changedTicket.ProviderResult.Ref = changedTicket.Ref
	if err := assertCommandEvidenceRequest(changedTicket, result); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("cross-ticket result reuse=%v", err)
	}
	postbuild := evidenceRequestFixture(RepositoryCommandPurposePostbuildCandidate)
	postbuild.CheckpointID = strings.Repeat("a", 40)
	_, postDigest, err := CanonicalRepositoryCommandEvidenceRequest(postbuild)
	if err != nil {
		t.Fatal(err)
	}
	postSemantic, err := RepositoryCommandEvidenceSemanticKey(postbuild)
	if err != nil {
		t.Fatal(err)
	}
	postResult := RepositoryCommandResult{Claim: contracts.RepositoryCommandClaim{TicketRef: postbuild.Ref, SemanticKey: postSemantic, RequestDigest: postDigest}}
	changedGeneration := postbuild
	changedGeneration.ProviderResult.Attempt++
	if err := assertCommandEvidenceRequest(changedGeneration, postResult); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("cross-builder-attempt result reuse=%v", err)
	}
}
