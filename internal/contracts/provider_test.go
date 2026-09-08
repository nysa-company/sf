package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func phaseInputCanonicalFixture() PhaseInput {
	return PhaseInput{
		Ticket:           domain.TicketRef{Channel: domain.ChannelDev, Project: "relay", Ticket: "SF-phase-input"},
		Phase:            domain.PhasePlanning,
		Attempt:          1,
		LeaderEpoch:      7,
		RunnerEpoch:      3,
		ExpectedVersion:  2,
		Prompt:           "produce a plan",
		Repository:       "/private/repo",
		Worktree:         "/private/worktree",
		WorktreeIdentity: "identity",
		BaseSHA:          strings.Repeat("a", 40),
		AllowedPaths:     []string{"app", "tests"},
		Provider:         domain.ProviderIdentity{Provider: "codex", Model: "gpt", Family: "openai", Version: "1"},
		AuthMode:         "chatgpt_subscription",
		Timeout:          time.Minute,
		Profile:          ProfileGuarded,
		Schema:           []byte(`{"type":"object"}`),
		RequestDigest:    "ignored while canonicalizing",
	}
}

func TestDecodeCanonicalPhaseInputAcceptsOnlyExactPreV53CanonicalPayload(t *testing.T) {
	input := phaseInputCanonicalFixture()
	legacy, err := canonicalPhaseInputV52(input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(legacy), `"Repair"`) {
		t.Fatalf("legacy payload unexpectedly contains Repair: %s", legacy)
	}
	sum := sha256.Sum256(legacy)
	digest := hex.EncodeToString(sum[:])

	decoded, err := DecodeCanonicalPhaseInput(legacy)
	if err != nil {
		t.Fatalf("legacy payload rejected: %v", err)
	}
	if decoded.Repair != nil || !PhaseInputDigestMatches(decoded, digest) {
		t.Fatalf("legacy input did not retain exact digest semantics: repair=%+v digest=%q", decoded.Repair, digest)
	}
	canonical, actual, err := CanonicalPhaseInput(decoded)
	if err != nil || string(canonical) != string(legacy) || actual != digest {
		t.Fatalf("legacy canonical replay payload=%s digest=%q err=%v", canonical, actual, err)
	}
	proposed := phaseInputCanonicalFixture()
	if !PhaseInputMatchesAuthenticatedClaim(proposed, decoded, digest) {
		t.Fatal("logical current input did not match decoded legacy claim")
	}
	proposed.Prompt = "tampered"
	if PhaseInputMatchesAuthenticatedClaim(proposed, decoded, digest) {
		t.Fatal("tampered logical input matched decoded legacy claim")
	}
	for name, mutate := range map[string]func(*PhaseInput, *PhaseInput, *string){
		"timeout": func(proposed, _ *PhaseInput, _ *string) { proposed.Timeout = 2 * time.Minute },
		"repair": func(proposed, _ *PhaseInput, _ *string) {
			proposed.Repair = &ProviderRepairContext{PriorAttempt: 1, PriorRequestDigest: strings.Repeat("a", 64)}
		},
		"claim":  func(_ *PhaseInput, claim *PhaseInput, _ *string) { claim.Prompt = "tampered" },
		"digest": func(_ *PhaseInput, _ *PhaseInput, digest *string) { *digest = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := phaseInputCanonicalFixture()
			claim, err := DecodeCanonicalPhaseInput(legacy)
			if err != nil {
				t.Fatal(err)
			}
			candidateDigest := digest
			mutate(&candidate, &claim, &candidateDigest)
			if PhaseInputMatchesAuthenticatedClaim(candidate, claim, candidateDigest) {
				t.Fatal("tampered legacy claim comparison accepted")
			}
		})
	}

	decoded.Prompt = "tampered"
	if PhaseInputDigestMatches(decoded, digest) {
		t.Fatal("mutated legacy input retained authenticated digest")
	}
	decoded, err = DecodeCanonicalPhaseInput(legacy)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Repair = &ProviderRepairContext{PriorAttempt: 1, PriorRequestDigest: strings.Repeat("a", 64)}
	if _, _, err := CanonicalPhaseInput(decoded); err == nil || PhaseInputDigestMatches(decoded, digest) {
		t.Fatal("legacy input accepted a post-decode repair context")
	}
}

func TestDecodeCanonicalPhaseInputKeepsV53AsTheOnlyNewCanonicalShape(t *testing.T) {
	input := phaseInputCanonicalFixture()
	payload, digest, err := CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), `"Repair":null`) {
		t.Fatalf("current payload omitted explicit repair field: %s", payload)
	}
	decoded, err := DecodeCanonicalPhaseInput(payload)
	if err != nil || !PhaseInputDigestMatches(decoded, digest) {
		t.Fatalf("current payload decode err=%v digest=%q", err, digest)
	}

	legacy, err := canonicalPhaseInputV52(input)
	if err != nil {
		t.Fatal(err)
	}
	withRepairWhitespace := append(append([]byte(nil), legacy[:len(legacy)-1]...), []byte(`,"Repair": null}`)...)
	if _, err := DecodeCanonicalPhaseInput(withRepairWhitespace); err == nil {
		t.Fatal("noncanonical current Repair:null whitespace representation accepted")
	}
	withWhitespace := append([]byte(" "), legacy...)
	if _, err := DecodeCanonicalPhaseInput(withWhitespace); err == nil {
		t.Fatal("noncanonical legacy whitespace accepted")
	}
}

func TestDecodeCanonicalPhaseInputKeepsCanonicalV53RepairInput(t *testing.T) {
	input := phaseInputCanonicalFixture()
	input.Repair = &ProviderRepairContext{PriorAttempt: 1, PriorRequestDigest: strings.Repeat("a", 64)}
	payload, digest, err := CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCanonicalPhaseInput(payload)
	if err != nil || decoded.Repair == nil || !PhaseInputDigestMatches(decoded, digest) {
		t.Fatalf("canonical repair input decode err=%v repair=%+v", err, decoded.Repair)
	}
}
