package processsupervisor

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestClaudeQualificationRejectsInvalidAuthorityWithoutCalls(t *testing.T) {
	s, _ := New(nil)
	defer s.Close()
	for _, leader := range []uint64{0, 1} {
		_, proof, err := s.QualifyClaude(context.Background(), "/missing", "unknown", domain.ChannelDev, leader)
		if err == nil || len(proof.Signature) != 0 {
			t.Fatal("invalid qualification signed")
		}
	}
}

func TestInstalledClaudeQualificationSignsFreshFixtureVerdict(t *testing.T) {
	if os.Getenv("SF_TEST_CLAUDE_QUALIFY") != "1" {
		t.Skip("explicit paid qualification probe")
	}
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Executable = testProviderGate(t)
	exe, err := exec.LookPath("claude")
	if err != nil {
		t.Fatal("Claude unavailable")
	}
	binding, proof, err := s.QualifyClaude(context.Background(), exe, "claude-sonnet-5", domain.ChannelDev, 1)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Identity != binding.Identity || proof.BinaryDigest != binding.BinaryDigest || !contracts.VerifyQualificationAttestation(s.PublicKey(), proof) {
		t.Fatal("qualification signature mismatch")
	}
}
