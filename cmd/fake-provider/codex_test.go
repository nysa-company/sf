package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestTakeoverVerificationEvidenceDigestBindsGeneratedSource(t *testing.T) {
	worktree := t.TempDir()
	prompt := "TICKET={\"type\":\"feature\"}\nPLAN={\"digest\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}\nSF_E2E_TAKEOVER\n"

	initialSource, err := writeCodexVerificationFixture(worktree, prompt)
	if err != nil {
		t.Fatal(err)
	}
	initial := decodeVerificationArtifact(t, prompt, initialSource)

	operatorSource := []byte("package app\n\n// operator takeover preserved\nfunc SoftwareFactoryFixture() string { return \"ready\" }\n")
	if err := os.WriteFile(filepath.Join(worktree, builderFixtureFile), operatorSource, 0o600); err != nil {
		t.Fatal(err)
	}
	freshSource, err := writeCodexVerificationFixture(worktree, prompt)
	if err != nil {
		t.Fatal(err)
	}
	fresh := decodeVerificationArtifact(t, prompt, freshSource)

	if string(initialSource) == string(freshSource) {
		t.Fatal("fresh takeover Reviewer reused the retained verification source")
	}
	if initial.EvidenceDigest == fresh.EvidenceDigest {
		t.Fatalf("distinct verification sources share evidence digest %q", fresh.EvidenceDigest)
	}
}

func decodeVerificationArtifact(t *testing.T, prompt string, source []byte) phaseartifact.Verification {
	t.Helper()
	raw, err := codexArtifact("verification", prompt, source)
	if err != nil {
		t.Fatal(err)
	}
	var artifact phaseartifact.Verification
	if err := json.Unmarshal(raw, &artifact); err != nil {
		t.Fatal(err)
	}
	return artifact
}
