package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/store"
)

// Explicit paid native acceptance. This uses the compiled production gate and
// a disposable Store; it never opens the user's SF channel database or daemon.
func TestCompiledClaudeQualificationPersistsOnlyCurrentSignedVerdict(t *testing.T) {
	if runtime.GOOS != "darwin" || os.Getenv("SF_TEST_COMPILED_CLAUDE_QUALIFY") != "1" {
		t.Skip("explicit native Claude qualification acceptance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	exe, err := exec.LookPath("claude")
	if err != nil {
		t.Fatal("Claude unavailable")
	}
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "qualification.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "compiled-claude-acceptance")
	if err != nil {
		t.Fatal(err)
	}
	s, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error("supervisor cleanup unclear")
		}
	}()
	s.Executable = buildDevRuntimeBundle(t)
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, s.PublicKey()); err != nil {
		t.Fatal(err)
	}
	binding, proof, err := s.QualifyClaude(ctx, exe, "claude-sonnet-5", domain.ChannelDev, leader)
	if err != nil || !contracts.VerifyQualificationAttestation(s.PublicKey(), proof) || proof.Identity != binding.Identity {
		t.Fatal("compiled Claude qualification failed", err)
	}
	q := store.ProviderQualification{Channel: domain.ChannelDev, RunID: proof.RunID, Provider: binding.Identity,
		BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest,
		AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: proof.ProbeDigest,
		Profile: store.QualificationGuarded, CreatedAt: time.Unix(0, proof.CreatedUnixNanos).UTC()}
	for i := 0; i < 2; i++ {
		if _, _, err := db.RecordAttestedProviderQualification(ctx, q, proof); err != nil {
			t.Fatal("signed verdict or exact replay refused", err)
		}
	}
	if _, err := db.AcquireLeader(ctx, domain.ChannelDev, "replacement-leader"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.RecordAttestedProviderQualification(ctx, q, proof); err == nil {
		t.Fatal("old leader qualification accepted")
	}
}
