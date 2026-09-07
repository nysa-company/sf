package multiprovider

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

func TestMixedSelectionRequiresTwoCurrentIndependentQualifications(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "mixed-selection")
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := contracts.NewDrainSigner()
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, signer.PublicKey()); err != nil {
		t.Fatal(err)
	}
	makeQ := func(provider, family, mode, run string) store.ProviderQualification {
		t.Helper()
		now := time.Now().UTC()
		identity := domain.ProviderIdentity{Provider: provider, Model: "fixture", Family: family, Version: "fixture"}
		d := strings.Repeat("d", 64)
		proof, err := signer.SignQualification(contracts.QualificationAttestation{Channel: domain.ChannelDev, RunID: run, Identity: identity, BinaryDigest: d, PolicyDigest: d, FixtureDigest: d, AuthDigest: d, AuthMode: mode, ProbeDigest: d, Profile: contracts.ProfileGuarded, CreatedUnixNanos: now.UnixNano(), LeaderEpoch: leader, Nonce: run})
		if err != nil {
			t.Fatal(err)
		}
		q, _, err := db.RecordAttestedProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: identity, BinaryDigest: d, PolicyDigest: d, FixtureDigest: d, AuthDigest: d, AuthMode: mode, ProbeDigest: d, Profile: store.QualificationGuarded, CreatedAt: now}, proof)
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	a := makeQ("claude", "anthropic", "claude_subscription", strings.Repeat("a", 32))
	b := makeQ("codex", "openai", "chatgpt_subscription", strings.Repeat("b", 32))
	result := codexprovider.QualificationResult{Channel: domain.ChannelDev, Builder: a, Reviewer: b, ModelCallMade: true}
	if out, err := selectQualifiedPair(ctx, db, domain.ChannelDev, result); err != nil || !out.Independent {
		t.Fatal("valid pair refused", err)
	}
	for _, bad := range []store.ProviderQualification{{}, a} {
		invalid := result
		invalid.Reviewer = bad
		if _, err := selectQualifiedPair(ctx, db, domain.ChannelDev, invalid); err == nil {
			t.Fatal("invalid pair selected")
		}
		pair, err := db.ProviderPair(ctx, domain.ChannelDev)
		if err != nil || pair.Builder.ID != a.ID || pair.Reviewer.ID != b.ID {
			t.Fatal("prior pair changed", err)
		}
	}
	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = supervisor.Close() })
	inspection := errors.New("current qualification reached runtime inspection")
	_, err = composeLocal(ctx, domain.ChannelDev, db, supervisor, func([]string) ([]providercoord.RuntimeCandidate, int, error) {
		return nil, 0, inspection
	})
	if !errors.Is(err, inspection) {
		t.Fatal("current qualification skipped runtime inspection", err)
	}
	if _, err := db.AcquireLeader(ctx, domain.ChannelDev, "new-leader"); err != nil {
		t.Fatal(err)
	}
	if _, err := selectQualifiedPair(ctx, db, domain.ChannelDev, result); err == nil {
		t.Fatal("stale qualifications selected")
	}
	observed := false
	coordinator, err := composeLocal(ctx, domain.ChannelDev, db, supervisor, func([]string) ([]providercoord.RuntimeCandidate, int, error) {
		observed = true
		return nil, 0, errors.New("runtime inspection must not run for stale qualifications")
	})
	if coordinator != nil {
		defer coordinator.Close()
	}
	if err != nil || observed || coordinator == nil {
		t.Fatal("restart inspected unusable runtime instead of returning idle coordinator", err)
	}
}

func TestUnavailableSelectionsDoNotCallModels(t *testing.T) {
	for _, pair := range [][2]string{{"cursor", "codex"}, {"claude", "claude"}, {"unknown", "claude"}} {
		result, err := QualifyLocalPair(context.Background(), nil, domain.ChannelDev, pair[0], pair[1], nil)
		if err == nil || result.ModelCallMade || result.Independent {
			t.Fatal("unsupported pair admitted")
		}
	}
}

func TestExactModelPreflightRejectsAliasesAndSameFamilyBeforeIO(t *testing.T) {
	t.Setenv("SF_CODEX_REVIEWER_MODEL", "gpt-5.6-luna")
	for _, tc := range []struct{ b, r, bm, rm string }{
		{"claude", "codex", "sonnet", "gpt-5.6-luna"},
		{"claude", "codex", "claude-sonnet-5", "auto"},
		{"codex", "codex", "gpt-5.6-sol", "gpt-5.6-luna"},
		{"codex", "codex", "gpt-5.6-sol", ""},
		{"cursor", "codex", "gpt-5.6-luna-low", "gpt-5.6-luna"},
		{"claude", "cursor", "claude-sonnet-5", "claude-sonnet-5-low"},
		{"cursor", "cursor", "cursor-grok-4.6-low", "cursor-grok-4.6-high"},
		{"cursor", "claude", "auto", "claude-sonnet-5"},
	} {
		// Deliberately unopened authorities: any attempted qualification IO
		// instead of closed-catalog preflight makes this test fail.
		result, err := QualifyLocalModels(context.Background(), &store.Store{}, domain.ChannelDev, tc.b, tc.r, tc.bm, tc.rm, &processsupervisor.Supervisor{})
		if err == nil || result.ModelCallMade || result.Independent {
			t.Fatal("unsafe model selection reached qualification")
		}
	}
}

func TestCursorModelResolutionKeepsExactRoleAndFamily(t *testing.T) {
	for _, tc := range []struct{ role, explicit, want, family string }{
		{"builder", "", "gpt-5.6-luna-low", "openai-gpt-5.6"},
		{"reviewer", "", "claude-sonnet-5-low", "anthropic-claude"},
		{"reviewer", "cursor-grok-4.6-low", "cursor-grok-4.6-low", "xai-grok"},
	} {
		model, family, err := resolveLocalModel("cursor", tc.role, tc.explicit)
		if err != nil || model != tc.want || family != tc.family {
			t.Fatal("exact role selection changed")
		}
	}
	if _, _, err := resolveLocalModel("cursor", "builder", "luna"); err == nil {
		t.Fatal("alias admitted")
	}
}
