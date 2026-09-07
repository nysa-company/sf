package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/cursorprovider"
	"github.com/nysa-company/sf/internal/domain"
)

func TestClaudeRuntimeRegistrationStagesAndPinsBinding(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Claude runtime is Darwin-qualified only")
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(dir, "claude")
	if err := os.WriteFile(exe, []byte("fixture; never executed"), 0700); err != nil {
		t.Fatal(err)
	}
	bundle, err := cliruntime.Resolve(context.Background(), "claude", exe)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	bound := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"}, BinaryDigest: bundle.Digest(), PolicyDigest: s.ProviderPolicyDigest("claude"), FixtureDigest: "fixture", AuthDigest: strings.Repeat("a", 64), AuthMode: claudeprovider.AuthModeSubscription}
	if _, err := s.RegisterRuntime(bound, exe, dir); err != nil {
		t.Fatal(err)
	}
	first := s.trusted[bound.Identity].snapshot
	if first == nil || first.path == exe || !stagedRuntimeMatches(first, bundle.Digest()) {
		t.Fatal("runtime not staged")
	}
	if _, err := s.RegisterRuntime(bound, exe, dir); err != nil || first != s.trusted[bound.Identity].snapshot {
		t.Fatal("idempotent registration replaced stage", err)
	}
	for name, mutate := range map[string]func(*contracts.RuntimeBinding){
		"prior Claude policy": func(b *contracts.RuntimeBinding) {
			prior := sha256.Sum256([]byte("sf-claude-policy-v2\x00draft07-common-subset-projection\x00private-home\x00keychain-oauth-access-only\x00staged-cli\x00restricted-safe-mode\x00dontAsk\x00role-file-tools\x00no-shell-mcp\x00no-session-persistence\x00no-autoupdate\x00bounded-stdio\x00drain-owned-cleanup\x00" + contracts.MultiCLIRequestPolicy))
			b.PolicyDigest = hex.EncodeToString(prior[:])
		},
		"version": func(b *contracts.RuntimeBinding) { b.Identity.Version = "2.1.264" },
		"family":  func(b *contracts.RuntimeBinding) { b.Identity.Family = "other" },
		"model":   func(b *contracts.RuntimeBinding) { b.Identity.Model = "sonnet" },
		"policy":  func(b *contracts.RuntimeBinding) { b.PolicyDigest = s.PolicyDigest() },
		"binary":  func(b *contracts.RuntimeBinding) { b.BinaryDigest = strings.Repeat("b", 64) },
		"api":     func(b *contracts.RuntimeBinding) { b.AuthMode = "api" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := bound
			mutate(&bad)
			if _, err := s.RegisterRuntime(bad, exe, dir); err == nil {
				t.Fatal("invalid binding admitted")
			}
			if s.trusted[bound.Identity].snapshot != first {
				t.Fatal("failed registration replaced prior runtime")
			}
		})
	}
}

func TestProviderPoliciesPreserveCodexAndSeparateTrustedCursor(t *testing.T) {
	var s *Supervisor
	if s.ProviderPolicyDigest("codex") != environmentPolicyDigest() || s.ProviderPolicyDigest("claude") == environmentPolicyDigest() {
		t.Fatal("policy identities conflated")
	}
	if s.ProviderPolicyDigest("cursor") == "" || s.ProviderPolicyDigest("cursor") == s.ProviderPolicyDigest("claude") || s.ProviderPolicyDigest("cursor") == s.ProviderPolicyDigest("codex") {
		t.Fatal("Cursor trust policy conflated")
	}
	for _, provider := range []string{"unknown", ""} {
		if s.ProviderPolicyDigest(provider) != "" {
			t.Fatal("unqualified policy exposed")
		}
	}
}

func TestCursorCatalogIdentityCannotAcquireAnotherProviderRuntimePolicy(t *testing.T) {
	s, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, model := range []string{"claude-sonnet-5-high", "gpt-5.6-luna-high", "cursor-grok-4.6-high"} {
		family, ok := cursorprovider.ModelFamily(model)
		if !ok {
			t.Fatal("fixture must be an observed catalog model")
		}
		for _, policy := range []string{"", s.ProviderPolicyDigest("claude"), s.ProviderPolicyDigest("codex")} {
			binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "cursor", Model: model, Family: family, Version: "2026.09.02-c22c1a3"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: policy, FixtureDigest: strings.Repeat("b", 64), AuthDigest: strings.Repeat("c", 64), AuthMode: cursorprovider.AuthModeBrowser}
			if _, err := s.RegisterRuntime(binding, "/unavailable/cursor-agent", "/unavailable/auth"); err == nil {
				t.Fatal("Cursor catalog/login identity acquired unqualified execution")
			}
		}
	}
	if len(s.trusted) != 0 {
		t.Fatal("failed Cursor registration retained runtime authority")
	}
}
