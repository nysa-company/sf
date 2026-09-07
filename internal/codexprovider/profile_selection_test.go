package codexprovider

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfiguredRoleAllowsLunaWithoutWeakeningCodexPair(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "codex"), []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("SF_CODEX_BUILDER_MODEL", "gpt-5.6-luna")
	t.Setenv("SF_CODEX_REVIEWER_MODEL", "gpt-5.6-luna")
	profiles := configuredProfiles()
	if len(profiles) != 2 || profiles[1].Model != "gpt-5.6-luna" {
		t.Fatal("mixed-provider Luna reviewer unavailable")
	}
	if defaultProfiles() != nil {
		t.Fatal("same-family Codex-only pair admitted")
	}
	t.Setenv("SF_CODEX_BUILDER_MODEL", "unknown-model")
	if configuredProfiles() != nil {
		t.Fatal("unknown model admitted")
	}
}

func TestSelectedModelCandidatesSurviveDifferentEnvironmentDefaults(t *testing.T) {
	fixture, _ := adapterFixture(t, "fixture", "gpt-5.6-sol")
	t.Setenv("PATH", filepath.Dir(fixture.executable))
	t.Setenv("CODEX_HOME", fixture.authHome)
	t.Setenv("SF_CODEX_BUILDER_MODEL", "gpt-5.6-luna")
	t.Setenv("SF_CODEX_REVIEWER_MODEL", "gpt-5.5")
	values, _, err := LocalRuntimeCandidatesForModels([]string{"gpt-5.6-sol", "gpt-5.6-sol"})
	if err != nil || len(values) != 1 {
		t.Fatalf("candidates=%d error=%v", len(values), err)
	}
	a, ok := values[0].Provider.(*Adapter)
	if !ok || a.model != "gpt-5.6-sol" {
		t.Fatal("selected model replaced by default")
	}
	if _, _, err := LocalRuntimeCandidatesForModels([]string{"auto"}); err == nil {
		t.Fatal("alias admitted")
	}
}
