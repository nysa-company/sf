package config

import (
	"bytes"
	"strings"
	"testing"
)

func TestRewriteProviderPresetPreservesUnrelatedSource(t *testing.T) {
	for _, source := range []string{
		"# keep comment\nphase_timeout = '60s'\n[commands]\nverify = ['go','test','./...'] # proof\n",
		"# keep comment\n[providers] # pair\nplanner=['codex'] # planner\nbuilder = [\n 'codex',\n]\nreviewer=['codex']\n[commands]\nverify=['go','test'] # proof\n",
		"# keep comment\nproviders = {planner=['codex'], builder=['codex'], reviewer=['codex']} # pair\nphase_timeout='60s'\n",
		"# keep comment\nproviders.planner=['codex']\n[commands]\nverify=['go','test'] # proof\n",
		"# keep comment\n['providers']\n'planner'=['codex']\n[commands]\nverify=['go','test'] # proof\n",
	} {
		for _, preset := range []string{"claude-codex", "codex-claude", "codex-codex"} {
			result, err := RewriteProviderPreset([]byte(source), preset)
			if err != nil {
				t.Fatalf("source=%q preset=%s: %v", source, preset, err)
			}
			if !bytes.Contains(result, []byte("# keep comment")) {
				t.Fatal("unrelated comment lost")
			}
			if strings.Contains(source, "# proof") && !bytes.Contains(result, []byte("# proof")) {
				t.Fatal("command comment lost")
			}
			again, err := RewriteProviderPreset(result, preset)
			if err != nil || !bytes.Equal(result, again) {
				t.Fatalf("rewrite not idempotent: %v", err)
			}
		}
	}
}

func TestRewriteProviderPresetRejectsInvalidSourceAndSelection(t *testing.T) {
	for _, source := range []string{"unknown = true", "[providers]\nbuilder=['codex']\nbuilder=['claude']", "providers = 'invalid'", strings.Repeat("x", MaxFileBytes+1)} {
		if _, err := RewriteProviderPreset([]byte(source), "claude-codex"); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	for _, preset := range []string{"", "select", "cursor-unknown", "unknown"} {
		if _, err := RewriteProviderPreset([]byte("# config\n"), preset); err == nil {
			t.Fatal("invalid preset accepted")
		}
	}
}
