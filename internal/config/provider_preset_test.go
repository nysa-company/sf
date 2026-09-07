package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCursorProviderPresetsArePreferencesOnly(t *testing.T) {
	for _, pair := range [][2]string{{"cursor", "codex"}, {"codex", "cursor"}, {"cursor", "claude"}, {"claude", "cursor"}, {"cursor", "cursor"}} {
		order, err := ProviderPreset(pair[0] + "-" + pair[1])
		want := ProviderOrder{Planner: []string{pair[0]}, Builder: []string{pair[0]}, Reviewer: []string{pair[1]}}
		if err != nil || !reflect.DeepEqual(order, want) {
			t.Fatal("exact preference changed", err)
		}
	}
}

func TestProviderPresetPreservesDetectedCommands(t *testing.T) {
	repo, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module example.test/preset\n\ngo 1.25\n"), 0600); err != nil {
		t.Fatal(err)
	}
	original, _, _, err := LoadProject(repo, "preset", DefaultMachineLimits())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareInitialConfigContext(context.Background(), repo, "", "", "claude-codex")
	if err != nil {
		t.Fatal(err)
	}
	defer plan.Close()
	projected, _, _, err := plan.LoadLockedProject("preset", DefaultMachineLimits(), nil)
	if err != nil || !reflect.DeepEqual(projected.Commands, original.Commands) {
		t.Fatal("preset changed command detection", err)
	}
	if _, err := plan.Install(); err != nil {
		t.Fatal(err)
	}
	installed, _, _, err := plan.LoadLockedProject("preset", DefaultMachineLimits(), nil)
	if err != nil || !reflect.DeepEqual(installed, projected) {
		t.Fatal("installed preset differs from preflight", err)
	}
}

func TestProviderPresetCannotHideUnsupportedStackBehindGoDefaults(t *testing.T) {
	repo, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "package.json"), []byte(`{"scripts":{"test":"npm run test:all"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := LoadProject(repo, "preset", DefaultMachineLimits()); !errors.Is(err, ErrCommandDetection) {
		t.Fatal("fixture must require explicit recipe", err)
	}
	plan, err := PrepareInitialConfigContext(context.Background(), repo, "", "", "claude-codex")
	if err == nil {
		_ = plan.Close()
		t.Fatal("preset hid unsupported recipe")
	}
	if _, err := os.Stat(filepath.Join(repo, ".sf", "config.toml")); !os.IsNotExist(err) {
		t.Fatal("failed preset installed file")
	}
}
