package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/pythonprepare"
)

func TestPythonProfileUsesLockedExactConfigWithoutOverwrite(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "tests"), 0700); err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareNysaPureConfigContext(t.Context(), root, PythonPytestV1Profile, "tests")
	if err != nil {
		t.Fatal(err)
	}
	if created, err := plan.Install(); err != nil || !created {
		t.Fatal(created, err)
	}
	if err := plan.ValidateUnchanged(); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	loaded, _, _, err := LoadProject(root, "python", DefaultMachineLimits())
	if err != nil {
		t.Fatal(err)
	}
	want, _ := pythonprepare.RecipeArgv("tests")
	if !sameStrings(loaded.Commands.Verify.Argv, want) || !sameStrings(loaded.Commands.Review.Argv, want) {
		t.Fatal("profile did not bind both commands")
	}
	before, err := os.ReadFile(filepath.Join(root, ".sf/config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err = PrepareNysaPureConfigContext(t.Context(), root, PythonPytestV1Profile, "tests")
	if err != nil {
		t.Fatal(err)
	}
	if created, err := plan.Install(); err != nil || created {
		t.Fatal("replay replaced config", created, err)
	}
	plan.Close()
	if err := os.WriteFile(filepath.Join(root, "other.py"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if changed, err := PrepareNysaPureConfigContext(t.Context(), root, PythonPytestV1Profile, "other.py"); err == nil {
		changed.Close()
		t.Fatal("different profile accepted")
	}
	after, err := os.ReadFile(filepath.Join(root, ".sf/config.toml"))
	if err != nil || string(before) != string(after) {
		t.Fatal("existing configuration changed", err)
	}
}
