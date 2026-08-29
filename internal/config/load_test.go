package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestLoadProjectDefaultsWithoutRepositoryFile(t *testing.T) {
	repository := t.TempDir()
	effective, snapshot, digest, err := LoadProject(repository, "nysa", DefaultMachineLimits())
	if err != nil {
		t.Fatal(err)
	}
	if effective.Repository != repository || effective.MergeMode != domain.MergeGuarded || effective.BaseBranch != "main" {
		t.Fatalf("effective=%+v", effective)
	}
	if len(snapshot) == 0 || len(digest) != 64 {
		t.Fatalf("snapshot=%q digest=%q", snapshot, digest)
	}
}

func TestLoadProjectStrictlyAppliesNarrowConfiguration(t *testing.T) {
	repository := t.TempDir()
	directory := filepath.Join(repository, ".sf")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration := `
base_branch = "trunk"
merge_mode = "manual"
merge_method = "rebase"
max_concurrent_tickets = 1
phase_timeout = "10m"
ticket_timeout = "1h"
max_ticket_cost_usd = 20

[commands]
verify = ["go", "test", "./..."]
review = ["go", "test", "-race", "./..."]

[providers]
planner = ["cursor", "codex"]
builder = ["cursor"]
reviewer = ["claude"]
`
	if err := os.WriteFile(filepath.Join(directory, "config.toml"), []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	effective, _, _, err := LoadProject(repository, "nysa", DefaultMachineLimits())
	if err != nil {
		t.Fatal(err)
	}
	if effective.BaseBranch != "trunk" || effective.MergeMode != domain.MergeManual || effective.MaxConcurrentTickets != 1 || effective.PhaseTimeout != 10*time.Minute || effective.TicketTimeout != time.Hour || effective.MaxTicketCostMicroUSD != 20_000_000 {
		t.Fatalf("effective=%+v", effective)
	}
	if got := effective.Commands.Verify.Argv; len(got) != 3 || got[2] != "./..." {
		t.Fatalf("verify argv=%q", got)
	}
}

func TestLoadProjectRejectsUnknownWideningAndUnsafeFiles(t *testing.T) {
	for name, configuration := range map[string]string{
		"unknown":    `unrestricted = true`,
		"autonomous": `merge_mode = "autonomous"`,
		"bad branch": `base_branch = "../main"`,
		"duplicate":  "[providers]\nbuilder = [\"cursor\", \"cursor\"]\n",
	} {
		t.Run(name, func(t *testing.T) {
			repository := t.TempDir()
			directory := filepath.Join(repository, ".sf")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "config.toml"), []byte(configuration), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := LoadProject(repository, "nysa", DefaultMachineLimits()); err == nil {
				t.Fatal("unsafe configuration was accepted")
			}
		})
	}

	t.Run("symlink", func(t *testing.T) {
		repository := t.TempDir()
		directory := filepath.Join(repository, ".sf")
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(target, []byte(`merge_mode = "manual"`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(directory, "config.toml")); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := LoadProject(repository, "nysa", DefaultMachineLimits()); err == nil {
			t.Fatal("symlink configuration was accepted")
		}
	})
}

func TestLoadMachineUsesDefaultsAndStrictBounds(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.toml")
	defaults, err := LoadMachine(missing)
	if err != nil || defaults != DefaultMachineLimits() {
		t.Fatalf("defaults=%+v err=%v", defaults, err)
	}

	path := filepath.Join(t.TempDir(), "machine.toml")
	configuration := `
max_concurrent_tickets = 1
max_phase_timeout = "20m"
max_ticket_timeout = "2h"
max_ticket_cost_usd = 30
allow_autonomous = false
`
	if err := os.WriteFile(path, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	limits, err := LoadMachine(path)
	if err != nil {
		t.Fatal(err)
	}
	if limits.MaxConcurrentTickets != 1 || limits.MaxPhaseTimeout != 20*time.Minute || limits.MaxTicketTimeout != 2*time.Hour || limits.MaxTicketCostMicroUSD != 30_000_000 || limits.AllowAutonomous {
		t.Fatalf("limits=%+v", limits)
	}

	if err := os.WriteFile(path, []byte(`unknown = true`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMachine(path); err == nil {
		t.Fatal("unknown machine setting was accepted")
	}
}

func TestConfigurationFileSizeIsBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "machine.toml")
	if err := os.WriteFile(path, []byte(strings.Repeat("#", MaxFileBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMachine(path); err == nil {
		t.Fatal("oversized configuration was accepted")
	}
}
