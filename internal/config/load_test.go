package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestLoadProjectDefaultsWithoutRepositoryFile(t *testing.T) {
	repository := t.TempDir()
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test\n\ngo 1.25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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

func TestLoadProjectRefusesNysaPackageScriptsForLocalV1(t *testing.T) {
	repository := t.TempDir()
	packageJSON := `{"name":"nysa-app","private":true,"workspaces":["packages/*"],"scripts":{"test":"npm run test:all","build":"npm run build:all","lint":"eslint ."},"engines":{"node":">=22"}}`
	if err := os.WriteFile(filepath.Join(repository, "package.json"), []byte(packageJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := LoadProject(repository, "nysa", DefaultMachineLimits()); !errors.Is(err, ErrCommandDetection) || !strings.Contains(err.Error(), "credential-free CI") {
		t.Fatalf("npm local detection error=%v", err)
	}
}

func TestLoadProjectAutoDetectsGoRepository(t *testing.T) {
	repository := t.TempDir()
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test\n\ngo 1.25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	effective, _, _, err := LoadProject(repository, "go-project", DefaultMachineLimits())
	if err != nil {
		t.Fatal(err)
	}
	for name, command := range map[string]Command{"verify": effective.Commands.Verify, "review": effective.Commands.Review} {
		if got := command.Argv; len(got) != 3 || got[0] != "go" || got[1] != "test" || got[2] != "./..." {
			t.Fatalf("%s argv=%q", name, got)
		}
	}
}

func TestLoadProjectAutoDetectsDependencyFreeNode22Recipe(t *testing.T) {
	repository := t.TempDir()
	if err := os.Mkdir(filepath.Join(repository, "test"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "package.json"), []byte(`{"name":"proof"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "test", "smoke.test.mjs"), []byte(`import test from "node:test"; test("ok", () => {});`), 0o600); err != nil {
		t.Fatal(err)
	}
	effective, _, _, err := LoadProject(repository, "node-project", DefaultMachineLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got := effective.Commands.Verify.Argv; len(got) != 2 || got[0] != "node" || got[1] != "--test" {
		t.Fatalf("node argv=%q", got)
	}
}

func TestLoadProjectGoDetectionRequiresBoundedLocalClosure(t *testing.T) {
	tests := map[string]struct {
		goMod  string
		vendor bool
		want   bool
	}{
		"dependency free":         {goMod: "module example.test/proof\n\ngo 1.25\n", want: true},
		"external without vendor": {goMod: "module example.test/proof\n\ngo 1.25\n\nrequire example.test/dep v1.0.0\n", want: false},
		"valid vendor":            {goMod: "module example.test/proof\n\ngo 1.25\n\nrequire example.test/dep v1.0.0\n", vendor: true, want: true},
		"malformed marker":        {goMod: "require example.test/dep v1.0.0\n", want: false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			repository := t.TempDir()
			if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte(test.goMod), 0o600); err != nil {
				t.Fatal(err)
			}
			if test.vendor {
				if err := os.Mkdir(filepath.Join(repository, "vendor"), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(repository, "vendor", "modules.txt"), []byte("# example.test/dep v1.0.0\n## explicit\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			_, _, _, err := LoadProject(repository, "proof", DefaultMachineLimits())
			if test.want && err != nil {
				t.Fatal(err)
			}
			if !test.want && (!errors.Is(err, ErrCommandDetection) || !strings.Contains(err.Error(), "CI takeover")) {
				t.Fatalf("closure refusal=%v", err)
			}
		})
	}
}

func TestLoadProjectGoDetectionRefusesSymlinkedAndOversizedClosureMarkers(t *testing.T) {
	t.Run("symlinked go mod", func(t *testing.T) {
		repository := t.TempDir()
		target := filepath.Join(t.TempDir(), "go.mod")
		if err := os.WriteFile(target, []byte("module example.test/proof\n\ngo 1.25\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(repository, "go.mod")); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := LoadProject(repository, "proof", DefaultMachineLimits()); !errors.Is(err, ErrCommandDetection) {
			t.Fatalf("symlinked go.mod error=%v", err)
		}
	})
	t.Run("oversized go mod", func(t *testing.T) {
		repository := t.TempDir()
		contents := "module example.test/proof\n\ngo 1.25\n" + strings.Repeat("# padding\n", int((1<<20)/10)+1)
		if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := LoadProject(repository, "proof", DefaultMachineLimits()); !errors.Is(err, ErrCommandDetection) {
			t.Fatalf("oversized go.mod error=%v", err)
		}
	})
}

func TestLoadProjectDetectionRefusesAmbiguousUnsupportedMissingScriptsAndSymlinks(t *testing.T) {
	tests := map[string]func(string) error{
		"ambiguous": func(repository string) error {
			if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test\n"), 0o600); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(repository, "package.json"), []byte(`{"scripts":{"test":"ok","build":"ok"}}`), 0o600)
		},
		"unsupported": func(string) error { return nil },
		"missing scripts": func(repository string) error {
			return os.WriteFile(filepath.Join(repository, "package.json"), []byte(`{"name":"missing"}`), 0o600)
		},
		"malformed test script": func(repository string) error {
			return os.WriteFile(filepath.Join(repository, "package.json"), []byte(`{"scripts":{"test":null,"build":"ok"}}`), 0o600)
		},
		"malformed build script": func(repository string) error {
			return os.WriteFile(filepath.Join(repository, "package.json"), []byte(`{"scripts":{"test":"ok","build":{}}}`), 0o600)
		},
		"empty script": func(repository string) error {
			return os.WriteFile(filepath.Join(repository, "package.json"), []byte(`{"scripts":{"test":"   ","build":"ok"}}`), 0o600)
		},
		"symlink": func(repository string) error {
			target := filepath.Join(t.TempDir(), "package.json")
			if err := os.WriteFile(target, []byte(`{"scripts":{"test":"ok","build":"ok"}}`), 0o600); err != nil {
				return err
			}
			return os.Symlink(target, filepath.Join(repository, "package.json"))
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			repository := t.TempDir()
			if err := prepare(repository); err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := LoadProject(repository, "project", DefaultMachineLimits()); err == nil || !errors.Is(err, ErrCommandDetection) {
				t.Fatalf("detection error=%v", err)
			}
		})
	}
}

func TestLoadProjectExplicitCommandsOverrideRepositoryDetection(t *testing.T) {
	repository := t.TempDir()
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(repository, ".sf"), 0o700); err != nil {
		t.Fatal(err)
	}
	configuration := "[commands]\nverify=[\"npm\",\"test\"]\nreview=[\"npm\",\"run\",\"build\"]\n"
	if err := os.WriteFile(filepath.Join(repository, ".sf", "config.toml"), []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	effective, _, _, err := LoadProject(repository, "project", DefaultMachineLimits())
	if err != nil || effective.Commands.Verify.Argv[0] != "npm" {
		t.Fatalf("explicit effective=%+v err=%v", effective, err)
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
