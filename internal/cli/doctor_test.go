package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
)

func TestDoctorHumanOutputIncludesTypedFailureDetails(t *testing.T) {
	report := DoctorReport{
		Schema:  doctorSchema,
		Channel: domain.ChannelDev,
		Checks:  []DoctorCheck{{ID: "gh_executable", Status: CheckFail, Summary: "gh executable is unavailable", NextAction: &domain.NextAction{Code: "gh_executable", Argv: []string{"sf-dev", "auth", "login", "github"}}}},
	}
	response := reportResponse(report)
	var output bytes.Buffer
	if err := Render(&output, response, false); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(output.Bytes(), []byte(`"gh_executable"`)) || bytes.Count(output.Bytes(), []byte("Next:")) != 1 {
		t.Fatalf("output=%q", output.String())
	}
}

func TestDoctorUsesInjectedReadOnlyRegistryAndDisablesAutonomy(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := config.ChannelPaths{Root: root, Socket: filepath.Join(root, "run", "sf.sock")}
	deps := DoctorDeps{
		Channel: domain.ChannelDev,
		Paths:   paths,
		Binary:  "sf-dev",
		Lookup: func(name string) (string, error) {
			return "/usr/bin/" + name, nil
		},
		StatFS: func(string) (*syscall.Statfs_t, error) {
			return &syscall.Statfs_t{Bavail: 100_000, Bsize: 4096}, nil
		},
		Worktree: func(context.Context, string) error { return errors.New("must not run without --repo") },
	}
	report := RunDoctor(context.Background(), deps)
	if report.AutonomousEligible {
		t.Fatal("doctor must fail closed for autonomous mode")
	}
	for _, check := range report.Checks {
		if check.Status == CheckFail {
			t.Fatalf("unexpected check failure: %+v", check)
		}
	}
	if report.Schema != doctorSchema || report.Channel != domain.ChannelDev {
		t.Fatalf("report identity=%+v", report)
	}
}

func TestDoctorFailureContainsSafeActionPerFailingCheck(t *testing.T) {
	root := t.TempDir()
	deps := DoctorDeps{
		Channel: domain.ChannelStable,
		Binary:  "sf",
		Paths:   config.ChannelPaths{Root: root, Socket: filepath.Join(root, "missing.sock")},
		Lookup: func(name string) (string, error) {
			if name == "gh" {
				return "", errors.New("missing")
			}
			return "/usr/bin/" + name, nil
		},
		StatFS: func(string) (*syscall.Statfs_t, error) {
			return &syscall.Statfs_t{Bavail: 100_000, Bsize: 4096}, nil
		},
	}
	report := RunDoctor(context.Background(), deps)
	var found bool
	for _, check := range report.Checks {
		if check.Status == CheckFail {
			found = true
			if check.NextAction == nil || len(check.NextAction.Argv) == 0 || check.NextAction.Argv[0] != "sf" {
				t.Fatalf("failing check lacks safe action: %+v", check)
			}
		}
	}
	if !found {
		t.Fatal("expected a doctor failure")
	}
}

func TestDoctorRejectsWorldReadableChannelRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	report := RunDoctor(context.Background(), DoctorDeps{
		Channel: domain.ChannelDev,
		Binary:  "sf-dev",
		Paths:   config.ChannelPaths{Root: root},
		Lookup:  func(string) (string, error) { return "/bin/tool", nil },
		StatFS:  func(string) (*syscall.Statfs_t, error) { return &syscall.Statfs_t{Bavail: 100_000, Bsize: 4096}, nil },
	})
	for _, check := range report.Checks {
		if check.ID == "channel_root" && check.Status == CheckFail {
			return
		}
	}
	t.Fatal("expected channel root mode failure")
}
