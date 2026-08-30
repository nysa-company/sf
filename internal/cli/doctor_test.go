package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
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

func TestDoctorDoesNotEchoRepositoryPath(t *testing.T) {
	root := t.TempDir()
	secretPath := filepath.Join(root, "repo-with-secret-token")
	report := RunDoctor(context.Background(), DoctorDeps{
		Channel:  domain.ChannelDev,
		Binary:   "sf-dev",
		Repo:     secretPath,
		Paths:    config.ChannelPaths{Root: root},
		Lookup:   func(string) (string, error) { return "/bin/tool", nil },
		StatFS:   func(string) (*syscall.Statfs_t, error) { return &syscall.Statfs_t{Bavail: 100_000, Bsize: 4096}, nil },
		Worktree: func(context.Context, string) error { return errors.New("not a worktree") },
	})
	response := reportResponse(report)
	data := string(response.Data)
	if strings.Contains(data, secretPath) {
		t.Fatalf("doctor report leaked repository path: %s", data)
	}
	if response.NextAction == nil || len(response.NextAction.Argv) != 2 || response.NextAction.Argv[1] != "doctor" {
		t.Fatalf("doctor action was not generic: %+v", response.NextAction)
	}
}

func TestDoctorReportsSelectedPairAndRequiredAuthentication(t *testing.T) {
	deps := healthyDoctorDeps(t)
	deps.Pair = func(context.Context, domain.Channel) (store.ProviderPair, error) {
		return qualifiedDoctorPair(), nil
	}
	deps.AuthStatus = func(context.Context) []localauth.Status {
		return []localauth.Status{
			authenticatedDoctorStatus(localauth.GitHub, "gh", "gh version 2.98.0"),
			authenticatedDoctorStatus(localauth.Cursor, "cursor-agent", "cursor-agent 1.7.0"),
			authenticatedDoctorStatus(localauth.Claude, "claude", "claude 2.1.0"),
			{Provider: localauth.Codex, Executable: "codex", State: localauth.StateUnavailable},
		}
	}
	report := RunDoctor(context.Background(), deps)
	if report.ProviderPair == nil || !report.ProviderPair.Independent {
		t.Fatalf("provider pair=%+v", report.ProviderPair)
	}
	if report.ProviderPair.Builder.Provider != "cursor" || report.ProviderPair.Builder.Family != "cursor-family" ||
		report.ProviderPair.Reviewer.Provider != "claude" || report.ProviderPair.Reviewer.Qualification != store.QualificationGuarded {
		t.Fatalf("provider pair=%+v", report.ProviderPair)
	}
	if len(report.Authentication) != len(localauth.Providers()) {
		t.Fatalf("authentication=%+v", report.Authentication)
	}
	for _, id := range []string{"authority_database", "provider_pair", "github_auth", "builder_auth", "reviewer_auth"} {
		if check := doctorCheckByID(t, report, id); check.Status != CheckPass {
			t.Fatalf("check %s=%+v", id, check)
		}
	}
	if report.AutonomousEligible || report.CredentialsStored {
		t.Fatalf("unsafe policy flags=%+v", report)
	}
}

func TestDoctorFailsClosedAndRedactsInjectedAuthentication(t *testing.T) {
	const secret = "super-secret-authentication-value"
	deps := healthyDoctorDeps(t)
	deps.AuthStatus = func(context.Context) []localauth.Status {
		return []localauth.Status{
			{Provider: localauth.GitHub, Executable: "gh", Installed: true, Authenticated: true, State: localauth.StateAuthenticated, Version: "gh 2.98\n" + secret, Reason: secret},
			{Provider: localauth.Cursor, Executable: "cursor-agent", State: localauth.StateUnavailable, Reason: secret},
			{Provider: localauth.Claude, Executable: "claude", State: localauth.StateUnavailable, Reason: secret},
			{Provider: localauth.Codex, Executable: "codex", State: localauth.StateUnavailable, Reason: secret},
		}
	}
	report := RunDoctor(context.Background(), deps)
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), secret) {
		t.Fatalf("doctor leaked injected authentication output: %s", encoded)
	}
	if check := doctorCheckByID(t, report, "authentication"); check.Status != CheckFail {
		t.Fatalf("authentication check=%+v", check)
	}
	if check := doctorCheckByID(t, report, "github_auth"); check.Status != CheckFail || check.NextAction == nil || check.NextAction.Argv[0] != "sf-dev" {
		t.Fatalf("github auth check=%+v", check)
	}
}

func TestDoctorRejectsUnsafeOrDependentProviderPairWithoutEcho(t *testing.T) {
	const secret = "password=super-secret-provider-value"
	deps := healthyDoctorDeps(t)
	pair := qualifiedDoctorPair()
	pair.Builder.Provider.Model = secret
	pair.Reviewer.Provider.Family = pair.Builder.Provider.Family
	deps.Pair = func(context.Context, domain.Channel) (store.ProviderPair, error) { return pair, nil }
	report := RunDoctor(context.Background(), deps)
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if report.ProviderPair != nil || strings.Contains(string(encoded), secret) {
		t.Fatalf("unsafe pair was exposed: %s", encoded)
	}
	if check := doctorCheckByID(t, report, "provider_pair"); check.Status != CheckFail || check.NextAction == nil || check.NextAction.Argv[0] != "sf-dev" {
		t.Fatalf("provider pair check=%+v", check)
	}
}

func healthyDoctorDeps(t *testing.T) DoctorDeps {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return DoctorDeps{
		Channel: domain.ChannelDev,
		Binary:  "sf-dev",
		Paths:   config.ChannelPaths{Root: root, Socket: filepath.Join(root, "missing.sock")},
		Lookup:  func(string) (string, error) { return "/bin/tool", nil },
		StatFS:  func(string) (*syscall.Statfs_t, error) { return &syscall.Statfs_t{Bavail: 100_000, Bsize: 4096}, nil },
		Pair: func(context.Context, domain.Channel) (store.ProviderPair, error) {
			return store.ProviderPair{}, store.ErrNotFound
		},
	}
}

func qualifiedDoctorPair() store.ProviderPair {
	created := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	return store.ProviderPair{
		Channel: domain.ChannelDev, SelectedAt: created,
		Builder: store.ProviderQualification{
			ID: 1, Channel: domain.ChannelDev, Provider: domain.ProviderIdentity{Provider: "cursor", Model: "cursor-model", Family: "cursor-family", Version: "1.7.0"},
			Profile: store.QualificationGuarded, CreatedAt: created,
		},
		Reviewer: store.ProviderQualification{
			ID: 2, Channel: domain.ChannelDev, Provider: domain.ProviderIdentity{Provider: "claude", Model: "claude-model", Family: "claude-family", Version: "2.1.0"},
			Profile: store.QualificationGuarded, CreatedAt: created,
		},
	}
}

func authenticatedDoctorStatus(provider localauth.Provider, executable, version string) localauth.Status {
	return localauth.Status{Provider: provider, Executable: executable, Installed: true, Authenticated: true, State: localauth.StateAuthenticated, Version: version}
}

func doctorCheckByID(t *testing.T, report DoctorReport, id string) DoctorCheck {
	t.Helper()
	for _, check := range report.Checks {
		if check.ID == id {
			return check
		}
	}
	t.Fatalf("doctor check %q was absent: %+v", id, report.Checks)
	return DoctorCheck{}
}
