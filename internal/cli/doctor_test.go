package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
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
	if !bytes.Contains(output.Bytes(), []byte("gh_executable: fail")) || bytes.Count(output.Bytes(), []byte("Next:")) != 1 {
		t.Fatalf("output=%q", output.String())
	}
}

func TestDoctorHealthyHumanGolden(t *testing.T) {
	report := DoctorReport{
		Schema: doctorSchema, Channel: domain.ChannelStable,
		Checks: []DoctorCheck{
			{ID: "channel_root", Status: CheckPass, Summary: "channel root is secure"},
			{ID: "builder_auth", Status: CheckPass, Summary: "Builder authentication is active"},
		},
		Authentication: []authStatusView{{Provider: localauth.Cursor, Installed: true, Authenticated: true, State: localauth.StateAuthenticated, Version: "cursor-agent 1.2.3"}},
		ProviderPair: &DoctorProviderPair{
			Builder:     DoctorProviderQualification{Role: "builder", Provider: "cursor", Model: "cursor-model", Family: "cursor-family", Version: "1.2.3", AuthMode: "local_session", Qualification: store.QualificationGuarded},
			Reviewer:    DoctorProviderQualification{Role: "reviewer", Provider: "claude", Model: "claude-model", Family: "claude-family", Version: "2.1.0", Qualification: store.QualificationGuarded},
			Independent: true,
		},
		GuardedEligible: true, AutonomousEligible: false, CredentialsStored: false,
	}
	var output bytes.Buffer
	if err := Render(&output, reportResponse(report), false); err != nil {
		t.Fatal(err)
	}
	want := "Doctor (channel: stable)\n" +
		"Guarded eligible: true\nAutonomous eligible: false\nCredentials stored by sf: false\n" +
		"Checks:\n- channel_root: pass — channel root is secure\n- builder_auth: pass — Builder authentication is active\n" +
		"Authentication:\n- cursor: authenticated, installed=true, authenticated=true, version=cursor-agent 1.2.3\n" +
		"Provider pair (independent: true)\n" +
		"- Builder: provider=cursor, model=cursor-model, family=cursor-family, version=1.2.3, qualification=qualified_guarded, auth_mode=local_session\n" +
		"- Reviewer: provider=claude, model=claude-model, family=claude-family, version=2.1.0, qualification=qualified_guarded\n"
	if output.String() != want {
		t.Fatalf("healthy doctor output=%q, want %q", output.String(), want)
	}
}

func TestDoctorGuardedEligibilityRequiresMandatoryHostChecks(t *testing.T) {
	deps := healthyDoctorDeps(t)
	deps.Pair = func(context.Context, domain.Channel) (store.ProviderPair, error) { return qualifiedDoctorPair(), nil }
	deps.AuthStatus = func(context.Context) []localauth.Status {
		return []localauth.Status{
			authenticatedDoctorStatus(localauth.GitHub, "gh", "gh 1.0"),
			authenticatedDoctorStatus(localauth.Cursor, "cursor-agent", "cursor 1.0"),
			authenticatedDoctorStatus(localauth.Claude, "claude", "claude 1.0"),
			{Provider: localauth.Codex, Executable: "codex", State: localauth.StateUnavailable},
		}
	}
	deps.Lookup = func(name string) (string, error) {
		if name == "git" {
			return "", errors.New("git missing")
		}
		return "/bin/" + name, nil
	}
	report := RunDoctor(context.Background(), deps)
	if report.GuardedEligible {
		t.Fatalf("guarded eligibility ignored mandatory host failure: %+v", report)
	}
	if check := doctorCheckByID(t, report, "git_executable"); check.Status != CheckFail {
		t.Fatalf("git check=%+v", check)
	}
}

func TestDoctorFailedHumanGoldenIncludesEveryAction(t *testing.T) {
	report := DoctorReport{
		Schema: doctorSchema, Channel: domain.ChannelDev,
		Checks: []DoctorCheck{
			{ID: "channel_root", Status: CheckFail, Summary: "channel root is not owner-only", NextAction: &domain.NextAction{Code: "channel_root", Argv: []string{"sf-dev", "doctor"}}},
			{ID: "github_auth", Status: CheckFail, Summary: "GitHub authentication is unavailable", NextAction: &domain.NextAction{Code: "provider_auth_missing", Argv: []string{"sf-dev", "auth", "login", "github"}}},
		},
		Authentication:    []authStatusView{{Provider: localauth.GitHub, Installed: true, Authenticated: false, State: localauth.StateUnauthenticated, Reason: "official CLI reports no active authentication", NextAction: &domain.NextAction{Code: "provider_auth_missing", Argv: []string{"sf-dev", "auth", "login", "github"}}}},
		CredentialsStored: false,
	}
	var output bytes.Buffer
	if err := Render(&output, reportResponse(report), false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Error: doctor_failed:", "channel_root: fail", "Action: sf-dev doctor", "github_auth: fail", "Action: sf-dev auth login github", "Reason: official CLI reports no active authentication", "Next: sf-dev doctor"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("failed doctor output=%q missing %q", output.String(), want)
		}
	}
}

func TestDoctorHumanRendererIgnoresAdditiveFields(t *testing.T) {
	response := api.Response{Version: api.Version, RequestID: "doctor-additive", OK: true, Mutation: api.Mutation{}, Data: json.RawMessage(`{"channel":"dev","checks":[{"id":"channel_root","status":"pass","summary":"secure","future_check_detail":"ignore"}],"authentication":[],"credentials_stored_by_sf":false,"future_top_level":"ignore"}`)}
	var output bytes.Buffer
	if err := Render(&output, response, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Doctor (channel: dev)") || !strings.Contains(output.String(), "channel_root: pass") || strings.Contains(output.String(), "future_") {
		t.Fatalf("additive field leaked or known field missing: %q", output.String())
	}
}

func TestDoctorUsesInjectedReadOnlyRegistryAndDisablesAutonomy(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sf-dx-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
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

func TestDoctorUsesReadOnlyDaemonHandshakeWhenSocketIsPresent(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sf-dx-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "run", "sf.sock")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	// A regular file is sufficient to exercise the injectable handshake after
	// filesystem validation without starting a daemon or opening a socket.
	if err := os.WriteFile(socket, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	called := false
	deps := healthyDoctorDeps(t)
	deps.Paths = config.ChannelPaths{Root: root, Socket: socket}
	deps.DaemonStatus = func(context.Context, config.ChannelPaths) error {
		called = true
		return nil
	}
	report := RunDoctor(context.Background(), deps)
	if called {
		t.Fatal("daemon handshake ran for an invalid socket")
	}
	if check := doctorCheckByID(t, report, "daemon_socket"); check.Status != CheckFail {
		t.Fatalf("socket check=%+v", check)
	}

	// Use an actual owner-only Unix socket for the positive branch. Doctor's
	// injected probe remains read-only and does not require daemon internals.
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(root, "run", "sf2.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	validSocket := listener.Addr().String()
	if err := os.Chmod(validSocket, 0o600); err != nil {
		t.Fatal(err)
	}
	deps.Paths.Socket = validSocket
	called = false
	deps.DaemonStatus = func(context.Context, config.ChannelPaths) error {
		called = true
		return nil
	}
	report = RunDoctor(context.Background(), deps)
	if !called {
		t.Fatal("daemon handshake did not run for a valid socket")
	}
	if check := doctorCheckByID(t, report, "daemon_status"); check.Status != CheckPass {
		t.Fatalf("daemon status check=%+v", check)
	}
}

func TestDoctorHandshakeFailureUsesChannelDaemonStatusAction(t *testing.T) {
	for _, test := range []struct {
		channel domain.Channel
		binary  string
	}{
		{channel: domain.ChannelStable, binary: "sf"},
		{channel: domain.ChannelDev, binary: "sf-dev"},
	} {
		t.Run(string(test.channel), func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Channel, deps.Binary = test.channel, test.binary
			root, err := os.MkdirTemp("/tmp", "sf-dx-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(root)
			_ = os.Chmod(root, 0o700)
			socket := filepath.Join(root, "sf.sock")
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := os.Chmod(socket, 0o600); err != nil {
				t.Fatal(err)
			}
			deps.Paths.Socket = socket
			deps.Paths.Root = root
			deps.DaemonStatus = func(context.Context, config.ChannelPaths) error { return errors.New("protocol mismatch") }
			report := RunDoctor(context.Background(), deps)
			check := doctorCheckByID(t, report, "daemon_status")
			if check.Status != CheckFail || check.NextAction == nil || strings.Join(check.NextAction.Argv, " ") != test.binary+" daemon status" {
				t.Fatalf("daemon handshake action=%+v", check)
			}
			if strings.Join(check.NextAction.Argv, " ") == test.binary+" doctor" {
				t.Fatal("daemon handshake failure loops back to doctor")
			}
		})
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
	if !report.GuardedEligible || report.AutonomousEligible || report.CredentialsStored {
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
