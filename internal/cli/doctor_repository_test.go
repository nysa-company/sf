package cli

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type doctorRepositoryDiagnostic string

func (d doctorRepositoryDiagnostic) Error() string                 { return "password=synthetic-secret" }
func (d doctorRepositoryDiagnostic) RuntimeDiagnosticCode() string { return string(d) }

func TestDoctorRepositoryReadIsBoundedSanitizedAndDistinctFromCredentialProbe(t *testing.T) {
	for _, tc := range []struct {
		name               string
		baseErr, accessErr error
		status             CheckStatus
		calls              int
	}{
		{"pass", nil, nil, CheckPass, 1},
		{"helper_pass_remote_fail", nil, errors.New("password=synthetic-secret"), CheckFail, 1},
		{"no_registered_base", store.ErrNotFound, nil, CheckNotRun, 0},
		{"unreadable_registration", errors.New("secret registration"), nil, CheckFail, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			deps := DoctorDeps{Channel: domain.ChannelDev, Binary: "sf-dev", Repo: "/fixture/repo",
				RepositoryBase: func(context.Context, string) (string, error) { return "release", tc.baseErr },
				RepositoryAccess: func(ctx context.Context, repository, base string) error {
					calls++
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) > 15*time.Second || repository != "/fixture/repo" || base != "release" {
						t.Fatal("probe missing exact identity or deadline")
					}
					return tc.accessErr
				},
			}
			report := DoctorReport{Checks: []DoctorCheck{{ID: "repository_worktree", Status: CheckPass}, {ID: "git_transport", Status: CheckPass}, {ID: "https_credentials", Status: CheckPass}}}
			checkDoctorRepositoryAccess(context.Background(), deps, &report)
			check := doctorCheckByID(t, report, "repository_access")
			if check.Status != tc.status || calls != tc.calls {
				t.Fatalf("check=%+v calls=%d", check, calls)
			}
			encoded, err := json.Marshal(report)
			if err != nil || strings.Contains(string(encoded), "secret") {
				t.Fatalf("unsafe report=%s err=%v", encoded, err)
			}
		})
	}
}

func TestDoctorRepositoryReadNotConfiguredDoesNotProbe(t *testing.T) {
	deps := (DoctorDeps{Channel: domain.ChannelDev}).defaults()
	if deps.RepositoryAccess != nil || deps.RepositoryBase != nil {
		t.Fatal("defaults installed an ambient repository probe")
	}
	report := DoctorReport{}
	checkDoctorRepositoryAccess(context.Background(), DoctorDeps{}, &report)
	if check := doctorCheckByID(t, report, "repository_access"); check.Status != CheckNotRun {
		t.Fatalf("check=%+v", check)
	}
}

func TestDoctorRepositoryReadFailureBlocksGuardedEligibility(t *testing.T) {
	deps := healthyDoctorDeps(t)
	deps.Pair = func(context.Context, domain.Channel) (store.ProviderPair, error) { return qualifiedDoctorPair(), nil }
	deps.AuthStatus = completeDoctorAuth
	deps.Attempts = func(context.Context, domain.Channel) ([]store.ProviderAttempt, error) { return nil, nil }
	deps.Repo = "/fixture/repo"
	deps.Worktree = func(context.Context, string) error { return nil }
	deps.Origin = func(context.Context, string) (string, error) { return "https://github.com/owner/repo.git", nil }
	deps.HTTPSCredentials = func(context.Context, string) error { return nil }
	deps.RepositoryBase = func(context.Context, string) (string, error) { return "release", nil }
	deps.RepositoryAccess = func(context.Context, string, string) error { return nil }
	if report := RunDoctor(context.Background(), deps); !report.GuardedEligible {
		t.Fatalf("passing fixture not eligible: %+v", report.Checks)
	}
	deps.RepositoryAccess = func(context.Context, string, string) error { return errors.New("remote read refused") }
	if report := RunDoctor(context.Background(), deps); report.GuardedEligible {
		t.Fatal("failed repository read remained eligible")
	}
}

func TestDoctorRepositoryDiagnosticOnlyRendersKnownGitReasons(t *testing.T) {
	for _, code := range []string{"git_remote_read", "git_missing_base", "password=synthetic-secret"} {
		deps := DoctorDeps{Repo: "/fixture/repo", Binary: "sf-dev",
			RepositoryBase:   func(context.Context, string) (string, error) { return "release", nil },
			RepositoryAccess: func(context.Context, string, string) error { return doctorRepositoryDiagnostic(code) },
		}
		report := DoctorReport{Checks: []DoctorCheck{{ID: "repository_worktree", Status: CheckPass}, {ID: "git_transport", Status: CheckPass}}}
		checkDoctorRepositoryAccess(context.Background(), deps, &report)
		check := doctorCheckByID(t, report, "repository_access")
		if want := contracts.RuntimeDiagnosticSummary(code); want != "" && check.Summary != want {
			t.Fatalf("summary=%q want=%q", check.Summary, want)
		}
		if strings.Contains(check.Summary, "secret") {
			t.Fatal("unknown reason leaked")
		}
	}
}

func TestDoctorRepositoryReadPreservesCancellationAndShorterDeadline(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if timeout {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		} else {
			cancel()
		}
		deps := DoctorDeps{Repo: "/fixture/repo", Binary: "sf-dev",
			RepositoryBase: func(context.Context, string) (string, error) { return "release", nil },
			RepositoryAccess: func(probe context.Context, _, _ string) error {
				if probe.Err() == nil {
					t.Fatal("caller cancellation/deadline was lost")
				}
				return probe.Err()
			},
		}
		report := DoctorReport{Checks: []DoctorCheck{{ID: "repository_worktree", Status: CheckPass}, {ID: "git_transport", Status: CheckPass}}}
		checkDoctorRepositoryAccess(ctx, deps, &report)
		cancel()
		if check := doctorCheckByID(t, report, "repository_access"); check.Status != CheckFail {
			t.Fatalf("check=%+v", check)
		}
	}
}

func TestDoctorRegisteredBaseUsesAuthenticatedSnapshotWithoutGuessing(t *testing.T) {
	projectConfig := config.DefaultProject("fixture", "/fixture/repo")
	projectConfig.BaseBranch = "release"
	frozen, err := config.Resolve(config.DefaultMachineLimits(), projectConfig, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, digest, err := config.Snapshot(frozen)
	if err != nil {
		t.Fatal(err)
	}
	project := store.Project{Channel: domain.ChannelDev, ID: "fixture", Path: "/fixture/repo", BaseRef: "release", ConfigGeneration: 1, ConfigSnapshot: snapshot, ConfigDigest: digest}
	if base, err := doctorRegisteredBase([]store.Project{project}, domain.ChannelDev, project.Path); err != nil || base != "release" {
		t.Fatalf("base=%q err=%v", base, err)
	}
	if _, err := doctorRegisteredBase(nil, domain.ChannelDev, project.Path); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing registration=%v", err)
	}
	for _, change := range []string{"base", "digest", "generation", "channel", "duplicate"} {
		t.Run(change, func(t *testing.T) {
			p := project
			switch change {
			case "base":
				p.BaseRef = "main"
			case "digest":
				p.ConfigDigest = strings.Repeat("0", 64)
			case "generation":
				p.ConfigGeneration = 0
			case "channel":
				p.Channel = domain.ChannelStable
			}
			projects := []store.Project{p}
			if change == "duplicate" {
				projects = append(projects, p)
			}
			if _, err := doctorRegisteredBase(projects, domain.ChannelDev, p.Path); err == nil || errors.Is(err, store.ErrNotFound) {
				t.Fatalf("invalid registration=%v", err)
			}
		})
	}
}
