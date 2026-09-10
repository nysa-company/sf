package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ghrunner"
	"github.com/nysa-company/sf/internal/store"
)

func TestDoctorHTTPSProbeFollowsFetchAndPushTransport(t *testing.T) {
	const https = "https://github.com/owner/repo.git"
	const ssh = "git@github.com:owner/repo.git"
	for _, tc := range []struct {
		name, fetch, push string
		wantProbe         bool
	}{
		{"HTTPS", https, "", true},
		{"HTTPS fetch SSH push", https, ssh, true},
		{"SSH fetch HTTPS push", ssh, https, true},
		{"SSH only", ssh, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Repo = "/fixture/repo"
			deps.Worktree = func(context.Context, string) error { return nil }
			deps.Origin = func(context.Context, string) (string, error) { return tc.fetch, nil }
			deps.PushOrigin = func(context.Context, string) (string, error) { return tc.push, nil }
			deps.SSHAgent = func(context.Context) DoctorSSHAgentState { return DoctorSSHAgentReady }
			deps.SSHAssets = func() error { return nil }
			calls := 0
			deps.HTTPSCredentials = func(_ context.Context, repository string) error {
				calls++
				if repository != "owner/repo" {
					t.Fatalf("repository=%q", repository)
				}
				return nil
			}
			report := RunDoctor(context.Background(), deps)
			if (calls == 1) != tc.wantProbe || calls > 1 {
				t.Fatalf("probe calls=%d", calls)
			}
			if tc.wantProbe {
				check := doctorCheckByID(t, report, "https_credentials")
				if check.Status != CheckPass || !strings.Contains(check.Summary, "permissions are not verified") {
					t.Fatal(check)
				}
			} else {
				for _, check := range report.Checks {
					if check.ID == "https_credentials" {
						t.Fatal("SSH-only report invented HTTPS check")
					}
				}
			}
			if check := doctorCheckByID(t, report, "repository_access"); check.Status != CheckNotRun {
				t.Fatal(check)
			}
		})
	}
}

func TestDoctorHTTPSFailureBlocksGuardedDespiteAPIAuthentication(t *testing.T) {
	for _, mode := range []string{"success", "failure", "not configured"} {
		t.Run(mode, func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Repo = "/fixture/repo"
			deps.Worktree = func(context.Context, string) error { return nil }
			deps.Recipe = func(context.Context, string) error { return nil }
			deps.Origin = func(context.Context, string) (string, error) { return "https://github.com/owner/repo.git", nil }
			deps.PushOrigin = func(context.Context, string) (string, error) { return "", nil }
			deps.AuthStatus = completeDoctorAuth
			deps.Attempts = func(context.Context, domain.Channel) ([]store.ProviderAttempt, error) { return nil, nil }
			deps.Pair = func(context.Context, domain.Channel) (store.ProviderPair, error) { return qualifiedDoctorPair(), nil }
			want := CheckNotRun
			if mode != "not configured" {
				deps.HTTPSCredentials = func(context.Context, string) error {
					if mode == "failure" {
						return errors.New("untrusted-secret-value /private/account")
					}
					return nil
				}
				want = CheckPass
				if mode == "failure" {
					want = CheckFail
				}
			}
			report := RunDoctor(context.Background(), deps)
			if check := doctorCheckByID(t, report, "github_auth"); check.Status != CheckPass {
				t.Fatal(check)
			}
			if check := doctorCheckByID(t, report, "https_credentials"); check.Status != want {
				t.Fatal(check)
			}
			if report.GuardedEligible != (mode == "success") {
				t.Fatalf("eligibility=%v checks=%+v", report.GuardedEligible, report.Checks)
			}
			payload, err := json.Marshal(report)
			if err != nil || strings.Contains(string(payload), "untrusted-secret-value") || strings.Contains(string(payload), "/private/account") {
				t.Fatalf("unsafe report: %s %v", payload, err)
			}
		})
	}
}

func TestDoctorHTTPSCommandHasExactIsolatedEnvironment(t *testing.T) {
	t.Setenv("GH_TOKEN", "untrusted-token")
	t.Setenv("GITHUB_TOKEN", "untrusted-token")
	t.Setenv("SSH_AUTH_SOCK", "/private/agent")
	capability := ghrunner.CredentialCapability{Path: "/private/snapshot/gh", Digest: "sha256:fixture"}
	command := doctorHTTPSCommand(context.Background(), "/private/bundle/sf-git-credential-dev", capability, "/operator", "/selected/gh", "owner/repo")
	wantEnv := []string{"HOME=/var/empty", "LANG=C", "LC_ALL=C", "SF_GIT_GH_BINARY=/private/snapshot/gh", "SF_GIT_GH_BINARY_DIGEST=sha256:fixture", "SF_GIT_GH_CONFIG_DIR=/selected/gh", "SF_GIT_GH_HOME=/operator", "SF_GIT_HTTPS_REPOSITORY=owner/repo"}
	if !reflect.DeepEqual(command.Env, wantEnv) || !reflect.DeepEqual(command.Args, []string{"/private/bundle/sf-git-credential-dev", "get"}) {
		t.Fatalf("command=%+v", command)
	}
	input, err := io.ReadAll(command.Stdin)
	if err != nil || string(input) != "protocol=https\nhost=github.com\npath=owner/repo.git\n\n" {
		t.Fatalf("input=%q err=%v", input, err)
	}
	if command.Stderr != io.Discard || command.SysProcAttr == nil || !command.SysProcAttr.Setpgid || command.Cancel == nil || command.WaitDelay <= 0 {
		t.Fatal("probe lost bounded isolated process configuration")
	}
}

func TestDoctorHTTPSProbeConsumesOnlyValidBoundedCredentials(t *testing.T) {
	for _, tc := range []struct {
		name, body   string
		pass, cancel bool
	}{
		{"valid", "printf 'username=fixture\\npassword=synthetic-secret\\n\\n'", true, false},
		{"valid gh response", "printf 'protocol=https\\nhost=github.com\\nusername=fixture\\npassword=synthetic-secret\\n\\n'", true, false},
		{"wrong gh host", "printf 'protocol=https\\nhost=example.test\\nusername=fixture\\npassword=synthetic-secret\\n\\n'", false, false},
		{"empty", "exit 0", false, false},
		{"malformed", "printf 'password=synthetic-secret\\n\\n'", false, false},
		{"oversize", "printf '%s' '" + strings.Repeat("x", 17000) + "'", false, false},
		{"error", "printf 'synthetic-secret' >&2; exit 1", false, false},
		{"cancel", "/bin/sleep 30", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			helper := filepath.Join(t.TempDir(), "helper")
			if err := os.WriteFile(helper, []byte("#!/bin/sh\n"+tc.body+"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if tc.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}
			err := probeDoctorHTTPSCredentials(ctx, helper, ghrunner.CredentialCapability{Path: "/fixture/gh", Digest: "sha256:fixture"}, "/operator", "/selected/gh", "owner/repo")
			if tc.pass {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err != errDoctorHTTPSCredentials || strings.Contains(err.Error(), "synthetic-secret") || strings.Contains(err.Error(), helper) {
				t.Fatalf("unsafe probe error: %v", err)
			}
			if tc.cancel && ctx.Err() == nil {
				t.Fatal("cancellation case did not exercise context deadline")
			}
		})
	}
}
