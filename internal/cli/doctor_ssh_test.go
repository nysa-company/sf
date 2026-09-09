package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDoctorSSHTransportDiagnostics(t *testing.T) {
	for _, origin := range []string{"git@github.com:owner/repo.git", "ssh://git@github.com/owner/repo.git", "ssh://git@github.com:22/owner/repo.git", "ssh://git@ssh.github.com:443/owner/repo.git"} {
		t.Run(origin, func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Repo = "/fixture/repo"
			deps.Worktree = func(context.Context, string) error { return nil }
			deps.Origin = func(context.Context, string) (string, error) { return origin, nil }
			deps.SSHAgent = func(context.Context) DoctorSSHAgentState { return DoctorSSHAgentReady }
			deps.SSHAssets = func() error { return nil }
			report := RunDoctor(context.Background(), deps)
			for _, id := range []string{"git_transport", "ssh_agent", "ssh_assets"} {
				if check := doctorCheckByID(t, report, id); check.Status != CheckPass {
					t.Fatalf("%s: %+v", id, check)
				}
			}
			if check := doctorCheckByID(t, report, "repository_access"); check.Status != CheckNotRun {
				t.Fatalf("access=%+v", check)
			}
			if !strings.Contains(doctorCheckByID(t, report, "ssh_agent").Summary, "unverified") {
				t.Fatal("local keys were presented as remote access")
			}
		})
	}
}

func TestDoctorSSHMissingAndEmptyRemainDistinct(t *testing.T) {
	for _, test := range []struct {
		state DoctorSSHAgentState
		want  string
	}{
		{DoctorSSHAgentMissing, "SSH_AUTH_SOCK is missing"},
		{DoctorSSHAgentEmpty, "no identities"},
		{DoctorSSHAgentUnsafe, "user-owned Unix socket"},
		{DoctorSSHAgentUnavailable, "agent is unavailable"},
		{DoctorSSHAgentState("secret-result"), "agent is unavailable"},
	} {
		t.Run(string(test.state), func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Repo = "/fixture/repo"
			deps.Worktree = func(context.Context, string) error { return nil }
			deps.Origin = func(context.Context, string) (string, error) { return "git@github.com:owner/repo.git", nil }
			deps.SSHAgent = func(context.Context) DoctorSSHAgentState { return test.state }
			deps.AuthStatus = completeDoctorAuth
			report := RunDoctor(context.Background(), deps)
			check := doctorCheckByID(t, report, "ssh_agent")
			if check.Status != CheckFail || !strings.Contains(check.Summary, test.want) || check.NextAction == nil {
				t.Fatalf("agent=%+v", check)
			}
			if check := doctorCheckByID(t, report, "github_auth"); check.Status != CheckPass {
				t.Fatalf("API authentication conflated with SSH: %+v", check)
			}
			encoded, _ := json.Marshal(report)
			if strings.Contains(string(encoded), "secret-result") {
				t.Fatal("raw agent state leaked")
			}
		})
	}
}

func TestDoctorTransportSkipsUnselectedInvalidAndHTTPSAgent(t *testing.T) {
	for _, test := range []struct {
		name, repo, origin string
		valid              bool
		transport          CheckStatus
	}{
		{"unselected", "", "", true, CheckNotRun},
		{"invalid-worktree", "/fixture/repo", "", false, CheckNotRun},
		{"https", "/fixture/repo", "https://github.com/owner/repo.git", true, CheckPass},
		{"hostile", "/fixture/repo", "ssh://git@secret-result/owner/repo.git", true, CheckFail},
		{"credential-url", "/fixture/repo", "https://secret-result@github.com/owner/repo.git", true, CheckFail},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Repo = test.repo
			deps.Worktree = func(context.Context, string) error {
				if !test.valid {
					return errors.New("invalid")
				}
				return nil
			}
			deps.Origin = func(context.Context, string) (string, error) {
				if test.repo == "" || !test.valid {
					t.Fatal("origin probed without valid worktree")
				}
				return test.origin, nil
			}
			deps.SSHAgent = func(context.Context) DoctorSSHAgentState {
				t.Fatal("SSH probed without SSH origin")
				return DoctorSSHAgentReady
			}
			deps.SSHAssets = func() error { t.Fatal("SSH assets probed without SSH origin"); return nil }
			report := RunDoctor(context.Background(), deps)
			if check := doctorCheckByID(t, report, "git_transport"); check.Status != test.transport {
				t.Fatalf("transport=%+v", check)
			}
			encoded, _ := json.Marshal(report)
			if strings.Contains(string(encoded), "secret-result") {
				t.Fatal("raw URL leaked")
			}
		})
	}
}

func TestDoctorSSHDefaultsDoNotAddAmbientProbes(t *testing.T) {
	deps := (DoctorDeps{}).defaults()
	if deps.Origin != nil || deps.PushOrigin != nil || deps.SSHAgent != nil || deps.SSHAssets != nil {
		t.Fatal("defaults installed ambient transport probes")
	}
}

func TestDoctorPushTransportIsInspectedSeparately(t *testing.T) {
	for _, test := range []struct {
		name, push string
		want       CheckStatus
		ssh        bool
	}{
		{"ssh-push", "git@github.com:owner/repo.git", CheckPass, true},
		{"fallback", "", CheckPass, false},
		{"different-repository", "git@github.com:other/repo.git", CheckFail, false},
		{"unsupported-push", "git@secret-result:owner/repo.git", CheckFail, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := healthyDoctorDeps(t)
			deps.Repo = "/fixture/repo"
			deps.Worktree = func(context.Context, string) error { return nil }
			deps.Origin = func(context.Context, string) (string, error) { return "https://github.com/owner/repo.git", nil }
			deps.PushOrigin = func(context.Context, string) (string, error) { return test.push, nil }
			called := false
			deps.SSHAgent = func(context.Context) DoctorSSHAgentState { called = true; return DoctorSSHAgentReady }
			report := RunDoctor(context.Background(), deps)
			if check := doctorCheckByID(t, report, "git_transport"); check.Status != test.want {
				t.Fatalf("transport=%+v", check)
			}
			if called != test.ssh {
				t.Fatalf("SSH checked=%v", called)
			}
			if test.ssh && !strings.Contains(doctorCheckByID(t, report, "git_transport").Summary, "push SSH") {
				t.Fatal("push SSH transport was not reported")
			}
			encoded, _ := json.Marshal(report)
			if strings.Contains(string(encoded), "secret-result") {
				t.Fatal("push URL leaked")
			}
		})
	}
}

func TestDoctorSSHAgentCommandIsReadOnlyAndScrubbed(t *testing.T) {
	t.Setenv("GH_TOKEN", "secret-result")
	t.Setenv("SSH_ASKPASS", "/hostile-program")
	t.Setenv("SSH_AUTH_SOCK", "/ambient-agent")
	command := doctorSSHAgentCommand(context.Background(), "/explicit-agent")
	if !reflect.DeepEqual(command.Args, []string{"/usr/bin/ssh-add", "-l"}) {
		t.Fatalf("argv=%v", command.Args)
	}
	want := []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "SSH_AUTH_SOCK=/explicit-agent"}
	if !reflect.DeepEqual(command.Env, want) || command.Stdout != nil || command.Stderr != nil || command.Stdin != nil || command.WaitDelay <= 0 {
		t.Fatal("agent command did not preserve scrubbed, discarded-output contract")
	}
}

func TestDoctorSSHAgentRejectsMissingAndNonSocketWithoutCommand(t *testing.T) {
	if got := productionDoctorSSHAgent("")(context.Background()); got != DoctorSSHAgentMissing {
		t.Fatalf("missing=%s", got)
	}
	path := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if got := productionDoctorSSHAgent(path)(context.Background()); got != DoctorSSHAgentUnsafe {
		t.Fatalf("regular file=%s", got)
	}
}

func TestDoctorOriginOutputBound(t *testing.T) {
	var buffer doctorOriginBuffer
	if _, err := buffer.Write([]byte(strings.Repeat("x", 4097))); err == nil || buffer.Len() != 0 {
		t.Fatal("oversized output retained")
	}
}

func TestDoctorSSHBundleErrorIsSanitized(t *testing.T) {
	deps := healthyDoctorDeps(t)
	deps.Repo = "/fixture/repo"
	deps.Worktree = func(context.Context, string) error { return nil }
	deps.Origin = func(context.Context, string) (string, error) { return "git@github.com:owner/repo.git", nil }
	deps.SSHAssets = func() error { return errors.New("secret-result") }
	report := RunDoctor(context.Background(), deps)
	if check := doctorCheckByID(t, report, "ssh_assets"); check.Status != CheckFail || strings.Contains(check.Summary, "secret-result") {
		t.Fatalf("assets=%+v", check)
	}
}
