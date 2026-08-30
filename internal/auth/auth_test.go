package auth

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type runnerCall struct {
	executable  string
	arguments   []string
	environment []string
	limit       int
}

type fakeRunner struct {
	calls          []runnerCall
	authenticated  bool
	interactive    int
	interactiveErr error
	versionOutput  []byte
	statusOutput   []byte
}

func (runner *fakeRunner) Probe(_ context.Context, executable string, arguments, environment []string, limit int) (ProbeResult, error) {
	runner.calls = append(runner.calls, runnerCall{executable: executable, arguments: append([]string(nil), arguments...), environment: append([]string(nil), environment...), limit: limit})
	if reflect.DeepEqual(arguments, []string{"--version"}) {
		return ProbeResult{Output: append([]byte(nil), runner.versionOutput...)}, nil
	}
	exit := 1
	if runner.authenticated {
		exit = 0
	}
	output := append([]byte(nil), runner.statusOutput...)
	if len(output) == 0 && reflect.DeepEqual(arguments, []string{"status"}) {
		if runner.authenticated {
			output = []byte("Logged in as sf-test@example.invalid")
		} else {
			output = []byte("Not logged in")
		}
	}
	return ProbeResult{ExitCode: exit, Output: output}, nil
}

func (runner *fakeRunner) Interactive(_ context.Context, _ string, _ []string, _ []string, terminal Terminal) (int, error) {
	runner.interactive++
	if terminal.In == nil || terminal.Out == nil || terminal.Err == nil {
		return -1, errors.New("terminal missing")
	}
	if runner.interactiveErr != nil {
		return -1, runner.interactiveErr
	}
	runner.authenticated = true
	return 0, nil
}

func TestAllowlistedProviderCommandsAndStatusOutputSuppression(t *testing.T) {
	manager, runner := managerFixture(t)
	runner.authenticated = true
	runner.versionOutput = []byte("gh version 2.98.0\nmore detail\n")
	runner.statusOutput = []byte("account: sofia token=must-not-survive")

	status := manager.Status(context.Background(), GitHub)
	if !status.Authenticated || status.State != StateAuthenticated || status.Version != "gh version 2.98.0" || status.Executable != "gh" {
		t.Fatalf("status=%+v", status)
	}
	if strings.Contains(status.Reason+status.Version, "sofia") || strings.Contains(status.Reason+status.Version, "must-not-survive") {
		t.Fatalf("raw status escaped: %+v", status)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("calls=%+v", runner.calls)
	}
	if got := runner.calls[1]; !reflect.DeepEqual(got.arguments, []string{"auth", "status", "--active", "--hostname", "github.com"}) || got.limit != 0 {
		t.Fatalf("status call=%+v", got)
	}
	assertSafeEnvironment(t, runner.calls[0].environment)
}

func TestAllOfficialCommandShapes(t *testing.T) {
	want := map[Provider]struct {
		binary string
		status []string
		login  []string
	}{
		GitHub: {"gh", []string{"auth", "status", "--active", "--hostname", "github.com"}, []string{"auth", "login", "--hostname", "github.com", "--git-protocol", "https", "--web"}},
		Cursor: {"cursor-agent", []string{"status"}, []string{"login"}},
		Claude: {"claude", []string{"auth", "status"}, []string{"auth", "login"}},
		Codex:  {"codex", []string{"login", "status"}, []string{"login"}},
	}
	for _, provider := range Providers() {
		item, ok := find(provider)
		if !ok {
			t.Fatalf("missing %s", provider)
		}
		expected := want[provider]
		if item.Executable != expected.binary || !reflect.DeepEqual(item.StatusArgs, expected.status) || !reflect.DeepEqual(item.LoginArgs, expected.login) {
			t.Errorf("%s definition=%+v", provider, item)
		}
	}
	for _, invalid := range []string{"", "openai", "github; rm", "cursor\n"} {
		if _, err := ParseProvider(invalid); !errors.Is(err, ErrUnknownProvider) {
			t.Errorf("ParseProvider(%q) err=%v", invalid, err)
		}
	}
}

func TestCursorStatusTextIsBoundedAndFailClosed(t *testing.T) {
	manager, runner := managerFixture(t)
	runner.versionOutput = []byte("cursor-agent 1.0\n")
	runner.authenticated = true
	runner.statusOutput = []byte("\x1b[32mLogged in as sofia@example.invalid\x1b[0m\nendpoint: production")
	status := manager.Status(context.Background(), Cursor)
	if !status.Authenticated || status.State != StateAuthenticated || runner.calls[1].limit != maximumOutput {
		t.Fatalf("status=%+v call=%+v", status, runner.calls[1])
	}
	runner.calls = nil
	runner.statusOutput = []byte("unexpected ambiguous output")
	status = manager.Status(context.Background(), Cursor)
	if status.State != StateProbeFailed || status.Authenticated {
		t.Fatalf("ambiguous status=%+v", status)
	}
	runner.calls = nil
	runner.statusOutput = []byte("Not authenticated")
	status = manager.Status(context.Background(), Cursor)
	if status.State != StateUnauthenticated || status.Authenticated {
		t.Fatalf("negative status=%+v", status)
	}
}

func TestExitCodeStatusRejectsUnexpectedFailure(t *testing.T) {
	if authenticated, err := interpretStatus(statusExitCode, ProbeResult{ExitCode: 2}); err == nil || authenticated {
		t.Fatalf("authenticated=%v err=%v", authenticated, err)
	}
}

func TestLoginIsInteractiveThenReprobesAndAlreadyAuthenticatedIsObserved(t *testing.T) {
	manager, runner := managerFixture(t)
	runner.versionOutput = []byte("cursor-agent 1.2.3\n")
	terminal := Terminal{In: strings.NewReader(""), Out: &bytes.Buffer{}, Err: &bytes.Buffer{}}
	status, attempted, err := manager.Login(context.Background(), Cursor, terminal)
	if err != nil || !attempted || !status.Authenticated || runner.interactive != 1 {
		t.Fatalf("status=%+v attempted=%v interactive=%d err=%v", status, attempted, runner.interactive, err)
	}
	status, attempted, err = manager.Login(context.Background(), Cursor, terminal)
	if err != nil || attempted || !status.Authenticated || runner.interactive != 1 {
		t.Fatalf("replay status=%+v attempted=%v interactive=%d err=%v", status, attempted, runner.interactive, err)
	}
}

func TestMissingAndUnsafeExecutablesFailClosed(t *testing.T) {
	manager := NewManager()
	manager.Lookup = func(string) (string, error) { return "", errors.New("missing") }
	status := manager.Status(context.Background(), Codex)
	if status.State != StateUnavailable || status.Installed {
		t.Fatalf("missing status=%+v", status)
	}
	if _, attempted, err := manager.Login(context.Background(), Codex, Terminal{}); !errors.Is(err, ErrNotInstalled) || attempted {
		t.Fatalf("login attempted=%v err=%v", attempted, err)
	}

	directory := canonicalTempDir(t)
	executable := filepath.Join(directory, "gh")
	if err := os.WriteFile(executable, []byte("fixture"), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(executable, 0o777); err != nil {
		t.Fatal(err)
	}
	manager = NewManager()
	manager.Lookup = func(string) (string, error) { return executable, nil }
	manager.Home = func() (string, error) { return directory, nil }
	status = manager.Status(context.Background(), GitHub)
	if status.State != StateProbeFailed || status.Installed {
		t.Fatalf("unsafe status=%+v", status)
	}
}

func TestVersionRedactionAndValidation(t *testing.T) {
	if got, err := safeVersion([]byte("tool 1.2 token=secret\n")); err != nil || strings.Contains(got, "secret") || !strings.Contains(got, "[REDACTED]") {
		t.Fatalf("got=%q err=%v", got, err)
	}
	if _, err := safeVersion([]byte("bad\x01version\n")); err == nil {
		t.Fatal("control character accepted")
	}
	if _, err := safeVersion([]byte(strings.Repeat("x", maximumVersion+1))); err == nil {
		t.Fatal("oversized version accepted")
	}
}

func TestOSRunnerBoundsOutputDiscardsStatusAndMapsExit(t *testing.T) {
	runner := OSRunner{}
	environment := []string{"PATH=/usr/bin:/bin", "HOME=/tmp", "LC_ALL=C", "LANG=C"}
	if _, err := runner.Probe(context.Background(), "/bin/sh", []string{"-c", "printf 123456"}, environment, 4); err == nil {
		t.Fatal("oversized output accepted")
	}
	result, err := runner.Probe(context.Background(), "/bin/sh", []string{"-c", "printf 'token=secret'; exit 9"}, environment, 0)
	if err != nil || result.ExitCode != 9 || len(result.Output) != 0 {
		t.Fatalf("discard result=%+v err=%v", result, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := runner.Probe(ctx, "/bin/sh", []string{"-c", "sleep 10"}, environment, 0); err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("deadline err=%v context=%v", err, ctx.Err())
	}
}

func managerFixture(t *testing.T) (Manager, *fakeRunner) {
	t.Helper()
	directory := canonicalTempDir(t)
	executable := filepath.Join(directory, "provider")
	if err := os.WriteFile(executable, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{versionOutput: []byte("provider 1.0\n")}
	manager := NewManager()
	manager.Lookup = func(string) (string, error) { return executable, nil }
	manager.Home = func() (string, error) { return directory, nil }
	manager.Getenv = func(key string) string {
		switch key {
		case "USER", "LOGNAME":
			return "sf-test"
		case "TERM":
			return "xterm-256color"
		case "GH_TOKEN", "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "SSH_AUTH_SOCK":
			return "must-not-pass"
		default:
			return ""
		}
	}
	manager.Runner = runner
	return manager, runner
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}

func assertSafeEnvironment(t *testing.T, environment []string) {
	t.Helper()
	allowed := map[string]bool{"HOME": true, "PATH": true, "LC_ALL": true, "LANG": true, "USER": true, "LOGNAME": true, "TERM": true}
	for _, item := range environment {
		key, _, ok := strings.Cut(item, "=")
		if !ok || !allowed[key] {
			t.Fatalf("unsafe environment entry %q", item)
		}
		lower := strings.ToLower(key)
		for _, secret := range []string{"token", "secret", "password", "credential", "api_key", "auth", "cookie", "session", "ssh"} {
			if strings.Contains(lower, secret) {
				t.Fatalf("credential environment leaked: %q", item)
			}
		}
	}
}
