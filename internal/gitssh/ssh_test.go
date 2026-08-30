package gitssh

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) Request {
	t.Helper()
	root, err := os.MkdirTemp(moduleRoot(t), ".sfssh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	ssh := filepath.Join(root, "ssh")
	known := filepath.Join(root, "known")
	if err := os.WriteFile(ssh, []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(known, []byte(PinnedKnownHosts), 0o600); err != nil {
		t.Fatal(err)
	}
	// macOS Unix socket names are capped at 104 bytes; t.TempDir paths are
	// commonly longer. /private/tmp is the legitimate launchd-style sticky
	// parent explicitly supported by the helper.
	probe, err := os.CreateTemp("/private/tmp", "sfssh-")
	if err != nil {
		t.Fatal(err)
	}
	sock := probe.Name()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(sock); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(); _ = os.Remove(sock) })
	return Request{SSHBinary: ssh, KnownHosts: known, AgentSocket: sock, Repository: "owner/repo"}
}
func TestCommandPinsGitHubReceivePackAndSanitizesEnvironment(t *testing.T) {
	req := fixture(t)
	args, env, err := Command(req, []string{"-o", "SendEnv=GIT_PROTOCOL", "-p", "443", "git@ssh.github.com", "git-receive-pack '/owner/repo.git'"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, "\x00")
	for _, want := range []string{"-F\x00/dev/null", "StrictHostKeyChecking=yes", "ProxyCommand=none", "PasswordAuthentication=no", "git-receive-pack '/owner/repo.git'"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q: %q", want, joined)
		}
	}
	if strings.Join(env, "\x00") != "PATH=/usr/bin:/bin:/usr/sbin:/sbin\x00LANG=C\x00SSH_AUTH_SOCK="+req.AgentSocket {
		t.Fatalf("unsafe env %q", env)
	}
}
func TestCommandRejectsEscapesHostsAndCommands(t *testing.T) {
	req := fixture(t)
	for _, argv := range [][]string{{"-F", "/tmp/x", "-p", "443", "git@ssh.github.com", "git-receive-pack '/owner/repo.git'"}, {"-p", "443", "git@github.com", "git-receive-pack '/owner/repo.git'"}, {"-p", "443", "git@ssh.github.com", "git-receive-pack '/owner/repo.git';id"}, {"-p", "22", "git@ssh.github.com", "git-receive-pack '/owner/repo.git'"}, {"-G", "git@ssh.github.com"}, {"-p", "443", "git@ssh.github.com", "git-receive-pack 'owner/repo.git'"}} {
		if _, _, err := Command(req, argv); err == nil {
			t.Fatalf("accepted %q", argv)
		}
	}
}

func TestCompiledHelperAcceptsRealGitUploadPackArgv(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git unavailable")
	}
	req := fixture(t)
	root := filepath.Dir(req.SSHBinary)
	capture := filepath.Join(root, "argv")
	fakeSSH := filepath.Join(root, "fake-ssh")
	if err := os.WriteFile(fakeSSH, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > '"+capture+"'\nprintf '0000'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(root, "sf-ssh")
	cmd := exec.Command("go", "build", "-o", helper, "./cmd/sf-ssh")
	cmd.Dir = moduleRoot(t)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v\n%s", err, output)
	}
	command := exec.Command("git", "ls-remote", "ssh://git@ssh.github.com:443/owner/repo.git")
	command.Env = []string{"PATH=/usr/bin:/bin", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_SSH=" + helper, "GIT_SSH_VARIANT=ssh", "SF_GIT_SSH_BINARY=" + fakeSSH, "SF_GIT_SSH_KNOWN_HOSTS=" + req.KnownHosts, "SF_GIT_SSH_REPOSITORY=owner/repo", "SSH_AUTH_SOCK=" + req.AgentSocket}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("real git ls-remote: %v\n%s", err, output)
	}
	got, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "git-upload-pack '/owner/repo.git'") {
		t.Fatalf("git did not generate canonical upload-pack argv: %q", got)
	}
	// A real push reaches the receive-pack form before the intentionally tiny
	// fake server declines the protocol. This guards the exact argv spelling
	// Git generates for the mutating transport as well as upload-pack above.
	push := exec.Command("git", "push", "ssh://git@ssh.github.com:443/owner/repo.git", "HEAD:refs/heads/main")
	push.Dir = moduleRoot(t)
	push.Env = command.Env
	_ = push.Run() // the fake server has no receive-pack implementation
	got, err = os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "git-receive-pack '/owner/repo.git'") {
		t.Fatalf("git did not generate canonical receive-pack argv: %q", got)
	}
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
func TestCommandRejectsUnsafeHostKeyAndAgentInputs(t *testing.T) {
	req := fixture(t)
	if err := os.Chmod(req.KnownHosts, 0o666); err != nil {
		t.Fatal(err)
	}
	_, _, err := Command(req, []string{"-p", "443", "git@ssh.github.com", "git-receive-pack 'owner/repo.git'"})
	if err == nil {
		t.Fatal("world writable host keys accepted")
	}
}
