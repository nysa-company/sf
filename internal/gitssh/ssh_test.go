package gitssh

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) Request {
	t.Helper()
	root := t.TempDir()
	ssh := filepath.Join(root, "ssh")
	known := filepath.Join(root, "known")
	if err := os.WriteFile(ssh, []byte("x"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(known, []byte(PinnedKnownHosts), 0o600); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join("/tmp", "sfssh-"+strings.ReplaceAll(t.Name(), "/", "-"))
	_ = os.Remove(sock)
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close(); _ = os.Remove(sock) })
	return Request{SSHBinary: ssh, KnownHosts: known, AgentSocket: sock, Repository: "owner/repo"}
}
func TestCommandPinsGitHubReceivePackAndSanitizesEnvironment(t *testing.T) {
	req := fixture(t)
	args, env, err := Command(req, []string{"-o", "SendEnv=GIT_PROTOCOL", "-p", "443", "git@ssh.github.com", "git-receive-pack 'owner/repo.git'"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(args, "\x00")
	for _, want := range []string{"-F\x00/dev/null", "StrictHostKeyChecking=yes", "ProxyCommand=none", "PasswordAuthentication=no", "git-receive-pack 'owner/repo.git'"} {
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
	for _, argv := range [][]string{{"-F", "/tmp/x", "-p", "443", "git@ssh.github.com", "git-receive-pack 'owner/repo.git'"}, {"-p", "443", "git@github.com", "git-receive-pack 'owner/repo.git'"}, {"-p", "443", "git@ssh.github.com", "git-receive-pack 'owner/repo.git';id"}, {"-p", "22", "git@ssh.github.com", "git-receive-pack 'owner/repo.git'"}} {
		if _, _, err := Command(req, argv); err == nil {
			t.Fatalf("accepted %q", argv)
		}
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
