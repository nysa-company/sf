// Package gitssh is the deliberately tiny GIT_SSH boundary used for GitHub
// publication.  It accepts Git's ssh argv and replaces it with a fixed ssh
// invocation; it never forwards user ssh options or configuration.
package gitssh

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
)

// PinnedKnownHosts is shipped with sf from GitHub's published SSH key list.
// Packaging must install this exact asset read-only and pass its absolute path.
//
//go:embed github_known_hosts
var PinnedKnownHosts string

var ErrRefused = errors.New("sf ssh invocation refused")

var repoName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}/[A-Za-z0-9][A-Za-z0-9_.-]{0,99}\.git$`)

// RepositoryFromOrigin accepts only literal GitHub SSH remote spellings. The
// returned repository is transport metadata; callers retain the original URL
// as their immutable Git identity. All accepted forms use our pinned endpoint.
func RepositoryFromOrigin(raw string) (string, bool) {
	for _, prefix := range []string{"git@github.com:", "ssh://git@github.com/", "ssh://git@github.com:22/", "ssh://git@ssh.github.com:443/"} {
		if strings.HasPrefix(raw, prefix) {
			name := strings.TrimPrefix(raw, prefix)
			if repoName.MatchString(name) {
				return strings.TrimSuffix(name, ".git"), true
			}
			return "", false
		}
	}
	return "", false
}

// Request is the complete, already-sanitized configuration accepted by the
// helper. SSH is deliberately a path, never a command string.
type Request struct{ SSHBinary, KnownHosts, AgentSocket, Repository string }

// ValidateInvocation accepts only the two Git smart transports that sf needs
// from the literal GitHub SSH origin forms. Git spells the path in an ssh URL
// with a leading slash (for example, "git-receive-pack '/owner/repo.git'").
// The SCP form omits that slash. Command maps both to the pinned port-443 host.
// It is important that this parser model Git's real argv, rather than a
// friendlier shell spelling: this executable is the trust boundary.
func ValidateInvocation(argv []string, want string) error {
	if want == "" || !repoName.MatchString(want+".git") {
		return fmt.Errorf("%w: repository", ErrRefused)
	}
	port := ""
	for len(argv) > 0 && strings.HasPrefix(argv[0], "-") {
		switch argv[0] {
		case "-p":
			if len(argv) < 2 || port != "" {
				return fmt.Errorf("%w: port", ErrRefused)
			}
			port, argv = argv[1], argv[2:]
		case "-o":
			if len(argv) < 2 || (argv[1] != "SendEnv=GIT_PROTOCOL" && argv[1] != "SetEnv=GIT_PROTOCOL=version=2") {
				return fmt.Errorf("%w: option", ErrRefused)
			}
			argv = argv[2:]
		default:
			return fmt.Errorf("%w: option", ErrRefused)
		}
	}
	if len(argv) != 2 {
		return fmt.Errorf("%w: host or command", ErrRefused)
	}
	pinned := argv[0] == "git@ssh.github.com" && port == "443"
	common := argv[0] == "git@github.com" && (port == "" || port == "22")
	commandOK := argv[1] == "git-receive-pack '/"+want+".git'" || argv[1] == "git-upload-pack '/"+want+".git'"
	// SCP-style origins produce a relative repository path; ssh:// produces
	// an absolute path. Both are replaced by the same fixed command below.
	if common && port == "" {
		commandOK = commandOK || argv[1] == "git-receive-pack '"+want+".git'" || argv[1] == "git-upload-pack '"+want+".git'"
	}
	if (!pinned && !common) || !commandOK {
		return fmt.Errorf("%w: host or command", ErrRefused)
	}
	return nil
}

// Command returns a fixed argv and env for system ssh. It uses no ssh config,
// proxy, prompt, password, or keyboard interactive path; authentication can
// only come from the supplied Unix-agent socket.
func Command(request Request, gitArgv []string) ([]string, []string, error) {
	if err := ValidateInvocation(gitArgv, request.Repository); err != nil {
		return nil, nil, err
	}
	for _, item := range []struct{ value, name string }{{request.SSHBinary, "ssh binary"}, {request.KnownHosts, "known hosts"}, {request.AgentSocket, "agent socket"}} {
		if !filepath.IsAbs(item.value) || filepath.Clean(item.value) != item.value {
			return nil, nil, fmt.Errorf("%w: %s", ErrRefused, item.name)
		}
	}
	for _, item := range []struct {
		path      string
		allowRoot bool
	}{{request.SSHBinary, true}, {request.KnownHosts, true}} {
		path := item.path
		info, err := os.Lstat(path)
		ownerOK := err == nil && (ownedByCurrentUser(info) || (item.allowRoot && ownedByCurrentUserOrRoot(info)))
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || !ownerOK || linkCount(info) != 1 || !secureParents(path, false) {
			return nil, nil, fmt.Errorf("%w: unsafe file", ErrRefused)
		}
	}
	data, err := os.ReadFile(request.KnownHosts)
	if err != nil || string(data) != PinnedKnownHosts {
		return nil, nil, fmt.Errorf("%w: unpinned github host keys", ErrRefused)
	}
	if err := ValidateAgentSocket(request.AgentSocket); err != nil {
		return nil, nil, err
	}
	service := "git-receive-pack"
	if strings.HasPrefix(gitArgv[len(gitArgv)-1], "git-upload-pack ") {
		service = "git-upload-pack"
	}
	args := []string{"-F", "/dev/null", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + request.KnownHosts, "-o", "GlobalKnownHostsFile=/dev/null", "-o", "IdentityFile=none", "-o", "IdentityAgent=SSH_AUTH_SOCK", "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no", "-o", "PreferredAuthentications=publickey", "-o", "ProxyCommand=none", "-o", "ProxyJump=none", "-o", "RequestTTY=no", "-o", "ClearAllForwardings=yes", "-p", "443", "git@ssh.github.com", service + " '/" + request.Repository + ".git'"}
	return args, []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "SSH_AUTH_SOCK=" + request.AgentSocket}, nil
}

// ValidateAgentSocket shares the transport's local agent boundary with
// diagnostics. Presence alone does not prove loaded keys or remote access.
func ValidateAgentSocket(socket string) error {
	if !filepath.IsAbs(socket) || filepath.Clean(socket) != socket || strings.ContainsAny(socket, "\x00\r\n") {
		return fmt.Errorf("%w: unsafe agent", ErrRefused)
	}
	info, err := os.Lstat(socket)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0o022 != 0 || !ownedByCurrentUser(info) || !secureParents(socket, true) {
		return fmt.Errorf("%w: unsafe agent", ErrRefused)
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Getuid()
}

func ownedByCurrentUserOrRoot(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (int(stat.Uid) == os.Getuid() || stat.Uid == 0)
}

func linkCount(info os.FileInfo) uint64 {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return uint64(stat.Nlink)
}

// secureParents rejects symlinked or group/world-writable non-sticky parent
// components. A private user-owned directory below /private/tmp is valid on
// macOS: launchd agent sockets conventionally live there, while the sticky
// system component itself cannot be replaced by another user.
func secureParents(path string, allowSticky bool) bool {
	for dir := filepath.Dir(path); ; dir = filepath.Dir(dir) {
		info, err := os.Lstat(dir)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return false
		}
		mode := info.Mode().Perm()
		if mode&0o022 != 0 && !(allowSticky && info.Mode()&os.ModeSticky != 0) {
			return false
		}
		if mode&0o022 == 0 && !ownedByCurrentUserOrRoot(info) && dir != "/" {
			return false
		}
		if dir == "/" {
			return true
		}
	}
}
