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

// Request is the complete, already-sanitized configuration accepted by the
// helper. SSH is deliberately a path, never a command string.
type Request struct{ SSHBinary, KnownHosts, AgentSocket, Repository string }

// ValidateInvocation accepts only the two Git smart transports that sf needs
// at the pinned GitHub port-443 endpoint. Git spells the path in an ssh URL
// with a leading slash (for example, "git-receive-pack '/owner/repo.git'").
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
	if port != "443" || len(argv) != 2 || argv[0] != "git@ssh.github.com" ||
		(argv[1] != "git-receive-pack '/"+want+".git'" && argv[1] != "git-upload-pack '/"+want+".git'") {
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
	for _, path := range []string{request.SSHBinary, request.KnownHosts} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 || !ownedByCurrentUser(info) || linkCount(info) != 1 || !secureParents(path, false) {
			return nil, nil, fmt.Errorf("%w: unsafe file", ErrRefused)
		}
	}
	data, err := os.ReadFile(request.KnownHosts)
	if err != nil || string(data) != PinnedKnownHosts {
		return nil, nil, fmt.Errorf("%w: unpinned github host keys", ErrRefused)
	}
	info, err := os.Lstat(request.AgentSocket)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0o022 != 0 || !ownedByCurrentUser(info) || !secureParents(request.AgentSocket, true) {
		return nil, nil, fmt.Errorf("%w: unsafe agent", ErrRefused)
	}
	service := "git-receive-pack"
	if strings.HasPrefix(gitArgv[len(gitArgv)-1], "git-upload-pack ") {
		service = "git-upload-pack"
	}
	args := []string{"-F", "/dev/null", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + request.KnownHosts, "-o", "GlobalKnownHostsFile=/dev/null", "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no", "-o", "PreferredAuthentications=publickey", "-o", "ProxyCommand=none", "-o", "ProxyJump=none", "-o", "RequestTTY=no", "-o", "ClearAllForwardings=yes", "-p", "443", "git@ssh.github.com", service + " '/" + request.Repository + ".git'"}
	return args, []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "SSH_AUTH_SOCK=" + request.AgentSocket}, nil
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
