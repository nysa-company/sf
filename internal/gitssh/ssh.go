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

// ValidateInvocation accepts only Git's receive-pack transport to the GitHub
// port-443 endpoint. Any option Git supplies is checked then discarded.
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
	if port != "443" || len(argv) != 2 || argv[0] != "git@ssh.github.com" || argv[1] != "git-receive-pack '"+want+".git'" {
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
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
			return nil, nil, fmt.Errorf("%w: unsafe file", ErrRefused)
		}
	}
	data, err := os.ReadFile(request.KnownHosts)
	if err != nil || string(data) != PinnedKnownHosts {
		return nil, nil, fmt.Errorf("%w: unpinned github host keys", ErrRefused)
	}
	info, err := os.Lstat(request.AgentSocket)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
		return nil, nil, fmt.Errorf("%w: unsafe agent", ErrRefused)
	}
	args := []string{"-F", "/dev/null", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + request.KnownHosts, "-o", "GlobalKnownHostsFile=/dev/null", "-o", "PasswordAuthentication=no", "-o", "KbdInteractiveAuthentication=no", "-o", "PreferredAuthentications=publickey", "-o", "ProxyCommand=none", "-o", "ProxyJump=none", "-o", "RequestTTY=no", "-o", "ClearAllForwardings=yes", "-p", "443", "git@ssh.github.com", "git-receive-pack '" + request.Repository + ".git'"}
	return args, []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "SSH_AUTH_SOCK=" + request.AgentSocket}, nil
}
