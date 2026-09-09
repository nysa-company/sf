package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/gitssh"
	"github.com/nysa-company/sf/internal/runtimeassets"
)

// DoctorSSHAgentState describes local agent evidence only, never GitHub
// authentication or repository access. No key identifiers leave the probe.
type DoctorSSHAgentState string

const (
	DoctorSSHAgentMissing     DoctorSSHAgentState = "missing"
	DoctorSSHAgentUnsafe      DoctorSSHAgentState = "unsafe"
	DoctorSSHAgentUnavailable DoctorSSHAgentState = "unavailable"
	DoctorSSHAgentEmpty       DoctorSSHAgentState = "empty"
	DoctorSSHAgentReady       DoctorSSHAgentState = "identities_present"
)

func checkDoctorGitTransport(ctx context.Context, deps DoctorDeps, report *DoctorReport) {
	transport := DoctorCheck{ID: "git_transport", Status: CheckNotRun, Summary: "select a repository to inspect its Git transport"}
	agent := DoctorCheck{ID: "ssh_agent", Status: CheckNotRun, Summary: "SSH agent inspection requires a supported SSH origin"}
	assets := DoctorCheck{ID: "ssh_assets", Status: CheckNotRun, Summary: "SSH bundle inspection requires a supported SSH origin"}
	defer func() {
		report.Checks = append(report.Checks, transport, agent, assets,
			DoctorCheck{ID: "repository_access", Status: CheckNotRun, Summary: "remote repository read/write access was not tested; local transport and API readiness do not prove repository access"})
	}()
	if deps.Repo == "" || !doctorChecksPass(*report, "repository_worktree") {
		return
	}
	if deps.Origin == nil {
		transport.Summary = "local origin inspection was not configured"
		return
	}
	origin, err := deps.Origin(ctx, deps.Repo)
	if err != nil {
		transport = failedCheck("git_transport", "local origin could not be inspected; configure one supported GitHub origin", deps.Binary, "init", "--help")
		return
	}
	fetchProtocol := doctorGitProtocol(origin)
	if fetchProtocol == "" {
		transport = failedCheck("git_transport", "origin is not a supported GitHub HTTPS or SSH URL", deps.Binary, "init", "--help")
		return
	}
	pushProtocol := "not inspected"
	if deps.PushOrigin != nil {
		pushOrigin, err := deps.PushOrigin(ctx, deps.Repo)
		if err != nil {
			transport = failedCheck("git_transport", "local push origin could not be inspected; configure at most one supported GitHub push URL", deps.Binary, "init", "--help")
			return
		}
		if pushOrigin == "" {
			pushOrigin = origin
		}
		pushProtocol = doctorGitProtocol(pushOrigin)
		if pushProtocol == "" {
			transport = failedCheck("git_transport", "push origin is not a supported GitHub HTTPS or SSH URL", deps.Binary, "init", "--help")
			return
		}
		if !strings.EqualFold(doctorGitRepository(origin), doctorGitRepository(pushOrigin)) {
			transport = failedCheck("git_transport", "fetch and push origins identify different GitHub repositories; align the selected repository origins", deps.Binary, "init", "--help")
			return
		}
	}
	transport.Status = CheckPass
	transport.Summary = "selected local Git transport: fetch " + fetchProtocol + "; push " + pushProtocol + "; URL rewrites are not applied; GitHub API authentication is checked separately"
	if fetchProtocol == "HTTPS" || pushProtocol == "HTTPS" {
		check := DoctorCheck{ID: "https_credentials", Status: CheckNotRun, Summary: "production HTTPS credential bridge probe was not configured; API login alone does not prove Git credential availability"}
		if deps.HTTPSCredentials != nil {
			if err := deps.HTTPSCredentials(ctx, doctorGitRepository(origin)); err != nil {
				check = failedCheck("https_credentials", "HTTPS credential bridge could not retrieve credentials; unlock the macOS login Keychain and authenticate gh in the same operator session, then rerun Doctor; plaintext token storage is not required", deps.Binary, "auth", "login", "github")
			} else {
				check.Status = CheckPass
				check.Summary = "packaged HTTPS bridge retrieved credentials in its execution environment; repository permissions are not verified"
			}
		}
		report.Checks = append(report.Checks, check)
	}
	if fetchProtocol != "SSH" && pushProtocol != "SSH" {
		return
	}
	transport.Summary += "; factory SSH uses pinned ssh.github.com:443"
	assets.Summary = "SSH bundle inspection was not configured"
	if deps.SSHAssets != nil {
		if err := deps.SSHAssets(); err != nil {
			assets = failedCheck("ssh_assets", "install the matching channel SSH helper and pinned GitHub host keys alongside this sf executable", deps.Binary, "doctor", "--help")
		} else {
			assets.Status, assets.Summary = CheckPass, "matching channel SSH helper and pinned GitHub host keys are present"
		}
	}
	agent.Summary = "local SSH agent inspection was not configured; loaded keys and GitHub authentication are unverified"
	if deps.SSHAgent == nil {
		return
	}
	state := deps.SSHAgent(ctx)
	summary := "SSH agent is unavailable; expose a working SSH_AUTH_SOCK and restart the foreground daemon after changing it"
	switch state {
	case DoctorSSHAgentReady:
		agent.Status, agent.Summary = CheckPass, "local SSH agent reports identities; key acceptance by GitHub and repository permissions are unverified; restart the foreground daemon if SSH_AUTH_SOCK changed"
		return
	case DoctorSSHAgentMissing:
		summary = "SSH_AUTH_SOCK is missing; start or expose your SSH agent, load an existing GitHub key, then restart the foreground daemon"
	case DoctorSSHAgentUnsafe:
		summary = "SSH_AUTH_SOCK must name a user-owned Unix socket without group/world write access; repair the agent setup and restart the foreground daemon"
	case DoctorSSHAgentEmpty:
		summary = "SSH agent reports no identities; load an existing GitHub key with ssh-add; if SSH_AUTH_SOCK changes, restart the foreground daemon"
	}
	agent = failedCheck("ssh_agent", summary, deps.Binary, "doctor", "--help")
}

func productionDoctorSSHAssets(channel domain.Channel) func() error {
	return func() error {
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		_, err = runtimeassets.ResolveSSH(channel, executable)
		return err
	}
}

func productionDoctorSSHAgent(socket string) func(context.Context) DoctorSSHAgentState {
	return func(ctx context.Context) DoctorSSHAgentState {
		if socket == "" {
			return DoctorSSHAgentMissing
		}
		if err := gitssh.ValidateAgentSocket(socket); err != nil {
			return DoctorSSHAgentUnsafe
		}
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		command := doctorSSHAgentCommand(probeCtx, socket)
		// Nil stdout/stderr are discarded by os/exec. Never retain fingerprints,
		// key comments, errors, or credentials, even when ssh-add fails.
		err := command.Run()
		if probeCtx.Err() != nil {
			return DoctorSSHAgentUnavailable
		}
		if err == nil {
			return DoctorSSHAgentReady
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return DoctorSSHAgentEmpty
		}
		return DoctorSSHAgentUnavailable
	}
}

func doctorSSHAgentCommand(ctx context.Context, socket string) *exec.Cmd {
	command := exec.CommandContext(ctx, "/usr/bin/ssh-add", "-l")
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "SSH_AUTH_SOCK=" + socket}
	command.WaitDelay = 200 * time.Millisecond
	return command
}

// doctorRepositoryOrigin reads one literal local value. It does not resolve
// URL rewrites, contact a remote, load global Git configuration, or invoke SSH.
func doctorRepositoryOrigin(ctx context.Context, repo string) (string, error) {
	return doctorRepositoryURL(ctx, repo, "remote.origin.url", false)
}

func doctorRepositoryPushOrigin(ctx context.Context, repo string) (string, error) {
	return doctorRepositoryURL(ctx, repo, "remote.origin.pushurl", true)
}

func doctorGitProtocol(origin string) string {
	if _, ok := gitssh.RepositoryFromOrigin(origin); ok {
		return "SSH"
	}
	if _, ok := doctorHTTPSRepository(origin); ok {
		return "HTTPS"
	}
	return ""
}

func doctorGitRepository(origin string) string {
	if repository, ok := doctorHTTPSRepository(origin); ok {
		return repository
	}
	repository, _ := gitssh.RepositoryFromOrigin(origin)
	return repository
}

// Keep the existing Git HTTPS path policy: names may start with '.', '_',
// or '-'. The SSH helper intentionally has a narrower repository grammar.
// Validate the literal URL; no decoded or normalized URL becomes evidence.
func doctorHTTPSRepository(origin string) (string, bool) {
	const prefix = "https://github.com/"
	if !strings.HasPrefix(origin, prefix) {
		return "", false
	}
	path := strings.TrimPrefix(origin, prefix)
	parts := strings.Split(path, "/")
	if len(parts) != 2 || !strings.HasSuffix(parts[1], ".git") {
		return "", false
	}
	for _, name := range []string{parts[0], strings.TrimSuffix(parts[1], ".git")} {
		if name == "" || len(name) > 100 {
			return "", false
		}
		for _, ch := range name {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
				return "", false
			}
		}
	}
	return strings.TrimSuffix(path, ".git"), true
}

func doctorRepositoryURL(ctx context.Context, repo, key string, optional bool) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	command := exec.CommandContext(probeCtx, "/usr/bin/git", "-C", repo, "config", "--local", "--no-includes", "--get-all", key)
	command.Env = []string{"PATH=/usr/bin:/bin:/usr/sbin:/sbin", "LANG=C", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"}
	command.WaitDelay = 200 * time.Millisecond
	var output doctorOriginBuffer
	command.Stdout = &output
	if err := command.Run(); err != nil {
		var exit *exec.ExitError
		if optional && probeCtx.Err() == nil && output.Len() == 0 && errors.As(err, &exit) && exit.ExitCode() == 1 {
			return "", nil
		}
		return "", errors.New("local origin unavailable")
	}
	origin := strings.TrimSuffix(output.String(), "\n")
	if origin == "" || strings.ContainsAny(origin, "\x00\r\n") {
		return "", errors.New("local origin ambiguous")
	}
	return origin, nil
}

type doctorOriginBuffer struct{ buffer bytes.Buffer }

func (buffer *doctorOriginBuffer) Len() int       { return buffer.buffer.Len() }
func (buffer *doctorOriginBuffer) String() string { return buffer.buffer.String() }

func (buffer *doctorOriginBuffer) Write(value []byte) (int, error) {
	if buffer.Len()+len(value) > 4096 {
		return 0, errors.New("local origin output exceeds limit")
	}
	return buffer.buffer.Write(value)
}
