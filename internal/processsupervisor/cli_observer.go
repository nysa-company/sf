package processsupervisor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providerjson"
)

var errCLIObservation = errors.New("Claude runtime observation failed; check version and login")

// ObserveClaudeRuntime is a status-only measurement for the adapter's observer.
// It never supplies a prompt, trusts project configuration, signs qualification,
// or asserts billing authority. Native role fixtures must pass separately.
func (s *Supervisor) ObserveClaudeRuntime(ctx context.Context, executable, model string) (contracts.RuntimeBinding, error) {
	return s.observeClaudeRuntime(ctx, executable, model, lookupCLISecret)
}

func (s *Supervisor) observeClaudeRuntime(ctx context.Context, executable, model string, lookup cliSecretLookup) (contracts.RuntimeBinding, error) {
	return s.observeClaudeOperation(ctx, executable, model, lookup, false)
}

func (s *Supervisor) observeClaudeOperation(ctx context.Context, executable, model string, lookup cliSecretLookup, authoring bool) (contracts.RuntimeBinding, error) {
	family, ok := claudeprovider.ModelFamily(model)
	if s == nil || runtime.GOOS != "darwin" || !ok {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	bundle, err := cliruntime.Resolve(ctx, "claude", executable)
	if err != nil {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	trusted := trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}
	if trusted.stage() != nil {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	defer os.RemoveAll(trusted.stagedDir)
	env, _, cleanup, err := vettedEnvironment("")
	if err != nil {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	defer cleanup()
	var home string
	for i, item := range env {
		if strings.HasPrefix(item, "HOME=") {
			home, err = filepath.EvalSymlinks(strings.TrimPrefix(item, "HOME="))
			if err != nil {
				return contracts.RuntimeBinding{}, errCLIObservation
			}
			env[i] = "HOME=" + home
		}
	}
	extra, authDigest, err := prepareCLICredentials(ctx, "claude", home, lookup)
	if err != nil {
		if errors.Is(err, errClaudeAuthRenewal) {
			return contracts.RuntimeBinding{}, errClaudeAuthRenewal
		}
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	env = append(env, extra...)
	probe := func(args ...string) ([]byte, error) {
		probeCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		defer stop()
		cmd := exec.CommandContext(probeCtx, trusted.stagedPath, args...)
		cmd.Dir, cmd.Env = home, env
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.WaitDelay = time.Second
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		var out, stderr limitedBuffer
		out.limit, stderr.limit = 64<<10, 16<<10
		cmd.Stdout, cmd.Stderr = &out, &stderr
		if cmd.Run() != nil || out.truncated || stderr.truncated {
			return nil, errCLIObservation
		}
		return out.Bytes(), nil
	}
	version, err := probe("--version")
	if err != nil || strings.TrimSpace(string(version)) != "2.1.263 (Claude Code)" {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	help, err := probe("--help")
	if err != nil {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	for _, flag := range []string{"--restricted", "--safe-mode", "--no-session-persistence", "--strict-mcp-config", "--json-schema"} {
		if !bytes.Contains(help, []byte(flag)) {
			return contracts.RuntimeBinding{}, errCLIObservation
		}
	}
	if authoring {
		for _, flag := range []string{"--bare", "--tools", "--max-turns", "--permission-mode", "--allowedTools", "--disallowedTools"} {
			if !bytes.Contains(help, []byte(flag)) {
				return contracts.RuntimeBinding{}, errCLIObservation
			}
		}
	}
	status, err := probe("--safe-mode", "--restricted", "auth", "status")
	if err != nil || !validObservedClaudeAuth(status) {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	current, err := cliruntime.Resolve(ctx, "claude", executable)
	if err != nil || current.Digest() != bundle.Digest() || current.Executable() != bundle.Executable() || !stagedRuntimeMatches(trusted.snapshot, bundle.Digest()) {
		return contracts.RuntimeBinding{}, errCLIObservation
	}
	// This identifies the required qualification suite, not a passing verdict.
	fixture := sha256.Sum256([]byte("sf-claude-fixture-v3:version,flags,oauth,complete-stream-json-v1,draft2020-projection,role-write,outside-read-denied,no-shell,no-mcp,cancel-drain"))
	return contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "claude", Model: model, Family: family, Version: "2.1.263"}, BinaryDigest: bundle.Digest(), PolicyDigest: s.ProviderPolicyDigest("claude"), FixtureDigest: hex.EncodeToString(fixture[:]), AuthDigest: authDigest, AuthMode: claudeprovider.AuthModeSubscription}, nil
}

func validObservedClaudeAuth(status []byte) bool {
	if len(status) > 16<<10 {
		return false
	}
	fields, err := providerjson.Object(status)
	if err != nil {
		return false
	}
	var loggedIn bool
	var method string
	return json.Unmarshal(fields["loggedIn"], &loggedIn) == nil && loggedIn && json.Unmarshal(fields["authMethod"], &method) == nil && method == "oauth_token"
}
