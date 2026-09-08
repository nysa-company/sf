package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/cursorprovider"
	"github.com/nysa-company/sf/internal/domain"
)

var errCursorObservation = errors.New("Cursor runtime observation failed; check version and browser login")

// Policy identity is not a signed qualification or Store admission grant.
func cursorPolicyDigest() string {
	sum := sha256.Sum256([]byte("sf-cursor-policy-v1\x00" + cursorprovider.TrustedHooksPolicy + "\x00outer-role-seatbelt-v1\x00owned-local-worker-lifecycle-v1\x00short-private-home\x00browser-keychain\x00staged-cli\x00same-session-terminal-v2\x00no-shell-mcp\x00bounded-stdio\x00drain-owned-cleanup\x00" + contracts.MultiCLIRequestPolicy))
	return hex.EncodeToString(sum[:])
}

// cursorEnvironment owns a short private home. The pinned CLI falls back to
// shared /tmp/.cursor for long homes; do not authorize that fallback. Cleanup
// belongs to the supervisor's process-completion lifetime once launched.
func cursorEnvironment(ctx context.Context, expected string, lookup cliSecretLookup) ([]string, string, string, func(), error) {
	if runtime.GOOS != "darwin" || ctx.Err() != nil {
		return nil, "", "", func() {}, errCursorObservation
	}
	home, err := os.MkdirTemp("/private/tmp", "sf-cursor-")
	if err != nil {
		return nil, "", "", func() {}, errCursorObservation
	}
	cleanup := func() { _ = os.RemoveAll(home) }
	extra, digest, err := prepareCLICredentials(ctx, "cursor", home, lookup)
	if err != nil || expected != "" && digest != expected {
		cleanup()
		return nil, "", "", func() {}, errCLICredentials
	}
	tmp := filepath.Join(home, "tmp")
	if os.Mkdir(tmp, 0700) != nil {
		cleanup()
		return nil, "", "", func() {}, errCursorObservation
	}
	env := append([]string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=" + home, "TMPDIR=" + tmp, "NO_COLOR=1", "TERM=dumb"}, extra...)
	return env, home, digest, cleanup, nil
}

// ObserveCursorRuntime is status/catalog-only, not a model request or passing
// qualification. The display label is included in the fixture digest, so a
// restart/catalog change cannot silently change how results are authenticated.
func (s *Supervisor) ObserveCursorRuntime(ctx context.Context, executable, model string) (contracts.RuntimeBinding, string, error) {
	return s.observeCursorRuntime(ctx, executable, model, lookupCLISecret)
}

func (s *Supervisor) observeCursorRuntime(ctx context.Context, executable, model string, lookup cliSecretLookup) (contracts.RuntimeBinding, string, error) {
	fail := func() (contracts.RuntimeBinding, string, error) {
		return contracts.RuntimeBinding{}, "", errCursorObservation
	}
	family, ok := cursorprovider.ModelFamily(model)
	if s == nil || runtime.GOOS != "darwin" || !ok {
		return fail()
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	bundle, err := cliruntime.Resolve(ctx, "cursor", executable)
	if err != nil {
		return fail()
	}
	trusted := trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}
	if trusted.stage() != nil {
		return fail()
	}
	defer os.RemoveAll(trusted.stagedDir)
	env, home, authDigest, cleanup, err := cursorEnvironment(ctx, "", lookup)
	if err != nil {
		return fail()
	}
	defer cleanup()
	probe := func(args ...string) ([]byte, error) {
		c, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		cmd := exec.CommandContext(c, trusted.stagedPath, args...)
		cmd.Dir, cmd.Env = home, env
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.WaitDelay = time.Second
		cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
		var out, stderr limitedBuffer
		out.limit, stderr.limit = 64<<10, 16<<10
		cmd.Stdout, cmd.Stderr = &out, &stderr
		if cmd.Run() != nil || out.truncated || stderr.truncated {
			return nil, errCursorObservation
		}
		return out.Bytes(), nil
	}
	version, err := probe("--version")
	if err != nil || strings.TrimSpace(string(version)) != "2026.09.02-c22c1a3" {
		return fail()
	}
	status, err := probe("status", "--format", "json")
	if err != nil || !cursorprovider.BrowserAuthenticated(status) {
		return fail()
	}
	catalog, err := probe("models")
	if err != nil {
		return fail()
	}
	display, err := cursorprovider.CatalogDisplay(catalog, model)
	if err != nil {
		return fail()
	}
	display, err = cursorprovider.SessionDisplay(model, display)
	if err != nil {
		return fail()
	}
	current, err := cliruntime.Resolve(ctx, "cursor", executable)
	if err != nil || current.Digest() != bundle.Digest() || current.Executable() != bundle.Executable() || !stagedRuntimeMatches(trusted.snapshot, bundle.Digest()) || ctx.Err() != nil {
		return fail()
	}
	return contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "cursor", Model: model, Family: family, Version: "2026.09.02-c22c1a3"}, BinaryDigest: bundle.Digest(), PolicyDigest: cursorPolicyDigest(), FixtureDigest: cursorprovider.FixtureDigest(model, display), AuthDigest: authDigest, AuthMode: cursorprovider.AuthModeBrowser}, display, nil
}
