package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"runtime"
	"time"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/codexruntime"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/cursorprovider"
)

// ProviderPolicyDigest identifies code-owned runtime policy. It does not sign
// or qualify it. Every provider still needs fresh signed native qualification.
// Preserve Codex's existing digest for stored claims and restart compatibility.
func (s *Supervisor) ProviderPolicyDigest(provider string) string {
	switch provider {
	case "codex":
		return environmentPolicyDigest()
	case "claude":
		sum := sha256.Sum256([]byte("sf-claude-policy-v4\x00complete-stream-json-v2\x00signed-server-rejection-v1\x00builder-inventory-guidance-v1\x00draft07-common-subset-projection\x00private-home\x00keychain-oauth-access-only\x00staged-cli\x00restricted-safe-mode\x00dontAsk\x00role-file-tools\x00no-shell-mcp\x00no-session-persistence\x00no-autoupdate\x00bounded-stdio\x00drain-owned-cleanup\x00" + contracts.MultiCLIRequestPolicy))
		return hex.EncodeToString(sum[:])
	case "cursor":
		return cursorPolicyDigest()
	default:
		return ""
	}
}

func registeredRuntime(binding contracts.RuntimeBinding, executable string) (trustedExecutable, error) {
	var supervisor *Supervisor
	policy := supervisor.ProviderPolicyDigest(binding.Identity.Provider)
	if policy == "" || binding.PolicyDigest != policy {
		return trustedExecutable{}, errors.New("unsupported provider runtime policy")
	}
	switch binding.Identity.Provider {
	case "codex":
		if binding.AuthMode != "chatgpt_subscription" {
			break
		}
		bundle, err := codexruntime.Resolve(executable)
		if err != nil {
			return trustedExecutable{}, err
		}
		return trustedExecutable{path: bundle.Codex.Path, digest: bundle.Digest, bundle: bundle}, nil
	case "claude":
		family, ok := claudeprovider.ModelFamily(binding.Identity.Model)
		// Only the version measured by the native role probes is admitted.
		// Expanding this requires rerunning the qualification compatibility suite.
		if runtime.GOOS != "darwin" || !ok || binding.Identity.Family != family || binding.Identity.Version != "2.1.263" || binding.AuthMode != claudeprovider.AuthModeSubscription || binding.FixtureDigest == "" {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		bundle, err := cliruntime.Resolve(ctx, "claude", executable)
		if err != nil {
			return trustedExecutable{}, err
		}
		return trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}, nil
	case "cursor":
		family, ok := cursorprovider.ModelFamily(binding.Identity.Model)
		if runtime.GOOS != "darwin" || !ok || binding.Identity.Family != family || binding.Identity.Version != "2026.09.02-c22c1a3" || binding.AuthMode != cursorprovider.AuthModeBrowser || len(binding.FixtureDigest) != 64 {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		bundle, err := cliruntime.Resolve(ctx, "cursor", executable)
		if err != nil {
			return trustedExecutable{}, err
		}
		return trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}, nil
	}
	return trustedExecutable{}, errors.New("unsupported provider version, family, host, or authentication mode")
}
