package providercoord

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestMultiCLIBindingRejectsImplicitBillingModes(t *testing.T) {
	for _, provider := range []string{"codex", "claude", "cursor"} {
		for _, mode := range []string{"", "chatgpt_subscription", "claude_subscription", "cursor_browser", "cursor_api", "api", "unknown"} {
			binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: provider, Model: "explicit", Family: "explicit", Version: "measured"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), AuthDigest: strings.Repeat("d", 64), AuthMode: mode}
			want := provider == "codex" && mode == "chatgpt_subscription" || provider == "claude" && mode == "claude_subscription" || provider == "cursor" && (mode == "cursor_browser" || mode == "cursor_api")
			if validBinding(binding) != want {
				t.Fatalf("wrong admission for %s/%s", provider, mode)
			}
		}
	}
}
