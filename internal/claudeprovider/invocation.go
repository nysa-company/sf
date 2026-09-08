// Package claudeprovider defines the Claude Code protocol boundary. Runtime
// registration and credential qualification must pass before these proposals
// can be launched; this package never starts a process.
package claudeprovider

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

const AuthModeSubscription = "claude_subscription"

// This provider-specific clarification belongs to the qualified invocation
// policy, not the shared canonical PhaseInput (including historical Codex).
// Bump the supervisor's Claude policy when this instruction changes.
const builderInventoryGuidance = `changed_files is your implementation contribution for this ticket, not the whole branch diff. Always exclude preserved verification-owned files listed in VERIFICATION.canonical_artifact.owned_files; their presence in the checkout or difference from the protected base does not mean you changed them. Include a protected file only when you actually need to change it and supply the required amendment_request.
After a base refresh, the existing implementation may already satisfy the plan. Inspect it, retain its implementation-file inventory, and report command evidence honestly; do not manufacture edits or include preserved tests merely to populate changed_files. Never claim that an unavailable command succeeded.`

// MatchesInvocation is shared with the supervisor so an adapter cannot expand
// permissions, add positional prompt injection, resume another session or
// change output handling after Store has bound the phase input. The executable
// and auth home are taken from the registered runtime, not the proposal.
func MatchesInvocation(ctx context.Context, executable, authHome string, input contracts.PhaseInput, proposed contracts.Invocation) bool {
	expected, err := Invocation(ctx, executable, authHome, input)
	return err == nil && reflect.DeepEqual(expected, proposed)
}

var ErrInvocation = errors.New("Claude invocation does not match the supported guarded contract")

// ModelFamily is intentionally closed. Aliases such as sonnet/default can
// change their underlying model without changing the claim and are refused.
// Different Claude products are not asserted to be independent families.
func ModelFamily(model string) (string, bool) {
	switch model {
	case "claude-sonnet-4-6", "claude-opus-4-6", "claude-sonnet-5", "claude-opus-5":
		return "anthropic-claude", true
	default:
		return "", false
	}
}

// SupportedModels names exact adapter IDs, not installed/account readiness.
func SupportedModels() []string {
	return []string{"claude-sonnet-5", "claude-opus-5", "claude-sonnet-4-6", "claude-opus-4-6"}
}

// Invocation proposes code-owned argv. It requires a qualified runtime with
// --restricted and --safe-mode support; it does not assert such a runtime is
// installed. No permission bypass, shell tool, MCP, fallback, or session resume
// is requested. SF's separate repository executor owns verification commands.
func Invocation(ctx context.Context, executable, authHome string, input contracts.PhaseInput) (contracts.Invocation, error) {
	if err := ctx.Err(); err != nil {
		return contracts.Invocation{}, err
	}
	family, ok := ModelFamily(input.Provider.Model)
	if !ok || input.Provider.Provider != "claude" || input.Provider.Family != family || input.Provider.Version == "" || input.AuthMode != AuthModeSubscription || input.Profile != contracts.ProfileGuarded {
		return contracts.Invocation{}, ErrInvocation
	}
	for _, path := range []string{executable, authHome, input.Worktree} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.ContainsRune(path, '\x00') {
			return contracts.Invocation{}, ErrInvocation
		}
	}
	if len(input.Prompt) == 0 || len(input.Prompt) > 64<<10 || strings.ContainsRune(input.Prompt, '\x00') || len(input.Schema) == 0 || len(input.Schema) > 64<<10 || !json.Valid(input.Schema) || input.Timeout <= 0 || input.Timeout > contracts.MultiCLIRequestTimeout {
		return contracts.Invocation{}, ErrInvocation
	}
	if input.RequestDigest != "" && !contracts.PhaseInputDigestMatches(input, input.RequestDigest) || input.Repair != nil && (input.RequestDigest == "" || !contracts.ValidProviderRepairContext(input.Repair)) {
		return contracts.Invocation{}, ErrInvocation
	}
	tools := "Read,Glob,Grep"
	switch input.Phase {
	case domain.PhasePlanning, domain.PhaseReview:
	case domain.PhaseVerification, domain.PhaseBuild:
		tools += ",Edit,Write"
	default:
		return contracts.Invocation{}, ErrInvocation
	}
	prompt := input.Prompt
	if input.Phase == domain.PhaseBuild {
		var identity struct {
			ID string `json:"$id"`
		}
		// Qualification fixtures can use a different output schema. Do not
		// invent Builder fields for their bounded permission/cancel probes.
		if json.Unmarshal(input.Schema, &identity) == nil && identity.ID == "sf.builder/v1" {
			prompt = builderInventoryGuidance + "\n\n" + prompt
		}
	}
	schema, err := cliSchema(input.Schema)
	if err != nil {
		return contracts.Invocation{}, err
	}
	if input.Repair != nil {
		prompt += "\n\nSF has authenticated a fully drained prior attempt for this exact phase. Inspect its retained changes, repair the complete artifact and account for every phase-owned change. Do not rely on the prior provider response."
	}
	return contracts.Invocation{
		Argv: []string{executable, "--print", "--output-format", "stream-json", "--verbose", "--json-schema", string(schema),
			"--model", input.Provider.Model, "--restricted", "--safe-mode", "--no-session-persistence",
			"--permission-mode", "dontAsk", "--tools", tools, "--allowedTools", tools,
			"--disallowedTools", "mcp__*", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`},
		Stdin: []byte(prompt), AuthHome: authHome,
	}, nil
}
