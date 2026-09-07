package cursorprovider

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

var ErrInvocation = errors.New("Cursor invocation does not match the trusted-hooks guarded contract")

// TrustedHooksPolicy names the operator-approved assumption, not a passing
// qualification. Native role permissions and process drain still need proof.
const TrustedHooksPolicy = "cursor-trusted-hooks-v1"

// Invocation is exec-free. The supervisor must install Permissions(input) in
// its private CLI configuration and independently enforce the role's physical
// file boundary before it can admit this proposal. Existing managed hooks are
// trusted, not silently removed or represented as isolated.
func Invocation(ctx context.Context, executable, authHome string, input contracts.PhaseInput) (contracts.Invocation, error) {
	if err := validateInvocation(ctx, executable, authHome, input); err != nil {
		return contracts.Invocation{}, err
	}
	// SF's outer profile constrains every file tool. The pinned CLI's nested
	// sandbox refuses inside it; disabling that inner shell sandbox does not
	// remove the mandatory supervisor-owned physical boundary.
	args := []string{executable, "--print", "--output-format", "stream-json", "--model", input.Provider.Model, "--workspace", input.Worktree, "--trust", "--sandbox", "disabled"}
	if input.Phase == domain.PhasePlanning || input.Phase == domain.PhaseReview {
		args = append(args, "--mode", "ask")
	}
	// Do not use --force. Allow entries are tool hints, not a default-deny
	// boundary; the supervisor must apply its qualified outer file profile.
	prompt := "Return exactly one JSON object matching the supplied output schema, without markdown or commentary. Do not invoke shell, Git, GitHub, MCP, or web tools. SF runs verification commands separately; never claim an unavailable command succeeded.\nOUTPUT_SCHEMA:\n" + string(input.Schema) + "\n\n" + input.Prompt
	if input.Phase == domain.PhaseBuild {
		prompt = "Report changed_files as your implementation contribution, excluding preserved verification-owned files. Do not manufacture changes after a base refresh.\n" + prompt
	}
	if input.Repair != nil {
		prompt += "\nSF authenticated a fully drained prior attempt. Inspect retained phase-owned changes and produce a complete corrected artifact; do not assume the prior response was valid."
	}
	return contracts.Invocation{Argv: args, Stdin: []byte(prompt), AuthHome: authHome}, nil
}

func MatchesInvocation(ctx context.Context, executable, authHome string, input contracts.PhaseInput, proposed contracts.Invocation) bool {
	want, err := Invocation(ctx, executable, authHome, input)
	return err == nil && reflect.DeepEqual(want, proposed)
}

// Permissions returns only a code-owned CLI policy proposal. Cursor's tool
// permissions are not claimed to constrain trusted hooks. Never use this JSON
// alone as a qualification or as a replacement for physical file enforcement.
func Permissions(input contracts.PhaseInput) ([]byte, error) {
	if err := validateInvocation(context.Background(), "/sf/cursor-agent", "/sf/auth", input); err != nil {
		return nil, err
	}
	allow := []string{"Read(" + input.Worktree + "/**)"}
	deny := []string{"Shell(*)", "Mcp(*:*)", "WebFetch(*)", "Write(.git)", "Write(.git/**)", "Write(**/.git/**)", "Write(.sf/**)", "Write(.cursor/**)", "Write(.claude/**)"}
	if input.Phase == domain.PhasePlanning || input.Phase == domain.PhaseReview {
		deny = append(deny, "Write(**)")
	} else {
		for _, p := range input.AllowedPaths {
			allow = append(allow, "Write("+p+")", "Write("+p+"/**)")
		}
	}
	return json.Marshal(struct {
		Permissions struct {
			Allow []string `json:"allow"`
			Deny  []string `json:"deny"`
		} `json:"permissions"`
	}{Permissions: struct {
		Allow []string `json:"allow"`
		Deny  []string `json:"deny"`
	}{Allow: allow, Deny: deny}})
}

func validateInvocation(ctx context.Context, executable, authHome string, input contracts.PhaseInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	family, ok := ModelFamily(input.Provider.Model)
	if !ok || input.Provider.Provider != "cursor" || input.Provider.Family != family || input.Provider.Version != "2026.09.02-c22c1a3" || input.AuthMode != AuthModeBrowser || input.Profile != contracts.ProfileGuarded {
		return ErrInvocation
	}
	for _, p := range []string{executable, authHome, input.Worktree} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || strings.ContainsAny(p, "\x00\r\n()*?[]{}") {
			return ErrInvocation
		}
	}
	if len(input.Prompt) == 0 || len(input.Prompt) > 64<<10 || strings.ContainsRune(input.Prompt, '\x00') || len(input.Schema) == 0 || len(input.Schema) > 64<<10 || !json.Valid(input.Schema) || input.Timeout <= 0 || input.Timeout > contracts.MultiCLIRequestTimeout {
		return ErrInvocation
	}
	if input.RequestDigest != "" && !contracts.PhaseInputDigestMatches(input, input.RequestDigest) || input.Repair != nil && (input.RequestDigest == "" || !contracts.ValidProviderRepairContext(input.Repair)) {
		return ErrInvocation
	}
	write := input.Phase == domain.PhaseBuild || input.Phase == domain.PhaseVerification
	if !write && input.Phase != domain.PhasePlanning && input.Phase != domain.PhaseReview || len(input.AllowedPaths) == 0 || len(input.AllowedPaths) > 256 {
		return ErrInvocation
	}
	for _, p := range input.AllowedPaths {
		if p == "" || filepath.IsAbs(p) || filepath.Clean(p) != p || p == ".." || strings.HasPrefix(p, "../") || strings.ContainsAny(p, "\x00\r\n\\()*?[]{}") || write && p == "." {
			return ErrInvocation
		}
	}
	return nil
}
