// Package executionpolicy defines the code-owned guarded execution baseline.
// It is intentionally explicit about its limit: these checks reduce ambient
// credentials and factory-controlled mutations, but do not claim hostile
// same-UID containment on macOS.
package executionpolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

var (
	ErrCommandDenied       = errors.New("command denied by execution policy")
	ErrAutonomyUnavailable = errors.New("autonomous execution profile is unavailable")
)

type GuardedQualification struct {
	Provider                 domain.ProviderIdentity
	AuthenticationIsolated   bool
	EnvironmentScrubbed      bool
	ProcessSupervised        bool
	GitIdentityAuthenticated bool
	HostileFixturePassed     bool
}

func (qualification GuardedQualification) Validate() error {
	if qualification.Provider.Provider == "" || qualification.Provider.Model == "" || qualification.Provider.Family == "" || qualification.Provider.Version == "" {
		return errors.New("complete provider identity is required")
	}
	checks := []struct {
		name string
		ok   bool
	}{
		{"isolated authentication", qualification.AuthenticationIsolated},
		{"scrubbed environment", qualification.EnvironmentScrubbed},
		{"process supervision", qualification.ProcessSupervised},
		{"Git identity authentication", qualification.GitIdentityAuthenticated},
		{"hostile fixture", qualification.HostileFixturePassed},
	}
	for _, check := range checks {
		if !check.ok {
			return fmt.Errorf("guarded qualification failed: %s", check.name)
		}
	}
	return nil
}

// SelectProfile cannot be raised by configuration. The accepted native spike
// recorded autonomous_eligible=false, so v1 returns a typed refusal for every
// autonomous request until code and its proof artifact are independently
// reviewed and changed.
func SelectProfile(mode domain.MergeMode, qualification GuardedQualification) (contracts.ExecutionProfile, error) {
	if err := qualification.Validate(); err != nil {
		return "", err
	}
	switch mode {
	case domain.MergeManual, domain.MergeGuarded:
		return contracts.ProfileGuarded, nil
	case domain.MergeAutonomous:
		return "", fmt.Errorf("%w: native-profile ADR 0002 records autonomous_eligible=false", ErrAutonomyUnavailable)
	default:
		return "", fmt.Errorf("invalid merge mode %q", mode)
	}
}

// MinimalEnvironment is built from fixed values and never inherits the daemon
// environment. In particular it omits GitHub/provider tokens, SSH agents,
// cloud credentials, and unrelated provider homes.
func MinimalEnvironment(home, temporary string) ([]string, error) {
	if err := secureDirectory("execution home", home); err != nil {
		return nil, err
	}
	if err := secureDirectory("execution temporary directory", temporary); err != nil {
		return nil, err
	}
	values := map[string]string{
		"CI":                  "1",
		"GCM_INTERACTIVE":     "Never",
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_OPTIONAL_LOCKS":  "0",
		"GIT_PAGER":           "cat",
		"GIT_TERMINAL_PROMPT": "0",
		"HOME":                home,
		"LANG":                "en_US.UTF-8",
		"LC_ALL":              "en_US.UTF-8",
		"NO_COLOR":            "1",
		"PAGER":               "cat",
		"PATH":                "/usr/bin:/bin:/usr/sbin:/sbin",
		"TMPDIR":              temporary,
		"GIT_CONFIG_GLOBAL":   "/dev/null",
		"GIT_ATTR_NOSYSTEM":   "1",
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result, nil
}

type CommandDecision struct {
	Allowed bool
	Code    string
	Reason  string
}

type CommandSnapshot struct {
	commands [][]string
	digest   string
}

func NewCommandSnapshot(commands ...[]string) (CommandSnapshot, error) {
	if len(commands) == 0 || len(commands) > 32 {
		return CommandSnapshot{}, errors.New("command policy requires 1 to 32 commands")
	}
	copyCommands := make([][]string, 0, len(commands))
	for _, command := range commands {
		decision := EvaluateRepositoryCommand(command)
		if !decision.Allowed {
			return CommandSnapshot{}, fmt.Errorf("%w: %s: %s", ErrCommandDenied, decision.Code, decision.Reason)
		}
		for _, existing := range copyCommands {
			if equalArgv(existing, command) {
				return CommandSnapshot{}, errors.New("command policy contains duplicate argv")
			}
		}
		copyCommands = append(copyCommands, append([]string(nil), command...))
	}
	data, err := json.Marshal(copyCommands)
	if err != nil {
		return CommandSnapshot{}, err
	}
	sum := sha256.Sum256(data)
	return CommandSnapshot{commands: copyCommands, digest: "sha256:" + hex.EncodeToString(sum[:])}, nil
}

func (snapshot CommandSnapshot) Digest() string { return snapshot.digest }

func (snapshot CommandSnapshot) Authorize(argv []string) error {
	decision := EvaluateRepositoryCommand(argv)
	if !decision.Allowed {
		return fmt.Errorf("%w: %s: %s", ErrCommandDenied, decision.Code, decision.Reason)
	}
	for _, expected := range snapshot.commands {
		if equalArgv(expected, argv) {
			return nil
		}
	}
	return fmt.Errorf("%w: command_policy_changed: argv is not in the immutable ticket snapshot", ErrCommandDenied)
}

func EvaluateRepositoryCommand(argv []string) CommandDecision {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return deny("empty_argv", "an executable argv is required")
	}
	if len(argv) > 64 {
		return deny("argv_too_large", "command exceeds the argument bound")
	}
	for _, argument := range argv {
		if strings.ContainsRune(argument, '\x00') {
			return deny("argv_nul", "arguments may not contain NUL")
		}
	}
	executable := strings.ToLower(filepath.Base(argv[0]))
	// A snapshot makes an argv immutable for a ticket; it cannot make a shell
	// or wrapper safe because wrappers reinterpret their remaining arguments.
	// Keep this boundary positive-allowlisted.
	switch executable {
	case "go":
		return evaluateGoVerification(argv)
	case "node":
		return evaluateNodeVerification(argv)
	default:
		return deny("repository_command_not_allowlisted", "only the exact guarded Go or dependency-free Node 22 test verification recipes are eligible")
	}
}

// evaluateNodeVerification is intentionally a single source argv. The
// supervisor adds its fixed Node 22 hardening flags after Store has bound this
// immutable recipe. Scripts, flags, npm, and shell wrappers are never source
// authority.
func evaluateNodeVerification(argv []string) CommandDecision {
	if len(argv) != 2 || argv[1] != "--test" {
		return deny("node_recipe_forbidden", "only the exact dependency-free Node 22 recipe `node --test` is eligible; flags, scripts, npm, and wrappers require operator takeover")
	}
	return CommandDecision{Allowed: true, Code: "allowed_node_test_recipe", Reason: "exact dependency-free Node 22 test recipe"}
}

// evaluateGoVerification is intentionally a recipe, not a flag denylist.  The
// Go driver runs before a test binary can enter Seatbelt, so flags which select
// an output, module mode, overlay, compiler, linker, cgo, or external tool
// would become pre-sandbox authority.  v1 therefore supports exactly one
// hermetic recipe.  Repositories whose tests need another command (including
// subprocess-using tests) require explicit operator takeover.
func evaluateGoVerification(argv []string) CommandDecision {
	if len(argv) != 3 || argv[1] != "test" || argv[2] != "./..." {
		return deny("go_recipe_forbidden", "only the hermetic recipe `go test ./...` is eligible; flags, tool selection, outputs, module changes, cgo, and subprocess-dependent tests require operator takeover")
	}
	return CommandDecision{Allowed: true, Code: "allowed_go_test_recipe", Reason: "exact hermetic Go test recipe"}
}

func secureDirectory(name, path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%s must be absolute", name)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", name, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%s must be a real directory", name)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("%s must be owner-only", name)
	}
	return nil
}

func deny(code, reason string) CommandDecision {
	return CommandDecision{Allowed: false, Code: code, Reason: reason}
}

func equalArgv(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
