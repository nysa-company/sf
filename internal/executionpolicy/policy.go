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
	switch executable {
	case "gh":
		return deny("github_cli_forbidden", "repository commands cannot invoke gh")
	case "curl", "wget", "ftp", "ssh", "scp", "sftp", "rsync", "nc", "ncat", "netcat":
		return deny("network_command_forbidden", "direct network commands require operator takeover")
	case "launchctl", "installer", "softwareupdate", "brew", "docker", "colima", "kubectl":
		return deny("host_mutation_forbidden", "service, package, VM, and host mutations are forbidden")
	case "git":
		return evaluateReadOnlyGit(argv)
	case "npx":
		return deny("package_execution_forbidden", "on-demand package execution requires operator takeover")
	case "npm", "pnpm", "yarn", "bun":
		if containsWord(argv[1:], "install", "add", "remove", "update", "upgrade", "publish", "link", "dlx", "exec") {
			return deny("package_mutation_forbidden", "dependency installation or publication requires operator takeover")
		}
	case "pip", "pip3", "gem", "cargo":
		if containsWord(argv[1:], "install", "uninstall", "update", "publish", "yank", "add", "remove") {
			return deny("package_mutation_forbidden", "dependency installation or publication requires operator takeover")
		}
	case "go":
		if len(argv) > 1 && (argv[1] == "get" || argv[1] == "install") {
			return deny("package_mutation_forbidden", "go get/install requires operator takeover")
		}
		if len(argv) > 2 && argv[1] == "env" && (argv[2] == "-w" || argv[2] == "-u") {
			return deny("host_mutation_forbidden", "go env mutation requires operator takeover")
		}
		if len(argv) > 2 && argv[1] == "clean" && containsWord(argv[2:], "-modcache", "-cache", "-testcache", "-fuzzcache") {
			return deny("host_mutation_forbidden", "shared Go cache mutation requires operator takeover")
		}
	case "make":
		if containsWord(argv[1:], "install", "deploy", "release", "publish") {
			return deny("release_command_forbidden", "install, deploy, release, and publish targets are forbidden")
		}
	}
	return CommandDecision{Allowed: true, Code: "allowed_registered_command", Reason: "argv is eligible for immutable registration"}
}

func evaluateReadOnlyGit(argv []string) CommandDecision {
	if len(argv) < 2 || strings.HasPrefix(argv[1], "-") {
		return deny("git_control_plane_forbidden", "Git global options and implicit operations are forbidden")
	}
	switch argv[1] {
	case "status", "rev-parse", "ls-files":
	case "diff":
		if !containsWord(argv[2:], "--no-ext-diff") || !containsWord(argv[2:], "--no-textconv") {
			return deny("git_external_execution_forbidden", "Git diff requires --no-ext-diff and --no-textconv")
		}
	default:
		return deny("git_mutation_forbidden", "only bounded read-only Git subcommands are allowed")
	}
	for _, argument := range argv[2:] {
		if argument == "--ext-diff" || argument == "--textconv" || strings.HasPrefix(argument, "--output") || strings.HasPrefix(argument, "--exec") {
			return deny("git_external_execution_forbidden", "Git may not execute helpers or write output")
		}
	}
	return CommandDecision{Allowed: true, Code: "allowed_read_only_git", Reason: "read-only Git command"}
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

func containsWord(values []string, targets ...string) bool {
	set := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		set[target] = struct{}{}
	}
	for _, value := range values {
		if _, exists := set[strings.ToLower(value)]; exists {
			return true
		}
	}
	return false
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
