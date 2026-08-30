// Package auth owns the direct, local authentication boundary for the
// allowlisted GitHub and provider CLIs. It never accepts, stores, or returns a
// credential byte: status output is discarded and login is attached directly
// to the operator's terminal.
package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/redact"
)

type Provider string

const (
	GitHub Provider = "github"
	Cursor Provider = "cursor"
	Claude Provider = "claude"
	Codex  Provider = "codex"
)

type State string

const (
	StateAuthenticated   State = "authenticated"
	StateUnauthenticated State = "unauthenticated"
	StateUnavailable     State = "unavailable"
	StateProbeFailed     State = "probe_failed"
)

var (
	ErrUnknownProvider = errors.New("unknown authentication provider")
	ErrNotInstalled    = errors.New("authentication executable is unavailable")
	ErrProbeFailed     = errors.New("authentication probe failed")
	ErrLoginFailed     = errors.New("interactive authentication failed")
	ErrBinaryChanged   = errors.New("authentication executable changed during probe")
)

const (
	probeTimeout   = 5 * time.Second
	maximumOutput  = 16 * 1024
	maximumVersion = 200
)

type definition struct {
	Provider    Provider
	Executable  string
	VersionArgs []string
	StatusArgs  []string
	LoginArgs   []string
	StatusMode  statusMode
}

type statusMode uint8

const (
	statusExitCode statusMode = iota
	statusCursorText
)

var definitions = []definition{
	{Provider: GitHub, Executable: "gh", VersionArgs: []string{"--version"}, StatusArgs: []string{"auth", "status", "--active", "--hostname", "github.com"}, LoginArgs: []string{"auth", "login", "--hostname", "github.com", "--git-protocol", "https", "--web"}},
	{Provider: Cursor, Executable: "cursor-agent", VersionArgs: []string{"--version"}, StatusArgs: []string{"status"}, LoginArgs: []string{"login"}, StatusMode: statusCursorText},
	{Provider: Claude, Executable: "claude", VersionArgs: []string{"--version"}, StatusArgs: []string{"auth", "status"}, LoginArgs: []string{"auth", "login"}},
	{Provider: Codex, Executable: "codex", VersionArgs: []string{"--version"}, StatusArgs: []string{"login", "status"}, LoginArgs: []string{"login"}},
}

// Status is a sanitized observation. Executable is the allowlisted command
// name, not a host path, and no provider output is retained.
type Status struct {
	Provider      Provider `json:"provider"`
	Executable    string   `json:"executable"`
	Installed     bool     `json:"installed"`
	Authenticated bool     `json:"authenticated"`
	State         State    `json:"state"`
	Version       string   `json:"version,omitempty"`
	Reason        string   `json:"reason,omitempty"`

	binary binaryIdentity
}

type Terminal struct {
	In  io.Reader
	Out io.Writer
	Err io.Writer
}

type ProbeResult struct {
	ExitCode int
	Output   []byte
}

type Runner interface {
	Probe(context.Context, string, []string, []string, int) (ProbeResult, error)
	Interactive(context.Context, string, []string, []string, Terminal) (int, error)
}

type Manager struct {
	Lookup     func(string) (string, error)
	Stat       func(string) (os.FileInfo, error)
	Eval       func(string) (string, error)
	CurrentUID func() uint32
	Home       func() (string, error)
	Getenv     func(string) string
	Runner     Runner
}

func NewManager() Manager {
	return Manager{}.defaults()
}

func (manager Manager) defaults() Manager {
	if manager.Lookup == nil {
		manager.Lookup = exec.LookPath
	}
	if manager.Stat == nil {
		manager.Stat = os.Stat
	}
	if manager.Eval == nil {
		manager.Eval = filepath.EvalSymlinks
	}
	if manager.CurrentUID == nil {
		manager.CurrentUID = func() uint32 { return uint32(os.Geteuid()) }
	}
	if manager.Home == nil {
		manager.Home = os.UserHomeDir
	}
	if manager.Getenv == nil {
		manager.Getenv = os.Getenv
	}
	if manager.Runner == nil {
		manager.Runner = OSRunner{}
	}
	return manager
}

func Providers() []Provider {
	result := make([]Provider, 0, len(definitions))
	for _, item := range definitions {
		result = append(result, item.Provider)
	}
	return result
}

func ParseProvider(value string) (Provider, error) {
	if strings.ContainsAny(value, "\x00\r\n\t") {
		return "", fmt.Errorf("%w: %q", ErrUnknownProvider, value)
	}
	provider := Provider(strings.ToLower(strings.TrimSpace(value)))
	if _, ok := find(provider); !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownProvider, value)
	}
	return provider, nil
}

func (manager Manager) StatusAll(ctx context.Context) []Status {
	statuses := make([]Status, 0, len(definitions))
	for _, item := range definitions {
		statuses = append(statuses, manager.Status(ctx, item.Provider))
	}
	return statuses
}

func (manager Manager) Status(ctx context.Context, provider Provider) Status {
	manager = manager.defaults()
	item, ok := find(provider)
	if !ok {
		return Status{Provider: provider, State: StateProbeFailed, Reason: "provider is not allowlisted"}
	}
	status := Status{Provider: provider, Executable: item.Executable, State: StateUnavailable}
	binary, err := manager.resolve(item.Executable)
	if err != nil {
		if errors.Is(err, ErrNotInstalled) {
			status.Reason = "executable is not installed"
		} else {
			status.State = StateProbeFailed
			status.Reason = "executable identity is unsafe"
		}
		return status
	}
	status.Installed = true
	status.binary = binary
	environment, err := manager.environment(binary.path)
	if err != nil {
		status.State = StateProbeFailed
		status.Reason = "authentication environment is unsafe"
		return status
	}
	versionContext, cancelVersion := context.WithTimeout(ctx, probeTimeout)
	versionResult, versionErr := manager.Runner.Probe(versionContext, binary.path, item.VersionArgs, environment, maximumOutput)
	cancelVersion()
	if versionErr != nil || versionResult.ExitCode != 0 {
		status.State = StateProbeFailed
		status.Reason = "version probe failed"
		return status
	}
	if err := manager.validate(binary); err != nil {
		status.State = StateProbeFailed
		status.Reason = "executable changed during probe"
		return status
	}
	version, err := safeVersion(versionResult.Output)
	if err != nil {
		status.State = StateProbeFailed
		status.Reason = "version output is invalid"
		return status
	}
	status.Version = version
	statusContext, cancelStatus := context.WithTimeout(ctx, probeTimeout)
	// GitHub, Claude, and Codex document an exit-code contract, so their raw
	// status is discarded at the process boundary. Cursor documents only a
	// human status command; its bounded output is interpreted fail-closed and
	// is never returned or persisted.
	statusLimit := 0
	if item.StatusMode == statusCursorText {
		statusLimit = maximumOutput
	}
	statusResult, statusErr := manager.Runner.Probe(statusContext, binary.path, item.StatusArgs, environment, statusLimit)
	cancelStatus()
	if statusErr != nil {
		status.State = StateProbeFailed
		status.Reason = "authentication status probe failed"
		return status
	}
	if err := manager.validate(binary); err != nil {
		status.State = StateProbeFailed
		status.Reason = "executable changed during probe"
		return status
	}
	authenticated, interpretationErr := interpretStatus(item.StatusMode, statusResult)
	if interpretationErr != nil {
		status.State = StateProbeFailed
		status.Reason = "authentication status was not recognized"
		return status
	}
	if authenticated {
		status.State = StateAuthenticated
		status.Authenticated = true
		return status
	}
	status.State = StateUnauthenticated
	status.Reason = "interactive login is required"
	return status
}

// Login delegates to the allowlisted official CLI with direct terminal I/O.
// It does not capture the login exchange and it re-probes authentication before
// reporting success. The bool reports whether a login process was started.
func (manager Manager) Login(ctx context.Context, provider Provider, terminal Terminal) (Status, bool, error) {
	manager = manager.defaults()
	item, ok := find(provider)
	if !ok {
		return Status{}, false, fmt.Errorf("%w: %q", ErrUnknownProvider, provider)
	}
	before := manager.Status(ctx, provider)
	if !before.Installed {
		return before, false, ErrNotInstalled
	}
	if before.State == StateProbeFailed {
		return before, false, ErrProbeFailed
	}
	if before.Authenticated {
		return before, false, nil
	}
	if terminal.In == nil || terminal.Out == nil || terminal.Err == nil {
		return before, false, fmt.Errorf("%w: terminal streams are required", ErrLoginFailed)
	}
	if err := manager.validate(before.binary); err != nil {
		return before, false, ErrBinaryChanged
	}
	environment, err := manager.environment(before.binary.path)
	if err != nil {
		return before, false, fmt.Errorf("%w: unsafe environment", ErrLoginFailed)
	}
	exitCode, runErr := manager.Runner.Interactive(ctx, before.binary.path, item.LoginArgs, environment, terminal)
	if runErr != nil || exitCode != 0 {
		if ctx.Err() != nil {
			return before, true, fmt.Errorf("%w: %w", ErrLoginFailed, ctx.Err())
		}
		return before, true, ErrLoginFailed
	}
	if err := manager.validate(before.binary); err != nil {
		return before, true, ErrBinaryChanged
	}
	after := manager.Status(ctx, provider)
	if !after.Authenticated {
		return after, true, ErrLoginFailed
	}
	return after, true, nil
}

type binaryIdentity struct {
	path string
	info os.FileInfo
}

func (manager Manager) resolve(name string) (binaryIdentity, error) {
	path, err := manager.Lookup(name)
	if err != nil {
		return binaryIdentity{}, ErrNotInstalled
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return binaryIdentity{}, fmt.Errorf("resolve executable: %w", err)
	}
	path, err = manager.Eval(path)
	if err != nil {
		return binaryIdentity{}, fmt.Errorf("resolve executable links: %w", err)
	}
	path = filepath.Clean(path)
	if strings.ContainsAny(path, "\x00\r\n"+string(os.PathListSeparator)) {
		return binaryIdentity{}, errors.New("executable path contains an unsafe character")
	}
	info, err := manager.Stat(path)
	if err != nil {
		return binaryIdentity{}, ErrNotInstalled
	}
	if err := protectedFile(info, manager.CurrentUID()); err != nil {
		return binaryIdentity{}, err
	}
	for directory := filepath.Dir(path); ; directory = filepath.Dir(directory) {
		info, err := manager.Stat(directory)
		if err != nil {
			return binaryIdentity{}, fmt.Errorf("inspect executable directory: %w", err)
		}
		// Homebrew intentionally keeps parts of its current-user prefix group
		// writable. Reject world-writable lookup chains, but bind the leaf itself
		// more tightly (regular, root/current-user owned, and neither group nor
		// world writable) and revalidate that inode around every command.
		if !info.IsDir() || info.Mode().Perm()&0o002 != 0 {
			return binaryIdentity{}, errors.New("executable directory is not protected")
		}
		owner, ok := fileOwner(info)
		if !ok || owner != 0 && owner != manager.CurrentUID() {
			return binaryIdentity{}, errors.New("executable directory has an unexpected owner")
		}
		if directory == filepath.Dir(directory) {
			break
		}
	}
	return binaryIdentity{path: path, info: info}, nil
}

func protectedFile(info os.FileInfo, uid uint32) error {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("executable is not a protected regular file")
	}
	owner, ok := fileOwner(info)
	if !ok || owner != 0 && owner != uid {
		return errors.New("executable has an unexpected owner")
	}
	return nil
}

func fileOwner(info os.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Uid, true
}

func (manager Manager) validate(binary binaryIdentity) error {
	info, err := manager.Stat(binary.path)
	if err != nil || binary.info == nil || !os.SameFile(binary.info, info) || info.Size() != binary.info.Size() || info.Mode() != binary.info.Mode() || !info.ModTime().Equal(binary.info.ModTime()) {
		return ErrBinaryChanged
	}
	return protectedFile(info, manager.CurrentUID())
}

func (manager Manager) environment(executable string) ([]string, error) {
	home, err := manager.Home()
	if err != nil || !filepath.IsAbs(home) || filepath.Clean(home) != home {
		return nil, errors.New("home directory is unavailable")
	}
	canonicalHome, err := manager.Eval(home)
	if err != nil || canonicalHome != home {
		return nil, errors.New("home directory is not canonical")
	}
	info, err := manager.Stat(home)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("home directory is unsafe")
	}
	owner, ok := fileOwner(info)
	if !ok || owner != manager.CurrentUID() {
		return nil, errors.New("home directory has an unexpected owner")
	}
	path := strings.Join(uniqueStrings([]string{filepath.Dir(executable), "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"}), string(os.PathListSeparator))
	environment := []string{"HOME=" + home, "PATH=" + path, "LC_ALL=C", "LANG=C"}
	for _, key := range []string{"USER", "LOGNAME", "TERM"} {
		if value := manager.Getenv(key); safeEnvironmentValue(value) {
			environment = append(environment, key+"="+value)
		}
	}
	return environment, nil
}

func safeEnvironmentValue(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("._-+/", character) {
			continue
		}
		return false
	}
	return true
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func safeVersion(output []byte) (string, error) {
	value := redact.String(string(output))
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" {
			continue
		}
		if len(line) > maximumVersion {
			return "", errors.New("version exceeds limit")
		}
		for _, character := range line {
			if character < 0x20 || character == 0x7f {
				return "", errors.New("version contains control characters")
			}
		}
		return line, nil
	}
	return "", errors.New("version is empty")
}

func interpretStatus(mode statusMode, result ProbeResult) (bool, error) {
	if mode == statusExitCode {
		switch result.ExitCode {
		case 0:
			return true, nil
		case 1:
			return false, nil
		default:
			return false, errors.New("status command returned an unsupported exit code")
		}
	}
	value := strings.ToLower(stripANSI(redact.String(string(result.Output))))
	negative := strings.Contains(value, "not logged in") || strings.Contains(value, "not authenticated") || strings.Contains(value, "unauthenticated")
	positive := strings.Contains(value, "logged in") || strings.Contains(value, "authenticated")
	if negative {
		return false, nil
	}
	if result.ExitCode == 0 && positive {
		return true, nil
	}
	return false, errors.New("cursor status did not contain an explicit authentication state")
}

func stripANSI(value string) string {
	var result strings.Builder
	result.Grow(len(value))
	for index := 0; index < len(value); {
		if value[index] != 0x1b || index+1 >= len(value) || value[index+1] != '[' {
			result.WriteByte(value[index])
			index++
			continue
		}
		index += 2
		for index < len(value) {
			character := value[index]
			index++
			if character >= 0x40 && character <= 0x7e {
				break
			}
		}
	}
	return result.String()
}

func find(provider Provider) (definition, bool) {
	for _, item := range definitions {
		if item.Provider == provider {
			return item, true
		}
	}
	return definition{}, false
}
