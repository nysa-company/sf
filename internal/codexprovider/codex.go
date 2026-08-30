// Package codexprovider implements the narrow, local Codex CLI adapter.
//
// It deliberately has no Git, GitHub, SQLite, or os/exec authority. Process
// creation remains in processsupervisor; this package only probes an explicit
// executable through an injected bounded runner and proposes argv/stdin.
package codexprovider

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
)

const (
	maxProbeOutput = 16 << 10
	maxJSONL       = 64 << 10
	maxEvents      = 256
)

var (
	ErrUnavailable         = errors.New("codex executable is unavailable")
	ErrUnauthenticated     = errors.New("codex authentication is unavailable")
	ErrUnsupportedAuthMode = errors.New("codex authentication mode is not the supported ChatGPT subscription")
	ErrCapability          = errors.New("codex executable lacks required exec capabilities")
	ErrMalformedJSONL      = errors.New("codex JSONL output is malformed")
	ErrNoFinalArtifact     = errors.New("codex did not return a final structured artifact")
	ErrOutputTooLarge      = errors.New("codex output exceeded the bounded contract")
	ErrUnsafeConfiguration = errors.New("codex adapter configuration is unsafe")
)

var versionPattern = regexp.MustCompile(`(?i)\b(?:codex(?:[- ]cli)?\s+)?v?(\d+\.\d+(?:\.\d+)?(?:[-+][A-Za-z0-9.-]+)?)\b`)

// ProbeRunner is deliberately smaller than os/exec. The production adapter
// uses auth.OSRunner, which creates a bounded, scrubbed probe process.
type ProbeRunner interface {
	Probe(context.Context, string, []string, []string, int) (auth.ProbeResult, error)
}

type Config struct {
	// Route is the local routing key. It is distinct from the durable
	// underlying provider identity (always "codex"), allowing separately
	// configured Codex model/family profiles without pretending they are
	// independent executables.
	Route      string
	Executable string
	// AuthHome is the existing Codex credential home. It is never opened,
	// copied, logged, or persisted by sf; processsupervisor only validates that
	// the directory is non-symlinked, owner/root-owned, and not group/world
	// writable before passing it as CODEX_HOME for Codex.
	AuthHome string
	Model    string
	Runner   ProbeRunner
}

type Adapter struct {
	route      string
	executable string
	authHome   string
	model      string
	family     string
	runner     ProbeRunner
}

// QualificationFixture is the hermetic hostile-fixture boundary. Production
// qualification may supply a disposable trusted fixture runner; ordinary
// tests inject a fake and never invoke a model or network. Probe names are
// persisted only as bounded reason codes, never as raw provider output.
type QualificationFixture interface {
	Digest() string
	Run(context.Context, *Adapter) ([]string, error)
}

// sealedQualificationFixture is intentionally unimplementable outside this
// package: the unexported method prevents an adapter (or a project config) from
// manufacturing a passing qualification fixture merely by copying its public
// digest.  Production qualification uses LocalQualificationFixture; tests may
// use an in-package deterministic fixture.
type sealedQualificationFixture interface {
	QualificationFixture
	qualificationFixtureSeal()
}

// QualificationAttestor is implemented only by the daemon's live process
// supervisor.  It makes a passing result unforgeable by an adapter or CLI
// helper: Store verifies its signature against the current daemon key.
type QualificationAttestor interface {
	AttestQualification(contracts.QualificationAttestation) (contracts.QualificationAttestation, error)
}

// LocalQualificationFixture runs bounded, credential-free local probes. It
// includes Codex's own configuration parser but never supplies an exec prompt,
// so qualification cannot spend model tokens.
func LocalQualificationFixture() QualificationFixture { return localQualificationFixture{} }

type localQualificationFixture struct{}

func (localQualificationFixture) Digest() string            { return fixtureDigest() }
func (localQualificationFixture) qualificationFixtureSeal() {}

func (localQualificationFixture) Run(ctx context.Context, adapter *Adapter) ([]string, error) {
	if adapter == nil || adapter.runner == nil {
		return []string{"configuration"}, ErrUnsafeConfiguration
	}
	// The adapter supplied to this fixture has an independently-enforced outer
	// Seatbelt runner. The inner `codex sandbox` command proves the exact Codex
	// profile too; neither command receives CODEX_HOME or prompt stdin.
	probe := func(name string, args []string, env []string, expect func(auth.ProbeResult) bool) error {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		result, err := adapter.runner.Probe(probeCtx, adapter.executable, args, env, maxProbeOutput)
		if err != nil || !expect(result) {
			return fmt.Errorf("qualification probe %s failed", name)
		}
		return nil
	}
	// Verify the current CLI accepts the guarded `exec` configuration without
	// running a model. This closes the historical false-green where `--profile`
	// selected a config file instead of the custom permission profile.
	if err := probe("exec_config_parse", append([]string{"exec", "--help"}, guardedConfig("read-only", "read")...), probeEnvironment(), func(result auth.ProbeResult) bool { return result.ExitCode == 0 }); err != nil {
		return []string{"exec_config_parse"}, nil
	}
	// `codex sandbox` is a local command runner, not `codex exec`; no model is
	// selected and stdin is not supplied. Read-only and workspace-write are
	// measured independently so a profile accidentally widening access fails.
	readOnly := append(append([]string{"sandbox", "--permission-profile", "sf-guarded"}, guardedConfig("read-only", "read")...), "--", "/bin/sh", "-c", "test -r /etc/hosts && ! test -w /etc/hosts")
	if err := probe("sandbox_read", readOnly, probeEnvironment(), func(result auth.ProbeResult) bool { return result.ExitCode == 0 }); err != nil {
		return []string{"sandbox_read"}, nil
	}
	writeDenied := append(append([]string{"sandbox", "--permission-profile", "sf-guarded"}, guardedConfig("read-only", "read")...), "--", "/bin/sh", "-c", "test -w /etc/hosts")
	if err := probe("sandbox_write_denied", writeDenied, probeEnvironment(), func(result auth.ProbeResult) bool { return result.ExitCode != 0 }); err != nil {
		return []string{"sandbox_write_denied"}, nil
	}
	workspace := append(append([]string{"sandbox", "--permission-profile", "sf-guarded"}, guardedConfig("workspace", "write")...), "--", "/bin/sh", "-c", "test -w .")
	if err := probe("sandbox_workspace_write", workspace, probeEnvironment(), func(result auth.ProbeResult) bool { return result.ExitCode == 0 }); err != nil {
		return []string{"sandbox_workspace_write"}, nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return []string{"network_loopback"}, nil
	}
	defer listener.Close()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			_, _ = connection.Write([]byte("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"))
			_ = connection.Close()
		}
	}()
	loopback := append(append([]string{"sandbox", "--permission-profile", "sf-guarded"}, guardedConfig("read-only", "read")...), "--", "/usr/bin/curl", "-fsS", "--connect-timeout", "1", "http://"+listener.Addr().String())
	if err := probe("network_loopback", loopback, probeEnvironment(), func(result auth.ProbeResult) bool { return result.ExitCode != 0 }); err != nil {
		return []string{"network_loopback"}, nil
	}
	external := append(append([]string{"sandbox", "--permission-profile", "sf-guarded"}, guardedConfig("read-only", "read")...), "--", "/usr/bin/curl", "-fsS", "--connect-timeout", "1", "http://198.51.100.1:9")
	if err := probe("network_external", external, probeEnvironment(), func(result auth.ProbeResult) bool { return result.ExitCode != 0 }); err != nil {
		return []string{"network_external"}, nil
	}
	credential := append(append([]string{"sandbox", "--permission-profile", "sf-guarded"}, guardedConfig("read-only", "read")...), "--", "/bin/sh", "-c", `test -r "$CODEX_HOME/auth.json"`)
	if err := probe("credential_isolation", credential, append(probeEnvironment(), "CODEX_HOME="+adapter.authHome), func(result auth.ProbeResult) bool { return result.ExitCode != 0 }); err != nil {
		return []string{"credential_isolation"}, nil
	}
	return nil, nil
}

// Qualify records a guarded-only verdict after identity, version, binary
// digest, supported exec flags, authentication availability, and the pinned
// hostile-fixture digest have all been observed. Autonomous eligibility is
// intentionally unavailable until the separately approved native-profile
// proof exists.
func Qualify(ctx context.Context, database *store.Store, channel domain.Channel, adapter *Adapter, fixture QualificationFixture, attestor QualificationAttestor) (store.ProviderQualification, error) {
	if database == nil || !channel.Valid() || adapter == nil || fixture == nil || attestor == nil || fixture.Digest() != fixtureDigest() {
		return store.ProviderQualification{}, ErrUnsafeConfiguration
	}
	if _, sealed := fixture.(sealedQualificationFixture); !sealed {
		return store.ProviderQualification{}, ErrUnsafeConfiguration
	}
	binding, err := adapter.qualificationBinding(ctx)
	if err != nil {
		return store.ProviderQualification{}, err
	}
	failed, fixtureErr := fixture.Run(ctx, adapter)
	if fixtureErr != nil {
		failed = append(failed, "fixture_execution")
	}
	failed, err = normalizeProbes(failed)
	if err != nil {
		return store.ProviderQualification{}, err
	}
	profile, reason := store.QualificationGuarded, ""
	if len(failed) != 0 {
		profile, reason = store.QualificationDisabled, "hostile_fixture_"+failed[0]
	}
	runID, err := qualificationRunID()
	if err != nil {
		return store.ProviderQualification{}, err
	}
	created := time.Now().UTC()
	value := store.ProviderQualification{Channel: channel, RunID: runID, Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, Profile: profile, FailedProbes: failed, ReasonCode: reason, CreatedAt: created}
	if profile == store.QualificationDisabled {
		// A failed fixture is useful audit evidence but must not carry an
		// admission attestation or auth-mode claim into a disabled record.
		value.AuthDigest, value.AuthMode = "", ""
		qualification, _, recordErr := database.RecordProviderQualification(ctx, value)
		return qualification, recordErr
	}
	leader, leaderErr := database.LeaderEpoch(ctx, channel)
	if leaderErr != nil {
		return store.ProviderQualification{}, leaderErr
	}
	probeDigest := qualificationProbeDigest(binding, failed)
	attestation, attestErr := attestor.AttestQualification(contracts.QualificationAttestation{Channel: channel, RunID: runID, Identity: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: probeDigest, Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: runID})
	if attestErr != nil {
		return store.ProviderQualification{}, attestErr
	}
	value.ProbeDigest = probeDigest
	qualification, _, err := database.RecordAttestedProviderQualification(ctx, value, attestation)
	return qualification, err
}

func New(config Config) (*Adapter, error) {
	path, err := resolveExecutable(config.Executable)
	if err != nil {
		return nil, err
	}
	authHome, err := resolveAuthHome(config.AuthHome)
	if err != nil {
		return nil, err
	}
	route := config.Route
	if route == "" {
		route = "codex"
	}
	model := config.Model
	if model == "" {
		model = "codex"
	}
	family, ok := familyForModel(model)
	if !ok || !safeName(route) {
		return nil, ErrUnsafeConfiguration
	}
	runner := config.Runner
	if runner == nil {
		runner = auth.OSRunner{}
	}
	return &Adapter{route: route, executable: path, authHome: authHome, model: model, family: family, runner: runner}, nil
}

// familyForModel is deliberately closed rather than configuration supplied.
// A route alias cannot claim independence from another route using the same
// inference family. Additions require an explicit code and qualification
// review with the provider's model-family evidence.
func familyForModel(model string) (string, bool) {
	switch model {
	case "gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol":
		return "openai-gpt-5.6", true
	case "gpt-5.5":
		return "openai-gpt-5.5", true
	default:
		return "", false
	}
}

func (a *Adapter) Name() string { return a.route }

func (a *Adapter) Probe(ctx context.Context) (domain.ProviderIdentity, error) {
	if a == nil || a.runner == nil {
		return domain.ProviderIdentity{}, ErrUnavailable
	}
	version, err := a.version(ctx)
	if err != nil {
		return domain.ProviderIdentity{}, err
	}
	if err := a.capabilities(ctx); err != nil {
		return domain.ProviderIdentity{}, err
	}
	if _, err := a.authMode(ctx); err != nil {
		return domain.ProviderIdentity{}, err
	}
	return domain.ProviderIdentity{Provider: "codex", Model: a.model, Family: a.family, Version: version}, nil
}

func (a *Adapter) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	identity, err := a.Probe(ctx)
	if err != nil {
		return contracts.RuntimeBinding{}, err
	}
	binary, err := digestFile(a.executable)
	if err != nil {
		return contracts.RuntimeBinding{}, err
	}
	auth, err := authDigest(a.authHome)
	if err != nil {
		return contracts.RuntimeBinding{}, err
	}
	return contracts.RuntimeBinding{
		Identity:      identity,
		BinaryDigest:  binary,
		PolicyDigest:  policyDigest(),
		FixtureDigest: fixtureDigest(),
		AuthDigest:    auth,
		AuthMode:      authModeChatGPTSubscription,
	}, nil
}

func (a *Adapter) qualificationBinding(ctx context.Context) (contracts.RuntimeBinding, error) {
	return a.Binding(ctx)
}

func (a *Adapter) Invocation(_ context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	if a == nil || input.Provider.Provider != "codex" || input.Provider.Model != a.model || input.Provider.Family != a.family || input.Provider.Version == "" || input.AuthMode != authModeChatGPTSubscription {
		return contracts.Invocation{}, errors.New("Codex provider identity does not match the runtime binding")
	}
	if input.Worktree == "" || !filepath.IsAbs(input.Worktree) || filepath.Clean(input.Worktree) != input.Worktree || input.Worktree == "/" {
		return contracts.Invocation{}, errors.New("Codex worktree must be an absolute clean path")
	}
	if len(input.Prompt) == 0 || len(input.Prompt) > 64<<10 || strings.ContainsRune(input.Prompt, '\x00') || len(input.Schema) == 0 || len(input.Schema) > 1<<20 || !json.Valid(input.Schema) {
		return contracts.Invocation{}, errors.New("Codex phase input is invalid")
	}
	parent, workspaceAccess, ok := permissionProfileForPhase(input.Phase)
	if !ok {
		return contracts.Invocation{}, errors.New("Codex phase has no supported sandbox")
	}
	// `-` is the documented exec stdin prompt marker. The argv is fixed except
	// for supervisor-owned private output paths and the authenticated worktree;
	// untrusted ticket text never becomes an argv element. The permissions
	// profile is deliberately code-owned, not project/user configuration.
	return contracts.Invocation{
		Argv:               codexArgv(a.executable, a.model, input.Worktree, parent, workspaceAccess),
		Stdin:              []byte(input.Prompt),
		OutputSchema:       append([]byte(nil), input.Schema...),
		CaptureLastMessage: true,
		AuthHome:           a.authHome,
	}, nil
}

func codexArgv(executable, model, worktree, parent, workspaceAccess string) []string {
	argv := []string{executable, "exec", "--ephemeral", "--json", "--ignore-user-config", "--ignore-rules"}
	argv = append(argv, guardedConfig(parent, workspaceAccess)...)
	return append(argv, "--model", model, "-C", worktree, "--output-schema", contracts.OutputSchemaPlaceholder, "--output-last-message", contracts.OutputLastMessagePlaceholder, "-")
}

func guardedConfig(parent, workspaceAccess string) []string {
	return []string{"--config", `default_permissions="sf-guarded"`, "--config", `permissions.sf-guarded.extends=":` + parent + `"`, "--config", `permissions.sf-guarded.filesystem={":root"="deny",":minimal"="read",":workspace_roots"="` + workspaceAccess + `"}`, "--config", `permissions.sf-guarded.network.enabled=false`}
}

func permissionProfileForPhase(phase domain.Phase) (string, string, bool) {
	switch phase {
	case domain.PhasePlanning, domain.PhaseReview:
		return "read-only", "read", true
	case domain.PhaseVerification, domain.PhaseBuild:
		return "workspace", "write", true
	default:
		return "", "", false
	}
}

func (a *Adapter) Parse(ctx context.Context, input contracts.PhaseInput, result contracts.CommandResult) (contracts.PhaseResult, error) {
	if err := ctx.Err(); err != nil {
		return contracts.PhaseResult{}, err
	}
	if result.StdoutTruncated || result.StderrTruncated || result.OutputLastMessageTruncated || len(result.Stdout) > maxJSONL || len(result.Stderr) > maxJSONL || len(result.OutputLastMessage) > 1<<20 {
		return contracts.PhaseResult{}, ErrOutputTooLarge
	}
	if result.ExitCode != 0 {
		return contracts.PhaseResult{}, fmt.Errorf("codex exec exited %d", result.ExitCode)
	}
	transcript, usage, usageTrusted, usageDetail, err := parseJSONL(result.Stdout, result.Stderr)
	if err != nil {
		return contracts.PhaseResult{}, err
	}
	artifact := bytes.TrimSpace(result.OutputLastMessage)
	if len(artifact) == 0 || !json.Valid(artifact) || len(artifact) > 1<<20 {
		return contracts.PhaseResult{}, ErrNoFinalArtifact
	}
	if input.AuthMode != authModeChatGPTSubscription {
		return contracts.PhaseResult{}, ErrUnsupportedAuthMode
	}
	// This route has been admitted only for the exact ChatGPT subscription
	// status. Its incremental API charge is therefore known to be zero; token
	// counters remain observability only and never become monetary usage.
	return contracts.PhaseResult{Outcome: "completed", Artifact: artifact, Transcript: transcript, Provider: input.Provider, UsageTrusted: true, UsageUnits: 0, TokenUsageTrusted: usageTrusted, TokenUsage: usage, TokenInputTokens: usageDetail.input, TokenCachedTokens: usageDetail.cached, TokenOutputTokens: usageDetail.output, TokenReasoningTokens: usageDetail.reasoning}, nil
}

const authModeChatGPTSubscription = "chatgpt_subscription"

func (a *Adapter) authMode(ctx context.Context) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := a.runner.Probe(probeCtx, a.executable, []string{"login", "status"}, a.authEnvironment(), maxProbeOutput)
	if err != nil || result.ExitCode != 0 || len(result.Output) == 0 || len(result.Output) > maxProbeOutput {
		return "", ErrUnauthenticated
	}
	if strings.TrimSpace(string(result.Output)) != "Logged in using ChatGPT" {
		return "", ErrUnsupportedAuthMode
	}
	return authModeChatGPTSubscription, nil
}

func (a *Adapter) version(ctx context.Context) (string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := a.runner.Probe(probeCtx, a.executable, []string{"--version"}, probeEnvironment(), maxProbeOutput)
	if err != nil || result.ExitCode != 0 || len(result.Output) == 0 || len(result.Output) > maxProbeOutput {
		return "", ErrUnavailable
	}
	match := versionPattern.FindStringSubmatch(string(result.Output))
	if len(match) != 2 || len(match[1]) > 100 {
		return "", ErrUnavailable
	}
	return match[1], nil
}

func (a *Adapter) capabilities(ctx context.Context) error {
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := a.runner.Probe(probeCtx, a.executable, []string{"exec", "--help"}, probeEnvironment(), maxProbeOutput)
	if err != nil || result.ExitCode != 0 || len(result.Output) == 0 || len(result.Output) > maxProbeOutput {
		return ErrCapability
	}
	output := string(result.Output)
	for _, required := range []string{"--json", "--output-schema", "--output-last-message", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--config", "--model", "-C"} {
		if !strings.Contains(output, required) {
			return ErrCapability
		}
	}
	return nil
}

func (a *Adapter) authEnvironment() []string {
	return append(probeEnvironment(), "CODEX_HOME="+a.authHome)
}

func probeEnvironment() []string { return []string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=/tmp"} }

func resolveExecutable(value string) (string, error) {
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return "", ErrUnavailable
	}
	resolved, err := filepath.EvalSymlinks(value)
	if err != nil || !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved {
		return "", ErrUnavailable
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 || info.Mode()&0o022 != 0 {
		return "", ErrUnavailable
	}
	return resolved, nil
}

func resolveAuthHome(value string) (string, error) {
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return "", ErrUnsafeConfiguration
	}
	resolved, err := filepath.EvalSymlinks(value)
	if err != nil || resolved != value {
		return "", ErrUnsafeConfiguration
	}
	info, err := os.Lstat(value)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || !trustedAuthHomeOwner(info) {
		return "", ErrUnsafeConfiguration
	}
	return value, nil
}

func trustedAuthHomeOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return stat.Uid == uint32(os.Getuid()) || stat.Uid == 0
}

func digestFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func policyDigest() string {
	// Keep this equal to the supervisor-owned environment contract. The Codex
	// command shape is independently pinned by fixtureDigest and capability
	// probing; this digest only names the launch environment that the
	// supervisor itself enforces.
	sum := sha256.Sum256([]byte("PATH=/usr/bin:/bin\x00LANG=C\x00LC_ALL=C\x00HOME=<private>\x00TMPDIR=<private>\x00CODEX_HOME=<private>"))
	return hex.EncodeToString(sum[:])
}
func fixtureDigest() string {
	sum := sha256.Sum256([]byte("codex-qualification-fixture/v1\x00hostile-fixture-required\x00no-live-network-in-tests"))
	return hex.EncodeToString(sum[:])
}
func authDigest(authHome string) (string, error) {
	// Bind non-secret filesystem identity and metadata, never credential
	// content. A replacement, rotation, or symlink race therefore invalidates
	// the binding while no token byte reaches memory outside Codex itself.
	file, err := openPrivateAuthFile(authHome)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || !trustedAuthHomeOwner(info) {
		return "", ErrUnsafeConfiguration
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("codex-auth/v2\x00%d\x00%d\x00%d\x00%d", stat.Dev, stat.Ino, info.Size(), info.ModTime().UnixNano())))
	return hex.EncodeToString(sum[:]), nil
}

func openPrivateAuthFile(home string) (*os.File, error) {
	if _, err := resolveAuthHome(home); err != nil {
		return nil, err
	}
	path := filepath.Join(home, "auth.json")
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() || before.Mode().Perm()&0o077 != 0 || !trustedAuthHomeOwner(before) {
		return nil, ErrUnauthenticated
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrUnauthenticated
	}
	after, statErr := file.Stat()
	if statErr != nil || !sameFileIdentity(before, after) {
		_ = file.Close()
		return nil, ErrUnsafeConfiguration
	}
	return file, nil
}

func sameFileIdentity(left, right os.FileInfo) bool {
	leftStat, leftOK := left.Sys().(*syscall.Stat_t)
	rightStat, rightOK := right.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && leftStat.Dev == rightStat.Dev && leftStat.Ino == rightStat.Ino && left.Mode() == right.Mode() && left.Size() == right.Size()
}

func qualificationProbeDigest(binding contracts.RuntimeBinding, failed []string) string {
	values := append([]string(nil), failed...)
	sort.Strings(values)
	sum := sha256.Sum256([]byte("codex-qualification-probes/v3\x00" + binding.BinaryDigest + "\x00" + binding.PolicyDigest + "\x00" + binding.FixtureDigest + "\x00" + binding.AuthDigest + "\x00" + binding.AuthMode + "\x00" + strings.Join(values, "\x00")))
	return hex.EncodeToString(sum[:])
}

type outerQualificationRunner struct {
	base    ProbeRunner
	profile string
}

func newOuterQualificationRunner(base ProbeRunner, authHome string) (ProbeRunner, error) {
	if runtime.GOOS != "darwin" {
		return nil, ErrUnsafeConfiguration
	}
	sandbox, err := exec.LookPath("sandbox-exec")
	if err != nil || sandbox != "/usr/bin/sandbox-exec" {
		return nil, ErrUnsafeConfiguration
	}
	if _, err := resolveAuthHome(authHome); err != nil {
		return nil, err
	}
	return outerQualificationRunner{base: base, profile: qualificationSandboxProfile(authHome)}, nil
}

func (r outerQualificationRunner) Probe(ctx context.Context, executable string, arguments, environment []string, limit int) (auth.ProbeResult, error) {
	if r.base == nil || r.profile == "" {
		return auth.ProbeResult{}, ErrUnsafeConfiguration
	}
	// Codex itself must read its private auth metadata to answer its documented
	// local status command. Keep that one bounded, exact probe outside the
	// hostile-fixture Seatbelt; every fixture command remains wrapped and never
	// receives CODEX_HOME.
	if len(arguments) == 2 && arguments[0] == "login" && arguments[1] == "status" {
		return r.base.Probe(ctx, executable, arguments, environment, limit)
	}
	args := append([]string{"-p", r.profile, executable}, arguments...)
	return r.base.Probe(ctx, "/usr/bin/sandbox-exec", args, environment, limit)
}

func qualificationSandboxProfile(authHome string) string {
	escaped := strings.ReplaceAll(authHome, `"`, `\\"`)
	// Specific Seatbelt denials win over the broad host-read compatibility
	// allowance. The outer profile constrains Codex itself as well as the
	// command passed to `codex sandbox`.
	return `(version 1) (allow default) (deny network*) (deny file-write*) (deny file-read* (subpath "` + escaped + `"))`
}
func safeName(value string) bool {
	return value != "" && len(value) <= 100 && !strings.ContainsAny(value, "\x00\r\n\t /\\")
}

func qualificationRunID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func normalizeProbes(values []string) ([]string, error) {
	known := map[string]struct{}{
		"configuration": {}, "exec_config_parse": {}, "sandbox_read": {}, "sandbox_write_denied": {}, "sandbox_workspace_write": {}, "network_loopback": {}, "network_external": {}, "credential_isolation": {},
		"version": {}, "capabilities": {}, "authentication": {}, "fixture_execution": {},
		"binary": {}, "network": {}, "root_denied": {}, "auth_denied": {}, "argv": {}, "jsonl": {},
	}
	set := map[string]struct{}{}
	for _, value := range values {
		if len(value) == 0 || len(value) > 64 || !qualificationProbeName.MatchString(value) {
			return nil, ErrUnsafeConfiguration
		}
		if _, ok := known[value]; !ok {
			return nil, ErrUnsafeConfiguration
		}
		set[value] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result, nil
}

var qualificationProbeName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

type tokenUsage struct {
	input, cached, output, reasoning int64
}

func parseJSONL(stdout, stderr []byte) (string, int64, bool, tokenUsage, error) {
	if len(stdout) == 0 || len(stdout) > maxJSONL {
		return "", 0, false, tokenUsage{}, ErrOutputTooLarge
	}
	lines := bytes.Split(bytes.TrimSpace(stdout), []byte{'\n'})
	if len(lines) == 0 || len(lines) > maxEvents {
		return "", 0, false, tokenUsage{}, ErrMalformedJSONL
	}
	var usage int64
	var detail tokenUsage
	usageTrusted := false
	for _, line := range lines {
		if len(line) == 0 || len(line) > maxJSONL {
			return "", 0, false, tokenUsage{}, ErrMalformedJSONL
		}
		var event struct {
			Type string `json:"type"`
			Item struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"item"`
			Error json.RawMessage `json:"error"`
			Usage json.RawMessage `json:"usage"`
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		if err := decoder.Decode(&event); err != nil || event.Type == "" {
			return "", 0, false, tokenUsage{}, ErrMalformedJSONL
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return "", 0, false, tokenUsage{}, ErrMalformedJSONL
		}
		if event.Type == "error" {
			return "", 0, false, tokenUsage{}, errors.New("codex returned a structured error")
		}
		if event.Type == "turn.completed" && len(event.Usage) != 0 {
			units, input, cached, output, reasoning, valid := parseUsage(event.Usage)
			if !valid || usageTrusted {
				return "", 0, false, tokenUsage{}, ErrMalformedJSONL
			}
			usage, detail, usageTrusted = units, tokenUsage{input: input, cached: cached, output: output, reasoning: reasoning}, true
		}
	}
	transcript := redact.String(string(stdout) + "\n" + string(stderr))
	if len(transcript) > maxJSONL {
		transcript = transcript[:maxJSONL]
	}
	return transcript, usage, usageTrusted, detail, nil
}

func parseUsage(raw json.RawMessage) (int64, int64, int64, int64, int64, bool) {
	if len(raw) == 0 || len(raw) > 4096 {
		return 0, 0, 0, 0, 0, false
	}
	var value struct {
		InputTokens     json.Number `json:"input_tokens"`
		CachedTokens    json.Number `json:"cached_input_tokens"`
		OutputTokens    json.Number `json:"output_tokens"`
		ReasoningTokens json.Number `json:"reasoning_tokens"`
		TotalTokens     json.Number `json:"total_tokens"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return 0, 0, 0, 0, 0, false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return 0, 0, 0, 0, 0, false
	}
	parse := func(number json.Number) (int64, bool) {
		if number == "" {
			return 0, true
		}
		result, err := number.Int64()
		return result, err == nil && result >= 0
	}
	input, okInput := parse(value.InputTokens)
	cached, okCached := parse(value.CachedTokens)
	output, okOutput := parse(value.OutputTokens)
	reasoning, okReasoning := parse(value.ReasoningTokens)
	total, okTotal := parse(value.TotalTokens)
	if !okInput || !okCached || !okOutput || !okReasoning || !okTotal || input > 1<<50 || cached > 1<<50 || output > 1<<50 || reasoning > 1<<50 || total > 1<<50 {
		return 0, 0, 0, 0, 0, false
	}
	if value.TotalTokens != "" {
		if value.InputTokens != "" && value.OutputTokens != "" && total != input+cached+output {
			return 0, 0, 0, 0, 0, false
		}
		return total, input, cached, output, reasoning, total > 0
	}
	if value.InputTokens == "" || value.OutputTokens == "" || input+cached+output <= 0 {
		return 0, 0, 0, 0, 0, false
	}
	return input + cached + output, input, cached, output, reasoning, true
}
