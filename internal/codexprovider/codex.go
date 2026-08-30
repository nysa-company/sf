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
	"os"
	"path/filepath"
	"regexp"
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

// Qualify records a guarded-only verdict after identity, version, binary
// digest, supported exec flags, authentication availability, and the pinned
// hostile-fixture digest have all been observed. Autonomous eligibility is
// intentionally unavailable until the separately approved native-profile
// proof exists.
func Qualify(ctx context.Context, database *store.Store, channel domain.Channel, adapter *Adapter, fixture QualificationFixture) (store.ProviderQualification, error) {
	if database == nil || !channel.Valid() || adapter == nil || fixture == nil || fixture.Digest() != fixtureDigest() {
		return store.ProviderQualification{}, ErrUnsafeConfiguration
	}
	binding, err := adapter.Binding(ctx)
	if err != nil {
		return store.ProviderQualification{}, err
	}
	failed, fixtureErr := fixture.Run(ctx, adapter)
	if fixtureErr != nil {
		failed = append(failed, "fixture_execution")
	}
	failed = normalizeProbes(failed)
	profile, reason := store.QualificationGuarded, ""
	if len(failed) != 0 {
		profile, reason = store.QualificationDisabled, "hostile_fixture_failed"
	}
	runID, err := qualificationRunID()
	if err != nil {
		return store.ProviderQualification{}, err
	}
	qualification, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{Channel: channel, RunID: runID, Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, Profile: profile, FailedProbes: failed, ReasonCode: reason, CreatedAt: time.Now().UTC()})
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
	result, err := a.runner.Probe(ctx, a.executable, []string{"login", "status"}, a.authEnvironment(), 0)
	if err != nil || result.ExitCode != 0 {
		return domain.ProviderIdentity{}, ErrUnauthenticated
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
	return contracts.RuntimeBinding{
		Identity:      identity,
		BinaryDigest:  binary,
		PolicyDigest:  policyDigest(),
		FixtureDigest: fixtureDigest(),
		AuthDigest:    authDigest(identity, a.authHome),
	}, nil
}

func (a *Adapter) Invocation(_ context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	if a == nil || input.Provider.Provider != "codex" || input.Provider.Model != a.model || input.Provider.Family != a.family || input.Provider.Version == "" {
		return contracts.Invocation{}, errors.New("Codex provider identity does not match the runtime binding")
	}
	if input.Worktree == "" || !filepath.IsAbs(input.Worktree) || filepath.Clean(input.Worktree) != input.Worktree || input.Worktree == "/" {
		return contracts.Invocation{}, errors.New("Codex worktree must be an absolute clean path")
	}
	if len(input.Prompt) == 0 || len(input.Prompt) > 64<<10 || strings.ContainsRune(input.Prompt, '\x00') || len(input.Schema) == 0 || len(input.Schema) > 1<<20 || !json.Valid(input.Schema) {
		return contracts.Invocation{}, errors.New("Codex phase input is invalid")
	}
	sandbox, ok := sandboxForPhase(input.Phase)
	if !ok {
		return contracts.Invocation{}, errors.New("Codex phase has no supported sandbox")
	}
	// `-` is the documented exec stdin prompt marker. The argv is fixed except
	// for supervisor-owned private output paths and the authenticated worktree;
	// untrusted ticket text never becomes an argv element. The permissions
	// profile is deliberately code-owned, not project/user configuration.
	return contracts.Invocation{
		Argv:               codexArgv(a.executable, a.model, input.Worktree, sandbox),
		Stdin:              []byte(input.Prompt),
		OutputSchema:       append([]byte(nil), input.Schema...),
		CaptureLastMessage: true,
		AuthHome:           a.authHome,
	}, nil
}

func codexArgv(executable, model, worktree, access string) []string {
	return []string{executable, "exec", "--ephemeral", "--json", "--ignore-user-config", "--ignore-rules", "--profile", "sf-guarded", "--config", `permissions.sf-guarded.extends=":` + access + `"`, "--config", `permissions.sf-guarded.filesystem={":root"="deny",":minimal"="read",":workspace_roots"="` + access + `"}`, "--config", `permissions.sf-guarded.network.enabled=false`, "--model", model, "-C", worktree, "--output-schema", contracts.OutputSchemaPlaceholder, "--output-last-message", contracts.OutputLastMessagePlaceholder, "-"}
}

func sandboxForPhase(phase domain.Phase) (string, bool) {
	switch phase {
	case domain.PhasePlanning, domain.PhaseReview:
		return "read-only", true
	case domain.PhaseVerification, domain.PhaseBuild:
		return "workspace", true
	default:
		return "", false
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
	transcript, usage, usageTrusted, err := parseJSONL(result.Stdout, result.Stderr)
	if err != nil {
		return contracts.PhaseResult{}, err
	}
	artifact := bytes.TrimSpace(result.OutputLastMessage)
	if len(artifact) == 0 || !json.Valid(artifact) || len(artifact) > 1<<20 {
		return contracts.PhaseResult{}, ErrNoFinalArtifact
	}
	// Codex reports token counts, not an authoritative monetary charge. Keep
	// them observable but leave the micro-USD charge untrusted until a
	// snapshotted pricing/reservation policy exists.
	return contracts.PhaseResult{Outcome: "completed", Artifact: artifact, Transcript: transcript, Provider: input.Provider, TokenUsageTrusted: usageTrusted, TokenUsage: usage}, nil
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
	for _, required := range []string{"--json", "--output-schema", "--output-last-message", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--profile", "--config", "--model", "-C"} {
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
	sum := sha256.Sum256([]byte("PATH=/usr/bin:/bin\x00LANG=C\x00HOME=<private>\x00TMPDIR=<private>"))
	return hex.EncodeToString(sum[:])
}
func fixtureDigest() string {
	sum := sha256.Sum256([]byte("codex-qualification-fixture/v1\x00hostile-fixture-required\x00no-live-network-in-tests"))
	return hex.EncodeToString(sum[:])
}
func authDigest(identity domain.ProviderIdentity, authHome string) string {
	// This is only a non-secret freshness binding. It intentionally hashes no
	// credential bytes and never exposes the auth-home path.
	sum := sha256.Sum256([]byte("codex-auth/v1\x00" + identity.Provider + "\x00" + identity.Version + "\x00" + authHome))
	return hex.EncodeToString(sum[:])
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

func normalizeProbes(values []string) []string {
	set := map[string]struct{}{}
	for _, value := range values {
		if len(value) == 0 || len(value) > 64 {
			continue
		}
		valid := value[0] >= 'a' && value[0] <= 'z'
		for _, character := range value[1:] {
			valid = valid && (character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_')
		}
		if valid {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func parseJSONL(stdout, stderr []byte) (string, int64, bool, error) {
	if len(stdout) == 0 || len(stdout) > maxJSONL {
		return "", 0, false, ErrOutputTooLarge
	}
	lines := bytes.Split(bytes.TrimSpace(stdout), []byte{'\n'})
	if len(lines) == 0 || len(lines) > maxEvents {
		return "", 0, false, ErrMalformedJSONL
	}
	var usage int64
	usageTrusted := false
	for _, line := range lines {
		if len(line) == 0 || len(line) > maxJSONL {
			return "", 0, false, ErrMalformedJSONL
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
			return "", 0, false, ErrMalformedJSONL
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			return "", 0, false, ErrMalformedJSONL
		}
		if event.Type == "error" {
			return "", 0, false, errors.New("codex returned a structured error")
		}
		if event.Type == "turn.completed" && len(event.Usage) != 0 {
			units, valid := parseUsage(event.Usage)
			if !valid || usageTrusted {
				return "", 0, false, ErrMalformedJSONL
			}
			usage, usageTrusted = units, true
		}
	}
	transcript := redact.String(string(stdout) + "\n" + string(stderr))
	if len(transcript) > maxJSONL {
		transcript = transcript[:maxJSONL]
	}
	return transcript, usage, usageTrusted, nil
}

func parseUsage(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 || len(raw) > 4096 {
		return 0, false
	}
	var value struct {
		InputTokens  json.Number `json:"input_tokens"`
		OutputTokens json.Number `json:"output_tokens"`
		TotalTokens  json.Number `json:"total_tokens"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return 0, false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return 0, false
	}
	parse := func(number json.Number) (int64, bool) {
		if number == "" {
			return 0, true
		}
		result, err := number.Int64()
		return result, err == nil && result >= 0
	}
	input, okInput := parse(value.InputTokens)
	output, okOutput := parse(value.OutputTokens)
	total, okTotal := parse(value.TotalTokens)
	if !okInput || !okOutput || !okTotal || input > 1<<50 || output > 1<<50 || total > 1<<50 {
		return 0, false
	}
	if value.TotalTokens != "" {
		if value.InputTokens != "" && value.OutputTokens != "" && total != input+output {
			return 0, false
		}
		return total, total > 0
	}
	if value.InputTokens == "" || value.OutputTokens == "" || input+output <= 0 {
		return 0, false
	}
	return input + output, true
}
