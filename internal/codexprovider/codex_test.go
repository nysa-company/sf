package codexprovider

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

type fakeProbe struct {
	version, help []byte
	login         []byte
	loginExit     int
	err           error
	calls         [][]string
}

func (f *fakeProbe) Probe(ctx context.Context, _ string, arguments, _ []string, _ int) (auth.ProbeResult, error) {
	f.calls = append(f.calls, append([]string(nil), arguments...))
	if f.err != nil {
		return auth.ProbeResult{}, f.err
	}
	if err := ctx.Err(); err != nil {
		return auth.ProbeResult{}, err
	}
	switch strings.Join(arguments, " ") {
	case "--version":
		return auth.ProbeResult{Output: append([]byte(nil), f.version...)}, nil
	case "exec --help":
		return auth.ProbeResult{Output: append([]byte(nil), f.help...)}, nil
	case "login status":
		return auth.ProbeResult{ExitCode: f.loginExit, Output: append([]byte(nil), f.login...)}, nil
	default:
		return auth.ProbeResult{}, errors.New("unexpected probe")
	}
}

type fixture struct {
	failed []string
	err    error
}

func (f fixture) Digest() string            { return fixtureDigest() }
func (f fixture) qualificationFixtureSeal() {}
func (f fixture) Run(context.Context, *Adapter) ([]string, error) {
	return append([]string(nil), f.failed...), f.err
}

func TestCodexInvocationUsesBoundedStdinAndPinnedSchema(t *testing.T) {
	adapter, probe := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	identity, err := adapter.Probe(context.Background())
	if err != nil || identity.Provider != "codex" || identity.Model != "gpt-5.6-luna" || identity.Family != "openai-gpt-5.6" || identity.Version != "1.2.3" {
		t.Fatalf("identity=%+v err=%v", identity, err)
	}
	worktree := privateDir(t, "worktree")
	input := contracts.PhaseInput{Provider: identity, AuthMode: authModeChatGPTSubscription, Phase: domain.PhasePlanning, Worktree: worktree, Prompt: "untrusted ticket; $(not-shell)", Schema: []byte(`{"type":"object"}`)}
	invocation, err := adapter.Invocation(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	want := codexArgv(adapter.executable, "gpt-5.6-luna", worktree, "read-only", "read")
	if !reflect.DeepEqual(invocation.Argv, want) || string(invocation.Stdin) != input.Prompt || string(invocation.OutputSchema) != string(input.Schema) || invocation.AuthHome != adapter.authHome {
		t.Fatalf("invocation=%+v", invocation)
	}
	if got := strings.Join(probe.calls[1], " "); got != "exec --help" {
		t.Fatalf("capability calls=%v", probe.calls)
	}
	input.Phase = domain.PhaseBuild
	build, err := adapter.Invocation(context.Background(), input)
	if err != nil || !strings.Contains(strings.Join(build.Argv, " "), `extends=":workspace"`) || !strings.Contains(strings.Join(build.Argv, " "), `:workspace_roots"="write`) {
		t.Fatalf("build invocation=%+v err=%v", build, err)
	}
	input.Phase = domain.PhasePublish
	if _, err := adapter.Invocation(context.Background(), input); err == nil {
		t.Fatal("unsupported phase was accepted")
	}
}

// TestInstalledCodexExecGuardedConfigParsesWithoutModel exercises the real
// local CLI parser only. `exec --help` accepts no prompt/stdin and cannot make
// a model request; this catches config-key drift such as using --profile for a
// permission profile before a paid invocation is ever admitted.
func TestInstalledCodexExecGuardedConfigParsesWithoutModel(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("the trusted local v1 qualification target is macOS")
	}
	binary, err := exec.LookPath("codex")
	if err != nil {
		t.Skip("Codex CLI is not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	args := append([]string{"exec", "--help"}, guardedConfig("read-only", "read")...)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "HOME=/tmp"}
	output, runErr := cmd.CombinedOutput()
	if runErr != nil {
		if len(output) > 1024 {
			output = output[:1024]
		}
		t.Fatalf("guarded Codex exec configuration did not parse (no model call): %v: %s", runErr, output)
	}
}

func TestCodexParseRejectsMalformedOversizedAndNonzeroOutput(t *testing.T) {
	adapter, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	identity, _ := adapter.Probe(context.Background())
	input := contracts.PhaseInput{Provider: identity, AuthMode: authModeChatGPTSubscription}
	valid := []byte("{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"intermediate\"},\"future\":true}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"final is in output-last-message\"}}\n{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":12,\"cached_input_tokens\":3,\"output_tokens\":8,\"reasoning_tokens\":5,\"total_tokens\":23}}\n")
	result, err := adapter.Parse(context.Background(), input, contracts.CommandResult{ExitCode: 0, Stdout: valid, OutputLastMessage: []byte(`{"schema":"sf.builder/v1"}`), Stderr: []byte("progress password=keep-out")})
	if err != nil || string(result.Artifact) != `{"schema":"sf.builder/v1"}` || !result.UsageTrusted || result.UsageUnits != 0 || !result.TokenUsageTrusted || result.TokenUsage != 23 || result.TokenInputTokens != 12 || result.TokenCachedTokens != 3 || result.TokenOutputTokens != 8 || result.TokenReasoningTokens != 5 || strings.Contains(result.Transcript, "keep-out") {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for _, command := range []contracts.CommandResult{
		{ExitCode: 1, Stdout: valid},
		{ExitCode: 0, Stdout: []byte("not-json\n")},
		{ExitCode: 0, Stdout: []byte(`{"type":"item.completed","item":{"type":"agent_message","text":"not json"}}` + "\n")},
		{ExitCode: 0, Stdout: valid, StdoutTruncated: true},
		{ExitCode: 0, Stdout: []byte(strings.Repeat("x", maxJSONL+1))},
	} {
		if _, err := adapter.Parse(context.Background(), input, command); err == nil {
			t.Fatalf("accepted bad result=%+v", command)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := adapter.Parse(cancelled, input, contracts.CommandResult{ExitCode: 0, Stdout: valid, OutputLastMessage: []byte(`{}`)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled parse err=%v", err)
	}
}

func TestCodexProbeFailsClosedForCapabilityAuthAndVersion(t *testing.T) {
	adapter, probe := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	probe.help = []byte("--json --sandbox -C")
	if _, err := adapter.Probe(context.Background()); !errors.Is(err, ErrCapability) {
		t.Fatalf("missing capability err=%v", err)
	}
	probe.help = requiredHelp()
	probe.loginExit = 1
	if _, err := adapter.Probe(context.Background()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("auth err=%v", err)
	}
	probe.loginExit = 0
	probe.login = []byte("Logged in using an API key\n")
	if _, err := adapter.Probe(context.Background()); !errors.Is(err, ErrUnsupportedAuthMode) {
		t.Fatalf("metered auth mode err=%v", err)
	}
	probe.login = []byte("Logged in using ChatGPT\n")
	probe.version = []byte("Codex development build\n")
	if _, err := adapter.Probe(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("version err=%v", err)
	}
	if _, err := New(Config{Executable: filepath.Join(t.TempDir(), "missing"), AuthHome: privateDir(t, "auth")}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("missing executable err=%v", err)
	}
}

func TestCodexQualificationExpiresWhenDaemonLeaderRestarts(t *testing.T) {
	ctx := context.Background()
	adapter, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	attestor := qualificationAttestor(t, database, domain.ChannelDev)
	if _, err := Qualify(ctx, database, domain.ChannelDev, adapter, fixture{}, attestor); err != nil {
		t.Fatal(err)
	}
	binding, err := adapter.Binding(ctx)
	if err != nil || !qualificationMatches(database, ctx, domain.ChannelDev, binding) {
		t.Fatalf("fresh qualification did not match: binding=%+v err=%v", binding, err)
	}
	restarted, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "qualification-restart")
	if err != nil || database.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, restarted.PublicKey()) != nil {
		t.Fatalf("restart recovery authority: leader=%d err=%v", leader, err)
	}
	if qualificationMatches(database, ctx, domain.ChannelDev, binding) {
		t.Fatal("a qualification signed by the previous daemon leader was admitted after restart")
	}
}

func TestCodexCredentialHomeAllowsReadOnlyUserDirectoryButRejectsWritableOrSymlinked(t *testing.T) {
	directory := privateDir(t, "home")
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(privateDir(t, "bin"), "codex")
	if err := os.WriteFile(executable, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Executable: executable, AuthHome: directory, Model: "gpt-5.6-luna", Runner: &fakeProbe{version: []byte("codex 1.2.3"), help: requiredHelp()}}); err != nil {
		t.Fatalf("read-only credential home rejected: %v", err)
	}
	if err := os.Chmod(directory, 0o775); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Executable: executable, AuthHome: directory, Model: "gpt-5.6-luna"}); !errors.Is(err, ErrUnsafeConfiguration) {
		t.Fatalf("group-writable credential home accepted: %v", err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "auth-link")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Executable: executable, AuthHome: link, Model: "gpt-5.6-luna"}); !errors.Is(err, ErrUnsafeConfiguration) {
		t.Fatalf("symlinked credential home accepted: %v", err)
	}
}

func TestCodexAuthDigestBindsOpenedAuthFileIdentityAndMetadata(t *testing.T) {
	adapter, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	before, err := adapter.Binding(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(adapter.authHome, "auth.json"), []byte(`{"fixture":"rotated-account"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	after, err := adapter.Binding(context.Background())
	if err != nil || before.AuthDigest == after.AuthDigest {
		t.Fatalf("auth binding did not change after credential metadata changed: before=%+v after=%+v err=%v", before, after, err)
	}
}

func TestCodexQualificationBindsFixtureAndBinaryDigest(t *testing.T) {
	adapter, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	database, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	attestor := qualificationAttestor(t, database, domain.ChannelDev)
	passed, err := Qualify(context.Background(), database, domain.ChannelDev, adapter, fixture{}, attestor)
	if err != nil || passed.Profile != store.QualificationGuarded || passed.Provider.Provider != "codex" || passed.FixtureDigest != fixtureDigest() {
		t.Fatalf("passed=%+v err=%v", passed, err)
	}
	failed, err := Qualify(context.Background(), database, domain.ChannelDev, adapter, fixture{failed: []string{"network", "network"}}, attestor)
	if err != nil || failed.Profile != store.QualificationDisabled || !reflect.DeepEqual(failed.FailedProbes, []string{"network"}) {
		t.Fatalf("failed=%+v err=%v", failed, err)
	}
	if _, err := Qualify(context.Background(), database, domain.ChannelDev, adapter, fixture{failed: []string{"unknown_probe"}}, attestor); !errors.Is(err, ErrUnsafeConfiguration) {
		t.Fatalf("unknown probe accepted: %v", err)
	}
	if _, err := Qualify(context.Background(), database, domain.ChannelDev, adapter, wrongFixture{}, attestor); !errors.Is(err, ErrUnsafeConfiguration) {
		t.Fatalf("fixture digest mismatch err=%v", err)
	}
	old := passed.BinaryDigest
	if err := os.WriteFile(adapter.executable, []byte("changed"), 0o700); err != nil {
		t.Fatal(err)
	}
	binding, err := adapter.Binding(context.Background())
	if err != nil || binding.BinaryDigest == old || qualificationMatches(database, context.Background(), domain.ChannelDev, binding) {
		t.Fatalf("digest binding=%+v err=%v", binding, err)
	}
}

func qualificationAttestor(t *testing.T, database *store.Store, channel domain.Channel) *processsupervisor.Supervisor {
	t.Helper()
	value, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(context.Background(), channel, "qualification-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetRecoveryAuthority(context.Background(), channel, leader, value.PublicKey()); err != nil {
		t.Fatal(err)
	}
	return value
}

func qualificationAttestorForLeader(t *testing.T, database *store.Store, channel domain.Channel, leader uint64) *processsupervisor.Supervisor {
	t.Helper()
	value, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetRecoveryAuthority(context.Background(), channel, leader, value.PublicKey()); err != nil {
		t.Fatal(err)
	}
	return value
}

type wrongFixture struct{}

func (wrongFixture) Digest() string                                  { return strings.Repeat("0", 64) }
func (wrongFixture) qualificationFixtureSeal()                       {}
func (wrongFixture) Run(context.Context, *Adapter) ([]string, error) { return nil, nil }

func TestCodexRejectsHostileWorktreeAndPrompt(t *testing.T) {
	adapter, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	identity, _ := adapter.Probe(context.Background())
	for _, input := range []contracts.PhaseInput{
		{Provider: identity, Worktree: "/tmp/../tmp", Prompt: "x", Schema: []byte(`{}`)},
		{Provider: identity, Worktree: privateDir(t, "work"), Prompt: "x\x00y", Schema: []byte(`{}`)},
		{Provider: identity, Worktree: privateDir(t, "work"), Prompt: "x", Schema: []byte(`no`)},
	} {
		if _, err := adapter.Invocation(context.Background(), input); err == nil {
			t.Fatalf("accepted hostile input=%+v", input)
		}
	}
}

func TestProbeContextCancellationDoesNotBecomeAuthentication(t *testing.T) {
	adapter, probe := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	probe.err = context.DeadlineExceeded
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	if _, err := adapter.Probe(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("timeout probe err=%v", err)
	}
}

type compositionSupervisor struct {
	signer     *contracts.DrainSigner
	registered []domain.ProviderIdentity
}

func newCompositionSupervisor(t *testing.T) *compositionSupervisor {
	t.Helper()
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	return &compositionSupervisor{signer: signer}
}
func (s *compositionSupervisor) PublicKey() []byte { return s.signer.PublicKey() }
func (s *compositionSupervisor) Run(context.Context, contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) (contracts.CommandResult, error) {
	return contracts.CommandResult{}, errors.New("must not launch in composition test")
}
func (s *compositionSupervisor) Drain(_ context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	return s.signer.ProveDrained(request)
}
func (s *compositionSupervisor) SetLaunchRecorder(func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error) {
}
func (s *compositionSupervisor) RegisterRuntime(binding contracts.RuntimeBinding, path, _ string) (string, error) {
	s.registered = append(s.registered, binding.Identity)
	return digestFile(path)
}

func TestComposeProfilesRequiresTwoQualifiedDistinctFamilies(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	builder, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	reviewer, _ := adapterFixture(t, "codex-reviewer", "gpt-5.5")
	attestor := qualificationAttestor(t, database, domain.ChannelDev)
	for _, adapter := range []*Adapter{builder, reviewer} {
		if _, recordErr := Qualify(ctx, database, domain.ChannelDev, adapter, fixture{}, attestor); recordErr != nil {
			t.Fatal(recordErr)
		}
	}
	buildBinding, _ := builder.Binding(ctx)
	reviewBinding, _ := reviewer.Binding(ctx)
	buildQ, _ := database.LatestProviderQualification(ctx, domain.ChannelDev, buildBinding.Identity)
	reviewQ, _ := database.LatestProviderQualification(ctx, domain.ChannelDev, reviewBinding.Identity)
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, buildQ.ID, reviewQ.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	supervisor := newCompositionSupervisor(t)
	if _, err := ComposeProfiles(ctx, domain.ChannelDev, database, supervisor, []Config{{Route: builder.route, Executable: builder.executable, AuthHome: builder.authHome, Model: builder.model, Runner: builder.runner}, {Route: reviewer.route, Executable: reviewer.executable, AuthHome: reviewer.authHome, Model: reviewer.model, Runner: reviewer.runner}}); err != nil {
		t.Fatal(err)
	}
	if len(supervisor.registered) != 2 || supervisor.registered[0].Family == supervisor.registered[1].Family {
		t.Fatalf("registered=%+v", supervisor.registered)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, buildQ.ID, buildQ.ID, time.Now().UTC()); !errors.Is(err, store.ErrProviderPairRefused) {
		t.Fatalf("same-family pair was accepted: %v", err)
	}
	noQualification, err := store.Open(ctx, filepath.Join(t.TempDir(), "none.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = noQualification.Close() })
	noQualificationSupervisor := newCompositionSupervisor(t)
	if _, err := ComposeProfiles(ctx, domain.ChannelDev, noQualification, noQualificationSupervisor, []Config{{Route: builder.route, Executable: builder.executable, AuthHome: builder.authHome, Model: builder.model, Runner: builder.runner}}); err != nil {
		t.Fatal(err)
	}
	if len(noQualificationSupervisor.registered) != 0 {
		t.Fatalf("unqualified Codex was registered: %+v", noQualificationSupervisor.registered)
	}
	alias, _ := adapterFixture(t, "codex-builder-alias", "gpt-5.6-luna")
	aliasBinding, err := alias.Binding(ctx)
	if err != nil || aliasBinding.Identity.Family != buildBinding.Identity.Family || aliasBinding.Identity.Model != buildBinding.Identity.Model {
		t.Fatalf("route alias changed durable model family: binding=%+v err=%v", aliasBinding, err)
	}
}

type jsonSupervisor struct{ signer *contracts.DrainSigner }

func newJSONSupervisor(t *testing.T) *jsonSupervisor {
	t.Helper()
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	return &jsonSupervisor{signer: signer}
}
func (s *jsonSupervisor) PublicKey() []byte { return s.signer.PublicKey() }
func (s *jsonSupervisor) Run(context.Context, contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) (contracts.CommandResult, error) {
	artifact := []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"proof"},"paths":["internal/x.go"],"commands":[["go","test"]],"risks":["risk"],"questions":[]}`)
	return contracts.CommandResult{ExitCode: 0, OutputLastMessage: artifact, Stdout: []byte("{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":3,\"total_tokens\":7}}\n")}, nil
}
func (s *jsonSupervisor) Drain(_ context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	return s.signer.ProveDrained(request)
}
func (s *jsonSupervisor) SetLaunchRecorder(func(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) error) {
}

func TestCodexJSONLSubscriptionPhaseReportsTrustedZeroIncrementalUsage(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	adapter, _ := adapterFixture(t, "codex-builder", "gpt-5.6-luna")
	raw := []byte("configuration")
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	project := store.Project{Channel: domain.ChannelDev, ID: "demo", Path: "/tmp/codex-project", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}
	if err := database.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "codex-test")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-codex"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticketValue, err := database.StartOrAdopt(ctx, ref, 1, "dev/demo/SF-codex", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	worktree := privateDir(t, "worktree")
	identity := `{"repository":"/tmp/codex-project"}`
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticketValue.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticketValue.RunnerEpoch}, Path: worktree, Branch: "dev/demo/SF-codex", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	attestor := qualificationAttestorForLeader(t, database, domain.ChannelDev, leader)
	builder, err := Qualify(ctx, database, domain.ChannelDev, adapter, fixture{}, attestor)
	if err != nil {
		t.Fatal(err)
	}
	reviewerIdentity := domain.ProviderIdentity{Provider: "fixture", Model: "reviewer", Family: "reviewer-family", Version: "1"}
	reviewer, _, err := database.RecordProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: strings.Repeat("2", 32), Provider: reviewerIdentity, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.SelectProviderSet(ctx, domain.ChannelDev, builder.ID, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := providercoord.NewRegistry()
	if err := registry.Register(ctx, adapter); err != nil {
		t.Fatal(err)
	}
	supervisor := newJSONSupervisor(t)
	coordinator, err := providercoord.New(registry, map[providercoord.Role]providercoord.Route{providercoord.RolePlanner: {Primary: adapter.Name()}}, database, nil, supervisor)
	if err != nil {
		t.Fatal(err)
	}
	result := coordinator.Run(ctx, providercoord.Request{Role: providercoord.RolePlanner, ExpectedVersion: ticketValue.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticketValue.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "plan", Repository: project.Path, Worktree: worktree, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"."}, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}})
	if result.Code != providercoord.Completed || result.CostUsed != 0 || len(result.Attempts) != 1 || result.Attempts[0].UsageUnits != 0 || result.Attempts[0].TokenUsage != 7 {
		current, _ := database.Ticket(ctx, ref)
		attempts, _ := database.ProviderAttempts(ctx, ref)
		t.Fatalf("result=%+v ticket=%+v attempts=%+v", result, current, attempts)
	}
}

func adapterFixture(t *testing.T, route, model string) (*Adapter, *fakeProbe) {
	t.Helper()
	directory := privateDir(t, "bin")
	executable := filepath.Join(directory, "codex")
	if err := os.WriteFile(executable, []byte("fixture-codex"), 0o700); err != nil {
		t.Fatal(err)
	}
	probe := &fakeProbe{version: []byte("codex 1.2.3\n"), help: requiredHelp(), login: []byte("Logged in using ChatGPT\n"), loginExit: 0}
	authHome := privateDir(t, "auth")
	if err := os.WriteFile(filepath.Join(authHome, "auth.json"), []byte(`{"fixture":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	adapter, err := New(Config{Route: route, Executable: executable, AuthHome: authHome, Model: model, Runner: probe})
	if err != nil {
		t.Fatal(err)
	}
	return adapter, probe
}

func requiredHelp() []byte {
	return []byte("exec --json --output-schema FILE --output-last-message FILE --ephemeral --ignore-user-config --ignore-rules --config KEY=VALUE --model NAME -C DIR\n")
}

func privateDir(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
