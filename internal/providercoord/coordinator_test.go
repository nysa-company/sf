package providercoord

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestMalformedOutputFallsBackAndNeverPersistsSecrets(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	identity := `{"repository":"/tmp/p"}`
	t.Cleanup(func() { db.Close() })
	raw := []byte("frozen")
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: "/tmp/p", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "test")
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-1"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/p/SF-1", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: root, Branch: "dev/p/SF-1", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	primary := testkit.NewScriptedProvider(id("cursor", "cursor-family"))
	secret := strings.Join([]string{"token", "must-not-persist"}, "=")
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderMalformed, Transcript: secret})
	fallback := testkit.NewScriptedProvider(id("claude", "claude-family"))
	fallback.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: plannerArtifact()})
	recordQual(t, db, primary)
	recordQual(t, db, fallback)
	primaryQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("cursor", "cursor-family"))
	fallbackQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("claude", "claude-family"))
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, primaryQualification.ID, primaryQualification.ID, fallbackQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	c, err := New(registry, map[Role]Route{RolePlanner: {Primary: "cursor", Fallback: "claude"}}, db, nil, testkit.NewSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Role: RolePlanner, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "x", Repository: "/tmp/p", Worktree: root, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"x"}, Timeout: time.Second, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}}
	result := c.Run(ctx, r)
	if result.Code != Failed || len(result.Attempts) != 1 || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("result=%+v", result)
	}
	rawSecret := sha256.Sum256([]byte(secret))
	if result.Attempts[0].TranscriptDigest == "" || result.Attempts[0].TranscriptDigest == "sha256:"+fmt.Sprintf("%x", rawSecret) {
		t.Fatalf("unsafe digest=%+v", result.Attempts[0])
	}
}

func TestCompletedResultExposesOnlyTheDurableAttemptKey(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}

	result := coordinator.Run(context.Background(), request)
	if result.Code != Completed {
		t.Fatalf("result=%+v", result)
	}
	want := store.ProviderAttemptResultKey{AttemptID: result.ProviderResult.AttemptID, Ref: ref, Phase: domain.PhasePlanning, Attempt: 1}
	if result.ProviderResult != want {
		t.Fatalf("provider result key=%+v want=%+v", result.ProviderResult, want)
	}
	if _, _, err := database.LoadHistoricalProviderAttemptResult(context.Background(), result.ProviderResult); err != nil {
		t.Fatalf("result key was not durably persisted: %v", err)
	}
}

func TestCoordinatorReusesOnlyExactCompletedResult(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
	first := coordinator.Run(context.Background(), request)
	if first.Code != Completed {
		t.Fatalf("first=%+v", first)
	}
	second := coordinator.Run(context.Background(), request)
	if second.Code != Completed || second.ProviderResult != first.ProviderResult {
		t.Fatalf("reused=%+v first=%+v", second, first)
	}
	if attempts, err := database.ProviderAttempts(context.Background(), ref); err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}

	for name, mutate := range map[string]func(*Request){
		"prompt":     func(r *Request) { r.Input.Prompt = "different" },
		"paths":      func(r *Request) { r.Input.AllowedPaths = []string{"different"} },
		"validation": func(r *Request) { r.Validation.ProtectedVerification = []string{"internal/protected.go"} },
		"fence":      func(r *Request) { r.Fence.LeaderEpoch++ },
		"version":    func(r *Request) { r.ExpectedVersion++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if result := coordinator.Run(context.Background(), changed); result.Code == Completed {
				t.Fatalf("false completion=%+v", result)
			}
			if attempts, err := database.ProviderAttempts(context.Background(), ref); err != nil || len(attempts) != 1 {
				t.Fatalf("attempts=%+v err=%v", attempts, err)
			}
		})
	}
}

type blockingBindingProvider struct {
	*testkit.ScriptedProvider
	entered chan struct{}
	release chan struct{}
}

func (p *blockingBindingProvider) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return contracts.RuntimeBinding{}, ctx.Err()
	}
	return p.ScriptedProvider.Binding(ctx)
}

func TestCoordinatorBeginRaceReusesCompletionWithoutSecondLaunch(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	plain := testkit.NewScriptedProvider(id("cursor", "cursor-family"))
	plain.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: plannerArtifact()})
	registry := NewRegistry()
	if err := registry.Register(context.Background(), plain); err != nil {
		t.Fatal(err)
	}
	// Use a separate production coordinator for A so B can be held after its
	// early Store reuse check but before BeginProviderAttempt.
	a, err := New(registry, map[Role]Route{RolePlanner: {Primary: "cursor"}}, database, nil, testkit.NewSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	blocked := &blockingBindingProvider{ScriptedProvider: primary, entered: make(chan struct{}, 1), release: make(chan struct{})}
	coordinator.registry.providers["cursor"] = blocked
	resultB := make(chan Result, 1)
	go func() { resultB <- coordinator.Run(context.Background(), request) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("B did not reach binding barrier")
	}
	resultA := a.Run(context.Background(), request)
	if resultA.Code != Completed {
		t.Fatalf("A=%+v", resultA)
	}
	close(blocked.release)
	result := <-resultB
	if result.Code != Completed || result.ProviderResult != resultA.ProviderResult {
		t.Fatalf("B=%+v A=%+v", result, resultA)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
}

func TestFallbackCompletionExposesFallbackAttemptKey(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Behavior: testkit.ProviderMalformed}}
	// The Store-selected planner qualification is the primary identity. Keep
	// that durable identity while using a distinct route alias to exercise the
	// coordinator's fallback path without weakening provider qualification.
	fallback := &aliasedProvider{ScriptedProvider: testkit.NewScriptedProvider(id("cursor", "cursor-family"))}
	fallback.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: plannerArtifact()})
	coordinator.registry.providers["claude"] = fallback
	result := coordinator.Run(context.Background(), request)
	if result.Code != Completed {
		t.Fatalf("fallback result=%+v", result)
	}
	if result.ProviderResult.AttemptID <= 0 || result.ProviderResult.Ref != ref || result.ProviderResult.Phase != domain.PhasePlanning || result.ProviderResult.Attempt != 2 {
		t.Fatalf("fallback provider result key=%+v", result.ProviderResult)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if attempts[0].Binding.Identity.Provider != "cursor" || attempts[1].Binding.Identity.Provider != "cursor" {
		t.Fatalf("attempt providers=%q,%q", attempts[0].Binding.Identity.Provider, attempts[1].Binding.Identity.Provider)
	}
	if _, _, err := database.LoadHistoricalProviderAttemptResult(context.Background(), result.ProviderResult); err != nil {
		t.Fatalf("fallback key was not durably persisted: %v", err)
	}
}

type aliasedProvider struct{ *testkit.ScriptedProvider }

func (p *aliasedProvider) Name() string { return "claude" }

type undrainedProvider struct {
	*testkit.ScriptedProvider
	drained bool
	request contracts.DrainRequest
}

type refusingSupervisor struct{ *testkit.Supervisor }

func (s refusingSupervisor) Drain(context.Context, contracts.DrainRequest) (contracts.DrainProof, error) {
	return contracts.DrainProof{}, fmt.Errorf("tracked process group remains live")
}

type faultingSupervisor struct {
	*testkit.Supervisor
	onDrain func()
}

type faultAfterDrainSupervisor struct {
	*testkit.Supervisor
	onDrain func()
}

type ambiguousRunSupervisor struct{ *testkit.Supervisor }

func (s ambiguousRunSupervisor) Run(context.Context, contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) (contracts.CommandResult, error) {
	return contracts.CommandResult{}, errors.New("supervisor failed after possible pre-exec child creation")
}
func (s ambiguousRunSupervisor) Drain(context.Context, contracts.DrainRequest) (contracts.DrainProof, error) {
	return contracts.DrainProof{}, errors.New("operator drain proof required")
}

func (s faultingSupervisor) Drain(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	if s.onDrain != nil {
		s.onDrain()
	}
	return contracts.DrainProof{}, fmt.Errorf("tracked process group remains live")
}

func (s faultAfterDrainSupervisor) Drain(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	if s.onDrain != nil {
		s.onDrain()
	}
	return s.Signer.ProveDrained(request)
}

func TestPersistenceFailureLatchesCoordinatorAndPreservesActiveClaim(t *testing.T) {
	var database *store.Store
	supervisor := &faultingSupervisor{Supervisor: testkit.NewSupervisor()}
	database, request, coordinator, ref, _ := newCoordinatorFixture(t, supervisor)
	supervisor.onDrain = func() {
		database.SetWriteFaultForTest(func() error { return errors.New("injected quarantine write failure") })
	}
	first := coordinator.Run(context.Background(), request)
	if !first.NeedsOperator || !first.PersistenceFailure || first.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("persistence failure was not surfaced: %+v", first)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "active" {
		t.Fatalf("active claim was not preserved after failed quarantine: %+v err=%v", attempts, err)
	}
	second := coordinator.Run(context.Background(), request)
	if !second.NeedsOperator || second.Code != NeedsOperator || second.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("latched coordinator admitted a later run: %+v", second)
	}
}

func TestCompletionPersistenceFailureDoesNotExposeProviderResultKey(t *testing.T) {
	var database *store.Store
	supervisor := &faultAfterDrainSupervisor{Supervisor: testkit.NewSupervisor()}
	database, request, coordinator, _, primary := newCoordinatorFixture(t, supervisor)
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
	supervisor.onDrain = func() {
		database.SetWriteFaultForTest(func() error { return errors.New("injected completion write failure") })
	}

	result := coordinator.Run(context.Background(), request)
	if result.Code != NeedsOperator || !result.PersistenceFailure || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("completion persistence result=%+v", result)
	}
}

func TestCanceledResultDoesNotExposeProviderResultKey(t *testing.T) {
	database, request, coordinator, ref, _ := newCoordinatorFixture(t, testkit.NewSupervisor())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	result := coordinator.Run(ctx, request)
	if !result.NeedsOperator || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		attempts, _ := database.ProviderAttempts(context.Background(), ref)
		t.Logf("canceled attempts=%+v", attempts)
		t.Fatalf("canceled result=%+v", result)
	}
}

func TestBudgetExhaustionDoesNotExposeProviderResultKey(t *testing.T) {
	_, request, coordinator, _, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact(), UsageUnits: 101}}

	result := coordinator.Run(context.Background(), request)
	if result.Code != BudgetExhausted || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("budget result=%+v", result)
	}
}

func newCoordinatorFixture(t *testing.T, supervisor contracts.ProcessSupervisor) (*store.Store, Request, *Coordinator, domain.TicketRef, *testkit.ScriptedProvider) {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	root := t.TempDir()
	raw := []byte("frozen")
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: "/tmp/p", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, _ := database.AcquireLeader(ctx, domain.ChannelDev, "persistence-test")
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-persistence"}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := database.StartOrAdopt(ctx, ref, 1, "dev/p/SF-persistence", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: root, Branch: "dev/p/SF-persistence", IdentityJSON: []byte(`{"repository":"/tmp/p"}`), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	primary := testkit.NewScriptedProvider(id("cursor", "cursor-family"))
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderHang})
	fallback := testkit.NewScriptedProvider(id("claude", "claude-family"))
	if err := recordQualForFixture(database, primary); err != nil {
		t.Fatal(err)
	}
	if err := recordQualForFixture(database, fallback); err != nil {
		t.Fatal(err)
	}
	primaryQualification, _ := database.LatestProviderQualification(ctx, domain.ChannelDev, id("cursor", "cursor-family"))
	fallbackQualification, _ := database.LatestProviderQualification(ctx, domain.ChannelDev, id("claude", "claude-family"))
	if _, _, err := database.SelectProviderSet(ctx, domain.ChannelDev, primaryQualification.ID, primaryQualification.ID, fallbackQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	coordinator, err := New(registry, map[Role]Route{RolePlanner: {Primary: "cursor", Fallback: "claude"}}, database, nil, supervisor)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Role: RolePlanner, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "x", Repository: "/tmp/p", Worktree: root, WorktreeIdentity: `{"repository":"/tmp/p"}`, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"x"}, Timeout: 200 * time.Millisecond, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}}
	return database, request, coordinator, ref, primary
}

func TestInvocationFailureClosesPreLaunchClaimWithoutQuarantine(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.InvocationErr = errors.New("adapter rejected input before launch")
	result := coordinator.Run(context.Background(), request)
	if len(result.Attempts) == 0 || result.Attempts[0].ErrorCode != "provider_invocation_failed" {
		t.Fatalf("invocation failure receipt=%+v", result)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) == 0 || attempts[0].State != "failed" || attempts[0].Outcome != "invocation_failed" {
		t.Fatalf("pre-launch attempt=%+v err=%v", attempts, err)
	}
}

func TestSupervisorRunAmbiguityRemainsQuarantinedForOperatorRecovery(t *testing.T) {
	database, request, coordinator, ref, _ := newCoordinatorFixture(t, ambiguousRunSupervisor{Supervisor: testkit.NewSupervisor()})
	result := coordinator.Run(context.Background(), request)
	if !result.NeedsOperator {
		t.Fatalf("ambiguous supervisor failure was not escalated: %+v", result)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "quarantined" || attempts[0].Outcome != "undrained" {
		t.Fatalf("post-spawn ambiguity was released instead of quarantined: %+v err=%v", attempts, err)
	}
}

func recordQualForFixture(database *store.Store, provider *testkit.ScriptedProvider) error {
	binding, err := provider.Binding(context.Background())
	if err != nil {
		return err
	}
	runSum := sha256.Sum256([]byte(binding.Identity.Provider + binding.Identity.Model + binding.Identity.Version))
	_, _, err = database.RecordProviderQualification(context.Background(), store.ProviderQualification{Channel: domain.ChannelDev, RunID: fmt.Sprintf("%x", runSum)[:32], Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
	return err
}

func (p *undrainedProvider) Drain(_ context.Context, request contracts.DrainRequest) (contracts.DrainResult, error) {
	p.request = request
	return contracts.DrainResult{Drained: p.drained}, nil
}

func TestCancellationQuarantinesWhenProviderDoesNotDrain(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	identity := `{"repository":"/tmp/p"}`
	t.Cleanup(func() { _ = db.Close() })
	raw := []byte("frozen")
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: "/tmp/p", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "cancel-test")
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-cancel"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/p/SF-cancel", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: root, Branch: "dev/p/SF-cancel", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	primary := &undrainedProvider{ScriptedProvider: testkit.NewScriptedProvider(id("cursor", "cursor-family")), drained: false}
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderHang})
	fallback := testkit.NewScriptedProvider(id("claude", "claude-family"))
	recordQual(t, db, primary.ScriptedProvider)
	recordQual(t, db, fallback)
	primaryQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("cursor", "cursor-family"))
	fallbackQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("claude", "claude-family"))
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, primaryQualification.ID, primaryQualification.ID, fallbackQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	c, err := New(registry, map[Role]Route{RolePlanner: {Primary: "cursor", Fallback: "claude"}}, db, nil, refusingSupervisor{testkit.NewSupervisor()})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Role: RolePlanner, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "x", Repository: "/tmp/p", Worktree: root, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"x"}, Timeout: time.Second, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}}
	// Leave enough startup budget for the race-instrumented SQLite admission
	// path to persist the claim before cancellation exercises quarantine.
	callCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	result := c.Run(callCtx, request)
	if !result.NeedsOperator {
		t.Fatalf("cancellation result=%+v", result)
	}
	attempts, err := db.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "quarantined" {
		t.Fatalf("quarantined attempts=%+v err=%v", attempts, err)
	}
	claim := attempts[0]
	_ = claim // the adapter cannot self-attest: only the refusing supervisor was consulted.
}
func id(name, family string) domain.ProviderIdentity {
	return domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: family, Version: "v1"}
}
func plannerArtifact() []byte {
	return []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"x"},"paths":["x"],"commands":[["go","test"]],"risks":["x"]}`)
}
func recordQual(t *testing.T, db *store.Store, p *testkit.ScriptedProvider) {
	t.Helper()
	b, e := p.Binding(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	runSum := sha256.Sum256([]byte(b.Identity.Provider + b.Identity.Model + b.Identity.Version))
	run := fmt.Sprintf("%x", runSum)[:32]
	_, _, e = db.RecordProviderQualification(context.Background(), store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: b.Identity, BinaryDigest: b.BinaryDigest, PolicyDigest: b.PolicyDigest, FixtureDigest: b.FixtureDigest, Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
	if e != nil {
		t.Fatal(e)
	}
}

func TestBindClaimToInputRejectsDurableIdentityDrift(t *testing.T) {
	identity := domain.ProviderIdentity{Provider: "codex", Model: "model", Family: "family", Version: "v1"}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-claim-binding"}
	request := Request{Role: RoleBuilder, ExpectedVersion: 5, Fence: domain.Fence{LeaderEpoch: 3, RunnerEpoch: 4}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhaseBuild, Prompt: "build", Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base", AllowedPaths: []string{"src"}, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}}
	launch := request.Input
	launch.Provider, launch.AuthMode = identity, "chatgpt_subscription"
	launch.Attempt, launch.LeaderEpoch, launch.RunnerEpoch, launch.ExpectedVersion = 2, 3, 4, 5
	_, requestDigest, err := contracts.CanonicalPhaseInput(launch)
	if err != nil {
		t.Fatal(err)
	}
	launch.RequestDigest = requestDigest
	claim := store.ProviderAttemptClaim{ID: 7, Ref: ref, Phase: domain.PhaseBuild, Role: "builder", Attempt: 2, Binding: contracts.RuntimeBinding{Identity: identity, AuthMode: "chatgpt_subscription"}, LeaderEpoch: 3, RunnerEpoch: 4, ExpectedVersion: 5, Repository: "/repo", Worktree: "/worktree", WorktreeIdentity: "identity", BaseSHA: "base", Input: launch, RequestDigest: requestDigest}
	input := request.Input
	if !bindClaimToInput(&input, claim, request, identity) || input.Attempt != claim.Attempt || input.LeaderEpoch != claim.LeaderEpoch || input.RunnerEpoch != claim.RunnerEpoch || input.ExpectedVersion != claim.ExpectedVersion {
		t.Fatalf("matching claim was not bound: input=%+v", input)
	}
	for name, mutate := range map[string]func(*contracts.PhaseInput){
		"ticket":     func(value *contracts.PhaseInput) { value.Ticket.Ticket = "SF-other" },
		"phase":      func(value *contracts.PhaseInput) { value.Phase = domain.PhasePlanning },
		"repository": func(value *contracts.PhaseInput) { value.Repository = "/other" },
		"worktree":   func(value *contracts.PhaseInput) { value.Worktree = "/other" },
		"identity":   func(value *contracts.PhaseInput) { value.WorktreeIdentity = "other" },
		"base":       func(value *contracts.PhaseInput) { value.BaseSHA = "other" },
		"prompt":     func(value *contracts.PhaseInput) { value.Prompt = "other" },
		"schema":     func(value *contracts.PhaseInput) { value.Schema = []byte(`{"type":"array"}`) },
		"paths":      func(value *contracts.PhaseInput) { value.AllowedPaths = []string{"other"} },
		"profile":    func(value *contracts.PhaseInput) { value.Profile = contracts.ProfileAutonomous },
		"timeout":    func(value *contracts.PhaseInput) { value.Timeout++ },
		"attempt":    func(value *contracts.PhaseInput) { value.Attempt = claim.Attempt + 1 },
		"leader":     func(value *contracts.PhaseInput) { value.LeaderEpoch++ },
		"runner":     func(value *contracts.PhaseInput) { value.RunnerEpoch++ },
		"version":    func(value *contracts.PhaseInput) { value.ExpectedVersion++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request.Input
			mutate(&changed)
			if bindClaimToInput(&changed, claim, request, identity) {
				t.Fatal("durable claim accepted mismatched phase input")
			}
		})
	}
}

func TestChangedFilesMustStayWithinClaimedAllowedPrefixes(t *testing.T) {
	for name, value := range map[string]struct {
		changed []string
		allowed bool
	}{
		"exact prefix":       {changed: []string{"src/main.go"}, allowed: true},
		"nested prefix":      {changed: []string{"src/internal/main.go"}, allowed: true},
		"sibling is denied":  {changed: []string{"src-old/main.go"}, allowed: false},
		"escape is denied":   {changed: []string{"../outside"}, allowed: false},
		"absolute is denied": {changed: []string{"/outside"}, allowed: false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := changedFilesAllowed(value.changed, []string{"src"}); got != value.allowed {
				t.Fatalf("changed=%q allowed=%v", value.changed, got)
			}
		})
	}
}
