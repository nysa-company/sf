package providercoord

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestReusedInputMatchesExactLegacyPhaseInput(t *testing.T) {
	input := contracts.PhaseInput{
		Ticket: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-legacy-reuse"}, Phase: domain.PhasePlanning,
		Attempt: 1, LeaderEpoch: 4, RunnerEpoch: 1, ExpectedVersion: 2, Prompt: "plan", Repository: "/repo", Worktree: "/repo/.sf/worktree", WorktreeIdentity: "identity", BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"."}, Provider: domain.ProviderIdentity{Provider: "codex", Model: "model", Family: "openai", Version: "1"}, AuthMode: "chatgpt_subscription", Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`),
	}
	payload, _, err := contracts.CanonicalPhaseInput(input)
	if err != nil {
		t.Fatal(err)
	}
	legacy := bytes.TrimSuffix(payload, []byte(`,"Repair":null}`))
	if len(legacy) == len(payload) {
		t.Fatalf("current payload has no v53 repair suffix: %s", payload)
	}
	legacy = append(append([]byte(nil), legacy...), '}')
	sum := sha256.Sum256(legacy)
	digest := fmt.Sprintf("%x", sum)
	decoded, err := contracts.DecodeCanonicalPhaseInput(legacy)
	if err != nil {
		t.Fatal(err)
	}
	claim := store.ProviderAttemptClaim{Attempt: 1, Binding: contracts.RuntimeBinding{Identity: input.Provider, AuthMode: input.AuthMode}, LeaderEpoch: 4, RunnerEpoch: 1, ExpectedVersion: 2, Input: decoded, RequestDigest: digest, RequestPayload: legacy}
	input.Provider, input.AuthMode, input.Attempt, input.LeaderEpoch, input.RunnerEpoch, input.ExpectedVersion = domain.ProviderIdentity{}, "", 0, 0, 0, 0
	if !reusedInputMatches(Request{Input: input}, claim) {
		t.Fatal("logical request did not reuse exact legacy claim input")
	}
}

func TestMalformedOutputIsIndeterminateAndNeverRepairsOrPersistsSecrets(t *testing.T) {
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
	// A provider transcript that cannot establish trusted usage is
	// indeterminate, even though it contains malformed/untrusted output.
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderSecret, Transcript: secret})
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
	if result.Code != ResultIndeterminate || !result.NeedsOperator || len(result.Attempts) != 1 || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("result=%+v", result)
	}
	if calls := primary.CallsSnapshot(); len(calls) != 1 {
		t.Fatalf("indeterminate result was repaired: %v", calls)
	}
	if fallbackCalls := fallback.CallsSnapshot(); len(fallbackCalls) != 0 {
		t.Fatalf("indeterminate result used fallback: %v", fallbackCalls)
	}
	attempts, err := db.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].Outcome != "result_indeterminate" || attempts[0].State != "failed" {
		t.Fatalf("durable indeterminate attempt=%+v err=%v", attempts, err)
	}
	replay := c.Run(ctx, r)
	if replay.Code != ResultIndeterminate || len(replay.Attempts) != 0 || len(primary.CallsSnapshot()) != 1 {
		t.Fatalf("indeterminate replay=%+v calls=%v", replay, primary.CallsSnapshot())
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

func TestSingleRouteRetriesInvalidArtifactWithinOneAdmission(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Capacity: 1}
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{
		{Artifact: []byte(`{"schema":"not-a-planner"}`)},
		{Artifact: plannerArtifact()},
	}

	result := coordinator.Run(context.Background(), request)
	if result.Code != Completed || result.ProviderResult.Attempt != 2 || len(result.Attempts) != 2 {
		t.Fatalf("single-route retry=%+v", result)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if attempts[0].State != "failed" || attempts[0].Outcome != "invalid_artifact" || attempts[1].State != "completed" || attempts[1].Outcome != "completed" {
		t.Fatalf("attempt lifecycle=%+v", attempts)
	}
	replayed := coordinator.Run(context.Background(), request)
	if replayed.Code != Completed || replayed.ProviderResult != result.ProviderResult {
		t.Fatalf("repair result was not reusable: first=%+v replay=%+v", result, replayed)
	}
	if attempts, err = database.ProviderAttempts(context.Background(), ref); err != nil || len(attempts) != 2 {
		t.Fatalf("repair replay attempts=%+v err=%v", attempts, err)
	}
}

func TestSingleRoutePausesAfterOneInvalidArtifactRepair(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Capacity: 1}
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{
		{Artifact: []byte(`{"schema":"not-a-planner"}`)},
		{Artifact: []byte(`{"schema":"still-not-a-planner"}`)},
	}

	result := coordinator.Run(context.Background(), request)
	if result.Code != AttemptExhausted || !result.NeedsOperator || len(result.Attempts) != 2 {
		t.Fatalf("single-route exhaustion=%+v", result)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	for index, attempt := range attempts {
		if attempt.State != "failed" || attempt.Outcome != "invalid_artifact" {
			t.Fatalf("attempt[%d]=%+v", index, attempt)
		}
	}
}

func TestInvalidArtifactRepairsSameRouteBeforeAvailabilityFallback(t *testing.T) {
	_, request, coordinator, _, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{
		{Artifact: []byte(`{"schema":"not-a-planner"}`)},
		{Artifact: plannerArtifact()},
	}
	fallback, ok := coordinator.registry.providers["claude"].(*testkit.ScriptedProvider)
	if !ok {
		t.Fatal("fixture fallback provider changed type")
	}

	result := coordinator.Run(context.Background(), request)
	if result.Code != Completed || len(result.Attempts) != 2 || result.Attempts[0].Provider != result.Attempts[1].Provider {
		t.Fatalf("same-route repair=%+v", result)
	}
	if calls := fallback.CallsSnapshot(); len(calls) != 0 {
		t.Fatalf("availability fallback handled invalid artifact: %v", calls)
	}
}

func TestRestartedInvalidArtifactRepairBlocksWhenRuntimeBindingRotated(t *testing.T) {
	supervisor := testkit.NewSupervisor()
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, supervisor)
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Fallback: "claude", Capacity: 1}
	claim, binding := seedInvalidProviderAttempt(t, database, request, primary, supervisor)
	request = recoverProviderRequest(t, database, request)
	if pending, found, err := database.PendingProviderRepair(context.Background(), ref, domain.PhasePlanning, string(RolePlanner), request.ExpectedVersion, request.Fence); err != nil || !found || pending.ID != claim.ID {
		t.Fatalf("pending repair=%+v found=%v err=%v", pending, found, err)
	}
	rotated := binding
	rotated.AuthDigest = strings.Repeat("f", 64)
	coordinator.registry.mu.Lock()
	coordinator.registry.providers["cursor"] = &bindingOverrideProvider{Provider: primary, binding: rotated}
	coordinator.registry.mu.Unlock()

	result := coordinator.Run(context.Background(), request)
	if result.Code != RepairUnavailable || !result.NeedsOperator || len(result.Attempts) != 0 {
		t.Fatalf("rotated repair result=%+v", result)
	}
	fallback := coordinator.registry.providers["claude"].(*testkit.ScriptedProvider)
	if calls := fallback.CallsSnapshot(); len(calls) != 0 {
		t.Fatalf("rotated repair launched fallback: %v", calls)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].Outcome != "invalid_artifact" {
		t.Fatalf("rotated repair attempts=%+v err=%v", attempts, err)
	}
}

func TestRestartedInvalidArtifactRepairUsesSameBindingExactlyOnce(t *testing.T) {
	supervisor := testkit.NewSupervisor()
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, supervisor)
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Capacity: 1}
	claim, _ := seedInvalidProviderAttempt(t, database, request, primary, supervisor)
	request = recoverProviderRequest(t, database, request)
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}

	result := coordinator.Run(context.Background(), request)
	if result.Code != Completed || result.ProviderResult.Attempt != claim.Attempt+1 || len(result.Attempts) != 1 {
		t.Fatalf("recovered repair=%+v", result)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 || attempts[0].Outcome != "invalid_artifact" || attempts[1].Outcome != "completed" {
		t.Fatalf("recovered repair attempts=%+v err=%v", attempts, err)
	}
	replay := coordinator.Run(context.Background(), request)
	if replay.Code != Completed || replay.ProviderResult != result.ProviderResult {
		t.Fatalf("recovered repair replay=%+v first=%+v", replay, result)
	}
}

type interruptedRepairProvider struct {
	*testkit.ScriptedProvider
	invocations, bindings int
}

func (p *interruptedRepairProvider) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	p.bindings++
	return p.ScriptedProvider.Binding(ctx)
}

func (p *interruptedRepairProvider) Invocation(ctx context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	p.invocations++
	if p.invocations == 2 {
		return contracts.Invocation{}, errors.New("repair invocation failed before launch")
	}
	return p.ScriptedProvider.Invocation(ctx, input)
}

func TestInterruptedRepairIsUnavailableBeforeBindingAndStableAcrossReplay(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Fallback: "claude", Capacity: 1}
	interrupted := &interruptedRepairProvider{ScriptedProvider: primary}
	coordinator.registry.providers["cursor"] = interrupted
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: []byte(`{"schema":"not-a-planner"}`)}}

	first := coordinator.Run(context.Background(), request)
	if first.Code != RepairUnavailable || !first.NeedsOperator || len(first.Attempts) != 2 {
		t.Fatalf("interrupted repair result=%+v", first)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 || attempts[0].Outcome != contracts.PhaseResultInvalidArtifact || attempts[1].Outcome != "invocation_failed" {
		t.Fatalf("interrupted repair durable attempts=%+v err=%v", attempts, err)
	}
	if len(primary.CallsSnapshot()) != 1 {
		t.Fatalf("repair invocation unexpectedly parsed provider output: %v", primary.CallsSnapshot())
	}
	fallback := coordinator.registry.providers["claude"].(*testkit.ScriptedProvider)
	if len(fallback.CallsSnapshot()) != 0 {
		t.Fatalf("interrupted repair used fallback: %v", fallback.CallsSnapshot())
	}

	replay := coordinator.Run(context.Background(), request)
	if replay.Code != RepairUnavailable || !replay.NeedsOperator || len(replay.Attempts) != 0 || interrupted.invocations != 2 || interrupted.bindings != 2 {
		t.Fatalf("interrupted repair replay=%+v invocations=%d bindings=%d", replay, interrupted.invocations, interrupted.bindings)
	}
	if attempts, err = database.ProviderAttempts(context.Background(), ref); err != nil || len(attempts) != 2 {
		t.Fatalf("replay changed interrupted repair attempts=%+v err=%v", attempts, err)
	}
}

type repairTimeoutProvider struct {
	*testkit.ScriptedProvider
	mu          sync.Mutex
	bindings    int
	invocations int
}

func (p *repairTimeoutProvider) Binding(ctx context.Context) (contracts.RuntimeBinding, error) {
	p.mu.Lock()
	p.bindings++
	p.mu.Unlock()
	return p.ScriptedProvider.Binding(ctx)
}

func (p *repairTimeoutProvider) Invocation(ctx context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	p.mu.Lock()
	p.invocations++
	p.mu.Unlock()
	return p.ScriptedProvider.Invocation(ctx, input)
}

func (p *repairTimeoutProvider) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bindings, p.invocations
}

func TestTimedOutSameBindingRepairIsUnavailableAcrossReplay(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())

	tracked := &repairTimeoutProvider{ScriptedProvider: primary}
	coordinator.registry.providers["cursor"] = tracked
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{
		{Artifact: []byte(`{"schema":"not-a-planner"}`)},
		{Behavior: testkit.ProviderHang},
	}

	first := coordinator.Run(context.Background(), request)
	if first.Code != RepairUnavailable || !first.NeedsOperator || len(first.Attempts) != 2 {
		t.Fatalf("timed-out repair result=%+v", first)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 || attempts[0].State != "failed" || attempts[0].Outcome != contracts.PhaseResultInvalidArtifact || attempts[1].State != "cancelled" || attempts[1].Outcome != "cancelled" {
		t.Fatalf("timed-out repair attempts=%+v err=%v", attempts, err)
	}
	bindings, invocations := tracked.counts()
	if bindings != 2 || invocations != 2 || len(primary.CallsSnapshot()) != 2 {
		t.Fatalf("timed-out repair launch counts bindings=%d invocations=%d calls=%v", bindings, invocations, primary.CallsSnapshot())
	}

	fallback := coordinator.registry.providers["claude"].(*testkit.ScriptedProvider)
	replay := coordinator.Run(context.Background(), request)
	if replay.Code != RepairUnavailable || !replay.NeedsOperator || len(replay.Attempts) != 0 {
		t.Fatalf("timed-out repair replay=%+v", replay)
	}
	newBindings, newInvocations := tracked.counts()
	if newBindings != bindings || newInvocations != invocations || len(primary.CallsSnapshot()) != 2 || len(fallback.CallsSnapshot()) != 0 {
		t.Fatalf("timed-out repair replay launched work bindings=%d/%d invocations=%d/%d primary=%v fallback=%v", newBindings, bindings, newInvocations, invocations, primary.CallsSnapshot(), fallback.CallsSnapshot())
	}
}

func TestIndeterminateBeginRaceDoesNotLaunchSecondProviderAttempt(t *testing.T) {
	supervisor := testkit.NewSupervisor()
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, supervisor)
	blocked := &blockingBindingProvider{ScriptedProvider: primary, entered: make(chan struct{}, 1), release: make(chan struct{})}
	coordinator.registry.providers["cursor"] = blocked
	resultB := make(chan Result, 1)
	go func() { resultB <- coordinator.Run(context.Background(), request) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("B did not reach binding barrier")
	}

	indeterminateProvider := &untrustedUsageProvider{ScriptedProvider: testkit.NewScriptedProvider(primary.Identity), usage: 1000}
	indeterminateProvider.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: plannerArtifact()})
	registry := NewRegistry()
	if err := registry.Register(context.Background(), indeterminateProvider); err != nil {
		t.Fatal(err)
	}
	coordinatorA, err := New(registry, map[Role]Route{RolePlanner: {Primary: indeterminateProvider.Name(), Capacity: 1}}, database, nil, supervisor)
	if err != nil {
		t.Fatal(err)
	}
	resultA := coordinatorA.Run(context.Background(), request)
	if resultA.Code != ResultIndeterminate || len(resultA.Attempts) != 1 {
		t.Fatalf("A indeterminate result=%+v", resultA)
	}
	close(blocked.release)
	result := <-resultB
	if result.Code != ResultIndeterminate || !result.NeedsOperator || len(result.Attempts) != 0 || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("B atomic admission result=%+v", result)
	}
	if blocked.InvocationCount() != 0 || len(primary.CallsSnapshot()) != 0 {
		t.Fatalf("B launched/parsing after indeterminate admission invocations=%d calls=%v", blocked.InvocationCount(), primary.CallsSnapshot())
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].Outcome != "result_indeterminate" {
		t.Fatalf("atomic admission attempts=%+v err=%v", attempts, err)
	}
}

func seedInvalidProviderAttempt(t *testing.T, database *store.Store, request Request, primary *testkit.ScriptedProvider, supervisor *testkit.Supervisor) (store.ProviderAttemptClaim, contracts.RuntimeBinding) {
	t.Helper()
	binding, err := primary.Binding(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	claimInput := request.Input
	claimInput.Provider, claimInput.AuthMode = binding.Identity, binding.AuthMode
	claimInput.LeaderEpoch, claimInput.RunnerEpoch, claimInput.ExpectedVersion = request.Fence.LeaderEpoch, request.Fence.RunnerEpoch, request.ExpectedVersion
	claim, err := database.BeginProviderAttempt(context.Background(), store.ProviderAttemptRequest{
		Ref: request.Input.Ticket, ExpectedVersion: request.ExpectedVersion, Fence: request.Fence,
		Phase: request.Input.Phase, Role: string(request.Role), Binding: binding,
		ConfigDigest: request.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
		Repository: request.Input.Repository, Worktree: request.Input.Worktree,
		WorktreeIdentity: request.Input.WorktreeIdentity, BaseSHA: request.Input.BaseSHA,
		SupervisorKey: supervisor.PublicKey(), Input: claimInput,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecordProviderLaunch(context.Background(), claim, contracts.ProviderLaunch{PID: 101, PGID: 101, BootIdentity: "boot", ProcessStartIdentity: "start", Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	drain, err := supervisor.Drain(context.Background(), drainRequest(claim))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.FinishProviderAttempt(context.Background(), claim, drain, request.ExpectedVersion, request.Fence, "failed", "invalid_artifact", 0, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return claim, binding
}

func recoverProviderRequest(t *testing.T, database *store.Store, request Request) Request {
	t.Helper()
	leader, err := database.AcquireLeader(context.Background(), request.Input.Ticket.Channel, "provider-repair-restart")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.FenceRecoveredRunners(context.Background(), request.Input.Ticket.Channel, leader); err != nil {
		t.Fatal(err)
	}
	ticket, err := database.Ticket(context.Background(), request.Input.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	request.ExpectedVersion = ticket.Version
	request.Fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	return request
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

func TestPrePublishingReadinessRequiresAllRoutesAndNoFatalLatch(t *testing.T) {
	_, _, coordinator, _, _ := newCoordinatorFixture(t, testkit.NewSupervisor())
	if err := coordinator.ReadyForPrePublishing(); !errors.Is(err, ErrPrePublishingNotReady) {
		t.Fatalf("incomplete route set readiness=%v", err)
	}
	coordinator.routes[RoleBuilder] = Route{Primary: "cursor", Fallback: "claude"}
	coordinator.routes[RoleReviewer] = Route{Primary: "claude", Fallback: "cursor"}
	if err := coordinator.ReadyForPrePublishing(); err != nil {
		t.Fatalf("complete route set readiness=%v", err)
	}
	coordinator.markPersistenceFailure(errors.New("durable result uncertain"))
	if err := coordinator.ReadyForPrePublishing(); !errors.Is(err, ErrPersistenceFatal) {
		t.Fatalf("fatal coordinator readiness=%v", err)
	}
}

type blockingBindingProvider struct {
	*testkit.ScriptedProvider
	entered     chan struct{}
	release     chan struct{}
	mu          sync.Mutex
	invocations int
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

func (p *blockingBindingProvider) Invocation(ctx context.Context, input contracts.PhaseInput) (contracts.Invocation, error) {
	p.mu.Lock()
	p.invocations++
	p.mu.Unlock()
	return p.ScriptedProvider.Invocation(ctx, input)
}

func (p *blockingBindingProvider) InvocationCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.invocations
}

type countingSupervisor struct {
	*testkit.Supervisor
	mu   sync.Mutex
	runs int
}

func (s *countingSupervisor) Run(ctx context.Context, request contracts.DrainRequest, invocation contracts.Invocation, input contracts.PhaseInput) (contracts.CommandResult, error) {
	s.mu.Lock()
	s.runs++
	s.mu.Unlock()
	return s.Supervisor.Run(ctx, request, invocation, input)
}

func (s *countingSupervisor) RunCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runs
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

// TestCoordinatorReviewerBeginRaceReusesCompletionWithoutSecondLaunch covers
// the race that matters to the verification worker: a second runner has
// already observed that CurrentVerification is absent, but has not yet opened
// Store admission when the first reviewer completes.  The binding barrier is
// deliberately after Coordinator's optimistic reuse read and before Begin.
func TestCoordinatorReviewerBeginRaceReusesCompletionWithoutSecondLaunch(t *testing.T) {
	ctx := context.Background()
	supervisorB := &countingSupervisor{Supervisor: testkit.NewSupervisor()}
	database, planning, coordinator, ref, primary := newCoordinatorFixture(t, supervisorB)
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
	planningResult := coordinator.Run(ctx, planning)
	if planningResult.Code != Completed {
		t.Fatalf("planning=%+v", planningResult)
	}
	_, planner, err := database.LoadCurrentProviderAttemptResult(ctx, planningResult.ProviderResult, planning.ExpectedVersion, planning.Fence)
	if err != nil || planner.Planner == nil {
		t.Fatalf("planner result=%+v parsed=%+v err=%v", planningResult, planner, err)
	}
	if _, err := database.RecordPlan(ctx, store.PlanArtifact{Ref: ref, ExpectedVersion: planning.ExpectedVersion, Fence: planning.Fence, Document: store.PlanDocument{
		Planner: planner.Planner, ProviderResult: &planningResult.ProviderResult,
		Acceptance: planner.Planner.Acceptance, ProofKind: string(planner.Planner.Proof.Kind), Paths: planner.Planner.Paths, Commands: planner.Planner.Commands, Risks: planner.Planner.Risks,
	}}); err != nil {
		t.Fatalf("record plan: %v", err)
	}
	if _, err := database.TransitionPlan(ctx, store.Transition{Ref: ref, ExpectedVersion: planning.ExpectedVersion, Fence: planning.Fence, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass"}); err != nil {
		t.Fatalf("transition plan: %v", err)
	}
	verifying, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	reviewer := planning
	reviewer.Role = RoleReviewer
	reviewer.ExpectedVersion = verifying.Version
	reviewer.Fence = domain.Fence{LeaderEpoch: planning.Fence.LeaderEpoch, RunnerEpoch: verifying.RunnerEpoch}
	reviewer.Input.Phase = domain.PhaseVerification
	reviewer.Input.Prompt = "review"
	// plannerArtifact owns x. The verification artifact must remain under the
	// same Store-derived boundary; an old fixture used internal/... here and
	// therefore correctly failed ValidateMutationPaths.
	reviewer.Input.AllowedPaths = append([]string(nil), planner.Planner.Paths...)
	reviewer.Validation = phaseartifact.Validation{TicketType: verifying.Type, AcceptanceDigest: planner.Digest}

	plain := testkit.NewScriptedProvider(id("claude", "claude-family"))
	plain.Add(domain.PhaseVerification, testkit.ProviderStep{Artifact: reviewerArtifact(planner.Digest, "x/reviewer_test.go"), ChangedFiles: nil})
	registry := NewRegistry()
	if err := registry.Register(ctx, plain); err != nil {
		t.Fatal(err)
	}
	a, err := New(registry, map[Role]Route{RoleReviewer: {Primary: "claude"}}, database, nil, testkit.NewSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	// The selected reviewer qualification is claude in newCoordinatorFixture.
	// Share its immutable binding with B while counting only B's potential
	// external execution.
	base, ok := coordinator.registry.providers["claude"].(*testkit.ScriptedProvider)
	if !ok {
		t.Fatal("fixture reviewer provider changed type")
	}
	blocked := &blockingBindingProvider{ScriptedProvider: base, entered: make(chan struct{}, 1), release: make(chan struct{})}
	coordinator.registry.providers["claude"] = blocked
	coordinator.routes[RoleReviewer] = Route{Primary: "claude", Capacity: 1}
	runsBeforeB := supervisorB.RunCount()

	resultB := make(chan Result, 1)
	go func() { resultB <- coordinator.Run(ctx, reviewer) }()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("B did not reach binding barrier")
	}
	if got, err := database.CurrentVerification(ctx, ref); err == nil {
		t.Fatalf("verification unexpectedly exists before A completion: %+v", got)
	}
	resultA := a.Run(ctx, reviewer)
	if resultA.Code != Completed {
		t.Fatalf("A=%+v", resultA)
	}
	close(blocked.release)
	result := <-resultB
	if result.Code != Completed || result.ProviderResult != resultA.ProviderResult {
		t.Fatalf("B=%+v A=%+v", result, resultA)
	}
	if calls := blocked.CallsSnapshot(); len(calls) != 0 {
		t.Fatalf("B parsed/launched provider despite Store reuse: %v", calls)
	}
	if blocked.InvocationCount() != 0 || supervisorB.RunCount() != runsBeforeB {
		t.Fatalf("B invocation=%d supervisor runs=%d before=%d", blocked.InvocationCount(), supervisorB.RunCount(), runsBeforeB)
	}
	attempts, err := database.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 2 { // planner + exactly one reviewer
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if attempts[1].Phase != domain.PhaseVerification || attempts[1].Role != string(RoleReviewer) || attempts[1].ID != resultA.ProviderResult.AttemptID {
		t.Fatalf("reviewer attempt=%+v result=%+v", attempts[1], resultA)
	}
}

func TestFallbackCompletionExposesFallbackAttemptKey(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.InvocationErr = errors.New("primary provider unavailable before launch")
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

func TestFallbackInvalidArtifactExhaustionIsStableAcrossRestart(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.InvocationErr = errors.New("primary provider unavailable before launch")
	fallback := &aliasedProvider{ScriptedProvider: testkit.NewScriptedProvider(id("cursor", "cursor-family"))}
	fallback.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: []byte(`{"schema":"not-a-planner"}`)})
	coordinator.registry.providers["claude"] = fallback

	first := coordinator.Run(context.Background(), request)
	if first.Code != AttemptExhausted || !first.NeedsOperator || len(first.Attempts) != 2 {
		t.Fatalf("in-process exhaustion=%+v", first)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 || attempts[0].Outcome != "invocation_failed" || attempts[1].Outcome != "invalid_artifact" {
		t.Fatalf("durable attempts=%+v err=%v", attempts, err)
	}
	fallbackCalls := len(fallback.CallsSnapshot())

	restarted := coordinator.Run(context.Background(), request)
	if restarted.Code != AttemptExhausted || !restarted.NeedsOperator || len(restarted.Attempts) != 0 {
		t.Fatalf("restart exhaustion=%+v", restarted)
	}
	if len(fallback.CallsSnapshot()) != fallbackCalls {
		t.Fatalf("fallback relaunched after durable exhaustion: before=%d after=%d", fallbackCalls, len(fallback.CallsSnapshot()))
	}
	if attempts, err = database.ProviderAttempts(context.Background(), ref); err != nil || len(attempts) != 2 {
		t.Fatalf("restart changed durable attempts=%+v err=%v", attempts, err)
	}
}

type aliasedProvider struct{ *testkit.ScriptedProvider }

func (p *aliasedProvider) Name() string { return "claude" }

type untrustedUsageProvider struct {
	*testkit.ScriptedProvider
	usage int64
}

func (p *untrustedUsageProvider) Parse(ctx context.Context, input contracts.PhaseInput, result contracts.CommandResult) (contracts.PhaseResult, error) {
	raw, err := p.ScriptedProvider.Parse(ctx, input, result)
	raw.UsageTrusted = false
	raw.UsageUnits = p.usage
	return raw, err
}

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

func TestControlInvalidationAfterDrainCancelsOldAttemptWithoutFatalLatch(t *testing.T) {
	supervisor := &faultAfterDrainSupervisor{Supervisor: testkit.NewSupervisor()}
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, supervisor)
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
	var control store.TransitionResult
	var controlErr error
	supervisor.onDrain = func() {
		control, controlErr = database.TransitionAndInvalidateRunner(context.Background(), store.Transition{
			Ref: ref, ExpectedVersion: request.ExpectedVersion, From: domain.StatePlanning, To: domain.StateStopping,
			ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: request.Fence, EventPayload: `{"intent":"take"}`,
		})
	}

	result := coordinator.Run(context.Background(), request)
	if controlErr != nil {
		t.Fatal(controlErr)
	}
	if result.Code != Canceled || !result.NeedsOperator || result.PersistenceFailure || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("revoked completion result=%+v", result)
	}
	if len(result.Attempts) != 1 || result.Attempts[0].ErrorCode != "provider_control_revoked" || result.Attempts[0].UsageUnits != 0 {
		t.Fatalf("revoked completion receipt=%+v", result.Attempts)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "cancelled" || attempts[0].Outcome != "cancelled" || attempts[0].UsageUnits != 0 {
		t.Fatalf("retired attempts=%+v err=%v", attempts, err)
	}
	active, err := database.ActiveProviderAttempts(context.Background(), ref.Channel)
	if err != nil || len(active) != 0 {
		t.Fatalf("active attempts=%+v err=%v", active, err)
	}
	leases, err := database.Leases(context.Background(), ref.Channel)
	if err != nil || len(leases) != 0 {
		t.Fatalf("provider leases=%+v err=%v", leases, err)
	}
	proof, err := database.ControlProof(context.Background(), ref)
	if err != nil || !proof.Drained() {
		t.Fatalf("control proof=%+v err=%v", proof, err)
	}
	stopping, err := database.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteControlTransition(context.Background(), store.Transition{
		Ref: ref, ExpectedVersion: control.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	// The coordinator did not latch the expected old-fence refusal as a
	// persistence fault. A later current-fence run remains admissible once the
	// runtime control boundary is explicitly rearmed.
	if err := coordinator.persistenceFailure(); err != nil {
		t.Fatalf("control retirement latched coordinator: %v", err)
	}
}

func TestCanceledResultDoesNotExposeProviderResultKey(t *testing.T) {
	database, request, coordinator, ref, _ := newCoordinatorFixture(t, testkit.NewSupervisor())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	result := coordinator.Run(ctx, request)
	if result.Code != Canceled || !result.NeedsOperator || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
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

func TestUntrustedUsageCannotChargeOrExhaustTicketBudget(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact()}}
	coordinator.registry.providers["cursor"] = &untrustedUsageProvider{ScriptedProvider: primary, usage: 10_000}
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Capacity: 1}

	result := coordinator.Run(context.Background(), request)
	if result.Code != ResultIndeterminate || result.CostUsed != 0 || result.ProviderResult != (store.ProviderAttemptResultKey{}) || len(result.Attempts) != 1 || result.Attempts[0].UsageUnits != 0 {
		t.Fatalf("untrusted usage result=%+v", result)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].UsageUnits != 0 || attempts[0].Outcome != "result_indeterminate" {
		t.Fatalf("untrusted usage attempts=%+v err=%v", attempts, err)
	}
	ticket, err := database.Ticket(context.Background(), ref)
	if err != nil || ticket.State != domain.StatePlanning || ticket.BlockedCode != "" {
		t.Fatalf("untrusted usage blocked ticket=%+v err=%v", ticket, err)
	}
}

type commandErrorSupervisor struct {
	*testkit.Supervisor
	runs, drains int
}

func (s *commandErrorSupervisor) Run(context.Context, contracts.DrainRequest, contracts.Invocation, contracts.PhaseInput) (contracts.CommandResult, error) {
	s.runs++
	return contracts.CommandResult{}, errors.New("supervisor command observation is ambiguous")
}

func (s *commandErrorSupervisor) Drain(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, error) {
	s.drains++
	return s.Supervisor.Drain(ctx, request)
}

func TestSupervisorCommandAmbiguityIsIndeterminateAndStableAcrossReplay(t *testing.T) {
	supervisor := &commandErrorSupervisor{Supervisor: testkit.NewSupervisor()}
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, supervisor)
	result := coordinator.Run(context.Background(), request)
	if result.Code != ResultIndeterminate || !result.NeedsOperator || len(result.Attempts) != 1 || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("ambiguous command result=%+v", result)
	}
	if supervisor.runs != 1 || supervisor.drains != 1 || len(primary.CallsSnapshot()) != 0 {
		t.Fatalf("ambiguous command launch/drain counts runs=%d drains=%d provider=%v", supervisor.runs, supervisor.drains, primary.CallsSnapshot())
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "failed" || attempts[0].Outcome != "result_indeterminate" {
		t.Fatalf("ambiguous command durable attempts=%+v err=%v", attempts, err)
	}
	replay := coordinator.Run(context.Background(), request)
	if replay.Code != ResultIndeterminate || len(replay.Attempts) != 0 || supervisor.runs != 1 || supervisor.drains != 1 {
		t.Fatalf("ambiguous command replay=%+v runs=%d drains=%d", replay, supervisor.runs, supervisor.drains)
	}
}

type bindingOverrideProvider struct {
	contracts.Provider
	binding contracts.RuntimeBinding
}

func (p *bindingOverrideProvider) Binding(context.Context) (contracts.RuntimeBinding, error) {
	return p.binding, nil
}

func TestExactCostCeilingConsumesCompletedResultBeforeBlockingLaterPhase(t *testing.T) {
	_, request, coordinator, _, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	primary.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: plannerArtifact(), UsageUnits: 100}}

	result := coordinator.Run(context.Background(), request)
	if result.Code != Completed || result.ProviderResult.AttemptID <= 0 || result.CostUsed != 100 {
		t.Fatalf("exact-ceiling result=%+v", result)
	}
}

type providerSequenceClock struct {
	values []time.Time
	next   int
}

func (clock *providerSequenceClock) Now() time.Time {
	if len(clock.values) == 0 {
		return time.Time{}
	}
	index := clock.next
	if index >= len(clock.values) {
		index = len(clock.values) - 1
	} else {
		clock.next++
	}
	return clock.values[index]
}

func TestCoordinatorMapsStoreDeadlineRaceToTicketBudgetExhausted(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	ticket, err := database.Ticket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Capacity: 1}
	coordinator.clock = &providerSequenceClock{values: []time.Time{
		ticket.CreatedAt.Add(ticket.MaxDuration - time.Second),
		ticket.CreatedAt.Add(ticket.MaxDuration + time.Nanosecond),
	}}
	callsBefore := len(primary.CallsSnapshot())

	result := coordinator.Run(context.Background(), request)
	if result.Code != BudgetExhausted || !result.NeedsOperator || result.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("deadline-race result=%+v", result)
	}
	if callsAfter := len(primary.CallsSnapshot()); callsAfter != callsBefore {
		t.Fatalf("provider invoked across deadline race: before=%d after=%d", callsBefore, callsAfter)
	}
}

func TestCoordinatorReportsAttemptExhaustedBeforeThirdProviderInvocation(t *testing.T) {
	database, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	// Consume the two bounded initial attempts with one explicit route. The
	// next coordinator pass must surface Store's admission refusal without
	// launching the adapter again.
	coordinator.routes[RolePlanner] = Route{Primary: "cursor", Capacity: 1}
	primary.InvocationErr = errors.New("definite adapter failure before launch")
	first := coordinator.Run(context.Background(), request)
	if first.Code != Failed || len(first.Attempts) != 1 {
		t.Fatalf("first bounded failure=%+v", first)
	}
	secondFailure := coordinator.Run(context.Background(), request)
	if secondFailure.Code != Failed || len(secondFailure.Attempts) != 1 {
		t.Fatalf("second bounded failure=%+v", secondFailure)
	}
	callsBefore := len(primary.CallsSnapshot())
	exhausted := coordinator.Run(context.Background(), request)
	if exhausted.Code != AttemptExhausted || !exhausted.NeedsOperator || len(exhausted.Attempts) != 0 || exhausted.ProviderResult != (store.ProviderAttemptResultKey{}) {
		t.Fatalf("immediate begin exhaustion=%+v", exhausted)
	}
	if callsAfter := len(primary.CallsSnapshot()); callsAfter != callsBefore {
		t.Fatalf("provider invoked after Store admission refusal: before=%d after=%d", callsBefore, callsAfter)
	}
	attempts, err := database.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("durable attempts=%+v err=%v", attempts, err)
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

func reviewerArtifact(acceptanceDigest, ownedFile string) []byte {
	return []byte(fmt.Sprintf(`{"schema":"sf.verification/v1","acceptance_digest":%q,"proof_kind":"acceptance","owned_files":[%q],"command":["go","test","./..."],"prebuild_outcome":"red","evidence_digest":"%s"}`,
		acceptanceDigest, ownedFile, strings.Repeat("e", 64)))
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
	repairLaunch := launch
	repairLaunch.RequestDigest = ""
	repairLaunch.Repair = &contracts.ProviderRepairContext{PriorAttempt: 1, PriorRequestDigest: strings.Repeat("a", 64)}
	_, repairDigest, err := contracts.CanonicalPhaseInput(repairLaunch)
	if err != nil {
		t.Fatal(err)
	}
	repairLaunch.RequestDigest = repairDigest
	repairClaim := claim
	repairClaim.Input, repairClaim.RequestDigest = repairLaunch, repairDigest
	repairInput := request.Input
	if !bindClaimToInput(&repairInput, repairClaim, request, identity) || repairInput.Repair == nil || repairInput.Repair.PriorAttempt != 1 {
		t.Fatalf("Store repair claim was not bound: input=%+v", repairInput)
	}
	forgedRepair := request.Input
	forgedRepair.Repair = repairLaunch.Repair
	if bindClaimToInput(&forgedRepair, repairClaim, request, identity) {
		t.Fatal("caller-supplied repair context was accepted")
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
