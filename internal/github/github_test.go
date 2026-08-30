package github

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

type verifierFunc func(context.Context, contracts.RepositoryIdentity, string, string, string) (contracts.ProtectedBranchObservation, error)

type mutationGuardFunc func(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error)

// supervisedFakeRunner is intentionally a real command runner rather than a
// canned Client method: the integration test below exercises the same bounded
// argv/env/process seam that production composition uses. Its cleanup proof
// models a supervisor that has drained every fake-gh child before the next
// mutation handoff.
type supervisedFakeRunner struct{}

func (supervisedFakeRunner) Run(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = env
	return command.Output()
}
func (supervisedFakeRunner) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Drained: true}, nil
}

type contradictoryCleanupRunner struct{}

func (contradictoryCleanupRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	return []byte(`{}`), nil
}
func (contradictoryCleanupRunner) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Drained: true, Quarantined: true}, nil
}

type intentRecorderFunc func(context.Context, domain.MergeIntent) error

func (f intentRecorderFunc) RecordMergeIntent(ctx context.Context, intent domain.MergeIntent) error {
	return f(ctx, intent)
}

type cleanupQuarantinerFunc func(context.Context) error

func (f cleanupQuarantinerFunc) QuarantineExternalMutations(ctx context.Context) error { return f(ctx) }
func (f cleanupQuarantinerFunc) ExternalMutationsQuarantined(context.Context) (bool, error) {
	return false, nil
}

func (f mutationGuardFunc) RunExternalMutation(ctx context.Context, claim domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
	return f(ctx, claim, start)
}

func (f verifierFunc) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
	return f(ctx, repository, baseRef, mergeCommit, originalBaseOID)
}

func fixture(t *testing.T) (*Client, *testkit.FakeGH, contracts.PullRequestIdentity) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, "fake-gh.json")
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	fake, err := testkit.NewFakeGH(state, repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "fake-gh")
	command := exec.Command("go", "build", "-o", binary, "./cmd/fake-gh")
	command.Dir = filepath.Join("..", "..")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build fake-gh: %v\n%s", err, output)
	}
	client := &Client{binaryPath: binary, home: filepath.Join(root, "home"), configDir: filepath.Join(root, "gh-config"), env: []string{"SF_FAKE_GH_STATE=" + state}, runner: commandRunnerFunc(runBounded), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }, mutationGuard: fixtureGuard(), mergeIntents: intentRecorderFunc(func(context.Context, domain.MergeIntent) error { return nil }), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), verifyProtectedBranch: verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	})}
	identity := contracts.PullRequestIdentity{Repository: repository, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	return client, fake, identity
}

func testClaim(kind string, identity contracts.PullRequestIdentity, values ...string) domain.ExternalEffectClaim {
	if kind == "merge" && identity.BaseOID == "" {
		identity.BaseOID = strings.Repeat("c", 40)
	}
	if kind == "merge" && len(values) == 2 {
		values = append(values, strings.Repeat("c", 40), strings.Repeat("c", 40), strings.Repeat("c", 40), strings.Repeat("c", 40))
	}
	return domain.ExternalEffectClaim{
		SemanticKey:   "test-" + kind,
		Ref:           domain.TicketRef{Channel: "dev", Project: "example", Ticket: "SF-44"},
		Kind:          kind,
		RequestDigest: requestDigest(kind, identity, values...),
		TicketVersion: 1,
		LeaderEpoch:   1,
		RunnerEpoch:   1,
		ClaimEpoch:    1,
	}
}

func testAuthorization(identity contracts.PullRequestIdentity) domain.MergeAuthorization {
	base := identity.BaseOID
	if base == "" {
		base = strings.Repeat("c", 40)
	}
	return domain.MergeAuthorization{ReviewedHead: identity.HeadOID, CurrentHead: identity.HeadOID, ReviewedBaseSHA: base, CurrentBaseSHA: base, ReviewedBaseHeadOID: base, CurrentBaseHeadOID: base, Approved: true, GatesGreen: true}
}

func createDraft(t *testing.T, client *Client, identity contracts.PullRequestIdentity, title, body string) PRMatch {
	t.Helper()
	claim := testClaim("draft_pr", identity, title, body)
	created, err := client.CreateDraftPullRequest(context.Background(), claim, identity, title, body)
	if err != nil {
		t.Fatal(err)
	}
	match, err := client.Observe(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	return match
}

func TestStoreClaimGuardAndClientComposeExactHeadFlow(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	database, err := store.Open(ctx, filepath.Join(root, "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "example", Ticket: "SF-44"}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "example", Path: root, BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "integration", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "github-integration")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/example/SF-44/publish", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	state := filepath.Join(root, "fake-gh.json")
	fake, err := testkit.NewFakeGH(state, repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "fake-gh")
	build := exec.Command("go", "build", "-o", binary, "./cmd/fake-gh")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake-gh: %v\n%s", err, output)
	}
	verified := false
	client, err := NewStoreClient(binary, filepath.Join(root, "home"), filepath.Join(root, "gh-config"), supervisedFakeRunner{}, database, verifierFunc(func(_ context.Context, gotRepository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		verified = gotRepository == repository && baseRef == "main" && originalBaseOID == strings.Repeat("c", 40)
		return contracts.ProtectedBranchObservation{Repository: gotRepository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	client.env = []string{"SF_FAKE_GH_STATE=" + state}
	identity := contracts.PullRequestIdentity{Repository: repository, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	claim := func(kind, digest string) domain.ExternalEffectClaim {
		fence := store.EffectFence{SemanticKey: "integration/" + kind, Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
		if _, err := database.PlanEffect(ctx, store.EffectPlan{SemanticKey: fence.SemanticKey, Ref: ref, Kind: kind, TicketVersion: fence.TicketVersion, Fence: fence.Fence, RequestDigest: digest}); err != nil {
			t.Fatal(err)
		}
		claimed, err := database.ClaimEffect(ctx, fence)
		if err != nil || !claimed.Claimed {
			t.Fatalf("claim %s: %+v err=%v", kind, claimed, err)
		}
		return claimed.ExternalClaim()
	}
	created, err := client.CreateDraftPullRequest(ctx, claim("draft_pr", requestDigest("draft_pr", identity, "title", "body")), identity, "title", "body")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MarkReady(ctx, claim("pr_ready", requestDigest("pr_ready", created)), created); err != nil {
		t.Fatal(err)
	}
	authorization := testAuthorization(created)
	mergeDigest := requestDigest("merge", created, created.HeadOID, "squash", authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID)
	mergeClaim := claim("merge", mergeDigest)
	if err := client.MergeExactHead(ctx, mergeClaim, created, created.HeadOID, "squash", authorization); err != nil {
		t.Fatalf("guarded merge=%v", err)
	}
	intent, found, err := database.MergeIntent(ctx, mergeClaim.SemanticKey)
	if err != nil || !found || intent.OriginalBaseOID != created.BaseOID || !intent.StrictStatusChecks || intent.ProtectionRuleID == "" || fake.MutationCount("pr_merge") != 1 || !verified {
		t.Fatalf("strict guarded intent=%+v found=%v err=%v verified=%v merge mutations=%d", intent, found, err, verified, fake.MutationCount("pr_merge"))
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(ctx, filepath.Join(root, "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	persisted, found, err := restarted.MergeIntent(ctx, mergeClaim.SemanticKey)
	if err != nil || !found || persisted.OriginalBaseOID != created.BaseOID || persisted.ProtectionRuleID != intent.ProtectionRuleID || !persisted.StrictStatusChecks {
		t.Fatalf("restart merge intent=%+v found=%v err=%v", persisted, found, err)
	}
}

func TestContractMutationRequiresClaimValidator(t *testing.T) {
	client, _, identity := fixture(t)
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); err != nil {
		t.Fatalf("validated claim=%v", err)
	}
	client.validateClaimFn = nil
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("missing validator=%v", err)
	}
}

func TestNewClientRejectsMissingAuthoritiesAndLiteralCannotRun(t *testing.T) {
	if _, err := NewClient("/bin/echo", t.TempDir(), t.TempDir(), commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return nil, nil
	}), func(context.Context, domain.ExternalEffectClaim) error { return nil }, mutationGuardFunc(func(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error) {
		return nil, nil
	}), verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	}), intentRecorderFunc(func(context.Context, domain.MergeIntent) error { return nil }), cleanupQuarantinerFunc(func(context.Context) error { return nil })); err != nil {
		t.Fatalf("valid client rejected: %v", err)
	}
	client := Client{}
	if err := client.AuthStatus(context.Background()); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("literal err=%v", err)
	}
}

func TestAuthStatusAcceptsOfficialHostsStateShape(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		want := []string{"auth", "status", "--json", "hosts"}
		if !reflect.DeepEqual(args, want) {
			return nil, errors.New("unexpected auth argv")
		}
		return []byte(`{"hosts":{"github.com":[{"state":"success","active":true,"host":"github.com","login":"sf-test","tokenSource":"keyring","scopes":"repo","gitProtocol":"https"}]}}`), nil
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), mutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }}
	if err := client.AuthStatus(context.Background()); err != nil {
		t.Fatalf("official auth status shape=%v", err)
	}
}

func TestMergeQueueGraphQLFailsClosedBeforeMerge(t *testing.T) {
	client, _, identity := fixture(t)
	identity.Number = 7
	called := false
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		called = true
		if len(args) < 4 || args[0] != "api" || args[1] != "--hostname" || args[2] != "github.com" || args[3] != "graphql" {
			return nil, errors.New("wrong queue argv")
		}
		return []byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":{"position":1}}}}}`), nil
	})
	queued, err := client.mergeQueued(context.Background(), identity)
	if err != nil || !queued || !called {
		t.Fatalf("queue observation queued=%v called=%v err=%v", queued, called, err)
	}
}

func TestPreflightCreateLostResponseAndExactAdoption(t *testing.T) {
	client, fake, identity := fixture(t)
	principal, err := client.Preflight(context.Background(), identity.Repository)
	if err != nil || principal.Login != "sf-test" {
		t.Fatalf("preflight=%+v err=%v", principal, err)
	}
	claim := testClaim("draft_pr", identity, "title", "<!-- sf:owned -->")
	if err := fake.SetResponse("pr_create", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	created, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "<!-- sf:owned -->")
	pr, observeErr := client.Observe(context.Background(), created)
	if err != nil || observeErr != nil || pr.Identity.Number != 1 || !pr.Draft {
		t.Fatalf("create/adopt=%+v err=%v", pr, err)
	}
	if fake.MutationCount("pr_create") != 1 {
		t.Fatalf("create mutations=%d", fake.MutationCount("pr_create"))
	}
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "<!-- sf:owned -->"); err != nil {
		t.Fatalf("idempotent adopt=%v", err)
	}
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: pr.Identity, Draft: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Observe(context.Background(), identity); !errors.Is(err, ErrAmbiguousPR) {
		t.Fatalf("ambiguous=%v", err)
	}
}

func TestCreateUncertainNeverAttemptsNumberOnlyOrphanClose(t *testing.T) {
	client, _, identity := fixture(t)
	var closed bool
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		switch {
		case len(args) >= 2 && args[0] == "pr" && args[1] == "list":
			return []byte("[]"), nil
		case len(args) >= 2 && args[0] == "pr" && args[1] == "create":
			return []byte("https://github.com/example/app/pull/999\n"), nil
		case len(args) >= 2 && args[0] == "pr" && args[1] == "close":
			closed = true
			return nil, nil
		default:
			return nil, errors.New("unexpected command")
		}
	})
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("uncertain create err=%v", err)
	}
	if closed {
		t.Fatal("uncertain create attempted number-only orphan close")
	}
}

func TestCreateFinalHandoffRefusesIfPRAppearsBeforeLaunch(t *testing.T) {
	client, _, identity := fixture(t)
	created := false
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "pr" && args[1] == "list" {
			return []byte("[]"), nil
		}
		if len(args) >= 2 && args[0] == "api" {
			return []byte(`{"object":{"sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}`), nil
		}
		if len(args) >= 2 && args[0] == "pr" && args[1] == "create" {
			created = true
		}
		return []byte("{}"), nil
	})
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("handoff race err=%v", err)
	}
	if created {
		t.Fatal("create launched after exact in-handoff identity appeared")
	}
}

func TestCleanupUncertaintyNeverBecomesMutationSuccess(t *testing.T) {
	client, _, identity := fixture(t)
	uncertainGuard := mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		_, _ = start(ctx)
		return nil, ErrProcessCleanup
	})

	client.mutationGuard = uncertainGuard
	createClaim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), createClaim, identity, "title", "body"); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("create cleanup uncertainty=%v", err)
	}

	client.mutationGuard = fixtureGuard()
	created := createDraft(t, client, identity, "before", "before body")
	client.mutationGuard = uncertainGuard
	editClaim := testClaim("pr_edit", created.Identity, "after", "after body")
	if err := client.UpdatePullRequest(context.Background(), editClaim, created.Identity, "after", "after body"); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("update cleanup uncertainty=%v", err)
	}

	client.mutationGuard = fixtureGuard()
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", created.Identity), created.Identity); err != nil {
		t.Fatal(err)
	}
	readyClaim := testClaim("pr_ready", created.Identity)
	client.mutationGuard = uncertainGuard
	if err := client.MarkReady(context.Background(), readyClaim, created.Identity); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("ready cleanup uncertainty=%v", err)
	}

	client.mutationGuard = fixtureGuard()
	mergeClaim := testClaim("merge", created.Identity, created.Identity.HeadOID, "merge")
	authorization := testAuthorization(created.Identity)
	client.mutationGuard = uncertainGuard
	if err := client.MergeExactHead(context.Background(), mergeClaim, created.Identity, created.Identity.HeadOID, "merge", authorization); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("guarded merge cleanup uncertainty=%v", err)
	}
}

func TestCleanupQuarantineIsNotACompletedProof(t *testing.T) {
	if (CleanupProof{Quarantined: true}).valid() {
		t.Fatal("quarantine was accepted as successful cleanup")
	}
	if (CleanupProof{Drained: true, Quarantined: true}).valid() {
		t.Fatal("contradictory drain/quarantine proof was accepted")
	}
}

func TestContradictoryCleanupProofLatchesStoreMutationGate(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "sf.sqlite")
	database, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "cleanup", Ticket: "SF-44"}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "cleanup", Path: t.TempDir(), BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "cleanup", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "cleanup")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/cleanup/SF-44", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := store.EffectFence{SemanticKey: "cleanup-effect", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
	if _, err := database.PlanEffect(ctx, store.EffectPlan{SemanticKey: fence.SemanticKey, Ref: ref, Kind: "pr_edit", TicketVersion: fence.TicketVersion, Fence: fence.Fence, RequestDigest: "cleanup"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimEffect(ctx, fence)
	if err != nil {
		t.Fatal(err)
	}
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: contradictoryCleanupRunner{}, quarantiner: database}
	if _, err := database.ExternalMutationGuard().RunExternalMutation(ctx, claimed.ExternalClaim(), func(runCtx context.Context) ([]byte, error) {
		return client.run(runCtx, "auth", "status", "--json", "hosts")
	}); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("contradictory cleanup=%v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.ExternalMutationGuard().RunExternalMutation(ctx, claimed.ExternalClaim(), func(context.Context) ([]byte, error) {
		t.Fatal("contradictory cleanup released the mutation gate")
		return nil, nil
	}); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("contradictory cleanup was not durably latched: %v", err)
	}
}

type quarantineRunner struct{}

func (quarantineRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	return []byte("{}"), nil
}
func (quarantineRunner) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Quarantined: true}, nil
}

func TestQuarantinedRunnerBlocksGuardedMutation(t *testing.T) {
	client, _, identity := fixture(t)
	client.runner = quarantineRunner{}
	started := false
	client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		started = true
		return start(ctx)
	})
	claim := testClaim("pr_edit", identity, "title", "body")
	if _, err := client.mutateExact(context.Background(), claim, identity, "pr", "edit", "1"); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("quarantined runner err=%v", err)
	}
	if !started {
		t.Fatal("guarded callback did not run")
	}
}

func fixtureGuard() contracts.ExternalMutationGuard {
	return mutationGuardFunc(func(ctx context.Context, claim domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		if claim.Ref.Validate() != nil || claim.SemanticKey == "" || claim.Kind == "" || claim.RequestDigest == "" || claim.TicketVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ClaimEpoch == 0 {
			return nil, ErrPolicyRefusal
		}
		return start(ctx)
	})
}

func TestChecksRejectsHeadDriftBetweenCheckObservations(t *testing.T) {
	_, _, identity := fixture(t)
	identity.Number = 7
	changed := identity
	changed.HeadOID = strings.Repeat("b", 40)
	open := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
	mutated := mergeWire(changed, "OPEN", "CLEAN", nil, nil)
	pre, _ := json.Marshal([]map[string]any{open})
	post, _ := json.Marshal([]map[string]any{mutated})
	checks := `[{"name":"unit","state":"SUCCESS","workflow":"ci","link":"https://example.test/1","bucket":"test"}]`
	listCalls := 0
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if args[0] == "pr" && args[1] == "list" {
			listCalls++
			if listCalls == 1 {
				return pre, nil
			}
			return post, nil
		}
		if args[0] == "pr" && args[1] == "checks" {
			return []byte(checks), nil
		}
		return nil, errors.New("unexpected command")
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil })}
	if _, err := client.RequiredChecks(context.Background(), identity); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("head drift checks=%v", err)
	}
}

func TestChecksMergeAndApprovalPolicies(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "1", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	checks, err := client.WaitChecks(context.Background(), pr.Identity, []CheckIdentity{{Name: "unit", ExternalID: "1"}}, time.Millisecond, time.Millisecond)
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "wrong", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WaitChecks(context.Background(), pr.Identity, []CheckIdentity{{Name: "unit", ExternalID: "1"}}, time.Millisecond, time.Millisecond); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("strict checks=%v", err)
	}
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
		t.Fatal(err)
	}
	pr.Draft = false
	if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	authorization := testAuthorization(pr.Identity)
	err = client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", authorization)
	if err != nil {
		t.Fatalf("guarded merge=%v", err)
	}
	if err := (ApprovalBinding{ReviewedHead: pr.Identity.HeadOID, CurrentHead: pr.Identity.HeadOID}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ApprovalBinding{ReviewedHead: pr.Identity.HeadOID, CurrentHead: "changed"}).Validate(); !errors.Is(err, ErrApprovalInvalid) {
		t.Fatalf("approval invalidation=%v", err)
	}
}

func TestDraftAndNonOpenPRsCannotMergeOrBeAdopted(t *testing.T) {
	client, _, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "merge")
	if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "merge", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("draft merge=%v", err)
	}
	closedClient, closedFake, closedIdentity := fixture(t)
	closed := closedIdentity
	closed.Number = 1
	if err := closedFake.InjectPullRequestForTest(testkit.PullRequest{Identity: closed, Draft: true, Merged: true}); err != nil {
		t.Fatal(err)
	}
	closedClaim := testClaim("draft_pr", closedIdentity, "title", "body")
	if _, err := closedClient.CreateDraftPullRequest(context.Background(), closedClaim, closedIdentity, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("merged draft adoption=%v", err)
	}
}

func TestMarkReadyRejectsMergedPRBeforeMutation(t *testing.T) {
	client, fake, identity := fixture(t)
	identity.Number = 1
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: identity, Merged: true}); err != nil {
		t.Fatal(err)
	}
	durable := testClaim("pr_ready", identity)
	if err := client.MarkReady(context.Background(), durable, identity); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("merged ready=%v", err)
	}
	if fake.MutationCount("pr_ready") != 0 {
		t.Fatalf("merged PR was mutated")
	}
}

func TestMarkReadySynchronizeGapCompensatesChangedSource(t *testing.T) {
	client, _, identity := fixture(t)
	identity.Number = 1
	changed := identity
	changed.HeadOID = strings.Repeat("b", 40)
	oldWire, err := json.Marshal(mergeWire(identity, "OPEN", "CLEAN", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	newWireValue := mergeWire(changed, "OPEN", "CLEAN", nil, nil)
	newWireValue["body"] = ownershipMarker(identity)
	newWire, err := json.Marshal(newWireValue)
	if err != nil {
		t.Fatal(err)
	}
	restoredWireValue := mergeWire(changed, "OPEN", "CLEAN", nil, nil)
	restoredWireValue["body"] = ownershipMarker(identity)
	restoredWireValue["isDraft"] = true
	restoredWire, err := json.Marshal(restoredWireValue)
	if err != nil {
		t.Fatal(err)
	}
	phase := 0
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if len(args) < 2 || args[0] != "pr" {
			return nil, errors.New("unexpected command")
		}
		switch args[1] {
		case "list":
			if phase == 0 {
				return []byte("[" + string(oldWire) + "]"), nil
			}
			if phase == 3 {
				return []byte("[" + string(restoredWire) + "]"), nil
			}
			return []byte("[" + string(newWire) + "]"), nil
		case "view":
			if phase == 0 {
				phase = 1
				return oldWire, nil
			}
			if phase == 3 {
				return restoredWire, nil
			}
			return newWire, nil
		case "ready":
			phase = 2
			return []byte("{}"), nil
		default:
			return nil, errors.New("unexpected command")
		}
	})
	claim := testClaim("pr_ready", identity)
	if err := client.MarkReady(context.Background(), claim, identity); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("changed-head ready=%v", err)
	}
}

func TestMarkReadyFinalHandoffRejectsChangedGuardedFields(t *testing.T) {
	cases := []struct {
		name string
		wire func(contracts.PullRequestIdentity) map[string]any
	}{
		{"closed", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "CLOSED", "CLEAN", nil, nil)
		}},
		{"merged", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "MERGED", "CLEAN", "2026-01-01T00:00:00Z", nil)
		}},
		{"head-changed", func(id contracts.PullRequestIdentity) map[string]any {
			id.HeadOID = strings.Repeat("b", 40)
			return mergeWire(id, "OPEN", "CLEAN", nil, nil)
		}},
		{"base-changed", func(id contracts.PullRequestIdentity) map[string]any {
			id.BaseRef = "release"
			return mergeWire(id, "OPEN", "CLEAN", nil, nil)
		}},
		{"auto-merge", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "OPEN", "CLEAN", nil, map[string]any{"enabledAt": "now"})
		}},
		{"queued", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "OPEN", "QUEUED", nil, nil)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client, _, identity := fixture(t)
			identity.Number = 1
			initialWire := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
			initialWire["isDraft"] = true
			initial, err := json.Marshal(initialWire)
			if err != nil {
				t.Fatal(err)
			}
			changed, err := json.Marshal(test.wire(identity))
			if err != nil {
				t.Fatal(err)
			}
			readyCalls := 0
			client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
				if len(args) >= 2 && args[0] == "pr" && args[1] == "list" {
					return []byte("[" + string(initial) + "]"), nil
				}
				if len(args) >= 2 && args[0] == "pr" && args[1] == "view" {
					return changed, nil
				}
				if len(args) >= 2 && args[0] == "pr" && args[1] == "ready" {
					readyCalls++
				}
				return []byte("{}"), nil
			})
			claim := testClaim("pr_ready", identity)
			if err := client.MarkReady(context.Background(), claim, identity); !errors.Is(err, ErrPolicyRefusal) {
				t.Fatalf("changed %s accepted: %v", test.name, err)
			}
			if readyCalls != 0 {
				t.Fatalf("changed %s launched ready %d times", test.name, readyCalls)
			}
		})
	}
}

func TestMergeRequiresFreshProtectedBranchProof(t *testing.T) {
	t.Run("unavailable verifier is never success", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "merge")
		client.verifyProtectedBranch = nil
		if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
			t.Fatal(err)
		}
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "merge", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("missing proof verifier=%v", err)
		}
	})
	t.Run("mismatched proof is never success", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "merge")
		client.verifyProtectedBranch = verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
			return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
		})
		if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
			t.Fatal(err)
		}
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "merge", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("mismatched proof=%v", err)
		}
	})
}

func TestMergeCrossBindsBaseAndRefusesMovedBaseDuringGuardedHandoff(t *testing.T) {
	t.Run("split local and GitHub base witness is refused before launch", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
			t.Fatal(err)
		}
		authorization := testAuthorization(pr.Identity)
		authorization.CurrentBaseHeadOID = strings.Repeat("d", 40)
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash", authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID)
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", authorization); !errors.Is(err, ErrApprovalInvalid) {
			t.Fatalf("split base witness=%v", err)
		}
		if got := fake.MutationCount("pr_merge"); got != 0 {
			t.Fatalf("split base launched merge %d times", got)
		}
	})
	t.Run("base movement after preflight but before launch is fenced", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
			t.Fatal(err)
		}
		client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
			if err := fake.SetBaseHeadOIDForTest(strings.Repeat("d", 40)); err != nil {
				return nil, err
			}
			return start(ctx)
		})
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("moved base handoff=%v", err)
		}
		if got := fake.MutationCount("pr_merge"); got != 0 {
			t.Fatalf("moved base launched merge %d times", got)
		}
	})
}

func TestMergeRequiresStrictServerProtectionWithoutBypass(t *testing.T) {
	for _, test := range []struct {
		name   string
		strict bool
		bypass int
		admin  bool
		rules  int
	}{
		{name: "non-strict", strict: false, admin: true},
		{name: "bypass allowance", strict: true, admin: true, bypass: 1},
		{name: "admin bypass", strict: true, admin: false},
		{name: "active ruleset", strict: true, admin: true, rules: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			pr := createDraft(t, client, identity, "title", "body")
			if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
				t.Fatal(err)
			}
			if err := fake.SetProtectionWitnessForTest(test.strict, test.admin, test.bypass, test.rules); err != nil {
				t.Fatal(err)
			}
			claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
			if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("protection=%v", err)
			}
			if got := fake.MutationCount("pr_merge"); got != 0 {
				t.Fatalf("unsafe protection launched merge %d times", got)
			}
		})
	}
	t.Run("force-push bypass allowance", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
			t.Fatal(err)
		}
		if err := fake.SetBypassForcePushAllowancesForTest(1); err != nil {
			t.Fatal(err)
		}
		if err := client.MergeExactHead(context.Background(), testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash"), pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("force-push bypass=%v", err)
		}
		if got := fake.MutationCount("pr_merge"); got != 0 {
			t.Fatalf("force-push bypass launched merge %d times", got)
		}
	})
}

func TestStrictProtectionTrustsOnlyAppliedRefRule(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			if !strings.Contains(strings.Join(args, "\x00"), "ref(qualifiedName:$qualifiedRef){branchProtectionRule") {
				return nil, errors.New("did not request applied ref rule")
			}
			// The actual ref rule is weak. The query assertion above ensures this
			// response cannot be replaced by an unordered duplicate-rule scan.
			return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"applied","pattern":"main","requiresStrictStatusChecks":false,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
		}
		return []byte(`[]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); !errors.Is(err, ErrGuardedMergeUnavailable) {
		t.Fatalf("weak applied rule=%v", err)
	}
}

func TestStrictProtectionRefusesNullAppliedRule(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			return []byte(`{"data":{"repository":{"ref":null}}}`), nil
		}
		return []byte(`[]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); !errors.Is(err, ErrGuardedMergeUnavailable) {
		t.Fatalf("null applied rule=%v", err)
	}
}

func TestStrictProtectionPinsRulesRESTToGitHubDespiteDefaultHost(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"rule","pattern":"main","requiresStrictStatusChecks":true,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
		}
		// This runner models a machine whose implicit gh host is a GHE server:
		// only an explicit github.com request receives the empty GitHub ruleset.
		hostPinned := false
		for index, arg := range args {
			if arg == "--hostname" && index+1 < len(args) && args[index+1] == "github.com" {
				hostPinned = true
			}
		}
		if !hostPinned {
			return []byte(`[{"type":"ghe-rule"}]`), nil
		}
		return []byte(`[]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); err != nil {
		t.Fatalf("github-pinned rules request=%v", err)
	}
}

func TestMergeFinalHandoffRefusesChangedSafetyWitness(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testkit.FakeGH, contracts.PullRequestIdentity) error
	}{
		{"rule-removed", func(fake *testkit.FakeGH, _ contracts.PullRequestIdentity) error {
			return fake.SetProtectionWitnessForTest(false, true, 0, 0)
		}},
		{"ruleset-added", func(fake *testkit.FakeGH, _ contracts.PullRequestIdentity) error {
			return fake.SetProtectionWitnessForTest(true, true, 0, 1)
		}},
		{"head-moved", func(fake *testkit.FakeGH, identity contracts.PullRequestIdentity) error {
			return fake.SetPullRequestHeadOIDForTest(identity.Number, strings.Repeat("b", 40))
		}},
		{"queue-entered", func(fake *testkit.FakeGH, _ contracts.PullRequestIdentity) error {
			return fake.SetMergeQueuedForTest(true)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			pr := createDraft(t, client, identity, "title", "body")
			if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
				t.Fatal(err)
			}
			client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
				if err := test.mutate(fake, pr.Identity); err != nil {
					return nil, err
				}
				return start(ctx)
			})
			err := client.MergeExactHead(context.Background(), testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash"), pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity))
			if !errors.Is(err, ErrPolicyRefusal) && !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("handoff %s err=%v", test.name, err)
			}
			if got := fake.MutationCount("pr_merge"); got != 0 {
				t.Fatalf("handoff %s launched merge %d times", test.name, got)
			}
		})
	}
}

func TestCleanupQuarantineWriteFailureLatchesProcess(t *testing.T) {
	var ran int
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: contradictoryCleanupRunner{}, cleanupLatched: &atomic.Bool{}, quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return errors.New("disk unavailable") })}
	if _, err := client.run(context.Background(), "auth", "status"); !errors.Is(err, ErrCleanupQuarantineFatal) {
		t.Fatalf("write failure=%v", err)
	}
	client.runner = commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) { ran++; return nil, nil })
	if _, err := client.run(context.Background(), "auth", "status"); !errors.Is(err, ErrCleanupQuarantineFatal) || ran != 0 {
		t.Fatalf("latched process ran=%d err=%v", ran, err)
	}
}

func TestMergeLostResponseReconcilesFromOriginalBaseWitness(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
		t.Fatal(err)
	}
	if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	var original string
	client.verifyProtectedBranch = verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		original = originalBaseOID
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	})
	claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
	if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); err != nil {
		t.Fatalf("lost-response merge=%v", err)
	}
	if original != pr.Identity.BaseOID || fake.MutationCount("pr_merge") != 1 {
		t.Fatalf("lost-response original=%q mutations=%d", original, fake.MutationCount("pr_merge"))
	}
}

func TestOfficialGHArgvGolden(t *testing.T) {
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	created := identity
	created.Number = 7
	createdMap := mergeWire(created, "OPEN", "CLEAN", nil, nil)
	createdMap["isDraft"] = true
	createdWire, err := json.Marshal(createdMap)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]string
	listCalls := 0
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		got = append(got, append([]string(nil), args...))
		switch args[0] + " " + args[1] {
		case "pr list":
			listCalls++
			if listCalls <= 2 {
				return []byte("[]"), nil
			}
			return []byte("[" + string(createdWire) + "]"), nil
		case "pr create":
			return []byte("https://github.com/example/app/pull/7\n"), nil
		case "api repos/example/app/git/ref/heads/sf/dev/example/SF-44-random":
			return []byte(`{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`), nil
		default:
			return nil, errors.New("unexpected command")
		}
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), mutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }}
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields},
		{"api", "repos/example/app/git/ref/heads/sf/dev/example/SF-44-random"},
		{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields},
		{"pr", "create", "--repo", "example/app", "--head", "example:sf/dev/example/SF-44-random", "--base", "main", "--draft", "--title", "title", "--body", "body\n\n" + ownershipMarker(identity)},
		{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("official gh argv\n got: %#v\nwant: %#v", got, want)
	}
}

func TestOfficialMergeArgvGoldenAndProof(t *testing.T) {
	identity := contracts.PullRequestIdentity{Number: 7, Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	wire := mergeWire(identity, "MERGED", "CLEAN", "2026-01-01T00:00:00Z", nil)
	wire["mergeCommit"] = map[string]string{"oid": strings.Repeat("b", 40)}
	payload, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]string
	verified := false
	viewCalls := 0
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		got = append(got, append([]string(nil), args...))
		if args[0] == "pr" && args[1] == "merge" {
			return nil, nil
		}
		if args[0] == "api" {
			if strings.Contains(strings.Join(args, "\x00"), "branchProtectionRule") {
				return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"rule-main","pattern":"main","requiresStrictStatusChecks":true,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
			}
			if len(args) == 6 && args[1] == "--hostname" && args[2] == "github.com" && args[3] == "--method" && args[4] == "GET" {
				return []byte(`[]`), nil
			}
			if len(args) == 2 && args[1] == "repos/example/app/git/ref/heads/sf/dev/example/SF-44-random" {
				return []byte(`{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`), nil
			}
			if len(args) == 2 && args[1] == "repos/example/app/git/ref/heads/main" {
				return []byte(`{"object":{"sha":"cccccccccccccccccccccccccccccccccccccccc"}}`), nil
			}
			return []byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":null}}}}`), nil
		}
		if args[0] == "pr" && args[1] == "list" {
			open := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
			return json.Marshal([]map[string]any{open})
		}
		if args[0] == "pr" && args[1] == "view" {
			viewCalls++
			if viewCalls < 2 {
				open := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
				return json.Marshal(open)
			}
			return payload, nil
		}
		return nil, errors.New("unexpected command")
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), mutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }, mergeIntents: intentRecorderFunc(func(context.Context, domain.MergeIntent) error { return nil }), verifyProtectedBranch: verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		verified = repository == identity.Repository && baseRef == "main" && mergeCommit == strings.Repeat("b", 40)
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	})}
	claim := testClaim("merge", identity, identity.HeadOID, "squash")
	authorization := testAuthorization(identity)
	if err := client.MergeExactHead(context.Background(), claim, identity, identity.HeadOID, "squash", authorization); err != nil || !verified {
		t.Fatalf("guarded merge verified=%v err=%v", verified, err)
	}
	queue := []string{"api", "--hostname", "github.com", "graphql", "-f", "query=query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){mergeQueueEntry{position}}}}", "-F", "owner=example", "-F", "name=app", "-F", "number=7"}
	protection := []string{"api", "--hostname", "github.com", "graphql", "-f", "query=query($owner:String!,$name:String!,$qualifiedRef:String!){repository(owner:$owner,name:$name){ref(qualifiedName:$qualifiedRef){branchProtectionRule{id pattern requiresStrictStatusChecks isAdminEnforced bypassPullRequestAllowances(first:1){totalCount} bypassForcePushAllowances(first:1){totalCount}}}}}", "-F", "owner=example", "-F", "name=app", "-F", "qualifiedRef=refs/heads/main"}
	rules := []string{"api", "--hostname", "github.com", "--method", "GET", "repos/example/app/rules/branches/main?per_page=1&page=1"}
	view := []string{"pr", "view", "7", "--repo", "example/app", "--json", prFields}
	want := [][]string{{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields}, queue, protection, rules, view, queue, protection, rules, {"pr", "merge", "7", "--repo", "example/app", "--match-head-commit", identity.HeadOID, "--squash"}, view}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("official merge argv\n got: %#v\nwant: %#v", got, want)
	}
}

func mergeWire(identity contracts.PullRequestIdentity, state, mergeState string, mergedAt, autoMerge any) map[string]any {
	return map[string]any{
		"number": identity.Number, "title": "title", "body": ownershipMarker(identity),
		"headRepositoryOwner": map[string]string{"login": identity.HeadOwner},
		"headRepository":      map[string]string{"nameWithOwner": identity.HeadOwner + "/" + identity.HeadRepository},
		"headRefName":         identity.HeadRef, "headRefOid": identity.HeadOID, "baseRefName": identity.BaseRef, "baseRefOid": strings.Repeat("c", 40),
		"isDraft": false, "mergedAt": mergedAt, "mergeCommit": nil, "state": state, "mergeStateStatus": mergeState, "autoMergeRequest": autoMerge,
	}
}

func TestUpdateAndReadyReconcileOnlyExactObservedState(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "before", "before body")
	if err := fake.SetResponse("pr_edit", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	updateClaim := testClaim("pr_edit", pr.Identity, "after", "after body")
	if err := client.UpdatePullRequest(context.Background(), updateClaim, pr.Identity, "after", "after body"); err != nil {
		t.Fatalf("update reconciliation=%v", err)
	}
	if err := fake.SetResponse("pr_ready", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	durable := testClaim("pr_ready", pr.Identity)
	if err := client.MarkReady(context.Background(), durable, pr.Identity); err != nil {
		t.Fatalf("ready reconciliation=%v", err)
	}
}

func TestChecksAllowExtrasButFailureDominatesPending(t *testing.T) {
	actual := []contracts.RequiredCheck{{Name: "required", ExternalID: "one", State: "SUCCESS"}, {Name: "extra", ExternalID: "two", State: "PENDING"}}
	if err := evaluateChecks(actual, []CheckIdentity{{Name: "required", ExternalID: "one"}}); !errors.Is(err, ErrChecksPending) {
		t.Fatalf("extra pending=%v", err)
	}
	actual[1].State = "FAILURE"
	if err := evaluateChecks(actual, []CheckIdentity{{Name: "required", ExternalID: "one"}}); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("failure precedence=%v", err)
	}
}

func TestWaitChecksBoundsBackgroundContext(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "one", State: "PENDING"}); err != nil {
		t.Fatal(err)
	}
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) { return fake.Run(args) })
	old := maxGHDeadline
	maxGHDeadline = 300 * time.Millisecond
	t.Cleanup(func() { maxGHDeadline = old })
	if _, err := client.WaitChecks(context.Background(), pr.Identity, []CheckIdentity{{Name: "unit", ExternalID: "one"}}, time.Millisecond, time.Millisecond); !errors.Is(err, ErrChecksPending) {
		t.Fatalf("bounded background polling=%v", err)
	}
}

func TestWaitChecksPreservesCancellationAsPending(t *testing.T) {
	client, _, identity := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.WaitChecks(ctx, identity, nil, time.Millisecond, time.Millisecond); !errors.Is(err, ErrChecksPending) {
		t.Fatalf("cancelled checks=%v", err)
	}
}

func TestStrictJSONBoundedSanitizedCommandBoundary(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: filepath.Join(t.TempDir(), "gh-config"), runner: commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return []byte(`{"unknown":true}`), nil
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil })}
	var value struct{}
	if err := client.json(context.Background(), &value, "repo", "view"); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("unknown json=%v", err)
	}
	client.runner = commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return make([]byte, maxResponse+1), nil
	})
	if _, err := client.run(context.Background(), "repo", "view"); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized=%v", err)
	}
	client.runner = commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return []byte("secret-token-in-output"), errors.New("failure")
	})
	if _, err := client.run(context.Background(), "repo", "view"); err == nil || err.Error() != "gh command failed" {
		t.Fatalf("sanitized error=%v", err)
	}
}

func TestRunBoundedKillsProcessGroupOnDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := runBounded(ctx, "/bin/sh", []string{"-c", "sleep 5 & wait"}, []string{"PATH=/usr/bin:/bin"})
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("stuck process group err=%v elapsed=%s", err, time.Since(started))
	}
}

func TestRunBoundedClosesRetainedPipeFromEscapedDescendant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	script := "import os,time\nif os.fork()==0:\n if os.fork()==0:\n  os.setsid(); print('escaped-child-pid='+str(os.getpid()), flush=True); time.sleep(5)\n os._exit(0)\ntime.sleep(5)"
	output, err := runBounded(ctx, "/usr/bin/python3", []string{"-c", script}, []string{"PATH=/usr/bin:/bin"})
	if !errors.Is(err, ErrProcessCleanup) || time.Since(started) > 2*time.Second {
		t.Fatalf("escaped retained pipe err=%v elapsed=%s output=%q", err, time.Since(started), output)
	}
	for _, field := range strings.Fields(string(output)) {
		if strings.HasPrefix(field, "escaped-child-pid=") {
			if pid, parseErr := strconv.Atoi(strings.TrimPrefix(field, "escaped-child-pid=")); parseErr == nil && pid > 0 {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
	}
}
