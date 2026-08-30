package github

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/testkit"
)

type verifierFunc func(context.Context, contracts.RepositoryIdentity, string, string) (contracts.ProtectedBranchObservation, error)

type mutationGuardFunc func(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error)

func (f mutationGuardFunc) RunExternalMutation(ctx context.Context, claim domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
	return f(ctx, claim, start)
}

func (f verifierFunc) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit string) (contracts.ProtectedBranchObservation, error) {
	return f(ctx, repository, baseRef, mergeCommit)
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
	client := &Client{Binary: binary, Home: filepath.Join(root, "home"), ConfigDir: filepath.Join(root, "gh-config"), Env: []string{"SF_FAKE_GH_STATE=" + state}, ValidateClaim: func(context.Context, domain.ExternalEffectClaim) error { return nil }, MutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), VerifyProtectedBranch: verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit string) (contracts.ProtectedBranchObservation, error) {
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, BaseHeadOID: strings.Repeat("c", 40), Contains: true}, nil
	})}
	identity := contracts.PullRequestIdentity{Repository: repository, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	return client, fake, identity
}

func TestContractMutationRequiresClaimValidator(t *testing.T) {
	client, _, identity := fixture(t)
	claim := domain.ExternalEffectClaim{SemanticKey: "k", Kind: "draft_pr", RequestDigest: requestDigest("draft_pr", identity, "title", "body")}
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); err != nil {
		t.Fatalf("validated claim=%v", err)
	}
	client.ValidateClaim = nil
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("missing validator=%v", err)
	}
}

func TestAuthStatusAcceptsOfficialHostsStateShape(t *testing.T) {
	client := Client{Binary: "/bin/echo", Home: t.TempDir(), ConfigDir: t.TempDir(), Run: func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		want := []string{"auth", "status", "--json", "hosts"}
		if !reflect.DeepEqual(args, want) {
			return nil, errors.New("unexpected auth argv")
		}
		return []byte(`{"hosts":{"github.com":[{"state":"success","active":true,"host":"github.com","login":"sf-test","tokenSource":"keyring","scopes":"repo","gitProtocol":"https"}]}}`), nil
	}}
	if err := client.AuthStatus(context.Background()); err != nil {
		t.Fatalf("official auth status shape=%v", err)
	}
}

func TestPreflightCreateLostResponseAndExactAdoption(t *testing.T) {
	client, fake, identity := fixture(t)
	principal, err := client.Preflight(context.Background(), identity.Repository)
	if err != nil || principal.Login != "sf-test" {
		t.Fatalf("preflight=%+v err=%v", principal, err)
	}
	plan, err := client.Plan(identity, "dev/example/SF-44/publish/1/draft-pr")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := client.Claim(plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetResponse("pr_create", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	pr, err := client.createOrAdopt(context.Background(), claim, "title", "<!-- sf:owned -->")
	if err != nil || pr.Identity.Number != 1 || !pr.Draft {
		t.Fatalf("create/adopt=%+v err=%v", pr, err)
	}
	if fake.MutationCount("pr_create") != 1 {
		t.Fatalf("create mutations=%d", fake.MutationCount("pr_create"))
	}
	if _, err := client.createOrAdopt(context.Background(), claim, "title", "<!-- sf:owned -->"); err != nil {
		t.Fatalf("idempotent adopt=%v", err)
	}
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: pr.Identity, Draft: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Observe(context.Background(), identity); !errors.Is(err, ErrAmbiguousPR) {
		t.Fatalf("ambiguous=%v", err)
	}
}

func TestChecksMergeAndApprovalPolicies(t *testing.T) {
	client, fake, identity := fixture(t)
	plan, _ := client.Plan(identity, "key")
	claim, _ := client.Claim(plan)
	pr, err := client.createOrAdopt(context.Background(), claim, "title", "body")
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := client.merge(context.Background(), claim, pr, pr.Identity.HeadOID, domain.MergeManual, "merge"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("manual merge=%v", err)
	}
	if _, err := client.merge(context.Background(), claim, pr, pr.Identity.HeadOID, domain.MergeAutonomous, "merge"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("autonomous merge=%v", err)
	}
	if err := fake.MarkReady(context.Background(), domain.ExternalEffectClaim{}, pr.Identity); err != nil {
		t.Fatal(err)
	}
	pr.Draft = false
	if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	outcome, err := client.merge(context.Background(), claim, pr, pr.Identity.HeadOID, domain.MergeGuarded, "squash")
	if err != nil || outcome != MergeApplied {
		t.Fatalf("guarded merge=%q err=%v", outcome, err)
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
	plan, _ := client.Plan(identity, "draft-policy")
	claim, _ := client.Claim(plan)
	pr, err := client.createOrAdopt(context.Background(), claim, "title", "body")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.merge(context.Background(), claim, pr, pr.Identity.HeadOID, domain.MergeGuarded, "merge"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("draft merge=%v", err)
	}
	closedClient, closedFake, closedIdentity := fixture(t)
	closed := closedIdentity
	closed.Number = 1
	if err := closedFake.InjectPullRequestForTest(testkit.PullRequest{Identity: closed, Draft: true, Merged: true}); err != nil {
		t.Fatal(err)
	}
	closedPlan, _ := closedClient.Plan(closedIdentity, "closed-policy")
	closedClaim, _ := closedClient.Claim(closedPlan)
	if _, err := closedClient.createOrAdopt(context.Background(), closedClaim, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("merged draft adoption=%v", err)
	}
}

func TestMergeQueueEntryRefusesBeforeCommand(t *testing.T) {
	client, _, identity := fixture(t)
	identity.Number = 1
	called := false
	client.Run = func(context.Context, string, []string, []string) ([]byte, error) { called = true; return nil, nil }
	plan, _ := client.Plan(identity, "queue")
	claim, _ := client.Claim(plan)
	if _, err := client.merge(context.Background(), claim, PRMatch{Identity: identity, State: "OPEN", MergeQueued: true}, identity.HeadOID, domain.MergeGuarded, "merge"); !errors.Is(err, ErrPolicyRefusal) || called {
		t.Fatalf("queue merge err=%v command=%v", err, called)
	}
}

func TestMergeRequiresFreshProtectedBranchProof(t *testing.T) {
	t.Run("unavailable verifier is never success", func(t *testing.T) {
		client, fake, identity := fixture(t)
		plan, _ := client.Plan(identity, "key")
		claim, _ := client.Claim(plan)
		pr, err := client.createOrAdopt(context.Background(), claim, "title", "body")
		if err != nil {
			t.Fatal(err)
		}
		client.VerifyProtectedBranch = nil
		if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
			t.Fatal(err)
		}
		if _, err := client.merge(context.Background(), claim, pr, pr.Identity.HeadOID, domain.MergeGuarded, "merge"); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("missing proof verifier=%v", err)
		}
	})
	t.Run("mismatched proof is never success", func(t *testing.T) {
		client, fake, identity := fixture(t)
		plan, _ := client.Plan(identity, "key")
		claim, _ := client.Claim(plan)
		pr, err := client.createOrAdopt(context.Background(), claim, "title", "body")
		if err != nil {
			t.Fatal(err)
		}
		client.VerifyProtectedBranch = verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit string) (contracts.ProtectedBranchObservation, error) {
			return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
		})
		if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
			t.Fatal(err)
		}
		if _, err := client.merge(context.Background(), claim, pr, pr.Identity.HeadOID, domain.MergeGuarded, "merge"); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("mismatched proof=%v", err)
		}
	})
}

func TestMergeNeverTrustsCLIExitWithoutFreshMergedObservation(t *testing.T) {
	client, _, identity := fixture(t)
	identity.Number = 1
	plan, _ := client.Plan(identity, "key")
	claim, _ := client.Claim(plan)
	pr := PRMatch{Identity: identity, State: "OPEN"}
	for _, test := range []struct {
		name string
		wire map[string]any
	}{
		{name: "zero-exit-unmerged", wire: mergeWire(identity, "OPEN", "CLEAN", nil, nil)},
		{name: "auto-or-queued", wire: mergeWire(identity, "OPEN", "QUEUED", nil, map[string]any{"enabledAt": "now"})},
		{name: "missing-merge-commit", wire: mergeWire(identity, "MERGED", "CLEAN", "2026-01-01T00:00:00Z", nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(test.wire)
			if err != nil {
				t.Fatal(err)
			}
			client.Run = func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
				if len(args) >= 2 && args[0] == "pr" && args[1] == "merge" {
					return nil, nil // gh may report success for a non-merge operation.
				}
				if len(args) >= 2 && args[0] == "pr" && args[1] == "view" {
					return payload, nil
				}
				return nil, errors.New("unexpected gh argv")
			}
			if _, err := client.merge(context.Background(), claim, pr, identity.HeadOID, domain.MergeGuarded, "merge"); !errors.Is(err, ErrPolicyRefusal) {
				t.Fatalf("merge must reject %s: %v", test.name, err)
			}
		})
	}
}

func TestOfficialGHArgvGolden(t *testing.T) {
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	created := identity
	created.Number = 7
	createdWire, err := json.Marshal(mergeWire(created, "OPEN", "CLEAN", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	var got [][]string
	listCalls := 0
	client := Client{Binary: "/bin/echo", Home: t.TempDir(), ConfigDir: t.TempDir(), Run: func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		got = append(got, append([]string(nil), args...))
		switch args[0] + " " + args[1] {
		case "pr list":
			listCalls++
			if listCalls == 1 {
				return []byte("[]"), nil
			}
			return []byte("[" + string(createdWire) + "]"), nil
		case "pr create":
			return []byte("https://github.com/example/app/pull/7\n"), nil
		default:
			return nil, errors.New("unexpected command")
		}
	}}
	claim := EffectClaim{Plan: EffectPlan{SemanticKey: "key", Identity: identity}, Claimed: true}
	if _, err := client.createOrAdopt(context.Background(), claim, "title", "body"); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
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
	client := Client{Binary: "/bin/echo", Home: t.TempDir(), ConfigDir: t.TempDir(), Run: func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		got = append(got, append([]string(nil), args...))
		if args[0] == "pr" && args[1] == "merge" {
			return nil, nil
		}
		if args[0] == "pr" && args[1] == "view" {
			return payload, nil
		}
		return nil, errors.New("unexpected command")
	}, VerifyProtectedBranch: verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit string) (contracts.ProtectedBranchObservation, error) {
		verified = repository == identity.Repository && baseRef == "main" && mergeCommit == strings.Repeat("b", 40)
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, BaseHeadOID: strings.Repeat("c", 40), Contains: true}, nil
	})}
	claim := EffectClaim{Plan: EffectPlan{SemanticKey: "key", Identity: identity}, Claimed: true}
	if outcome, err := client.merge(context.Background(), claim, PRMatch{Identity: identity, State: "OPEN"}, identity.HeadOID, domain.MergeGuarded, "squash"); err != nil || outcome != MergeApplied || !verified {
		t.Fatalf("proven merge outcome=%q verified=%v err=%v", outcome, verified, err)
	}
	want := [][]string{{"pr", "merge", "7", "--repo", "example/app", "--match-head-commit", identity.HeadOID, "--squash"}, {"pr", "view", "7", "--repo", "example/app", "--json", prFields}}
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
	plan, _ := client.Plan(identity, "update")
	claim, _ := client.Claim(plan)
	pr, err := client.createOrAdopt(context.Background(), claim, "before", "before body")
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetResponse("pr_edit", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	if err := client.updateOrObserve(context.Background(), claim, pr, "after", "after body"); err != nil {
		t.Fatalf("update reconciliation=%v", err)
	}
	if err := fake.SetResponse("pr_ready", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	durable := domain.ExternalEffectClaim{SemanticKey: "ready", Kind: "pr_ready", RequestDigest: requestDigest("pr_ready", pr.Identity)}
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
	plan, _ := client.Plan(identity, "checks-deadline")
	claim, _ := client.Claim(plan)
	pr, err := client.createOrAdopt(context.Background(), claim, "title", "body")
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "one", State: "PENDING"}); err != nil {
		t.Fatal(err)
	}
	client.Run = func(_ context.Context, _ string, args, _ []string) ([]byte, error) { return fake.Run(args) }
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
	client := Client{Binary: "/bin/echo", Home: t.TempDir(), ConfigDir: filepath.Join(t.TempDir(), "gh-config"), Run: func(context.Context, string, []string, []string) ([]byte, error) {
		return []byte(`{"unknown":true}`), nil
	}}
	var value struct{}
	if err := client.json(context.Background(), &value, "repo", "view"); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("unknown json=%v", err)
	}
	client.Run = func(context.Context, string, []string, []string) ([]byte, error) {
		return make([]byte, maxResponse+1), nil
	}
	if _, err := client.run(context.Background(), "repo", "view"); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized=%v", err)
	}
	client.Run = func(context.Context, string, []string, []string) ([]byte, error) {
		return []byte("secret-token-in-output"), errors.New("failure")
	}
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
