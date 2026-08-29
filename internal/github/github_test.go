package github

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/testkit"
)

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
	client := &Client{Binary: binary, Home: filepath.Join(root, "home"), ConfigDir: filepath.Join(root, "gh-config"), Env: []string{"SF_FAKE_GH_STATE=" + state}, ValidateClaim: func(context.Context, domain.ExternalEffectClaim) error { return nil }}
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
