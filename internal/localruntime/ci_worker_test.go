package localruntime

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/testkit"
)

// Keep the fake-gh fixture on the production poller's narrow, read-only
// boundary. Its state file is deliberately reusable after a test restart.
var _ CIObserver = (*testkit.FakeGH)(nil)

func TestFakeGHCIRequiredCheckPolicyRequiresAuthentication(t *testing.T) {
	gh, err := testkit.NewFakeGH(t.TempDir()+"/fake-gh.json", contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gh.ObserveCIRequiredCheckPolicy(context.Background(), contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}})
	if err == nil {
		t.Fatal("unauthenticated fake-gh policy observation succeeded")
	}
}
