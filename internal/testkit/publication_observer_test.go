package testkit

import (
	"context"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestObserveDraftPullRequestRejectsForeignExactSource(t *testing.T) {
	remote, err := NewFakeGH(t.TempDir()+"/remote.json", contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/test", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	if err := remote.withState(func() (bool, error) {
		remote.state.PRs = append(remote.state.PRs, PullRequest{Identity: contracts.PullRequestIdentity{Repository: identity.Repository, Number: 7, HeadOwner: identity.HeadOwner, HeadRepository: identity.HeadRepository, HeadRef: identity.HeadRef, HeadOID: identity.HeadOID, BaseRef: identity.BaseRef, FactoryOwned: false}, Draft: true})
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := remote.ObserveDraftPullRequest(context.Background(), identity); err == nil {
		t.Fatal("foreign exact-source PR was treated as absence")
	}
}

func TestObserveDraftPullRequestRecoversDroppedCreateResponse(t *testing.T) {
	remote, err := NewFakeGH(t.TempDir()+"/remote.json", contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/test", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	if err := remote.SetResponse("pr_create", ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.CreateDraftPullRequest(context.Background(), EffectClaimForTest("draft_pr", identity, "title", "body"), identity, "title", "body"); err == nil {
		t.Fatal("expected dropped response")
	}
	got, state, draft, found, err := remote.ObserveDraftPullRequest(context.Background(), identity)
	if err != nil || !found || state != "OPEN" || !draft || got.Number == 0 || got.HeadOID != identity.HeadOID {
		t.Fatalf("lost create was not recoverable: pr=%+v state=%q draft=%v found=%v err=%v", got, state, draft, found, err)
	}
}
