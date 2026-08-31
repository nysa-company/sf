package publication

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	githubboundary "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/store"
)

func TestPublicationKeysAreDeterministicAndCandidateBound(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-publication"}
	worktree := store.StoredWorktree{Path: "/tmp/publication", Branch: "sf/dev/1111111111111111/2222222222222222-33333333333333333333333333333333"}
	candidate := store.StoredCandidate{Snapshot: domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}}
	first := pushRequestDigest(ref, "/tmp/repository", worktree, candidate)
	if first != pushRequestDigest(ref, "/tmp/repository", worktree, candidate) || !strings.HasPrefix(first, "sha256:") {
		t.Fatalf("push request is not stable: %q", first)
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "nysa", Name: "app"}, HeadOwner: "nysa", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	title, body := "sf: SF-publication", "typed evidence"
	if got, want := draftKey(identity, title, body), "github/draft-pr/v1/"+githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body); got != want {
		t.Fatalf("draft semantic key=%q want %q", got, want)
	}
	changed := candidate
	changed.Snapshot.HeadSHA = strings.Repeat("c", 40)
	if pushRequestDigest(ref, "/tmp/repository", worktree, changed) == first {
		t.Fatal("candidate head did not affect push request")
	}
}

func TestSamePRRequiresThePublishedBaseAndOwnedIdentity(t *testing.T) {
	base := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "nysa", Name: "app"}, Number: 2, HeadOwner: "nysa", HeadRepository: "app", HeadRef: "sf/dev/ref", HeadOID: strings.Repeat("b", 40), BaseRef: "main", BaseOID: strings.Repeat("a", 40), FactoryOwned: true}
	if !samePR(base, base) {
		t.Fatal("exact PR rejected")
	}
	changed := base
	changed.BaseOID = strings.Repeat("c", 40)
	if samePR(changed, base) {
		t.Fatal("base drift accepted")
	}
	changed = base
	changed.FactoryOwned = false
	if samePR(changed, base) {
		t.Fatal("foreign PR accepted")
	}
}
