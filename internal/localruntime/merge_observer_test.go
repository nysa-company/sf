package localruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gh "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/store"
)

type historicalStoreFake struct {
	evidence store.PublishedCandidateEvidence
	pre      bool
	preErr   error
	missing  bool
	calls    int
}

func (s *historicalStoreFake) LoadHistoricalPublishedCandidate(context.Context, domain.TicketRef) (store.PublishedCandidateEvidence, error) {
	s.calls++
	if s.missing {
		return store.PublishedCandidateEvidence{}, store.ErrNotFound
	}
	return s.evidence, nil
}

func (s *historicalStoreFake) MergeObservationPrePublication(context.Context, domain.TicketRef) (bool, error) {
	return s.pre, s.preErr
}

type publishedObserverFake struct {
	match gh.PRMatch
	err   error
}

func (f publishedObserverFake) ObservePublishedPullRequest(context.Context, contracts.PullRequestIdentity) (gh.PRMatch, error) {
	return f.match, f.err
}
func (publishedObserverFake) AuthStatus(context.Context) error { return nil }
func (publishedObserverFake) Repository(context.Context, contracts.RepositoryIdentity) (contracts.RepositoryIdentity, error) {
	return contracts.RepositoryIdentity{}, errors.New("unused")
}
func (publishedObserverFake) FindPullRequest(context.Context, contracts.PullRequestIdentity) (contracts.PullRequestIdentity, bool, error) {
	return contracts.PullRequestIdentity{}, false, errors.New("unused")
}
func (publishedObserverFake) CreateDraftPullRequest(context.Context, domain.ExternalEffectClaim, contracts.PullRequestIdentity, string, string) (contracts.PullRequestIdentity, error) {
	return contracts.PullRequestIdentity{}, errors.New("unused")
}
func (publishedObserverFake) UpdatePullRequest(context.Context, domain.ExternalEffectClaim, contracts.PullRequestIdentity, string, string) error {
	return errors.New("unused")
}
func (publishedObserverFake) RequiredChecks(context.Context, contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	return nil, errors.New("unused")
}
func (publishedObserverFake) MarkReady(context.Context, domain.ExternalEffectClaim, contracts.PullRequestIdentity) error {
	return errors.New("unused")
}
func (publishedObserverFake) MergeExactHead(context.Context, domain.ExternalEffectClaim, contracts.PullRequestIdentity, string, string, domain.MergeAuthorization) error {
	return errors.New("unused")
}

func TestPublishedMergeObserverUsesHistoricalEvidenceAcrossRepeatedCancellationChecks(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-1"}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 7, HeadOwner: "acme", HeadRepository: "app", HeadRef: "sf/dev/app/SF-1", HeadOID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseRef: "main", BaseOID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", FactoryOwned: true}
	storeFake := &historicalStoreFake{evidence: store.PublishedCandidateEvidence{Ref: ref, PullRequest: identity}}
	observer := publishedMergeObserver{Store: storeFake, GitHub: publishedObserverFake{match: gh.PRMatch{Identity: identity, State: "MERGED", Merged: true, MergeCommit: "cccccccccccccccccccccccccccccccccccccccc", BaseHeadOID: identity.BaseOID}}}
	for i := 0; i < 2; i++ {
		merged, err := observer.MergeObserved(context.Background(), ref)
		if err != nil || !merged {
			t.Fatalf("observation %d merged=%v err=%v", i, merged, err)
		}
	}
	if storeFake.calls != 2 {
		t.Fatalf("historical evidence calls=%d want 2", storeFake.calls)
	}
}

func TestPublishedMergeObserverRequiresPrePublicationProofWhenWitnessIsAbsent(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-1"}
	storeFake := &historicalStoreFake{pre: true, missing: true}
	merged, err := (publishedMergeObserver{Store: storeFake, GitHub: publishedObserverFake{}}).MergeObserved(context.Background(), ref)
	if err != nil || merged {
		t.Fatalf("benign pre-publication absence merged=%v err=%v", merged, err)
	}
	storeFake.pre = false
	if merged, err = (publishedMergeObserver{Store: storeFake, GitHub: publishedObserverFake{}}).MergeObserved(context.Background(), ref); !errors.Is(err, store.ErrPublicationEvidence) || merged {
		t.Fatalf("effect-before-witness absence merged=%v err=%v", merged, err)
	}
}

func TestPublishedMergeObserverRejectsContradictoryOrMalformedMergeFacts(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-1"}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 7, HeadOwner: "acme", HeadRepository: "app", HeadRef: "sf/dev/app/SF-1", HeadOID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseRef: "main", BaseOID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", FactoryOwned: true}
	storeFake := &historicalStoreFake{evidence: store.PublishedCandidateEvidence{Ref: ref, PullRequest: identity}}
	cases := []gh.PRMatch{
		{Identity: identity, State: "MERGED", Merged: false, BaseHeadOID: identity.BaseOID},
		{Identity: identity, State: "MERGED", Merged: true, Draft: false, MergeCommit: "not-an-oid", BaseHeadOID: identity.BaseOID},
		{Identity: identity, State: "MERGED", Merged: true, MergeCommit: "cccccccccccccccccccccccccccccccccccccccc", BaseHeadOID: "not-an-oid"},
	}
	for i, match := range cases {
		merged, err := (publishedMergeObserver{Store: storeFake, GitHub: publishedObserverFake{match: match}}).MergeObserved(context.Background(), ref)
		if err == nil || merged {
			t.Fatalf("case %d malformed merge merged=%v err=%v", i, merged, err)
		}
	}
}

func TestPublishedMergeObserverAllowsMergedBaseMovement(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-1"}
	historical := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 7, HeadOwner: "acme", HeadRepository: "app", HeadRef: "sf/dev/app/SF-1", HeadOID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseRef: "main", BaseOID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", FactoryOwned: true}
	observed := historical
	observed.BaseOID = "dddddddddddddddddddddddddddddddddddddddd"
	storeFake := &historicalStoreFake{evidence: store.PublishedCandidateEvidence{Ref: ref, PullRequest: historical}}
	match := gh.PRMatch{Identity: observed, State: "MERGED", Merged: true, MergeCommit: "cccccccccccccccccccccccccccccccccccccccc", BaseHeadOID: observed.BaseOID}
	merged, err := (publishedMergeObserver{Store: storeFake, GitHub: publishedObserverFake{match: match}}).MergeObserved(context.Background(), ref)
	if err != nil || !merged {
		t.Fatalf("merged base movement merged=%v err=%v", merged, err)
	}
}

func TestPublishedMergeObserverRejectsWrongObservedIdentity(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-1"}
	want := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 7, HeadOwner: "acme", HeadRepository: "app", HeadRef: "sf/dev/app/SF-1", HeadOID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseRef: "main", BaseOID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", FactoryOwned: true}
	wrong := want
	wrong.HeadOID = "cccccccccccccccccccccccccccccccccccccccc"
	storeFake := &historicalStoreFake{evidence: store.PublishedCandidateEvidence{Ref: ref, PullRequest: want}}
	merged, err := (publishedMergeObserver{Store: storeFake, GitHub: publishedObserverFake{match: gh.PRMatch{Identity: wrong, State: "MERGED", Merged: true}}}).MergeObserved(context.Background(), ref)
	if err == nil || merged {
		t.Fatalf("wrong identity merged=%v err=%v", merged, err)
	}
}
