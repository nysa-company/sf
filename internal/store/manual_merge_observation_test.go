package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestManualMergeObservationAuthorityIsAtomicAndFailsClosedOnTamper(t *testing.T) {
	fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeManual)
	completeFinalReview(t, fixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingManualMerge, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observed := contracts.PublishedPullRequestObservation{Identity: publication.PullRequest, State: "MERGED", Merged: true, MergeCommit: strings.Repeat("c", 40), BaseHeadOID: publication.PullRequest.BaseOID}
	authority := NewManualMergeObservation(publication, observed)
	authority, err = fixture.db.BindManualMergeObservation(fixture.ctx, waiting.Ref, authority, domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	transition := Transition{Ref: waiting.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingManualMerge, To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: authority.CurrentFence}
	result, err := fixture.db.RecordManualMergeObservation(fixture.ctx, transition, authority)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != waiting.Version+1 {
		t.Fatalf("transition version=%d want=%d", result.Version, waiting.Version+1)
	}
	current, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil || current.State != domain.StateReconciling {
		t.Fatalf("reconciling ticket=%+v err=%v", current, err)
	}
	loaded, err := fixture.db.LoadManualMergeObservation(fixture.ctx, waiting.Ref)
	if err != nil || loaded != authority {
		t.Fatalf("loaded authority=%+v err=%v", loaded, err)
	}
	if err := fixture.db.ValidateManualMergeObservation(fixture.ctx, waiting.Ref, NewManualMergeObservation(publication, observed)); err != nil {
		t.Fatalf("fresh read-only observation rejected: %v", err)
	}
	tampered := NewManualMergeObservation(publication, observed)
	tampered.MergeCommit = strings.Repeat("d", 40)
	if err := fixture.db.ValidateManualMergeObservation(fixture.ctx, waiting.Ref, tampered); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("tampered observation err=%v", err)
	}
}

