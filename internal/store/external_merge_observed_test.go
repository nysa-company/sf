package store

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestGenericExternalMergeObservationCannotMutateGuardedOrManualStates(t *testing.T) {
	for _, target := range []domain.State{domain.StateWaitingApproval, domain.StateMerging, domain.StateWaitingManualMerge} {
		t.Run(string(target), func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, target)
			defer fixture.db.Close()

			beforeEvents, err := fixture.db.Events(fixture.ctx, current.Ref.Channel, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			_, err = fixture.db.Transition(fixture.ctx, Transition{
				Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
				To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: fence,
				EventPayload: `{"merged_pr_identity_exact":true}`,
			})
			if !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("generic external merge observation from %s was accepted: %v", target, err)
			}

			after, err := fixture.db.Ticket(fixture.ctx, current.Ref)
			if err != nil {
				t.Fatal(err)
			}
			afterEvents, err := fixture.db.Events(fixture.ctx, current.Ref.Channel, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, current) || !reflect.DeepEqual(afterEvents, beforeEvents) {
				t.Fatalf("generic external merge observation mutated %s: before ticket=%+v after=%+v before events=%+v after events=%+v", target, current, after, beforeEvents, afterEvents)
			}
		})
	}
}

func TestTypedExternalMergeObservationAdvancesWaitingApprovalWithoutMergeMutation(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	defer fixture.db.Close()
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observed := contracts.PublishedPullRequestObservation{
		Identity: publication.PullRequest, State: "MERGED", Merged: true,
		MergeCommit: strings.Repeat("9", 40), BaseHeadOID: publication.PullRequest.BaseOID,
	}
	authority, err := fixture.db.BindExternalMergeObservation(fixture.ctx, current.Ref, NewManualMergeObservation(publication, observed), fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordExternalMergeObservation(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateWaitingApproval,
		To: domain.StateExternalMerged, Trigger: "external_merge_observed", Fence: fence,
	}, authority); err != nil {
		t.Fatalf("typed external merge observation: %v", err)
	}
	got, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil || got.State != domain.StateExternalMerged {
		t.Fatalf("ticket after external merge=%+v err=%v", got, err)
	}
	if _, err := fixture.db.LoadExternalMergeObservation(fixture.ctx, current.Ref); err != nil {
		t.Fatalf("load durable external merge evidence: %v", err)
	}
}

func TestTypedExternalMergeObservationClassifiesManualHeadDrift(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateWaitingManualMerge)
	defer fixture.db.Close()
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observedIdentity := publication.PullRequest
	observedIdentity.HeadOID = strings.Repeat("d", 40)
	observed := contracts.PublishedPullRequestObservation{
		Identity: observedIdentity, State: "MERGED", Merged: true,
		MergeCommit: strings.Repeat("9", 40), BaseHeadOID: publication.PullRequest.BaseOID,
	}
	authority, err := fixture.db.BindExternalMergeObservation(fixture.ctx, current.Ref, NewManualMergeObservation(publication, observed), fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordExternalMergeObservation(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateWaitingManualMerge,
		To: domain.StateExternalMerged, Trigger: "external_merge_observed", Fence: fence,
	}, authority); err != nil {
		t.Fatalf("typed manual head-drift observation: %v", err)
	}
	got, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil || got.State != domain.StateExternalMerged {
		t.Fatalf("ticket after manual head drift=%+v err=%v", got, err)
	}
	if _, err := fixture.db.LoadExternalMergeObservation(fixture.ctx, current.Ref); err != nil {
		t.Fatalf("load durable manual head-drift evidence: %v", err)
	}
}
