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
	observed := contracts.PublishedPullRequestObservation{Identity: publication.PullRequest, State: "MERGED", Merged: true, Ready: true, Title: "volatile title", Body: "volatile body", MergeCommit: strings.Repeat("c", 40), BaseHeadOID: publication.PullRequest.BaseOID}
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
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, current.Version, "tampered_competing_transition", domain.StateReconciling, domain.StateBlocked, `{}`, now()); err != nil {
		t.Fatalf("append competing manual transition: %v", err)
	}
	if err := fixture.db.ValidateManualMergeObservation(fixture.ctx, waiting.Ref, NewManualMergeObservation(publication, observed)); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("manual observation accepted competing state transition: %v", err)
	}
	var path string
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("database path=%q err=%v", path, err)
	}
	if err := fixture.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(fixture.ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	leader, err := reopened.AcquireLeader(fixture.ctx, waiting.Ref.Channel, "manual-conflicting-transition")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, waiting.Ref.Channel, leader); changed != 0 || !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("manual conflict recovery changed=%d err=%v", changed, err)
	}
}
