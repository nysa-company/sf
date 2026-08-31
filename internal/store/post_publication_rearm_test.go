package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func bindTerminalMergeEffect(t *testing.T, db *Store, fixture finalReviewFixture, ticket Ticket, fence domain.Fence, semanticKey string) {
	t.Helper()
	publication, found, err := loadPublicationEvidenceRow(fixture.ctx, db.db, ticket.Ref)
	if err != nil || !found {
		t.Fatalf("merge publication found=%v err=%v", found, err)
	}
	requestDigest := "merge-request-" + semanticKey
	if _, err := db.PlanEffect(fixture.ctx, EffectPlan{SemanticKey: semanticKey, Ref: ticket.Ref, Kind: "merge", TicketVersion: ticket.Version, Fence: fence, RequestDigest: requestDigest}); err != nil {
		t.Fatalf("plan terminal merge effect: %v", err)
	}
	claim, err := db.ClaimEffect(fixture.ctx, EffectFence{SemanticKey: semanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim terminal merge effect=%+v err=%v", claim, err)
	}
	intent := domain.MergeIntent{
		Ref: ticket.Ref, SemanticKey: semanticKey, RequestDigest: requestDigest,
		TicketVersion: ticket.Version, LeaderEpoch: claim.Effect.LeaderEpoch,
		RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch,
		RepositoryHost:    publication.PullRequest.Repository.Host,
		RepositoryOwner:   publication.PullRequest.Repository.Owner,
		RepositoryName:    publication.PullRequest.Repository.Name,
		PullRequestNumber: publication.PullRequest.Number, HeadOID: publication.PullRequest.HeadOID,
		BaseRef: publication.PullRequest.BaseRef, OriginalBaseOID: publication.PullRequest.BaseOID,
		ProtectionRuleID: "post-publication-rule", StrictStatusChecks: true,
		AdminEnforced: true, Method: "squash",
	}
	if err := db.RecordMergeIntent(fixture.ctx, intent); err != nil {
		t.Fatalf("record terminal merge intent: %v", err)
	}
	if _, err := db.ConfirmEffect(fixture.ctx, EffectFence{SemanticKey: semanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, "merge-observed"); err != nil {
		t.Fatalf("confirm terminal merge effect: %v", err)
	}
}

// postPublicationPauseResume creates the same durable pause/take/resume
// sequence used by the daemon. It returns the pre-stop proof so the rearm
// authority must authenticate the sealed generation rather than trusting the
// resumed ticket alone.
func postPublicationPauseResume(t *testing.T) (*Store, Ticket, Ticket) {
	t.Helper()
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	stopped, current := postPublicationPauseResumeAt(t, db, ticket, fence, ticket.State)
	return db, stopped, current
}

func postPublicationPauseResumeAt(t *testing.T, db *Store, ticket Ticket, fence domain.Fence, target domain.State) (Ticket, Ticket) {
	t.Helper()
	ctx := t.Context()
	proof, err := db.ControlProof(ctx, ticket.Ref)
	if err != nil || !proof.Drained() {
		t.Fatalf("post-publication control proof=%+v err=%v", proof, err)
	}
	stopping, err := db.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: ticket.State, To: domain.StateStopping,
		ResumeState: ticket.State, Trigger: "operator_pause_or_take",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}, EventPayload: `{"intent":"pause"}`,
	})
	if err != nil {
		t.Fatalf("post-publication stop=%+v err=%v", stopping, err)
	}
	paused, err := db.CompleteControlTransition(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: ticket.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("post-publication pause=%+v err=%v", paused, err)
	}
	resumed, err := db.TransitionPublishedResume(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: target,
		Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`,
	})
	if target != domain.StatePublishing && target != domain.StateWaitingCI {
		// The publication-specific resume primitive intentionally handles only
		// publication states. Other post-publication states use the ordinary
		// state transition after the exact stop/drain pair.
		resumed, err = db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: target, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`})
	}
	if err != nil {
		t.Fatalf("post-publication resume=%+v err=%v", resumed, err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || current.State != ticket.State || current.Version != resumed.Version {
		t.Fatalf("post-publication current=%+v result=%+v err=%v", current, resumed, err)
	}
	return proof.Ticket, current
}

func TestPostPublicationRearmProofAuthenticatesWaitingCIResume(t *testing.T) {
	db, stopped, current := postPublicationPauseResume(t)
	defer db.Close()
	capability, err := db.PostPublicationRearmProof(t.Context(), current.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("post-publication rearm capability=%v err=%v", capability, err)
	}
	if needed, err := db.RuntimeRearmNeeded(t.Context(), current.Ref); err != nil || !needed {
		t.Fatalf("sealed post-publication rearm needed=%v err=%v", needed, err)
	}
	var consumed bool
	if err := db.ActivateRearm(t.Context(), capability, func(admission *RuntimeAdmissionCapability) error {
		_, version, fence, ok := admission.ConsumeRuntimeAdmission()
		consumed = ok && version == current.Version && fence.RunnerEpoch == current.RunnerEpoch
		return nil
	}); err != nil || !consumed {
		t.Fatalf("post-publication activation consumed=%v err=%v", consumed, err)
	}
	if err := db.ActivateRearm(t.Context(), capability, func(*RuntimeAdmissionCapability) error { return nil }); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("post-publication capability replay=%v", err)
	}
}

func TestPostPublicationRearmProofRejectsUncertainMutation(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: "postpub/uncertain", Ref: ticket.Ref, Kind: "repository_command", TicketVersion: ticket.Version, Fence: fence, RequestDigest: "postpub-uncertain"}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.ClaimEffect(ctx, EffectFence{SemanticKey: "postpub/uncertain", Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	proof, err := db.ControlProof(ctx, ticket.Ref)
	if err != nil || proof.Drained() {
		t.Fatalf("uncertain mutation proof=%+v err=%v", proof, err)
	}
	if _, err := db.PostPublicationRearmProof(ctx, ticket.Ref, proof.Ticket); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("uncertain post-publication mutation rearmed: %v", err)
	}
}

func TestPostPublicationRearmProofAfterResumeAndLeaderTakeover(t *testing.T) {
	db, _, current := postPublicationPauseResume(t)
	ctx := t.Context()
	var path string
	if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("database path=%q err=%v", path, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	leader, err := reopened.AcquireLeader(ctx, domain.ChannelDev, "postpub-reopen")
	if err != nil {
		t.Fatalf("acquire replacement leader: %v", err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("fence changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(ctx, current.Ref)
	if err != nil || live.Version != current.Version+1 || live.RunnerEpoch != current.RunnerEpoch+1 {
		t.Fatalf("fenced ticket=%+v err=%v", live, err)
	}
	recoveredStopped, err := reopened.StoppedRuntimeTicket(ctx, live.Ref)
	if err != nil {
		t.Fatalf("load durable stop tuple: %v", err)
	}
	capability, err := reopened.PostPublicationRearmProof(ctx, live.Ref, recoveredStopped)
	if err != nil || capability == nil {
		t.Fatalf("rearm after reopen capability=%v err=%v", capability, err)
	}
}

func TestPostPublicationRearmProofAuthenticatesReviewingAndApprovalStates(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   domain.MergeMode
		target domain.State
	}{
		{name: "reviewing", mode: domain.MergeGuarded, target: domain.StateReviewing},
		{name: "waiting approval", mode: domain.MergeGuarded, target: domain.StateWaitingApproval},
		{name: "waiting manual merge", mode: domain.MergeManual, target: domain.StateWaitingManualMerge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, tc.mode)
			if tc.target != domain.StateReviewing {
				completeFinalReview(t, fixture)
				if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.target, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
					t.Fatal(err)
				}
				fixture.ticket, _ = fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
				fixture.fence.RunnerEpoch = fixture.ticket.RunnerEpoch
			}
			stopped, current := postPublicationPauseResumeAt(t, fixture.db, fixture.ticket, fixture.fence, tc.target)
			defer fixture.db.Close()
			if capability, err := fixture.db.PostPublicationRearmProof(t.Context(), current.Ref, stopped); err != nil || capability == nil {
				t.Fatalf("state=%s capability=%v err=%v", tc.target, capability, err)
			}
		})
	}
}

func TestPostPublicationRearmProofRejectsUnboundMergeState(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	defer fixture.db.Close()
	current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	// No merge intent/effect exists in this reviewing fixture. The generic
	// transition is allowed to construct the state for this negative test, but
	// the post-publication authority must refuse to rearm without merge proof.
	if _, err := fixture.db.Transition(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: current.State, To: domain.StateMerging, Trigger: "operator_resume", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	current, _ = fixture.db.Ticket(fixture.ctx, current.Ref)
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fixture.fence, domain.StateMerging)
	if _, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("unbound merge state rearmed: %v", err)
	}
}

func TestPostPublicationRearmProofAuthenticatesMergingAndReconciling(t *testing.T) {
	for _, target := range []domain.State{domain.StateMerging, domain.StateReconciling} {
		t.Run(string(target), func(t *testing.T) {
			fixture := finalReviewLifecycleFixture(t)
			defer fixture.db.Close()
			completeFinalReview(t, fixture)
			current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
			if target != domain.StateWaitingApproval {
				if _, err := fixture.db.Transition(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: current.State, To: target, Trigger: "merge_start", Fence: fence, EventPayload: `{}`}); err != nil {
					t.Fatalf("transition to %s: %v", target, err)
				}
				current, err = fixture.db.Ticket(fixture.ctx, current.Ref)
				if err != nil {
					t.Fatal(err)
				}
				fence.RunnerEpoch = current.RunnerEpoch
			}
			bindTerminalMergeEffect(t, fixture.db, fixture, current, fence, "merge/rearm/"+string(target))
			stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, target)
			capability, err := fixture.db.PostPublicationRearmProof(t.Context(), resumed.Ref, stopped)
			if err != nil || capability == nil {
				t.Fatalf("state=%s capability=%v err=%v", target, capability, err)
			}
		})
	}
}
