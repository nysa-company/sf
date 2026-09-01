package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/statemachine"
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
		PullRequestNumber: publication.PullRequest.Number, HeadOwner: publication.PullRequest.HeadOwner, HeadRepository: publication.PullRequest.HeadRepository, HeadRef: publication.PullRequest.HeadRef, HeadOID: publication.PullRequest.HeadOID,
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

func postPublicationPauseAt(t *testing.T, db *Store, ticket Ticket, fence domain.Fence) (Ticket, Ticket) {
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
	pausedResult, err := db.CompleteControlTransition(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused,
		ResumeState: ticket.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("post-publication pause=%+v err=%v", pausedResult, err)
	}
	paused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || paused.Version != pausedResult.Version || paused.State != domain.StatePaused || paused.ResumeState != ticket.State {
		t.Fatalf("post-publication paused ticket=%+v result=%+v err=%v", paused, pausedResult, err)
	}
	return proof.Ticket, paused
}

func postPublicationPauseResumeAt(t *testing.T, db *Store, ticket Ticket, fence domain.Fence, target domain.State) (Ticket, Ticket) {
	t.Helper()
	ctx := t.Context()
	stopped, paused := postPublicationPauseAt(t, db, ticket, fence)
	var err error
	resume := Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: target, Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch + 1}, EventPayload: `{}`}
	var resumed TransitionResult
	switch target {
	case domain.StatePublishing, domain.StateWaitingCI:
		resumed, err = db.TransitionPublishedResume(ctx, resume)
	case domain.StateMerging:
		resumed, err = db.TransitionGuardedMergeResume(ctx, resume)
	case domain.StateReconciling:
		resumed, err = db.TransitionPostPublicationReconcileResume(ctx, resume)
	default:
		resumed, err = db.Transition(ctx, resume)
	}
	if err != nil {
		t.Fatalf("post-publication resume=%+v err=%v", resumed, err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || current.State != ticket.State || current.Version != resumed.Version {
		t.Fatalf("post-publication current=%+v result=%+v err=%v", current, resumed, err)
	}
	return stopped, current
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
	// A generic Store transition cannot manufacture guarded merging and bypass
	// ApplyOperatorDecision's exact-head approval authority.
	if _, err := fixture.db.Transition(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: current.State, To: domain.StateMerging, Trigger: "operator_resume", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		if !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("generic guarded merge entry=%v", err)
		}
		return
	}
	t.Fatal("generic guarded merge entry succeeded")
}

func TestGenericTransitionCannotResumePausedWaitingApprovalAsMerging(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	defer fixture.db.Close()
	if current.State != domain.StateWaitingApproval {
		t.Fatalf("waiting approval ticket=%+v", current)
	}
	stopping, err := fixture.db.TransitionAndInvalidateRunner(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
		To: domain.StateStopping, ResumeState: current.State, Trigger: "operator_pause_or_take",
		Fence: fence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	paused, err := fixture.db.CompleteControlTransition(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping,
		To: domain.StatePaused, ResumeState: current.State, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch + 1}, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.db.Transition(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_resume",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch + 1}, EventPayload: `{}`,
	})
	if !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("generic paused waiting-approval resume entered merging: %v", err)
	}
	stored, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil || stored.State != domain.StatePaused || stored.Version != paused.Version || stored.ResumeState != domain.StateWaitingApproval {
		t.Fatalf("failed resume mutated paused ticket=%+v err=%v", stored, err)
	}
}

func TestGenericMergeEntryTransitionRejectsEveryMergingEntry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		from       domain.State
		trigger    string
		wantRefuse bool
	}{
		{name: "reviewing forged", from: domain.StateReviewing, trigger: "operator_resume", wantRefuse: true},
		{name: "paused arbitrary", from: domain.StatePaused, trigger: "forged", wantRefuse: true},
		{name: "paused resume", from: domain.StatePaused, trigger: "operator_resume", wantRefuse: true},
		{name: "paused retry", from: domain.StatePaused, trigger: "operator_retry", wantRefuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := genericMergeEntryTransition(Transition{From: tc.from, To: domain.StateMerging, Trigger: tc.trigger}); got != tc.wantRefuse {
				t.Fatalf("generic merge entry refused=%v, want %v", got, tc.wantRefuse)
			}
		})
	}
}

func TestTransitionGuardedMergeResumeAuthenticatesControlAndSemanticPaths(t *testing.T) {
	t.Run("operator control resume", func(t *testing.T) {
		fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
		defer fixture.db.Close()
		stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateMerging)
		if resumed.State != domain.StateMerging || resumed.Version != current.Version+3 || resumed.RunnerEpoch != current.RunnerEpoch+1 {
			t.Fatalf("control resumed ticket=%+v source=%+v", resumed, current)
		}
		if replay, err := fixture.db.GuardedMergeRetryReplay(fixture.ctx, resumed.Ref); err != nil || replay != GuardedMergeRetryNotReplay {
			t.Fatalf("normal control was classified as merge retry replay=%v err=%v", replay, err)
		}
		if capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped); err != nil || capability == nil {
			t.Fatalf("control resume capability=%v err=%v", capability, err)
		}
	})

	t.Run("semantic retry", func(t *testing.T) {
		fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
		defer fixture.db.Close()
		pausedResult, err := fixture.db.Transition(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
			To: domain.StatePaused, ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
			Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		paused, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || paused.State != domain.StatePaused || paused.Version != pausedResult.Version || paused.RunnerEpoch != current.RunnerEpoch {
			t.Fatalf("semantic paused ticket=%+v result=%+v err=%v", paused, pausedResult, err)
		}
		resumedResult, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
			To: domain.StateMerging, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
		})
		if err != nil {
			t.Fatalf("semantic retry: %v", err)
		}
		resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || resumed.State != domain.StateMerging || resumed.Version != resumedResult.Version || resumed.Version != current.Version+2 || resumed.RunnerEpoch != current.RunnerEpoch {
			t.Fatalf("semantic resumed ticket=%+v result=%+v err=%v", resumed, resumedResult, err)
		}
		stopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, current.Ref)
		if err != nil || stopped.Version != paused.Version || stopped.RunnerEpoch != paused.RunnerEpoch {
			t.Fatalf("semantic stopped tuple=%+v err=%v", stopped, err)
		}
		if needed, err := fixture.db.RuntimeRearmNeeded(fixture.ctx, current.Ref); err != nil || !needed {
			t.Fatalf("semantic rearm needed=%v err=%v", needed, err)
		}
		if replay, err := fixture.db.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryNeedsRearm {
			t.Fatalf("semantic retry replay=%v err=%v", replay, err)
		}
		capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, current.Ref, stopped)
		if err != nil || capability == nil {
			t.Fatalf("semantic retry capability=%v err=%v", capability, err)
		}
		var admission *RuntimeAdmissionCapability
		if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
			if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
				return errors.New("semantic retry admission was not consumable")
			}
			admission = value
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if replay, err := fixture.db.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryAlreadyRearmed {
			t.Fatalf("armed semantic retry replay=%v err=%v", replay, err)
		}
		if admission == nil {
			t.Fatal("semantic retry admission was not captured")
		}
		if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
			t.Fatal(err)
		}
		if replay, err := fixture.db.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryAlreadyRearmed {
			t.Fatalf("open semantic retry replay=%v err=%v", replay, err)
		}
	})
}

func TestGuardedMergeResumeAuthenticatesMixedControlLineagesBeforePromotion(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()

	firstStopped, firstResumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateMerging)
	firstFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: firstResumed.RunnerEpoch}
	firstCapability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, firstResumed.Ref, firstStopped)
	if err != nil {
		t.Fatal(err)
	}
	var firstAdmission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, firstCapability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("first mixed-lineage admission was not consumable")
		}
		firstAdmission = value
		return nil
	}); err != nil || firstAdmission == nil {
		t.Fatalf("activate first mixed-lineage admission=%v err=%v", firstAdmission, err)
	}
	if err := firstAdmission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}

	semanticPaused, err := fixture.db.Transition(fixture.ctx, Transition{
		Ref: firstResumed.Ref, ExpectedVersion: firstResumed.Version, From: domain.StateMerging,
		To: domain.StatePaused, ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: firstFence, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Same-state evidence rows are durable audit facts, not competing
	// lifecycle transitions. Semantic recovery must permit them while still
	// requiring exactly one canonical state-changing pause event.
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, firstResumed.Ref.Channel, firstResumed.Ref.Project, firstResumed.Ref.Ticket, semanticPaused.Version, "semantic_pause_audit", domain.StatePaused, domain.StatePaused, `{}`, now()); err != nil {
		t.Fatalf("append semantic pause audit: %v", err)
	}
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: firstResumed.Ref, ExpectedVersion: semanticPaused.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_retry", Fence: firstFence, EventPayload: `{"intent":"retry"}`,
	}); err != nil {
		t.Fatalf("normal-to-semantic retry: %v", err)
	}
	semanticResumed, err := fixture.db.Ticket(fixture.ctx, firstResumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	semanticStopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, semanticResumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	semanticCapability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, semanticResumed.Ref, semanticStopped)
	if err != nil {
		t.Fatalf("semantic mixed-lineage rearm: %v", err)
	}
	var semanticAdmission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, semanticCapability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("semantic mixed-lineage admission was not consumable")
		}
		semanticAdmission = value
		return nil
	}); err != nil || semanticAdmission == nil {
		t.Fatalf("activate semantic mixed-lineage admission=%v err=%v", semanticAdmission, err)
	}
	if err := semanticAdmission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}

	semanticFence := domain.Fence{LeaderEpoch: firstFence.LeaderEpoch, RunnerEpoch: semanticResumed.RunnerEpoch}
	lastStopped, lastResumed := postPublicationPauseResumeAt(t, fixture.db, semanticResumed, semanticFence, domain.StateMerging)
	if _, err := fixture.db.PostPublicationRearmProof(fixture.ctx, lastResumed.Ref, lastStopped); err != nil {
		t.Fatalf("semantic-to-normal rearm: %v", err)
	}
}

func TestGuardedMergeResumeRejectsMalformedHistoricalSemanticPayload(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	paused, err := fixture.db.Transition(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
		To: domain.StatePaused, ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
	}); err != nil {
		t.Fatal(err)
	}
	semanticResumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	semanticStopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, current.Ref, semanticStopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("semantic admission was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate semantic admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	semanticFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: semanticResumed.RunnerEpoch}
	_, normalPaused := postPublicationPauseAt(t, fixture.db, semanticResumed, semanticFence)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE events SET payload='not-json' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='retry_or_correction_exhausted'`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, paused.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: normalPaused.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_resume",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: normalPaused.RunnerEpoch}, EventPayload: `{}`,
	}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("malformed historical semantic payload accepted: %v", err)
	}
}

func TestMergeIntentForProofRejectsFutureRecoverySuffix(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	step := RunnerRecoveryLedger{
		Ref:                current.Ref,
		PriorTicketVersion: current.Version,
		PriorRunnerEpoch:   current.RunnerEpoch,
		PriorLeaderEpoch:   fence.LeaderEpoch,
		TicketVersion:      current.Version + 1,
		RunnerEpoch:        current.RunnerEpoch + 1,
		LeaderEpoch:        fence.LeaderEpoch + 1,
		CreatedAt:          time.Now().UTC(),
	}
	payload, err := runnerRecoveryPayload(step)
	if err != nil {
		t.Fatal(err)
	}
	step.RecoveryDigest = publicationIdentityDigest(payload)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, step.Ref.Channel, step.Ref.Project, step.Ref.Ticket, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch, step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch, step.RecoveryDigest, step.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	key := "merge/rearm/armed/" + string(domain.StateMerging)
	intent, found, err := fixture.db.MergeIntent(fixture.ctx, key)
	if err != nil || !found {
		t.Fatalf("merge intent found=%v err=%v", found, err)
	}
	if _, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, intent.HeadOID); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("future recovery suffix was ignored: %v", err)
	}
}

func TestTransitionGuardedMergeResumeRejectsTamperedAuthorityBeforeCommit(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Store, finalReviewFixture, Ticket)
	}{
		{
			name: "approval removed",
			mutate: func(db *Store, fixture finalReviewFixture, paused Ticket) {
				if _, err := db.db.ExecContext(fixture.ctx, `DELETE FROM approvals WHERE channel=? AND project_id=? AND ticket_id=?`, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "claim epoch mismatch",
			mutate: func(db *Store, fixture finalReviewFixture, paused Ticket) {
				if _, err := db.db.ExecContext(fixture.ctx, `UPDATE merge_intents SET claim_epoch=claim_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "pull request head mismatch",
			mutate: func(db *Store, fixture finalReviewFixture, paused Ticket) {
				if _, err := db.db.ExecContext(fixture.ctx, `UPDATE merge_intents SET head_oid=? WHERE channel=? AND project_id=? AND ticket_id=?`, strings.Repeat("f", 40), paused.Ref.Channel, paused.Ref.Project, paused.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
			defer fixture.db.Close()
			_, paused := postPublicationPauseAt(t, fixture.db, current, fence)
			tc.mutate(fixture.db, fixture, paused)
			if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
				Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
				To: domain.StateMerging, Trigger: "operator_resume",
				Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: paused.RunnerEpoch}, EventPayload: `{}`,
			}); !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("tampered merge resume accepted: %v", err)
			}
			stored, err := fixture.db.Ticket(fixture.ctx, paused.Ref)
			if err != nil || stored.State != domain.StatePaused || stored.Version != paused.Version || stored.ResumeState != domain.StateMerging {
				t.Fatalf("rejected merge resume mutated ticket=%+v err=%v", stored, err)
			}
		})
	}
}

func TestSemanticGuardedMergeRetryRecoversBeforeRearm(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	pausedResult, err := fixture.db.Transition(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
		To: domain.StatePaused, ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: pausedResult.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
	}); err != nil {
		t.Fatal(err)
	}
	resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
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
	leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "semantic-merge-retry-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("semantic retry fence changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(fixture.ctx, current.Ref)
	if err != nil || live.State != domain.StateMerging || live.Version != resumed.Version+1 || live.RunnerEpoch != resumed.RunnerEpoch+1 {
		t.Fatalf("semantic retry recovered ticket=%+v err=%v", live, err)
	}
	stopped, err := reopened.StoppedRuntimeTicket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := reopened.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryNeedsRearm {
		t.Fatalf("recovered semantic retry replay=%v err=%v", replay, err)
	}
	if capability, err := reopened.PostPublicationRearmProof(fixture.ctx, current.Ref, stopped); err != nil || capability == nil {
		t.Fatalf("semantic retry recovered capability=%v err=%v", capability, err)
	}
}

func TestSemanticGuardedMergeRetryAuthenticatesPausedLeaderHandoff(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()

	pausedResult, err := fixture.db.Transition(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
		To: domain.StatePaused, ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	leader, err := fixture.db.AcquireLeader(fixture.ctx, current.Ref.Channel, "semantic-merge-paused-leader-handoff")
	if err != nil || leader <= fence.LeaderEpoch {
		t.Fatalf("acquire paused leader=%d prior=%d err=%v", leader, fence.LeaderEpoch, err)
	}
	newFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: pausedResult.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_retry", Fence: newFence, EventPayload: `{"intent":"retry"}`,
	}); err != nil {
		t.Fatalf("semantic retry under new paused leader: %v", err)
	}
	resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil || resumed.State != domain.StateMerging || resumed.Version != pausedResult.Version+1 || resumed.RunnerEpoch != current.RunnerEpoch {
		t.Fatalf("resumed ticket=%+v err=%v", resumed, err)
	}
	stopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, current.Ref)
	if err != nil || stopped.Version != pausedResult.Version || stopped.RunnerEpoch != current.RunnerEpoch {
		t.Fatalf("stopped tuple=%+v err=%v", stopped, err)
	}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, current.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("paused-leader semantic rearm capability=%v err=%v", capability, err)
	}
}

func TestPostPublicationReconcilingResumeAuthenticatesBeforeCommit(t *testing.T) {
	t.Run("semantic retry rejects generic bypass and rearms", func(t *testing.T) {
		fixture, current, fence := preparePostPublicationRearmState(t, domain.StateReconciling)
		defer fixture.db.Close()
		pausedResult, err := fixture.db.Transition(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReconciling,
			To: domain.StatePaused, ResumeState: domain.StateReconciling, Trigger: "retry_or_correction_exhausted",
			Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		resume := Transition{
			Ref: current.Ref, ExpectedVersion: pausedResult.Version, From: domain.StatePaused,
			To: domain.StateReconciling, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
		}
		if _, err := fixture.db.Transition(fixture.ctx, resume); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("generic reconciling retry bypass=%v", err)
		}
		paused, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || paused.State != domain.StatePaused || paused.Version != pausedResult.Version || paused.ResumeState != domain.StateReconciling {
			t.Fatalf("generic bypass mutated paused ticket=%+v err=%v", paused, err)
		}
		if _, err := fixture.db.TransitionPostPublicationReconcileResume(fixture.ctx, resume); err != nil {
			t.Fatalf("authenticated reconciling retry: %v", err)
		}
		resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || resumed.State != domain.StateReconciling || resumed.Version != paused.Version+1 {
			t.Fatalf("resumed reconciling ticket=%+v err=%v", resumed, err)
		}
		stopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, current.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if replay, err := fixture.db.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryNeedsRearm {
			t.Fatalf("reconciling retry replay=%v err=%v", replay, err)
		}
		if capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, current.Ref, stopped); err != nil || capability == nil {
			t.Fatalf("reconciling semantic rearm capability=%v err=%v", capability, err)
		}
	})

	t.Run("tampered merge authority leaves ticket paused", func(t *testing.T) {
		fixture, current, fence := preparePostPublicationRearmState(t, domain.StateReconciling)
		defer fixture.db.Close()
		pausedResult, err := fixture.db.Transition(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReconciling,
			To: domain.StatePaused, ResumeState: domain.StateReconciling, Trigger: "retry_or_correction_exhausted",
			Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.TransitionPostPublicationReconcileResume(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: pausedResult.Version, From: domain.StatePaused,
			To: domain.StateReconciling, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
		}); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered reconciling retry accepted: %v", err)
		}
		stored, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || stored.State != domain.StatePaused || stored.Version != pausedResult.Version {
			t.Fatalf("rejected reconciling retry mutated ticket=%+v err=%v", stored, err)
		}
	})

	t.Run("semantic retry authenticates paused leader handoff", func(t *testing.T) {
		fixture, current, fence := preparePostPublicationRearmState(t, domain.StateReconciling)
		defer fixture.db.Close()
		pausedResult, err := fixture.db.Transition(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReconciling,
			To: domain.StatePaused, ResumeState: domain.StateReconciling, Trigger: "retry_or_correction_exhausted",
			Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		leader, err := fixture.db.AcquireLeader(fixture.ctx, current.Ref.Channel, "semantic-reconciling-paused-leader-handoff")
		if err != nil || leader <= fence.LeaderEpoch {
			t.Fatalf("acquire paused leader=%d prior=%d err=%v", leader, fence.LeaderEpoch, err)
		}
		newFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
		if _, err := fixture.db.TransitionPostPublicationReconcileResume(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: pausedResult.Version, From: domain.StatePaused,
			To: domain.StateReconciling, Trigger: "operator_retry", Fence: newFence, EventPayload: `{"intent":"retry"}`,
		}); err != nil {
			t.Fatalf("reconciling retry under new paused leader: %v", err)
		}
		resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || resumed.State != domain.StateReconciling || resumed.Version != pausedResult.Version+1 {
			t.Fatalf("leader-handoff reconciling ticket=%+v err=%v", resumed, err)
		}
	})

	t.Run("semantic retry survives restart before rearm", func(t *testing.T) {
		fixture, current, fence := preparePostPublicationRearmState(t, domain.StateReconciling)
		paused, err := fixture.db.Transition(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReconciling,
			To: domain.StatePaused, ResumeState: domain.StateReconciling, Trigger: "retry_or_correction_exhausted",
			Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.TransitionPostPublicationReconcileResume(fixture.ctx, Transition{
			Ref: current.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
			To: domain.StateReconciling, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
		}); err != nil {
			t.Fatal(err)
		}
		resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil {
			t.Fatal(err)
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
		leader, err := reopened.AcquireLeader(fixture.ctx, current.Ref.Channel, "semantic-reconciling-restart")
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, current.Ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("semantic reconciling fence changed=%d err=%v", changed, err)
		}
		live, err := reopened.Ticket(fixture.ctx, current.Ref)
		if err != nil || live.State != domain.StateReconciling || live.Version != resumed.Version+1 || live.RunnerEpoch != resumed.RunnerEpoch+1 {
			t.Fatalf("semantic reconciling recovered ticket=%+v err=%v", live, err)
		}
		stopped, err := reopened.StoppedRuntimeTicket(fixture.ctx, current.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if replay, err := reopened.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryNeedsRearm {
			t.Fatalf("semantic reconciling replay=%v err=%v", replay, err)
		}
		if capability, err := reopened.PostPublicationRearmProof(fixture.ctx, current.Ref, stopped); err != nil || capability == nil {
			t.Fatalf("semantic reconciling recovered capability=%v err=%v", capability, err)
		}
	})
}

func TestGuardedMergeRetryReplayRejectsUnrelatedSealedRuntime(t *testing.T) {
	fixture, current, _ := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	if err := fixture.db.SealRuntimeControl(fixture.ctx, current.Ref); err != nil {
		t.Fatal(err)
	}
	if replay, err := fixture.db.GuardedMergeRetryReplay(fixture.ctx, current.Ref); err != nil || replay != GuardedMergeRetryNotReplay {
		t.Fatalf("unrelated sealed runtime replay=%v err=%v", replay, err)
	}
}

func TestRuntimeAdmissionReadyKeepsGuardedMergeSealedUntilExactBegin(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateMerging)
	resumedFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	if ready, err := fixture.db.RuntimeAdmissionReady(fixture.ctx, resumed.Ref, resumed.Version, resumedFence); err != nil || ready {
		t.Fatalf("sealed merge admission ready=%v err=%v", ready, err)
	}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("runtime admission capability was not consumable")
		}
		admission = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if ready, err := fixture.db.RuntimeAdmissionReady(fixture.ctx, resumed.Ref, resumed.Version, resumedFence); err != nil || ready {
		t.Fatalf("armed merge admission ready=%v err=%v", ready, err)
	}
	if admission == nil {
		t.Fatal("runtime admission capability was not captured")
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if ready, err := fixture.db.RuntimeAdmissionReady(fixture.ctx, resumed.Ref, resumed.Version, resumedFence); err != nil || !ready {
		t.Fatalf("opened merge admission ready=%v err=%v", ready, err)
	}
	// A second complete control cycle may occur before the scheduler has had a
	// chance to re-observe and promote the original confirmed merge. The resume
	// authority must walk both exact triplets instead of requiring an
	// intermediate promotion as an artificial checkpoint.
	secondStopped, secondResumed := postPublicationPauseResumeAt(t, fixture.db, resumed, resumedFence, domain.StateMerging)
	secondFence := domain.Fence{LeaderEpoch: resumedFence.LeaderEpoch, RunnerEpoch: secondResumed.RunnerEpoch}
	secondCapability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, secondResumed.Ref, secondStopped)
	if err != nil {
		t.Fatalf("second merge rearm proof: %v", err)
	}
	var secondAdmission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, secondCapability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("second runtime admission capability was not consumable")
		}
		secondAdmission = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if secondAdmission == nil {
		t.Fatal("second runtime admission capability was not captured")
	}
	if err := secondAdmission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	mergeKey := "merge/rearm/armed/" + string(domain.StateMerging)
	intent, found, err := fixture.db.MergeIntent(fixture.ctx, mergeKey)
	if err != nil || !found {
		t.Fatalf("merge intent found=%v err=%v", found, err)
	}
	if _, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, intent.HeadOID); err != nil {
		t.Fatalf("confirmed merge did not survive two pre-promotion control cycles: %v", err)
	}
	mergeEffect, err := fixture.db.Effect(fixture.ctx, mergeKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ReconcileInvalidatedEffect(fixture.ctx, InvalidatedEffectObservation{
		Prior: EffectObservation{
			EffectFence: EffectFence{SemanticKey: mergeKey, Ref: secondResumed.Ref, TicketVersion: mergeEffect.TicketVersion, Fence: domain.Fence{LeaderEpoch: mergeEffect.LeaderEpoch, RunnerEpoch: mergeEffect.RunnerEpoch, ClaimEpoch: mergeEffect.ClaimEpoch}},
			Present:     true,
			Identity:    mergeEffect.ObservedIdentity,
		},
		Current: EffectFence{SemanticKey: mergeKey, Ref: secondResumed.Ref, TicketVersion: secondResumed.Version, Fence: secondFence},
	}); err != nil {
		t.Fatalf("promote merge proof after two control cycles: %v", err)
	}
	if _, err := fixture.db.TransitionGuardedMergeObserved(fixture.ctx, Transition{
		Ref: secondResumed.Ref, ExpectedVersion: secondResumed.Version, From: domain.StateMerging,
		To: domain.StateReconciling, Trigger: "merge_observed", Fence: secondFence, EventPayload: `{}`,
	}); err != nil {
		t.Fatal(err)
	}
	successor, err := fixture.db.Ticket(fixture.ctx, secondResumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := fixture.db.RuntimeAdmissionReady(fixture.ctx, successor.Ref, successor.Version, secondFence); err != nil || !ready {
		t.Fatalf("opened successor admission ready=%v err=%v", ready, err)
	}
	// The predecessor token cannot close a still-open successor. Controller
	// first seals the current Store tuple, after which the same old activity is
	// allowed to join only this exact authenticated merge_observed successor.
	if err := secondAdmission.SealStoreAdmission(fixture.ctx); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old merge admission sealed open successor: %v", err)
	}
	if err := fixture.db.SealRuntimeControl(fixture.ctx, successor.Ref); err != nil {
		t.Fatalf("seal reconciling successor: %v", err)
	}
	if err := secondAdmission.SealStoreAdmission(fixture.ctx); err != nil {
		t.Fatalf("join sealed reconciling successor with predecessor token: %v", err)
	}
	if ready, err := fixture.db.RuntimeAdmissionReady(fixture.ctx, successor.Ref, successor.Version, secondFence); err != nil || ready {
		t.Fatalf("sealed successor remained admitted ready=%v err=%v", ready, err)
	}
}

func TestManualMergeObservationCarriesRuntimeAdmissionToSealedReconcile(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateWaitingManualMerge)
	defer fixture.db.Close()
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateWaitingManualMerge)
	resumedFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("manual runtime admission capability was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate manual admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observed := contracts.PublishedPullRequestObservation{
		Identity: publication.PullRequest, State: "MERGED", Merged: true,
		MergeCommit: strings.Repeat("9", 40), BaseHeadOID: publication.PullRequest.BaseOID,
	}
	authority, err := fixture.db.BindManualMergeObservation(fixture.ctx, resumed.Ref, NewManualMergeObservation(publication, observed), resumedFence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordManualMergeObservation(fixture.ctx, Transition{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, From: domain.StateWaitingManualMerge,
		To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: resumedFence,
	}, authority); err != nil {
		t.Fatal(err)
	}
	successor, err := fixture.db.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || successor.State != domain.StateReconciling || successor.Version != resumed.Version+1 || successor.RunnerEpoch != resumed.RunnerEpoch {
		t.Fatalf("manual reconciling successor=%+v err=%v", successor, err)
	}
	if err := admission.SealStoreAdmission(fixture.ctx); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old manual admission sealed open successor: %v", err)
	}
	if err := fixture.db.SealRuntimeControl(fixture.ctx, successor.Ref); err != nil {
		t.Fatalf("seal manual reconciling successor: %v", err)
	}
	if err := admission.SealStoreAdmission(fixture.ctx); err != nil {
		t.Fatalf("join sealed manual reconciling successor: %v", err)
	}
	sealed, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, successor.Ref)
	if err != nil || sealed.Version != successor.Version || sealed.RunnerEpoch != successor.RunnerEpoch {
		t.Fatalf("manual successor seal=%+v err=%v", sealed, err)
	}
}

func TestManualMergeObservationRecoversImmediatelyAfterCrash(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateWaitingManualMerge)
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateWaitingManualMerge)
	resumedFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("manual crash admission was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate manual crash admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observed := contracts.PublishedPullRequestObservation{
		Identity: publication.PullRequest, State: "MERGED", Merged: true,
		MergeCommit: strings.Repeat("8", 40), BaseHeadOID: publication.PullRequest.BaseOID,
	}
	authority, err := fixture.db.BindManualMergeObservation(fixture.ctx, resumed.Ref, NewManualMergeObservation(publication, observed), resumedFence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordManualMergeObservation(fixture.ctx, Transition{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, From: domain.StateWaitingManualMerge,
		To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: resumedFence,
	}, authority); err != nil {
		t.Fatal(err)
	}
	reconciling, err := fixture.db.Ticket(fixture.ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
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
	leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "manual-observation-immediate-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("manual observation fence changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || live.State != domain.StateReconciling || live.Version != reconciling.Version+1 || live.RunnerEpoch != reconciling.RunnerEpoch+1 {
		t.Fatalf("manual recovered reconciling ticket=%+v err=%v", live, err)
	}
	recoveredStopped, err := reopened.StoppedRuntimeTicket(fixture.ctx, live.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if capability, err := reopened.PostPublicationRearmProof(fixture.ctx, live.Ref, recoveredStopped); err != nil || capability == nil {
		t.Fatalf("manual observation recovered capability=%v err=%v", capability, err)
	}
}

func TestGuardedMergePromotionSurvivesCrashBeforeMergeObserved(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateMerging)
	resumedFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("runtime admission capability was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate resumed merge admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	secondStopped, secondResumed := postPublicationPauseResumeAt(t, fixture.db, resumed, resumedFence, domain.StateMerging)
	secondFence := domain.Fence{LeaderEpoch: resumedFence.LeaderEpoch, RunnerEpoch: secondResumed.RunnerEpoch}
	secondCapability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, secondResumed.Ref, secondStopped)
	if err != nil {
		t.Fatalf("second pre-promotion rearm proof: %v", err)
	}
	var secondAdmission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, secondCapability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("second runtime admission capability was not consumable")
		}
		secondAdmission = value
		return nil
	}); err != nil || secondAdmission == nil {
		t.Fatalf("activate second resumed merge admission=%v err=%v", secondAdmission, err)
	}
	if err := secondAdmission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	resumed, resumedFence = secondResumed, secondFence
	mergeKey := "merge/rearm/armed/" + string(domain.StateMerging)
	mergeEffect, err := fixture.db.Effect(fixture.ctx, mergeKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ReconcileInvalidatedEffect(fixture.ctx, InvalidatedEffectObservation{
		Prior:   EffectObservation{EffectFence: EffectFence{SemanticKey: mergeKey, Ref: resumed.Ref, TicketVersion: mergeEffect.TicketVersion, Fence: domain.Fence{LeaderEpoch: mergeEffect.LeaderEpoch, RunnerEpoch: mergeEffect.RunnerEpoch, ClaimEpoch: mergeEffect.ClaimEpoch}}, Present: true, Identity: mergeEffect.ObservedIdentity},
		Current: EffectFence{SemanticKey: mergeKey, Ref: resumed.Ref, TicketVersion: resumed.Version, Fence: resumedFence},
	}); err != nil {
		t.Fatalf("promote merge before crash: %v", err)
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
	intent, found, err := reopened.MergeIntent(fixture.ctx, mergeKey)
	if err != nil || !found {
		t.Fatalf("reopened merge intent found=%v err=%v", found, err)
	}
	if _, err := reopened.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, intent.HeadOID); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("sealed crash endpoint exposed merge proof: %v", err)
	}
	leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "merge-promotion-crash")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("fence promoted merge changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || live.Version != resumed.Version+1 || live.RunnerEpoch != resumed.RunnerEpoch+1 {
		t.Fatalf("recovered promoted merge ticket=%+v err=%v", live, err)
	}
	recoveredStopped, err := reopened.StoppedRuntimeTicket(fixture.ctx, live.Ref)
	if err != nil {
		t.Fatal(err)
	}
	recoveredCapability, err := reopened.PostPublicationRearmProof(fixture.ctx, live.Ref, recoveredStopped)
	if err != nil {
		t.Fatalf("rearm promoted merge after crash: %v", err)
	}
	var recoveredAdmission *RuntimeAdmissionCapability
	if err := reopened.ActivateRearm(fixture.ctx, recoveredCapability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("recovered runtime admission capability was not consumable")
		}
		recoveredAdmission = value
		return nil
	}); err != nil || recoveredAdmission == nil {
		t.Fatalf("activate recovered merge admission=%v err=%v", recoveredAdmission, err)
	}
	if err := recoveredAdmission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	mergeEffect, err = reopened.Effect(fixture.ctx, mergeKey)
	if err != nil {
		t.Fatal(err)
	}
	liveFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}
	if _, err := reopened.ReconcileInvalidatedEffect(fixture.ctx, InvalidatedEffectObservation{
		Prior:   EffectObservation{EffectFence: EffectFence{SemanticKey: mergeKey, Ref: live.Ref, TicketVersion: mergeEffect.TicketVersion, Fence: domain.Fence{LeaderEpoch: mergeEffect.LeaderEpoch, RunnerEpoch: mergeEffect.RunnerEpoch, ClaimEpoch: mergeEffect.ClaimEpoch}}, Present: true, Identity: mergeEffect.ObservedIdentity},
		Current: EffectFence{SemanticKey: mergeKey, Ref: live.Ref, TicketVersion: live.Version, Fence: liveFence},
	}); err != nil {
		t.Fatalf("promote recovered merge to live fence: %v", err)
	}
	if _, err := reopened.TransitionGuardedMergeObserved(fixture.ctx, Transition{Ref: live.Ref, ExpectedVersion: live.Version, From: domain.StateMerging, To: domain.StateReconciling, Trigger: "merge_observed", Fence: liveFence, EventPayload: `{}`}); err != nil {
		t.Fatalf("consume recovered promoted merge: %v", err)
	}
}

func TestSemanticGuardedMergeObservationRecoversBeforeRearm(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	paused, err := fixture.db.Transition(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
		To: domain.StatePaused, ResumeState: domain.StateMerging, Trigger: "retry_or_correction_exhausted",
		Fence: fence, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_retry", Fence: fence, EventPayload: `{"intent":"retry"}`,
	}); err != nil {
		t.Fatal(err)
	}
	resumed, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := fixture.db.StoppedRuntimeTicket(fixture.ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
	if err != nil {
		t.Fatalf("semantic pre-observation rearm: %v", err)
	}
	var admission *RuntimeAdmissionCapability
	if err := fixture.db.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("semantic runtime admission was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate semantic admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	mergeKey := "merge/rearm/armed/" + string(domain.StateMerging)
	effect, err := fixture.db.Effect(fixture.ctx, mergeKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ReconcileInvalidatedEffect(fixture.ctx, InvalidatedEffectObservation{
		Prior: EffectObservation{
			EffectFence: EffectFence{SemanticKey: mergeKey, Ref: resumed.Ref, TicketVersion: effect.TicketVersion, Fence: domain.Fence{LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch}},
			Present:     true,
			Identity:    effect.ObservedIdentity,
		},
		Current: EffectFence{SemanticKey: mergeKey, Ref: resumed.Ref, TicketVersion: resumed.Version, Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}},
	}); err != nil {
		t.Fatalf("promote semantic merge proof: %v", err)
	}
	if _, err := fixture.db.TransitionGuardedMergeObserved(fixture.ctx, Transition{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, From: domain.StateMerging,
		To: domain.StateReconciling, Trigger: "merge_observed",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("record semantic merge observation: %v", err)
	}
	reconciling, err := fixture.db.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || reconciling.State != domain.StateReconciling || reconciling.Version != resumed.Version+1 || reconciling.RunnerEpoch != resumed.RunnerEpoch {
		t.Fatalf("semantic reconciling endpoint=%+v err=%v", reconciling, err)
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
	leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "semantic-merge-observed-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("semantic merge-observed fence changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || live.State != domain.StateReconciling || live.Version != reconciling.Version+1 || live.RunnerEpoch != reconciling.RunnerEpoch+1 {
		t.Fatalf("semantic recovered reconciling ticket=%+v err=%v", live, err)
	}
	recoveredStopped, err := reopened.StoppedRuntimeTicket(fixture.ctx, live.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if capability, err := reopened.PostPublicationRearmProof(fixture.ctx, live.Ref, recoveredStopped); err != nil || capability == nil {
		t.Fatalf("semantic merge-observed recovered capability=%v err=%v", capability, err)
	}
}

func TestGuardedMergeResumeAuthenticatesRecoveryThenControlBeforePromotion(t *testing.T) {
	fixture, current, _ := preparePostPublicationRearmState(t, domain.StateMerging)
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
	leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "merge-recovery-before-control")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("fence pre-control merge changed=%d err=%v", changed, err)
	}
	recovered, err := reopened.Ticket(fixture.ctx, current.Ref)
	if err != nil || recovered.Version != current.Version+1 || recovered.RunnerEpoch != current.RunnerEpoch+1 {
		t.Fatalf("recovered merge ticket=%+v err=%v", recovered, err)
	}
	recoveredFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch}
	stopped, resumed := postPublicationPauseResumeAt(t, reopened, recovered, recoveredFence, domain.StateMerging)
	resumedFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}
	capability, err := reopened.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
	if err != nil {
		t.Fatalf("rearm after recovery/control: %v", err)
	}
	var admission *RuntimeAdmissionCapability
	if err := reopened.ActivateRearm(fixture.ctx, capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("recovery/control admission capability was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate recovery/control admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	mergeKey := "merge/rearm/armed/" + string(domain.StateMerging)
	intent, found, err := reopened.MergeIntent(fixture.ctx, mergeKey)
	if err != nil || !found {
		t.Fatalf("merge intent found=%v err=%v", found, err)
	}
	if _, err := reopened.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, intent.HeadOID); err != nil {
		t.Fatalf("confirmed merge did not survive recovery then control: %v", err)
	}
	effect, err := reopened.Effect(fixture.ctx, mergeKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.ReconcileInvalidatedEffect(fixture.ctx, InvalidatedEffectObservation{
		Prior:   EffectObservation{EffectFence: EffectFence{SemanticKey: mergeKey, Ref: resumed.Ref, TicketVersion: effect.TicketVersion, Fence: domain.Fence{LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch}}, Present: true, Identity: effect.ObservedIdentity},
		Current: EffectFence{SemanticKey: mergeKey, Ref: resumed.Ref, TicketVersion: resumed.Version, Fence: resumedFence},
	}); err != nil {
		t.Fatalf("promote merge after recovery/control: %v", err)
	}
	if _, err := reopened.TransitionGuardedMergeObserved(fixture.ctx, Transition{Ref: resumed.Ref, ExpectedVersion: resumed.Version, From: domain.StateMerging, To: domain.StateReconciling, Trigger: "merge_observed", Fence: resumedFence, EventPayload: `{}`}); err != nil {
		t.Fatalf("consume merge after recovery/control: %v", err)
	}
}

func TestGuardedMergeResumeRejectsCompetingApprovalStateChange(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
	defer fixture.db.Close()
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, current.Version, "forged_competing_transition", domain.StateWaitingApproval, domain.StateBlocked, `{}`, "2026-08-31T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	_, paused := postPublicationPauseAt(t, fixture.db, current, fence)
	if _, err := fixture.db.TransitionGuardedMergeResume(fixture.ctx, Transition{
		Ref: paused.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateMerging, Trigger: "operator_resume",
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: paused.RunnerEpoch}, EventPayload: `{}`,
	}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("competing approval transition accepted: %v", err)
	}
}

func TestPostPublicationRearmProofRejectsMergingWithoutApprovalOrExactClaim(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Store, finalReviewFixture, Ticket)
	}{
		{
			name: "approval removed",
			mutate: func(db *Store, fixture finalReviewFixture, current Ticket) {
				if _, err := db.db.ExecContext(fixture.ctx, `DELETE FROM approvals WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket); err != nil {
					t.Fatalf("delete approval: %v", err)
				}
			},
		},
		{
			name: "same endpoint claim mismatch",
			mutate: func(db *Store, fixture finalReviewFixture, current Ticket) {
				if _, err := db.db.ExecContext(fixture.ctx, `UPDATE merge_intents SET claim_epoch=claim_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket); err != nil {
					t.Fatalf("tamper merge claim epoch: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
			defer fixture.db.Close()
			stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateMerging)
			tc.mutate(fixture.db, fixture, resumed)
			if _, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped); !errors.Is(err, ErrControlNotDrained) {
				t.Fatalf("%s merging rearmed: %v", tc.name, err)
			}
		})
	}
}

func TestPostPublicationRearmProofAuthenticatesMergingAndReconciling(t *testing.T) {
	for _, target := range []domain.State{domain.StateMerging, domain.StateReconciling} {
		t.Run(string(target), func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, target)
			defer fixture.db.Close()
			stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, target)
			capability, err := fixture.db.PostPublicationRearmProof(t.Context(), resumed.Ref, stopped)
			if err != nil || capability == nil {
				t.Fatalf("state=%s capability=%v err=%v", target, capability, err)
			}
		})
	}
}

func TestTransitionGuardedMergeObservedRejectsMissingAuthorityAndNonCanonicalPayload(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*Store, Ticket)
		payload string
	}{
		{
			name: "missing merge intent",
			mutate: func(db *Store, ticket Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `DELETE FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
					t.Fatalf("delete merge intent: %v", err)
				}
			},
			payload: "{}",
		},
		{
			name:    "noncanonical payload",
			mutate:  func(*Store, Ticket) {},
			payload: `{"forged":true}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, domain.StateMerging)
			defer fixture.db.Close()
			tc.mutate(fixture.db, current)
			if _, err := fixture.db.TransitionGuardedMergeObserved(fixture.ctx, Transition{
				Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
				To: domain.StateReconciling, Trigger: "merge_observed", Fence: fence, EventPayload: tc.payload,
			}); !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("guarded merge observation accepted %s: %v", tc.name, err)
			}
			stored, err := fixture.db.Ticket(fixture.ctx, current.Ref)
			if err != nil || stored.State != domain.StateMerging || stored.Version != current.Version {
				t.Fatalf("failed guarded observation mutated ticket=%+v err=%v", stored, err)
			}
		})
	}
}

func TestGuardedReconcilingAuthoritiesRejectNonCanonicalMergeObservation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Store, Ticket)
	}{
		{
			name: "payload",
			mutate: func(db *Store, ticket Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `UPDATE events SET payload='{"unexpected":true}' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='merge_observed' AND from_state='merging' AND to_state='reconciling'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version); err != nil {
					t.Fatalf("tamper guarded merge payload: %v", err)
				}
			},
		},
		{
			name: "second state transition",
			mutate: func(db *Store, ticket Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,'reconciling','blocked','{}',?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version, "tampered_transition", now()); err != nil {
					t.Fatalf("append alternate guarded transition: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, domain.StateReconciling)
			intent, found, err := fixture.db.MergeIntent(fixture.ctx, "merge/rearm/armed/reconciling")
			if err != nil || !found {
				fixture.db.Close()
				t.Fatalf("load guarded merge intent found=%v err=%v", found, err)
			}
			tc.mutate(fixture.db, current)
			if _, err := fixture.db.MergeIntentForProof(fixture.ctx, intent.RepositoryHost, intent.RepositoryOwner, intent.RepositoryName, intent.BaseRef, intent.OriginalBaseOID, strings.Repeat("d", len(intent.HeadOID))); !errors.Is(err, ErrEvidenceConflict) {
				fixture.db.Close()
				t.Fatalf("noncanonical guarded merge observation exposed proof intent: %v", err)
			}
			_, paused := postPublicationPauseAt(t, fixture.db, current, fence)
			if _, err := fixture.db.TransitionPostPublicationReconcileResume(fixture.ctx, Transition{
				Ref: current.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
				To: domain.StateReconciling, Trigger: "operator_resume",
				Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: paused.RunnerEpoch}, EventPayload: `{}`,
			}); !errors.Is(err, ErrEvidenceConflict) {
				fixture.db.Close()
				t.Fatalf("noncanonical guarded merge observation resumed: %v", err)
			}
			stored, err := fixture.db.Ticket(fixture.ctx, current.Ref)
			if err != nil || stored.State != domain.StatePaused || stored.Version != paused.Version || stored.ResumeState != domain.StateReconciling {
				fixture.db.Close()
				t.Fatalf("rejected guarded reconcile resume mutated ticket=%+v err=%v", stored, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPostPublicationRearmProofAuthenticatesManualReconcilingAfterRestart(t *testing.T) {
	fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeManual)
	completeFinalReview(t, fixture)
	current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReviewing, To: domain.StateWaitingManualMerge, Trigger: "review_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observed := contracts.PublishedPullRequestObservation{Identity: publication.PullRequest, State: "MERGED", Merged: true, MergeCommit: strings.Repeat("d", 40), BaseHeadOID: publication.PullRequest.BaseOID}
	authority, err := fixture.db.BindManualMergeObservation(fixture.ctx, current.Ref, NewManualMergeObservation(publication, observed), domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordManualMergeObservation(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingManualMerge, To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: authority.CurrentFence}, authority); err != nil {
		t.Fatal(err)
	}
	reconciling, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, resumed := postPublicationPauseResumeAt(t, fixture.db, reconciling, domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: reconciling.RunnerEpoch}, domain.StateReconciling)
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
	leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "manual-reconciling-rearm")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("manual reconciling fence changed=%d err=%v", changed, err)
	}
	recovered, err := reopened.StoppedRuntimeTicket(fixture.ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := reopened.PostPublicationRearmProof(fixture.ctx, resumed.Ref, recovered)
	if err != nil || capability == nil {
		t.Fatalf("manual reconciling rearm capability=%v err=%v", capability, err)
	}
	if err := reopened.ValidateManualMergeObservation(fixture.ctx, resumed.Ref, NewManualMergeObservation(publication, observed)); err != nil {
		t.Fatalf("manual reconciling observation after control recovery: %v", err)
	}
}

func TestManualReconcilingSemanticRetryAuthenticatesBeforeCommit(t *testing.T) {
	fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeManual)
	completeFinalReview(t, fixture)
	current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReviewing, To: domain.StateWaitingManualMerge, Trigger: "review_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observed := contracts.PublishedPullRequestObservation{Identity: publication.PullRequest, State: "MERGED", Merged: true, MergeCommit: strings.Repeat("e", 40), BaseHeadOID: publication.PullRequest.BaseOID}
	authority, err := fixture.db.BindManualMergeObservation(fixture.ctx, current.Ref, NewManualMergeObservation(publication, observed), domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordManualMergeObservation(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingManualMerge, To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: authority.CurrentFence}, authority); err != nil {
		t.Fatal(err)
	}
	reconciling, err := fixture.db.Ticket(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	reconcileFence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: reconciling.RunnerEpoch}
	paused, err := fixture.db.Transition(fixture.ctx, Transition{
		Ref: reconciling.Ref, ExpectedVersion: reconciling.Version, From: domain.StateReconciling,
		To: domain.StatePaused, ResumeState: domain.StateReconciling, Trigger: "retry_or_correction_exhausted",
		Fence: reconcileFence, EventPayload: `{"reason":"retry_budget"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionPostPublicationReconcileResume(fixture.ctx, Transition{
		Ref: reconciling.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused,
		To: domain.StateReconciling, Trigger: "operator_retry", Fence: reconcileFence, EventPayload: `{"intent":"retry"}`,
	}); err != nil {
		t.Fatalf("manual reconciling semantic retry: %v", err)
	}
	resumed, err := fixture.db.Ticket(fixture.ctx, reconciling.Ref)
	if err != nil || resumed.State != domain.StateReconciling || resumed.Version != paused.Version+1 {
		t.Fatalf("manual semantic resumed ticket=%+v err=%v", resumed, err)
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
	leader, err := reopened.AcquireLeader(fixture.ctx, reconciling.Ref.Channel, "manual-semantic-reconciling-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, reconciling.Ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("manual semantic fence changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(fixture.ctx, reconciling.Ref)
	if err != nil || live.State != domain.StateReconciling || live.Version != resumed.Version+1 || live.RunnerEpoch != resumed.RunnerEpoch+1 {
		t.Fatalf("manual semantic recovered ticket=%+v err=%v", live, err)
	}
	stopped, err := reopened.StoppedRuntimeTicket(fixture.ctx, reconciling.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if capability, err := reopened.PostPublicationRearmProof(fixture.ctx, reconciling.Ref, stopped); err != nil || capability == nil {
		t.Fatalf("manual semantic recovered capability=%v err=%v", capability, err)
	}
	if err := reopened.ValidateManualMergeObservation(fixture.ctx, reconciling.Ref, NewManualMergeObservation(publication, observed)); err != nil {
		t.Fatalf("manual observation after semantic recovery: %v", err)
	}
}

func TestPostPublicationRearmProofAfterRestartAcrossStates(t *testing.T) {
	for _, target := range []domain.State{
		domain.StateReviewing,
		domain.StateWaitingApproval,
		domain.StateWaitingManualMerge,
		domain.StateMerging,
		domain.StateReconciling,
	} {
		t.Run(string(target), func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, target)
			_, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, target)

			var path string
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
				t.Fatalf("database path=%q err=%v", path, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(fixture.ctx, path)
			if err != nil {
				t.Fatalf("reopen store: %v", err)
			}
			defer reopened.Close()
			leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "postpub-state-reopen-"+string(target))
			if err != nil {
				t.Fatalf("acquire replacement leader: %v", err)
			}
			if changed, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
				t.Fatalf("fence changed=%d err=%v", changed, err)
			}
			recovered, err := reopened.StoppedRuntimeTicket(fixture.ctx, resumed.Ref)
			if err != nil {
				t.Fatalf("load recovered stop tuple: %v", err)
			}
			capability, err := reopened.PostPublicationRearmProof(fixture.ctx, resumed.Ref, recovered)
			if err != nil || capability == nil {
				t.Fatalf("state=%s capability=%v err=%v", target, capability, err)
			}
		})
	}
}

func preparePostPublicationRearmState(t *testing.T, target domain.State) (finalReviewFixture, Ticket, domain.Fence) {
	t.Helper()
	if target == domain.StatePublishing {
		db, ctx, ticket, fence := publicationLifecycleFixture(t)
		return finalReviewFixture{db: db, ctx: ctx, ticket: ticket, fence: fence}, ticket, fence
	}
	if target == domain.StateWaitingCI {
		db, ticket, fence := ciAuthorityPublishedFixture(t)
		return finalReviewFixture{db: db, ctx: t.Context(), ticket: ticket, fence: fence}, ticket, fence
	}
	mergeMode := domain.MergeGuarded
	if target == domain.StateWaitingManualMerge {
		mergeMode = domain.MergeManual
	}
	fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, mergeMode)
	if target != domain.StateReviewing {
		completeFinalReview(t, fixture)
		current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
		to := domain.StateWaitingApproval
		if target == domain.StateWaitingManualMerge {
			to = domain.StateWaitingManualMerge
		}
		if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateReviewing, To: to, Trigger: "review_pass", Fence: fence, EventPayload: `{}`}); err != nil {
			t.Fatal(err)
		}
	}
	current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
	if target == domain.StateMerging || target == domain.StateReconciling {
		if current.State != domain.StateWaitingApproval {
			t.Fatalf("guarded merge setup expected waiting_approval for %s, got %s", target, current.State)
		}
		if _, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{
			Ref: current.Ref, ExpectedVersion: current.Version, Fence: fence,
			ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 501, Decision: "approved",
		}}); err != nil {
			t.Fatalf("approve guarded merge: %v", err)
		}
		current, err = fixture.db.Ticket(fixture.ctx, current.Ref)
		if err != nil || current.State != domain.StateMerging {
			t.Fatalf("guarded merging ticket=%+v err=%v", current, err)
		}
		fence = domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
		bindTerminalMergeEffect(t, fixture.db, fixture, current, fence, "merge/rearm/armed/"+string(target))
		if target == domain.StateReconciling {
			spec, err := statemachine.LoadEmbeddedApproved()
			if err != nil {
				t.Fatalf("load normative state machine: %v", err)
			}
			if transition, err := spec.Select(string(domain.StateMerging), "merge_observed", map[string]bool{
				"source_head_equals_reviewed_head": true,
				"protected_branch_contains_merge":  true,
			}); err != nil || transition.To != string(domain.StateReconciling) {
				t.Fatalf("select normative guarded merge observation transition=%+v err=%v", transition, err)
			}
			if _, err := fixture.db.Transition(fixture.ctx, Transition{
				Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
				To: domain.StateReconciling, Trigger: "merge_observed", Fence: fence, EventPayload: `{}`,
			}); !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("generic guarded merge observation bypass=%v", err)
			}
			if _, err := fixture.db.TransitionGuardedMergeObserved(fixture.ctx, Transition{
				Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateMerging,
				To: domain.StateReconciling, Trigger: "merge_observed", Fence: fence, EventPayload: `{}`,
			}); err != nil {
				t.Fatalf("record guarded merge observation: %v", err)
			}
			current, err = fixture.db.Ticket(fixture.ctx, current.Ref)
			if err != nil || current.State != domain.StateReconciling {
				t.Fatalf("guarded reconciling ticket=%+v err=%v", current, err)
			}
			fence = domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
		}
	}
	return fixture, current, fence
}

func TestFenceRecoveredRunnersAcceptsArmedPostPublicationRearm(t *testing.T) {
	for _, target := range []domain.State{domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing, domain.StateWaitingApproval, domain.StateWaitingManualMerge, domain.StateMerging, domain.StateReconciling} {
		t.Run(string(target), func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, target)
			stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, target)
			capability, err := fixture.db.PostPublicationRearmProof(t.Context(), resumed.Ref, stopped)
			if err != nil || capability == nil {
				t.Fatalf("pre-crash rearm capability=%v err=%v", capability, err)
			}
			consumed := false
			if err := fixture.db.ActivateRearm(t.Context(), capability, func(admission *RuntimeAdmissionCapability) error {
				_, _, _, ok := admission.ConsumeRuntimeAdmission()
				consumed = ok
				return nil
			}); err != nil || !consumed {
				t.Fatalf("activate before crash consumed=%v err=%v", consumed, err)
			}
			var path string
			if err := fixture.db.db.QueryRowContext(t.Context(), `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
				t.Fatalf("database path=%q err=%v", path, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("reopen store: %v", err)
			}
			defer reopened.Close()
			leader, err := reopened.AcquireLeader(t.Context(), domain.ChannelDev, "armed-postpub-reopen-"+string(target))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := reopened.FenceRecoveredRunners(t.Context(), domain.ChannelDev, leader); err != nil || changed != 1 {
				t.Fatalf("armed recovery changed=%d err=%v", changed, err)
			}
		})
	}
}

func TestFenceRecoveredRunnersAcceptsRepeatedArmedPostPublicationCrashes(t *testing.T) {
	for _, target := range []domain.State{domain.StatePublishing, domain.StateWaitingCI, domain.StateReviewing, domain.StateWaitingApproval, domain.StateWaitingManualMerge, domain.StateMerging, domain.StateReconciling} {
		t.Run(string(target), func(t *testing.T) {
			fixture, current, fence := preparePostPublicationRearmState(t, target)
			stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, target)
			capability, err := fixture.db.PostPublicationRearmProof(t.Context(), resumed.Ref, stopped)
			if err != nil || capability == nil {
				t.Fatalf("first rearm capability=%v err=%v", capability, err)
			}
			if err := fixture.db.ActivateRearm(t.Context(), capability, func(admission *RuntimeAdmissionCapability) error {
				if _, _, _, ok := admission.ConsumeRuntimeAdmission(); !ok {
					return errors.New("first admission capability was not consumed")
				}
				return nil
			}); err != nil {
				t.Fatalf("first activate: %v", err)
			}
			var path string
			if err := fixture.db.db.QueryRowContext(t.Context(), `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
				t.Fatalf("first database path=%q err=%v", path, err)
			}
			if err := fixture.db.Close(); err != nil {
				t.Fatal(err)
			}
			first, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("first reopen: %v", err)
			}
			leader2, err := first.AcquireLeader(t.Context(), domain.ChannelDev, "second-crash-first-reopen-"+string(target))
			if err != nil {
				first.Close()
				t.Fatal(err)
			}
			if changed, err := first.FenceRecoveredRunners(t.Context(), domain.ChannelDev, leader2); err != nil || changed != 1 {
				first.Close()
				t.Fatalf("first fence changed=%d err=%v", changed, err)
			}
			firstLive, err := first.Ticket(t.Context(), resumed.Ref)
			if err != nil {
				first.Close()
				t.Fatal(err)
			}
			stoppedAgain, err := first.StoppedRuntimeTicket(t.Context(), resumed.Ref)
			if err != nil {
				first.Close()
				t.Fatalf("load immutable stop after first fence: %v", err)
			}
			secondCapability, err := first.PostPublicationRearmProof(t.Context(), resumed.Ref, stoppedAgain)
			if err != nil || secondCapability == nil {
				first.Close()
				t.Fatalf("second rearm capability=%v err=%v", secondCapability, err)
			}
			if err := first.ActivateRearm(t.Context(), secondCapability, func(admission *RuntimeAdmissionCapability) error {
				if _, _, _, ok := admission.ConsumeRuntimeAdmission(); !ok {
					return errors.New("second admission capability was not consumed")
				}
				return nil
			}); err != nil {
				first.Close()
				t.Fatalf("second activate: %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			second, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("second reopen: %v", err)
			}
			defer second.Close()
			leader3, err := second.AcquireLeader(t.Context(), domain.ChannelDev, "second-crash-second-reopen-"+string(target))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := second.FenceRecoveredRunners(t.Context(), domain.ChannelDev, leader3); err != nil || changed != 1 {
				t.Fatalf("second fence changed=%d err=%v", changed, err)
			}
			secondLive, err := second.Ticket(t.Context(), resumed.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if secondLive.Version != firstLive.Version+1 || secondLive.RunnerEpoch != firstLive.RunnerEpoch+1 {
				t.Fatalf("second fence counters first=%+v second=%+v", firstLive, secondLive)
			}
			thirdStopped, err := second.StoppedRuntimeTicket(t.Context(), resumed.Ref)
			if err != nil {
				t.Fatal(err)
			}
			thirdCapability, err := second.PostPublicationRearmProof(t.Context(), resumed.Ref, thirdStopped)
			if err != nil || thirdCapability == nil {
				t.Fatalf("third rearm capability=%v err=%v", thirdCapability, err)
			}
			if err := second.ActivateRearm(t.Context(), thirdCapability, func(admission *RuntimeAdmissionCapability) error {
				if _, _, _, ok := admission.ConsumeRuntimeAdmission(); !ok {
					return errors.New("third admission capability was not consumed")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var thirdPath string
			if err := second.db.QueryRowContext(t.Context(), `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&thirdPath); err != nil || thirdPath == "" {
				t.Fatalf("third database path=%q err=%v", thirdPath, err)
			}
			if err := second.Close(); err != nil {
				t.Fatal(err)
			}
			third, err := Open(t.Context(), thirdPath)
			if err != nil {
				t.Fatalf("third reopen: %v", err)
			}
			defer third.Close()
			leader4, err := third.AcquireLeader(t.Context(), domain.ChannelDev, "second-crash-third-reopen-"+string(target))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := third.FenceRecoveredRunners(t.Context(), domain.ChannelDev, leader4); err != nil || changed != 1 {
				t.Fatalf("third fence changed=%d err=%v", changed, err)
			}
			thirdLive, err := third.Ticket(t.Context(), resumed.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if thirdLive.Version != secondLive.Version+1 || thirdLive.RunnerEpoch != secondLive.RunnerEpoch+1 {
				t.Fatalf("third fence counters second=%+v third=%+v", secondLive, thirdLive)
			}
			finalStopped, err := third.StoppedRuntimeTicket(t.Context(), resumed.Ref)
			if err != nil {
				t.Fatal(err)
			}
			finalCapability, err := third.PostPublicationRearmProof(t.Context(), resumed.Ref, finalStopped)
			if err != nil || finalCapability == nil {
				t.Fatalf("final rearm capability=%v err=%v", finalCapability, err)
			}
			if err := third.ActivateRearm(t.Context(), finalCapability, func(admission *RuntimeAdmissionCapability) error {
				if _, _, _, ok := admission.ConsumeRuntimeAdmission(); !ok {
					return errors.New("final admission capability was not consumed")
				}
				return nil
			}); err != nil {
				t.Fatalf("final activate: %v", err)
			}
		})
	}
}

func TestFenceRecoveredRunnersRejectsTamperedArmedPostPublicationAuthority(t *testing.T) {
	fixture, current, fence := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateWaitingApproval)
	capability, err := fixture.db.PostPublicationRearmProof(t.Context(), resumed.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("rearm capability=%v err=%v", capability, err)
	}
	if err := fixture.db.ActivateRearm(t.Context(), capability, func(admission *RuntimeAdmissionCapability) error {
		_, _, _, ok := admission.ConsumeRuntimeAdmission()
		if !ok {
			return errors.New("admission capability was not consumed")
		}
		return nil
	}); err != nil {
		t.Fatalf("activate before tamper: %v", err)
	}
	if _, err := fixture.db.db.ExecContext(t.Context(), `UPDATE runtime_ticket_controls SET authority_leader_epoch=authority_leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket); err != nil {
		t.Fatalf("tamper armed authority: %v", err)
	}
	var path string
	if err := fixture.db.db.QueryRowContext(t.Context(), `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("database path=%q err=%v", path, err)
	}
	if err := fixture.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	leader, err := reopened.AcquireLeader(t.Context(), domain.ChannelDev, "tampered-armed-postpub")
	if err != nil {
		t.Fatal(err)
	}
	before, err := reopened.Ticket(t.Context(), resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	var beforeRows int
	if err := reopened.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket).Scan(&beforeRows); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.FenceRecoveredRunners(t.Context(), domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("tampered armed authority was accepted: %v", err)
	}
	after, err := reopened.Ticket(t.Context(), resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	var afterRows int
	if err := reopened.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket).Scan(&afterRows); err != nil {
		t.Fatal(err)
	}
	if after.Version != before.Version || after.RunnerEpoch != before.RunnerEpoch || afterRows != beforeRows {
		t.Fatalf("tampered authority mutated recovery state before=%+v/%d after=%+v/%d", before, beforeRows, after, afterRows)
	}
}

func TestFenceRecoveredRunnersRejectsMissingPostPublicationControl(t *testing.T) {
	db, _, current := postPublicationPauseResume(t)
	ctx := t.Context()
	var path string
	if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("database path=%q err=%v", path, err)
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket); err != nil {
		t.Fatalf("delete runtime control: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	leader, err := reopened.AcquireLeader(ctx, domain.ChannelDev, "missing-postpub-control")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("missing post-publication control was accepted: %v", err)
	}
}

func TestFenceRecoveredRunnersRejectsCorruptPostPublicationControl(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	ctx := fixture.ctx
	current, err := fixture.db.Ticket(ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}
	_, current = postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateReviewing)
	var path string
	if err := fixture.db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("database path=%q err=%v", path, err)
	}
	if _, err := fixture.db.db.ExecContext(ctx, `UPDATE runtime_ticket_controls SET authority_version=authority_version+1 WHERE channel=? AND project_id=? AND ticket_id=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket); err != nil {
		t.Fatalf("corrupt runtime control: %v", err)
	}
	if err := fixture.db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()
	leader, err := reopened.AcquireLeader(ctx, domain.ChannelDev, "corrupt-postpub-control")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("corrupt post-publication control was accepted: %v", err)
	}
}
