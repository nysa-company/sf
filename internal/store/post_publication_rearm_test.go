package store

import (
	"errors"
	"strings"
	"testing"

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
			stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, current, fence, domain.StateReconciling)
			if _, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped); !errors.Is(err, ErrControlNotDrained) {
				fixture.db.Close()
				t.Fatalf("noncanonical guarded merge observation rearmed: %v", err)
			}
			var path string
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
				fixture.db.Close()
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
			leader, err := reopened.AcquireLeader(fixture.ctx, domain.ChannelDev, "tampered-guarded-reconciling-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reopened.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("noncanonical guarded merge observation recovered: %v", err)
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
