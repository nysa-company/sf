package store

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func installOpenRuntimeAuthority(t *testing.T, database *Store, ref domain.TicketRef, state string, authorityVersion uint64, fence domain.Fence) {
	t.Helper()
	if _, err := database.db.ExecContext(t.Context(), `INSERT INTO runtime_ticket_controls(
		channel,project_id,ticket_id,state,generation,
		stop_version,stop_leader_epoch,stop_runner_epoch,
		authority_version,authority_leader_epoch,authority_runner_epoch,updated_at)
		VALUES(?,?,?, ?,1, ?,?,?, ?,?,?,?)`,
		ref.Channel, ref.Project, ref.Ticket, state,
		authorityVersion, fence.LeaderEpoch, fence.RunnerEpoch,
		authorityVersion, fence.LeaderEpoch, fence.RunnerEpoch, now()); err != nil {
		t.Fatal(err)
	}
}

func openRuntimeAuthorityVersion(t *testing.T, database *Store, ref domain.TicketRef) uint64 {
	t.Helper()
	var version uint64
	if err := database.db.QueryRowContext(t.Context(), `SELECT authority_version FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func openPostPublicationRuntimeAdmission(t *testing.T, database *Store, ref domain.TicketRef, stopped Ticket) {
	t.Helper()
	capability, err := database.PostPublicationRearmProof(t.Context(), ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := database.ActivateRearm(t.Context(), capability, func(value *RuntimeAdmissionCapability) error {
		if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
			return errors.New("runtime admission capability was not consumable")
		}
		admission = value
		return nil
	}); err != nil || admission == nil {
		t.Fatalf("activate runtime admission=%v err=%v", admission, err)
	}
	if err := admission.OpenStoreAdmission(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// reopenAndFencePostPublication simulates the only recovery sequence that a
// fresh daemon is permitted to use: durable Store reopen, leader acquisition,
// and one signed runner fence before the runtime admission is rearmed.
func reopenAndFencePostPublication(t *testing.T, database *Store, ref domain.TicketRef, name string) (*Store, Ticket, Ticket, domain.Fence) {
	t.Helper()
	var path string
	if err := database.db.QueryRowContext(t.Context(), `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("database path=%q err=%v", path, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := reopened.AcquireLeader(t.Context(), ref.Channel, name)
	if err != nil {
		reopened.Close()
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(t.Context(), ref.Channel, leader); err != nil || changed != 1 {
		reopened.Close()
		t.Fatalf("fence recovery changed=%d err=%v", changed, err)
	}
	live, err := reopened.Ticket(t.Context(), ref)
	if err != nil {
		reopened.Close()
		t.Fatal(err)
	}
	stopped, err := reopened.StoppedRuntimeTicket(t.Context(), ref)
	if err != nil {
		reopened.Close()
		t.Fatal(err)
	}
	return reopened, stopped, live, domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}
}

// A post-publication operator retry may resume a reviewing ticket.  The
// reviewer result is necessarily fresh at that resumed fence, while the CI
// checkpoint that authorizes it is older.  This exercises the exact bounded
// reviewing bridge and then proves that the open admission also advances over
// the later manual-observation transition.
func TestFinalReviewAndManualMergeContinueAcrossPostPublicationRetry(t *testing.T) {
	fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeManual)
	defer fixture.db.Close()
	stopped, resumed := postPublicationPauseRetryAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
	resumedFence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	openPostPublicationRuntimeAdmission(t, fixture.db, resumed.Ref, stopped)

	resumedFixture := fixture
	resumedFixture.ticket = resumed
	resumedFixture.fence = resumedFence
	completeFinalReview(t, resumedFixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, From: domain.StateReviewing,
		To: domain.StateWaitingManualMerge, Trigger: "review_pass", Fence: resumedFence, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("final review after retry resume: %v", err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || waiting.State != domain.StateWaitingManualMerge {
		t.Fatalf("manual waiting ticket=%+v err=%v", waiting, err)
	}
	waitingFence := domain.Fence{LeaderEpoch: resumedFence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}
	publication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := fixture.db.BindManualMergeObservation(fixture.ctx, waiting.Ref, NewManualMergeObservation(publication, contracts.PublishedPullRequestObservation{
		Identity: publication.PullRequest, State: "MERGED", Merged: true,
		MergeCommit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BaseHeadOID: publication.PullRequest.BaseOID,
	}), waitingFence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.RecordManualMergeObservation(fixture.ctx, Transition{
		Ref: waiting.Ref, ExpectedVersion: waiting.Version, From: domain.StateWaitingManualMerge,
		To: domain.StateReconciling, Trigger: "external_merge_observed", Fence: waitingFence,
	}, observation); err != nil {
		t.Fatalf("manual observation after retry resume: %v", err)
	}
	current, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil || current.State != domain.StateReconciling {
		t.Fatalf("manual successor=%+v err=%v", current, err)
	}
	if got := openRuntimeAuthorityVersion(t, fixture.db, waiting.Ref); got != current.Version {
		t.Fatalf("manual continuation authority version=%d want=%d", got, current.Version)
	}
}

func TestPostPublicationReviewRetryCannotBypassProviderExhaustion(t *testing.T) {
	fixture := finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeManual)
	defer fixture.db.Close()
	_, reviewer := setupProviderPair(t, fixture.db, fixture.ctx)
	worktree, err := fixture.db.Worktree(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		request := ProviderAttemptRequest{
			Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence,
			Phase: domain.PhaseReview, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: fixture.ticket.ConfigDigest,
			Capacity: 1, At: time.Now().UTC(), ExpectedHead: fixture.candidate.Snapshot.HeadSHA, ExpectedProof: fixture.candidate.Snapshot.ProofDigest,
			Repository: "/tmp/provider", Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA,
			SupervisorKey: providerTestSigner.PublicKey(),
		}
		request.Input = contracts.PhaseInput{Ticket: request.Ref, Phase: request.Phase, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Prompt: "final review", Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, AllowedPaths: []string{"."}, Provider: request.Binding.Identity, AuthMode: request.Binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
		claim, err := fixture.db.BeginProviderAttempt(fixture.ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.db.FinishProviderAttempt(fixture.ctx, claim, proof(t, claim), fixture.ticket.Version, fixture.fence, "failed", "failed", 1, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	paused, err := fixture.db.TransitionProviderExhausted(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StatePaused, ResumeState: domain.StateReviewing, Trigger: "retry_or_correction_exhausted", Fence: fixture.fence})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.Transition(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateReviewing, ResumeState: domain.StateReviewing, Trigger: "operator_retry", Fence: fixture.fence, EventPayload: `{}`}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("generic reviewing retry bypassed provider exhaustion: %v", err)
	}
}

func TestFinalReviewRetryResumeReusesCompletedReviewAfterReopenAndFence(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	stopped, resumed := postPublicationPauseRetryAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
	resumedFence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	openPostPublicationRuntimeAdmission(t, fixture.db, resumed.Ref, stopped)
	resumedFixture := fixture
	resumedFixture.ticket = resumed
	resumedFixture.fence = resumedFence
	claim := completeFinalReview(t, resumedFixture)

	reopened, recoveredStopped, live, liveFence := reopenAndFencePostPublication(t, fixture.db, resumed.Ref, "review-retry-reopen")
	defer reopened.Close()
	if live.State != domain.StateReviewing {
		t.Fatalf("recovered reviewing ticket=%+v", live)
	}
	openPostPublicationRuntimeAdmission(t, reopened, live.Ref, recoveredStopped)
	reused, err := reopened.LatestReusableProviderAttempt(t.Context(), LatestReusableProviderAttemptRequest{
		Ref: live.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: live.Version, Fence: liveFence,
	})
	if err != nil || reused.Key.AttemptID != claim.ID || !reused.Recovered {
		t.Fatalf("recovered final review=%+v err=%v", reused, err)
	}
	if _, err := reopened.TransitionFinalReview(t.Context(), Transition{
		Ref: live.Ref, ExpectedVersion: live.Version, From: domain.StateReviewing,
		To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: liveFence, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("transition recovered final review: %v", err)
	}
}

// A provider response may commit immediately before an operator takes control.
// Reusing it after the exact pause/retry and daemon fence must authenticate
// both the control triplet and the signed recovery row; it must not rerun the
// reviewer merely because its immutable claim predates the pause.
func TestCompletedFinalReviewBeforeRetrySurvivesReopenAndFence(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	claim := completeFinalReview(t, fixture)
	stopped, resumed := postPublicationPauseRetryAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
	openPostPublicationRuntimeAdmission(t, fixture.db, resumed.Ref, stopped)

	reopened, recoveredStopped, live, liveFence := reopenAndFencePostPublication(t, fixture.db, resumed.Ref, "completed-review-retry-reopen")
	defer reopened.Close()
	if live.State != domain.StateReviewing {
		t.Fatalf("recovered reviewing ticket=%+v", live)
	}
	openPostPublicationRuntimeAdmission(t, reopened, live.Ref, recoveredStopped)
	reused, err := reopened.LatestReusableProviderAttempt(t.Context(), LatestReusableProviderAttemptRequest{
		Ref: live.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: live.Version, Fence: liveFence,
	})
	if err != nil || reused.Key.AttemptID != claim.ID || !reused.Recovered {
		t.Fatalf("pre-control final review=%+v err=%v", reused, err)
	}
}

// Approval is historical once a later guarded merge effect has been issued.
// The approval recovery proof must use the sealed waiting endpoint/control
// lineage rather than require the ticket to remain live at waiting_approval.
func TestGuardedMergeContinuesAcrossPostPublicationApprovalBridge(t *testing.T) {
	fixture, waiting, waitingFence := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	defer fixture.db.Close()
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, waiting, waitingFence, domain.StateWaitingApproval)
	resumedFence := domain.Fence{LeaderEpoch: waitingFence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	openPostPublicationRuntimeAdmission(t, fixture.db, resumed.Ref, stopped)
	if _, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: resumedFence,
		ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 701, Decision: "approved",
	}}); err != nil {
		t.Fatalf("approve after post-publication resume: %v", err)
	}
	merging, err := fixture.db.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || merging.State != domain.StateMerging {
		t.Fatalf("merging ticket=%+v err=%v", merging, err)
	}
	mergingFence := domain.Fence{LeaderEpoch: resumedFence.LeaderEpoch, RunnerEpoch: merging.RunnerEpoch}
	bindTerminalMergeEffect(t, fixture.db, fixture, merging, mergingFence, "merge/rearm/approval-bridge")
	if _, err := fixture.db.TransitionGuardedMergeObserved(fixture.ctx, Transition{
		Ref: merging.Ref, ExpectedVersion: merging.Version, From: domain.StateMerging,
		To: domain.StateReconciling, Trigger: "merge_observed", Fence: mergingFence, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("guarded merge after approval bridge: %v", err)
	}
	current, err := fixture.db.Ticket(fixture.ctx, merging.Ref)
	if err != nil || current.State != domain.StateReconciling {
		t.Fatalf("guarded successor=%+v err=%v", current, err)
	}
	if got := openRuntimeAuthorityVersion(t, fixture.db, merging.Ref); got != current.Version {
		t.Fatalf("guarded continuation authority version=%d want=%d", got, current.Version)
	}
}

func TestGuardedMergeApprovalResumeContinuesAfterReopenAndFence(t *testing.T) {
	fixture, waiting, waitingFence := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, waiting, waitingFence, domain.StateWaitingApproval)
	resumedFence := domain.Fence{LeaderEpoch: waitingFence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	openPostPublicationRuntimeAdmission(t, fixture.db, resumed.Ref, stopped)
	if _, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: resumedFence,
		ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 703, Decision: "approved",
	}}); err != nil {
		t.Fatalf("approve before reopen: %v", err)
	}

	reopened, recoveredStopped, live, liveFence := reopenAndFencePostPublication(t, fixture.db, resumed.Ref, "approval-merge-reopen")
	defer reopened.Close()
	if live.State != domain.StateMerging {
		t.Fatalf("recovered merging ticket=%+v", live)
	}
	openPostPublicationRuntimeAdmission(t, reopened, live.Ref, recoveredStopped)
	bindTerminalMergeEffect(t, reopened, fixture, live, liveFence, "merge/rearm/approval-reopen")
	if _, err := reopened.TransitionGuardedMergeObserved(t.Context(), Transition{
		Ref: live.Ref, ExpectedVersion: live.Version, From: domain.StateMerging,
		To: domain.StateReconciling, Trigger: "merge_observed", Fence: liveFence, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("guarded merge after reopen: %v", err)
	}
	current, err := reopened.Ticket(t.Context(), live.Ref)
	if err != nil || current.State != domain.StateReconciling {
		t.Fatalf("reconciled ticket=%+v err=%v", current, err)
	}
}

func TestGuardedMergeApprovalBridgeTamperFailsClosed(t *testing.T) {
	fixture, waiting, waitingFence := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	defer fixture.db.Close()
	stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, waiting, waitingFence, domain.StateWaitingApproval)
	resumedFence := domain.Fence{LeaderEpoch: waitingFence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	openPostPublicationRuntimeAdmission(t, fixture.db, resumed.Ref, stopped)
	if _, err := fixture.db.ApplyOperatorDecision(fixture.ctx, OperatorDecisionRequest{OperatorDecision: OperatorDecision{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: resumedFence,
		ReviewedHead: fixture.candidate.Snapshot.HeadSHA, OperatorUID: 702, Decision: "approved",
	}}); err != nil {
		t.Fatalf("approve before bridge tamper: %v", err)
	}
	merging, err := fixture.db.Ticket(fixture.ctx, resumed.Ref)
	if err != nil || merging.State != domain.StateMerging {
		t.Fatalf("merging ticket=%+v err=%v", merging, err)
	}
	mergingFence := domain.Fence{LeaderEpoch: resumedFence.LeaderEpoch, RunnerEpoch: merging.RunnerEpoch}
	bindTerminalMergeEffect(t, fixture.db, fixture, merging, mergingFence, "merge/rearm/approval-tamper")
	beforeAuthority := openRuntimeAuthorityVersion(t, fixture.db, merging.Ref)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE events SET trigger='forged_resume' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, merging.Ref.Channel, merging.Ref.Project, merging.Ref.Ticket, resumed.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionGuardedMergeObserved(fixture.ctx, Transition{
		Ref: merging.Ref, ExpectedVersion: merging.Version, From: domain.StateMerging,
		To: domain.StateReconciling, Trigger: "merge_observed", Fence: mergingFence, EventPayload: `{}`,
	}); err == nil {
		t.Fatal("forged approval bridge advanced guarded merge")
	}
	after, err := fixture.db.Ticket(fixture.ctx, merging.Ref)
	if err != nil || !reflect.DeepEqual(after, merging) {
		t.Fatalf("tampered bridge changed ticket before=%+v after=%+v err=%v", merging, after, err)
	}
	if got := openRuntimeAuthorityVersion(t, fixture.db, merging.Ref); got != beforeAuthority {
		t.Fatalf("tampered bridge advanced authority version=%d want=%d", got, beforeAuthority)
	}
}

func TestFinalReviewRetryResumeTamperFailsClosed(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	defer fixture.db.Close()
	_, resumed := postPublicationPauseRetryAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
	resumedFence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: resumed.RunnerEpoch}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE events SET trigger='forged_retry' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket, resumed.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, resumed.Ref, resumed.Version, resumedFence); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("forged retry continuation authority=%v", err)
	}
}

func TestAdvanceOpenRuntimeAuthorityRejectsFutureOrMismatchedControl(t *testing.T) {
	for index, tc := range []struct {
		name    string
		state   string
		version func(uint64) uint64
		mutate  func(domain.Fence) domain.Fence
	}{
		{name: "future version", state: "open", version: func(version uint64) uint64 { return version + 1 }, mutate: func(fence domain.Fence) domain.Fence { return fence }},
		{name: "sealed state", state: "sealed", version: func(version uint64) uint64 { return version }, mutate: func(fence domain.Fence) domain.Fence { return fence }},
		{name: "leader mismatch", state: "open", version: func(version uint64) uint64 { return version }, mutate: func(fence domain.Fence) domain.Fence { fence.LeaderEpoch++; return fence }},
		{name: "runner mismatch", state: "open", version: func(version uint64) uint64 { return version }, mutate: func(fence domain.Fence) domain.Fence { fence.RunnerEpoch++; return fence }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, ctx := openTestStore(t)
			defer database.Close()
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID(fmt.Sprintf("SF-runtime-authority-%d", index+1))}
			if err := database.CreateTicket(ctx, ticket(ref, "runtime-authority")); err != nil {
				t.Fatal(err)
			}
			leader, err := database.AcquireLeader(ctx, ref.Channel, "runtime-authority-"+tc.name)
			if err != nil {
				t.Fatal(err)
			}
			queued, err := database.Ticket(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/"+string(ref.Ticket)+"/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
			if err != nil {
				t.Fatal(err)
			}
			fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
			storedFence := tc.mutate(fence)
			authorityVersion := tc.version(started.Version)
			installOpenRuntimeAuthority(t, database, ref, tc.state, authorityVersion, storedFence)
			err = database.write(ctx, func(conn *sql.Conn) error {
				return advanceOpenRuntimeAuthority(ctx, conn, ref, started.Version, fence)
			})
			if !errors.Is(err, ErrStaleFence) {
				t.Fatalf("advance err=%v", err)
			}
			if got := openRuntimeAuthorityVersion(t, database, ref); got != authorityVersion {
				t.Fatalf("rejected authority changed=%d want=%d", got, authorityVersion)
			}
		})
	}
}
