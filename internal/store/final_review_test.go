package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

type finalReviewFixture struct {
	db        *Store
	ctx       context.Context
	ticket    Ticket
	fence     domain.Fence
	candidate StoredCandidate
}

func TestStoreRejectsSpikeAutonomousTicket(t *testing.T) {
	db, ctx := openTestStore(t)
	setupProviderProject(t, db, ctx)
	err := db.CreateTicket(ctx, Ticket{Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-spike-autonomous"}, SourceDigest: sha256Digest([]byte("spike autonomous")), Type: domain.TicketSpike, MergeMode: domain.MergeAutonomous})
	if err == nil {
		t.Fatal("spike autonomous ticket was accepted")
	}
}

// finalReviewLifecycleFixture starts from the production Store-backed
// planner/verification/builder/candidate/publication sequence, then appends a
// green CI observation and its immutable reviewing transition. CI polling is
// not composed yet; the fixture deliberately writes the immutable boundary
// that the future poller is required to produce.
func finalReviewLifecycleFixture(t *testing.T) finalReviewFixture {
	return finalReviewLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeGuarded)
}

func finalReviewLifecycleFixtureFor(t *testing.T, ticketType domain.TicketType, mergeMode domain.MergeMode) finalReviewFixture {
	t.Helper()
	db, ctx, publishing, fence := publicationLifecycleFixtureFor(t, ticketType, mergeMode)
	recordFixturePublication(t, db, ctx, publishing, fence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: publishing.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	waiting, err := db.Ticket(ctx, publishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	var witness, host, owner, repo, headOwner, headRepo, headRef, headOID, baseRef, baseOID string
	var number int
	err = db.db.QueryRowContext(ctx, `SELECT witness_digest,github_host,github_owner,github_name,github_pr_number,github_head_owner,github_head_repository,github_head_ref,github_head_oid,github_base_ref,github_base_oid FROM publication_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket).Scan(&witness, &host, &owner, &repo, &number, &headOwner, &headRepo, &headRef, &headOID, &baseRef, &baseOID)
	if err != nil {
		t.Fatal(err)
	}
	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	observationDigest := publicationIdentityDigest([]byte("final-review-green-observation:" + string(waiting.Ref.Ticket)))
	checks := []workflowprompt.Check{{Name: "unit", ExternalID: "run-1", Status: "success"}}
	checkIdentity, err := workflowprompt.NewChecksIdentity("fixture", candidate.Snapshot.HeadSHA, checks)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO ci_observations(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,publication_witness_digest,pr_host,pr_owner,pr_repo,pr_number,pr_head_owner,pr_head_repo,pr_head_ref,pr_head_oid,pr_base_ref,pr_base_oid,pr_factory_owned,pr_open,pr_draft,observed_ticket_version,observed_leader_epoch,observed_runner_epoch,observed_at,required_set_digest,required_check_count,classification,diagnostic_digest,diagnostic_text,diagnostic_json,observation_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, candidate.Snapshot.TreeSHA, witness, host, owner, repo, number, headOwner, headRepo, headRef, headOID, baseRef, baseOID, 1, 1, 1, waiting.Version, fence.LeaderEpoch, waiting.RunnerEpoch, observedAt, checkIdentity.SetDigest, len(checks), "green", "", "", "", observationDigest); err != nil {
		t.Fatal(err)
	}
	var observationID int64
	if err := db.db.QueryRowContext(ctx, `SELECT observation_id FROM ci_observations WHERE observation_digest=?`, observationDigest).Scan(&observationID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `INSERT INTO ci_observation_checks(observation_id,observation_digest,canonical_name,external_id,normalized_state,failing_diagnostic_digest,failing_diagnostic_text) VALUES(?,?,?,?,?,?,?)`, observationID, observationDigest, checks[0].Name, checks[0].ExternalID, checks[0].Status, "", ""); err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC().Format(time.RFC3339Nano)
	event, err := db.db.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, waiting.Version+1, "checks_green", domain.StateWaitingCI, domain.StateReviewing, `{}`, created)
	if err != nil {
		t.Fatal(err)
	}
	eventID, err := event.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE tickets SET state='reviewing',resume_state=NULL,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='waiting_ci' AND version=?`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, waiting.Version); err != nil {
		t.Fatal(err)
	}
	transitionDigest := publicationIdentityDigest([]byte("final-review-green-transition:" + string(waiting.Ref.Ticket)))
	if _, err := db.db.ExecContext(ctx, `INSERT INTO ci_transition_evidence(channel,project_id,ticket_id,candidate_generation,candidate_head_sha,candidate_tree_sha,ticket_version,event_id,event_created_at,observation_classification,observation_digest,observation_ticket_version,observation_leader_epoch,observation_runner_epoch,prior_publication_witness_digest,prior_state,resulting_state,resulting_trigger,transition_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, candidate.Snapshot.Generation, candidate.Snapshot.HeadSHA, candidate.Snapshot.TreeSHA, waiting.Version+1, eventID, created, "green", observationDigest, waiting.Version, fence.LeaderEpoch, waiting.RunnerEpoch, witness, domain.StateWaitingCI, domain.StateReviewing, "checks_green", transitionDigest, created); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, waiting.Ref)
	if err != nil || ticket.State != domain.StateReviewing {
		t.Fatalf("reviewing ticket=%+v err=%v", ticket, err)
	}
	return finalReviewFixture{db: db, ctx: ctx, ticket: ticket, fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}, candidate: candidate}
}

func completeFinalReview(t *testing.T, fixture finalReviewFixture) ProviderAttemptClaim {
	return completeFinalReviewWith(t, fixture, phaseartifact.ReviewPass, "")
}

func completeFinalReviewWith(t *testing.T, fixture finalReviewFixture, decision phaseartifact.ReviewDecision, owner string) ProviderAttemptClaim {
	t.Helper()
	_, reviewer := setupProviderPair(t, fixture.db, fixture.ctx)
	worktree, err := fixture.db.Worktree(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	request := ProviderAttemptRequest{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence, Phase: domain.PhaseReview, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: fixture.ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC(), ExpectedHead: fixture.candidate.Snapshot.HeadSHA, ExpectedProof: fixture.candidate.Snapshot.ProofDigest, Repository: "/tmp/provider", Worktree: worktree.Path, WorktreeIdentity: string(worktree.IdentityJSON), BaseSHA: worktree.BaseSHA, SupervisorKey: providerTestSigner.PublicKey()}
	request.Input = contracts.PhaseInput{Ticket: request.Ref, Phase: request.Phase, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Prompt: "final review", Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, AllowedPaths: []string{"."}, Provider: request.Binding.Identity, AuthMode: request.Binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded, Schema: []byte(`{"type":"object"}`)}
	claim, err := fixture.db.BeginProviderAttempt(fixture.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.RecordProviderLaunch(fixture.ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "final-review", ProcessStartIdentity: fmt.Sprintf("final-review-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	findings := `[]`
	if decision != phaseartifact.ReviewPass {
		findings = `["fix exact finding"]`
	}
	raw := contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: []byte(fmt.Sprintf(`{"schema":"sf.reviewer/v1","decision":"%s","repair_owner":"%s","findings":%s,"reviewed_head":"%s","proof_digest":"%s"}`, decision, owner, findings, fixture.candidate.Snapshot.HeadSHA, fixture.candidate.Snapshot.ProofDigest)), UsageTrusted: true, UsageUnits: 1}
	if _, err := fixture.db.CompleteProviderAttemptSuccess(fixture.ctx, claim, proof(t, claim), fixture.ticket.Version, fixture.fence, raw, phaseartifact.Validation{TicketType: fixture.ticket.Type, ExpectedReviewedHead: fixture.candidate.Snapshot.HeadSHA, ExpectedProofDigest: fixture.candidate.Snapshot.ProofDigest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return claim
}

func TestReviewRepairAndOperatorEscalationConsumeExactStoredReviewerResult(t *testing.T) {
	for _, tc := range []struct {
		name     string
		decision phaseartifact.ReviewDecision
		owner    string
		to       domain.State
		trigger  string
		payload  string
	}{
		{name: "builder repair", decision: phaseartifact.ReviewRepair, owner: "builder", to: domain.StateBuilding, trigger: "review_repair", payload: `{}`},
		{name: "verification repair", decision: phaseartifact.ReviewRepair, owner: "reviewer", to: domain.StateVerifying, trigger: "review_repair", payload: `{}`},
		{name: "operator escalation", decision: phaseartifact.ReviewNeedsOperator, owner: "operator", to: domain.StateBlocked, trigger: "typed_blocker", payload: `{"code":"review_needs_operator"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixture(t)
			claim := completeFinalReviewWith(t, fixture, tc.decision, tc.owner)
			reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence})
			if err != nil || reused.Key.AttemptID != claim.ID || reused.Parsed.Reviewer == nil || reused.Parsed.Reviewer.Decision != tc.decision || reused.Parsed.Reviewer.RepairOwner != tc.owner {
				t.Fatalf("replayed reviewer=%+v err=%v", reused, err)
			}
			var result TransitionResult
			if tc.trigger == "review_repair" {
				result, err = fixture.db.TransitionReviewRepair(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			} else {
				result, err = fixture.db.TransitionReviewNeedsOperator(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, ResumeState: domain.StateReviewing, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			}
			if err != nil || result.Version != fixture.ticket.Version+1 {
				t.Fatalf("transition=%+v err=%v", result, err)
			}
			current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || current.State != tc.to || (tc.to == domain.StateBlocked && current.BlockedCode != "review_needs_operator") {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
			var attempts, budget int
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE id=?`, claim.ID).Scan(&attempts); err != nil || attempts != 1 {
				t.Fatalf("attempt replay=%d err=%v", attempts, err)
			}
			if tc.decision == phaseartifact.ReviewRepair {
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&budget); err != nil || budget != 1 {
					t.Fatalf("repair budget=%d err=%v", budget, err)
				}
				phase, role := domain.PhaseBuild, "builder"
				if tc.to == domain.StateVerifying {
					phase, role = domain.PhaseVerification, "reviewer"
				}
				// The old target-phase result is durable provenance but cannot be
				// selected as replay authority for this next repair cycle. A Worker
				// receiving ErrNotFound proceeds to launch a fresh provider attempt.
				if _, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: phase, Role: role, ExpectedVersion: current.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}}); !errors.Is(err, ErrNotFound) {
					t.Fatalf("repair boundary reused stale %s result: %v", phase, err)
				}
				var boundaries int
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM final_review_repair_boundaries WHERE channel=? AND project_id=? AND ticket_id=? AND target_state=? AND transition_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, tc.to, current.Version).Scan(&boundaries); err != nil || boundaries != 1 {
					t.Fatalf("repair boundary count=%d err=%v", boundaries, err)
				}
			}
			// A response-loss retry cannot consume the completed reviewer result a
			// second time. The old reviewing fence is intentionally stale after
			// the first atomic transition, so neither a second correction charge
			// nor a second blocked event is possible.
			if tc.trigger == "review_repair" {
				_, err = fixture.db.TransitionReviewRepair(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			} else {
				_, err = fixture.db.TransitionReviewNeedsOperator(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, ResumeState: domain.StateReviewing, Trigger: tc.trigger, Fence: fixture.fence, EventPayload: tc.payload})
			}
			if !errors.Is(err, ErrStaleFence) {
				t.Fatalf("replayed final-review result err=%v, want stale fence", err)
			}
			if tc.decision == phaseartifact.ReviewRepair {
				if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT used FROM ticket_counters WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&budget); err != nil || budget != 1 {
					t.Fatalf("replayed repair budget=%d err=%v", budget, err)
				}
			}
		})
	}
}

func TestFinalReviewStoreReplayUsesCompletedAttemptWithoutSecondProviderMutation(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	claim := completeFinalReview(t, fixture)
	reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence})
	if err != nil || reused.Key.AttemptID != claim.ID || reused.Recovered || reused.Parsed.Reviewer == nil || reused.Parsed.Reviewer.Decision != phaseartifact.ReviewPass {
		t.Fatalf("reused final review=%+v err=%v", reused, err)
	}
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	var attempts, transitions int
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("review attempt count=%d err=%v", attempts, err)
	}
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&transitions); err != nil || transitions != 1 {
		t.Fatalf("review transition count=%d err=%v", transitions, err)
	}
}

func TestFinalReviewStoreRecoveryRebindsExactResultOnce(t *testing.T) {
	fixture := finalReviewLifecycleFixture(t)
	claim := completeFinalReview(t, fixture)
	leader, err := fixture.db.AcquireLeader(fixture.ctx, domain.ChannelDev, "final-review-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("fence recovery changed=%d err=%v", changed, err)
	}
	live, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	liveFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}
	reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: live.Ref, Phase: domain.PhaseReview, Role: "reviewer", ExpectedVersion: live.Version, Fence: liveFence})
	if err != nil || reused.Key.AttemptID != claim.ID || !reused.Recovered {
		t.Fatalf("recovered review=%+v err=%v", reused, err)
	}
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: live.Ref, ExpectedVersion: live.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: liveFence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	var attempts, transitions int
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review'`, live.Ref.Channel, live.Ref.Project, live.Ref.Ticket).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("review attempts=%d err=%v", attempts, err)
	}
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass'`, live.Ref.Channel, live.Ref.Project, live.Ref.Ticket).Scan(&transitions); err != nil || transitions != 1 {
		t.Fatalf("review transitions=%d err=%v", transitions, err)
	}
}

func TestReviewRepairBoundaryBridgesStartupBeforeFreshTargetClaim(t *testing.T) {
	for _, tc := range []struct {
		name  string
		owner string
		phase domain.Phase
		role  string
	}{
		{name: "builder", owner: "builder", phase: domain.PhaseBuild, role: "builder"},
		{name: "verification", owner: "reviewer", phase: domain.PhaseVerification, role: "reviewer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixture(t)
			completeFinalReviewWith(t, fixture, phaseartifact.ReviewRepair, tc.owner)
			to := domain.StateBuilding
			if tc.owner == "reviewer" {
				to = domain.StateVerifying
			}
			if _, err := fixture.db.TransitionReviewRepair(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: to, Trigger: "review_repair", Fence: fixture.fence, EventPayload: `{}`}); err != nil {
				t.Fatal(err)
			}
			// Simulate process death in the exact gap after the repair commit and
			// before a new Builder/Verifier claim is admitted.
			leader, err := fixture.db.AcquireLeader(fixture.ctx, domain.ChannelDev, "review-repair-restart-"+tc.owner)
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
				t.Fatalf("repair startup bridge changed=%d err=%v", changed, err)
			}
			live, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || live.State != to {
				t.Fatalf("recovered repair ticket=%+v err=%v", live, err)
			}
			if _, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{Ref: live.Ref, Phase: tc.phase, Role: tc.role, ExpectedVersion: live.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: live.RunnerEpoch}}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("recovered repair reused old %s result: %v", tc.phase, err)
			}
		})
	}
}

func TestFinalReviewTransitionsDeriveManualGuardedSpikeAndRejectAutonomous(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ticketType domain.TicketType
		mergeMode  domain.MergeMode
		to         domain.State
		wantErr    bool
	}{
		{name: "guarded", ticketType: domain.TicketFeature, mergeMode: domain.MergeGuarded, to: domain.StateWaitingApproval},
		{name: "manual", ticketType: domain.TicketFeature, mergeMode: domain.MergeManual, to: domain.StateWaitingManualMerge},
		{name: "spike report only", ticketType: domain.TicketSpike, mergeMode: domain.MergeManual, to: domain.StateDone},
		{name: "autonomous unavailable", ticketType: domain.TicketFeature, mergeMode: domain.MergeAutonomous, to: domain.StateWaitingApproval, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := finalReviewLifecycleFixtureFor(t, tc.ticketType, tc.mergeMode)
			completeFinalReview(t, fixture)
			_, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateReviewing, To: tc.to, Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`})
			if tc.wantErr {
				if !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("autonomous transition err=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || current.State != tc.to {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
		})
	}
}

func TestFinalReviewAuthorityRejectsMissingAndForgedCILineage(t *testing.T) {
	// A candidate and publication witness alone are insufficient: a reviewing
	// state with no green CI observation/transition is fail-closed.
	missingDB, missingCtx, publishing, missingFence := publicationLifecycleFixture(t)
	recordFixturePublication(t, missingDB, missingCtx, publishing, missingFence)
	if _, err := missingDB.TransitionPublishedCandidate(missingCtx, Transition{Ref: publishing.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: missingFence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	missing, err := missingDB.Ticket(missingCtx, publishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missingDB.db.ExecContext(missingCtx, `UPDATE tickets SET state='reviewing',version=version+1 WHERE channel=? AND project_id=? AND id=?`, missing.Ref.Channel, missing.Ref.Project, missing.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := missingDB.FinalReviewAuthority(missingCtx, missing.Ref, missing.Version+1, domain.Fence{LeaderEpoch: missingFence.LeaderEpoch, RunnerEpoch: missing.RunnerEpoch}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("missing CI lineage accepted: %v", err)
	}

	fixture := finalReviewLifecycleFixture(t)
	// The immutable observation/check rows reject physical tampering. The
	// authority below also recomputes the canonical required-set digest, which
	// is what protects it if a database was restored from inconsistent bytes.
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_observation_checks SET normalized_state='failure'`); err == nil {
		t.Fatal("immutable check mutation unexpectedly succeeded")
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE ci_transition_evidence SET transition_digest=?`, publicationIdentityDigest([]byte("tampered"))); err == nil {
		t.Fatal("immutable CI transition mutation unexpectedly succeeded")
	}
	if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); err != nil {
		t.Fatalf("authentic CI lineage rejected: %v", err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version, fixture.ticket.RunnerEpoch, fixture.fence.LeaderEpoch, fixture.ticket.Version+1, fixture.ticket.RunnerEpoch+1, fixture.fence.LeaderEpoch+1, publicationIdentityDigest([]byte("forged")), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) && !errors.Is(err, ErrStaleFence) {
		t.Fatalf("forged recovery lineage accepted: %v", err)
	}
}
