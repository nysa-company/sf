package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func providerRetryWaitingFixture(t *testing.T) finalReviewFixture {
	t.Helper()
	beforePlanner := func(db *Store, ctx context.Context, ticket Ticket, fence domain.Fence) Ticket {
		// Cross a real control invalidation first. A runner-1 fixture would
		// bypass the old failing post-publication control branch entirely.
		stopping, err := db.TransitionAndInvalidateRunner(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: fence, EventPayload: `{"intent":"pause"}`})
		if err != nil {
			t.Fatal(err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence.RunnerEpoch = ticket.RunnerEpoch
		pausedControl, err := db.CompleteControlTransition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: fence, EventPayload: `{"drained":true}`})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Transition(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: pausedControl.Version, From: domain.StatePaused, To: domain.StatePlanning, Trigger: "operator_resume", Fence: fence, EventPayload: `{"operator":"test"}`}); err != nil {
			t.Fatal(err)
		}
		openExactRuntimeAdmission(t, db, ticket.Ref)
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil || ticket.RunnerEpoch <= 1 {
			t.Fatalf("missing control invalidation: %v", err)
		}
		builder, _ := setupProviderPair(t, db, ctx)
		worktree, err := db.Worktree(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		for attempt := 0; attempt < 2; attempt++ {
			request := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(builder), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC()})
			request.Worktree, request.Input.Worktree = worktree.Path, worktree.Path
			request.WorktreeIdentity, request.Input.WorktreeIdentity = string(worktree.IdentityJSON), string(worktree.IdentityJSON)
			claim, err := db.BeginProviderAttempt(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.TransitionProviderExhausted(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "retry_or_correction_exhausted", Fence: fence}); err != nil {
			t.Fatal(err)
		}
		paused, err := db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.SealRuntimeControl(ctx, ticket.Ref); err != nil {
			t.Fatal(err)
		}
		if _, err := db.TransitionProviderRetry(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, ResumeState: domain.StatePlanning, Trigger: "operator_retry", Fence: fence}); err != nil {
			t.Fatal(err)
		}
		capability, err := db.ProviderRetryRearmProof(ctx, ticket.Ref, paused)
		if err != nil {
			t.Fatal(err)
		}
		var admission *RuntimeAdmissionCapability
		if err := db.ActivateRearm(ctx, capability, func(value *RuntimeAdmissionCapability) error {
			if _, _, _, ok := value.ConsumeRuntimeAdmission(); !ok {
				return ErrEvidenceConflict
			}
			admission = value
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if admission == nil {
			t.Fatal("missing retry admission")
		}
		if err := admission.OpenStoreAdmission(ctx); err != nil {
			t.Fatal(err)
		}
		current, err := db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		return current
	}
	db, ctx, publishing, fence := publicationLifecycleFixtureFor(t, domain.TicketFeature, domain.MergeGuarded, beforePlanner)
	recordFixturePublication(t, db, ctx, publishing, fence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: publishing.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	waiting, err := db.Ticket(ctx, publishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := db.LoadPublishedCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	checks := []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-1", NormalizedState: "success"}}
	if err := recordCIAuthorityPolicy(db, ctx, publication, waiting, checks); err != nil {
		t.Fatal(err)
	}
	observation := ciAuthorityObservationFor(publication, waiting, fence, "green", time.Now().UTC(), checks)
	if err := db.recordCIObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	canonical, err := db.LoadCurrentCIObservation(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: waiting.Ref, ObservationDigest: canonical.ObservationDigest, ExpectedVersion: waiting.Version, Fence: fence}); err != nil {
		t.Fatal(err)
	}
	reviewing, err := db.Ticket(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fixture := finalReviewFixture{db: db, ctx: ctx, ticket: reviewing, fence: fence, candidate: candidate}
	completeFinalReview(t, fixture)
	if _, err := db.TransitionFinalReview(ctx, Transition{Ref: reviewing.Ref, ExpectedVersion: reviewing.Version, From: domain.StateReviewing, To: domain.StateWaitingApproval, Trigger: "review_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	fixture.ticket, err = db.Ticket(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestProviderRetryWaitingApprovalRecoversTwice(t *testing.T) {
	fixture := providerRetryWaitingFixture(t)
	for restart := 0; restart < 2; restart++ {
		if err := fixture.db.restoreRuntimeControls(fixture.ctx); err != nil {
			t.Fatal(err)
		}
		leader, err := fixture.db.AcquireLeader(fixture.ctx, fixture.ticket.Ref.Channel, "provider-retry-waiting-restart")
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, fixture.ticket.Ref.Channel, leader); err != nil || changed != 1 {
			t.Fatalf("restart %d: changed=%d err=%v", restart, changed, err)
		}
		current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
		if err != nil || current.State != domain.StateWaitingApproval || current.Version != fixture.ticket.Version+1 || current.RunnerEpoch != fixture.ticket.RunnerEpoch+1 {
			t.Fatalf("restart %d ticket=%+v err=%v", restart, current, err)
		}
		fixture.ticket = current
	}
}

func TestProviderRetryWaitingApprovalRejectsTamperedAuthority(t *testing.T) {
	for _, mode := range []string{"stop", "authority", "payload", "epoch_digest", "missing_epoch", "final_review"} {
		t.Run(mode, func(t *testing.T) {
			fixture := providerRetryWaitingFixture(t)
			db, ctx, ref := fixture.db, fixture.ctx, fixture.ticket.Ref
			if err := db.restoreRuntimeControls(ctx); err != nil {
				t.Fatal(err)
			}
			var statement string
			switch mode {
			case "stop":
				statement = `UPDATE runtime_ticket_controls SET stop_runner_epoch=stop_runner_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`
			case "authority":
				statement = `UPDATE runtime_ticket_controls SET authority_version=authority_version+1 WHERE channel=? AND project_id=? AND ticket_id=?`
			case "payload":
				statement = `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='retry_or_correction_exhausted'`
			case "epoch_digest":
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER provider_retry_epochs_immutable_update`); err != nil {
					t.Fatal(err)
				}
				statement = `UPDATE provider_retry_epochs SET retry_digest='` + strings.Repeat("f", 64) + `' WHERE channel=? AND project_id=? AND ticket_id=?`
			case "missing_epoch":
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER provider_retry_epochs_immutable_delete`); err != nil {
					t.Fatal(err)
				}
				statement = `DELETE FROM provider_retry_epochs WHERE channel=? AND project_id=? AND ticket_id=?`
			case "final_review":
				statement = `UPDATE events SET trigger='forged_pass' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='review_pass'`
			}
			if _, err := db.db.ExecContext(ctx, statement, ref.Channel, ref.Project, ref.Ticket); err != nil {
				t.Fatal(err)
			}
			leader, err := db.AcquireLeader(ctx, ref.Channel, "tampered-provider-retry-waiting")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.FenceRecoveredRunners(ctx, ref.Channel, leader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("tamper %s: err=%v", mode, err)
			}
			current, err := db.Ticket(ctx, ref)
			if err != nil || current.Version != fixture.ticket.Version || current.RunnerEpoch != fixture.ticket.RunnerEpoch {
				t.Fatal("refused recovery changed ticket")
			}
		})
	}
}
