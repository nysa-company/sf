package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// verificationAmendmentFixture drives the real Store-owned plan, original
// verification, Builder amendment request, and independent Reviewer evidence
// boundaries. It deliberately stops before the decision transition so each
// test can exercise the decision/replay authority without manufacturing rows.
type verificationAmendmentFixture struct {
	db         *Store
	ctx        context.Context
	ticket     Ticket
	fence      domain.Fence
	config     string
	builder    ProviderQualification
	reviewer   ProviderQualification
	proposal   phaseartifact.Verification
	amended    phaseartifact.Verification
	amendedKey ProviderAttemptResultKey
	amendedArt VerificationArtifact
	priorProof string
	priorRev   uint64
	budgetID   string
}

type verificationAmendmentAuthoritySnapshot struct {
	ticket                        Ticket
	events, budgets, requests     int
	revisions, resultBindings     int
	providerResults, commandBinds int
}

func snapshotVerificationAmendmentAuthority(t *testing.T, fixture verificationAmendmentFixture) verificationAmendmentAuthoritySnapshot {
	t.Helper()
	ticket, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	result := verificationAmendmentAuthoritySnapshot{ticket: ticket}
	for table, target := range map[string]*int{
		"events":                               &result.events,
		"ticket_budget_uses":                   &result.budgets,
		"verification_amendment_requests":      &result.requests,
		"verification_revisions":               &result.revisions,
		"verification_result_bindings":         &result.resultBindings,
		"provider_attempt_results":             &result.providerResults,
		"verification_command_result_bindings": &result.commandBinds,
	} {
		if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM `+table+` WHERE channel=? AND project_id=? AND ticket_id=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func TestGenericTransitionCannotForgeVerificationAmendment(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) (verificationAmendmentFixture, Transition)
	}{
		{
			name: "request",
			setup: func(t *testing.T) (verificationAmendmentFixture, Transition) {
				fixture := verificationAmendmentLifecycle(t, false)
				if _, err := fixture.db.TransitionVerificationAmendmentRejected(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey); err != nil {
					t.Fatal(err)
				}
				current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				fixture.ticket = current
				fixture.fence.RunnerEpoch = current.RunnerEpoch
				return fixture, Transition{Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StateBuilding, To: domain.StateVerifying, Trigger: "verification_amendment_requested", Fence: fixture.fence, EventPayload: "{}"}
			},
		},
		{
			name: "accepted",
			setup: func(t *testing.T) (verificationAmendmentFixture, Transition) {
				fixture := verificationAmendmentLifecycle(t, true)
				return fixture, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_accepted", Fence: fixture.fence, EventPayload: "{}"}
			},
		},
		{
			name: "rejected",
			setup: func(t *testing.T) (verificationAmendmentFixture, Transition) {
				fixture := verificationAmendmentLifecycle(t, false)
				return fixture, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture, transition := tc.setup(t)
			before := snapshotVerificationAmendmentAuthority(t, fixture)
			if _, err := fixture.db.Transition(fixture.ctx, transition); !errors.Is(err, ErrEvidenceConflict) {
				t.Fatalf("generic %s transition=%v, want evidence conflict", transition.Trigger, err)
			}
			after := snapshotVerificationAmendmentAuthority(t, fixture)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("generic %s mutated amendment authority: before=%+v after=%+v", transition.Trigger, before, after)
			}
		})
	}
}

func TestTransitionVerificationAmendmentRequestRejectsNonCanonicalPayload(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, false)
	if _, err := fixture.db.TransitionVerificationAmendmentRejected(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey); err != nil {
		t.Fatal(err)
	}
	building, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	builderKey := completeReplacementAmendmentBuilder(t, fixture, building, fence)
	before := snapshotVerificationAmendmentAuthority(t, fixture)
	if _, err := fixture.db.TransitionVerificationAmendmentRequest(fixture.ctx, Transition{Ref: building.Ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StateVerifying, Trigger: "verification_amendment_requested", Fence: fence, EventPayload: `{"unexpected":true}`}, builderKey); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("noncanonical amendment request payload=%v, want evidence conflict", err)
	}
	after := snapshotVerificationAmendmentAuthority(t, fixture)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("noncanonical amendment request mutated authority: before=%+v after=%+v", before, after)
	}
}

func TestVerificationAmendmentAcceptRejectReplayAndExactReviewerBinding(t *testing.T) {
	t.Run("accepted binds the reviewer that recorded the replacement revision", func(t *testing.T) {
		fixture := verificationAmendmentLifecycle(t, true)
		decision, err := fixture.db.VerificationAmendmentDecision(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence, fixture.amendedKey)
		if err != nil || decision != VerificationAmendmentAccepted {
			t.Fatalf("accepted decision=%q err=%v", decision, err)
		}
		result, err := fixture.db.TransitionVerificationAmendmentAccepted(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_accepted", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey)
		accepted, readErr := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
		if err != nil || readErr != nil || result.Version == 0 || accepted.State != domain.StateBuilding {
			t.Fatalf("accepted transition=%+v err=%v", result, err)
		}
		var boundID int64
		var boundAttempt, reviewers int
		if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT provider_attempt_id,provider_attempt FROM verification_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.priorRev+1).Scan(&boundID, &boundAttempt); err != nil || boundID != fixture.amendedKey.AttemptID || boundAttempt != fixture.amendedKey.Attempt {
			t.Fatalf("replacement binding=%d/%d expected=%d/%d err=%v", boundID, boundAttempt, fixture.amendedKey.AttemptID, fixture.amendedKey.Attempt, err)
		}
		if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempt_results WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND role='reviewer'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&reviewers); err != nil || reviewers != 2 {
			t.Fatalf("reviewer result replay count=%d err=%v", reviewers, err)
		}
	})

	t.Run("rejection reuses the original revision and is replay-safe", func(t *testing.T) {
		fixture := verificationAmendmentLifecycle(t, false)
		// A rejection returns the previous proof digest while retaining the
		// Builder-selected command; changing command is a malformed decision.
		originalProof, err := workflowprompt.CanonicalVerificationProofBytes(fixture.amended)
		if err != nil {
			t.Fatal(err)
		}
		if sha256Digest(originalProof) != fixture.priorProof {
			t.Fatal("rejection fixture does not reproduce the prior proof digest")
		}
		for replay := 0; replay < 2; replay++ {
			decision, err := fixture.db.VerificationAmendmentDecision(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence, fixture.amendedKey)
			if err != nil || decision != VerificationAmendmentRejected {
				t.Fatalf("rejection replay=%d decision=%q err=%v", replay, decision, err)
			}
		}
		if _, err := fixture.db.TransitionVerificationAmendmentRejected(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey); err != nil {
			t.Fatal(err)
		}
		var current uint64
		if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT current_revision FROM verifications WHERE channel=? AND project_id=? AND ticket_id=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&current); err != nil || current != fixture.priorRev {
			t.Fatalf("rejection must not append a revision: current=%d err=%v", current, err)
		}
	})
}

func TestVerificationAmendmentRejectsCommandMismatchAndSourceTamper(t *testing.T) {
	// The phase budget intentionally permits only the original Reviewer plus
	// one independent amendment Reviewer.  Build the second result malformed
	// from the outset instead of fabricating a third provider attempt merely to
	// exercise the command check.
	fixture := verificationAmendmentLifecycleWithReviewer(t, false, func(value *phaseartifact.Verification) {
		value.Command = []string{"go", "test", "./different"}
		value.EvidenceDigest = "same-proof-with-a-different-command"
	})
	if _, err := fixture.db.VerificationAmendmentDecision(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence, fixture.amendedKey); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("command mismatch decision=%v", err)
	}

	// The immutable trigger protects normal writes. Simulate a privileged
	// durable corruption and require every amendment authority consumer to
	// rehydrate the Builder source instead of trusting the amendment row.
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER verification_amendment_requests_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE verification_amendment_requests SET proposed_command_json='["go","test","./tampered"]' WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.PendingVerificationAmendment(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("tampered amendment loaded: %v", err)
	}
}

func TestVerificationAmendmentRejectsMissingRequestEvent(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, true)
	// V51 binds this Store-owned amendment transition to its exact phase entry.
	// Simulate privileged durable corruption by removing the immutable child
	// witness before deleting its referenced event; ordinary SQL must retain the
	// foreign-key protection.
	for _, trigger := range []string{"provider_phase_attempt_entries_immutable_delete", "provider_phase_entries_immutable_delete"} {
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER `+trigger); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM provider_phase_attempt_entries
		WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND entry_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM provider_phase_entries
		WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND entry_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger='verification_amendment_requested' AND from_state='building' AND to_state='verifying'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.PendingVerificationAmendment(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("amendment without request event loaded: %v", err)
	}
	replay := fixture.amendedArt
	replay.ExpectedVersion, replay.Fence, replay.ProviderResult = fixture.ticket.Version, fixture.fence, &fixture.amendedKey
	if _, err := fixture.db.RecordVerification(fixture.ctx, replay); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("amendment replay without request event accepted: %v", err)
	}
}

func TestVerificationAmendmentRejectsCompetingRequestEvent(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, false)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,from_state,to_state,trigger,payload,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version, domain.StateBuilding, domain.StateVerifying, "forged_competing_transition", "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.PendingVerificationAmendment(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("amendment with competing request event loaded: %v", err)
	}
}

func TestVerificationAmendmentRejectsRetargetedBudgetRequest(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, false)
	var consumedVersion, consumedLeader, consumedRunner uint64
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch
		FROM verification_amendment_requests WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version).Scan(&consumedVersion, &consumedLeader, &consumedRunner); err != nil {
		t.Fatal(err)
	}
	const unrelatedRequestID = "unrelated-amendment-budget"
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `INSERT INTO ticket_budget_uses(channel,project_id,ticket_id,kind,request_id,ticket_version,leader_epoch,runner_epoch,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, "correction", unrelatedRequestID, consumedVersion, consumedLeader, consumedRunner, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER verification_amendment_requests_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE verification_amendment_requests SET correction_budget_request_id=?
		WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version=?`, unrelatedRequestID, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.PendingVerificationAmendment(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("retargeted amendment budget accepted: %v", err)
	}
}

func TestRecordVerificationRejectsUnboundAmendmentProjection(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, true)
	// Remove the request row through the privileged tamper path so this test
	// proves RecordVerification itself does not accept caller-authored amendment
	// metadata merely because the provider result and revision are valid.
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER verification_amendment_requests_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER verification_amendment_requests_immutable_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM verification_amendment_requests WHERE channel=? AND project_id=? AND ticket_id=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	forged := fixture.amendedArt
	forged.AmendsRevision = fixture.priorRev
	forged.Reason = "caller-authored amendment"
	forged.Requester = "untrusted-caller"
	if _, err := fixture.db.RecordVerification(fixture.ctx, forged); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("unbound amendment projection accepted: %v", err)
	}
}

func TestBuilderAmendmentTakesPrecedenceOverHistoricalReviewerRepair(t *testing.T) {
	const historicalRequestID = "final-review/historical-repair"
	fixture := verificationAmendmentLifecycleWithReviewerAndBeforeRequest(t, true, nil, func(t *testing.T, db *Store, ctx context.Context, ticket Ticket, fence domain.Fence, prior VerificationRevision) {
		t.Helper()
		// Seed the immutable rows left by a prior final-review repair. The new
		// Builder request below is the next Verifying episode, so the historical
		// repair must not authorize its Reviewer result.
		if _, err := db.db.ExecContext(ctx, `INSERT INTO ticket_counters(channel,project_id,ticket_id,kind,used,limit_count) VALUES(?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, "correction", 1, 2); err != nil {
			t.Fatalf("insert historical reviewer repair counter: %v", err)
		}
		if _, err := db.db.ExecContext(ctx, `INSERT INTO ticket_budget_uses(channel,project_id,ticket_id,kind,request_id,ticket_version,leader_epoch,runner_epoch,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, "correction", historicalRequestID, ticket.Version-1, fence.LeaderEpoch, fence.RunnerEpoch, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatalf("insert historical reviewer repair budget: %v", err)
		}
		if _, err := db.db.ExecContext(ctx, `INSERT INTO final_review_repair_boundaries(channel,project_id,ticket_id,target_state,transition_ticket_version,reviewer_attempt_id,reviewer_attempt,reviewer_typed_sha256,prior_verification_revision,amendment_reason,requester,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, domain.StateVerifying, ticket.Version, 1, 1, strings.Repeat("a", 64), prior.Revision, "historical reviewer repair", "final-reviewer", "correction", historicalRequestID, ticket.Version-1, fence.LeaderEpoch, fence.RunnerEpoch, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatalf("insert historical reviewer repair boundary: %v", err)
		}
	})
	var amends uint64
	var reason, requester string
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COALESCE(amends_revision,0),amendment_reason,requester FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.priorRev+1).Scan(&amends, &reason, &requester); err != nil {
		t.Fatal(err)
	}
	if amends != fixture.priorRev || reason != "the original proof command misses the regression" || requester != "builder" {
		t.Fatalf("replacement bound historical repair instead of active builder amendment: amends=%d reason=%q requester=%q", amends, reason, requester)
	}
}

func TestRecordVerificationRejectsRejectedHistoricalAmendmentInNewVerifyingEpisode(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, false)
	if _, err := fixture.db.TransitionVerificationAmendmentRejected(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey); err != nil {
		t.Fatal(err)
	}
	fixture = resumeRejectedAmendmentIntoFreshVerification(t, fixture)
	// The phase budget intentionally caps this ticket at the original Reviewer
	// plus the resolved amendment Reviewer.  Reuse that immutable historical
	// key to prove it cannot be revived in a later Verifying episode; deleting a
	// phase-run row to mint a duplicate attempt would weaken the very uniqueness
	// invariant under test.
	forged := fixture.amendedArt
	forged.ExpectedVersion, forged.Fence, forged.ProviderResult = fixture.ticket.Version, fixture.fence, &fixture.amendedKey
	forged.AmendsRevision, forged.Reason, forged.Requester = fixture.priorRev, "the original proof command misses the regression", "builder"
	if _, err := fixture.db.RecordVerification(fixture.ctx, forged); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("rejected historical amendment revived: %v", err)
	}
	var current uint64
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT current_revision FROM verifications WHERE channel=? AND project_id=? AND ticket_id=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&current); err != nil || current != fixture.priorRev {
		t.Fatalf("historical rejection changed revision=%d err=%v", current, err)
	}
}

func TestVerificationRevisionsAreImmutable(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, false)
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE verification_revisions SET proof_digest=? WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, strings.Repeat("f", 64), fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.priorRev); err == nil {
		t.Fatal("verification revision update unexpectedly succeeded")
	}
	if _, err := fixture.db.db.ExecContext(fixture.ctx, `DELETE FROM verification_revisions WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.priorRev); err == nil {
		t.Fatal("verification revision delete unexpectedly succeeded")
	}
}

func TestVerificationAmendmentConsumesExactlyOneBoundedCorrectionBudget(t *testing.T) {
	fixture := verificationAmendmentLifecycle(t, true)
	var uses int
	if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction' AND request_id=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.budgetID, fixture.ticket.Version-1, fixture.fence.LeaderEpoch, fixture.fence.RunnerEpoch).Scan(&uses); err != nil || uses != 1 {
		t.Fatalf("amendment budget uses=%d err=%v", uses, err)
	}
	if _, err := fixture.db.PendingVerificationAmendment(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); err != nil {
		t.Fatalf("exact budget-backed amendment rejected: %v", err)
	}
}

func TestVerificationAmendmentRecoveryRequestBridgeBindsStartingFence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		leaderDelta uint64
		wantErr     bool
	}{
		{name: "exact consumed fence", wantErr: false},
		{name: "unrelated starting leader", leaderDelta: 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := verificationAmendmentLifecycle(t, false)
			var consumedVersion, consumedRunner, consumedLeader uint64
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT consumed_ticket_version,consumed_runner_epoch,consumed_leader_epoch
				FROM verification_amendment_requests
				WHERE channel=? AND project_id=? AND ticket_id=? AND transition_ticket_version=?`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.ticket.Version).Scan(&consumedVersion, &consumedRunner, &consumedLeader); err != nil {
				t.Fatal(err)
			}
			startLeader := consumedLeader + tc.leaderDelta
			err := validateRunnerVerificationAmendmentAdvance(fixture.ctx, fixture.db.db, fixture.ticket.Ref, consumedVersion, consumedRunner, startLeader, fixture.ticket.Version, consumedRunner, startLeader)
			if tc.wantErr && !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("mismatched starting fence accepted: %v", err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("exact starting fence rejected: %v", err)
			}
		})
	}
}

func TestVerificationAmendmentReplaysCompletedReviewerAcrossRunnerRecovery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		accepted bool
	}{
		{name: "accepted before transition", accepted: true},
		{name: "rejected before transition", accepted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := verificationAmendmentLifecycle(t, tc.accepted)
			key := fixture.amendedKey

			if err := fixture.db.restoreRuntimeControls(fixture.ctx); err != nil {
				t.Fatal(err)
			}
			leader, err := fixture.db.AcquireLeader(fixture.ctx, fixture.ticket.Ref.Channel, "verification-amendment-response-loss-"+strings.ReplaceAll(tc.name, " ", "-"))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, fixture.ticket.Ref.Channel, leader); err != nil || changed != 1 {
				t.Fatalf("recovery changed=%d err=%v", changed, err)
			}
			fixture.ticket, err = fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || fixture.ticket.State != domain.StateVerifying {
				t.Fatalf("recovered ticket=%+v err=%v", fixture.ticket, err)
			}
			fixture.fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: fixture.ticket.RunnerEpoch}

			decision, err := fixture.db.VerificationAmendmentDecision(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence, key)
			if err != nil {
				t.Fatalf("recovered decision=%v", err)
			}
			if tc.accepted {
				if decision != VerificationAmendmentAccepted {
					t.Fatalf("decision=%q", decision)
				}
				replay := fixture.amendedArt
				replay.ExpectedVersion, replay.Fence, replay.ProviderResult = fixture.ticket.Version, fixture.fence, &key
				if _, err := fixture.db.RecordVerification(fixture.ctx, replay); err != nil {
					t.Fatalf("recovered record=%v", err)
				}
				if _, err := fixture.db.TransitionVerificationAmendmentAccepted(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_accepted", Fence: fixture.fence, EventPayload: "{}"}, key); err != nil {
					t.Fatalf("recovered accepted transition=%v", err)
				}
			} else {
				if decision != VerificationAmendmentRejected {
					t.Fatalf("decision=%q", decision)
				}
				if _, err := fixture.db.TransitionVerificationAmendmentRejected(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}, key); err != nil {
					t.Fatalf("recovered rejected transition=%v", err)
				}
			}
			current, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
			if err != nil || current.State != domain.StateBuilding {
				t.Fatalf("completion ticket=%+v err=%v", current, err)
			}
			var attempts int
			if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND role='reviewer'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&attempts); err != nil || attempts != 2 {
				t.Fatalf("reviewer attempts=%d err=%v", attempts, err)
			}
		})
	}
}

func TestVerificationAmendmentDecisionHistoryCrossesRecoveryBeforeDecision(t *testing.T) {
	for _, tc := range []struct {
		name          string
		accepted      bool
		recoverBefore bool
	}{
		{name: "accepted_direct", accepted: true},
		{name: "accepted_after_recovery", accepted: true, recoverBefore: true},
		{name: "rejected_direct"},
		{name: "rejected_after_recovery", recoverBefore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := verificationAmendmentLifecycle(t, tc.accepted)
			requestVersion := fixture.ticket.Version
			requestFence := fixture.fence
			if tc.recoverBefore {
				recoverAmendmentFixtureFence(t, &fixture, "before-decision")
			}

			pending, err := fixture.db.PendingVerificationAmendment(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence)
			if err != nil || pending.TransitionTicketVersion != requestVersion || pending.Fence != requestFence {
				t.Fatalf("recovered pending=%+v request=%d/%+v err=%v", pending, requestVersion, requestFence, err)
			}
			decision, err := fixture.db.VerificationAmendmentDecision(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence, fixture.amendedKey)
			want := VerificationAmendmentRejected
			if tc.accepted {
				want = VerificationAmendmentAccepted
			}
			if err != nil || decision != want {
				t.Fatalf("recovered decision=%q want=%q err=%v", decision, want, err)
			}
			if tc.accepted {
				replay := fixture.amendedArt
				replay.ExpectedVersion, replay.Fence, replay.ProviderResult = fixture.ticket.Version, fixture.fence, &fixture.amendedKey
				if _, err := fixture.db.RecordVerification(fixture.ctx, replay); err != nil {
					t.Fatalf("recovered record=%v", err)
				}
				if _, err := fixture.db.TransitionVerificationAmendmentAccepted(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_accepted", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey); err != nil {
					t.Fatal(err)
				}
			} else if _, err := fixture.db.TransitionVerificationAmendmentRejected(fixture.ctx, Transition{Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "amendment_rejected", Fence: fixture.fence, EventPayload: "{}"}, fixture.amendedKey); err != nil {
				t.Fatal(err)
			}

			recoverAmendmentFixtureFence(t, &fixture, "after-decision")
			verification, err := fixture.db.CurrentVerification(fixture.ctx, fixture.ticket.Ref)
			if err != nil || verification.Revision.Revision == 0 || verification.Fence != fixture.fence {
				t.Fatalf("current verification=%+v err=%v", verification, err)
			}
			recoverAmendmentFixtureFence(t, &fixture, "before-next-builder")
			verification, err = fixture.db.CurrentVerification(fixture.ctx, fixture.ticket.Ref)
			if err != nil || verification.Revision.Revision == 0 || verification.Fence != fixture.fence {
				t.Fatalf("replayed verification=%+v err=%v", verification, err)
			}
		})
	}
}

func recoverAmendmentFixtureFence(t *testing.T, fixture *verificationAmendmentFixture, workflow string) {
	t.Helper()
	if err := fixture.db.restoreRuntimeControls(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	leader, err := fixture.db.AcquireLeader(fixture.ctx, fixture.ticket.Ref.Channel, "verification-amendment-"+workflow)
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.db.FenceRecoveredRunners(fixture.ctx, fixture.ticket.Ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("recovery changed=%d err=%v", changed, err)
	}
	fixture.ticket, err = fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fixture.fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: fixture.ticket.RunnerEpoch}
}

func verificationAmendmentLifecycle(t *testing.T, recordReplacement bool) verificationAmendmentFixture {
	return verificationAmendmentLifecycleWithReviewer(t, recordReplacement, nil)
}

func verificationAmendmentLifecycleWithReviewer(t *testing.T, recordReplacement bool, mutateReviewer func(*phaseartifact.Verification)) verificationAmendmentFixture {
	return verificationAmendmentLifecycleWithReviewerAndBeforeRequest(t, recordReplacement, mutateReviewer, nil)
}

func verificationAmendmentLifecycleWithReviewerAndBeforeRequest(t *testing.T, recordReplacement bool, mutateReviewer func(*phaseartifact.Verification), beforeRequest func(*testing.T, *Store, context.Context, Ticket, domain.Fence, VerificationRevision)) verificationAmendmentFixture {
	t.Helper()
	db, ctx := openTestStore(t)
	configDigest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "verification-amendment")
	if err != nil {
		t.Fatal(err)
	}
	ticket := setupProviderTicket(t, db, ctx, "SF-verification-amendment", leader)
	builder, reviewer := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}

	complete := func(phase domain.Phase, role string, binding contracts.RuntimeBinding, raw []byte, validation phaseartifact.Validation) ProviderAttemptClaim {
		t.Helper()
		claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: configDigest, Capacity: 1, At: time.Now().UTC()}))
		if err != nil {
			t.Fatalf("begin %s/%s provider attempt: %v", phase, role, err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "amendment", ProcessStartIdentity: fmt.Sprintf("amendment-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatalf("record %s/%s provider launch: %v", phase, role, err)
		}
		if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
			t.Fatalf("complete %s/%s provider attempt: %v", phase, role, err)
		}
		return claim
	}

	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"verification amendment is reviewed"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test"}, Details: "red"}, Paths: []string{"internal"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"proof"}}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	planner := complete(domain.PhasePlanning, "planner", runtime(builder), planRaw, phaseartifact.Validation{TicketType: ticket.Type})
	planKey := ProviderAttemptResultKey{AttemptID: planner.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: planner.Attempt}
	if _, err := db.RecordPlan(ctx, PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &plan, ProviderResult: &planKey, Acceptance: plan.Acceptance, ProofKind: string(plan.Proof.Kind), Paths: plan.Paths, Commands: plan.Commands, Risks: plan.Risks}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPlan(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	planIdentity, err := workflowprompt.NewPlanIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	original := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal/proof_test.go"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: sha256Digest([]byte("original"))}
	originalRaw, _ := json.Marshal(original)
	originalClaim := complete(domain.PhaseVerification, "reviewer", runtime(reviewer), originalRaw, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: planIdentity.Digest})
	originalKey := ProviderAttemptResultKey{AttemptID: originalClaim.ID, Ref: ticket.Ref, Phase: domain.PhaseVerification, Attempt: originalClaim.Attempt}
	originalIntent, _ := workflowprompt.CanonicalVerificationIntentBytes(original)
	originalProof, _ := workflowprompt.CanonicalVerificationProofBytes(original)
	checkpoint := strings.Repeat("c", 40)
	originalCommand := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, ticket.Ref, ticket.Version, fence, originalKey, sha256Digest(originalIntent), sha256Digest(originalProof), "", "", 1)
	prior, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: originalIntent, Proof: originalProof, OwnedFiles: original.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &originalKey, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: strings.Repeat("a", 40), TreeOID: strings.Repeat("d", 40)}, CommandResult: originalCommand})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionVerification(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	if beforeRequest != nil {
		beforeRequest(t, db, ctx, ticket, fence, prior)
	}

	proposal := original
	proposal.Command = []string{"go", "test", "./..."}
	proposal.EvidenceDigest = sha256Digest([]byte("replacement"))
	proposalProof, _ := workflowprompt.CanonicalVerificationProofBytes(proposal)
	builderArtifact := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "replace the protected proof", ChangedFiles: []string{"internal/proof_test.go"}, Commands: [][]string{{"go", "test"}}, AmendmentRequest: &phaseartifact.AmendmentRequest{OldProofDigest: prior.ProofDigest, ProposedDigest: sha256Digest(proposalProof), ProposedCommand: proposal.Command, Reason: "the original proof command misses the regression"}}
	builderRaw, _ := json.Marshal(builderArtifact)
	builderClaim := complete(domain.PhaseBuild, "builder", runtime(builder), builderRaw, phaseartifact.Validation{TicketType: ticket.Type, ProtectedVerification: original.OwnedFiles})
	builderKey := ProviderAttemptResultKey{AttemptID: builderClaim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: builderClaim.Attempt}
	if _, err := db.TransitionVerificationAmendmentRequest(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateBuilding, To: domain.StateVerifying, Trigger: "verification_amendment_requested", Fence: fence, EventPayload: "{}"}, builderKey); err != nil {
		t.Fatalf("transition verification amendment request: %v", err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fence.RunnerEpoch = ticket.RunnerEpoch
	amended := proposal
	if !recordReplacement {
		amended.EvidenceDigest = sha256Digest([]byte("original"))
	}
	if mutateReviewer != nil {
		mutateReviewer(&amended)
	}
	amendedRaw, _ := json.Marshal(amended)
	amendedClaim := complete(domain.PhaseVerification, "reviewer", runtime(reviewer), amendedRaw, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: planIdentity.Digest})
	amendedKey := ProviderAttemptResultKey{AttemptID: amendedClaim.ID, Ref: ticket.Ref, Phase: domain.PhaseVerification, Attempt: amendedClaim.Attempt}
	amendedIntent, _ := workflowprompt.CanonicalVerificationIntentBytes(amended)
	amendedProof, _ := workflowprompt.CanonicalVerificationProofBytes(amended)
	amendedCommand := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, ticket.Ref, ticket.Version, fence, amendedKey, sha256Digest(amendedIntent), sha256Digest(amendedProof), "", "", 1)
	amendedCheckpoint := strings.Repeat("e", 40)
	amendedArt := VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: amendedIntent, Proof: amendedProof, OwnedFiles: amended.OwnedFiles, CheckpointID: amendedCheckpoint, ProviderResult: &amendedKey, Checkpoint: CommitObservation{CommitOID: amendedCheckpoint, ParentOID: checkpoint, TreeOID: strings.Repeat("f", 40)}, CommandResult: amendedCommand}
	if recordReplacement {
		if pending, err := db.PendingVerificationAmendment(ctx, ticket.Ref, ticket.Version, fence); err != nil || pending.BuilderResult != builderKey {
			t.Fatalf("pending amendment=%+v err=%v", pending, err)
		}
		if decision, err := db.VerificationAmendmentDecision(ctx, ticket.Ref, ticket.Version, fence, amendedKey); err != nil || decision != VerificationAmendmentAccepted {
			t.Fatalf("accepted amendment decision=%q err=%v", decision, err)
		}
		if _, err := db.RecordVerification(ctx, amendedArt); err != nil {
			t.Fatal(err)
		}
	}
	return verificationAmendmentFixture{db: db, ctx: ctx, ticket: ticket, fence: fence, config: configDigest, builder: builder, reviewer: reviewer, proposal: proposal, amended: amended, amendedKey: amendedKey, amendedArt: amendedArt, priorProof: prior.ProofDigest, priorRev: prior.Revision, budgetID: fmt.Sprintf("verification-amendment/%d/%s", builderClaim.ID, mustHistoricalTypedSHA(t, db, ctx, builderKey))}
}

func completeReplacementAmendmentBuilder(t *testing.T, fixture verificationAmendmentFixture, ticket Ticket, fence domain.Fence) ProviderAttemptResultKey {
	t.Helper()
	proposalProof, err := workflowprompt.CanonicalVerificationProofBytes(fixture.proposal)
	if err != nil {
		t.Fatal(err)
	}
	builderArtifact := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "replace the protected proof", ChangedFiles: []string{"internal/proof_test.go"}, Commands: [][]string{{"go", "test"}}, AmendmentRequest: &phaseartifact.AmendmentRequest{OldProofDigest: fixture.priorProof, ProposedDigest: sha256Digest(proposalProof), ProposedCommand: fixture.proposal.Command, Reason: "the original proof command misses the regression"}}
	raw, err := json.Marshal(builderArtifact)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.db.BeginProviderAttempt(fixture.ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(fixture.builder), ConfigDigest: fixture.config, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.RecordProviderLaunch(fixture.ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "amendment", ProcessStartIdentity: fmt.Sprintf("amendment-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.CompleteProviderAttemptSuccess(fixture.ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type, ProtectedVerification: []string{"internal/proof_test.go"}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}
}

func resumeRejectedAmendmentIntoFreshVerification(t *testing.T, fixture verificationAmendmentFixture) verificationAmendmentFixture {
	t.Helper()
	building, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil || building.State != domain.StateBuilding {
		t.Fatalf("building ticket=%+v err=%v", building, err)
	}
	worktree, err := fixture.db.Worktree(fixture.ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	baseline := TakeoverRemoteBaseline{Registered: true, WorktreePath: worktree.Path, WorktreeBranch: worktree.Branch, WorktreeIdentity: sha256Digest(worktree.IdentityJSON), BaseOID: worktree.BaseSHA}
	take, err := json.Marshal(map[string]any{"intent": "take", "operator": "sofia", "operator_uid": uint32(501)})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := fixture.db.TransitionAndInvalidateRunner(fixture.ctx, Transition{Ref: building.Ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StateStopping, ResumeState: domain.StateBuilding, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}, EventPayload: string(take)})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := fixture.db.Ticket(fixture.ctx, building.Ref)
	if err != nil || stopped.Version != stopping.Version {
		t.Fatalf("stopped ticket=%+v transition=%+v err=%v", stopped, stopping, err)
	}
	drain, err := json.Marshal(map[string]any{"drained": true, "intent": "take", "remote": baseline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.CompleteControlTransition(fixture.ctx, Transition{Ref: building.Ref, ExpectedVersion: stopped.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StateBuilding, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: stopped.RunnerEpoch}, EventPayload: string(drain)}); err != nil {
		t.Fatal(err)
	}
	paused, err := fixture.db.Ticket(fixture.ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	source := contracts.OperatorSourceCommit{CommitOID: strings.Repeat("f", 40), ParentOID: fixture.amendedArt.Checkpoint.ParentOID, TreeOID: strings.Repeat("e", 40), Changes: []contracts.OperatorSourceChange{{Status: "M", Path: "internal/source_resume.go"}}}
	resumed, err := fixture.db.TransitionOperatorSourceResume(fixture.ctx, OperatorSourceResume{Ref: building.Ref, ExpectedVersion: paused.Version, Fence: domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: paused.RunnerEpoch}, Operator: "sofia", SourceCommit: source, Remote: baseline})
	if err != nil {
		t.Fatal(err)
	}
	fixture.ticket, err = fixture.db.Ticket(fixture.ctx, building.Ref)
	if err != nil || fixture.ticket.Version != resumed.Version || fixture.ticket.State != domain.StateVerifying {
		t.Fatalf("resumed ticket=%+v transition=%+v err=%v", fixture.ticket, resumed, err)
	}
	openExactRuntimeAdmission(t, fixture.db, building.Ref)
	fixture.fence = domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: fixture.ticket.RunnerEpoch}
	return fixture
}

func mustHistoricalTypedSHA(t *testing.T, db *Store, ctx context.Context, key ProviderAttemptResultKey) string {
	t.Helper()
	result, _, err := db.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return result.TypedSHA256
}
