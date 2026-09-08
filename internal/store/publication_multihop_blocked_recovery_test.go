package store

import (
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPublishingResumeRejectsOverlappingRunnerRecovery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pause  func(*testing.T, *Store, Ticket, domain.Fence) TransitionResult
		resume func(*testing.T, *Store, Ticket, domain.Fence, TransitionResult) TransitionResult
	}{
		{
			name: "typed blocker",
			pause: func(t *testing.T, db *Store, ticket Ticket, fence domain.Fence) TransitionResult {
				t.Helper()
				result, err := db.TransitionPublishedBlock(t.Context(), Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateBlocked, ResumeState: domain.StatePublishing, Trigger: "typed_blocker", Fence: fence, EventPayload: `{"code":"publication_retry_required"}`})
				if err != nil {
					t.Fatal(err)
				}
				return result
			},
			resume: func(t *testing.T, db *Store, ticket Ticket, fence domain.Fence, paused TransitionResult) TransitionResult {
				t.Helper()
				result, err := db.TransitionPublishedResume(t.Context(), Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StateBlocked, To: domain.StatePublishing, Trigger: "operator_recover", Fence: fence, EventPayload: `{}`})
				if err != nil {
					t.Fatal(err)
				}
				return result
			},
		},
		{
			name: "semantic pause",
			pause: func(t *testing.T, db *Store, ticket Ticket, fence domain.Fence) TransitionResult {
				t.Helper()
				result, err := db.TransitionPublishedPause(t.Context(), Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StatePaused, ResumeState: domain.StatePublishing, Trigger: "retry_or_correction_exhausted", Fence: fence, EventPayload: `{}`})
				if err != nil {
					t.Fatal(err)
				}
				return result
			},
			resume: func(t *testing.T, db *Store, ticket Ticket, fence domain.Fence, paused TransitionResult) TransitionResult {
				t.Helper()
				result, err := db.TransitionPublishedResume(t.Context(), Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePublishing, Trigger: "operator_retry", Fence: fence, EventPayload: `{}`})
				if err != nil {
					t.Fatal(err)
				}
				return result
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx, ticket, fence := publicationLifecycleFixture(t)
			recordFixturePublication(t, db, ctx, ticket, fence)
			candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			paused := tc.pause(t, db, ticket, fence)
			resumed := tc.resume(t, db, ticket, fence, paused)

			step := RunnerRecoveryLedger{
				Ref:                ticket.Ref,
				PriorTicketVersion: paused.Version - 1,
				PriorRunnerEpoch:   fence.RunnerEpoch,
				PriorLeaderEpoch:   fence.LeaderEpoch,
				TicketVersion:      paused.Version,
				RunnerEpoch:        fence.RunnerEpoch + 1,
				LeaderEpoch:        fence.LeaderEpoch + 1,
				CreatedAt:          time.Now().UTC(),
			}
			payload, err := runnerRecoveryPayload(step)
			if err != nil {
				t.Fatal(err)
			}
			step.RecoveryDigest = publicationIdentityDigest(payload)
			if _, err := db.db.ExecContext(ctx, `INSERT INTO runner_recovery_ledger(channel,project_id,ticket_id,prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, step.Ref.Channel, step.Ref.Project, step.Ref.Ticket, step.PriorTicketVersion, step.PriorRunnerEpoch, step.PriorLeaderEpoch, step.TicketVersion, step.RunnerEpoch, step.LeaderEpoch, step.RecoveryDigest, step.CreatedAt.Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}

			if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, candidate, resumed.Version, fence); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("candidate-only recovery accepted overlapping runner handoff: %v", err)
			}
			if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("publication replay accepted overlapping runner handoff: %v", err)
			}
		})
	}
}

func TestCandidateOnlyPublishingBlockedRecoverSurvivesMultipleRunnerRecoveries(t *testing.T) {
	db, ctx, ticket, _ := publicationLifecycleFixture(t)
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}

	var leader uint64
	for recovery := 1; recovery <= 2; recovery++ {
		leader, err = db.AcquireLeader(ctx, domain.ChannelDev, "candidate-only-blocked-recovery")
		if err != nil {
			t.Fatalf("recovery %d acquire leader: %v", recovery, err)
		}
		if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); fenceErr != nil || changed != 1 {
			t.Fatalf("recovery %d fence changed=%d err=%v", recovery, changed, fenceErr)
		}
		if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, leader); err != nil {
			t.Fatalf("recovery %d candidate-only startup proof: %v", recovery, err)
		}
	}

	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != domain.StatePublishing || current.Version != candidate.TicketVersion+3 {
		t.Fatalf("multi-hop publishing ticket=%+v candidate=%+v", current, candidate)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	blocked, err := db.TransitionPublishedBlock(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: current.Version,
		From:            domain.StatePublishing,
		To:              domain.StateBlocked,
		ResumeState:     domain.StatePublishing,
		Trigger:         "typed_blocker",
		Fence:           fence,
		EventPayload:    `{"code":"publication_retry_required"}`,
	})
	if err != nil {
		t.Fatalf("candidate-only publication block: %v", err)
	}

	resumed, err := db.TransitionPublishedResume(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: blocked.Version,
		From:            domain.StateBlocked,
		To:              domain.StatePublishing,
		Trigger:         "operator_recover",
		Fence:           fence,
		EventPayload:    `{}`,
	})
	if err != nil {
		t.Fatalf("multi-hop candidate-only blocked recovery: %v", err)
	}
	if resumed.Version != blocked.Version+1 || resumed.EventID == 0 {
		t.Fatalf("multi-hop candidate-only resume=%+v blocked=%+v", resumed, blocked)
	}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, candidate, resumed.Version, fence); err != nil {
		t.Fatalf("resumed candidate-only publishing authority: %v", err)
	}
	blockedAgain, err := db.TransitionPublishedBlock(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: resumed.Version,
		From:            domain.StatePublishing,
		To:              domain.StateBlocked,
		ResumeState:     domain.StatePublishing,
		Trigger:         "typed_blocker",
		Fence:           fence,
		EventPayload:    `{"code":"publication_retry_required"}`,
	})
	if err != nil {
		t.Fatalf("second candidate-only publication block: %v", err)
	}
	resumedAgain, err := db.TransitionPublishedResume(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: blockedAgain.Version,
		From:            domain.StateBlocked,
		To:              domain.StatePublishing,
		Trigger:         "operator_recover",
		Fence:           fence,
		EventPayload:    `{}`,
	})
	if err != nil {
		t.Fatalf("second candidate-only blocked recovery: %v", err)
	}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, candidate, resumedAgain.Version, fence); err != nil {
		t.Fatalf("repeated candidate-only publishing authority: %v", err)
	}
	restartLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "candidate-only-blocked-restart")
	if err != nil {
		t.Fatalf("post-recover acquire leader: %v", err)
	}
	if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, restartLeader); fenceErr != nil || changed != 1 {
		t.Fatalf("post-recover fence changed=%d err=%v", changed, fenceErr)
	}
	if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, restartLeader); err != nil {
		t.Fatalf("post-recover candidate-only startup proof: %v", err)
	}
	restarted, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	restartedFence := domain.Fence{LeaderEpoch: restartLeader, RunnerEpoch: restarted.RunnerEpoch}
	if restarted.Version != resumedAgain.Version+1 || restarted.RunnerEpoch != fence.RunnerEpoch+1 {
		t.Fatalf("post-recover ticket=%+v prior=%+v", restarted, resumedAgain)
	}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, candidate, restarted.Version, restartedFence); err != nil {
		t.Fatalf("post-restart candidate-only publishing authority: %v", err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("candidate-only recovery manufactured publication evidence: %v", err)
	}
}

func TestCandidateOnlyPublishingBlockedRecoverCanSeedFirstRunnerRecovery(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	current := ticket
	for cycle := 1; cycle <= 2; cycle++ {
		blocked, err := db.TransitionPublishedBlock(ctx, Transition{
			Ref:             ticket.Ref,
			ExpectedVersion: current.Version,
			From:            domain.StatePublishing,
			To:              domain.StateBlocked,
			ResumeState:     domain.StatePublishing,
			Trigger:         "typed_blocker",
			Fence:           fence,
			EventPayload:    `{"code":"publication_retry_required"}`,
		})
		if err != nil {
			t.Fatalf("cycle %d block: %v", cycle, err)
		}
		resumed, err := db.TransitionPublishedResume(ctx, Transition{
			Ref:             ticket.Ref,
			ExpectedVersion: blocked.Version,
			From:            domain.StateBlocked,
			To:              domain.StatePublishing,
			Trigger:         "operator_recover",
			Fence:           fence,
			EventPayload:    `{}`,
		})
		if err != nil {
			t.Fatalf("cycle %d recover: %v", cycle, err)
		}
		current.Version = resumed.Version
	}

	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "candidate-only-first-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); fenceErr != nil || changed != 1 {
		t.Fatalf("first recovery after publication pairs changed=%d err=%v", changed, fenceErr)
	}
	recovered, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	recoveredFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, candidate, recovered.Version, recoveredFence); err != nil {
		t.Fatalf("first signed recovery after publication pairs: %v", err)
	}
}

func TestCandidateOnlyPublishingMixedControlsRejectLeaderTakeoverBeforeFence(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := db.TransitionPublishedBlock(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateBlocked,
		ResumeState: domain.StatePublishing, Trigger: "typed_blocker", Fence: fence,
		EventPayload: `{"code":"publication_retry_required"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublishedResume(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePublishing,
		Trigger: "operator_recover", Fence: fence, EventPayload: `{}`,
	}); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, paused := postPublicationPauseAt(t, db, current, fence)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "candidate-only-paused-takeover")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublishedResume(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePublishing,
		Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}, EventPayload: `{}`,
	}); err == nil {
		t.Fatal("mixed publication controls authorized a bare leader takeover before startup fencing")
	}
	stillPaused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || stillPaused.State != domain.StatePaused || stillPaused.Version != paused.Version || stillPaused.RunnerEpoch != paused.RunnerEpoch {
		t.Fatalf("refused leader-only resume mutated ticket=%+v err=%v", stillPaused, err)
	}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, candidate, paused.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}); err == nil {
		t.Fatal("candidate publication authority accepted the new leader without a signed recovery")
	}
}

func TestPublishingResumeRejectsWaitingCIExhaustionTrigger(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	paused, err := db.TransitionPublishedPause(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: ticket.Version,
		From:            domain.StatePublishing,
		To:              domain.StatePaused,
		ResumeState:     domain.StatePublishing,
		Trigger:         "retry_or_correction_exhausted",
		Fence:           fence,
		EventPayload:    `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := db.TransitionPublishedResume(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: paused.Version,
		From:            domain.StatePaused,
		To:              domain.StatePublishing,
		Trigger:         "operator_retry",
		Fence:           fence,
		EventPayload:    `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE events SET trigger='ci_red_exhausted' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, paused.Version); err != nil {
		t.Fatal(err)
	}
	if err := authenticateSemanticPublicationResume(ctx, db.db, ticket.Ref, paused.Version, resumed.Version, domain.StatePublishing); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("publishing resume accepted waiting-ci-only trigger: %v", err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("publication reader accepted waiting-ci-only trigger: %v", err)
	}
}
