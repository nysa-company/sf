package store

import (
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPublishedCandidateRejectsFutureRunnerRecoveryRow(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)

	step := RunnerRecoveryLedger{
		Ref:                ticket.Ref,
		PriorTicketVersion: ticket.Version,
		PriorRunnerEpoch:   ticket.RunnerEpoch,
		PriorLeaderEpoch:   fence.LeaderEpoch,
		TicketVersion:      ticket.Version + 1,
		RunnerEpoch:        ticket.RunnerEpoch + 1,
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

	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("publication reader ignored future recovery row: %v", err)
	}
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{
		Ref:             ticket.Ref,
		ExpectedVersion: ticket.Version,
		From:            domain.StatePublishing,
		To:              domain.StateWaitingCI,
		Trigger:         "effects_confirmed",
		Fence:           fence,
	}); !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("publication transition ignored future recovery row: %v", err)
	}
}

func TestCandidateCheckpointRowsAreImmutable(t *testing.T) {
	for _, mutation := range []struct {
		name string
		sql  string
	}{
		{name: "update", sql: `UPDATE candidate_snapshots SET head_sha='1111111111111111111111111111111111111111' WHERE channel=? AND project_id=? AND ticket_id=?`},
		{name: "delete", sql: `DELETE FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			db, _, ticket, _ := publicationLifecycleFixture(t)
			defer db.Close()
			if _, err := db.db.ExecContext(t.Context(), mutation.sql, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err == nil {
				t.Fatalf("candidate checkpoint %s unexpectedly succeeded", mutation.name)
			}
		})
	}
}

func TestCandidateOnlyPublishingRejectsUnauthenticatedLatestBinding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		leader uint64
		runner uint64
	}{
		{name: "leader", leader: 999, runner: 1},
		{name: "runner", leader: 1, runner: 999},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, ctx, ticket, fence := publicationLifecycleFixture(t)
			candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.db.ExecContext(ctx, `INSERT INTO candidate_result_bindings(channel,project_id,ticket_id,generation,binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,commit_parent_oid) VALUES(?,?,?,?,?,?,?,?,?,?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, candidate.Snapshot.Generation, ticket.Version, tc.leader, tc.runner, candidate.BuilderResult.AttemptID, candidate.BuilderResult.Attempt, candidate.Commit.ParentOID); err != nil {
				t.Fatal(err)
			}
			selected, err := db.RecoverableCandidate(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, selected, ticket.Version, fence); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("unauthenticated latest binding was accepted: %+v err=%v", selected, err)
			}
		})
	}
}

func TestCandidateOnlyPublishingCanRecordAfterMultipleRunnerRecoveries(t *testing.T) {
	db, ctx, ticket, _ := publicationLifecycleFixture(t)
	defer db.Close()

	var leader uint64
	var err error
	for recovery := 0; recovery < 2; recovery++ {
		leader, err = db.AcquireLeader(ctx, domain.ChannelDev, "candidate-only-record-recovery")
		if err != nil {
			t.Fatalf("recovery %d acquire leader: %v", recovery+1, err)
		}
		if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); fenceErr != nil || changed != 1 {
			t.Fatalf("recovery %d fence changed=%d err=%v", recovery+1, changed, fenceErr)
		}
		if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, leader); err != nil {
			t.Fatalf("recovery %d candidate-only startup proof: %v", recovery+1, err)
		}
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != ticket.Version+2 {
		t.Fatalf("recovered ticket version=%d want=%d", current.Version, ticket.Version+2)
	}
	recoveredFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	recordFixturePublication(t, db, ctx, current, recoveredFence)
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{
		Ref:             current.Ref,
		ExpectedVersion: current.Version,
		From:            domain.StatePublishing,
		To:              domain.StateWaitingCI,
		Trigger:         "effects_confirmed",
		Fence:           recoveredFence,
		EventPayload:    `{}`,
	}); err != nil {
		t.Fatalf("transition recovered publication: %v", err)
	}
	loaded, err := db.LoadPublishedCandidate(ctx, current.Ref)
	if err != nil {
		t.Fatalf("load recovered publication: %v", err)
	}
	if loaded.TicketVersion != current.Version || loaded.Candidate.TicketVersion >= loaded.TicketVersion {
		t.Fatalf("recovered publication=%+v", loaded)
	}
}

func TestCurrentCandidateBindingCanSeedNextPublishingRecovery(t *testing.T) {
	db, ctx, ticket, _ := publicationLifecycleFixture(t)
	defer db.Close()
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}

	firstLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "current-candidate-recovery-one")
	if err != nil {
		t.Fatal(err)
	}
	if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, firstLeader); fenceErr != nil || changed != 1 {
		t.Fatalf("first fence changed=%d err=%v", changed, fenceErr)
	}
	firstTicket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	firstFence := domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: firstTicket.RunnerEpoch}
	reusable, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{
		Ref: ticket.Ref, Phase: domain.PhaseBuild, Role: "builder",
		ExpectedVersion: firstTicket.Version, Fence: firstFence,
	})
	if err != nil || !reusable.Recovered || reusable.Key != candidate.BuilderResult {
		t.Fatalf("reusable builder=%+v err=%v", reusable, err)
	}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{
		Ref: ticket.Ref, ExpectedVersion: firstTicket.Version, Fence: firstFence,
		Snapshot: candidate.Snapshot, BuilderResult: candidate.BuilderResult,
		Commit: candidate.Commit, Reason: "rebind candidate before next restart",
		CommandResult: candidate.CommandBinding.Key,
	}); err != nil {
		t.Fatalf("rebind current candidate: %v", err)
	}
	current, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil || current.TicketVersion != firstTicket.Version || current.Fence != firstFence {
		t.Fatalf("current candidate=%+v ticket=%+v fence=%+v err=%v", current, firstTicket, firstFence, err)
	}

	secondLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "current-candidate-recovery-two")
	if err != nil {
		t.Fatal(err)
	}
	if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); fenceErr != nil || changed != 1 {
		t.Fatalf("second fence changed=%d err=%v", changed, fenceErr)
	}
	secondTicket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	secondFence := domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: secondTicket.RunnerEpoch}
	if secondTicket.Version != firstTicket.Version+1 || secondTicket.RunnerEpoch != firstTicket.RunnerEpoch+1 {
		t.Fatalf("second recovery ticket=%+v first=%+v", secondTicket, firstTicket)
	}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, current, secondTicket.Version, secondFence); err != nil {
		t.Fatalf("current candidate did not seed next recovery: %v", err)
	}
}

func TestRecordCandidateCannotAppendBindingAfterPublicationWitness(t *testing.T) {
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	recordFixturePublication(t, db, ctx, ticket, fence)
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "post-witness-candidate-replay")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("post-witness fence changed=%d err=%v", changed, err)
	}
	if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	currentFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: current.Version, Fence: currentFence, Snapshot: candidate.Snapshot, BuilderResult: candidate.BuilderResult, Commit: candidate.Commit, Reason: "late recovered builder replay", CommandResult: candidate.CommandBinding.Key}); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("post-witness candidate binding append=%v", err)
	}
	if _, err := db.LoadPublishedCandidate(ctx, ticket.Ref); err != nil {
		t.Fatalf("post-witness replay poisoned publication: %v", err)
	}
}
