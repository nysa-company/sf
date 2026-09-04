package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

type completedCandidateRepairTestFixture struct {
	db                    *Store
	building              Ticket
	buildFence            domain.Fence
	candidate             StoredCandidate
	preRedRecoveryVersion uint64
}

func newCompletedCandidateRepairTestFixture(t *testing.T) completedCandidateRepairTestFixture {
	return newCompletedCandidateRepairTestFixtureWithPreRedRecovery(t, false)
}

func newCompletedCandidateRepairTestFixtureWithPreRedRecovery(t *testing.T, recoverBeforeRed bool) completedCandidateRepairTestFixture {
	t.Helper()
	ctx := t.Context()
	var db *Store
	var waiting Ticket
	var publication PublishedCandidateEvidence
	var observation CIObservation
	var preRedRecoveryVersion uint64
	if !recoverBeforeRed {
		db, waiting, publication, observation = redCIConsumptionFixture(t)
	} else {
		var err error
		db, waiting, _ = ciAuthorityPublishedFixture(t)
		leader, leaderErr := db.AcquireLeader(ctx, waiting.Ref.Channel, "candidate-repair-pre-red-recovery")
		if leaderErr != nil {
			db.Close()
			t.Fatal(leaderErr)
		}
		if changed, fenceErr := db.FenceRecoveredRunners(ctx, waiting.Ref.Channel, leader); fenceErr != nil || changed != 1 {
			db.Close()
			t.Fatalf("pre-red recovery changed=%d err=%v", changed, fenceErr)
		}
		if err := db.RebindRecoveredPublishedCandidates(ctx, waiting.Ref.Channel, leader); err != nil {
			db.Close()
			t.Fatal(err)
		}
		waiting, err = db.Ticket(ctx, waiting.Ref)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		preRedRecoveryVersion = waiting.Version
		publication, err = db.LoadPublishedCandidate(ctx, waiting.Ref)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		if err := recordCIAuthorityPolicy(db, ctx, publication, waiting, ciAuthorityLintPolicy()); err != nil {
			db.Close()
			t.Fatal(err)
		}
		observation = ciAuthorityObservationFor(publication, waiting, domain.Fence{LeaderEpoch: leader, RunnerEpoch: waiting.RunnerEpoch}, "red", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "failure", FailingDiagnosticText: "lint failed after recovery"}})
		if err := db.recordCIObservation(ctx, observation); err != nil {
			db.Close()
			t.Fatal(err)
		}
		observation, err = db.LoadCurrentCIObservation(ctx, waiting.Ref)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	authority := redCICorrectionAuthority(t, waiting, observation)
	if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{
		Ref:               waiting.Ref,
		ObservationDigest: observation.ObservationDigest,
		ExpectedVersion:   waiting.Version,
		Fence:             observation.ObservedFence,
		CorrectionBudget:  &authority,
	}); err != nil {
		t.Fatal(err)
	}
	building, err := db.Ticket(ctx, waiting.Ref)
	if err != nil || building.State != domain.StateBuilding {
		t.Fatalf("building ticket=%+v err=%v", building, err)
	}
	buildFence := domain.Fence{LeaderEpoch: observation.ObservedFence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}

	// The immutable generation-one verification remains the authenticated
	// input to the correction Builder, but its Store projection must be bound
	// to the exact current repair endpoint for both Worker and PhaseRunner.
	verification, err := db.CurrentVerification(ctx, waiting.Ref)
	if err != nil || verification.TicketVersion != building.Version || verification.Fence != buildFence {
		t.Fatalf("repair verification=%+v err=%v", verification, err)
	}
	// Before a generation-two Builder result exists, the predecessor Builder
	// result is historical context, never reusable work for the repair cycle.
	if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{
		Ref: waiting.Ref, Phase: domain.PhaseBuild, Role: "builder",
		ExpectedVersion: building.Version, Fence: buildFence,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("predecessor Builder remained reusable in repair cycle: %v", err)
	}

	builderQualification, _ := setupProviderPair(t, db, ctx)
	worktree, err := db.Worktree(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	request := supervised(t, ProviderAttemptRequest{
		Ref: waiting.Ref, ExpectedVersion: building.Version, Fence: buildFence,
		Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builderQualification),
		ConfigDigest: building.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
	})
	request.WorktreeIdentity = string(worktree.IdentityJSON)
	request.Input.WorktreeIdentity = string(worktree.IdentityJSON)
	request.BaseSHA = worktree.BaseSHA
	request.Input.BaseSHA = worktree.BaseSHA
	claim, err := db.BeginProviderAttempt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{
		PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "repair-composition",
		ProcessStartIdentity: fmt.Sprintf("repair-composition-%d", claim.ID), Worktree: claim.Worktree,
	}); err != nil {
		t.Fatal(err)
	}
	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"repair","changed_files":["internal/repair.go"],"commands":[["go","test"]]}`)
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), building.Version, buildFence,
		contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: builderRaw, UsageTrusted: true, UsageUnits: 1},
		phaseartifact.Validation{TicketType: building.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: waiting.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || parsed.Builder == nil {
		t.Fatalf("repair Builder result=%+v err=%v", parsed, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	commandPolicyDigest := sha256Digest([]byte("repair-composition-policy"))
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate,
		waiting.Ref, building.Version, buildFence, key,
		publication.Candidate.Snapshot.VerificationIntentDigest,
		publication.Candidate.Snapshot.ProofDigest,
		verification.Checkpoint.CommitOID, "sha256:"+commandPolicyDigest, 0)

	// Worker intentionally submits generation zero and lets Store allocate the
	// exact next generation. A correction commit is a child of the predecessor
	// candidate, not a sibling from the old verification checkpoint.
	successor := domain.CandidateSnapshot{
		BaseSHA: publication.Candidate.Snapshot.BaseSHA,
		HeadSHA: strings.Repeat("7", 40), TreeSHA: strings.Repeat("8", 40),
		SourceDigest:             publication.Candidate.Snapshot.SourceDigest,
		VerificationIntentDigest: publication.Candidate.Snapshot.VerificationIntentDigest,
		ProofDigest:              publication.Candidate.Snapshot.ProofDigest,
		CommandPolicyDigest:      commandPolicyDigest,
		BuilderEvidenceDigest:    builderDigest,
	}
	commit := CommitObservation{CommitOID: successor.HeadSHA, ParentOID: publication.Candidate.Snapshot.HeadSHA, TreeOID: successor.TreeSHA}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{
		Ref: waiting.Ref, ExpectedVersion: building.Version, Fence: buildFence,
		Snapshot: successor, BuilderResult: key, Commit: commit,
		Reason: "authenticated CI repair", CommandResult: command,
	}); err != nil {
		t.Fatalf("record correction candidate: %v", err)
	}
	stored, err := db.RecoverableCandidate(ctx, waiting.Ref)
	if err != nil || stored.Snapshot.Generation != publication.Candidate.Snapshot.Generation+1 || stored.Commit != commit {
		t.Fatalf("stored correction candidate=%+v err=%v", stored, err)
	}
	var completions int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, stored.Snapshot.Generation).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if completions != 1 {
		t.Fatalf("RecordCandidate did not atomically append repair completion: %d", completions)
	}
	return completedCandidateRepairTestFixture{db: db, building: building, buildFence: buildFence, candidate: stored, preRedRecoveryVersion: preRedRecoveryVersion}
}

func TestCandidateRepairBuildingAdmitsFreshBuilderAndRecordsCompletionAtomically(t *testing.T) {
	fixture := newCompletedCandidateRepairTestFixture(t)
	defer fixture.db.Close()

	ctx := t.Context()
	if _, err := fixture.db.TransitionCandidate(ctx, Transition{
		Ref: fixture.building.Ref, ExpectedVersion: fixture.building.Version, From: domain.StateBuilding,
		To: domain.StatePublishing, Trigger: "phase_pass", Fence: fixture.buildFence, EventPayload: "{}",
	}, fixture.candidate.Snapshot); err != nil {
		t.Fatalf("transition corrected candidate: %v", err)
	}
}

func TestCandidateRepairSecondRedPausesWithoutSpendingAnotherCorrection(t *testing.T) {
	fixture := newCompletedCandidateRepairTestFixture(t)
	defer fixture.db.Close()
	ctx := t.Context()

	if _, err := fixture.db.TransitionCandidate(ctx, Transition{
		Ref: fixture.building.Ref, ExpectedVersion: fixture.building.Version, From: domain.StateBuilding,
		To: domain.StatePublishing, Trigger: "phase_pass", Fence: fixture.buildFence, EventPayload: "{}",
	}, fixture.candidate.Snapshot); err != nil {
		t.Fatal(err)
	}
	publishing, err := fixture.db.Ticket(ctx, fixture.building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	recordFixturePublication(t, fixture.db, ctx, publishing, fixture.buildFence)
	if _, err := fixture.db.TransitionPublishedCandidate(ctx, Transition{
		Ref: publishing.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing,
		To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fixture.buildFence, EventPayload: "{}",
	}); err != nil {
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(ctx, publishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadPublishedCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	retained, err := fixture.db.candidateRepairBuildAuthorityHistoricalAt(ctx, fixture.db.db, waiting.Ref, fixture.building.Version, fixture.buildFence)
	if err != nil || retained.context.PredecessorGeneration+1 != retained.context.TargetGeneration || retained.context.PredecessorGeneration == publication.Candidate.Snapshot.Generation {
		t.Fatalf("retained repair authority was shadowed by successor publication authority=%+v err=%v", retained, err)
	}
	checks := ciAuthorityLintPolicy()
	if err := recordCIAuthorityPolicy(fixture.db, ctx, publication, waiting, checks); err != nil {
		t.Fatal(err)
	}
	red := ciAuthorityObservationFor(publication, waiting, fixture.buildFence, "red", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "failure", FailingDiagnosticText: "still failing"}})
	if err := fixture.db.recordCIObservation(ctx, red); err != nil {
		t.Fatal(err)
	}
	loaded, err := fixture.db.LoadCurrentCIObservation(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	authority := redCICorrectionAuthority(t, waiting, loaded)
	if _, err := fixture.db.ConsumeCIObservation(ctx, CIObservationTransition{
		Ref: waiting.Ref, ObservationDigest: loaded.ObservationDigest,
		ExpectedVersion: waiting.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority,
	}); err != nil {
		t.Fatal(err)
	}

	current, err := fixture.db.Ticket(ctx, waiting.Ref)
	if err != nil || current.State != domain.StatePaused || current.ResumeState != domain.StateWaitingCI {
		t.Fatalf("second red ticket=%+v err=%v", current, err)
	}
	var uses, bindings int
	if err := fixture.db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if uses != 1 || bindings != 1 {
		t.Fatalf("second red spent another repair uses=%d bindings=%d", uses, bindings)
	}
	var payload string
	if err := fixture.db.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='checks_red' AND from_state='waiting_ci' AND to_state='paused'`, waiting.Ref.Channel, waiting.Ref.Project, waiting.Ref.Ticket, current.Version).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"code":"ci_red_exhausted"`) || !strings.Contains(payload, `"reason":"required CI checks remain red after the single diagnosed repair loop"`) {
		t.Fatalf("second-red audit payload=%s", payload)
	}
}
