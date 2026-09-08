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

func pendingCandidateRepairStartupFixture(t *testing.T) (*Store, Ticket, domain.Fence, CandidateRepairBuildContext) {
	t.Helper()
	db, waiting, _, observation := redCIConsumptionFixture(t)
	authority := redCICorrectionAuthority(t, waiting, observation)
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{
		Ref:               waiting.Ref,
		ObservationDigest: observation.ObservationDigest,
		ExpectedVersion:   waiting.Version,
		Fence:             observation.ObservedFence,
		CorrectionBudget:  &authority,
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	building, err := db.Ticket(t.Context(), waiting.Ref)
	if err != nil || building.State != domain.StateBuilding {
		db.Close()
		t.Fatalf("pending repair ticket=%+v err=%v", building, err)
	}
	buildFence := domain.Fence{LeaderEpoch: observation.ObservedFence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	context, err := db.CandidateRepairBuildContext(t.Context(), building.Ref, building.Version, buildFence)
	if err != nil {
		db.Close()
		t.Fatalf("pending repair context: %v", err)
	}
	var attempts, completions int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND entry_ticket_version=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket, building.Version).Scan(&attempts); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket, context.TargetGeneration).Scan(&completions); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if attempts != 0 || completions != 0 {
		db.Close()
		t.Fatalf("fixture crossed pre-Builder crash window attempts=%d completions=%d", attempts, completions)
	}
	return db, building, buildFence, context
}

func completeCandidateRepairBuilderBeforeCandidate(t *testing.T, db *Store, building Ticket, fence domain.Fence) (ProviderAttemptResultKey, phaseartifact.Builder) {
	t.Helper()
	ctx := t.Context()
	builderQualification, _ := setupProviderPair(t, db, ctx)
	worktree, err := db.Worktree(ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	request := supervised(t, ProviderAttemptRequest{
		Ref: building.Ref, ExpectedVersion: building.Version, Fence: fence,
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
		PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "repair-startup",
		ProcessStartIdentity: fmt.Sprintf("repair-startup-%d", claim.ID), Worktree: claim.Worktree,
	}); err != nil {
		t.Fatal(err)
	}
	raw := contracts.PhaseResult{
		Provider:     claim.Binding.Identity,
		Artifact:     []byte(`{"schema":"sf.builder/v1","summary":"repair before candidate","changed_files":["internal/repair.go"],"commands":[["go","test"]]}`),
		UsageTrusted: true, UsageUnits: 1,
	}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), building.Version, fence, raw, phaseartifact.Validation{TicketType: building.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: building.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || parsed.Builder == nil {
		t.Fatalf("completed repair Builder=%+v err=%v", parsed, err)
	}
	return key, *parsed.Builder
}

func reopenCandidateRepairStartupStore(t *testing.T, db *Store) *Store {
	t.Helper()
	var path string
	if err := db.db.QueryRowContext(t.Context(), `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("candidate repair database path=%q err=%v", path, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	return reopened
}

func completedCandidateRepairReviewingFixture(t *testing.T, fixture completedCandidateRepairTestFixture) finalReviewFixture {
	t.Helper()
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
	candidate, err := fixture.db.RecoverableCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.db.LoadPublishedCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	policy := CIRequiredCheckPolicy{
		Ref: waiting.Ref, CandidateGeneration: candidate.Snapshot.Generation,
		CandidateHeadSHA: candidate.Snapshot.HeadSHA, CandidateTreeSHA: candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest, ProtectedBranchRef: publication.PullRequest.BaseRef,
		ProtectedBranchOID: publication.PullRequest.BaseOID, PolicySourceDigest: strings.Repeat("b", 64),
		AuthenticatedPrincipal: "repair-startup", RequiredChecks: []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-repair"}},
		authenticated: true,
	}
	canonicalPolicy, err := canonicalCIPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.RecordCIRequiredCheckPolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	observation := CIObservation{
		Ref: waiting.Ref, CandidateGeneration: candidate.Snapshot.Generation,
		CandidateHeadSHA: candidate.Snapshot.HeadSHA, CandidateTreeSHA: candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest, PolicyWitnessDigest: canonicalPolicy.PolicyWitnessDigest,
		PullRequest: publication.PullRequest, ObservedTicketVersion: waiting.Version,
		ObservedFence: fixture.buildFence, ObservedAt: time.Now().UTC(),
		RequiredChecks: []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-repair", NormalizedState: "success"}},
		Classification: "green",
	}
	canonicalObservation, err := canonicalCIObservation(observation)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.db.recordCIObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.db.ConsumeCIObservation(ctx, CIObservationTransition{
		Ref: waiting.Ref, ObservationDigest: canonicalObservation.ObservationDigest,
		ExpectedVersion: waiting.Version, Fence: fixture.buildFence,
	}); err != nil {
		t.Fatal(err)
	}
	reviewing, err := fixture.db.Ticket(ctx, waiting.Ref)
	if err != nil || reviewing.State != domain.StateReviewing {
		t.Fatalf("candidate repair reviewing ticket=%+v err=%v", reviewing, err)
	}
	return finalReviewFixture{
		db: fixture.db, ctx: ctx, ticket: reviewing,
		fence:     domain.Fence{LeaderEpoch: fixture.buildFence.LeaderEpoch, RunnerEpoch: reviewing.RunnerEpoch},
		candidate: candidate,
	}
}

func TestFinalReviewAuthorityAuthenticatesCandidateRepairParentWithoutWeakeningOrdinaryParent(t *testing.T) {
	t.Run("ordinary checkpoint parent", func(t *testing.T) {
		fixture := finalReviewLifecycleFixture(t)
		defer fixture.db.Close()
		authority, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence)
		if err != nil || authority.Candidate.Commit.ParentOID != authority.Verification.Checkpoint.CommitOID {
			t.Fatalf("ordinary final review authority=%+v err=%v", authority, err)
		}
	})

	t.Run("authenticated repair parent and tamper", func(t *testing.T) {
		completed := newCompletedCandidateRepairTestFixture(t)
		fixture := completedCandidateRepairReviewingFixture(t, completed)
		defer fixture.db.Close()
		authority, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence)
		if err != nil || authority.Candidate.Commit.ParentOID == authority.Verification.Checkpoint.CommitOID {
			t.Fatalf("repair final review authority=%+v err=%v", authority, err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER candidate_result_bindings_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE candidate_result_bindings SET commit_parent_oid=? WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, strings.Repeat("9", 40), fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket, fixture.candidate.Snapshot.Generation); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.FinalReviewAuthority(fixture.ctx, fixture.ticket.Ref, fixture.ticket.Version, fixture.fence); !errors.Is(err, ErrEvidenceConflict) {
			t.Fatalf("tampered repair parent final review err=%v", err)
		}
	})
}

func TestPostPublicationRearmProofAuthenticatesCandidateRepairParentWithoutWeakeningOrdinaryParent(t *testing.T) {
	t.Run("ordinary checkpoint parent", func(t *testing.T) {
		fixture := finalReviewLifecycleFixture(t)
		defer fixture.db.Close()
		stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
		capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
		if err != nil || capability == nil {
			t.Fatalf("ordinary reviewing rearm capability=%v err=%v", capability, err)
		}
	})

	t.Run("authenticated repair parent", func(t *testing.T) {
		completed := newCompletedCandidateRepairTestFixture(t)
		fixture := completedCandidateRepairReviewingFixture(t, completed)
		defer fixture.db.Close()
		stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
		capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
		if err != nil || capability == nil {
			t.Fatalf("candidate-repair reviewing rearm capability=%v err=%v", capability, err)
		}
	})

	t.Run("repair parent tamper", func(t *testing.T) {
		completed := newCompletedCandidateRepairTestFixture(t)
		fixture := completedCandidateRepairReviewingFixture(t, completed)
		defer fixture.db.Close()
		stopped, resumed := postPublicationPauseResumeAt(t, fixture.db, fixture.ticket, fixture.fence, domain.StateReviewing)
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER candidate_result_bindings_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE candidate_result_bindings SET commit_parent_oid=? WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, strings.Repeat("9", 40), resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket, fixture.candidate.Snapshot.Generation); err != nil {
			t.Fatal(err)
		}
		capability, err := fixture.db.PostPublicationRearmProof(fixture.ctx, resumed.Ref, stopped)
		if capability != nil || !errors.Is(err, ErrControlNotDrained) {
			t.Fatalf("tampered candidate-repair reviewing rearm capability=%v err=%v", capability, err)
		}
		current, ticketErr := fixture.db.Ticket(fixture.ctx, resumed.Ref)
		if ticketErr != nil || current.Version != resumed.Version || current.RunnerEpoch != resumed.RunnerEpoch || current.State != domain.StateReviewing {
			t.Fatalf("tampered rearm mutated ticket=%+v err=%v", current, ticketErr)
		}
	})
}

func TestCandidateRepairStartupBeforeBuilderSurvivesRepeatedRunnerRecovery(t *testing.T) {
	db, building, priorFence, repair := pendingCandidateRepairStartupFixture(t)
	defer db.Close()
	ctx := t.Context()

	current := building
	for recovery := 1; recovery <= 2; recovery++ {
		previous := current
		previousLeader := priorFence.LeaderEpoch
		leader, err := db.AcquireLeader(ctx, building.Ref.Channel, "candidate-repair-before-builder")
		if err != nil {
			t.Fatalf("recovery %d acquire leader: %v", recovery, err)
		}
		changed, err := db.FenceRecoveredRunners(ctx, building.Ref.Channel, leader)
		if err != nil || changed != 1 {
			t.Fatalf("recovery %d fence changed=%d err=%v", recovery, changed, err)
		}
		current, err = db.Ticket(ctx, building.Ref)
		if err != nil || current.State != domain.StateBuilding || current.Version != previous.Version+1 || current.RunnerEpoch != previous.RunnerEpoch+1 {
			t.Fatalf("recovery %d ticket=%+v err=%v", recovery, current, err)
		}
		latest, found, err := loadLatestRunnerRecovery(ctx, db.db, building.Ref)
		if err != nil || !found || latest.PriorTicketVersion != previous.Version || latest.PriorRunnerEpoch != previous.RunnerEpoch || latest.PriorLeaderEpoch != previousLeader || latest.TicketVersion != current.Version || latest.RunnerEpoch != current.RunnerEpoch || latest.LeaderEpoch != leader {
			t.Fatalf("recovery %d ledger=%+v found=%v err=%v", recovery, latest, found, err)
		}
		priorFence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
		context, err := db.CandidateRepairBuildContext(ctx, current.Ref, current.Version, priorFence)
		if err != nil || context.TargetGeneration != repair.TargetGeneration || context.PredecessorGeneration != repair.PredecessorGeneration || context.EntryTicketVersion != repair.EntryTicketVersion {
			t.Fatalf("recovery %d repair context=%+v err=%v", recovery, context, err)
		}
		if reusable, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{
			Ref: current.Ref, Phase: domain.PhaseBuild, Role: "builder",
			ExpectedVersion: current.Version, Fence: priorFence,
		}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("recovery %d reused predecessor Builder=%+v err=%v", recovery, reusable, err)
		}
	}

	var latestGeneration uint64
	var completions int
	if err := db.db.QueryRowContext(ctx, `SELECT MAX(generation) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket).Scan(&latestGeneration); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket, repair.TargetGeneration).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if latestGeneration != repair.PredecessorGeneration || completions != 0 {
		t.Fatalf("startup fabricated repair output latest_generation=%d completions=%d", latestGeneration, completions)
	}
}

func TestCandidateRepairStartupCompletedBuilderBeforeCandidateReusesExactlyOnceAfterRepeatedRestart(t *testing.T) {
	db, building, priorFence, repair := pendingCandidateRepairStartupFixture(t)
	builderFence := priorFence
	key, builder := completeCandidateRepairBuilderBeforeCandidate(t, db, building, priorFence)
	ctx := t.Context()

	var latestGeneration uint64
	var completions int
	if err := db.db.QueryRowContext(ctx, `SELECT MAX(generation) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket).Scan(&latestGeneration); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket, repair.TargetGeneration).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if latestGeneration != repair.PredecessorGeneration || completions != 0 {
		t.Fatalf("completed Builder fabricated candidate latest_generation=%d completions=%d", latestGeneration, completions)
	}

	current := building
	var reusable LatestReusableProviderAttemptResult
	for recovery := 1; recovery <= 2; recovery++ {
		db = reopenCandidateRepairStartupStore(t, db)
		previous := current
		previousLeader := priorFence.LeaderEpoch
		leader, err := db.AcquireLeader(ctx, building.Ref.Channel, fmt.Sprintf("candidate-repair-completed-builder-%d", recovery))
		if err != nil {
			db.Close()
			t.Fatalf("recovery %d acquire leader: %v", recovery, err)
		}
		changed, err := db.FenceRecoveredRunners(ctx, building.Ref.Channel, leader)
		if err != nil || changed != 1 {
			db.Close()
			t.Fatalf("recovery %d fence changed=%d err=%v", recovery, changed, err)
		}
		current, err = db.Ticket(ctx, building.Ref)
		if err != nil || current.State != domain.StateBuilding || current.Version != previous.Version+1 || current.RunnerEpoch != previous.RunnerEpoch+1 {
			db.Close()
			t.Fatalf("recovery %d ticket=%+v err=%v", recovery, current, err)
		}
		latest, found, err := loadLatestRunnerRecovery(ctx, db.db, building.Ref)
		if err != nil || !found || latest.PriorTicketVersion != previous.Version || latest.PriorRunnerEpoch != previous.RunnerEpoch || latest.PriorLeaderEpoch != previousLeader || latest.TicketVersion != current.Version || latest.RunnerEpoch != current.RunnerEpoch || latest.LeaderEpoch != leader {
			db.Close()
			t.Fatalf("recovery %d ledger=%+v found=%v err=%v", recovery, latest, found, err)
		}
		priorFence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
		context, err := db.CandidateRepairBuildContext(ctx, current.Ref, current.Version, priorFence)
		if err != nil || context.TargetGeneration != repair.TargetGeneration || context.PredecessorGeneration != repair.PredecessorGeneration || context.EntryTicketVersion != repair.EntryTicketVersion {
			db.Close()
			t.Fatalf("recovery %d repair context=%+v err=%v", recovery, context, err)
		}
		reusable, err = db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{
			Ref: current.Ref, Phase: domain.PhaseBuild, Role: "builder",
			ExpectedVersion: current.Version, Fence: priorFence,
		})
		if err != nil || !reusable.Recovered || reusable.Key != key || reusable.Parsed.Builder == nil {
			db.Close()
			t.Fatalf("recovery %d reusable Builder=%+v err=%v", recovery, reusable, err)
		}
	}
	defer db.Close()

	predecessor, err := db.RecoverableCandidate(ctx, current.Ref)
	if err != nil || predecessor.Snapshot.Generation != repair.PredecessorGeneration || predecessor.Snapshot.HeadSHA != repair.PredecessorHeadSHA {
		t.Fatalf("repair predecessor=%+v err=%v", predecessor, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := sha256Digest([]byte("candidate-repair-startup-policy"))
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate,
		current.Ref, current.Version, priorFence, key,
		repair.Verification.Revision.IntentDigest, repair.Verification.Revision.ProofDigest,
		repair.Verification.Checkpoint.CommitOID, "sha256:"+policyDigest, 0)
	successor := domain.CandidateSnapshot{
		BaseSHA: predecessor.Snapshot.BaseSHA, HeadSHA: strings.Repeat("6", 40), TreeSHA: strings.Repeat("7", 40),
		SourceDigest:             predecessor.Snapshot.SourceDigest,
		VerificationIntentDigest: repair.Verification.Revision.IntentDigest,
		ProofDigest:              repair.Verification.Revision.ProofDigest,
		CommandPolicyDigest:      policyDigest, BuilderEvidenceDigest: builderDigest,
	}
	commit := CommitObservation{CommitOID: successor.HeadSHA, ParentOID: repair.PredecessorHeadSHA, TreeOID: successor.TreeSHA}
	evidence := CandidateEvidence{
		Ref: current.Ref, ExpectedVersion: current.Version, Fence: priorFence,
		Snapshot: successor, BuilderResult: key, Commit: commit,
		Reason: "authenticated CI repair after startup", CommandResult: command,
	}
	if _, err := db.RecordCandidate(ctx, evidence); err != nil {
		t.Fatalf("record recovered repair candidate: %v", err)
	}
	if _, err := db.RecordCandidate(ctx, evidence); err != nil {
		t.Fatalf("replay recovered repair candidate: %v", err)
	}
	stored, err := db.RecoverableCandidate(ctx, current.Ref)
	if err != nil || stored.Snapshot.Generation != repair.TargetGeneration || stored.BuilderResult != key || stored.Commit != commit {
		t.Fatalf("stored recovered repair candidate=%+v err=%v", stored, err)
	}
	var candidates, results, sourceResults, commands, events int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=? AND provider_attempt_id=? AND provider_attempt=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration, current.Version, priorFence.LeaderEpoch, priorFence.RunnerEpoch, key.AttemptID, key.Attempt).Scan(&results); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? AND binding_ticket_version=? AND leader_epoch=? AND runner_epoch=? AND provider_attempt_id=? AND provider_attempt=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration, building.Version, builderFence.LeaderEpoch, builderFence.RunnerEpoch, key.AttemptID, key.Attempt).Scan(&sourceResults); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_command_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration).Scan(&commands); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='candidate_recorded'`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, current.Version).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if candidates != 1 || results != 1 || sourceResults != 1 || commands != 1 || completions != 1 || events != 1 {
		t.Fatalf("recovered repair append candidates=%d live_results=%d source_results=%d commands=%d completions=%d events=%d", candidates, results, sourceResults, commands, completions, events)
	}
}

func TestCandidateRepairStartupRetainedBindingDoesNotAuthorizeOrBlockLaterBuildEntry(t *testing.T) {
	completed := newCompletedCandidateRepairTestFixture(t)
	reviewing := completedCandidateRepairReviewingFixture(t, completed)
	completeFinalReviewWith(t, reviewing, phaseartifact.ReviewRepair, "builder")
	if _, err := reviewing.db.TransitionReviewRepair(reviewing.ctx, Transition{
		Ref: reviewing.ticket.Ref, ExpectedVersion: reviewing.ticket.Version, From: domain.StateReviewing,
		To: domain.StateBuilding, Trigger: "review_repair", Fence: reviewing.fence, EventPayload: "{}",
	}); err != nil {
		reviewing.db.Close()
		t.Fatal(err)
	}
	later, err := reviewing.db.Ticket(reviewing.ctx, reviewing.ticket.Ref)
	if err != nil || later.State != domain.StateBuilding || later.Version != reviewing.ticket.Version+1 {
		reviewing.db.Close()
		t.Fatalf("later review-repair Build=%+v err=%v", later, err)
	}
	laterFence := domain.Fence{LeaderEpoch: reviewing.fence.LeaderEpoch, RunnerEpoch: later.RunnerEpoch}
	if context, err := reviewing.db.CandidateRepairBuildContext(reviewing.ctx, later.Ref, later.Version, laterFence); !errors.Is(err, ErrNotFound) {
		reviewing.db.Close()
		t.Fatalf("retained CI repair authorized later Build context=%+v err=%v", context, err)
	}
	var bindings, completions int
	if err := reviewing.db.db.QueryRowContext(reviewing.ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, later.Ref.Channel, later.Ref.Project, later.Ref.Ticket).Scan(&bindings); err != nil {
		reviewing.db.Close()
		t.Fatal(err)
	}
	if err := reviewing.db.db.QueryRowContext(reviewing.ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=?`, later.Ref.Channel, later.Ref.Project, later.Ref.Ticket).Scan(&completions); err != nil {
		reviewing.db.Close()
		t.Fatal(err)
	}
	entry, err := loadProviderPhaseEntryAt(reviewing.ctx, reviewing.db.db, later.Ref, domain.PhaseBuild, later.Version)
	if err != nil || entry.Version != later.Version || entry.From != domain.StateReviewing || entry.State != domain.StateBuilding || entry.Trigger != "review_repair" {
		reviewing.db.Close()
		t.Fatalf("later Build entry=%+v err=%v", entry, err)
	}
	if bindings != 1 || completions != 1 {
		reviewing.db.Close()
		t.Fatalf("retained completed repair bindings=%d completions=%d", bindings, completions)
	}

	db := reopenCandidateRepairStartupStore(t, reviewing.db)
	defer db.Close()
	leader, err := db.AcquireLeader(reviewing.ctx, later.Ref.Channel, "candidate-repair-unrelated-later-build")
	if err != nil {
		t.Fatal(err)
	}
	latest, latestFound, err := loadLatestRunnerRecovery(reviewing.ctx, db.db, later.Ref)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.db.Conn(reviewing.ctx)
	if err != nil {
		t.Fatal(err)
	}
	prior, applicable, predecessorErr := db.candidateRepairRecoveryPredecessor(reviewing.ctx, conn, later.Ref, later.State, later.Version, later.RunnerEpoch, leader, latest, latestFound)
	closeErr := conn.Close()
	if predecessorErr != nil || closeErr != nil || applicable || prior != 0 {
		t.Fatalf("retained CI repair applied to later Build prior=%d applicable=%v predecessor=%v close=%v", prior, applicable, predecessorErr, closeErr)
	}
	changed, err := db.FenceRecoveredRunners(reviewing.ctx, later.Ref.Channel, leader)
	if err != nil || changed != 1 {
		t.Fatalf("later Build recovery changed=%d err=%v", changed, err)
	}
	current, err := db.Ticket(reviewing.ctx, later.Ref)
	if err != nil || current.State != domain.StateBuilding || current.Version != later.Version+1 || current.RunnerEpoch != later.RunnerEpoch+1 {
		t.Fatalf("recovered later Build=%+v err=%v", current, err)
	}
	recovery, found, err := loadLatestRunnerRecovery(reviewing.ctx, db.db, later.Ref)
	if err != nil || !found || recovery.PriorTicketVersion != later.Version || recovery.PriorRunnerEpoch != later.RunnerEpoch || recovery.PriorLeaderEpoch != laterFence.LeaderEpoch || recovery.TicketVersion != current.Version || recovery.RunnerEpoch != current.RunnerEpoch || recovery.LeaderEpoch != leader {
		t.Fatalf("later Build recovery row=%+v found=%v err=%v", recovery, found, err)
	}
	currentFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	if context, err := db.CandidateRepairBuildContext(reviewing.ctx, current.Ref, current.Version, currentFence); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retained CI repair authorized recovered later Build context=%+v err=%v", context, err)
	}
	if reused, err := db.LatestReusableProviderAttempt(reviewing.ctx, LatestReusableProviderAttemptRequest{
		Ref: current.Ref, Phase: domain.PhaseBuild, Role: "builder",
		ExpectedVersion: current.Version, Fence: currentFence,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retained CI repair reused old Builder in later entry result=%+v err=%v", reused, err)
	}
	builderQualification, _ := setupProviderPair(t, db, reviewing.ctx)
	worktree, err := db.Worktree(reviewing.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	request := supervised(t, ProviderAttemptRequest{
		Ref: current.Ref, ExpectedVersion: current.Version, Fence: currentFence,
		Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builderQualification),
		ConfigDigest: current.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
	})
	request.WorktreeIdentity = string(worktree.IdentityJSON)
	request.Input.WorktreeIdentity = string(worktree.IdentityJSON)
	request.BaseSHA = worktree.BaseSHA
	request.Input.BaseSHA = worktree.BaseSHA
	claim, err := db.BeginProviderAttempt(reviewing.ctx, request)
	if err != nil {
		t.Fatalf("fresh later Builder admission: %v", err)
	}
	var entryVersion uint64
	if err := db.db.QueryRowContext(reviewing.ctx, `SELECT entry_ticket_version FROM provider_phase_attempt_entries WHERE provider_attempt_id=?`, claim.ID).Scan(&entryVersion); err != nil || entryVersion != later.Version {
		t.Fatalf("fresh later Builder entry version=%d want=%d err=%v", entryVersion, later.Version, err)
	}
}

func TestCandidateRepairStartupTamperRollsBackFence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, Ticket)
	}{
		{
			name: "binding digest",
			mutate: func(t *testing.T, db *Store, building Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `DROP TRIGGER candidate_repair_bindings_immutable_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(t.Context(), `UPDATE candidate_repair_bindings SET repair_context_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, "sha256:"+strings.Repeat("0", 64), building.Ref.Channel, building.Ref.Project, building.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "phase entry digest",
			mutate: func(t *testing.T, db *Store, building Ticket) {
				if _, err := db.db.ExecContext(t.Context(), `DROP TRIGGER provider_phase_entries_immutable_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(t.Context(), `UPDATE provider_phase_entries SET entry_digest=? WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND entry_ticket_version=?`, strings.Repeat("0", 64), building.Ref.Channel, building.Ref.Project, building.Ref.Ticket, building.Version); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, building, _, _ := pendingCandidateRepairStartupFixture(t)
			defer db.Close()
			ctx := t.Context()
			var beforeRows int
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket).Scan(&beforeRows); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db, building)
			leader, err := db.AcquireLeader(ctx, building.Ref.Channel, "candidate-repair-tamper")
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := db.FenceRecoveredRunners(ctx, building.Ref.Channel, leader); !errors.Is(err, ErrPublicationEvidence) || changed != 0 {
				t.Fatalf("tampered fence changed=%d err=%v", changed, err)
			}
			current, err := db.Ticket(ctx, building.Ref)
			if err != nil || current.State != building.State || current.Version != building.Version || current.RunnerEpoch != building.RunnerEpoch {
				t.Fatalf("tampered fence changed ticket=%+v err=%v", current, err)
			}
			var afterRows int
			if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=?`, building.Ref.Channel, building.Ref.Project, building.Ref.Ticket).Scan(&afterRows); err != nil {
				t.Fatal(err)
			}
			if afterRows != beforeRows {
				t.Fatalf("tampered fence appended ledger before=%d after=%d", beforeRows, afterRows)
			}
		})
	}
}
