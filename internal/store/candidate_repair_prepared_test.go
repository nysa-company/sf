package store

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func prepareCandidateRepairSuccessorBeforeRecord(t *testing.T, db *Store, building Ticket, fence domain.Fence, repair CandidateRepairBuildContext, builderKey ProviderAttemptResultKey) (contracts.RepositoryCommandResultKey, CommitObservation) {
	t.Helper()
	ctx := t.Context()
	worktree, err := db.Worktree(ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	project, err := db.Project(ctx, building.Ref.Channel, building.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operatorSourcePlanFrom(ctx, conn, building.Ref)
	if closeErr := conn.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil || plan.Document.Planner == nil {
		t.Fatalf("repair plan=%+v err=%v", plan, err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	reviewer, parsedReviewer, err := db.LoadHistoricalProviderAttemptResult(ctx, repair.Verification.ProviderResult)
	if err != nil || reviewer.Claim.Role != "reviewer" || parsedReviewer.Verify == nil {
		t.Fatalf("repair reviewer=%+v parsed=%+v err=%v", reviewer.Claim, parsedReviewer, err)
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(*parsedReviewer.Verify, repair.Verification.Revision.IntentDigest, repair.Verification.Revision.ProofDigest, repair.Verification.Revision.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	_, parsedBuilder, err := db.LoadHistoricalProviderAttemptResult(ctx, builderKey)
	if err != nil || parsedBuilder.Builder == nil {
		t.Fatalf("repair Builder=%+v err=%v", parsedBuilder, err)
	}
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate,
		building.Ref, building.Version, fence, builderKey,
		repair.Verification.Revision.IntentDigest, repair.Verification.Revision.ProofDigest,
		repair.Verification.Revision.CheckpointID, "", 0)
	commandResult, err := db.LoadRepositoryCommandResult(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	digest := CanonicalRepositoryCommitDigest("candidate", building.Ref, building.Version, fence, worktree, builderKey, command, commandResult.ResultDigest, struct {
		Plan         workflowprompt.PlanIdentity
		Verification workflowprompt.VerificationIdentity
		Builder      phaseartifact.Builder
	}{planIdentity, verificationIdentity, *parsedBuilder.Builder})
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: building.Ref, TicketVersion: building.Version, Fence: fence}, RequestDigest: digest,
		Repository: project.Path, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit",
		BaseRef: project.BaseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: repair.PredecessorHeadSHA}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit := CommitObservation{CommitOID: strings.Repeat("8", 40), ParentOID: repair.PredecessorHeadSHA, TreeOID: strings.Repeat("9", 40)}
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit.CommitOID, commit.TreeOID); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit.CommitOID, ParentOID: commit.ParentOID, TreeOID: commit.TreeOID}); err != nil {
		t.Fatal(err)
	}
	return command, commit
}

func TestCandidateRepairPreparedSuccessorSurvivesRestartWithoutSecondCommandOrCommit(t *testing.T) {
	db, building, fence, repair := pendingCandidateRepairStartupFixture(t)
	builderKey, builder := completeCandidateRepairBuilderBeforeCandidate(t, db, building, fence)
	command, commit := prepareCandidateRepairSuccessorBeforeRecord(t, db, building, fence, repair, builderKey)
	ctx := t.Context()

	current := building
	for recovery := 1; recovery <= 2; recovery++ {
		db = reopenCandidateRepairStartupStore(t, db)
		leader, err := db.AcquireLeader(ctx, building.Ref.Channel, fmt.Sprintf("candidate-repair-prepared-%d", recovery))
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		changed, err := db.FenceRecoveredRunners(ctx, building.Ref.Channel, leader)
		if err != nil || changed != 1 {
			db.Close()
			t.Fatalf("recovery %d changed=%d err=%v", recovery, changed, err)
		}
		current, err = db.Ticket(ctx, building.Ref)
		if err != nil || current.State != domain.StateBuilding {
			db.Close()
			t.Fatalf("recovery %d ticket=%+v err=%v", recovery, current, err)
		}
		fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	}
	defer db.Close()

	witness, found, err := db.CandidateRepairRecoverablePreparedCandidateWitness(ctx, current.Ref, current.Version, fence, builderKey)
	if err != nil || !found || witness.Command.Key != command || witness.Commit != commit || witness.Builder != builderKey || witness.Repair.TargetGeneration != repair.TargetGeneration || witness.EffectState != EffectConfirmed {
		t.Fatalf("prepared repair witness=%+v found=%v err=%v", witness, found, err)
	}
	replay, replayFound, err := db.CandidateRepairRecoverablePreparedCandidateWitness(ctx, current.Ref, current.Version, fence, builderKey)
	if err != nil || !replayFound || replay.Command.Key != witness.Command.Key || replay.Commit != witness.Commit || replay.Claim != witness.Claim {
		t.Fatalf("prepared repair witness replay=%+v found=%v err=%v", replay, replayFound, err)
	}
	var commandCount, gitCount int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_results WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.EntryTicketVersion).Scan(&commandCount); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_intents WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit' AND ticket_version>=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.EntryTicketVersion).Scan(&gitCount); err != nil {
		t.Fatal(err)
	}
	if commandCount != 1 || gitCount != 1 {
		t.Fatalf("recovery duplicated mutations commands=%d git_intents=%d", commandCount, gitCount)
	}

	builderDigest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil {
		t.Fatal(err)
	}
	evidence := CandidateEvidence{Ref: current.Ref, ExpectedVersion: current.Version, Fence: fence, BuilderResult: builderKey, Commit: commit, CommandResult: command, Reason: "recovered prepared CI repair candidate",
		Snapshot: domain.CandidateSnapshot{BaseSHA: witness.Worktree.BaseSHA, HeadSHA: commit.CommitOID, TreeSHA: commit.TreeOID, SourceDigest: current.SourceDigest,
			VerificationIntentDigest: repair.Verification.Revision.IntentDigest, ProofDigest: repair.Verification.Revision.ProofDigest,
			CommandPolicyDigest: strings.TrimPrefix(witness.Command.Claim.PolicyDigest, "sha256:"), BuilderEvidenceDigest: builderDigest}}
	if _, err := db.RecordCandidate(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RecordCandidate(ctx, evidence); err != nil {
		t.Fatalf("candidate replay: %v", err)
	}
	var candidates, completions int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.TargetGeneration).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if candidates != 1 || completions != 1 {
		t.Fatalf("candidate/completion replay candidates=%d completions=%d", candidates, completions)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_results WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.EntryTicketVersion).Scan(&commandCount); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_intents WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit' AND ticket_version>=?`, current.Ref.Channel, current.Ref.Project, current.Ref.Ticket, repair.EntryTicketVersion).Scan(&gitCount); err != nil {
		t.Fatal(err)
	}
	if commandCount != 1 || gitCount != 1 {
		t.Fatalf("candidate append duplicated mutations commands=%d git_intents=%d", commandCount, gitCount)
	}
}
