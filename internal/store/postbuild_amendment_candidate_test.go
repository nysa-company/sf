package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func assertPostbuildAmendmentCandidateHandoff(t *testing.T, db *Store, ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, refresh bool) {
	t.Helper()
	ticket, err := db.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := db.CurrentVerification(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	for _, live := range []bool{false, true} {
		if superseded, err := db.postbuildAmendmentSupersededAt(ctx, db.db, ref, version, fence, live); err != nil || superseded {
			t.Fatalf("current amendment superseded (live=%t): %t %v", live, superseded, err)
		}
	}
	worktree, err := db.Worktree(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	builder, _ := setupProviderPair(t, db, ctx)
	artifact := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "fresh accepted-amendment candidate", ChangedFiles: []string{"internal/implementation.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.BeginProviderAttempt(ctx, supervisedOperatorSource(t, db, ctx, ProviderAttemptRequest{Ref: ref, ExpectedVersion: version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "amendment-candidate", ProcessStartIdentity: fmt.Sprintf("candidate-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type, ExpectedVerificationCommand: artifact.Commands[0], ProtectedVerification: verification.Revision.OwnedFiles}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild, AttemptID: claim.ID, Attempt: claim.Attempt}
	digest, err := phaseartifact.BuilderEvidenceDigest(artifact)
	if err != nil {
		t.Fatal(err)
	}
	policy := sha256Digest([]byte("postbuild-amendment-candidate-policy"))
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ref, version, fence, key, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Checkpoint.CommitOID, "sha256:"+policy, 0)
	head, tree := strings.Repeat("1", 40), strings.Repeat("2", 40)
	plan, err := db.Plan(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	_, parsedVerification, err := db.LoadHistoricalProviderAttemptResult(ctx, verification.ProviderResult)
	if err != nil {
		t.Fatal(err)
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(*parsedVerification.Verify, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	commandResult, err := db.LoadRepositoryCommandResult(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	commitDigest := CanonicalRepositoryCommitDigest("candidate", ref, version, fence, worktree, key, command, commandResult.ResultDigest, struct {
		Plan         workflowprompt.PlanIdentity
		Verification workflowprompt.VerificationIdentity
		Builder      phaseartifact.Builder
	}{planIdentity, verificationIdentity, artifact})
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: ref, TicketVersion: version, Fence: fence}, RequestDigest: commitDigest, Repository: commandResult.Claim.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: commandResult.Claim.BaseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: verification.Checkpoint.CommitOID}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, found, err := db.PostbuildAmendmentPreparedCandidateWitness(ctx, ref, version, fence, key); err != nil || found {
		t.Fatalf("absent prepared=%t err=%v", found, err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "git/commit", TicketVersion: version, Fence: fence, RequestDigest: commitDigest}); err != nil {
		t.Fatal(err)
	}
	gitClaim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.PostbuildAmendmentPreparedCandidateWitness(ctx, ref, version, fence, key); err == nil {
		t.Fatal("partial candidate intent accepted as absence")
	}
	lease, err := db.AcquireGitMutation(ctx, gitClaim)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.(interface {
		RecordPreparedCommit(context.Context, string, string) error
	}).RecordPreparedCommit(ctx, head, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	witness, found, err := db.PostbuildAmendmentPreparedCandidateWitness(ctx, ref, version, fence, key)
	if err != nil || !found || witness.Command.Key != command || witness.Commit.CommitOID != head || witness.EffectState != EffectExecuting {
		t.Fatalf("prepared candidate=%+v found=%t err=%v", witness, found, err)
	}
	if _, err := db.ConfirmPreparedCommit(ctx, gitClaim, contracts.PreparedCommitObservation{CommitOID: head, ParentOID: verification.Checkpoint.CommitOID, TreeOID: tree}); err != nil {
		t.Fatal(err)
	}
	preparedLeader, err := db.AcquireLeader(ctx, ref.Channel, "prepared-amendment-candidate-restart")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReconcileEffects(ctx, ref.Channel, preparedLeader); err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, ref.Channel, preparedLeader); err != nil || changed != 1 {
		t.Fatalf("prepared restart=%d err=%v", changed, err)
	}
	version++
	fence = domain.Fence{LeaderEpoch: preparedLeader, RunnerEpoch: fence.RunnerEpoch + 1}
	restartedWitness, found, err := db.PostbuildAmendmentPreparedCandidateWitness(ctx, ref, version, fence, key)
	if err != nil || !found || restartedWitness.Commit != witness.Commit || restartedWitness.Claim != witness.Claim || restartedWitness.Command.Key != command || restartedWitness.EffectState != EffectConfirmed {
		t.Fatalf("restarted prepared=%+v found=%t err=%v", restartedWitness, found, err)
	}
	snapshot := domain.CandidateSnapshot{BaseSHA: worktree.BaseSHA, HeadSHA: head, TreeSHA: tree, SourceDigest: ticket.SourceDigest, VerificationIntentDigest: verification.Revision.IntentDigest, ProofDigest: verification.Revision.ProofDigest, CommandPolicyDigest: policy, BuilderEvidenceDigest: digest}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ref, ExpectedVersion: version, Fence: fence, Snapshot: snapshot, BuilderResult: key, Commit: CommitObservation{CommitOID: head, ParentOID: verification.Checkpoint.CommitOID, TreeOID: tree}, Reason: "candidate persisted before build_pass", CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	value, err := db.PostbuildVerificationAmendmentContext(ctx, ref, version, fence)
	if err != nil || value.Candidate == nil || value.Candidate.BuilderResult != key || value.Candidate.Commit.CommitOID != head {
		t.Fatalf("persisted candidate handoff=%+v err=%v", value.Candidate, err)
	}
	if _, err := db.ValidateCurrentCandidateForBuildTransition(ctx, ref, version, fence); err != nil {
		t.Fatalf("candidate replay=%v", err)
	}
	leader, err := db.AcquireLeader(ctx, ref.Channel, "persisted-amendment-candidate-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("candidate restart=%d err=%v", changed, err)
	}
	version++
	fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch + 1}
	// The immutable candidate still names its pre-restart binding. Authenticate
	// that historical material independently from the signed suffix; treating
	// it as a current endpoint would reject the newly appended recovery row.
	if err := db.readProtectedBaseRefreshSnapshot(ctx, func(conn *sql.Conn) error {
		stored, err := db.latestCandidateFrom(ctx, conn, ref, false)
		if err != nil {
			return fmt.Errorf("historical candidate: %w", err)
		}
		if stored.TicketVersion != value.Candidate.TicketVersion || stored.Fence != value.Candidate.Fence || stored.TicketVersion >= version {
			return fmt.Errorf("candidate binding unexpectedly changed across recovery")
		}
		if err := db.reauthenticateStoredCandidateCheckpointFrom(ctx, conn, ref, stored); err != nil {
			return fmt.Errorf("candidate material: %w", err)
		}
		if err := postbuildRepairSignedSourcePrefix(ctx, conn, ref, stored.TicketVersion, stored.Fence, version, fence); err != nil {
			return fmt.Errorf("candidate recovery suffix: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	recovered, err := db.PostbuildVerificationAmendmentContext(ctx, ref, version, fence)
	if err != nil || recovered.Candidate == nil || recovered.Candidate.BuilderResult != key || recovered.Candidate.Commit != value.Candidate.Commit || recovered.Candidate.CommandBinding != value.Candidate.CommandBinding {
		t.Fatalf("recovered candidate handoff=%+v err=%v", recovered.Candidate, err)
	}
	if refresh {
		// Rebind the immutable candidate at the recovered endpoint before its
		// normal publication transition, exactly as Worker replay does.
		if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ref, ExpectedVersion: version, Fence: fence, Snapshot: recovered.Candidate.Snapshot, BuilderResult: key, Commit: recovered.Candidate.Commit, Reason: "recovered candidate before publication", CommandResult: command}); err != nil {
			t.Fatal(err)
		}
		assertPostbuildAmendmentPublishedRefreshRestart(t, db, ctx, ref, version, fence, recovered.Candidate.Snapshot)
		return
	}
	// Malformed immutable candidate evidence must not become either dirty
	// amendment admission or absence. Corrupt only this terminal fixture.
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER candidate_snapshots_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE candidate_snapshots SET proof_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, strings.Repeat("9", 64), ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PostbuildVerificationAmendmentContext(ctx, ref, version, fence); err == nil {
		t.Fatal("malformed candidate handoff accepted")
	}
}
