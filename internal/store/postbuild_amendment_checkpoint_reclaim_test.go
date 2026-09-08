package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func postbuildCheckpointReclaimFixture(t *testing.T) (*Store, context.Context, GitMutationIntent) {
	t.Helper()
	db, ctx, request, builder, snapshot := postbuildAmendmentFixture(t)
	entry, err := db.TransitionPostbuildVerificationAmendmentRequest(ctx, request, builder, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	value, err := db.PostbuildVerificationAmendmentContext(ctx, request.Ref, entry.Version, request.Fence)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, request.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, binding := setupProviderPair(t, db, ctx)
	claim, err := db.BeginProviderAttempt(ctx, supervisedOperatorSource(t, db, ctx, ProviderAttemptRequest{Ref: request.Ref, ExpectedVersion: entry.Version, Fence: request.Fence, Phase: domain.PhaseVerification, Role: "reviewer", Binding: runtime(binding), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "checkpoint-reclaim", ProcessStartIdentity: fmt.Sprintf("review-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	_, original, err := db.LoadHistoricalProviderAttemptResult(ctx, value.Verification.ProviderResult)
	if err != nil || original.Verify == nil {
		t.Fatal(err)
	}
	artifact := postbuildAmendmentProposal(*original.Verify)
	raw, _ := json.Marshal(artifact)
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), entry.Version, request.Fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: artifact.AcceptanceDigest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{Ref: request.Ref, Phase: domain.PhaseVerification, AttemptID: claim.ID, Attempt: claim.Attempt}
	intentBytes, _ := workflowprompt.CanonicalVerificationIntentBytes(artifact)
	proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(artifact)
	commandKey := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, request.Ref, entry.Version, request.Fence, key, sha256Digest(intentBytes), sha256Digest(proofBytes), "", "", 1)
	if _, err := db.RecordPostbuildAmendmentCheckpointSnapshot(ctx, request.Ref, entry.Version, request.Fence, key, commandKey, sha256Digest([]byte("accepted-checkpoint"))); err != nil {
		t.Fatal(err)
	}
	command, err := db.LoadRepositoryCommandResult(ctx, commandKey)
	if err != nil {
		t.Fatal(err)
	}
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: request.Ref, TicketVersion: entry.Version, Fence: request.Fence}, RequestDigest: CanonicalVerificationAmendmentCheckpointDigest(value.Amendment, value.Worktree, key, commandKey, command.ResultDigest, artifact), Repository: command.Claim.Repository, Worktree: value.Worktree.Path, Branch: value.Worktree.Branch, Operation: "commit", BaseRef: command.Claim.BaseRef, ExpectedBaseOID: value.Worktree.BaseSHA, ExpectedHeadOID: value.Repair.OriginalCheckpointOID}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	return db, ctx, intent
}

func TestPostbuildCheckpointReclaimPlannedAndUncertainBounded(t *testing.T) {
	db, ctx, intent := postbuildCheckpointReclaimFixture(t)
	claim, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
	if err != nil || claim.SemanticKey != intent.SemanticKey || claim.RequestDigest != intent.RequestDigest {
		t.Fatalf("planned reclaim=%+v err=%v", claim, err)
	}
	again, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
	if err != nil || again != claim {
		t.Fatalf("executing replay=%+v err=%v", again, err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence); !errors.Is(err, ErrControlNotDrained) {
		t.Fatalf("live writer reclaimed=%v", err)
	}
	prepared := lease.(interface {
		RecordPreparedCommit(context.Context, string, string) error
	})
	commit, tree := strings.Repeat("e", 40), strings.Repeat("f", 40)
	if err := prepared.RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < maxPostbuildAmendmentCheckpointReclaims; i++ {
		if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
			t.Fatal(err)
		}
		prior := claim
		claim, err = db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence)
		if err != nil || claim.ClaimEpoch <= prior.ClaimEpoch || claim.SemanticKey != prior.SemanticKey {
			t.Fatalf("uncertain reclaim=%+v err=%v", claim, err)
		}
		facts, err := db.GitMutationIntentFacts(ctx, claim.SemanticKey)
		if err != nil || facts.PreparedCommitOID != commit || facts.PreparedTreeOID != tree {
			t.Fatalf("prepared changed=%+v err=%v", facts, err)
		}
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("unbounded reclaims=%v", err)
	}
}

func TestPostbuildCheckpointReclaimDoesNotReopenConfirmed(t *testing.T) {
	db, ctx, intent := postbuildCheckpointReclaimFixture(t)
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("e", 40), strings.Repeat("f", 40)
	if err := lease.(interface {
		RecordPreparedCommit(context.Context, string, string) error
	}).RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, intent.Fence); !errors.Is(err, ErrGitMutationIntent) {
		t.Fatalf("confirmed reopened=%v", err)
	}
}

func TestPostbuildCheckpointReclaimRequiresSignedRestart(t *testing.T) {
	db, ctx, intent := postbuildCheckpointReclaimFixture(t)
	claim, err := db.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, intent.Ref.Channel, "checkpoint-reclaim-restart")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion, domain.Fence{LeaderEpoch: leader, RunnerEpoch: intent.Fence.RunnerEpoch}); err == nil {
		t.Fatal("leader-only reclaim accepted")
	}
	if changed, err := db.FenceRecoveredRunners(ctx, intent.Ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("recovery=%d err=%v", changed, err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: intent.Fence.RunnerEpoch + 1}
	reclaimed, err := db.ReclaimPostbuildAmendmentCheckpoint(ctx, intent.Ref, intent.TicketVersion+1, fence)
	if err != nil || reclaimed.SemanticKey != claim.SemanticKey || reclaimed.TicketVersion != intent.TicketVersion+1 || reclaimed.ClaimEpoch <= claim.ClaimEpoch {
		t.Fatalf("restart reclaim=%+v err=%v", reclaimed, err)
	}
	if _, err := db.AcquireGitMutation(ctx, claim); err == nil {
		t.Fatal("old claim still launches")
	}
	lease, err := db.AcquireGitMutation(ctx, reclaimed)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}
