package store

import (
	"bytes"
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

func TestPostbuildPendingAmendmentTwoRecoveriesAndDecision(t *testing.T) {
	for _, scenario := range []struct{ accepted, refresh bool }{{false, false}, {true, false}, {true, true}} {
		accepted := scenario.accepted
		t.Run(fmt.Sprintf("accepted_%t_refresh_%t", accepted, scenario.refresh), func(t *testing.T) {
			db, ctx, request, builderKey, snapshot := postbuildAmendmentFixture(t)
			entry, err := db.TransitionPostbuildVerificationAmendmentRequest(ctx, request, builderKey, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			original, err := db.PostbuildVerificationAmendmentContext(ctx, request.Ref, entry.Version, request.Fence)
			if err != nil {
				t.Fatal(err)
			}
			version, fence := entry.Version, request.Fence
			assertPending := func() {
				t.Helper()
				pending, err := db.PendingVerificationAmendment(ctx, request.Ref, version, fence)
				if err != nil || pending.BuilderResult != builderKey || pending.TransitionTicketVersion != entry.Version || pending.Fence != request.Fence {
					t.Fatalf("pending source changed: %+v err=%v", pending, err)
				}
				current, err := db.PostbuildVerificationAmendmentContext(ctx, request.Ref, version, fence)
				if err != nil || current.Snapshot != snapshot || current.Verification.ProviderResult != original.Verification.ProviderResult || !bytes.Equal(current.Verification.Proof, original.Verification.Proof) {
					t.Fatalf("retained proof: %v", err)
				}
				if current, err := db.CurrentVerification(ctx, request.Ref); err == nil && current.ProviderResult == original.Verification.ProviderResult {
					t.Fatal("prior Reviewer became fresh pending verification")
				}
				if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: request.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: version, Fence: fence}); !errors.Is(err, ErrNotFound) {
					t.Fatalf("prior Reviewer selection=%v", err)
				}
			}
			assertPending()
			for i := 1; i <= 2; i++ {
				leader, err := db.AcquireLeader(ctx, request.Ref.Channel, fmt.Sprintf("pending-amendment-%d", i))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.PostbuildVerificationAmendmentContext(ctx, request.Ref, version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch}); err == nil {
					t.Fatal("leader-only takeover accepted")
				}
				if changed, err := db.FenceRecoveredRunners(ctx, request.Ref.Channel, leader); err != nil || changed != 1 {
					t.Fatalf("pending recovery %d: changed=%d err=%v", i, changed, err)
				}
				version++
				fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: fence.RunnerEpoch + 1}
				assertPending()
			}
			ticket, err := db.Ticket(ctx, request.Ref)
			if err != nil {
				t.Fatal(err)
			}
			_, reviewer := setupProviderPair(t, db, ctx)
			_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, original.Verification.ProviderResult)
			if err != nil || parsed.Verify == nil {
				t.Fatalf("original verification: %v", err)
			}
			artifact := *parsed.Verify
			if accepted {
				artifact = postbuildAmendmentProposal(artifact)
			}
			claim, err := db.BeginProviderAttempt(ctx, supervisedOperatorSource(t, db, ctx, ProviderAttemptRequest{Ref: request.Ref, ExpectedVersion: version, Fence: fence, Phase: domain.PhaseVerification, Role: "reviewer", Binding: runtime(reviewer), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
			if err != nil {
				t.Fatal(err)
			}
			if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "amendment-review", ProcessStartIdentity: fmt.Sprintf("review-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: artifact.AcceptanceDigest}, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			key := ProviderAttemptResultKey{Ref: request.Ref, Phase: domain.PhaseVerification, AttemptID: claim.ID, Attempt: claim.Attempt}
			if reusable, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: request.Ref, Phase: domain.PhaseVerification, Role: "reviewer", ExpectedVersion: version, Fence: fence}); err != nil || reusable.Key != key {
				t.Fatalf("fresh Reviewer selection=%+v err=%v", reusable.Key, err)
			}
			want := VerificationAmendmentRejected
			trigger := "amendment_rejected"
			if accepted {
				want, trigger = VerificationAmendmentAccepted, "amendment_accepted"
				intent, _ := workflowprompt.CanonicalVerificationIntentBytes(artifact)
				proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(artifact)
				command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, request.Ref, version, fence, key, sha256Digest(intent), sha256Digest(proofBytes), "", "", 1)
				checkpoint := strings.Repeat("e", 40)
				projected := VerificationArtifact{Ref: request.Ref, ExpectedVersion: version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: artifact.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &key, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: original.Repair.OriginalCheckpointOID, TreeOID: strings.Repeat("f", 40)}, CommandResult: command, AmendsRevision: original.Amendment.Prior.Revision, Reason: original.Amendment.Reason, Requester: original.Amendment.Requester}
				if _, err := db.RecordVerification(ctx, projected); !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("projected metadata bypassed missing receipt = %v", err)
				}
				if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: request.Ref, ExpectedVersion: version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: artifact.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &key, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: original.Repair.OriginalCheckpointOID, TreeOID: strings.Repeat("f", 40)}, CommandResult: command}); !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("accepted checkpoint without snapshot = %v", err)
				}
				receipt, err := db.RecordPostbuildAmendmentCheckpointSnapshot(ctx, request.Ref, version, fence, key, command, repositoryResultDigest([]byte("accepted-reviewer-snapshot")))
				if err != nil {
					t.Fatal(err)
				}
				loaded, err := db.PostbuildAmendmentCheckpointSnapshot(ctx, request.Ref, version, fence)
				if err != nil || loaded != receipt || receipt.ImplementationDigest != snapshot.ImplementationDigest {
					t.Fatalf("checkpoint receipt = %+v, %v", loaded, err)
				}
				if _, err := db.RecordPostbuildAmendmentCheckpointSnapshot(ctx, request.Ref, version, fence, key, command, repositoryResultDigest([]byte("changed-reviewer-snapshot"))); !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("recapture changed bytes = %v", err)
				}
				if _, err := db.db.ExecContext(ctx, `UPDATE postbuild_amendment_checkpoint_snapshots SET full_snapshot_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, repositoryResultDigest([]byte("tamper")), request.Ref.Channel, request.Ref.Project, request.Ref.Ticket); err == nil {
					t.Fatal("mutable checkpoint receipt")
				}
				if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: request.Ref, ExpectedVersion: version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: artifact.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &key, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: original.Repair.OriginalCheckpointOID, TreeOID: strings.Repeat("f", 40)}, CommandResult: command}); err != nil {
					t.Fatal(err)
				}
				if _, err := db.RecordVerification(ctx, projected); err != nil {
					t.Fatalf("exact projected replay refused = %v", err)
				}
				projected.Checkpoint.ParentOID = strings.Repeat("d", 40)
				if _, err := db.RecordVerification(ctx, projected); !errors.Is(err, ErrEvidenceConflict) {
					t.Fatalf("projected replay accepted wrong parent = %v", err)
				}
			}
			decision := Transition{Ref: request.Ref, ExpectedVersion: version, Fence: fence, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: trigger, EventPayload: "{}"}
			var decided TransitionResult
			if accepted {
				decided, err = db.TransitionVerificationAmendmentAccepted(ctx, decision, key)
			} else {
				decided, err = db.TransitionVerificationAmendmentRejected(ctx, decision, key)
			}
			if err != nil {
				t.Fatal(err)
			}
			current, err := db.CurrentVerification(ctx, request.Ref)
			if err != nil || (accepted && current.ProviderResult != key) || (!accepted && current.ProviderResult != original.Verification.ProviderResult) {
				t.Fatalf("decision current proof=%+v err=%v", current, err)
			}
			if _, err := db.PostbuildRepairContext(ctx, request.Ref, decided.Version, fence); !errors.Is(err, ErrNotFound) {
				t.Fatalf("repair prompt not superseded: %v", err)
			}
			if accepted {
				if _, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: request.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: decided.Version, Fence: fence}); !errors.Is(err, ErrNotFound) {
					t.Fatalf("requesting Builder reused after accepted fresh entry: %v", err)
				}
			}
			bound, err := db.PostbuildVerificationAmendmentContext(ctx, request.Ref, decided.Version, fence)
			if err != nil || bound.Decision != want || bound.Reviewer != key || bound.Snapshot != snapshot {
				t.Fatalf("decision context=%+v err=%v", bound, err)
			}
			if accepted {
				assertPostbuildAmendmentCandidateHandoff(t, db, ctx, request.Ref, decided.Version, fence, scenario.refresh)
			}
		})
	}
}
