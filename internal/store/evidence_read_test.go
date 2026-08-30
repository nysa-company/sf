package store

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestEvidenceReadsRebuildCurrentWorkflowAuthority(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	planDocument := PlanDocument{Acceptance: []string{"works"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"migration"}}
	planDigest, err := database.RecordPlan(ctx, PlanArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Document: planDocument})
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.RecordVerification(ctx, VerificationArtifact{
		Ref: ref, ExpectedVersion: version, Fence: fence, Intent: []byte("intent"), Proof: []byte("proof"),
		OwnedFiles: []string{"verification_test.go"}, CheckpointID: evidenceOID("a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt := PhaseAttempt{
		Ref: ref, Phase: domain.PhasePlanning, Attempt: 1, ExpectedVersion: version, Fence: fence,
		Provider:   domain.ProviderIdentity{Provider: "cursor", Model: "model", Family: "family", Version: "1"},
		WorktreeID: "worktree-identity", BaseSHA: evidenceOID("b"),
	}
	if err := database.StartPhaseAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	attempt.Outcome, attempt.UsageJSON = "passed", []byte(`{"tokens":12}`)
	if err := database.CompletePhaseAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	worktree := WorktreeRegistration{
		Ref: ref, ExpectedVersion: version, Fence: fence, Path: "/tmp/sf-worktree", Branch: "sf/dev/project/ticket-random",
		IdentityJSON: []byte(`{"device":1,"inode":2}`), BaseSHA: evidenceOID("b"), HeadSHA: evidenceOID("c"),
	}
	if err := database.RegisterWorktree(ctx, worktree); err != nil {
		t.Fatal(err)
	}

	storedPlan, err := database.Plan(ctx, ref)
	if err != nil || storedPlan.Digest != planDigest || !reflect.DeepEqual(storedPlan.Document, planDocument) || storedPlan.TicketVersion != version || storedPlan.Fence != fence || storedPlan.CreatedAt.IsZero() {
		t.Fatalf("plan=%+v err=%v", storedPlan, err)
	}
	storedVerification, err := database.CurrentVerification(ctx, ref)
	if err != nil || storedVerification.Revision.Revision != 1 || !bytes.Equal(storedVerification.Intent, []byte("intent")) || !bytes.Equal(storedVerification.Proof, []byte("proof")) || storedVerification.Fence != fence {
		t.Fatalf("verification=%+v err=%v", storedVerification, err)
	}
	storedWorktree, err := database.Worktree(ctx, ref)
	if err != nil || storedWorktree.Path != worktree.Path || storedWorktree.Branch != worktree.Branch || !bytes.Equal(storedWorktree.IdentityJSON, worktree.IdentityJSON) || storedWorktree.Fence != fence {
		t.Fatalf("worktree=%+v err=%v", storedWorktree, err)
	}
	attempts, err := database.PhaseAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "completed" || attempts[0].Outcome != "passed" || attempts[0].Provider != attempt.Provider || attempts[0].StartedAt.IsZero() || attempts[0].FinishedAt.IsZero() {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}

	storedVerification.Intent[0] = 'X'
	again, err := database.CurrentVerification(ctx, ref)
	if err != nil || !bytes.Equal(again.Intent, []byte("intent")) {
		t.Fatalf("verification read aliased database bytes: %+v err=%v", again, err)
	}
}

func TestEvidenceReadsFailClosedOnAbsenceAndTampering(t *testing.T) {
	database, ctx, ref, version, fence := evidenceFixture(t)
	if _, err := database.Plan(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing plan error=%v", err)
	}
	if _, err := database.CurrentVerification(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing verification error=%v", err)
	}
	if _, err := database.LatestCandidate(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing candidate error=%v", err)
	}
	if _, err := database.Worktree(ctx, ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing worktree error=%v", err)
	}
	document := PlanDocument{Acceptance: []string{"works"}, ProofKind: "regression", Paths: []string{"internal"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"risk"}}
	if _, err := database.RecordPlan(ctx, PlanArtifact{Ref: ref, ExpectedVersion: version, Fence: fence, Document: document}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(ctx, `UPDATE plans SET artifact_bytes='{"acceptance":["changed"]}' WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Plan(ctx, ref); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("tampered plan error=%v", err)
	}
}
