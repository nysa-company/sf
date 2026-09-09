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

// Source-only Store fixture: normal completed provider/evidence APIs, with
// shape-valid snapshot locators. No physical checkout admission is asserted.
func postbuildAmendmentFixture(t *testing.T) (*Store, context.Context, Transition, ProviderAttemptResultKey, PostbuildAmendmentSnapshot) {
	t.Helper()
	db, ctx, failure := postbuildFailureFixture(t, 1)
	entry, err := db.TransitionPostbuildRepair(ctx, PostbuildRepairRequest{PostbuildFailureRequest: failure, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := db.Ticket(ctx, failure.Ref)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := db.CurrentVerification(ctx, failure.Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, verifier, err := db.LoadHistoricalProviderAttemptResult(ctx, verification.ProviderResult)
	if err != nil || verifier.Verify == nil {
		t.Fatalf("verification provider: %v", err)
	}
	builder, _ := setupProviderPair(t, db, ctx)
	claim, err := db.BeginProviderAttempt(ctx, supervisedOperatorSource(t, db, ctx, ProviderAttemptRequest{Ref: failure.Ref, ExpectedVersion: entry.Version, Fence: failure.Fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builder), ConfigDigest: ticket.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "postbuild-amendment", ProcessStartIdentity: fmt.Sprintf("amendment-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	proposal := postbuildAmendmentProposal(*verifier.Verify)
	proposalProof, err := workflowprompt.CanonicalVerificationProofBytes(proposal)
	if err != nil {
		t.Fatal(err)
	}
	artifact := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "request independent proof amendment", ChangedFiles: append([]string(nil), verification.Revision.OwnedFiles...), Commands: [][]string{verifier.Verify.Command}, AmendmentRequest: &phaseartifact.AmendmentRequest{OldProofDigest: verification.Revision.ProofDigest, ProposedDigest: sha256Digest(proposalProof), ProposedCommand: verifier.Verify.Command, Reason: "independent review requested for a contradictory assertion"}}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, failure.Fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: ticket.Type, ProtectedVerification: verification.Revision.OwnedFiles}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	protected, err := PostbuildAmendmentProtectedPathsDigest(verification.Revision.OwnedFiles)
	if err != nil {
		t.Fatal(err)
	}
	transition := Transition{Ref: failure.Ref, ExpectedVersion: entry.Version, Fence: failure.Fence, From: domain.StateBuilding, To: domain.StateVerifying, Trigger: "verification_amendment_requested", EventPayload: "{}"}
	return db, ctx, transition, ProviderAttemptResultKey{Ref: failure.Ref, Phase: domain.PhaseBuild, AttemptID: claim.ID, Attempt: claim.Attempt}, PostbuildAmendmentSnapshot{FullSnapshotDigest: "sha256:" + strings.Repeat("b", 64), ImplementationDigest: "sha256:" + strings.Repeat("c", 64), ProtectedPathsDigest: protected}
}

func postbuildAmendmentProposal(original phaseartifact.Verification) phaseartifact.Verification {
	original.EvidenceDigest = sha256Digest([]byte("postbuild-proposed-proof-evidence"))
	return original
}

func TestPostbuildAmendmentRequiresSnapshotAndRetainsExactBinding(t *testing.T) {
	db, ctx, transition, key, snapshot := postbuildAmendmentFixture(t)
	if _, err := db.TransitionVerificationAmendmentRequest(ctx, transition, key); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("ordinary route bypassed snapshot: %v", err)
	}
	wrong := snapshot
	wrong.ProtectedPathsDigest = "sha256:" + strings.Repeat("d", 64)
	if _, err := db.TransitionPostbuildVerificationAmendmentRequest(ctx, transition, key, wrong); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("wrong frozen protected set accepted: %v", err)
	}
	result, err := db.TransitionPostbuildVerificationAmendmentRequest(ctx, transition, key, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != transition.ExpectedVersion+1 {
		t.Fatal("wrong request endpoint")
	}
	context, err := db.PostbuildVerificationAmendmentContext(ctx, transition.Ref, result.Version, transition.Fence)
	if err != nil || context.Snapshot != snapshot || context.Amendment.BuilderResult != key || context.Binding.BuilderResult != key || context.Builder.Claim.ID != key.AttemptID || context.Binding.AmendmentTransitionVersion != result.Version || context.Verification.Checkpoint.CommitOID != context.Repair.OriginalCheckpointOID || context.Decision != "" {
		t.Fatalf("companion context=%+v err=%v", context, err)
	}
	for _, query := range []string{`SELECT COUNT(*) FROM verification_amendment_requests`, `SELECT COUNT(*) FROM postbuild_amendment_snapshots`} {
		var count int
		if err := db.db.QueryRowContext(ctx, query).Scan(&count); err != nil || count != 1 {
			t.Fatalf("atomic rows=%d err=%v", count, err)
		}
	}
	for _, statement := range []string{`UPDATE postbuild_amendment_snapshots SET created_at=created_at`, `DELETE FROM postbuild_amendment_snapshots`} {
		if _, err := db.db.ExecContext(ctx, statement); err == nil {
			t.Fatal("immutable companion mutated")
		}
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER postbuild_amendment_snapshots_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE postbuild_amendment_snapshots SET implementation_digest=?`, "sha256:"+strings.Repeat("e", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PendingVerificationAmendment(ctx, transition.Ref, result.Version, transition.Fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("tampered companion hidden by ordinary reader: %v", err)
	}
	if _, err := db.PostbuildVerificationAmendmentContext(ctx, transition.Ref, result.Version, transition.Fence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("tampered companion accepted: %v", err)
	}
}

func TestPostbuildAmendmentSnapshotInsertFailureRollsBackRequestAndBudget(t *testing.T) {
	db, ctx, transition, key, snapshot := postbuildAmendmentFixture(t)
	var beforeBudgets, beforeEvents int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ticket_budget_uses`).Scan(&beforeBudgets); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&beforeEvents); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `CREATE TRIGGER fail_postbuild_snapshot BEFORE INSERT ON postbuild_amendment_snapshots BEGIN SELECT RAISE(ABORT,'injected companion failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPostbuildVerificationAmendmentRequest(ctx, transition, key, snapshot); err == nil {
		t.Fatal("injected failure was ignored")
	}
	for _, check := range []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM ticket_budget_uses`, beforeBudgets},
		{`SELECT COUNT(*) FROM events`, beforeEvents},
		{`SELECT COUNT(*) FROM verification_amendment_requests`, 0},
		{`SELECT COUNT(*) FROM postbuild_amendment_snapshots`, 0},
	} {
		var got int
		if err := db.db.QueryRowContext(ctx, check.query).Scan(&got); err != nil || got != check.want {
			t.Fatalf("partial transaction: got=%d want=%d err=%v", got, check.want, err)
		}
	}
	ticket, err := db.Ticket(ctx, transition.Ref)
	if err != nil || ticket.State != domain.StateBuilding || ticket.Version != transition.ExpectedVersion {
		t.Fatalf("failed request changed ticket: %+v %v", ticket, err)
	}
}

func TestPostbuildAmendmentProtectedScopeDigestIsExact(t *testing.T) {
	first, err := PostbuildAmendmentProtectedPathsDigest([]string{"z_test.go", "a_test.go"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PostbuildAmendmentProtectedPathsDigest([]string{"a_test.go", "z_test.go"})
	if err != nil || first != second {
		t.Fatal("scope identity depends on ordering")
	}
	widened, err := PostbuildAmendmentProtectedPathsDigest([]string{"a_test.go", "src.go", "z_test.go"})
	if err != nil || widened == first {
		t.Fatal("scope widening retained identity")
	}
	if _, err := PostbuildAmendmentProtectedPathsDigest([]string{"a_test.go", "a_test.go"}); err == nil {
		t.Fatal("duplicate scope accepted")
	}
}
