package worktreecoord

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
)

type preparedAdmissionFixture struct {
	*amendmentAdmissionFixture
	receipt      store.PostbuildAmendmentCheckpointSnapshot
	receiptReads int
}

func (f *preparedAdmissionFixture) PostbuildAmendmentCheckpointSnapshot(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildAmendmentCheckpointSnapshot, error) {
	f.receiptReads++
	value := f.receipt
	if f.mode == "late receipt" && f.receiptReads == 2 {
		value.BindingDigest += "changed"
	}
	return value, nil
}

func TestPreparedPostbuildAdmissionNeverGrantsFreshProviderAuthority(t *testing.T) {
	for _, mode := range []string{"child unsynced", "parent exact", "parent drift", "foreign head", "wrong reviewer", "late receipt", "late prepared", "late bytes", "implementation mutation", "missing prepared", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			request, project, original, observed := postbuildRepairAdmissionFixture(t)
			f := &preparedAdmissionFixture{amendmentAdmissionFixture: &amendmentAdmissionFixture{project: project, observed: observed, mode: mode, preparedFound: true}}
			f.proof = store.PostbuildVerificationAmendmentContext{Worktree: original.Worktree, Verification: original.Verification, Plan: original.Plan, Builder: original.Builder}
			f.proof.Binding.OriginalCheckpointOID = observed.Changes.Head
			f.proof.Binding.BindingDigest = "companion"
			f.proof.Amendment.TransitionTicketVersion = request.Version
			f.proof.Snapshot.ImplementationDigest = "implementation"
			f.prepared = store.CommitObservation{ParentOID: observed.Changes.Head, CommitOID: original.Worktree.BaseSHA, TreeOID: original.Worktree.BaseSHA}
			f.completed = store.LatestReusableProviderAttemptResult{Result: original.Builder}
			f.completed.Key.AttemptID = 20
			f.completed.Result.Claim.ExpectedVersion = request.Version
			f.completed.Parsed.Verify = &phaseartifact.Verification{OwnedFiles: original.Verification.Revision.OwnedFiles}
			f.receipt = store.PostbuildAmendmentCheckpointSnapshot{Ref: request.Ref, AmendmentTransitionVersion: request.Version, Reviewer: f.completed.Key, CompanionBindingDigest: "companion", FullSnapshotDigest: observed.Digest, ImplementationDigest: "implementation"}
			f.observed.Changes.Head = f.prepared.CommitOID
			f.observed.Changes.Paths = []string{"src/main.go", "src/proof_test.go"}
			switch mode {
			case "parent exact":
				f.observed.Changes.Head = f.prepared.ParentOID
			case "parent drift":
				f.observed.Changes.Head = f.prepared.ParentOID
				f.observed.Digest += "changed"
			case "foreign head":
				f.observed.Changes.Head = "foreign"
			case "wrong reviewer":
				f.receipt.Reviewer.AttemptID++
			case "missing prepared":
				f.preparedFound = false
			case "rejected":
				f.proof.Decision = store.VerificationAmendmentRejected
			}
			_, err := authenticatePreparedPostbuildAmendment(context.Background(), request, f, f)
			if mode == "child unsynced" || mode == "parent exact" {
				if err != nil || f.preparedReads != 2 || f.receiptReads != 2 || f.inspections != 2 || f.fences != 1 {
					t.Fatalf("admission err=%v calls=%+v", err, f)
				}
			} else if err == nil {
				t.Fatal("unsafe prepared admission accepted")
			}
		})
	}
}
