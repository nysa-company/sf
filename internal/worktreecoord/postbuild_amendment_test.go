package worktreecoord

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
)

type amendmentAdmissionFixture struct {
	proof                                                   store.PostbuildVerificationAmendmentContext
	project                                                 store.Project
	observed                                                git.RetainedWorktreeInspection
	completed                                               store.LatestReusableProviderAttemptResult
	completedErr                                            error
	prepared                                                store.CommitObservation
	preparedFound                                           bool
	preparedReads                                           int
	preparedCandidate                                       store.PostbuildAmendmentPreparedCandidateWitness
	preparedCandidateFound                                  bool
	mode                                                    string
	loads, reuses, inspections, implementationReads, fences int
}

func (f *amendmentAdmissionFixture) PostbuildAmendmentPreparedCandidateWitness(context.Context, domain.TicketRef, uint64, domain.Fence, store.ProviderAttemptResultKey) (store.PostbuildAmendmentPreparedCandidateWitness, bool, error) {
	return f.preparedCandidate, f.preparedCandidateFound, nil
}

func (f *amendmentAdmissionFixture) PostbuildAmendmentPreparedCheckpoint(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CommitObservation, bool, error) {
	f.preparedReads++
	if f.mode == "late prepared" && f.preparedReads == 2 {
		return store.CommitObservation{}, false, store.ErrStaleFence
	}
	return f.prepared, f.preparedFound, nil
}

func (f *amendmentAdmissionFixture) PostbuildVerificationAmendmentContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.PostbuildVerificationAmendmentContext, error) {
	f.loads++
	if f.mode == "late authority" && f.loads == 2 {
		return store.PostbuildVerificationAmendmentContext{}, store.ErrStaleFence
	}
	return f.proof, nil
}
func (f *amendmentAdmissionFixture) Project(context.Context, domain.Channel, domain.ProjectID) (store.Project, error) {
	return f.project, nil
}
func (f *amendmentAdmissionFixture) TicketWorktreePath(domain.TicketRef) (string, error) {
	return "/private/worktree", nil
}
func (f *amendmentAdmissionFixture) LatestReusableProviderAttempt(context.Context, store.LatestReusableProviderAttemptRequest) (store.LatestReusableProviderAttemptResult, error) {
	f.reuses++
	if f.mode == "late attempt" && f.reuses == 2 {
		return f.completed, nil
	}
	if f.mode == "late result" && f.reuses == 2 {
		value := f.completed
		value.Result.TypedSHA256 += "changed"
		return value, nil
	}
	return f.completed, f.completedErr
}
func (f *amendmentAdmissionFixture) CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error) {
	return store.StoredVerification{}, store.ErrNotFound
}
func (f *amendmentAdmissionFixture) AssertTicketFence(context.Context, domain.TicketRef, uint64, domain.Fence) error {
	f.fences++
	if f.mode == "late fence" {
		return store.ErrStaleFence
	}
	return nil
}
func (f *amendmentAdmissionFixture) InspectRetainedWorktree(context.Context, git.Worktree) (git.RetainedWorktreeInspection, error) {
	f.inspections++
	if f.mode == "snapshot deadline" {
		return git.RetainedWorktreeInspection{}, errors.Join(context.DeadlineExceeded, errors.New("private subprocess output"))
	}
	value := f.observed
	if f.mode == "late bytes" && f.inspections == 2 {
		value.Digest += "changed"
	}
	return value, nil
}

func TestPostbuildAmendmentPersistedCandidateAdmission(t *testing.T) {
	for _, mode := range []string{"valid", "dirty", "foreign head", "missing builder", "wrong builder", "late authority", "late bytes", "snapshot deadline"} {
		t.Run(mode, func(t *testing.T) {
			request, project, original, observed := postbuildRepairAdmissionFixture(t)
			request.Version = 8
			f := &amendmentAdmissionFixture{project: project, observed: observed, mode: mode}
			f.proof = store.PostbuildVerificationAmendmentContext{Worktree: original.Worktree, Plan: original.Plan, Verification: original.Verification, Builder: original.Builder, CurrentVerification: original.Verification, Decision: store.VerificationAmendmentAccepted}
			f.proof.Amendment.TransitionTicketVersion = 6
			f.proof.Binding.OriginalCheckpointOID = observed.Changes.Head
			f.completed.Key = store.ProviderAttemptResultKey{Ref: request.Ref, Phase: domain.PhaseBuild, AttemptID: 15, Attempt: 3}
			f.completed.Result.Claim.ExpectedVersion = 7
			f.completed.Result.TypedSHA256 = strings.Repeat("e", 64)
			f.completed.Parsed.Builder = &phaseartifact.Builder{ChangedFiles: []string{"src/main.go"}}
			f.proof.Candidate = &store.StoredCandidate{BuilderResult: f.completed.Key, Snapshot: domain.CandidateSnapshot{HeadSHA: strings.Repeat("a", 40)}}
			f.observed.Changes.Head = f.proof.Candidate.Snapshot.HeadSHA
			f.observed.Changes.Paths = nil
			switch mode {
			case "dirty":
				f.observed.Changes.Paths = []string{"src/main.go"}
			case "foreign head":
				f.observed.Changes.Head = strings.Repeat("b", 40)
			case "missing builder":
				f.completedErr = store.ErrNotFound
			case "wrong builder":
				f.completed.Key.AttemptID++
			}
			_, err := authenticatePostbuildVerificationAmendment(context.Background(), request, f, f)
			if mode == "valid" {
				if err != nil || f.implementationReads != 0 || f.fences != 1 || f.inspections != 2 {
					t.Fatalf("candidate handoff: %v", err)
				}
			} else if err == nil {
				t.Fatal("unsafe candidate handoff accepted")
			}
			if mode == "snapshot deadline" && (!errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrUnready) || strings.Contains(err.Error(), "private subprocess output")) {
				t.Fatalf("unsafe or missing typed diagnostic: %v", err)
			}
		})
	}
}

func TestPostbuildAmendmentAdmissionPreservesSafeDeadline(t *testing.T) {
	request, project, original, observed := postbuildRepairAdmissionFixture(t)
	request.Version = 8
	f := &amendmentAdmissionFixture{project: project, observed: observed, mode: "snapshot deadline", completedErr: store.ErrNotFound}
	f.proof = store.PostbuildVerificationAmendmentContext{Worktree: original.Worktree, Plan: original.Plan, Verification: original.Verification, Builder: original.Builder}
	f.proof.Amendment.TransitionTicketVersion = 6
	f.proof.Binding.OriginalCheckpointOID = observed.Changes.Head
	_, err := authenticatePostbuildVerificationAmendment(context.Background(), request, f, f)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrUnready) || strings.Contains(err.Error(), "private subprocess output") || !strings.Contains(err.Error(), "initial physical snapshot") {
		t.Fatalf("unsafe or missing typed diagnostic: %v", err)
	}
}

func TestPostbuildAmendmentAdmissionHonorsCallerDeadline(t *testing.T) {
	request, _, _, _ := postbuildRepairAdmissionFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	f := &amendmentAdmissionFixture{}
	if _, err := authenticatePostbuildVerificationAmendment(ctx, request, f, f); !errors.Is(err, context.DeadlineExceeded) || f.loads != 0 || f.inspections != 0 {
		t.Fatalf("expired caller admitted work: %v", err)
	}
	prepared := &preparedAdmissionFixture{amendmentAdmissionFixture: f}
	if _, err := authenticatePreparedPostbuildAmendment(ctx, request, prepared, prepared); !errors.Is(err, context.DeadlineExceeded) || f.loads != 0 || f.inspections != 0 {
		t.Fatalf("expired prepared caller admitted work: %v", err)
	}
}

func TestPostbuildAmendmentPreparedCandidateAdmission(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "wrong parent", "wrong builder", "dirty", "late authority"} {
		t.Run(mode, func(t *testing.T) {
			request, project, original, observed := postbuildRepairAdmissionFixture(t)
			request.Version = 8
			f := &amendmentAdmissionFixture{project: project, observed: observed, mode: mode, preparedCandidateFound: true}
			f.proof = store.PostbuildVerificationAmendmentContext{Worktree: original.Worktree, Plan: original.Plan, Verification: original.Verification, Builder: original.Builder, CurrentVerification: original.Verification, Decision: store.VerificationAmendmentAccepted}
			f.proof.Amendment.TransitionTicketVersion = 6
			f.proof.Binding.OriginalCheckpointOID = observed.Changes.Head
			f.completed.Key = store.ProviderAttemptResultKey{Ref: request.Ref, Phase: domain.PhaseBuild, AttemptID: 15, Attempt: 3}
			f.completed.Result.Claim.ExpectedVersion = 7
			f.completed.Result.TypedSHA256 = strings.Repeat("e", 64)
			f.completed.Parsed.Builder = &phaseartifact.Builder{ChangedFiles: []string{"src/main.go"}}
			f.preparedCandidate = store.PostbuildAmendmentPreparedCandidateWitness{Builder: f.completed.Key, Commit: store.CommitObservation{ParentOID: original.Verification.Checkpoint.CommitOID, CommitOID: strings.Repeat("a", 40)}}
			f.observed.Changes.Head, f.observed.Changes.Paths = f.preparedCandidate.Commit.CommitOID, nil
			switch mode {
			case "missing":
				f.preparedCandidateFound = false
			case "wrong parent":
				f.preparedCandidate.Commit.ParentOID = strings.Repeat("c", 40)
				if f.preparedCandidate.Commit.ParentOID == original.Verification.Checkpoint.CommitOID {
					t.Fatal("wrong-parent fixture must differ from the authenticated checkpoint")
				}
			case "wrong builder":
				f.preparedCandidate.Builder.AttemptID++
			case "dirty":
				f.observed.Changes.Paths = []string{"src/main.go"}
			}
			_, err := authenticatePostbuildVerificationAmendment(context.Background(), request, f, f)
			if (err == nil) != (mode == "valid") {
				t.Fatalf("prepared candidate admission: %v", err)
			}
		})
	}
}
func (f *amendmentAdmissionFixture) InspectRetainedImplementation(_ context.Context, _ git.Worktree, baseline string, protected []string) (string, error) {
	f.implementationReads++
	if baseline != f.proof.Binding.OriginalCheckpointOID || len(protected) != 1 || protected[0] != "src/proof_test.go" {
		return "", git.ErrUnsafeWorktree
	}
	if f.mode == "implementation mutation" {
		return "changed", nil
	}
	return f.proof.Snapshot.ImplementationDigest, nil
}

// These are composition tests: Store and Git retain responsibility for real
// evidence lineage and no-follow bounded byte observations in their own suites.
func TestPostbuildAmendmentAdmissionSequencing(t *testing.T) {
	for _, lane := range []string{"pending", "reviewer completed", "accepted", "builder completed"} {
		for _, mode := range []string{"valid", "foreign head", "foreign path", "implementation mutation", "protected drift", "old result", "late result", "late attempt", "late authority", "late bytes", "late fence", "rejected", "cancel"} {
			t.Run(lane+"/"+mode, func(t *testing.T) {
				request, project, original, observed := postbuildRepairAdmissionFixture(t)
				request.Version = 8
				f := &amendmentAdmissionFixture{project: project, observed: observed, mode: mode, completedErr: store.ErrNotFound}
				f.proof = store.PostbuildVerificationAmendmentContext{Worktree: original.Worktree, Plan: original.Plan, Verification: original.Verification, Builder: original.Builder, CurrentVerification: original.Verification}
				f.proof.Amendment.TransitionTicketVersion = 6
				f.proof.Binding.OriginalCheckpointOID = observed.Changes.Head
				f.proof.Snapshot = store.PostbuildAmendmentSnapshot{FullSnapshotDigest: observed.Digest, ImplementationDigest: "sha256:" + strings.Repeat("d", 64)}
				f.completed.Result.Claim.ExpectedVersion = 6
				f.completed.Result.TypedSHA256 = strings.Repeat("e", 64)
				f.completed.Key.AttemptID = 15
				if lane == "reviewer completed" {
					f.completedErr = nil
					f.completed.Parsed.Verify = &phaseartifact.Verification{OwnedFiles: []string{"src/proof_test.go"}}
					f.observed.Digest += "reviewer changed protected bytes"
				}
				if lane == "accepted" || lane == "builder completed" {
					f.proof.Decision = store.VerificationAmendmentAccepted
					f.proof.CurrentVerification.Checkpoint.CommitOID = strings.Repeat("f", 40)
					f.observed.Changes.Head = f.proof.CurrentVerification.Checkpoint.CommitOID
					if lane == "builder completed" {
						f.completedErr = nil
						f.completed.Result.Claim.ExpectedVersion = 7
						f.completed.Parsed.Builder = &phaseartifact.Builder{ChangedFiles: []string{"src/main.go"}}
					}
				}
				wantOK := mode == "valid" || mode == "implementation mutation" && lane == "builder completed" || mode == "old result" && (lane == "pending" || lane == "accepted") || mode == "late attempt" && (lane == "reviewer completed" || lane == "builder completed")
				switch mode {
				case "foreign head":
					f.observed.Changes.Head = strings.Repeat("0", 40)
				case "foreign path":
					f.proof.Worktree.Path += "foreign"
				case "protected drift":
					f.observed.Changes.Paths = []string{"src/proof_test.go/nested"}
					f.observed.Digest += "protected change"
					wantOK = lane == "reviewer completed"
				case "old result":
					f.completed.Result.Claim.ExpectedVersion = 5
				case "rejected":
					f.proof.Decision = store.VerificationAmendmentRejected
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if mode == "cancel" {
					cancel()
				}
				got, err := authenticatePostbuildVerificationAmendment(ctx, request, f, f)
				if wantOK {
					if err != nil || got.Path != original.Worktree.Path || f.loads != 2 || f.inspections != 2 || f.reuses != 2 || f.fences != 1 {
						t.Fatalf("admission err=%v loads=%d inspections=%d reuses=%d fences=%d", err, f.loads, f.inspections, f.reuses, f.fences)
					}
				} else if err == nil {
					t.Fatal("unsafe admission accepted")
				}
			})
		}
	}
}

func TestPostbuildAmendmentPreparedCheckpointAdmission(t *testing.T) {
	for _, mode := range []string{"valid", "missing prepared", "foreign child", "foreign parent", "late prepared", "protected drift", "implementation mutation", "old reviewer", "no reviewer"} {
		t.Run(mode, func(t *testing.T) {
			request, project, original, observed := postbuildRepairAdmissionFixture(t)
			request.Version = 8
			f := &amendmentAdmissionFixture{project: project, observed: observed, mode: mode, preparedFound: true}
			f.proof = store.PostbuildVerificationAmendmentContext{Worktree: original.Worktree, Plan: original.Plan, Verification: original.Verification, Builder: original.Builder}
			f.proof.Amendment.TransitionTicketVersion = 6
			f.proof.Binding.OriginalCheckpointOID = observed.Changes.Head
			f.proof.Snapshot = store.PostbuildAmendmentSnapshot{FullSnapshotDigest: observed.Digest, ImplementationDigest: "sha256:" + strings.Repeat("d", 64)}
			f.completed.Result.Claim.ExpectedVersion = 6
			f.completed.Result.TypedSHA256 = strings.Repeat("e", 64)
			f.completed.Parsed.Verify = &phaseartifact.Verification{OwnedFiles: []string{"src/proof_test.go"}}
			f.prepared = store.CommitObservation{ParentOID: observed.Changes.Head, CommitOID: strings.Repeat("f", 40), TreeOID: strings.Repeat("a", 40)}
			f.observed.Changes.Head = f.prepared.CommitOID
			f.observed.Digest += "checkpoint committed"
			switch mode {
			case "missing prepared":
				f.preparedFound = false
			case "foreign child":
				f.observed.Changes.Head = strings.Repeat("0", 40)
			case "foreign parent":
				f.prepared.ParentOID = strings.Repeat("0", 40)
			case "protected drift":
				f.observed.Changes.Paths = []string{"src/proof_test.go"}
			case "old reviewer":
				f.completed.Result.Claim.ExpectedVersion = 5
			case "no reviewer":
				f.completedErr = store.ErrNotFound
			}
			got, err := authenticatePostbuildVerificationAmendment(context.Background(), request, f, f)
			if mode == "valid" {
				if err != nil || got.Path != original.Worktree.Path || f.preparedReads != 2 || f.implementationReads != 1 || f.inspections != 2 || f.fences != 1 {
					t.Fatalf("prepared replay err=%v prepared=%d implementation=%d inspections=%d fences=%d", err, f.preparedReads, f.implementationReads, f.inspections, f.fences)
				}
			} else if err == nil {
				t.Fatal("unsafe prepared replay accepted")
			}
		})
	}
}
