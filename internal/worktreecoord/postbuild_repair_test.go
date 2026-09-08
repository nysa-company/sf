package worktreecoord

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
)

// Store's suite proves the immutable entry and absence of competing writers;
// Git's suite proves physical bytes/no-follow/ignored-file handling. These tests
// exercise their composition and require late authority failures to stop launch.
func TestPostbuildRepairAdmissionRequiresExactSnapshotAndAuthority(t *testing.T) {
	for _, mode := range []string{"valid", "absent", "stale", "foreign ref", "foreign identity", "foreign registration", "wrong verification", "amendment", "different bytes", "foreign head", "undeclared", "outside plan", "protected", "inspection failure", "late revocation", "changed binding", "late fence", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, project, proof, observed := postbuildRepairAdmissionFixture(t)
			loads, inspections, fences := 0, 0, 0
			load := func(context.Context) (store.PostbuildRepairBuildContext, error) {
				loads++
				if mode == "absent" {
					return store.PostbuildRepairBuildContext{}, store.ErrNotFound
				}
				if mode == "stale" || mode == "late revocation" && loads == 2 {
					return store.PostbuildRepairBuildContext{}, store.ErrStaleFence
				}
				value := proof
				switch mode {
				case "foreign ref":
					value.Repair.Ref.Ticket = "SF-other"
				case "foreign identity":
					value.Worktree.IdentityJSON = []byte(`{}`)
				case "foreign registration":
					value.Worktree.Path += "-replacement"
				case "wrong verification":
					value.Verification.Revision.CheckpointID = strings.Repeat("d", 40)
				case "amendment":
					value.BuilderArtifact.AmendmentRequest = &phaseartifact.AmendmentRequest{}
				case "outside plan":
					planner := *value.Plan.Document.Planner
					planner.Paths = []string{"elsewhere"}
					value.Plan.Document.Planner = &planner
				case "changed binding":
					if loads == 2 {
						value.Repair.BindingDigest = "sha256:" + strings.Repeat("d", 64)
					}
				}
				return value, nil
			}
			inspect := func(_ context.Context, worktree git.Worktree) (git.RetainedWorktreeInspection, error) {
				inspections++
				if worktree.Path != proof.Worktree.Path || worktree.Identity.Repository != project.Path {
					t.Fatal("wrong physical identity supplied")
				}
				value := observed
				switch mode {
				case "different bytes":
					value.Digest = "sha256:" + strings.Repeat("d", 64)
				case "foreign head":
					value.Changes.Head = strings.Repeat("d", 40)
				case "undeclared":
					value.Changes.Paths = []string{"src/other.go"}
				case "protected":
					value.Changes.Paths = []string{"src/proof_test.go"}
				case "inspection failure":
					return git.RetainedWorktreeInspection{}, git.ErrUnsafeWorktree
				case "cancel":
					cancel()
				}
				return value, nil
			}
			assert := func(context.Context) error {
				fences++
				if mode == "late fence" {
					return store.ErrStaleFence
				}
				return nil
			}
			got, err := authenticatePostbuildRepair(ctx, request, project, proof.Worktree.Path, load, inspect, assert, false)
			if mode == "valid" {
				if err != nil || got.Path != proof.Worktree.Path || loads != 2 || inspections != 1 || fences != 1 {
					t.Fatalf("valid admission failed: %+v %v loads=%d inspections=%d fences=%d", got, err, loads, inspections, fences)
				}
				return
			}
			if err == nil || got.Path != "" {
				t.Fatalf("invalid admission produced worktree: %+v %v", got, err)
			}
			if mode == "absent" && (!errors.Is(err, store.ErrNotFound) || inspections != 0) {
				t.Fatal("absence lost or inspected before authority")
			}
			if mode == "stale" && (!errors.Is(err, store.ErrStaleFence) || inspections != 0) {
				t.Fatal("stale authority reached physical inspection")
			}
		})
	}
}

func postbuildRepairAdmissionFixture(t *testing.T) (EnsureRequest, store.Project, store.PostbuildRepairBuildContext, git.RetainedWorktreeInspection) {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-postbuild"}
	request := EnsureRequest{Ref: ref, Version: 5, Fence: domain.Fence{LeaderEpoch: 2, RunnerEpoch: 3}}
	project := store.Project{Channel: ref.Channel, ID: ref.Project, Path: "/private/fixture", BaseRef: "main"}
	base, head := strings.Repeat("a", 40), strings.Repeat("b", 40)
	identity := git.Identity{Repository: project.Path, Worktree: "/private/worktree", HeadRef: "sf/dev/fixture", BaseRef: "main", BaseHead: base}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	worktree := store.StoredWorktree{Path: identity.Worktree, Branch: identity.HeadRef, BaseSHA: base, HeadSHA: base, State: "registered", IdentityJSON: raw}
	key := store.ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild, AttemptID: 12, Attempt: 1}
	verification := store.StoredVerification{Revision: store.VerificationRevision{Revision: 1, CheckpointID: head, OwnedFiles: []string{"src/proof_test.go"}}, Checkpoint: store.CommitObservation{CommitOID: head}}
	proof := store.PostbuildRepairBuildContext{
		Repair:   store.PostbuildRepair{PostbuildFailureRequest: store.PostbuildFailureRequest{Ref: ref, ExpectedVersion: 4, BuilderResult: key}, EntryVersion: 5, OriginalCheckpointOID: head, VerificationRevision: 1, RetainedWorktreeDigest: "sha256:" + strings.Repeat("c", 64), BindingDigest: "sha256:" + strings.Repeat("e", 64), BuilderTypedDigest: strings.Repeat("f", 64), Verification: verification},
		Worktree: worktree, Verification: verification,
		Plan:            store.StoredPlan{Document: store.PlanDocument{Planner: &phaseartifact.Planner{Paths: []string{"src"}}}},
		Builder:         store.ProviderAttemptResult{AttemptID: key.AttemptID, TypedSHA256: strings.Repeat("f", 64), Claim: store.ProviderAttemptClaim{ID: key.AttemptID, Ref: ref, Phase: domain.PhaseBuild, Role: "builder", Repository: project.Path, Worktree: worktree.Path, WorktreeIdentity: string(raw), BaseSHA: base}},
		BuilderArtifact: phaseartifact.Builder{ChangedFiles: []string{"src/main.go", "src/proof_test.go"}},
	}
	observed := git.RetainedWorktreeInspection{Changes: git.WorktreeChanges{Head: head, Paths: []string{"src/main.go"}}, Digest: proof.Repair.RetainedWorktreeDigest}
	return request, project, proof, observed
}

func TestPostbuildRepairAdmissionRequiresStore(t *testing.T) {
	if _, err := (Coordinator{}).AuthenticatePostbuildRepair(context.Background(), EnsureRequest{}); err == nil {
		t.Fatal("missing authority accepted")
	}
}

func TestCompletedPostbuildRepairRequiresFreshResultAndStableSnapshot(t *testing.T) {
	for _, mode := range []string{"valid", "predecessor result", "earlier entry", "changed snapshot", "changed result", "revoked after second inspection", "protected"} {
		t.Run(mode, func(t *testing.T) {
			request, project, proof, observed := postbuildRepairAdmissionFixture(t)
			proof.Builder.AttemptID++
			proof.Builder.Claim.ID = proof.Builder.AttemptID
			proof.Builder.Claim.Attempt = proof.Repair.BuilderResult.Attempt + 1
			proof.Builder.Claim.ExpectedVersion = proof.Repair.EntryVersion
			proof.Builder.TypedSHA256 = strings.Repeat("a", 64)
			observed.Digest = "sha256:" + strings.Repeat("b", 64) // New Builder bytes.
			if mode == "predecessor result" {
				proof.Builder.AttemptID = proof.Repair.BuilderResult.AttemptID
				proof.Builder.Claim.ID = proof.Builder.AttemptID
			}
			if mode == "earlier entry" {
				proof.Builder.Claim.ExpectedVersion = proof.Repair.EntryVersion - 1
			}
			if mode == "protected" {
				observed.Changes.Paths = []string{"src/proof_test.go"}
			}
			loads, inspections := 0, 0
			load := func(context.Context) (store.PostbuildRepairBuildContext, error) {
				loads++
				value := proof
				if mode == "changed result" && loads == 2 {
					value.Builder.TypedSHA256 = strings.Repeat("c", 64)
				}
				if mode == "revoked after second inspection" && loads == 3 {
					return store.PostbuildRepairBuildContext{}, store.ErrStaleFence
				}
				return value, nil
			}
			inspect := func(context.Context, git.Worktree) (git.RetainedWorktreeInspection, error) {
				inspections++
				value := observed
				if mode == "changed snapshot" && inspections == 2 {
					value.Digest = "sha256:" + strings.Repeat("c", 64)
				}
				return value, nil
			}
			got, err := authenticatePostbuildRepair(context.Background(), request, project, proof.Worktree.Path, load, inspect, func(context.Context) error { return nil }, true)
			if mode == "valid" {
				if err != nil || got.Path != proof.Worktree.Path || loads != 3 || inspections != 2 {
					t.Fatalf("completed result refused: %+v %v loads=%d inspections=%d", got, err, loads, inspections)
				}
			} else if err == nil || got.Path != "" {
				t.Fatalf("invalid completed result accepted: %+v %v", got, err)
			}
		})
	}
}
