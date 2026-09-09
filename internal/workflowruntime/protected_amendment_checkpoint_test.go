package workflowruntime_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

func TestRepositoryMaterializerPostbuildAmendmentRealEndToEnd(t *testing.T) {
	for _, accepted := range []bool{true, false} {
		name := "rejected"
		if accepted {
			name = "accepted"
		}
		t.Run(name, func(t *testing.T) {
			f := newMaterializerRealFixtureWithProvider(t, func(t *testing.T) string { return writeMaterializerAmendmentProvider(t, accepted) })
			f.worker.Engine = f.state.StateMachine
			for _, want := range []domain.State{domain.StateVerifying, domain.StateBuilding} {
				if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != want {
					t.Fatalf("setup=%+v want=%s err=%v", result, want, err)
				}
			}
			original, err := f.db.CurrentVerification(f.ctx, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != domain.StateBuilding || !result.Transitioned {
				t.Fatalf("failed postbuild repair=%+v err=%v", result, err)
			}
			if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != domain.StateVerifying {
				t.Fatalf("repair amendment request=%+v err=%v", result, err)
			}
			assertMaterializerProviderAttempts(t, f.db, f.ref, 4)
			pending, err := f.db.Ticket(f.ctx, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			coordinator := worktreecoord.Coordinator{Store: f.db, Git: f.materializer.Git}
			if _, err := coordinator.AuthenticatePostbuildVerificationAmendment(f.ctx, worktreecoord.EnsureRequest{Ref: f.ref, Version: pending.Version, Fence: f.fence}); err != nil {
				t.Fatalf("retained Reviewer admission: %v", err)
			}
			implementationBefore, err := os.ReadFile(filepath.Join(f.worktree, "tracked_test.go"))
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.worker.Run(f.ctx, f.ref, f.fence)
			if err != nil {
				t.Fatalf("fresh amendment Reviewer: %v", err)
			}
			assertMaterializerProviderAttempts(t, f.db, f.ref, 5)
			implementationAfter, err := os.ReadFile(filepath.Join(f.worktree, "tracked_test.go"))
			if err != nil || string(implementationBefore) != string(implementationAfter) {
				t.Fatalf("Reviewer/checkpoint changed implementation: %v", err)
			}
			if !accepted {
				if result.State != domain.StateBlocked {
					t.Fatalf("rejection did not stop: %+v", result)
				}
				if head := rawMaterializerGit(t, f.worktree, "rev-parse", "HEAD"); head != original.Checkpoint.CommitOID {
					t.Fatalf("rejection moved checkpoint: %s", head)
				}
				if _, err := f.db.LatestCandidate(f.ctx, f.ref); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("rejection created candidate: %v", err)
				}
				if replay, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || replay.State != domain.StateBlocked {
					t.Fatalf("blocked replay=%+v %v", replay, err)
				}
				assertMaterializerProviderAttempts(t, f.db, f.ref, 5)
				return
			}
			if result.State != domain.StateBuilding {
				t.Fatalf("accepted amendment=%+v", result)
			}
			amended, err := f.db.CurrentVerification(f.ctx, f.ref)
			if err != nil || amended.Revision.Revision != original.Revision.Revision+1 || amended.Checkpoint.ParentOID != original.Checkpoint.CommitOID || amended.ProviderResult == original.ProviderResult {
				t.Fatalf("fresh proof=%+v err=%v", amended, err)
			}
			if paths := rawMaterializerGit(t, f.worktree, "diff-tree", "--no-commit-id", "--name-only", "-r", amended.Checkpoint.CommitOID); paths != "proof_test.go" {
				t.Fatalf("checkpoint included implementation: %q", paths)
			}
			if _, err := coordinator.AuthenticatePostbuildVerificationAmendment(f.ctx, worktreecoord.EnsureRequest{Ref: f.ref, Version: result.Version, Fence: f.fence}); err != nil {
				t.Fatalf("retained fresh Builder admission: %v", err)
			}
			if result, err := f.worker.Run(f.ctx, f.ref, f.fence); err != nil || result.State != domain.StatePublishing {
				t.Fatalf("fresh post-amendment Builder=%+v err=%v", result, err)
			}
			assertMaterializerProviderAttempts(t, f.db, f.ref, 6)
			candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
			if err != nil || candidate.BuilderResult.AttemptID <= amended.ProviderResult.AttemptID {
				t.Fatalf("candidate reused pre-amendment Builder: %+v %v", candidate, err)
			}
			retained, err := f.db.RecoverableVerification(f.ctx, f.ref)
			if err != nil || !reflect.DeepEqual(retained.Revision, amended.Revision) {
				t.Fatalf("candidate lost amended proof: %v", err)
			}
		})
	}
}

// The initial proof is contradictory. Only the independent amendment Reviewer
// changes it; it stays red until the next Builder changes FeatureValue to 2.
// The requesting Builder declares the protected proposal but never writes it.
func writeMaterializerAmendmentProvider(t *testing.T, accepted bool) string {
	t.Helper()
	path := writeMaterializerProvider(t)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	script := strings.ReplaceAll(string(data), "proof.txt", "proof_test.go")
	plan := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"real materializer"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "real"}, Paths: []string{"go.mod", "proof_test.go", "tracked_test.go"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"none"}}
	identity, err := workflowprompt.NewPlanIdentity(plan)
	if err != nil {
		t.Fatal(err)
	}
	proofDigest := func(evidence string) string {
		data, err := workflowprompt.CanonicalVerificationProofBytes(phaseartifact.Verification{AcceptanceDigest: identity.Digest, ProofKind: phaseartifact.ProofAcceptance, PrebuildOutcome: "red", EvidenceDigest: strings.Repeat(evidence, 64)})
		if err != nil {
			t.Fatal(err)
		}
		return materializerDigest(string(data))
	}
	proposal := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "request independent proof correction", ChangedFiles: []string{"proof_test.go", "tracked_test.go"}, Commands: [][]string{{"go", "test", "./..."}}, AmendmentRequest: &phaseartifact.AmendmentRequest{OldProofDigest: proofDigest("a"), ProposedDigest: proofDigest("b"), ProposedCommand: []string{"go", "test", "./..."}, Reason: "proof unconditionally fails; independently test FeatureValue equals 2"}}
	encoded, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	reviewer := `printf '%s\n' 'package example; import "testing"; func TestProof(t *testing.T) { t.Fatal("contradictory proof") }' > proof_test.go
`
	if accepted {
		reviewer += `if printf '%s' "$prompt" | grep -q '"prior_proof_digest"'; then
  evidence=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  printf '%s\n' 'package example; import "testing"; func TestProof(t *testing.T) { if FeatureValue() != 2 { t.Fatal("requires value 2") } }' > proof_test.go
fi
`
	}
	script = strings.Replace(script, "\t  printf '%s\\n' '{\"schema\":\"sf.verification/v1\"", reviewer+"\t  printf '%s\\n' '{\"schema\":\"sf.verification/v1\"", 1)
	script = strings.Replace(script, "func TestFeature(t *testing.T) {}", "func TestFeature(t *testing.T) {}\nfunc FeatureValue() int { return 1 }", 1)
	marker := "else\n\tprintf '%s\\n' '{\"schema\":\"sf.planner/v1\""
	builder := `if grep -q 'requires value 2' proof_test.go; then
  printf '%s\n' 'package example; import "testing"; func TestFeature(t *testing.T) {}; func FeatureValue() int { return 2 }' > tracked_test.go
elif printf '%s' "$prompt" | grep -q 'sf.postbuild_repair/v1'; then
  printf '%s\n' '` + string(encoded) + `' > "$last"
fi
`
	script = strings.Replace(script, marker, builder+marker, 1)
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
