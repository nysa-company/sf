package workflowruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func TestRepositoryMaterializerMissingStoreFailsClosed(t *testing.T) {
	var materializer RepositoryMaterializer
	request := workflowworker.PhaseRequest{Phase: "verification"}
	if _, err := materializer.MaterializeVerificationCheckpoint(context.Background(), request, phaseartifact.Verification{}, store.ProviderAttemptResultKey{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("verification materializer error=%v", err)
	}
	if err := materializer.AuthenticateVerificationCheckpoint(context.Background(), request, phaseartifact.Verification{}, workflowworker.VerificationCheckpoint{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("verification authenticator error=%v", err)
	}
	request.Phase = "build"
	if _, err := materializer.MaterializeCandidate(context.Background(), request, workflowprompt.PlanIdentity{}, workflowprompt.VerificationIdentity{}, phaseartifact.Builder{}, store.ProviderAttemptResultKey{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("candidate materializer error=%v", err)
	}
	if err := materializer.AuthenticateCandidate(context.Background(), request, workflowprompt.PlanIdentity{}, workflowprompt.VerificationIdentity{}, phaseartifact.Builder{}, workflowworker.CandidateWitness{}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("candidate authenticator error=%v", err)
	}
}

func TestCandidatePlanScopeRequiresExactNonEmptyPersistedScope(t *testing.T) {
	base := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"accept"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test", "./..."}, Details: "details"}, Paths: []string{"src"}, Commands: [][]string{{"go", "test", "./..."}}, Risks: []string{"none"}}
	supplied, err := workflowprompt.NewPlanIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := candidatePlanScope(base, supplied); err != nil || len(got) != 1 || got[0] != "src" {
		t.Fatalf("valid scope=%v err=%v", got, err)
	}
	for name, stored := range map[string]phaseartifact.Planner{
		"empty":   {Schema: base.Schema, Acceptance: base.Acceptance, Proof: base.Proof, Commands: base.Commands, Risks: base.Risks},
		"dot":     {Schema: base.Schema, Acceptance: base.Acceptance, Proof: base.Proof, Paths: []string{"."}, Commands: base.Commands, Risks: base.Risks},
		"invalid": {Schema: base.Schema, Acceptance: base.Acceptance, Proof: base.Proof, Paths: []string{"../outside"}, Commands: base.Commands, Risks: base.Risks},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := candidatePlanScope(stored, supplied); !errors.Is(err, ErrRepositoryMaterialization) {
				t.Fatalf("scope=%+v err=%v", stored.Paths, err)
			}
		})
	}
	tampered := supplied
	tampered.Plan.Paths = []string{"docs"}
	if _, err := candidatePlanScope(base, tampered); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("tampered plan accepted: %v", err)
	}
}

func TestCandidateChangedFilesMustBeSubsetOfPlanScope(t *testing.T) {
	if err := candidateChangedFilesWithinScope([]string{"src/main.go"}, []string{"src"}); err != nil {
		t.Fatalf("valid subset rejected: %v", err)
	}
	if err := candidateChangedFilesWithinScope([]string{"docs/readme.md"}, []string{"src"}); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("path escalation accepted: %v", err)
	}
	if err := candidateChangedFilesWithinScope([]string{"src/main.go"}, nil); !errors.Is(err, ErrRepositoryMaterialization) {
		t.Fatalf("empty scope accepted: %v", err)
	}
}
