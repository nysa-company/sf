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
