package workflowworker

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

type failedPostbuildMaterializer struct{ calls int }

func (m *failedPostbuildMaterializer) MaterializeCandidate(context.Context, PhaseRequest, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity, phaseartifact.Builder, store.ProviderAttemptResultKey) (CandidateWitness, error) {
	m.calls++
	return CandidateWitness{}, fmt.Errorf("fixture terminal result: %w", ErrPostbuildCommandFailed)
}

func TestCompletedBuilderPostbuildFailureBlocksOnceAndPreservesEvidence(t *testing.T) {
	evidence := &fakeEvidence{
		hasPlan: true,
		plan: store.StoredPlan{Document: store.PlanDocument{
			Acceptance: []string{"accept"},
			ProofKind:  "regression",
			Paths:      []string{"internal"},
			Commands:   [][]string{{"go", "test", "./..."}},
		}},
	}
	stateMachine := &fakeEngine{}
	verificationRunner := &fakeRunner{outputs: []fakePhase{verificationOutput()}}
	worker := newWorker(domain.StateVerifying, verificationRunner, evidence, stateMachine)
	worker.Checkpoint = fakeCheckpoint{}
	if _, err := worker.Run(context.Background(), testRef, testFence); err != nil {
		t.Fatalf("verification: %v", err)
	}
	if stateMachine.state.State != domain.StateBuilding || evidence.verifications != 1 {
		t.Fatalf("verification state=%s records=%d", stateMachine.state.State, evidence.verifications)
	}

	builderRunner := &fakeRunner{outputs: []fakePhase{builderOutput(false)}, evidence: evidence}
	failure := &failedPostbuildMaterializer{}
	worker.Runner = builderRunner
	worker.CandidateMaterializer = failure
	got, err := worker.Run(context.Background(), testRef, testFence)
	if err != nil || !got.Transitioned || got.State != domain.StateBlocked {
		t.Fatalf("post-build result=%+v err=%v", got, err)
	}
	if stateMachine.signals != 2 || stateMachine.last.Trigger != "typed_blocker" || stateMachine.last.From != domain.StateBuilding || stateMachine.last.Attributes["no_unreconciled_external_mutation"] != "true" || !strings.Contains(stateMachine.last.EventPayload, `"code":"postbuild_command_failed"`) {
		t.Fatalf("post-build signal=%+v calls=%d", stateMachine.last, stateMachine.signals)
	}
	if failure.calls != 1 || len(builderRunner.requests) != 1 || evidence.candidates != 0 {
		t.Fatalf("materializations=%d builder calls=%d candidate records=%d", failure.calls, len(builderRunner.requests), evidence.candidates)
	}
	if retained, ok := evidence.providerResults[builderOutput(false).result.ProviderResult.AttemptID]; !ok || retained.Builder == nil {
		t.Fatalf("completed Builder was not retained: %+v", evidence.providerResults)
	}

	replay, replayErr := worker.Run(context.Background(), testRef, testFence)
	if replayErr != nil || replay.Transitioned || replay.State != domain.StateBlocked || failure.calls != 1 || len(builderRunner.requests) != 1 || stateMachine.signals != 2 {
		t.Fatalf("blocked replay=%+v err=%v materializations=%d builder calls=%d signals=%d", replay, replayErr, failure.calls, len(builderRunner.requests), stateMachine.signals)
	}
}
