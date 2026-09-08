package workflowworker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

type optionalRepairMaterializer struct {
	fakeCandidateMaterializer
	calls      int
	err        error
	gotKey     store.ProviderAttemptResultKey
	gotCommand contracts.RepositoryCommandResultKey
}

func (m *optionalRepairMaterializer) PreparePostbuildRepair(_ context.Context, request PhaseRequest, key store.ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey) (store.PostbuildRepairRequest, error) {
	m.calls++
	m.gotKey, m.gotCommand = key, command
	return store.PostbuildRepairRequest{PostbuildFailureRequest: store.PostbuildFailureRequest{Ref: request.Ticket.Ref, ExpectedVersion: request.Ticket.Version, Fence: request.Fence, BuilderResult: key, CommandResult: command}, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)}, m.err
}

type optionalRepairTestEngine struct {
	StateMachine
	calls int
	err   error
	got   store.PostbuildRepairRequest
	apply func(context.Context, store.PostbuildRepairRequest) (contracts.TransitionResult, error)
}

func (e *optionalRepairTestEngine) SignalPostbuildRepair(ctx context.Context, request store.PostbuildRepairRequest) (contracts.TransitionResult, error) {
	e.calls++
	e.got = request
	if e.apply != nil {
		return e.apply(ctx, request)
	}
	return contracts.TransitionResult{}, e.err
}

func TestOptionalPostbuildRepairPreservesFailureOnUncertainty(t *testing.T) {
	for _, name := range []string{"success", "missing_physical", "missing_engine", "sentinel_only", "unrelated_error", "physical_refusal", "store_refusal"} {
		t.Run(name, func(t *testing.T) {
			key := builderOutput(false).result.ProviderResult
			command := contracts.RepositoryCommandResultKey{SemanticKey: "failed-command", ClaimEpoch: 1}
			var original error = fmt.Errorf("wrapped: %w", &PostbuildFailure{CommandResult: command})
			materializer := &optionalRepairMaterializer{}
			engine := &optionalRepairTestEngine{}
			runner := &fakeRunner{}
			worker := Worker{CandidateMaterializer: materializer, Engine: engine, Runner: runner}
			wantPrepare, wantSignal := 1, 1
			switch name {
			case "missing_physical":
				worker.CandidateMaterializer = fakeCandidateMaterializer{}
				wantPrepare, wantSignal = 0, 0
			case "missing_engine":
				worker.Engine = &fakeEngine{}
				wantPrepare, wantSignal = 0, 0
			case "sentinel_only":
				original = ErrPostbuildCommandFailed
				wantPrepare, wantSignal = 0, 0
			case "unrelated_error":
				original = errors.New("command not observed")
				wantPrepare, wantSignal = 0, 0
			case "physical_refusal":
				materializer.err = store.ErrEvidenceConflict
				wantSignal = 0
			case "store_refusal":
				engine.err = store.ErrStaleFence
			}
			request := PhaseRequest{Ticket: ticket(domain.StateBuilding), Fence: testFence}
			got := worker.tryPostbuildRepair(context.Background(), request, key, original)
			if name == "success" {
				if !errors.Is(got, errPostbuildRepairStarted) || engine.got.BuilderResult != key || engine.got.CommandResult != command || engine.got.Ref != request.Ticket.Ref || engine.got.ExpectedVersion != request.Ticket.Version || engine.got.Fence != request.Fence {
					t.Fatal("successful marker lost exact request binding")
				}
			} else if got != original {
				t.Fatalf("original failure was replaced: %v", got)
			}
			if materializer.calls != wantPrepare || engine.calls != wantSignal || len(runner.requests) != 0 {
				t.Fatalf("prepare=%d signal=%d launches=%d", materializer.calls, engine.calls, len(runner.requests))
			}
		})
	}
}

type realOptionalRepairMaterializer struct {
	*realFailedPostbuildMaterializer
	prepareCalls int
}

func (m *realOptionalRepairMaterializer) MaterializeCandidate(ctx context.Context, request PhaseRequest, plan workflowprompt.PlanIdentity, verification workflowprompt.VerificationIdentity, builder phaseartifact.Builder, key store.ProviderAttemptResultKey) (CandidateWitness, error) {
	witness, err := m.realFailedPostbuildMaterializer.MaterializeCandidate(ctx, request, plan, verification, builder, key)
	if err != nil && m.key.SemanticKey != "" {
		return CandidateWitness{}, &PostbuildFailure{CommandResult: m.key}
	}
	return witness, err
}

func (m *realOptionalRepairMaterializer) PreparePostbuildRepair(ctx context.Context, request PhaseRequest, key store.ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey) (store.PostbuildRepairRequest, error) {
	m.prepareCalls++
	value := store.PostbuildRepairRequest{PostbuildFailureRequest: store.PostbuildFailureRequest{Ref: request.Ticket.Ref, ExpectedVersion: request.Ticket.Version, Fence: request.Fence, BuilderResult: key, CommandResult: command}, RetainedWorktreeDigest: "sha256:" + strings.Repeat("a", 64)}
	_, err := m.db.AuthenticatePostbuildFailure(ctx, value.PostbuildFailureRequest)
	// This adapter stands in for the optional physical boundary. This test
	// proves orchestration/Store fencing, not physical dirty-file admission.
	return value, err
}

func TestWorkerPostbuildRepairTransitionAndLostResponseDoNotRelaunch(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_response_%t", lost), func(t *testing.T) {
			fixture := newRealPlanningRecoveryFixture(t)
			defer fixture.db.Close()
			if _, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err == nil {
				t.Fatal("expected injected planner response loss")
			}
			fixture.recover(t)
			if result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err != nil || result.State != domain.StateVerifying {
				t.Fatalf("plan replay=%+v err=%v", result, err)
			}
			if result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err != nil || result.State != domain.StateBuilding {
				t.Fatalf("verification=%+v err=%v", result, err)
			}
			before, err := fixture.db.Ticket(fixture.ctx, fixture.ref)
			if err != nil {
				t.Fatal(err)
			}
			materializer := &realOptionalRepairMaterializer{realFailedPostbuildMaterializer: &realFailedPostbuildMaterializer{db: fixture.db}}
			engine := &optionalRepairTestEngine{StateMachine: fixture.worker.Engine}
			engine.apply = func(ctx context.Context, request store.PostbuildRepairRequest) (contracts.TransitionResult, error) {
				result, err := fixture.machine.SignalPostbuildRepair(ctx, request)
				if err == nil && lost {
					return contracts.TransitionResult{}, errors.New("injected post-commit response loss")
				}
				return result, err
			}
			fixture.worker.Engine, fixture.worker.CandidateMaterializer = engine, materializer
			result, runErr := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence)
			if !lost && (runErr != nil || !result.Transitioned || result.State != domain.StateBuilding) {
				t.Fatalf("repair=%+v err=%v", result, runErr)
			}
			if lost && runErr == nil {
				t.Fatal("lost response should not claim successful observation")
			}
			after, err := fixture.db.Ticket(fixture.ctx, fixture.ref)
			if err != nil || after.State != domain.StateBuilding || after.Version != before.Version+1 {
				t.Fatalf("repair state=%+v err=%v runErr=%v", after, err, runErr)
			}
			if engine.calls != 1 || materializer.prepareCalls != 1 || materializer.calls != 1 || fixture.runner.calls[domain.PhaseBuild] != 1 {
				t.Fatalf("signal=%d prepare=%d command=%d launches=%d", engine.calls, materializer.prepareCalls, materializer.calls, fixture.runner.calls[domain.PhaseBuild])
			}
			if _, err := fixture.db.LatestCandidate(fixture.ctx, fixture.ref); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("candidate persisted after failed command: %v", err)
			}
		})
	}
}
