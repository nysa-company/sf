package workflowruntime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
	"github.com/nysa-company/sf/internal/workflowworker"
)

type phaseStore struct {
	project store.Project
	plan    store.StoredPlan
	verify  store.StoredVerification
	results map[int64]store.ProviderAttemptResult
	parsed  map[int64]phaseartifact.Parsed
}

func (s *phaseStore) Project(context.Context, domain.Channel, domain.ProjectID) (store.Project, error) {
	return s.project, nil
}
func (s *phaseStore) Plan(context.Context, domain.TicketRef) (store.StoredPlan, error) {
	return s.plan, nil
}
func (s *phaseStore) CurrentVerification(context.Context, domain.TicketRef) (store.StoredVerification, error) {
	return s.verify, nil
}
func (s *phaseStore) LoadHistoricalProviderAttemptResult(_ context.Context, key store.ProviderAttemptResultKey) (store.ProviderAttemptResult, phaseartifact.Parsed, error) {
	result, ok := s.results[key.AttemptID]
	if !ok {
		return store.ProviderAttemptResult{}, phaseartifact.Parsed{}, errors.New("missing provider result")
	}
	return result, s.parsed[key.AttemptID], nil
}

func phaseProviderResult(key store.ProviderAttemptResultKey, request workflowworker.PhaseRequest, role providercoord.Role) store.ProviderAttemptResult {
	return store.ProviderAttemptResult{AttemptID: key.AttemptID, RawArtifact: []byte(`{"durable":true}`), Claim: store.ProviderAttemptClaim{
		ID: key.AttemptID, Ref: key.Ref, Phase: key.Phase, Role: string(role), Attempt: key.Attempt,
		ExpectedVersion: request.Ticket.Version, LeaderEpoch: request.Fence.LeaderEpoch, RunnerEpoch: request.Fence.RunnerEpoch,
		Repository: "/repo", Worktree: request.Worktree.Path, WorktreeIdentity: string(request.Worktree.IdentityJSON), BaseSHA: request.Worktree.BaseSHA,
		Binding: contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "codex", Model: "m", Family: "f", Version: "v"}},
	}}
}

func bindCoordinatorResult(t *testing.T, coordinator *fakePlannerCoordinator, evidence *phaseStore, key store.ProviderAttemptResultKey) {
	t.Helper()
	coordinator.onRun = func(request providercoord.Request) {
		result := evidence.results[key.AttemptID]
		input := request.Input
		input.Provider = result.Claim.Binding.Identity
		input.AuthMode = result.Claim.Binding.AuthMode
		input.Attempt = key.Attempt
		input.LeaderEpoch = result.Claim.LeaderEpoch
		input.RunnerEpoch = result.Claim.RunnerEpoch
		input.ExpectedVersion = result.Claim.ExpectedVersion
		payload, digest, err := contracts.CanonicalPhaseInput(input)
		if err != nil {
			t.Fatalf("canonical provider input: %v", err)
		}
		input.RequestDigest = digest
		validation, _, err := phaseartifact.CanonicalValidation(request.Validation)
		if err != nil {
			t.Fatalf("canonical validation: %v", err)
		}
		result.Claim.Input, result.Claim.RequestDigest, result.Claim.RequestPayload, result.Validation = input, digest, payload, validation
		evidence.results[key.AttemptID] = result
	}
}

func phaseFixture(t *testing.T) (workflowworker.PhaseRequest, *phaseStore, *fakePlannerCoordinator, workflowprompt.PlanIdentity, workflowprompt.VerificationIdentity) {
	t.Helper()
	request, base, coordinator := plannerFixture(t)
	planner := *base.parsed.Planner
	planner.Paths = []string{"internal/feature.go"}
	planner.Risks = []string{"test coverage"}
	planIdentity, err := workflowprompt.NewPlanIdentity(planner)
	if err != nil {
		t.Fatal(err)
	}
	planKey := store.ProviderAttemptResultKey{AttemptID: base.result.AttemptID, Ref: request.Ticket.Ref, Phase: domain.PhasePlanning, Attempt: 1}
	planResult := phaseProviderResult(planKey, request, providercoord.RolePlanner)
	verification := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"internal/feature_test.go"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "missing", EvidenceDigest: "evidence"}
	intent, err := workflowprompt.VerificationIntentDigest(verification)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := workflowprompt.VerificationProofDigest(verification)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := "dddddddddddddddddddddddddddddddddddddddd"
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(verification, intent, proof, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	verificationKey := store.ProviderAttemptResultKey{AttemptID: 12, Ref: request.Ticket.Ref, Phase: domain.PhaseVerification, Attempt: 1}
	verificationResult := phaseProviderResult(verificationKey, request, providercoord.RoleReviewer)
	return request, &phaseStore{
		project: base.project,
		plan:    store.StoredPlan{Digest: planIdentity.Digest, Document: store.PlanDocument{Planner: &planner, ProviderResult: &planKey, Acceptance: planner.Acceptance, ProofKind: string(planner.Proof.Kind), Paths: planner.Paths, Commands: planner.Commands, Risks: planner.Risks}, TicketVersion: request.Ticket.Version, Fence: request.Fence},
		verify:  store.StoredVerification{Revision: store.VerificationRevision{Revision: 1, IntentDigest: intent, ProofDigest: proof, OwnedFiles: verification.OwnedFiles, CheckpointID: checkpoint}, TicketVersion: request.Ticket.Version, Fence: request.Fence, ProviderResult: verificationKey, Checkpoint: store.CommitObservation{CommitOID: checkpoint, ParentOID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", TreeOID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}},
		results: map[int64]store.ProviderAttemptResult{planKey.AttemptID: planResult, verificationKey.AttemptID: verificationResult},
		parsed:  map[int64]phaseartifact.Parsed{planKey.AttemptID: {Phase: domain.PhasePlanning, Provider: planResult.Claim.Binding.Identity, Planner: &planner}, verificationKey.AttemptID: {Phase: domain.PhaseVerification, Provider: verificationResult.Claim.Binding.Identity, Verify: &verification}},
	}, coordinator, planIdentity, verificationIdentity
}

func TestPhaseRunnerVerificationUsesStoredPlannerWitness(t *testing.T) {
	request, evidence, coordinator, plan, _ := phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseVerification, domain.StateVerifying
	key := store.ProviderAttemptResultKey{AttemptID: 13, Ref: request.Ticket.Ref, Phase: domain.PhaseVerification, Attempt: 2}
	result := phaseProviderResult(key, request, providercoord.RoleReviewer)
	artifact := evidence.parsed[12].Verify
	evidence.results[key.AttemptID] = result
	evidence.parsed[key.AttemptID] = phaseartifact.Parsed{Phase: domain.PhaseVerification, Provider: result.Claim.Binding.Identity, Verify: artifact}
	coordinator.result = providercoord.Result{Code: providercoord.Completed, ProviderResult: key, Parsed: &phaseartifact.Parsed{Phase: domain.PhaseBuild}}
	bindCoordinatorResult(t, coordinator, evidence, key)

	out, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request)
	if err != nil || out.ProviderResult != key {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if coordinator.request.Role != providercoord.RoleReviewer || coordinator.request.Input.Phase != domain.PhaseVerification || !reflect.DeepEqual(coordinator.request.Input.AllowedPaths, plan.Plan.Paths) || !reflect.DeepEqual(coordinator.request.Validation, phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: plan.Digest}) {
		t.Fatalf("request=%+v", coordinator.request)
	}
}

func TestPhaseRunnerBuildUsesExactVerificationAndRejectsRefusals(t *testing.T) {
	request, evidence, coordinator, plan, verification := phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseBuild, domain.StateBuilding
	key := store.ProviderAttemptResultKey{AttemptID: 14, Ref: request.Ticket.Ref, Phase: domain.PhaseBuild, Attempt: 1}
	result := phaseProviderResult(key, request, providercoord.RoleBuilder)
	builder := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "implemented", ChangedFiles: []string{"internal/feature.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	evidence.results[key.AttemptID] = result
	evidence.parsed[key.AttemptID] = phaseartifact.Parsed{Phase: domain.PhaseBuild, Provider: result.Claim.Binding.Identity, Builder: &builder}
	coordinator.result = providercoord.Result{Code: providercoord.Completed, ProviderResult: key}
	bindCoordinatorResult(t, coordinator, evidence, key)

	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	want := phaseartifact.Validation{TicketType: request.Ticket.Type, AcceptanceDigest: plan.Digest, ProtectedVerification: verification.OwnedFiles}
	if coordinator.request.Role != providercoord.RoleBuilder || !reflect.DeepEqual(coordinator.request.Input.AllowedPaths, plan.Plan.Paths) || !reflect.DeepEqual(coordinator.request.Validation, want) {
		t.Fatalf("request=%+v", coordinator.request)
	}

	request, evidence, coordinator, _, _ = phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseBuild, domain.StateBuilding
	request.Ticket.ConfigSnapshot[0] ^= 1
	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); !errors.Is(err, ErrConfigDigestMismatch) || coordinator.calls != 0 {
		t.Fatalf("tampered config err=%v calls=%d", err, coordinator.calls)
	}
	request, evidence, coordinator, _, _ = phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseBuild, domain.StateBuilding
	evidence.plan.Document.Planner.Paths = []string{"."}
	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); !errors.Is(err, ErrProviderResultInvalid) || coordinator.calls != 0 {
		t.Fatalf("tampered plan err=%v calls=%d", err, coordinator.calls)
	}
}

func TestPhaseRunnerRefusesCoordinatorKeyWithDifferentLaunchInput(t *testing.T) {
	request, evidence, coordinator, _, _ := phaseFixture(t)
	request.Phase, request.Ticket.State = domain.PhaseVerification, domain.StateVerifying
	key := store.ProviderAttemptResultKey{AttemptID: 15, Ref: request.Ticket.Ref, Phase: domain.PhaseVerification, Attempt: 3}
	result := phaseProviderResult(key, request, providercoord.RoleReviewer)
	evidence.results[key.AttemptID] = result
	evidence.parsed[key.AttemptID] = phaseartifact.Parsed{Phase: domain.PhaseVerification, Provider: result.Claim.Binding.Identity, Verify: evidence.parsed[12].Verify}
	coordinator.result = providercoord.Result{Code: providercoord.Completed, ProviderResult: key}
	bindCoordinatorResult(t, coordinator, evidence, key)
	bind := coordinator.onRun
	coordinator.onRun = func(input providercoord.Request) {
		bind(input)
		stored := evidence.results[key.AttemptID]
		stored.Claim.Input.Prompt = "substituted historical prompt"
		evidence.results[key.AttemptID] = stored
	}
	if _, err := (PhaseRunner{Store: evidence, Coordinator: coordinator}).Run(context.Background(), request); !errors.Is(err, ErrProviderResultInvalid) {
		t.Fatalf("mismatched durable input err=%v", err)
	}
}
