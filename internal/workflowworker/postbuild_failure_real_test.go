package workflowworker

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
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

type realFailedPostbuildMaterializer struct {
	db    *store.Store
	calls int
	key   contracts.RepositoryCommandResultKey
}

func (m *realFailedPostbuildMaterializer) MaterializeCandidate(ctx context.Context, request PhaseRequest, _ workflowprompt.PlanIdentity, _ workflowprompt.VerificationIdentity, _ phaseartifact.Builder, provider store.ProviderAttemptResultKey) (CandidateWitness, error) {
	m.calls++
	key, err := recordRealFailedPostbuildCommand(ctx, m.db, request, provider)
	if err != nil {
		return CandidateWitness{}, err
	}
	result, err := m.db.LoadRepositoryCommandResult(ctx, key)
	if err != nil {
		return CandidateWitness{}, fmt.Errorf("reload failed post-build result: %w", err)
	}
	if !result.Result.Observed || result.Result.ExitCode == 0 {
		return CandidateWitness{}, errors.New("failed post-build result is not an observed nonzero exit")
	}
	m.key = key
	return CandidateWitness{}, fmt.Errorf("post-build exit %d: %w", result.Result.ExitCode, ErrPostbuildCommandFailed)
}

func TestRealStoreCompletedBuilderPostbuildFailureBlocksWithoutRepeat(t *testing.T) {
	fixture := newRealPlanningRecoveryFixture(t)
	defer fixture.db.Close()

	if _, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err == nil {
		t.Fatal("expected injected planner response loss")
	}
	fixture.recover(t)
	if result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err != nil || result.State != domain.StateVerifying {
		t.Fatalf("planner replay=%+v err=%v", result, err)
	}
	if result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence); err != nil || result.State != domain.StateBuilding {
		t.Fatalf("verification transition=%+v err=%v", result, err)
	}

	failed := &realFailedPostbuildMaterializer{db: fixture.db}
	fixture.worker.CandidateMaterializer = failed
	result, err := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence)
	if err != nil || !result.Transitioned || result.State != domain.StateBlocked {
		t.Fatalf("post-build disposition=%+v err=%v", result, err)
	}
	ticket, err := fixture.db.Ticket(fixture.ctx, fixture.ref)
	if err != nil || ticket.State != domain.StateBlocked || ticket.ResumeState != domain.StateBuilding || ticket.BlockedCode != "postbuild_command_failed" {
		t.Fatalf("blocked ticket=%+v err=%v", ticket, err)
	}
	command, err := fixture.db.LoadRepositoryCommandResult(fixture.ctx, failed.key)
	if err != nil || command.Result.ExitCode != 23 || !command.Result.Observed {
		t.Fatalf("failed command=%+v err=%v", command, err)
	}
	effect, err := fixture.db.Effect(fixture.ctx, failed.key.SemanticKey)
	if err != nil || effect.State != store.EffectFailed || effect.ClaimEpoch != failed.key.ClaimEpoch {
		t.Fatalf("failed effect=%+v err=%v", effect, err)
	}
	if _, err := fixture.db.LatestCandidate(fixture.ctx, fixture.ref); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("candidate exists after failed proof: %v", err)
	}
	attempts, err := fixture.db.ProviderAttempts(fixture.ctx, fixture.ref)
	if err != nil {
		t.Fatal(err)
	}
	buildAttempts := 0
	for _, attempt := range attempts {
		if attempt.Phase == domain.PhaseBuild {
			buildAttempts++
		}
	}
	if buildAttempts != 1 || fixture.runner.calls[domain.PhaseBuild] != 1 || failed.calls != 1 {
		t.Fatalf("Builder attempts=%d calls=%d materializations=%d", buildAttempts, fixture.runner.calls[domain.PhaseBuild], failed.calls)
	}

	replay, replayErr := fixture.worker.Run(fixture.ctx, fixture.ref, fixture.fence)
	if replayErr != nil || replay.Transitioned || replay.State != domain.StateBlocked || fixture.runner.calls[domain.PhaseBuild] != 1 || failed.calls != 1 {
		t.Fatalf("blocked replay=%+v err=%v Builder calls=%d materializations=%d", replay, replayErr, fixture.runner.calls[domain.PhaseBuild], failed.calls)
	}
}

func recordRealFailedPostbuildCommand(ctx context.Context, db *store.Store, request PhaseRequest, provider store.ProviderAttemptResultKey) (contracts.RepositoryCommandResultKey, error) {
	if db == nil || request.Verification == nil {
		return contracts.RepositoryCommandResultKey{}, errors.New("failed post-build fixture requires Store verification")
	}
	project, err := db.Project(ctx, request.Ticket.Ref.Channel, request.Ticket.Ref.Project)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	argv, err := json.Marshal([]string{"go", "test", "./..."})
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	commandDigest := "sha256:" + realDigest(string(argv))
	policy := "sha256:" + realDigest("failed-postbuild-policy")
	spec, executable := "sha256:"+strings.Repeat("2", 64), "sha256:"+strings.Repeat("3", 64)
	evidence := store.RepositoryCommandEvidenceRequest{
		Purpose:                  store.RepositoryCommandPurposePostbuildCandidate,
		Ref:                      request.Ticket.Ref,
		TicketVersion:            request.Ticket.Version,
		LeaderEpoch:              request.Fence.LeaderEpoch,
		RunnerEpoch:              request.Fence.RunnerEpoch,
		ProviderResult:           provider,
		VerificationIntentDigest: request.Verification.Revision.IntentDigest,
		ProofDigest:              request.Verification.Revision.ProofDigest,
		CheckpointID:             request.Verification.Revision.CheckpointID,
		ConfigCommandDigest:      commandDigest,
		Worktree:                 request.Worktree.Path,
		WorktreeIdentity:         string(request.Worktree.IdentityJSON),
		BaseSHA:                  request.Worktree.BaseSHA,
		PolicyDigest:             policy,
		SpecDigest:               spec,
		ExecutablePath:           "/usr/bin/false",
		ExecutableDigest:         executable,
	}
	_, requestDigest, err := store.CanonicalRepositoryCommandEvidenceRequest(evidence)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	semantic, err := store.RepositoryCommandEvidenceSemanticKey(evidence)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if _, err := db.PlanEffect(ctx, store.EffectPlan{SemanticKey: semantic, Ref: request.Ticket.Ref, Kind: "repository_command", TicketVersion: request.Ticket.Version, Fence: request.Fence, RequestDigest: requestDigest}); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	claim, err := db.IssueRepositoryCommandClaim(ctx, store.RepositoryCommandIntent{
		EffectFence:      store.EffectFence{SemanticKey: semantic, Ref: request.Ticket.Ref, TicketVersion: request.Ticket.Version, Fence: request.Fence},
		RequestDigest:    requestDigest,
		Repository:       project.Path,
		Worktree:         request.Worktree.Path,
		WorktreeIdentity: string(request.Worktree.IdentityJSON),
		Branch:           request.Worktree.Branch,
		BaseRef:          project.BaseRef,
		BaseSHA:          request.Worktree.BaseSHA,
		CommandDigest:    commandDigest,
		SpecDigest:       spec,
		PolicyDigest:     policy,
		ExecutablePath:   "/usr/bin/false",
		ExecutableDigest: executable,
	})
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	lease, err := db.AcquireRepositoryCommand(ctx, claim)
	if err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	launch := contracts.RepositoryCommandLaunch{PID: 657, PGID: 657, BootIdentity: "worker", ProcessStartIdentity: "worker-postbuild-failed"}
	if err := lease.RecordRepositoryCommandLaunch(ctx, launch); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if err := lease.FinishRepositoryCommandLaunch(ctx, launch); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if err := db.CompleteRepositoryCommand(ctx, claim, contracts.CommandResult{ExitCode: 23, Duration: time.Millisecond, Observed: true, ObservedAt: time.Now().UTC()}); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	if err := lease.Release(); err != nil {
		return contracts.RepositoryCommandResultKey{}, err
	}
	return contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}, nil
}
