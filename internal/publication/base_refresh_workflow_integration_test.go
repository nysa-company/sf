package publication_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

var errBaseRefreshFreshBuilderInvoked = errors.New("base-refresh fixture invoked fresh builder")

// TestBaseRefreshBuildingInvokesFreshPhaseRunner composes a completed
// protected-base refresh with the production workflow worker. The old Builder
// result remains immutable predecessor provenance, but it must not be replayed
// on the new base. Reaching the sentinel at the PhaseRunner boundary proves the
// refreshed Building state starts a fresh Builder cycle.
func TestBaseRefreshBuildingInvokesFreshPhaseRunner(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	oldTicket, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldWorktree, err := f.db.Worktree(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldCandidate, err := f.db.HistoricalCandidate(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	baseRef := baseRefreshIntegrationWorktreeBaseRef(oldWorktree.Branch)
	runGit(t, oldWorktree.Path, "update-ref", baseRef, oldWorktree.BaseSHA)

	newBase := advanceBaseRefreshIntegrationMain(t, f.bare)
	assertBaseRefreshIntegrationRemote(t, f, oldWorktree, newBase)
	adapter := &baseRefreshIntegrationGit{store: f.db, runner: f.runner}
	refreshed, err := baseRefreshIntegrationWorker(f, adapter).Run(ctx, f.ref, domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: oldTicket.RunnerEpoch})
	if err != nil || refreshed.State != domain.StateBuilding || !refreshed.Transitioned {
		t.Fatalf("base refresh=%+v err=%v", refreshed, err)
	}
	if adapter.verifyCalls != 1 || adapter.prepareCalls != 1 || adapter.applyCalls != 1 || adapter.refTransactions != 1 {
		t.Fatalf("refresh calls verify=%d prepare=%d apply=%d ref_transactions=%d", adapter.verifyCalls, adapter.prepareCalls, adapter.applyCalls, adapter.refTransactions)
	}

	building, err := f.db.Ticket(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: f.fence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	refreshContext, err := f.db.ProtectedBaseRefreshBuildContext(ctx, f.ref, building.Version, fence)
	if err != nil {
		t.Fatalf("load protected-base refresh context: %v", err)
	}
	if !reflect.DeepEqual(refreshContext.Predecessor, oldCandidate) || refreshContext.Completion.Preparation != adapter.prepared {
		t.Fatalf("refresh context=%+v predecessor=%+v", refreshContext, oldCandidate)
	}
	if _, err := f.db.LatestReusableProviderAttempt(ctx, store.LatestReusableProviderAttemptRequest{
		Ref:             f.ref,
		Phase:           domain.PhaseBuild,
		Role:            "builder",
		ExpectedVersion: building.Version,
		Fence:           fence,
	}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("pre-refresh Builder remained reusable: %v", err)
	}

	spec, err := statemachine.LoadEmbeddedApproved()
	if err != nil {
		t.Fatal(err)
	}
	runner := &baseRefreshFreshBuilderSentinel{}
	worker := workflowworker.Worker{
		Evidence: f.db,
		Engine:   engine.New(f.db, spec),
		Runner:   runner,
	}
	result, err := worker.Run(ctx, f.ref, fence)
	if !errors.Is(err, errBaseRefreshFreshBuilderInvoked) {
		t.Fatalf("workflow result=%+v err=%v", result, err)
	}
	if runner.calls != 1 {
		t.Fatalf("fresh Builder calls=%d want=1", runner.calls)
	}
	if runner.request.Phase != domain.PhaseBuild || !reflect.DeepEqual(runner.request.Ticket, building) || runner.request.Fence != fence {
		t.Fatalf("fresh Builder request=%+v building=%+v fence=%+v", runner.request, building, fence)
	}
	if runner.request.Plan == nil || runner.request.Verification == nil || runner.request.Candidate != nil || runner.request.RecoveryVerification != nil || runner.request.Amendment != nil {
		t.Fatalf("fresh Builder evidence projection=%+v", runner.request)
	}
	expectedVerification := refreshContext.Verification
	expectedVerification.TicketVersion = building.Version
	expectedVerification.Fence = fence
	if !reflect.DeepEqual(*runner.request.Verification, expectedVerification) {
		t.Fatalf("fresh Builder verification=%+v want=%+v", *runner.request.Verification, expectedVerification)
	}
	if runner.request.Worktree.BaseSHA != newBase || runner.request.Worktree.HeadSHA != adapter.prepared.CommitOID || runner.request.Worktree.TicketVersion != building.Version || runner.request.Worktree.Fence != fence {
		t.Fatalf("fresh Builder worktree=%+v", runner.request.Worktree)
	}
	current, err := f.db.Ticket(ctx, f.ref)
	if err != nil || !reflect.DeepEqual(current, building) {
		t.Fatalf("sentinel changed ticket=%+v want=%+v err=%v", current, building, err)
	}
	if retained, err := f.db.HistoricalCandidate(ctx, f.ref); err != nil || !reflect.DeepEqual(retained, oldCandidate) {
		t.Fatalf("predecessor candidate changed=%+v err=%v", retained, err)
	}
	assertBaseRefreshIntegrationPhysicalState(t, runner.request.Worktree, adapter.prepared, newBase)
	assertBaseRefreshIntegrationNoPublicationMutation(t, f, oldWorktree, newBase)
}

type baseRefreshFreshBuilderSentinel struct {
	calls   int
	request workflowworker.PhaseRequest
}

func (r *baseRefreshFreshBuilderSentinel) Run(_ context.Context, request workflowworker.PhaseRequest) (workflowworker.PhaseResult, error) {
	r.calls++
	r.request = request
	return workflowworker.PhaseResult{}, errBaseRefreshFreshBuilderInvoked
}
