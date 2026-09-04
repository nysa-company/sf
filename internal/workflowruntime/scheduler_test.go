package workflowruntime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type fakeTickets struct {
	tickets []store.Ticket
	err     error
}

type currentFakeTickets struct {
	fakeTickets
	current store.Ticket
}

type mergeReadyFakeTickets struct {
	fakeTickets
	ready bool
	err   error
}

type runtimeReadyMergeTickets struct {
	mergeReadyFakeTickets
	runtimeReady bool
	runtimeErr   error
}

type sourceResumeProofTickets struct {
	fakeTickets
	proof     store.OperatorSourceResumeProof
	found     bool
	err       error
	fresh     bool
	freshErr  error
	repair    *store.CandidateRepairBuildContext
	repairErr error
}

func (f mergeReadyFakeTickets) MergeReconciliationReady(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error) {
	return f.ready, f.err
}

func (f runtimeReadyMergeTickets) RuntimeAdmissionReady(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error) {
	return f.runtimeReady, f.runtimeErr
}

func (f sourceResumeProofTickets) OperatorSourceResumeProof(context.Context, domain.TicketRef, uint64, domain.Fence) (store.OperatorSourceResumeProof, bool, error) {
	return f.proof, f.found, f.err
}

func (f sourceResumeProofTickets) OperatorSourceResumeRequiresFreshVerification(context.Context, domain.TicketRef, uint64) (bool, error) {
	return f.fresh, f.freshErr
}

func (f sourceResumeProofTickets) CandidateRepairBuildContext(context.Context, domain.TicketRef, uint64, domain.Fence) (store.CandidateRepairBuildContext, error) {
	if f.repairErr != nil {
		return store.CandidateRepairBuildContext{}, f.repairErr
	}
	if f.repair == nil {
		return store.CandidateRepairBuildContext{}, store.ErrNotFound
	}
	return *f.repair, nil
}

func (f currentFakeTickets) Ticket(context.Context, domain.TicketRef) (store.Ticket, error) {
	return f.current, nil
}

func (f fakeTickets) ListTickets(context.Context, domain.Channel) ([]store.Ticket, error) {
	return f.tickets, f.err
}

func (f fakeTickets) Ticket(_ context.Context, ref domain.TicketRef) (store.Ticket, error) {
	for _, ticket := range f.tickets {
		if ticket.Ref == ref {
			return ticket, nil
		}
	}
	return store.Ticket{}, store.ErrNotFound
}

func (f fakeTickets) RuntimeAdmissionReady(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error) {
	return true, nil
}

type fakeEnsure struct {
	calls []worktreecoord.EnsureRequest
	err   error
}

type fakeSourceResumeEnsure struct {
	fakeEnsure
	proofCalls  []store.OperatorSourceResumeProof
	proofResult store.StoredWorktree
	proofErr    error
}

type blockingSourceResumeEnsure struct {
	fakeSourceResumeEnsure
	entered chan struct{}
	release chan struct{}
}

func (f *fakeSourceResumeEnsure) AuthenticateOperatorSourceResume(_ context.Context, _ worktreecoord.EnsureRequest, proof store.OperatorSourceResumeProof) (store.StoredWorktree, error) {
	f.proofCalls = append(f.proofCalls, proof)
	if f.proofErr != nil {
		return store.StoredWorktree{}, f.proofErr
	}
	return f.proofResult, nil
}

func (f *blockingSourceResumeEnsure) AuthenticateOperatorSourceResume(_ context.Context, _ worktreecoord.EnsureRequest, proof store.OperatorSourceResumeProof) (store.StoredWorktree, error) {
	f.proofCalls = append(f.proofCalls, proof)
	close(f.entered)
	<-f.release
	return f.proofResult, nil
}

func (f *fakeEnsure) Ensure(_ context.Context, request worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	f.calls = append(f.calls, request)
	if f.err != nil {
		return store.StoredWorktree{}, f.err
	}
	return store.StoredWorktree{Path: "/tmp/wt", State: "registered"}, nil
}

type fakeWorker struct {
	calls []domain.TicketRef
	err   error
}

func (f *fakeWorker) Run(_ context.Context, ref domain.TicketRef, _ domain.Fence) (workflowworker.RunResult, error) {
	f.calls = append(f.calls, ref)
	return workflowworker.RunResult{Ref: ref}, f.err
}

func ticket(ref domain.TicketRef, state domain.State) store.Ticket {
	return store.Ticket{Ref: ref, State: state, Version: 4, RunnerEpoch: 7}
}

func TestSchedulerIgnoresQueuedAndInvokesOneStableFirstTicket(t *testing.T) {
	queued := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "queued"}, domain.StateQueued)
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "b", Ticket: "second"}, domain.StatePlanning)
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StateVerifying)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{queued, second, first}}, ensurer, worker)
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9, RunnerEpoch: 7})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != first.Ref {
		t.Fatalf("result=%+v calls=%v", result, worker.calls)
	}
	if len(ensurer.calls) != 1 || ensurer.calls[0].Ref != first.Ref || ensurer.calls[0].Version != first.Version || ensurer.calls[0].Fence.LeaderEpoch != 9 {
		t.Fatalf("ensure calls=%+v", ensurer.calls)
	}
}

func TestSchedulerInvokesReviewingTicket(t *testing.T) {
	reviewing := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "review"}, domain.StateReviewing)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{reviewing}}, ensurer, worker)
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != reviewing.Ref {
		t.Fatalf("result=%+v calls=%v", result, worker.calls)
	}
}

func TestSchedulerDoesNotRunSecondPhaseAndBusyIsBenign(t *testing.T) {
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StatePlanning)
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "second"}, domain.StateBuilding)
	ensurer := &fakeEnsure{err: store.ErrBusy}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{second, first}}, ensurer, worker)
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1, RunnerEpoch: 7})
	if result.Outcome != OutcomeBusy || len(worker.calls) != 0 || len(ensurer.calls) != 2 {
		t.Fatalf("result=%+v ensure=%d worker=%d", result, len(ensurer.calls), len(worker.calls))
	}
}

func TestSchedulerAdmitsPublishingAndPollsWaitingCIWithoutAWorktree(t *testing.T) {
	publishing := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "publishing"}, domain.StatePublishing)
	waiting := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "b", Ticket: "waiting"}, domain.StateWaitingCI)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{waiting, publishing}}, ensurer, worker)
	fence := domain.Fence{LeaderEpoch: 9}
	first := scheduler.Tick(context.Background(), fence)
	if first.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != publishing.Ref {
		t.Fatalf("first result=%+v calls=%v", first, worker.calls)
	}
	if len(ensurer.calls) != 1 || ensurer.calls[0].Ref != publishing.Ref {
		t.Fatalf("first ensure calls=%v", ensurer.calls)
	}
	ciEnsurer := &fakeEnsure{}
	ciWorker := &fakeWorker{}
	poller := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{waiting}}, ciEnsurer, ciWorker)
	result := poller.Tick(context.Background(), fence)
	if result.Outcome != OutcomeInvoked || len(ciWorker.calls) != 1 || ciWorker.calls[0] != waiting.Ref || len(ciEnsurer.calls) != 0 {
		t.Fatalf("waiting_ci result=%+v worker=%v ensures=%v", result, ciWorker.calls, ciEnsurer.calls)
	}
}

func TestSchedulerPrePublishingAdmissionInvokesBlockerWithoutWorktree(t *testing.T) {
	publishing := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "publishing"}, domain.StatePublishing)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{publishing}}, ensurer, worker)
	scheduler.AdmitPublishing = false
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || len(ensurer.calls) != 0 {
		t.Fatalf("pre-publishing result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerObservesManualMergeWithoutWorktree(t *testing.T) {
	manual := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "manual"}, domain.StateWaitingManualMerge)
	ensurer := &fakeEnsure{err: store.ErrNotFound}
	worker := &fakeWorker{}
	result := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{manual}}, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != manual.Ref || len(ensurer.calls) != 0 {
		t.Fatalf("manual result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerObservesExternalMergeWhileWaitingApprovalWithoutWorktree(t *testing.T) {
	waiting := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "approval"}, domain.StateWaitingApproval)
	ensurer := &fakeEnsure{err: store.ErrNotFound}
	worker := &fakeWorker{}
	result := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{waiting}}, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != waiting.Ref || len(ensurer.calls) != 0 {
		t.Fatalf("waiting approval result=%+v worker=%v ensures=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerReconcilesObservedMergeWithoutWorktree(t *testing.T) {
	reconciling := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "reconciling"}, domain.StateReconciling)
	ensurer := &fakeEnsure{err: store.ErrNotFound}
	worker := &fakeWorker{}
	result := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{reconciling}}, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != reconciling.Ref || len(ensurer.calls) != 0 {
		t.Fatalf("reconciling result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerReconcilesSettledMergeWithoutUnavailableWorktree(t *testing.T) {
	merging := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "settled-merge"}, domain.StateMerging)
	ensurer := &fakeEnsure{err: store.ErrNotFound}
	worker := &fakeWorker{}
	source := mergeReadyFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{merging}}, ready: true}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != merging.Ref || len(ensurer.calls) != 0 {
		t.Fatalf("settled merge result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerRunsAuthenticatedOperatorSourceResumeWithoutPristineEnsure(t *testing.T) {
	building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "operator-source-resume"}, domain.StateBuilding)
	proof := store.OperatorSourceResumeProof{Ref: building.Ref, Version: building.Version, Fence: domain.Fence{LeaderEpoch: 9, RunnerEpoch: building.RunnerEpoch}, SourceCommit: contracts.OperatorSourceCommit{CommitOID: strings.Repeat("a", 40), ParentOID: strings.Repeat("b", 40), TreeOID: strings.Repeat("c", 40), Changes: []contracts.OperatorSourceChange{{Status: "M", Path: "internal/feature.go"}}}}
	ensurer := &fakeSourceResumeEnsure{fakeEnsure: fakeEnsure{err: worktreecoord.ErrQuarantined}, proofResult: store.StoredWorktree{Path: "/tmp/source", State: "registered"}}
	worker := &fakeWorker{}
	source := sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, proof: proof, found: true}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != building.Ref || len(ensurer.calls) != 0 || len(ensurer.proofCalls) != 1 || result.Worktree.Path != ensurer.proofResult.Path || result.Worktree.State != ensurer.proofResult.State {
		t.Fatalf("authenticated source resume=%+v worker=%v ensure=%v proof=%v", result, worker.calls, ensurer.calls, ensurer.proofCalls)
	}
}

func TestSchedulerRetainsPristineEnsureWhenOperatorSourceResumeIsNotProven(t *testing.T) {
	building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "unproven-source-resume"}, domain.StateBuilding)
	for _, value := range []struct {
		name  string
		found bool
	}{
		{name: "proof absent"},
	} {
		t.Run(value.name, func(t *testing.T) {
			ensurer := &fakeEnsure{err: worktreecoord.ErrInProgress}
			worker := &fakeWorker{}
			source := sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, found: value.found}
			result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
			if result.Outcome != OutcomeInProgress || !errors.Is(result.Err, ErrInProgress) || len(worker.calls) != 0 || len(ensurer.calls) != 1 {
				t.Fatalf("unproven source resume=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
			}
		})
	}
}

func TestSchedulerNeverFallsBackToPristineEnsureWhenSourceFreshnessNeedsProof(t *testing.T) {
	verifying := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "fresh-source-proof-missing"}, domain.StateVerifying)
	ensurer := &fakeEnsure{err: worktreecoord.ErrInProgress}
	worker := &fakeWorker{}
	source := sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{verifying}}, fresh: true}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeReadiness || !errors.Is(result.Err, ErrReadiness) || len(worker.calls) != 0 || len(ensurer.calls) != 0 {
		t.Fatalf("missing required source proof fell back to Ensure: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerFailsClosedWhenOperatorSourceResumeProofErrors(t *testing.T) {
	building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "source-proof-error"}, domain.StateBuilding)
	ensurer := &fakeEnsure{err: worktreecoord.ErrInProgress}
	worker := &fakeWorker{}
	source := sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, err: store.ErrBusy}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeBusy || len(worker.calls) != 0 || len(ensurer.calls) != 0 {
		t.Fatalf("proof error bypassed source handoff safety: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerClassifiesCandidateRepairBeforeHistoricalSourceResume(t *testing.T) {
	building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "source-resume-ci-repair"}, domain.StateBuilding)
	repair := store.CandidateRepairBuildContext{Ref: building.Ref, TargetGeneration: 2, PredecessorGeneration: 1}
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	// If Scheduler incorrectly applies the initial source-resume proof to this
	// later repair, the injected error would stop the tick before normal Ensure.
	source := sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, err: store.ErrBusy, repair: &repair}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(ensurer.calls) != 1 || len(worker.calls) != 1 || worker.calls[0] != building.Ref {
		t.Fatalf("candidate repair was classified as initial source resume: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerFailsClosedOnMalformedCandidateRepairBeforeSourceFallback(t *testing.T) {
	building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "malformed-source-resume-ci-repair"}, domain.StateBuilding)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	source := sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, found: true, repairErr: store.ErrEvidenceConflict}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeReadiness || !errors.Is(result.Err, ErrReadiness) || len(ensurer.calls) != 0 || len(worker.calls) != 0 {
		t.Fatalf("malformed repair fell through to source proof: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerDoesNotInvokeWorkerWhenSourceResumeAuthenticationIsStopped(t *testing.T) {
	building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "source-resume-stop"}, domain.StateBuilding)
	proof := store.OperatorSourceResumeProof{Ref: building.Ref, Version: building.Version, Fence: domain.Fence{LeaderEpoch: 9, RunnerEpoch: building.RunnerEpoch}, SourceCommit: contracts.OperatorSourceCommit{CommitOID: strings.Repeat("a", 40), ParentOID: strings.Repeat("b", 40), TreeOID: strings.Repeat("c", 40), Changes: []contracts.OperatorSourceChange{{Status: "M", Path: "internal/feature.go"}}}}
	ensurer := &blockingSourceResumeEnsure{
		fakeSourceResumeEnsure: fakeSourceResumeEnsure{proofResult: store.StoredWorktree{Path: "/tmp/source", State: "registered"}},
		entered:                make(chan struct{}),
		release:                make(chan struct{}),
	}
	worker := &fakeWorker{}
	scheduler := NewScheduler(domain.ChannelDev, sourceResumeProofTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}, proof: proof, found: true}, ensurer, worker)
	stopped := make(chan struct{})
	scheduler.admission.afterStop = func() { close(stopped) }
	resultCh := make(chan TickResult, 1)
	go func() { resultCh <- scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9}) }()
	select {
	case <-ensurer.entered:
	case <-time.After(time.Second):
		t.Fatal("source-resume authenticator was not invoked")
	}
	stopCh := make(chan error, 1)
	go func() { stopCh <- scheduler.admission.Stop(context.Background(), building.Ref) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel source-resume authentication")
	}
	close(ensurer.release)
	select {
	case err := <-stopCh:
		if err != nil {
			t.Fatalf("stop=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stop did not join source-resume authentication")
	}
	select {
	case result := <-resultCh:
		if result.Outcome != OutcomeCanceled || !errors.Is(result.Err, ErrCanceled) || len(worker.calls) != 0 || len(ensurer.calls) != 0 {
			t.Fatalf("stopped source resume invoked work: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
		}
	case <-time.After(time.Second):
		t.Fatal("scheduler tick did not return after source-resume stop")
	}
}

func TestSchedulerNeverConsumesSealedMergeBeforeRuntimeRearm(t *testing.T) {
	merging := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "sealed-merge"}, domain.StateMerging)
	ensurer := &fakeEnsure{}
	worker := &fakeWorker{}
	source := runtimeReadyMergeTickets{
		mergeReadyFakeTickets: mergeReadyFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{merging}}, ready: true},
		runtimeReady:          false,
	}
	result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeCanceled || !errors.Is(result.Err, ErrCanceled) || len(worker.calls) != 0 || len(ensurer.calls) != 0 {
		t.Fatalf("sealed merge crossed runtime admission: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}

	source.runtimeReady = true
	result = NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
	if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != merging.Ref || len(ensurer.calls) != 0 {
		t.Fatalf("opened merge did not reconcile: result=%+v worker=%v ensure=%v", result, worker.calls, ensurer.calls)
	}
}

func TestSchedulerNeverConsumesSealedRuntimeAcrossActiveStates(t *testing.T) {
	states := []domain.State{
		domain.StatePlanning,
		domain.StateVerifying,
		domain.StateBuilding,
		domain.StateReviewing,
		domain.StatePublishing,
		domain.StateWaitingCI,
		domain.StateWaitingApproval,
		domain.StateWaitingManualMerge,
		domain.StateMerging,
		domain.StateReconciling,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			candidate := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: domain.TicketID("sealed-" + string(state))}, state)
			ensurer := &fakeEnsure{}
			worker := &fakeWorker{}
			source := runtimeReadyMergeTickets{
				mergeReadyFakeTickets: mergeReadyFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{candidate}}, ready: true},
				runtimeReady:          false,
			}
			result := NewScheduler(domain.ChannelDev, source, ensurer, worker).Tick(context.Background(), domain.Fence{LeaderEpoch: 9})
			if result.Outcome != OutcomeCanceled || !errors.Is(result.Err, ErrCanceled) || len(worker.calls) != 0 || len(ensurer.calls) != 0 {
				t.Fatalf("sealed %s crossed runtime admission: result=%+v worker=%v ensure=%v", state, result, worker.calls, ensurer.calls)
			}
		})
	}
}

func TestSchedulerBindsEachTicketRunnerEpochToTheSameLeader(t *testing.T) {
	first := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "first"}, domain.StatePlanning)
	first.RunnerEpoch = 3
	second := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "a", Ticket: "second"}, domain.StatePlanning)
	second.RunnerEpoch = 8
	ensurer := &fakeEnsure{err: store.ErrBusy}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{first, second}}, ensurer, &fakeWorker{})
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 12})
	if result.Outcome != OutcomeBusy || len(ensurer.calls) != 2 || ensurer.calls[0].Fence != (domain.Fence{LeaderEpoch: 12, RunnerEpoch: 3}) || ensurer.calls[1].Fence != (domain.Fence{LeaderEpoch: 12, RunnerEpoch: 8}) {
		t.Fatalf("result=%+v calls=%+v", result, ensurer.calls)
	}
}

func TestSchedulerCancellationAndInvalidFenceAreTyped(t *testing.T) {
	worker := &fakeWorker{}
	ensurer := &fakeEnsure{}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{}, ensurer, worker)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result := scheduler.Tick(ctx, domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}); result.Outcome != OutcomeCanceled || !errors.Is(result.Err, ErrCanceled) {
		t.Fatalf("canceled result=%+v", result)
	}
	if result := scheduler.Tick(context.Background(), domain.Fence{}); result.Outcome != OutcomeReadiness || !errors.Is(result.Err, ErrReadiness) {
		t.Fatalf("invalid fence result=%+v", result)
	}
}

func TestSchedulerMapsInProgressWithoutProviderText(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}
	ensurer := &fakeEnsure{err: worktreecoord.ErrInProgress}
	scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, ensurer, &fakeWorker{})
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1, RunnerEpoch: 7})
	if result.Outcome != OutcomeInProgress || !errors.Is(result.Err, ErrInProgress) || result.Err.Error() != ErrInProgress.Error() {
		t.Fatalf("result=%+v", result)
	}
}

func TestSchedulerRejectsAdmissionAfterDurableTicketChange(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-durable-stop"}
	snapshot := ticket(ref, domain.StatePlanning)
	current := snapshot
	current.State = domain.StateStopping
	current.ResumeState = domain.StatePlanning
	current.Version++
	ensurer := &fakeEnsure{}
	scheduler := NewScheduler(domain.ChannelDev, currentFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{snapshot}}, current: current}, ensurer, &fakeWorker{})
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1})
	if result.Outcome != OutcomeStale || !errors.Is(result.Err, ErrStale) || len(ensurer.calls) != 0 {
		t.Fatalf("stale durable state admitted work: result=%+v ensure=%d", result, len(ensurer.calls))
	}
}

func TestDirectSchedulerConstructionCannotBypassAdmission(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-no-admission"}
	literal := &Scheduler{}
	if result := literal.Tick(context.Background(), domain.Fence{LeaderEpoch: 1}); result.Outcome != OutcomeReadiness || !errors.Is(result.Err, ErrInvalidScheduler) {
		t.Fatalf("literal scheduler bypassed admission: %+v", result)
	}
	if _, err := NewRuntime(literal, time.Millisecond); !errors.Is(err, ErrInvalidScheduler) {
		t.Fatalf("literal runtime=%v, want invalid scheduler", err)
	}
	if err := (&Runtime{Scheduler: literal, Interval: time.Millisecond, workers: 1}).Start(context.Background(), domain.Fence{LeaderEpoch: 1}); !errors.Is(err, ErrInvalidScheduler) {
		t.Fatalf("literal Runtime.Start=%v, want invalid scheduler", err)
	}
	var absent *admission
	if _, _, admitted := absent.Begin(context.Background(), ref, 1, 1, 1); admitted {
		t.Fatal("nil Admission admitted work")
	}
	if err := absent.Stop(context.Background(), ref); !errors.Is(err, ErrInvalidScheduler) {
		t.Fatalf("nil Admission.Stop=%v", err)
	}
}

func TestRearmTokenRejectsLifecycleChangeBeforeExactBegin(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-token-race"}
	admission := newAdmission()
	if err := admission.Stop(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if err := admission.Rearm(ref, 6, 1, 7, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	proved := ticket(ref, domain.StatePlanning)
	proved.Version, proved.RunnerEpoch = 6, 7
	changed := proved
	changed.Version++
	changed.State = domain.StateVerifying
	ensurer := &fakeEnsure{}
	scheduler := NewScheduler(domain.ChannelDev, currentFakeTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{changed}}, current: changed}, ensurer, &fakeWorker{})
	scheduler.admission = admission
	result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 1})
	if result.Outcome != OutcomeCanceled || len(ensurer.calls) != 0 {
		t.Fatalf("changed identity consumed rearm token: result=%+v ensures=%d", result, len(ensurer.calls))
	}
	_, end, admitted := admission.Begin(context.Background(), ref, proved.Version, 1, proved.RunnerEpoch)
	if !admitted {
		t.Fatal("stale lifecycle change cleared the exact token")
	}
	end()
}
