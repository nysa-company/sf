package workflowruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

type schedulerPostbuildRepairTickets struct {
	fakeTickets
	proof      store.PostbuildRepairBuildContext
	proofErr   error
	proofCalls []worktreecoord.EnsureRequest
}

func (s *schedulerPostbuildRepairTickets) PostbuildRepairContext(_ context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.PostbuildRepairBuildContext, error) {
	s.proofCalls = append(s.proofCalls, worktreecoord.EnsureRequest{Ref: ref, Version: version, Fence: fence})
	return s.proof, s.proofErr
}

type schedulerPostbuildRepairWorktrees struct {
	fakeEnsure
	authCalls []worktreecoord.EnsureRequest
	authErr   error
	cancel    context.CancelFunc
}

func (w *schedulerPostbuildRepairWorktrees) AuthenticatePostbuildRepair(_ context.Context, request worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	w.authCalls = append(w.authCalls, request)
	if w.cancel != nil {
		w.cancel()
	}
	if w.authErr != nil {
		return store.StoredWorktree{}, w.authErr
	}
	return store.StoredWorktree{Path: "/private/retained-postbuild", State: "registered"}, nil
}

// Logical Store context is a routing proof, never a replacement for physical
// authentication. These adapters deliberately provide no broad dirty Ensure.
func TestSchedulerPostbuildRepairRequiresPhysicalAdmission(t *testing.T) {
	for _, mode := range []string{"valid", "missing authenticator", "physical refusal", "physical absent", "physical cancellation", "cancel after observation", "corrupt context", "stale context", "absent context"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-postbuild-scheduler"}, domain.StateBuilding)
			expected := worktreecoord.EnsureRequest{Ref: building.Ref, Version: building.Version, Fence: domain.Fence{LeaderEpoch: 9, RunnerEpoch: building.RunnerEpoch}}
			source := &schedulerPostbuildRepairTickets{
				fakeTickets: fakeTickets{tickets: []store.Ticket{building}},
				proof:       store.PostbuildRepairBuildContext{Repair: store.PostbuildRepair{PostbuildFailureRequest: store.PostbuildFailureRequest{Ref: building.Ref}, EntryVersion: building.Version}},
			}
			physical := &schedulerPostbuildRepairWorktrees{}
			var worktrees WorktreeEnsurer = physical
			worker := &fakeWorker{}
			switch mode {
			case "missing authenticator":
				worktrees = &physical.fakeEnsure
			case "physical refusal":
				physical.authErr = worktreecoord.ErrUnready
			case "physical absent":
				physical.authErr = store.ErrNotFound
			case "physical cancellation":
				physical.authErr = context.Canceled
			case "cancel after observation":
				physical.cancel = cancel
			case "corrupt context":
				source.proofErr = store.ErrEvidenceConflict
			case "stale context":
				source.proofErr = store.ErrStaleFence
			case "absent context":
				source.proofErr = store.ErrNotFound
			}
			result := NewScheduler(domain.ChannelDev, source, worktrees, worker).Tick(ctx, domain.Fence{LeaderEpoch: 9})
			if len(source.proofCalls) != 1 || source.proofCalls[0] != expected {
				t.Fatalf("logical proof not bound to live ticket: %+v", source.proofCalls)
			}
			if mode == "absent context" {
				if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != building.Ref || len(physical.calls) != 1 || physical.calls[0] != expected || len(physical.authCalls) != 0 {
					t.Fatalf("absence lost ordinary Ensure: result=%+v worker=%v ensure=%v auth=%v", result, worker.calls, physical.calls, physical.authCalls)
				}
				return
			}
			if len(physical.calls) != 0 {
				t.Fatalf("present repair fell back to Ensure: %+v", physical.calls)
			}
			if mode == "valid" {
				if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != building.Ref || len(physical.authCalls) != 1 || physical.authCalls[0] != expected || result.Worktree.Path != "/private/retained-postbuild" {
					t.Fatalf("valid repair not dispatched exactly once: %+v worker=%v auth=%v", result, worker.calls, physical.authCalls)
				}
				return
			}
			if result.Outcome == OutcomeInvoked || len(worker.calls) != 0 {
				t.Fatalf("refused repair invoked Worker: %+v worker=%v", result, worker.calls)
			}
			if (mode == "corrupt context" || mode == "stale context" || mode == "missing authenticator") && len(physical.authCalls) != 0 {
				t.Fatal("physical authentication preceded usable authority")
			}
			if mode == "physical cancellation" || mode == "cancel after observation" {
				if result.Outcome != OutcomeCanceled || !errors.Is(result.Err, ErrCanceled) {
					t.Fatalf("cancellation not preserved: %+v", result)
				}
			}
		})
	}
}

type schedulerCompletedPostbuildTickets struct {
	*schedulerPostbuildRepairTickets
	completedErr   error
	completedCalls []worktreecoord.EnsureRequest
}

func (s *schedulerCompletedPostbuildTickets) PostbuildRepairCompletedBuildContext(_ context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (store.PostbuildRepairBuildContext, error) {
	s.completedCalls = append(s.completedCalls, worktreecoord.EnsureRequest{Ref: ref, Version: version, Fence: fence})
	return s.proof, s.completedErr
}

type schedulerCompletedPostbuildWorktrees struct {
	schedulerPostbuildRepairWorktrees
	completedErr    error
	completedCalls  []worktreecoord.EnsureRequest
	completedCancel context.CancelFunc
}

func (w *schedulerCompletedPostbuildWorktrees) AuthenticateCompletedPostbuildRepair(_ context.Context, request worktreecoord.EnsureRequest) (store.StoredWorktree, error) {
	w.completedCalls = append(w.completedCalls, request)
	if w.completedCancel != nil {
		w.completedCancel()
	}
	if w.completedErr != nil {
		return store.StoredWorktree{}, w.completedErr
	}
	return store.StoredWorktree{Path: "/private/completed-repair", State: "registered"}, nil
}

func TestSchedulerCompletedPostbuildRepairNeverFallsBackAfterAmbiguousAttempt(t *testing.T) {
	for _, mode := range []string{"completed", "no fresh attempt", "unfinished attempt", "corrupt completed result", "missing finalizer", "physical refusal", "physical absent", "cancel after completion inspection"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			building := ticket(domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-completed-repair"}, domain.StateBuilding)
			expected := worktreecoord.EnsureRequest{Ref: building.Ref, Version: building.Version, Fence: domain.Fence{LeaderEpoch: 9, RunnerEpoch: building.RunnerEpoch}}
			source := &schedulerCompletedPostbuildTickets{schedulerPostbuildRepairTickets: &schedulerPostbuildRepairTickets{fakeTickets: fakeTickets{tickets: []store.Ticket{building}}}}
			physical := &schedulerCompletedPostbuildWorktrees{}
			var worktrees WorktreeEnsurer = physical
			switch mode {
			case "no fresh attempt":
				source.completedErr = store.ErrNotFound
			case "unfinished attempt", "corrupt completed result":
				source.completedErr = store.ErrEvidenceConflict
			case "missing finalizer":
				worktrees = &physical.schedulerPostbuildRepairWorktrees
			case "physical refusal":
				physical.completedErr = worktreecoord.ErrUnready
			case "physical absent":
				physical.completedErr = store.ErrNotFound
			case "cancel after completion inspection":
				physical.completedCancel = cancel
			}
			worker := &fakeWorker{}
			result := NewScheduler(domain.ChannelDev, source, worktrees, worker).Tick(ctx, domain.Fence{LeaderEpoch: 9})
			if len(source.completedCalls) != 1 || source.completedCalls[0] != expected || len(physical.calls) != 0 {
				t.Fatalf("completed authority/Ensure mismatch: result=%+v proof=%v ensure=%v", result, source.completedCalls, physical.calls)
			}
			if mode == "no fresh attempt" {
				if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || len(physical.authCalls) != 1 || physical.authCalls[0] != expected || len(physical.completedCalls) != 0 {
					t.Fatalf("strict absence did not select initial admission: %+v initial=%v completed=%v", result, physical.authCalls, physical.completedCalls)
				}
				return
			}
			if len(physical.authCalls) != 0 {
				t.Fatalf("completed/ambiguous attempt fell back to initial fingerprint: %+v", physical.authCalls)
			}
			if mode == "completed" {
				if result.Outcome != OutcomeInvoked || len(worker.calls) != 1 || worker.calls[0] != building.Ref || len(physical.completedCalls) != 1 || physical.completedCalls[0] != expected || result.Worktree.Path != "/private/completed-repair" {
					t.Fatalf("completed repair not dispatched once: %+v worker=%v completed=%v", result, worker.calls, physical.completedCalls)
				}
				return
			}
			if result.Outcome == OutcomeInvoked || len(worker.calls) != 0 {
				t.Fatalf("failed completed admission reached Worker: %+v worker=%v", result, worker.calls)
			}
			if (mode == "unfinished attempt" || mode == "corrupt completed result" || mode == "missing finalizer") && len(physical.completedCalls) != 0 {
				t.Fatal("ineligible completed proof reached physical authentication")
			}
			if mode == "cancel after completion inspection" && result.Outcome != OutcomeCanceled {
				t.Fatalf("cancellation not retained: %+v", result)
			}
		})
	}
}
