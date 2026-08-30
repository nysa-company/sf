package workflowruntime

import (
	"context"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// admission is the exact-ticket activity boundary shared by a Scheduler and
// its Runtime.  A ticket is registered before the first external boundary
// (worktree Ensure) and remains registered until Worker returns.  Stop first
// latches the ticket, then cancels and joins only that registered activity.
// A stopped ticket cannot be admitted again by this runtime instance.
type admission struct {
	mu      sync.Mutex
	stopped map[domain.TicketRef]activityIdentity
	allowed map[domain.TicketRef]admissionPermission
	active  map[domain.TicketRef]*activity
	// faults records a durable-seal failure. A memory stop is retained and
	// Controller.Drain observes this instead of completing on a volatile seal.
	faults map[domain.TicketRef]error
	// afterOpen is package-test-only synchronization for the Store-open to
	// runtime-commit cancellation boundary. It is always nil in production.
	afterOpen func()
	// afterStop is the matching package-test-only stop-race hook.
	afterStop func()
}

type activity struct {
	cancel   context.CancelFunc
	done     chan struct{}
	identity activityIdentity
	// sealStore is the durable close half of a consumed rearm capability.
	// Stop invokes it before cancellation, never after joining.
	sealStore func(context.Context) error
}

type activityIdentity struct{ version, leader, runner uint64 }

type admissionPermission struct {
	identity activityIdentity
	// openStore is the sealed second half of a Store/runtime rearm. It is
	// called only by the first exact Begin, before normal successors become
	// admissible in Store.
	openStore func(context.Context) error
	// suspendStore reports whether the exact pending admission may retry. A
	// concurrent operator seal returns false without reopening Store.
	suspendStore func(context.Context) (bool, error)
	// sealStore permanently closes a committed activity.
	sealStore func(context.Context) error
}

func newAdmission() *admission {
	return &admission{stopped: make(map[domain.TicketRef]activityIdentity), allowed: make(map[domain.TicketRef]admissionPermission), active: make(map[domain.TicketRef]*activity), faults: make(map[domain.TicketRef]error)}
}

// Begin atomically checks the stop latch and publishes activity before the
// caller can enter an external boundary.  admitted false is a benign stop or
// duplicate admission; it deliberately starts no goroutine.
func (a *admission) Begin(ctx context.Context, ref domain.TicketRef, version, leader, runner uint64) (run context.Context, end func(), admitted bool) {
	if a == nil {
		return nil, nil, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if _, stopped := a.stopped[ref]; stopped {
		permission, ok := a.allowed[ref]
		identity := activityIdentity{version: version, leader: leader, runner: runner}
		if !ok || permission.identity != identity {
			a.mu.Unlock()
			return nil, nil, false
		}
		if _, active := a.active[ref]; active {
			a.mu.Unlock()
			return nil, nil, false
		}
		// Register the exact activity before opening Store. If the open fails,
		// roll the entry back while preserving the one-use pending tuple, so a
		// later tick can retry without a partial open.
		run, cancel := context.WithCancel(ctx)
		entry := &activity{cancel: cancel, done: make(chan struct{}), identity: identity}
		a.active[ref] = entry
		a.mu.Unlock()
		if permission.openStore != nil {
			if err := permission.openStore(run); err != nil {
				a.mu.Lock()
				if a.active[ref] == entry {
					delete(a.active, ref)
					close(entry.done)
				}
				a.mu.Unlock()
				cancel()
				return nil, nil, false
			}
		}
		if a.afterOpen != nil {
			a.afterOpen()
		}
		a.mu.Lock()
		if a.active[ref] != entry || run.Err() != nil {
			// Store has opened, but this Begin lost its runtime commit race. The
			// compensating close uses an independent context because parent
			// cancellation is precisely the case being repaired.
			a.mu.Unlock()
			retryable := false
			var sealErr error
			if permission.suspendStore != nil {
				sealCtx, cancelSeal := context.WithTimeout(context.Background(), time.Second)
				retryable, sealErr = permission.suspendStore(sealCtx)
				cancelSeal()
			}
			a.mu.Lock()
			a.stopped[ref] = identity
			if !retryable {
				delete(a.allowed, ref)
			}
			if sealErr != nil {
				a.faults[ref] = sealErr
			}
			a.mu.Unlock()
			a.mu.Lock()
			if a.active[ref] == entry {
				delete(a.active, ref)
				close(entry.done)
			}
			a.mu.Unlock()
			cancel()
			return nil, nil, false
		}
		// An authorization is single-use. Only this exact Begin clears the
		// stopped latch; a lifecycle change before it mismatches and leaves the
		// ticket stopped. Once this identity has begun, its normal successor
		// versions can progress without an operator rearming every phase.
		delete(a.allowed, ref)
		delete(a.stopped, ref)
		entry.sealStore = permission.sealStore
		a.mu.Unlock()
		return run, func() {
			a.mu.Lock()
			if a.active[ref] == entry {
				delete(a.active, ref)
				close(entry.done)
			}
			a.mu.Unlock()
			cancel()
		}, true
	}
	if _, active := a.active[ref]; active {
		a.mu.Unlock()
		return nil, nil, false
	}
	run, cancel := context.WithCancel(ctx)
	entry := &activity{cancel: cancel, done: make(chan struct{}), identity: activityIdentity{version: version, leader: leader, runner: runner}}
	a.active[ref] = entry
	a.mu.Unlock()
	return run, func() {
		a.mu.Lock()
		if a.active[ref] == entry {
			delete(a.active, ref)
			close(entry.done)
		}
		a.mu.Unlock()
		cancel()
	}, true
}

// Stop prevents all future admission for ref, cancels its current activity,
// and joins it.  The caller context bounds only the wait; no activity is
// detached if the wait expires.
func (a *admission) Stop(ctx context.Context, ref domain.TicketRef) error {
	if a == nil {
		return ErrInvalidScheduler
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	entry := a.active[ref]
	if entry != nil {
		a.stopped[ref] = entry.identity
	} else if _, alreadyStopped := a.stopped[ref]; !alreadyStopped {
		a.stopped[ref] = activityIdentity{}
	}
	delete(a.allowed, ref)
	priorFault := a.faults[ref]
	var sealStore func(context.Context) error
	if entry != nil {
		sealStore = entry.sealStore
	}
	a.mu.Unlock()
	if priorFault != nil {
		return priorFault
	}
	// A rearmed activity may have opened Store immediately before Stop. Its
	// durable seal must commit before cancellation/join; using a bounded
	// independent context avoids inheriting precisely the cancellation being
	// processed. Failure is returned to Controller, which must not complete
	// the drain transition on a memory-only latch.
	if sealStore != nil {
		sealCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := sealStore(sealCtx)
		cancel()
		if err != nil {
			a.mu.Lock()
			a.faults[ref] = err
			a.mu.Unlock()
			return err
		}
	}
	a.mu.Lock()
	entry = a.active[ref]
	if entry != nil {
		entry.cancel()
	}
	if a.afterStop != nil {
		a.afterStop()
	}
	a.mu.Unlock()
	if entry == nil {
		return nil
	}
	select {
	case <-entry.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Rearm installs one exact, single-use admission authorization.  It never
// clears the stop latch; only a Begin with the exact proved identity consumes
// this token.  Thus a later lifecycle transition cannot inherit permission.
func (a *admission) Rearm(ref domain.TicketRef, version, leader, runner uint64, open func(context.Context) error, suspend func(context.Context) (bool, error), seal func(context.Context) error) error {
	if a == nil || version == 0 || leader == 0 || runner == 0 {
		return ErrInvalidScheduler
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	prior, stopped := a.stopped[ref]
	if !stopped || a.active[ref] != nil {
		return ErrRuntimeRearm
	}
	if (prior.version != 0 || prior.runner != 0 || prior.leader != 0) && version <= prior.version && leader == prior.leader && runner == prior.runner {
		return ErrRuntimeRearm
	}
	if _, allowed := a.allowed[ref]; allowed {
		return ErrRuntimeRearm
	}
	a.allowed[ref] = admissionPermission{identity: activityIdentity{version: version, leader: leader, runner: runner}, openStore: open, suspendStore: suspend, sealStore: seal}
	return nil
}

// Retire removes a terminal ticket's stopped record only after all activity
// and one-use permissions are gone.  A Store-backed lifecycle controller must
// call this solely after proving terminal state; it is not an admission path.
func (a *admission) Retire(ref domain.TicketRef) error {
	if a == nil {
		return ErrInvalidScheduler
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, stopped := a.stopped[ref]
	if !stopped {
		return nil
	}
	if a.active[ref] != nil {
		return ErrRuntimeRearm
	}
	// ApplyRetirement is reachable only with Store's opaque terminal
	// capability. A terminal transition can happen after rearm but before its
	// matching Begin, so discard that unconsumed token together with the latch.
	delete(a.allowed, ref)
	delete(a.stopped, ref)
	return nil
}

func (a *admission) Stopped(ref domain.TicketRef) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, stopped := a.stopped[ref]
	return stopped
}
