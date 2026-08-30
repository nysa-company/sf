package workflowruntime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

var (
	ErrRuntimeStarted    = errors.New("workflow runtime is already started")
	ErrRuntimeNotStarted = errors.New("workflow runtime is not started")
	ErrRuntimeInterval   = errors.New("workflow runtime interval is invalid")
	ErrRuntimeWorkers    = errors.New("workflow runtime worker count is invalid")
	ErrRuntimeRearm      = errors.New("workflow runtime ticket cannot be rearmed")
)

const (
	maxRuntimeInterval = time.Hour
	maxRuntimeWorkers  = 2
)

// Runtime is a daemon-neutral lifecycle wrapper around the one-tick
// Scheduler. Every loop shares one Scheduler and its admission lineage; the
// bounded worker count controls only distinct ticket concurrency.
type Runtime struct {
	Scheduler *Scheduler
	Interval  time.Duration
	workers   int

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

type RuntimeConfig struct {
	Interval time.Duration
	Workers  int
}

// ControlBundle is the sealed daemon handoff for this exact Runtime.
type ControlBundle struct{ runtime *Runtime }

func (r *Runtime) ControlBundle() *ControlBundle {
	if r == nil {
		return nil
	}
	return &ControlBundle{runtime: r}
}

func (b *ControlBundle) Valid() bool {
	return b != nil && b.runtime != nil && b.runtime.Scheduler != nil && b.runtime.Scheduler.validate() == nil
}

func (b *ControlBundle) Drain(ctx context.Context, ref domain.TicketRef) error {
	if !b.Valid() {
		return ErrInvalidScheduler
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	return b.runtime.Scheduler.admission.Stop(ctx, ref)
}

func (b *ControlBundle) ApplyRearm(capability *store.RuntimeAdmissionCapability) error {
	if !b.Valid() || capability == nil {
		return ErrRuntimeRearm
	}
	ref, version, fence, issued := capability.ConsumeRuntimeAdmission()
	if !issued || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ErrRuntimeRearm
	}
	return b.runtime.Scheduler.admission.Rearm(ref, version, fence.LeaderEpoch, fence.RunnerEpoch, capability.OpenStoreAdmission, capability.SuspendStoreAdmission, capability.SealStoreAdmission)
}

func (b *ControlBundle) ApplyRetirement(ctx context.Context, capability *store.RuntimeRetirementCapability) error {
	if !b.Valid() || capability == nil {
		return ErrRuntimeRearm
	}
	err := capability.RetireRuntime(ctx, func(ref domain.TicketRef) error {
		if ref.Validate() != nil {
			return ErrRuntimeRearm
		}
		return b.runtime.Scheduler.admission.Retire(ref)
	})
	if errors.Is(err, store.ErrStaleFence) {
		return ErrRuntimeRearm
	}
	return err
}

func NewRuntime(scheduler *Scheduler, interval time.Duration) (*Runtime, error) {
	return NewRuntimeWithConfig(scheduler, RuntimeConfig{Interval: interval, Workers: 1})
}

func NewRuntimeWithConfig(scheduler *Scheduler, configuration RuntimeConfig) (*Runtime, error) {
	if scheduler == nil || scheduler.validate() != nil {
		return nil, ErrInvalidScheduler
	}
	if configuration.Interval <= 0 || configuration.Interval > maxRuntimeInterval {
		return nil, ErrRuntimeInterval
	}
	if configuration.Workers < 1 || configuration.Workers > maxRuntimeWorkers {
		return nil, ErrRuntimeWorkers
	}
	return &Runtime{Scheduler: scheduler, Interval: configuration.Interval, workers: configuration.Workers}, nil
}

// Start starts the loop immediately with the supplied leader fence. The
// scheduler binds each ticket's runner epoch when each tick snapshots it.
func (r *Runtime) Start(ctx context.Context, fence domain.Fence) error {
	if r == nil || r.Scheduler == nil || r.Scheduler.validate() != nil {
		return ErrInvalidScheduler
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return ErrRuntimeStarted
	}
	if r.Interval <= 0 || r.Interval > maxRuntimeInterval {
		return ErrRuntimeInterval
	}
	if r.workers < 1 || r.workers > maxRuntimeWorkers {
		return ErrRuntimeWorkers
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.started, r.cancel, r.done = true, cancel, make(chan struct{})
	done := r.done
	var loops sync.WaitGroup
	loops.Add(r.workers)
	for worker := 0; worker < r.workers; worker++ {
		go func() {
			defer loops.Done()
			r.loop(loopCtx, fence)
		}()
	}
	go func() {
		loops.Wait()
		close(done)
	}()
	return nil
}

// Serve starts and waits for the runtime. It is convenient for a future
// daemon main without making this package own signals or process lifecycle.
func (r *Runtime) Serve(ctx context.Context, fence domain.Fence) error {
	if err := r.Start(ctx, fence); err != nil {
		return err
	}
	return r.Wait(context.Background())
}

func (r *Runtime) loop(ctx context.Context, fence domain.Fence) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Tick owns the complete Ensure -> Worker sequence. It returns only
		// once the in-flight operation has honored cancellation or completed.
		r.Scheduler.Tick(ctx, fence)
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(r.Interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

// Cancel requests shutdown and is safe to call before or after Wait.
func (r *Runtime) Cancel() {
	if r == nil {
		return
	}
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Wait waits for the loop and, importantly, does not return while its current
// Ensure or Worker call is still running. A caller context bounds the wait;
// it does not detach or abandon the runtime goroutine.
func (r *Runtime) Wait(ctx context.Context) error {
	if r == nil {
		return ErrInvalidScheduler
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	done := r.done
	started := r.started
	r.mu.Unlock()
	if !started || done == nil {
		return ErrRuntimeNotStarted
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close requests cancellation and waits without abandoning an in-flight
// external boundary. It is idempotent after Start.
func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	r.Cancel()
	return r.Wait(context.Background())
}
