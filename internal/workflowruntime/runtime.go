package workflowruntime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

var (
	ErrRuntimeStarted    = errors.New("workflow runtime is already started")
	ErrRuntimeNotStarted = errors.New("workflow runtime is not started")
	ErrRuntimeInterval   = errors.New("workflow runtime interval is invalid")
)

const maxRuntimeInterval = time.Hour

// Runtime is a daemon-neutral lifecycle wrapper around the one-tick
// Scheduler. It has exactly one in-flight Tick and never launches a second
// tick until the bounded interval has elapsed.
type Runtime struct {
	Scheduler *Scheduler
	Interval  time.Duration

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

func NewRuntime(scheduler *Scheduler, interval time.Duration) (*Runtime, error) {
	if scheduler == nil || scheduler.validate() != nil {
		return nil, ErrInvalidScheduler
	}
	if interval <= 0 || interval > maxRuntimeInterval {
		return nil, ErrRuntimeInterval
	}
	return &Runtime{Scheduler: scheduler, Interval: interval}, nil
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
	loopCtx, cancel := context.WithCancel(ctx)
	r.started, r.cancel, r.done = true, cancel, make(chan struct{})
	done := r.done
	go r.loop(loopCtx, fence, done)
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

func (r *Runtime) loop(ctx context.Context, fence domain.Fence, done chan struct{}) {
	defer close(done)
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
