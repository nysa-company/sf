package processsupervisor

import (
	"context"
	"os"
	"time"

	"github.com/nysa-company/sf/internal/pythonclosure"
	"golang.org/x/sys/unix"
)

type pythonScratchMonitor struct {
	failure <-chan error
	done    <-chan struct{}
	cancel  context.CancelFunc
}

// startPythonScratchMonitor retains its own close-on-exec descriptor, so caller
// cleanup cannot cause a reused FD to be inspected. Failure is delivered once;
// cancellation is silent. The launch owner must react to failure with bounded
// process termination/drain, not simply stop watching or release its lease.
func startPythonScratchMonitor(ctx context.Context, rootFD int) (*pythonScratchMonitor, error) {
	if ctx == nil || rootFD < 0 {
		return nil, ErrUnclear
	}
	fd, err := unix.FcntlInt(uintptr(rootFD), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, ErrUnclear
	}
	root := os.NewFile(uintptr(fd), "python-scratch-monitor")
	if _, err := pythonclosure.InspectScratchDirectoryFD(ctx, fd); err != nil {
		root.Close()
		return nil, err
	}
	monitorCtx, cancel := context.WithCancel(ctx)
	failure, done := make(chan error, 1), make(chan struct{})
	monitor := &pythonScratchMonitor{failure: failure, done: done, cancel: cancel}
	go func() {
		defer close(done)
		defer root.Close()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
				_, err := pythonclosure.InspectScratchDirectoryFD(monitorCtx, fd)
				if err != nil {
					if monitorCtx.Err() == nil {
						failure <- err
					}
					return
				}
			}
		}
	}()
	return monitor, nil
}

// Stop waits for descriptor ownership to return. It is safe to call repeatedly.
// It does not remove scratch files or claim that the launched process drained.
func (m *pythonScratchMonitor) Stop() {
	if m == nil {
		return
	}
	m.cancel()
	<-m.done
}
