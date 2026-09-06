package processsupervisor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

func pythonScratchFixture(t *testing.T) (string, *os.File) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return root, f
}

func TestPythonScratchMonitorRetainsDescriptorAndReportsLimit(t *testing.T) {
	root, fd := pythonScratchFixture(t)
	m, err := startPythonScratchMonitor(t.Context(), int(fd.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	fd.Close()
	// Monitoring must survive caller descriptor closure and see later writes.
	f, err := os.Create(filepath.Join(root, "oversized"))
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(pythonclosure.ScratchFileBytes + 1)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-m.failure:
		if !errors.Is(err, pythonclosure.ErrLimit) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor missed scratch limit")
	}
	m.Stop()
	m.Stop()
	select {
	case err := <-m.failure:
		t.Fatalf("duplicate failure: %v", err)
	default:
	}
	if _, err := os.Stat(filepath.Join(root, "oversized")); err != nil {
		t.Fatal("monitor deleted evidence", err)
	}
}

func TestPythonScratchMonitorCancellationIsSilent(t *testing.T) {
	_, fd := pythonScratchFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	m, err := startPythonScratchMonitor(ctx, int(fd.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	m.Stop()
	select {
	case err := <-m.failure:
		t.Fatalf("cancel reported failure: %v", err)
	default:
	}
	if _, err := startPythonScratchMonitor(ctx, int(fd.Fd())); err == nil {
		t.Fatal("started cancelled monitor")
	}
	if _, err := startPythonScratchMonitor(nil, int(fd.Fd())); err == nil {
		t.Fatal("started nil-context monitor")
	}
	if _, err := startPythonScratchMonitor(t.Context(), -1); err == nil {
		t.Fatal("started invalid descriptor monitor")
	}
}

func TestPythonScratchMonitorRejectsExistingLimitBeforeStart(t *testing.T) {
	root, fd := pythonScratchFixture(t)
	f, err := os.Create(filepath.Join(root, "oversized"))
	if err != nil {
		t.Fatal(err)
	}
	err = f.Truncate(pythonclosure.ScratchFileBytes + 1)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	m, err := startPythonScratchMonitor(t.Context(), int(fd.Fd()))
	if m != nil {
		m.Stop()
		t.Fatal("started monitor over an already-invalid scratch directory")
	}
	if !errors.Is(err, pythonclosure.ErrLimit) {
		t.Fatalf("expected scratch limit, got %v", err)
	}
	if _, err := fd.Stat(); err != nil {
		t.Fatal("failed admission closed caller descriptor", err)
	}
}

func TestPythonScratchMonitorConcurrentStop(t *testing.T) {
	_, fd := pythonScratchFixture(t)
	m, err := startPythonScratchMonitor(t.Context(), int(fd.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	var stops sync.WaitGroup
	for range 16 {
		stops.Add(1)
		go func() {
			defer stops.Done()
			m.Stop()
		}()
	}
	done := make(chan struct{})
	go func() { stops.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent cleanup did not complete")
	}
	if _, err := fd.Stat(); err != nil {
		t.Fatal("monitor cleanup closed caller descriptor", err)
	}
}
