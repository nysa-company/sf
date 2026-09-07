package processsupervisor

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRuntimeCleanupRequiresRunAndWait(t *testing.T) {
	for _, waitFirst := range []bool{false, true} {
		var count atomic.Int32
		run, wait := cleanupAfterRunAndWait(func() { count.Add(1) })
		first, last := run, wait
		if waitFirst {
			first, last = wait, run
		}
		first()
		first()
		if count.Load() != 0 {
			t.Fatal("resources removed while another owner remains")
		}
		last()
		last()
		if count.Load() != 1 {
			t.Fatal("cleanup must run exactly once")
		}
	}
}

func TestRuntimeCleanupConcurrentCompletion(t *testing.T) {
	var count atomic.Int32
	run, wait := cleanupAfterRunAndWait(func() { count.Add(1) })
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(2)
		go func() { defer group.Done(); run() }()
		go func() { defer group.Done(); wait() }()
	}
	group.Wait()
	if count.Load() != 1 {
		t.Fatal("concurrent owners cleaned more or less than once")
	}
}
