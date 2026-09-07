package processsupervisor

import "sync"

// cleanupAfterRunAndWait keeps both directions safe: early Run cancellation
// must not remove a live process's credentials, while an early Wait completion
// must not delete the final artifact before Run reads it. Each owner may signal
// repeatedly; cleanup runs once after both owners finish.
func cleanupAfterRunAndWait(cleanup func()) (func(), func()) {
	var mu sync.Mutex
	remaining := 2
	signal := func() {
		mu.Lock()
		remaining--
		last := remaining == 0
		mu.Unlock()
		if last {
			cleanup()
		}
	}
	var runOnce, waitOnce sync.Once
	return func() { runOnce.Do(signal) }, func() { waitOnce.Do(signal) }
}
