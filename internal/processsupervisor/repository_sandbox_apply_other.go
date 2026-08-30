//go:build !darwin || !cgo

package processsupervisor

// Product v1 is macOS-only. A build without Darwin's sandbox_init API must
// fail closed rather than silently running a repository test unsandboxed.
func ApplyRepositoryTestSandbox(string) error { return ErrUnclear }
