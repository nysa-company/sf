//go:build !darwin && !linux

package processsupervisor

// This supervisor intentionally supports only hosts where a kernel identity
// can be observed. Other Unix hosts fail closed before any recovery signal.
func processStartIdentity(int) (string, error) { return "", ErrUnclear }
func hostBootIdentity() (string, error)        { return "", ErrUnclear }
