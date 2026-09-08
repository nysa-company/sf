package processsupervisor

import (
	"runtime"
	"syscall"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

const RepositoryPythonFileSizeLimit = pythonclosure.ScratchFileBytes
const RepositoryPythonOpenFileLimit = 128

// ApplyRepositoryPythonResourceLimits is called only in the isolated gate
// child, never the daemon. Hard limits cannot be raised by Python code after
// exec. RLIMIT_FSIZE limits each file, not aggregate scratch consumption; a
// separate aggregate budget is required before production admission.
func ApplyRepositoryPythonResourceLimits() error {
	if runtime.GOOS != "darwin" {
		return ErrUnclear
	}
	for _, limit := range []struct {
		resource int
		value    uint64
	}{
		{syscall.RLIMIT_CORE, 0},
		{syscall.RLIMIT_FSIZE, RepositoryPythonFileSizeLimit},
		{syscall.RLIMIT_NOFILE, RepositoryPythonOpenFileLimit},
	} {
		var current syscall.Rlimit
		if syscall.Getrlimit(limit.resource, &current) != nil {
			return ErrUnclear
		}
		// Never raise a stricter inherited limit.
		value := limit.value
		if current.Cur < value {
			value = current.Cur
		}
		if current.Max < value {
			value = current.Max
		}
		if syscall.Setrlimit(limit.resource, &syscall.Rlimit{Cur: value, Max: value}) != nil {
			return ErrUnclear
		}
	}
	return nil
}
