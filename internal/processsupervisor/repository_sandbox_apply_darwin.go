//go:build darwin && cgo

package processsupervisor

/*
#cgo LDFLAGS: -lsandbox
#include <sandbox.h>
#include <stdlib.h>

static int sf_repository_sandbox_init(const char *profile, char **errorbuf) {
	return sandbox_init(profile, 0, errorbuf);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// ApplyRepositoryTestSandbox confines the current wrapper process before it
// execs the test binary. Unlike sandbox-exec, sandbox_init does not insert a
// helper child; the wrapper remains the process-group leader established by
// cmd/sf, so POSIX rejects a later setsid call by the test binary.
func ApplyRepositoryTestSandbox(profile string) error {
	if profile == "" {
		return ErrUnclear
	}
	input := C.CString(profile)
	defer C.free(unsafe.Pointer(input))
	var detail *C.char
	if rc := C.sf_repository_sandbox_init(input, &detail); rc != 0 {
		if detail != nil {
			defer C.sandbox_free_error(detail)
			return fmt.Errorf("initialize repository test sandbox: %s", C.GoString(detail))
		}
		return ErrUnclear
	}
	return nil
}
