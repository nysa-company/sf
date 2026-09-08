package daemon

import "errors"

// These code-owned errors let production preflight report a safe explanation
// without displaying arbitrary tool/configuration output from a Doctor hook.
var (
	ErrStartRecipeUnsupported  = errors.New("configured repository recipe is unsupported")
	ErrStartRuntimeUnsupported = errors.New("local execution requires macOS")
)
