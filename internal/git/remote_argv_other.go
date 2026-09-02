//go:build !sf_e2e

package git

// rewriteE2ERemoteArgv is intentionally inert in ordinary builds.  The
// fixture transport must never be enabled by an inherited environment value
// in a production binary.
func rewriteE2ERemoteArgv(argv []string) ([]string, error) { return argv, nil }
