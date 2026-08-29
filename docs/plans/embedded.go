// Package plans exposes the reviewed state-machine artifact to the production
// binary without making its runtime behavior depend on a checkout-relative
// documentation path.
package plans

import _ "embed"

//go:embed 2026-08-29-software-factory-v1-state-machine.json
var stateMachine []byte

// StateMachine returns a private copy so callers cannot mutate the embedded
// authority for another daemon in the same process.
func StateMachine() []byte { return append([]byte(nil), stateMachine...) }
