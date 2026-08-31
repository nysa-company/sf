package contracts

import "errors"

// Typed external mutation outcomes distinguish a proven pre-handoff failure
// from an outcome that must be reconciled before any retry. Adapters expose
// these through their package-level aliases without depending on one another.
var (
	ErrDraftCreateUncertain         = errors.New("draft pull request creation is uncertain; reconcile before retrying")
	ErrPullRequestUpdateUncertain   = errors.New("pull request update is uncertain; reconcile before retrying")
	ErrDraftCreateBeforeStart       = errors.New("draft pull request creation failed before mutation handoff")
	ErrPullRequestUpdateBeforeStart = errors.New("pull request update failed before mutation handoff")
)
