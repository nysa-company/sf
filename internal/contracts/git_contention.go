package contracts

import "errors"

// ErrGitMutationContended means a valid Git claim was not acquired because a
// different active repository writer currently holds the exclusion.
// It must not describe a same-claim lease, quarantine, SQL/commit failure,
// or an ambiguous/lost acquisition response. Only nil-lease responses carrying
// this proof may be retried in the same bounded operation scope.
var ErrGitMutationContended = errors.New("git mutation repository writer is active")
