package git

import "fmt"

// ProtectedBaseChange reports two exact, distinct protected-base observations.
// It is diagnostic input for refresh coordination, never mutation authority:
// a coordinator must obtain a Store claim and re-observe the ref under its Git
// lease before preparing a refreshed candidate. Missing or malformed remote
// output is deliberately not a base-change observation.
type ProtectedBaseChange struct {
	Expected string
	Observed string
}

func (e *ProtectedBaseChange) Error() string {
	return fmt.Sprintf("%v: remote base moved", ErrUnexpectedRemote)
}

func (e *ProtectedBaseChange) Unwrap() error { return ErrUnexpectedRemote }

func protectedBaseChange(expected, observed string) error {
	if !validOID(expected) || !validOID(observed) || len(expected) != len(observed) || expected == observed {
		return fmt.Errorf("%w: protected base observation is unavailable", ErrUnexpectedRemote)
	}
	return &ProtectedBaseChange{Expected: expected, Observed: observed}
}
