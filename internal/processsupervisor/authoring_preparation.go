package processsupervisor

import "errors"

type authoringPreparationError struct {
	stage string
	cause error
}

func (e *authoringPreparationError) Error() string {
	return "authoring preparation: " + AuthoringPreparationCategory(e)
}
func (e *authoringPreparationError) Unwrap() error { return e.cause }

func preparationFailure(stage string, cause error) error {
	// Do not retain arbitrary subprocess/credential errors, even via Unwrap.
	switch cause {
	case errCLIObservation, errClaudeAuthRenewal, ErrUnclear:
	default:
		cause = errCLIObservation
	}
	return &authoringPreparationError{stage: stage, cause: cause}
}

// AuthoringPreparationCategory returns only code-owned diagnostic stages.
// Unknown errors (including nil) never expose their text or imply success.
func AuthoringPreparationCategory(err error) string {
	var failure *authoringPreparationError
	if errors.As(err, &failure) && failure != nil {
		switch failure.stage {
		case "unsupported", "resolve", "staging", "authlookup", "authrenewal", "version", "help", "authstatus", "binding", "supervisor_state":
			return failure.stage
		}
	}
	return "unknown"
}
