package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

type ExitCode int

const (
	ExitOK            ExitCode = 0
	ExitInput         ExitCode = 2
	ExitAction        ExitCode = 3
	ExitWait          ExitCode = 4
	ExitPolicy        ExitCode = 5
	ExitCompatibility ExitCode = 6
	ExitInternal      ExitCode = 7
)

func exitCode(response api.Response) ExitCode {
	if response.OK {
		return ExitOK
	}
	if response.Error == nil {
		return ExitInternal
	}
	switch response.Error.Code {
	case "invalid_command", "invalid_ticket", "invalid_argument":
		return ExitInput
	case "operator_action_required", "provider_auth_missing", "blocked_process", "uncertain_effect", "not_configured", "doctor_not_configured", "doctor_failed":
		return ExitAction
	case "daemon_unavailable", "provider_waiting", "checks_pending":
		return ExitWait
	case "policy_refusal", "safety_blocked", "unqualified_provider":
		return ExitPolicy
	case "protocol_incompatible", "version_mismatch", "schema_mismatch":
		return ExitCompatibility
	default:
		return ExitInternal
	}
}

func nextAction(response api.Response) *domain.NextAction {
	if response.NextAction == nil || len(response.NextAction.Argv) == 0 {
		return nil
	}
	copy := *response.NextAction
	copy.Argv = append([]string(nil), response.NextAction.Argv...)
	return &copy
}

// validateCLIResponse tightens the wire envelope for a command boundary. The
// API validator permits a failed response without next_action so that daemon
// decoding can remain a structural check; the CLI must never expose such a
// response to an operator or automation.
func validateCLIResponse(response api.Response) error {
	if err := response.Validate(); err != nil {
		return err
	}
	if !response.OK {
		action := nextAction(response)
		if action == nil || strings.TrimSpace(action.Argv[0]) == "" {
			return errors.New("failed response requires one non-empty executable next action")
		}
	}
	return nil
}

// Render writes either the human response or the exact API response JSON. It
// does not derive separate semantics for the two formats.
func Render(writer io.Writer, response api.Response, jsonOutput bool) error {
	if err := validateCLIResponse(response); err != nil {
		return err
	}
	if jsonOutput {
		return encodeJSON(writer, response)
	}
	if response.OK {
		if len(response.Data) == 0 || string(response.Data) == "null" {
			_, err := io.WriteString(writer, "OK\n")
			return err
		}
		var value any
		if err := json.Unmarshal(response.Data, &value); err != nil {
			return fmt.Errorf("response data is not JSON: %w", err)
		}
		_, err := fmt.Fprintf(writer, "OK\n%s\n", stableJSON(value))
		return err
	}

	problem := response.Error
	if problem == nil {
		return errors.New("failed response has no error")
	}
	if nextAction(response) == nil {
		return errors.New("failed response requires one executable next action")
	}
	if _, err := fmt.Fprintf(writer, "Error: %s: %s\n", problem.Code, problem.Message); err != nil {
		return err
	}
	if response.Mutation.Attempted {
		if _, err := fmt.Fprintf(writer, "Mutation: attempted (%s)\n", response.Mutation.Kind); err != nil {
			return err
		}
	} else {
		if _, err := io.WriteString(writer, "Mutation: none\n"); err != nil {
			return err
		}
	}
	if len(response.Data) > 0 && string(response.Data) != "null" {
		var value any
		if err := json.Unmarshal(response.Data, &value); err == nil {
			if _, err := fmt.Fprintf(writer, "Data: %s\n", stableJSON(value)); err != nil {
				return err
			}
		}
	}
	if action := nextAction(response); action != nil {
		_, err := fmt.Fprintf(writer, "Next: %s\n", shellWords(action.Argv))
		return err
	}
	return nil
}

func encodeJSON(writer io.Writer, response api.Response) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(response)
}

func stableJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func shellWords(argv []string) string {
	words := make([]string, len(argv))
	for index, arg := range argv {
		if arg == "" || strings.ContainsAny(arg, " \t\n\"'") {
			words[index] = "'" + strings.ReplaceAll(arg, "'", "'\\''") + "'"
		} else {
			words[index] = arg
		}
	}
	return strings.Join(words, " ")
}
