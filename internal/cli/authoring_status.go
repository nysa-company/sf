package cli

import (
	"encoding/json"
	"fmt"
	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/spf13/cobra"
	"strings"
)

func (a *app) announceAuthoringTurn(session, key string) error {
	argv := a.authoringRecoveryResponse(api.Response{}, session, key).NextAction.Argv
	_, err := fmt.Fprintf(a.errOut, "Turn recovery (read only): %s\n", strings.Join(argv, " "))
	return err
}

func (a *app) authoringRecoveryResponse(response api.Response, session, key string) api.Response {
	response.NextAction = &domain.NextAction{Code: "authoring_status", Argv: []string{binaryForChannel(a.channel), "ticket", "new", "--status", session, "--turn", key}}
	response.Mutation = api.Mutation{Attempted: true, Kind: "authoring_turn", Identity: session + "/" + key}
	return response
}

// Recovery is a single read. It cannot save, resend, refine, or dispatch even
// when the recovered result proposes a valid home mutation.
func (a *app) readAuthoringStatus(cmd *cobra.Command, session, key string) error {
	fail := func(message string) error {
		return a.emit(failure("invalid_argument", message, commandHelpAction(cmd)))
	}
	if !contracts.AuthoringID(session) || !contracts.AuthoringID(key) {
		return fail("--status requires the exact session ID and --turn key")
	}
	response := a.request("authoring.status", "", params(map[string]any{"session": session, "key": key}, a.channel))
	if !response.OK {
		return a.emit(response)
	}
	var envelope struct {
		Turn authoringTurnView `json:"authoring_turn"`
	}
	if json.Unmarshal(response.Data, &envelope) != nil {
		return fail("invalid authoring status receipt")
	}
	turn := envelope.Turn
	if turn.Session != session || turn.Key != key || turn.Channel != string(a.channel) || !homeProjectPattern.MatchString(turn.Project) || turn.Purpose != "ticket_draft" && turn.Purpose != "home_intent" || response.Mutation.Attempted {
		return fail("authoring status identity or read-only contract mismatch")
	}
	switch turn.State {
	case "reserved", "launched", "uncertain":
		if turn.Result != nil || turn.Outcome != "" {
			return fail("inconsistent pending authoring receipt")
		}
	case "completed":
		switch turn.Outcome {
		case "success":
			if turn.Result == nil || authoring.ValidatePurpose(turn.Purpose, *turn.Result) != nil {
				return fail("invalid completed authoring content")
			}
			clean, err := authoring.SanitizePurpose(turn.Purpose, *turn.Result, redact.NewPolicy("", nil))
			if err != nil {
				return fail("invalid completed authoring content")
			}
			turn.Result = &clean
		case "cancelled", "failed", "interrupted":
			if turn.Result != nil {
				return fail("unexpected unsuccessful authoring content")
			}
		default:
			return fail("invalid authoring outcome")
		}
	default:
		return fail("invalid authoring state")
	}
	response.Data, _ = json.Marshal(map[string]any{"authoring_turn": turn})
	if a.json {
		return a.emit(response)
	}
	if _, err := fmt.Fprintf(a.out, "Authoring session: %s\nTurn: %s\nProject/channel: %s/%s\nPurpose: %s\nState: %s\nOutcome: %s\nRead only: nothing saved, submitted, dispatched or resent.\n", session, key, turn.Project, turn.Channel, turn.Purpose, turn.State, turn.Outcome); err != nil {
		return err
	}
	if turn.Result != nil {
		result := *turn.Result
		if turn.Purpose == "home_intent" {
			if _, err := fmt.Fprintf(a.out, "Recovered proposal (not executed): %s; selector: %s\n", result.Intent.Action, result.Intent.Selector); err != nil {
				return err
			}
		} else if result.Kind == "question" {
			if _, err := fmt.Fprintf(a.out, "Question: %s\n", result.Question); err != nil {
				return err
			}
		} else {
			source, err := authoring.Markdown(result)
			if err != nil {
				return fail("completed result cannot render as a ticket")
			}
			if _, err := fmt.Fprintln(a.out, source); err != nil {
				return err
			}
		}
	}
	a.last = &response
	return nil
}
