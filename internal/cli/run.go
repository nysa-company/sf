package cli

import (
	"encoding/json"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/spf13/cobra"
)

// Run composes existing requests. It has no local lifecycle state and never
// retries a mutation, selects a replacement identity, or resumes a pause.
func (a *app) runCommand() *cobra.Command {
	var project string
	var watch bool
	var estimates bool
	command := &cobra.Command{Use: "run <ticket.md> --project <name>", Short: "Submit a ticket and start it if queued, optionally watching progress", Args: cobra.ExactArgs(1),
		Long: "Submit through the daemon, then start only its exact queued ticket. Repeated runs reuse the same source identity; active/paused tickets are not restarted or resumed. Uncertain responses stop without retry. --watch follows status; Ctrl-C stops watching, not the ticket. --json --watch emits response envelopes as NDJSON.",
		RunE: func(cmd *cobra.Command, args []string) error {
			parsed, err := readLocalTicket(args[0])
			if err != nil {
				return a.emit(failure("invalid_ticket", err.Error(), commandHelpAction(cmd)))
			}
			if a.expectedDraftDigest != "" && parsed.Digest != a.expectedDraftDigest {
				return a.emit(failure("invalid_ticket", "draft changed after preview; inspect it before submitting", commandHelpAction(cmd)))
			}
			submitted := a.request("ticket.submit", "", params(map[string]any{"source": string(parsed.Source), "project": project, "new": false}, a.channel))
			if !submitted.OK {
				if uncertainCLIResponse(submitted) {
					submitted.Mutation = api.Mutation{Attempted: true, Kind: "ticket_submit"}
					submitted.NextAction = &domain.NextAction{Code: "inspect_submission", Argv: []string{binaryForChannel(a.channel), "tickets", "--project", project}}
				}
				return a.emit(submitted)
			}
			var selected selectableTicket
			if json.Unmarshal(submitted.Data, &selected) != nil || !validSelectionID(selected.ID) || selected.Project != project || selected.Channel != a.channel || !selected.State.Valid() || !submitted.Mutation.Attempted || submitted.Mutation.Kind != "ticket_submit" || submitted.Mutation.Identity != selected.ID || selected.State != domain.StateQueued && !submitted.Mutation.Observed {
				response := failure("invalid_response", "submission identity could not be authenticated; no start was attempted", []string{binaryForChannel(a.channel), "tickets", "--project", project})
				response.Mutation = api.Mutation{Attempted: true, Kind: "ticket_submit"}
				return a.emit(response)
			}
			result := submitted
			switch selected.State {
			case domain.StatePaused, domain.StateBlocked, domain.StateStopping, domain.StateCancelling:
				response := failure("operator_action_required", "existing ticket requires an explicit control action; run did not resume, recover, or restart it", []string{binaryForChannel(a.channel), "status", selected.ID})
				response.Mutation = submitted.Mutation
				response.Data = submitted.Data
				return a.emit(response)
			}
			if selected.State == domain.StateQueued {
				values := map[string]any{}
				if estimates {
					values["accept_cost_estimates"] = true
				}
				result = a.request("ticket.start", selected.ID, params(values, a.channel))
				if !result.OK {
					// Submission is already durable even when start was refused.
					result.Mutation = api.Mutation{Attempted: true, Kind: "ticket_run", Identity: selected.ID}
					if uncertainCLIResponse(result) {
						result.NextAction = &domain.NextAction{Code: "inspect_start", Argv: []string{binaryForChannel(a.channel), "status", selected.ID}}
					}
					return a.emit(result)
				}
				var started selectableTicket
				if json.Unmarshal(result.Data, &started) != nil || started.ID != selected.ID || started.Project != project || started.Channel != a.channel || started.State != domain.StatePlanning || result.Mutation.Kind != "ticket_start" || result.Mutation.Identity != string(a.channel)+"/"+project+"/"+selected.ID+"/planning" {
					response := failure("invalid_response", "start response identity does not match the submitted ticket; inspect its status", []string{binaryForChannel(a.channel), "status", selected.ID})
					response.Mutation = api.Mutation{Attempted: true, Kind: "ticket_run", Identity: selected.ID}
					return a.emit(response)
				}
			}
			if err := a.emit(result); err != nil {
				return err
			}
			if watch && a.last != nil && a.last.OK {
				return a.watchStatus(cmd.Context(), selected.ID)
			}
			return nil
		},
	}
	command.Flags().StringVar(&project, "project", "", "registered project name")
	command.Flags().BoolVar(&watch, "watch", false, "follow the exact ticket after submission/start")
	command.Flags().BoolVar(&estimates, "accept-cost-estimates", false, "accept estimated (not verified) costs for a queued ticket; not a hard dollar cap")
	_ = command.MarkFlagRequired("project")
	return command
}

func uncertainCLIResponse(response api.Response) bool {
	if response.Error == nil {
		return false
	}
	switch response.Error.Code {
	case "daemon_unavailable", "internal_error", "invalid_response", "protocol_incompatible":
		return true
	default:
		return false
	}
}
