package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// Every route owns fresh commands and flag closures. The namespace changes
// discovery, never the authority or meaning of the underlying daemon request.
func (a *app) ticketCommand() *cobra.Command {
	group := a.ticketDraftCommand()
	group.Short = "Draft, start, inspect, and control tickets"
	list := a.ticketsCommand()
	list.Use = "list"
	group.AddCommand(a.submitCommand(), a.ticketStartCommand(), list, a.ticketViewCommand(), a.ticketWatchCommand(), a.logsCommand(), a.controlCommand("pause"), a.controlCommand("resume"), a.controlCommand("cancel"), a.controlCommand("take"), a.retryCommand(), a.recoverCommand(), a.approveCommand(), a.rejectCommand())
	return group
}

func (a *app) ticketStartCommand() *cobra.Command {
	// Keep submit/start composition in one implementation, including its exact
	// source digest, uncertain-result handling, and partial-success identity.
	command := a.runCommand()
	compose := command.RunE
	existing := a.startCommand()
	var file string
	command.Use = "start [ticket]"
	command.Short = "Start a queued ticket, or submit and start an exact saved file"
	command.Long = "Start an existing queued ticket by ID, or use --file with --project to submit and start. IDs and --file are mutually exclusive. Submission starts the deadline. Existing paused tickets are never silently resumed. --watch follows durable status; Ctrl-C detaches."
	// Project is required for file submission, but optional for ID selection.
	delete(command.Flags().Lookup("project").Annotations, cobra.BashCompOneRequiredFlag)
	command.Flags().StringVar(&file, "file", "", "saved Markdown ticket to submit and start (exclusive with ID)")
	command.Args = func(cmd *cobra.Command, args []string) error {
		if err := cobra.MaximumNArgs(1)(cmd, args); err != nil {
			return err
		}
		if cmd.Flags().Changed("file") {
			project, _ := cmd.Flags().GetString("project")
			if len(args) != 0 || strings.TrimSpace(file) == "" || strings.TrimSpace(project) == "" {
				return fmt.Errorf("use either a ticket ID or --file <ticket.md> --project <name>")
			}
			return nil
		}
		if len(args) == 0 && !a.canSelectInteractively() {
			return fmt.Errorf("supply a ticket ID or --file <ticket.md> --project <name>")
		}
		return nil
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("file") {
			return compose(cmd, []string{file})
		}
		estimates, _ := cmd.Flags().GetBool("accept-cost-estimates")
		if err := existing.Flags().Set("accept-cost-estimates", fmt.Sprint(estimates)); err != nil {
			return err
		}
		if err := existing.RunE(cmd, args); err != nil {
			return err
		}
		watch, _ := cmd.Flags().GetBool("watch")
		if watch && a.last != nil && a.last.OK {
			return a.watchStatus(cmd.Context(), args[0])
		}
		return nil
	}
	return command
}

// Selection metadata is centralized here for the two public route families.
// Setup descendants with similarly named commands are deliberately excluded.
func ticketSelectionCommands(root *cobra.Command, decisions bool) []*cobra.Command {
	allowed := map[string]bool{"start": true, "status": true, "show": true, "view": true, "watch": true, "logs": true, "pause": true, "resume": true, "recover": true, "cancel": true, "retry": true, "take": true}
	if decisions {
		allowed = map[string]bool{"approve": true, "reject": true}
	}
	var result []*cobra.Command
	for _, command := range root.Commands() {
		if command.Name() == "ticket" {
			result = append(result, ticketSelectionCommands(command, decisions)...)
		} else if allowed[command.Name()] {
			result = append(result, command)
		}
	}
	return result
}
