package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/spf13/cobra"
)

func (a *app) configureDecisionSelection(root *cobra.Command) {
	for _, command := range root.Commands() {
		if command.Name() != "approve" && command.Name() != "reject" {
			continue
		}
		command.Use = strings.Replace(command.Use, "<ticket>", "[ticket]", 1)
		var project string
		var selectFlag bool
		command.Flags().StringVar(&project, "project", "", "scope interactive decision selection to a project")
		command.Flags().BoolVar(&selectFlag, "select", false, "inspect and confirm the exact reviewed head interactively")
		originalArgs, originalRun := command.Args, command.RunE
		command.Args = func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && a.canSelectInteractively() {
				return nil
			}
			return originalArgs(cmd, args)
		}
		command.RunE = func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 && !selectFlag && project == "" && !shortTicketPrefix(args[0]) {
				return originalRun(cmd, args)
			}
			fail := func(message string) error {
				return a.emit(failure("invalid_decision", message, commandHelpAction(cmd)))
			}
			if !a.canSelectInteractively() {
				return fail("decision selection requires a terminal; scripts must supply a full ticket ID and should bind --head")
			}
			input := ""
			if len(args) > 0 {
				input = args[0]
			}
			id, refused := a.selectTicket(cmd, input, project)
			if refused != nil {
				return a.emit(*refused)
			}
			response := a.request("ticket.status", id, params(map[string]any{"watch": false}, a.channel))
			if !response.OK {
				return a.emit(response)
			}
			var view struct {
				Ticket   selectableTicket `json:"ticket"`
				Evidence struct {
					Candidate struct {
						Head string `json:"head_sha"`
					} `json:"candidate"`
				} `json:"evidence"`
			}
			if json.Unmarshal(response.Data, &view) != nil || view.Ticket.ID != id || view.Ticket.Channel != a.channel || view.Ticket.Project == "" || project != "" && view.Ticket.Project != project || view.Ticket.State != domain.StateWaitingApproval || !api.ValidReviewedHead(view.Evidence.Candidate.Head) {
				return fail("selected ticket has no current approval candidate; refresh status before deciding")
			}
			head := view.Evidence.Candidate.Head
			if cmd.Flags().Changed("head") {
				requested, _ := cmd.Flags().GetString("head")
				if requested != head {
					return fail("the supplied head differs from the displayed candidate; no decision was sent")
				}
			}
			if _, err := fmt.Fprintf(a.errOut, "%s [%s/%s]\n%s\nReviewed head: %s\nInspect this commit's diff before deciding. Type %s to confirm this exact head; anything else cancels:\n", id, safeSelectionLabel(string(a.channel)), safeSelectionLabel(view.Ticket.Project), safeSelectionLabel(view.Ticket.Title), head, cmd.Name()); err != nil {
				return fail("could not display decision confirmation")
			}
			reader := a.input
			if reader == nil {
				reader = os.Stdin
			}
			answer, err := readSelectionAnswer(reader)
			if err != nil || cmd.Context().Err() != nil || strings.TrimSpace(answer) != cmd.Name() {
				return fail("decision cancelled; no approval or rejection was sent")
			}
			if err := cmd.Flags().Set("head", head); err != nil {
				return fail("could not bind the confirmed head")
			}
			return originalRun(cmd, []string{id})
		}
	}
}
