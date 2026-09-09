package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/mattn/go-isatty"
	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/spf13/cobra"
)

type selectableTicket struct {
	ID      string         `json:"ticket"`
	Title   string         `json:"title"`
	Project string         `json:"project"`
	Channel domain.Channel `json:"channel"`
	State   domain.State   `json:"state"`
}

func (a *app) canSelectInteractively() bool {
	if a.json {
		return false
	}
	if a.interactive != nil {
		return a.interactive()
	}
	return isatty.IsTerminal(os.Stdin.Fd()) && isatty.IsTerminal(os.Stderr.Fd())
}

// Selection only resolves an identity, never authorizes a transition. The
// daemon rechecks state and authority for the resulting full ticket ID.
// Approval/rejection use a separate candidate-bound confirmation flow.
func (a *app) configureTicketSelection(root *cobra.Command) {
	allowed := map[string]bool{"start": true, "status": true, "show": true, "logs": true, "pause": true, "resume": true, "recover": true, "cancel": true, "retry": true, "take": true}
	for _, command := range root.Commands() {
		if !allowed[command.Name()] {
			continue
		}
		command.Use = strings.Replace(command.Use, "<ticket>", "[ticket]", 1)
		command.Long = command.Short + ".\n\nUse a full ticket ID or a unique 6–31 character lowercase hex prefix.\nOmit the ID in a terminal to choose by title; q cancels without an action.\nPiped input and --json require an explicit ID or unique prefix.\nApproval and rejection use a separate candidate-bound confirmation flow."
		if command.Name() == "status" {
			command.Long += "\nPlain status lists all tickets; use --select for the interactive picker."
		}
		var project string
		var selectFlag bool
		command.Flags().StringVar(&project, "project", "", "scope ticket selection to a registered project")
		if command.Name() == "status" {
			command.Flags().BoolVar(&selectFlag, "select", false, "select a ticket interactively instead of listing all tickets")
		}
		originalArgs, originalRun := command.Args, command.RunE
		command.Args = func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 && a.canSelectInteractively() {
				return nil
			}
			return originalArgs(cmd, args)
		}
		command.RunE = func(cmd *cobra.Command, args []string) error {
			// Keep the established no-argument status list behavior.
			if cmd.Name() == "status" && len(args) == 0 && !selectFlag {
				if project != "" {
					watch, _ := cmd.Flags().GetBool("watch")
					if watch {
						return a.watchProjectStatus(cmd.Context(), project)
					}
					return a.emit(a.request("ticket.status", "", params(map[string]any{"project": project, "watch": false}, a.channel)))
				}
				return originalRun(cmd, args)
			}
			input := ""
			if len(args) > 0 {
				input = args[0]
			}
			// Exact IDs preserve the direct command path, including legacy IDs.
			if input != "" && !shortTicketPrefix(input) && project == "" {
				return originalRun(cmd, args)
			}
			id, failureResponse := a.selectTicket(cmd, input, project)
			if failureResponse != nil {
				return a.emit(*failureResponse)
			}
			return originalRun(cmd, []string{id})
		}
	}
}

func shortTicketPrefix(value string) bool {
	value = strings.TrimPrefix(value, "SF-")
	if len(value) < 6 || len(value) >= 32 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (a *app) selectTicket(cmd *cobra.Command, input, project string) (string, *api.Response) {
	fail := func(message string) (string, *api.Response) {
		response := failure("invalid_ticket_reference", message, commandHelpAction(cmd))
		return "", &response
	}
	if input == "" && !a.canSelectInteractively() {
		return fail("supply a ticket ID; interactive selection requires a terminal and cannot be used with --json")
	}
	response := a.request("ticket.status", "", params(map[string]any{"watch": false, "project": project}, a.channel))
	if !response.OK {
		return "", &response
	}
	var inventory struct {
		Channel domain.Channel     `json:"channel"`
		Tickets []selectableTicket `json:"tickets"`
	}
	if err := json.Unmarshal(response.Data, &inventory); err != nil || inventory.Channel != a.channel || inventory.Tickets == nil || len(inventory.Tickets) >= 1000 {
		return fail("ticket inventory is invalid or may be incomplete; use the full ticket ID without a project filter")
	}
	seen := map[string]bool{}
	var matches []selectableTicket
	prefix := strings.TrimPrefix(input, "SF-")
	for _, item := range inventory.Tickets {
		ref := domain.TicketRef{Channel: item.Channel, Project: domain.ProjectID(item.Project), Ticket: domain.TicketID(item.ID)}
		if ref.Validate() != nil || item.Channel != a.channel || !item.State.Valid() || !validSelectionID(item.ID) || seen[item.ID] || project != "" && item.Project != project {
			return fail("ticket inventory contains an inconsistent identity; no action was taken")
		}
		seen[item.ID] = true
		if input == "" || item.ID == input || shortTicketPrefix(input) && strings.HasPrefix(strings.TrimPrefix(item.ID, "SF-"), prefix) {
			matches = append(matches, item)
		}
	}
	if len(matches) == 0 {
		return fail("no matching ticket was found in this channel/project; run tickets to inspect available IDs")
	}
	if input != "" && len(matches) == 1 {
		return matches[0].ID, nil
	}
	if !a.canSelectInteractively() {
		return fail("ticket prefix is ambiguous; use a longer prefix, full ID, or --project")
	}
	if _, err := fmt.Fprintf(a.errOut, "Select a ticket for %s (enter a number; q cancels):\n", cmd.Name()); err != nil {
		return fail("could not display ticket selection")
	}
	for index, item := range matches {
		if _, err := fmt.Fprintf(a.errOut, "%d) %s  [%s/%s]  %s\n", index+1, item.ID, safeSelectionLabel(item.Project), safeSelectionLabel(string(item.State)), safeSelectionLabel(item.Title)); err != nil {
			return fail("could not display ticket selection")
		}
	}
	reader := a.input
	if reader == nil {
		reader = os.Stdin
	}
	if cmd.Context().Err() != nil {
		return fail("ticket selection cancelled; no action was taken")
	}
	line, err := readSelectionAnswer(reader)
	if err != nil || cmd.Context().Err() != nil {
		return fail("ticket selection cancelled; no action was taken")
	}
	answer := strings.TrimSpace(line)
	if answer == "q" || answer == "" {
		return fail("ticket selection cancelled; no action was taken")
	}
	index, err := strconv.Atoi(answer)
	if err != nil || index < 1 || index > len(matches) {
		return fail("invalid selection; no action was taken")
	}
	return matches[index-1].ID, nil
}

// Do not read ahead into a later confirmation prompt. A new Scanner per
// prompt can otherwise consume and discard the next answer from buffered input.
func readSelectionAnswer(reader io.Reader) (string, error) {
	return readBoundedAnswer(reader, 128)
}

func readBoundedAnswer(reader io.Reader, limit int) (string, error) {
	var value strings.Builder
	var one [1]byte
	for value.Len() <= limit {
		n, err := reader.Read(one[:])
		if n == 1 {
			if one[0] == '\n' {
				return value.String(), nil
			}
			value.WriteByte(one[0])
		}
		if err != nil {
			if err == io.EOF && value.Len() > 0 && value.Len() <= limit {
				return value.String(), nil
			}
			return "", err
		}
		if n == 0 {
			return "", io.ErrNoProgress
		}
	}
	return "", errors.New("selection answer exceeds bound")
}

func safeSelectionLabel(value string) string {
	runes := []rune(value)
	if len(runes) > 160 {
		runes = runes[:160]
	}
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, string(runes))
}

func validSelectionID(value string) bool {
	if !strings.HasPrefix(value, "SF-") || len(value) > 128 || len(value) <= 3 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
