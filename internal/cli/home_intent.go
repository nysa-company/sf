package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strconv"
	"strings"
)

func (a *app) homeAnswer(cmd *cobra.Command, prompt string) (string, error) {
	if cmd.Context().Err() != nil {
		return "", cmd.Context().Err()
	}
	if _, err := io.WriteString(a.errOut, prompt); err != nil {
		return "", err
	}
	input := a.input
	if input == nil {
		input = os.Stdin
	}
	answer, err := readBoundedAnswer(input, 16<<10)
	answer = strings.TrimSpace(answer)
	if err != nil || cmd.Context().Err() != nil || !authoring.SafeText(answer, 16<<10) {
		return "", errors.New("home cancelled or invalid input")
	}
	return answer, nil
}
func (a *app) homeIntent(cmd *cobra.Command, project, model string) error {
	fail := func(message string) error {
		return a.emit(failure("invalid_argument", message, []string{binaryName(), "home", "--no-ai"}))
	}
	var err error
	if project == "" {
		project, err = a.homeAnswer(cmd, "Registered project name: ")
		if err != nil {
			return fail("home cancelled")
		}
	}
	if !homeProjectPattern.MatchString(project) {
		return fail("a registered project is required")
	}
	if model == "" {
		model, err = a.homeAnswer(cmd, "Claude model for bounded intent interpretation: ")
		if err != nil {
			return fail("home cancelled")
		}
	}
	if !contracts.AuthoringID(model) {
		return fail("an explicit supported Claude model is required")
	}
	request, err := a.homeAnswer(cmd, "What would you like to do? (Use home --no-ai for numbered navigation): ")
	if err != nil || request == "" {
		return fail("home cancelled; no inference requested")
	}
	response := a.request("authoring.create", "", params(map[string]any{"project": project, "model": model, "purpose": "home_intent", "context_files": []string{}}, a.channel))
	if !response.OK {
		return a.emit(response)
	}
	var envelope struct {
		Session authoringSessionView `json:"authoring_session"`
	}
	if json.Unmarshal(response.Data, &envelope) != nil {
		return fail("invalid authoring session")
	}
	session := envelope.Session
	if !contracts.AuthoringID(session.ID) || session.Channel != string(a.channel) || session.Project != project || session.Purpose != "home_intent" || session.Capability.Identity.Provider != "claude" || session.Capability.Identity.Model != model || session.ContextDigest != contracts.AuthoringDigest(nil) || len(session.ContextFiles) != 0 {
		return fail("authoring identity mismatch; no inference requested")
	}
	consent, err := a.homeAnswer(cmd, fmt.Sprintf("Session %s; project %s; Claude / %s. One SF intent turn, 90s maximum, bounded internal turns/retries; cost unknown, not one API request. No tools or repository context. Send request? Type send: ", session.ID, project, model))
	if err != nil || consent != "send" {
		return fail("home cancelled; no inference requested")
	}
	key := requestID()
	if err := a.announceAuthoringTurn(session.ID, key); err != nil {
		return fail("could not display exact turn recovery command; no inference requested")
	}
	response = a.request("authoring.turn", "", params(map[string]any{"session": session.ID, "key": key, "prompt": request, "context_digest": session.ContextDigest}, a.channel))
	if !response.OK {
		a.cancelAuthoring(session.ID, key)
		return a.emit(a.authoringRecoveryResponse(response, session.ID, key))
	}
	result, err := a.waitAuthoringPurpose(cmd.Context(), session.ID, key, "home_intent", project, response)
	if err != nil {
		a.cancelAuthoring(session.ID, key)
		return a.emit(a.authoringRecoveryResponse(failure("authoring_unavailable", "intent unavailable; cancellation requested independently, no ticket action taken", nil), session.ID, key))
	}
	return a.dispatchHomeIntent(cmd, result.Intent, project, model)
}

func (a *app) dispatchHomeIntent(cmd *cobra.Command, intent *contracts.AuthoringIntent, project, model string) error {
	fail := func(message string) error {
		return a.emit(failure("invalid_argument", message, []string{binaryName(), "home", "--no-ai"}))
	}
	if authoring.ValidatePurpose("home_intent", contracts.AuthoringResult{Kind: "home_intent", Intent: intent}) != nil || !homeProjectPattern.MatchString(project) {
		return fail("unsupported intent; no ticket action taken")
	}
	if cmd.Context().Err() != nil {
		return fail("home cancelled; no ticket action taken")
	}
	if intent.Action == "new" {
		return a.dispatchHome(cmd, []string{"ticket", "new", "--project", project, "--model", model})
	}
	if intent.Action == "list" {
		return a.dispatchHome(cmd, []string{"ticket", "list", "--project", project})
	}
	// Only fresh owner-scoped inventory resolves model text to ticket identity.
	response := a.request("ticket.status", "", params(map[string]any{"watch": false, "project": project}, a.channel))
	if !response.OK {
		return a.emit(response)
	}
	var inventory struct {
		Channel domain.Channel     `json:"channel"`
		Tickets []selectableTicket `json:"tickets"`
	}
	if json.Unmarshal(response.Data, &inventory) != nil || inventory.Channel != a.channel || inventory.Tickets == nil || len(inventory.Tickets) >= 1000 {
		return fail("ticket inventory unavailable or incomplete")
	}
	seen := map[string]bool{}
	var matches []selectableTicket
	for _, item := range inventory.Tickets {
		ref := domain.TicketRef{Channel: item.Channel, Project: domain.ProjectID(item.Project), Ticket: domain.TicketID(item.ID)}
		if ref.Validate() != nil || item.Channel != a.channel || item.Project != project || !item.State.Valid() || !validSelectionID(item.ID) || seen[item.ID] || len(item.Title) > 4096 {
			return fail("inconsistent ticket inventory; no action taken")
		}
		seen[item.ID] = true
		selector := intent.Selector
		if selector == "" || item.ID == selector || shortTicketPrefix(selector) && strings.HasPrefix(strings.TrimPrefix(item.ID, "SF-"), strings.TrimPrefix(selector, "SF-")) || strings.Contains(strings.ToLower(item.Title), strings.ToLower(selector)) {
			matches = append(matches, item)
		}
	}
	if len(matches) == 0 {
		return fail("no matching ticket; use ticket list to inspect current titles and IDs")
	}
	selected := matches[0]
	if len(matches) > 1 || intent.Selector == "" {
		if _, err := fmt.Fprintln(a.errOut, "Choose the actual ticket (q cancels):"); err != nil {
			return fail("could not display selection")
		}
		for i, item := range matches {
			if _, err := fmt.Fprintf(a.errOut, "%d) %s [%s] %s\n", i+1, item.ID, item.State, safeSelectionLabel(item.Title)); err != nil {
				return fail("could not display selection")
			}
		}
		answer, err := a.homeAnswer(cmd, "Ticket number: ")
		index, e := strconv.Atoi(answer)
		if err != nil || e != nil || index < 1 || index > len(matches) {
			return fail("selection cancelled; no ticket action taken")
		}
		selected = matches[index-1]
	}
	switch intent.Action {
	case "start", "pause", "cancel":
		confirmation, err := a.homeAnswer(cmd, fmt.Sprintf("Actual ticket: %s\nTitle: %s\nProject/channel: %s/%s\nCurrent state: %s\nProposed action: %s\nType %s %s to confirm this exact action: ", selected.ID, safeDraftPreview(selected.Title), project, a.channel, selected.State, intent.Action, intent.Action, selected.ID))
		if err != nil || confirmation != intent.Action+" "+selected.ID {
			return fail("action cancelled; no ticket mutation requested")
		}
	}
	return a.dispatchHome(cmd, []string{"ticket", intent.Action, selected.ID, "--project", project})
}
