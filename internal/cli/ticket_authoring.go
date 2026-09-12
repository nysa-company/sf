package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/spf13/cobra"
)

type authoringSessionView struct {
	Channel       string                        `json:"channel"`
	ID            string                        `json:"id"`
	Project       string                        `json:"project"`
	Purpose       string                        `json:"purpose"`
	Capability    contracts.AuthoringCapability `json:"capability"`
	ContextDigest string                        `json:"context_digest"`
	ContextFiles  []string                      `json:"context_files"`
}
type authoringTurnView struct {
	Channel string                     `json:"channel"`
	Project string                     `json:"project"`
	Purpose string                     `json:"purpose"`
	Session string                     `json:"session"`
	Key     string                     `json:"key"`
	State   string                     `json:"state"`
	Outcome string                     `json:"outcome"`
	Result  *contracts.AuthoringResult `json:"result"`
}

func (a *app) authorTicketDraft(cmd *cobra.Command, args []string, project, model string, files []string) error {
	fail := func(message string) error {
		return a.emit(failure("invalid_argument", message, commandHelpAction(cmd)))
	}
	input := a.input
	if input == nil {
		input = os.Stdin
	}
	read := func(prompt string) (string, error) {
		if cmd.Context().Err() != nil {
			return "", cmd.Context().Err()
		}
		if _, err := io.WriteString(a.errOut, prompt); err != nil {
			return "", err
		}
		answer, err := readBoundedAnswer(input, 16<<10)
		answer = strings.TrimSpace(answer)
		if err != nil || !authoring.SafeText(answer, 16<<10) {
			return "", errors.New("input cancelled or invalid")
		}
		return answer, nil
	}
	path := ""
	var err error
	if len(args) > 0 {
		path, err = draftOutputPath(args[0])
		if err != nil {
			return fail(err.Error())
		}
	}
	if project == "" {
		project, err = read("Registered project name: ")
		if err != nil {
			return fail("creation cancelled")
		}
	}
	if !homeProjectPattern.MatchString(project) {
		return fail("a registered project name is required")
	}
	if model == "" {
		model, err = read("Claude model for drafting (no provider substitution): ")
		if err != nil {
			return fail("creation cancelled")
		}
	}
	if !contracts.AuthoringID(model) {
		return fail("an explicit supported Claude model is required")
	}
	response := a.request("authoring.create", "", params(map[string]any{"project": project, "model": model, "purpose": "ticket_draft", "context_files": files}, a.channel))
	if !response.OK {
		return a.emit(response)
	}
	var envelope struct {
		Session authoringSessionView `json:"authoring_session"`
	}
	if json.Unmarshal(response.Data, &envelope) != nil {
		return fail("invalid authoring session; no inference requested")
	}
	session := envelope.Session
	if !contracts.AuthoringID(session.ID) || session.Channel != string(a.channel) || session.Project != project || session.Purpose != "ticket_draft" || session.Capability.Identity.Provider != "claude" || session.Capability.Identity.Model != model || len(session.ContextDigest) != 64 || len(session.ContextFiles) != len(files) {
		return fail("authoring session identity mismatch; no inference requested")
	}
	for i, path := range session.ContextFiles {
		if path != files[i] || !authoring.SafeText(path, 1024) || strings.ContainsAny(path, "\n\t") {
			return fail("reference file consent mismatch; no inference requested")
		}
	}
	if _, err := fmt.Fprintf(a.errOut, "Authoring session: %s\nProject: %s\nProvider/model: Claude / %s\nNo tools. At most 4 SF authoring turns, 90 seconds each; bounded internal turns/retries. Cost is unknown; one SF turn is not one API request. Nothing is submitted.\nReference files:\n", session.ID, project, model); err != nil {
		return fail("could not display consent; no inference requested")
	}
	for _, path := range session.ContextFiles {
		if _, err := fmt.Fprintf(a.errOut, "- %s\n", path); err != nil {
			return fail("could not display reference files; no inference requested")
		}
	}
	prompt, err := read("Describe the small change (or cancel): ")
	if err != nil || prompt == "" || prompt == "cancel" {
		return fail("creation cancelled; no inference requested")
	}
	dialogue := prompt
	for turn := 0; turn < contracts.AuthoringTurnLimit; turn++ {
		consent, err := read("Send this authoring turn to Claude? Type send, or cancel: ")
		if err != nil || consent != "send" {
			return fail("creation cancelled; no new inference requested")
		}
		key := requestID()
		if err := a.announceAuthoringTurn(session.ID, key); err != nil {
			return fail("could not display exact turn recovery command; no inference requested")
		}
		response = a.request("authoring.turn", "", params(map[string]any{"session": session.ID, "key": key, "prompt": prompt, "context_digest": session.ContextDigest}, a.channel))
		if !response.OK {
			a.cancelAuthoring(session.ID, key)
			return a.emit(a.authoringRecoveryResponse(response, session.ID, key))
		}
		result, pollErr := a.waitAuthoringPurpose(cmd.Context(), session.ID, key, "ticket_draft", project, response)
		if pollErr != nil {
			a.cancelAuthoring(session.ID, key)
			return a.emit(a.authoringRecoveryResponse(failure("authoring_unavailable", "authoring interrupted or unavailable; cancellation requested independently. Inspect the exact turn; do not resend it", nil), session.ID, key))
		}
		if result.Kind == "question" {
			if _, err := fmt.Fprintf(a.errOut, "Claude asks: %s\n", result.Question); err != nil {
				return fail("could not display question")
			}
			prompt, err = read("Answer (or cancel): ")
			if err != nil || prompt == "cancel" {
				return fail("creation cancelled; no new inference requested")
			}
			dialogue += "\nProvider question (reference): " + result.Question + "\nUser answer: " + prompt
			prompt = dialogue
			continue
		}
		source, err := authoring.Markdown(result)
		if err != nil {
			return fail("provider draft was invalid; use ticket new --no-ai")
		}
		if _, err := fmt.Fprintf(a.errOut, "Complete draft preview (provider proposal; review all assumptions):\n%s\n", source); err != nil {
			return fail("could not display complete preview; no file written")
		}
		choice, err := read("Type save, refine, or cancel: ")
		if err != nil {
			return fail("creation cancelled; no file written")
		}
		if choice == "refine" {
			prompt, err = read("Requested refinement: ")
			if err != nil {
				return fail("creation cancelled")
			}
			dialogue += "\nPrevious reviewed draft (reference):\n" + source + "\nRequested refinement:\n" + prompt
			prompt = dialogue
			continue
		}
		if choice != "save" {
			return fail("creation cancelled; no file written")
		}
		if path == "" {
			path, err = draftOutputPath(draftFilename(result.Title))
			if err != nil {
				return fail(err.Error())
			}
		}
		if err := a.saveTicketDraft(cmd, path, source); err != nil {
			return err
		}
		if a.last != nil && !a.last.OK {
			return nil
		}
		choice, err = read("Saved. Separately submit and start for project " + project + " with guarded merge, 1h duration and $10 policy budget (not a hard billing cap)? Type start, otherwise leave saved: ")
		if err != nil || choice != "start" {
			return nil
		}
		a.expectedDraftDigest = contracts.AuthoringDigest([]byte(source))
		defer func() { a.expectedDraftDigest = "" }()
		return a.dispatchHome(cmd, []string{"ticket", "start", "--file", path, "--project", project})
	}
	return fail("authoring turn budget exhausted; no automatic retry. Use ticket new --no-ai to continue manually")
}

func (a *app) cancelAuthoring(session, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clone := *a
	clone.ctx = ctx
	clone.request("authoring.cancel", "", params(map[string]any{"session": session, "key": key}, a.channel))
}
func (a *app) waitAuthoringPurpose(ctx context.Context, session, key, purpose, project string, response api.Response) (contracts.AuthoringResult, error) {
	ctx, cancel := context.WithTimeout(ctx, contracts.AuthoringTurnTimeout+65*time.Second)
	defer cancel()
	clone := *a
	clone.ctx = ctx
	for {
		if ctx.Err() != nil || !response.OK {
			return contracts.AuthoringResult{}, errors.New("authoring unavailable")
		}
		var envelope struct {
			Turn authoringTurnView `json:"authoring_turn"`
		}
		if json.Unmarshal(response.Data, &envelope) != nil {
			return contracts.AuthoringResult{}, errors.New("invalid authoring receipt")
		}
		turn := envelope.Turn
		if turn.Session != session || turn.Key != key || turn.Channel != string(a.channel) || turn.Purpose != purpose || turn.Project != project {
			return contracts.AuthoringResult{}, errors.New("authoring identity mismatch")
		}
		switch turn.State {
		case "completed":
			if turn.Outcome != "success" || turn.Result == nil || authoring.ValidatePurpose(purpose, *turn.Result) != nil {
				return contracts.AuthoringResult{}, errors.New("authoring failed")
			}
			return authoring.SanitizePurpose(purpose, *turn.Result, redact.NewPolicy("", nil))
		case "reserved", "launched":
		default:
			return contracts.AuthoringResult{}, errors.New("authoring outcome uncertain")
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return contracts.AuthoringResult{}, ctx.Err()
		case <-timer.C:
		}
		response = clone.request("authoring.status", "", params(map[string]any{"session": session, "key": key}, a.channel))
	}
}
