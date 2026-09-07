package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// This picker selects a portable preference, never grants qualification,
// billing consent, or model independence. Installation/qualification remain
// separate checks before a ticket can execute.
func (a *app) selectInitialProviderPreset(ctx context.Context) (string, error) {
	if !a.canSelectInteractively() {
		return "", errors.New("provider selection requires a terminal; use an explicit --providers preset for scripts and JSON")
	}
	if ctx.Err() != nil {
		return "", errors.New("provider selection cancelled; no configuration changed")
	}
	if _, err := fmt.Fprint(a.errOut, "Choose provider preferences (Planner follows Builder):\n1) Codex Builder + Codex Reviewer\n2) Claude Builder + Codex Reviewer\n3) Codex Builder + Claude Reviewer\n4) Cursor Builder + Codex Reviewer\n5) Codex Builder + Cursor Reviewer\n6) Cursor Builder + Claude Reviewer\n7) Claude Builder + Cursor Reviewer\n8) Cursor Builder + Cursor Reviewer\nQualification must still prove the exact model families are independent and may invoke paid models.\nClaude requires explicit estimated-cost consent at ticket start.\nCursor is experimental and treats installed hooks as trusted dependencies. Use --models select during qualification to choose independent models across CLIs.\nChoose 1-8, or q to cancel without changes: "); err != nil {
		return "", err
	}
	reader := a.input
	if reader == nil {
		reader = os.Stdin
	}
	answer, err := readSelectionAnswer(reader)
	if err != nil || ctx.Err() != nil {
		return "", errors.New("provider selection cancelled; no configuration changed")
	}
	switch strings.TrimSpace(answer) {
	case "1":
		return "codex-codex", nil
	case "2":
		return "claude-codex", nil
	case "3":
		return "codex-claude", nil
	case "4":
		return "cursor-codex", nil
	case "5":
		return "codex-cursor", nil
	case "6":
		return "cursor-claude", nil
	case "7":
		return "claude-cursor", nil
	case "8":
		return "cursor-cursor", nil
	case "", "q":
		return "", errors.New("provider selection cancelled; no configuration changed")
	default:
		return "", errors.New("choose 1 through 8; no configuration changed")
	}
}
