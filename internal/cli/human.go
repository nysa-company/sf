package cli

import (
	"fmt"
	"io"
	"strings"
)

// renderHumanData is deliberately a tolerant projection of the wire data. It
// only reads fields currently emitted by the daemon and ignores additive
// fields, leaving unknown response shapes on the stable JSON fallback.
func renderHumanData(writer io.Writer, value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		_, err := fmt.Fprintf(writer, "OK\n%s\n", stableJSON(value))
		return err
	}
	if checks, ok := object["checks"].([]any); ok {
		return renderChecks(writer, checks)
	}
	if events, ok := object["events"].([]any); ok {
		return renderEvents(writer, object, events)
	}
	if tickets, ok := object["tickets"].([]any); ok {
		return renderTickets(writer, object, tickets)
	}
	if nested, ok := object["ticket"].(map[string]any); ok {
		return renderTicket(writer, nested, object["evidence"])
	}
	if _, hasState := object["state"]; hasState {
		return renderTicket(writer, object, object["evidence"])
	}
	_, err := fmt.Fprintf(writer, "OK\n%s\n", stableJSON(value))
	return err
}

func renderTickets(writer io.Writer, parent map[string]any, values []any) error {
	if _, err := fmt.Fprintf(writer, "Tickets (%s)\n", stringField(parent, "channel")); err != nil {
		return err
	}
	if len(values) == 0 {
		_, err := io.WriteString(writer, "No tickets.\n")
		return err
	}
	for _, value := range values {
		item, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if _, err := fmt.Fprintf(writer, "- %s  %s\n", stringField(item, "ticket"), stringField(item, "state")); err != nil {
			return err
		}
	}
	return nil
}

func renderTicket(writer io.Writer, ticket map[string]any, evidence any) error {
	id := stringField(ticket, "ticket")
	title := stringField(ticket, "title")
	if title != "" {
		if _, err := fmt.Fprintf(writer, "%s  %s\n", id, title); err != nil {
			return err
		}
	} else if id != "" {
		if _, err := fmt.Fprintln(writer, id); err != nil {
			return err
		}
	} else if _, err := io.WriteString(writer, "Ticket\n"); err != nil {
		return err
	}
	for _, field := range []struct{ label, key string }{
		{"Channel", "channel"}, {"Project", "project"}, {"State", "state"},
		{"Merge mode", "merge_mode"}, {"Resume state", "resume_state"},
		{"Version", "version"}, {"Runner epoch", "runner_epoch"},
	} {
		if text := displayField(ticket, field.key); text != "" {
			if _, err := fmt.Fprintf(writer, "%s: %s\n", field.label, text); err != nil {
				return err
			}
		}
	}
	if title != "" {
		if problem := stringField(ticket, "problem"); problem != "" {
			if _, err := fmt.Fprintf(writer, "Problem: %s\n", problem); err != nil {
				return err
			}
		}
		if acceptance, ok := ticket["acceptance"].([]any); ok {
			if _, err := fmt.Fprintf(writer, "Acceptance: %d item(s)\n", len(acceptance)); err != nil {
				return err
			}
		}
	}
	if evidenceMap, ok := evidence.(map[string]any); ok {
		if err := renderEvidence(writer, evidenceMap); err != nil {
			return err
		}
	}
	return nil
}

func renderEvidence(writer io.Writer, evidence map[string]any) error {
	if plan, ok := evidence["plan"].(map[string]any); ok {
		if proof := stringField(plan, "proof_kind"); proof != "" {
			if _, err := fmt.Fprintf(writer, "Proof: %s\n", proof); err != nil {
				return err
			}
		}
	}
	if candidate, ok := evidence["candidate"].(map[string]any); ok {
		if head := stringField(candidate, "head_sha"); head != "" {
			if _, err := fmt.Fprintf(writer, "Head: %s\n", head); err != nil {
				return err
			}
		}
	}
	if worktree, ok := evidence["worktree"].(map[string]any); ok {
		if branch := stringField(worktree, "branch"); branch != "" {
			if _, err := fmt.Fprintf(writer, "Branch: %s\n", branch); err != nil {
				return err
			}
		}
	}
	return nil
}

func renderEvents(writer io.Writer, parent map[string]any, events []any) error {
	if _, err := fmt.Fprintf(writer, "Logs: %s\n", stringField(parent, "ticket")); err != nil {
		return err
	}
	if len(events) == 0 {
		_, err := io.WriteString(writer, "No events.\n")
		return err
	}
	for _, value := range events {
		event, ok := value.(map[string]any)
		if !ok {
			continue
		}
		from, to := stringField(event, "from"), stringField(event, "to")
		transition := strings.TrimSpace(from + " -> " + to)
		if from == "" && to == "" {
			transition = stringField(event, "trigger")
		}
		if _, err := fmt.Fprintf(writer, "- #%s %s\n", displayField(event, "id"), transition); err != nil {
			return err
		}
	}
	return nil
}

func renderChecks(writer io.Writer, checks []any) error {
	if _, err := io.WriteString(writer, "Doctor\n"); err != nil {
		return err
	}
	for _, value := range checks {
		check, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if _, err := fmt.Fprintf(writer, "%s: %s (%s)\n", stringField(check, "id"), stringField(check, "status"), stringField(check, "summary")); err != nil {
			return err
		}
	}
	return nil
}

func stringField(value map[string]any, key string) string {
	text, _ := value[key].(string)
	return text
}

func displayField(value map[string]any, key string) string {
	switch item := value[key].(type) {
	case string:
		return item
	case float64:
		return fmt.Sprintf("%.0f", item)
	default:
		return ""
	}
}
