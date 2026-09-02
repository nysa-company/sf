package cli

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// renderHumanData is a tolerant projection of the current wire shapes. It
// only reads fields owned by the CLI contract and ignores additive fields.
// Unknown response shapes retain the stable JSON fallback.
func renderHumanData(writer io.Writer, value any) error {
	object, ok := value.(map[string]any)
	if !ok {
		_, err := fmt.Fprintf(writer, "OK\n%s\n", stableJSON(value))
		return err
	}
	if _, hasChecks := object["checks"]; hasChecks {
		return renderDoctor(writer, object)
	}
	if events, ok := object["events"].([]any); ok {
		return renderEvents(writer, object, events)
	}
	if tickets, ok := object["tickets"].([]any); ok {
		return renderTickets(writer, object, tickets)
	}
	if nested, ok := object["ticket"].(map[string]any); ok {
		return renderTicket(writer, nested, object["evidence"], object)
	}
	if _, hasState := object["state"]; hasState {
		return renderTicket(writer, object, object["evidence"], object)
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
		if action, ok := item["next_action"].(map[string]any); ok {
			if err := renderAction(writer, "  Next", action); err != nil {
				return err
			}
		}
	}
	return nil
}

func renderTicket(writer io.Writer, ticket map[string]any, evidence any, context map[string]any) error {
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
		{"Version", "version"}, {"Runner epoch", "runner_epoch"}, {"Blocker", "blocked_code"},
	} {
		if text := displayField(ticket, field.key); text != "" {
			if _, err := fmt.Fprintf(writer, "%s: %s\n", field.label, text); err != nil {
				return err
			}
		}
	}
	if operator, ok := context["operator"].(map[string]any); ok {
		if err := renderOperator(writer, operator); err != nil {
			return err
		}
	}
	if takeover, ok := context["takeover"].(map[string]any); ok {
		if err := renderTakeover(writer, takeover); err != nil {
			return err
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
	if action, ok := context["next_action"].(map[string]any); ok {
		if err := renderAction(writer, "Next", action); err != nil {
			return err
		}
	}
	if evidenceMap, ok := evidence.(map[string]any); ok {
		if err := renderEvidence(writer, evidenceMap); err != nil {
			return err
		}
	}
	return nil
}

func renderTakeover(writer io.Writer, takeover map[string]any) error {
	// An absolute path is intentionally displayed only in the authenticated
	// `take` response. Ordinary status evidence omits it; the purpose of this
	// handoff view is to give the operator one directly usable checkout.
	if !boolField(takeover, "registered") || stringField(takeover, "path") == "" {
		if _, err := io.WriteString(writer, "Takeover worktree: not created yet\n"); err != nil {
			return err
		}
		return nil
	}
	if _, err := fmt.Fprintf(writer, "Takeover worktree: %s\n", stringField(takeover, "path")); err != nil {
		return err
	}
	for _, field := range []struct{ label, key string }{
		{"Branch", "branch"}, {"Repository", "repository"}, {"Origin", "origin"}, {"Push origin", "push_origin"},
		{"Local base", "base_sha"}, {"Local head", "head_sha"}, {"Remote base", "remote_base_sha"}, {"Remote candidate", "remote_candidate_sha"},
		{"Change kind", "change_kind"}, {"Retained proof", "retained_proof_digest"}, {"Retained policy", "retained_policy_digest"},
		{"Retained version", "retained_version"}, {"Retained leader", "retained_leader_epoch"}, {"Retained runner", "retained_runner_epoch"},
	} {
		if text := displayField(takeover, field.key); text != "" {
			if _, err := fmt.Fprintf(writer, "%s: %s\n", field.label, text); err != nil {
				return err
			}
		}
	}
	if stringField(takeover, "remote_candidate_sha") == "" {
		if _, err := io.WriteString(writer, "Remote candidate: absent\n"); err != nil {
			return err
		}
	}
	remoteIdentity := "changed"
	if boolField(takeover, "remote_identity_exact") {
		remoteIdentity = "exact"
	}
	if _, err := fmt.Fprintf(writer, "Remote identity: %s\n", remoteIdentity); err != nil {
		return err
	}
	if changed, ok := takeover["changed_files"].([]any); ok && len(changed) > 0 {
		files := make([]string, 0, len(changed))
		for _, value := range changed {
			if text, ok := value.(string); ok && text != "" {
				files = append(files, text)
			}
		}
		if len(files) > 0 {
			if _, err := fmt.Fprintf(writer, "Changed files: %s\n", strings.Join(files, ", ")); err != nil {
				return err
			}
		}
	}
	return nil
}

func renderOperator(writer io.Writer, operator map[string]any) error {
	label, uid, username := stringField(operator, "label"), displayField(operator, "uid"), stringField(operator, "username")
	parts := make([]string, 0, 2)
	if label != "" {
		parts = append(parts, "label="+label)
	}
	if uid != "" {
		parts = append(parts, "uid="+uid)
	}
	if username != "" && username != label {
		parts = append(parts, "username="+username)
	}
	if len(parts) == 0 {
		return nil
	}
	_, err := fmt.Fprintf(writer, "Operator: %s\n", strings.Join(parts, ", "))
	return err
}

func renderEvidence(writer io.Writer, evidence map[string]any) error {
	if plan, ok := evidence["plan"].(map[string]any); ok {
		parts := []string{}
		for _, field := range []struct{ label, key string }{{"digest", "digest"}, {"proof", "proof_kind"}, {"acceptance", "acceptance_count"}, {"paths", "path_count"}, {"commands", "command_count"}, {"risks", "risk_count"}} {
			if value := displayField(plan, field.key); value != "" {
				parts = append(parts, field.label+"="+value)
			}
		}
		if len(parts) > 0 {
			if _, err := fmt.Fprintf(writer, "Plan: %s\n", strings.Join(parts, ", ")); err != nil {
				return err
			}
		}
	}
	if verification, ok := evidence["verification"].(map[string]any); ok {
		parts := []string{}
		for _, field := range []struct{ label, key string }{{"revision", "revision"}, {"checkpoint", "checkpoint_id"}, {"intent", "intent_digest"}, {"proof", "proof_digest"}} {
			if value := displayField(verification, field.key); value != "" {
				parts = append(parts, field.label+"="+value)
			}
		}
		if amended := displayField(verification, "amends_revision"); amended != "" {
			parts = append(parts, "amends="+amended)
		}
		if len(parts) > 0 {
			if _, err := fmt.Fprintf(writer, "Verification: %s\n", strings.Join(parts, ", ")); err != nil {
				return err
			}
		}
	}
	if candidate, ok := evidence["candidate"].(map[string]any); ok {
		parts := []string{}
		for _, field := range []struct{ label, key string }{{"generation", "generation"}, {"head", "head_sha"}, {"base", "base_sha"}, {"tree", "tree_sha"}} {
			if value := displayField(candidate, field.key); value != "" {
				parts = append(parts, field.label+"="+value)
			}
		}
		if len(parts) > 0 {
			if _, err := fmt.Fprintf(writer, "Candidate: %s\n", strings.Join(parts, ", ")); err != nil {
				return err
			}
		}
	}
	if worktree, ok := evidence["worktree"].(map[string]any); ok {
		parts := []string{}
		for _, field := range []struct{ label, key string }{{"branch", "branch"}, {"state", "state"}, {"head", "head_sha"}} {
			if value := displayField(worktree, field.key); value != "" {
				parts = append(parts, field.label+"="+value)
			}
		}
		// Deliberately omit worktree.path: human status must not expose an
		// absolute local path by default.
		if len(parts) > 0 {
			if _, err := fmt.Fprintf(writer, "Worktree: %s\n", strings.Join(parts, ", ")); err != nil {
				return err
			}
		}
	}
	if attempts, ok := evidence["phase_attempts"].([]any); ok && len(attempts) > 0 {
		if _, err := io.WriteString(writer, "Phase attempts:\n"); err != nil {
			return err
		}
		for _, value := range attempts {
			attempt, ok := value.(map[string]any)
			if !ok {
				continue
			}
			parts := []string{}
			for _, field := range []struct{ label, key string }{{"phase", "phase"}, {"attempt", "attempt"}, {"state", "state"}, {"outcome", "outcome"}} {
				if text := displayField(attempt, field.key); text != "" {
					parts = append(parts, field.label+"="+text)
				}
			}
			if len(parts) > 0 {
				if _, err := fmt.Fprintf(writer, "- %s\n", strings.Join(parts, ", ")); err != nil {
					return err
				}
			}
		}
	}
	if decisions, ok := evidence["operator_decisions"].([]any); ok && len(decisions) > 0 {
		if _, err := io.WriteString(writer, "Operator decisions:\n"); err != nil {
			return err
		}
		for _, value := range decisions {
			decision, ok := value.(map[string]any)
			if !ok {
				continue
			}
			parts := []string{}
			for _, field := range []struct{ label, key string }{{"id", "id"}, {"decision", "decision"}, {"reviewed_head", "reviewed_head"}} {
				if text := displayField(decision, field.key); text != "" {
					parts = append(parts, field.label+"="+text)
				}
			}
			if invalidated, ok := decision["invalidated"].(bool); ok {
				parts = append(parts, "invalidated="+strconv.FormatBool(invalidated))
			}
			if len(parts) > 0 {
				if _, err := fmt.Fprintf(writer, "- %s\n", strings.Join(parts, ", ")); err != nil {
					return err
				}
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

func renderDoctor(writer io.Writer, report map[string]any) error {
	if _, err := fmt.Fprintf(writer, "Doctor (channel: %s)\n", stringField(report, "channel")); err != nil {
		return err
	}
	if value, ok := report["guarded_eligible"].(bool); ok {
		if _, err := fmt.Fprintf(writer, "Guarded eligible: %t\n", value); err != nil {
			return err
		}
	}
	if value, ok := report["autonomous_eligible"].(bool); ok {
		if _, err := fmt.Fprintf(writer, "Autonomous eligible: %t\n", value); err != nil {
			return err
		}
	}
	if value, ok := report["credentials_stored_by_sf"].(bool); ok {
		if _, err := fmt.Fprintf(writer, "Credentials stored by sf: %t\n", value); err != nil {
			return err
		}
	}
	if checks, ok := report["checks"].([]any); ok {
		if _, err := io.WriteString(writer, "Checks:\n"); err != nil {
			return err
		}
		for _, value := range checks {
			check, ok := value.(map[string]any)
			if !ok {
				continue
			}
			if _, err := fmt.Fprintf(writer, "- %s: %s — %s\n", stringField(check, "id"), stringField(check, "status"), stringField(check, "summary")); err != nil {
				return err
			}
			if action, ok := check["next_action"].(map[string]any); ok {
				if err := renderAction(writer, "Action", action); err != nil {
					return err
				}
			}
		}
	}
	if auth, ok := report["authentication"].([]any); ok {
		if _, err := io.WriteString(writer, "Authentication:\n"); err != nil {
			return err
		}
		for _, value := range auth {
			status, ok := value.(map[string]any)
			if !ok {
				continue
			}
			parts := []string{stringField(status, "state")}
			if installed, ok := status["installed"].(bool); ok {
				parts = append(parts, "installed="+strconv.FormatBool(installed))
			}
			if authenticated, ok := status["authenticated"].(bool); ok {
				parts = append(parts, "authenticated="+strconv.FormatBool(authenticated))
			}
			if version := stringField(status, "version"); version != "" {
				parts = append(parts, "version="+version)
			}
			if authMode := stringField(status, "auth_mode"); authMode != "" {
				parts = append(parts, "auth_mode="+authMode)
			}
			if _, err := fmt.Fprintf(writer, "- %s: %s\n", stringField(status, "provider"), strings.Join(parts, ", ")); err != nil {
				return err
			}
			if reason := stringField(status, "reason"); reason != "" {
				if _, err := fmt.Fprintf(writer, "  Reason: %s\n", reason); err != nil {
					return err
				}
			}
			if action, ok := status["next_action"].(map[string]any); ok {
				if err := renderAction(writer, "Action", action); err != nil {
					return err
				}
			}
		}
	}
	if pair, ok := report["provider_pair"].(map[string]any); ok {
		if _, err := fmt.Fprintf(writer, "Provider pair (independent: %t)\n", boolField(pair, "independent")); err != nil {
			return err
		}
		for _, role := range []string{"builder", "reviewer"} {
			provider, ok := pair[role].(map[string]any)
			if !ok {
				continue
			}
			parts := []string{}
			for _, field := range []struct{ label, key string }{{"provider", "provider"}, {"model", "model"}, {"family", "family"}, {"version", "version"}, {"qualification", "qualification"}, {"auth_mode", "auth_mode"}} {
				if text := stringField(provider, field.key); text != "" {
					parts = append(parts, field.label+"="+text)
				}
			}
			if _, err := fmt.Fprintf(writer, "- %s: %s\n", roleLabel(role), strings.Join(parts, ", ")); err != nil {
				return err
			}
		}
	}
	return nil
}

func roleLabel(role string) string {
	switch role {
	case "builder":
		return "Builder"
	case "reviewer":
		return "Reviewer"
	default:
		return role
	}
}

func renderAction(writer io.Writer, label string, action map[string]any) error {
	argv, ok := action["argv"].([]any)
	if !ok || len(argv) == 0 {
		return nil
	}
	values := make([]string, 0, len(argv))
	for _, item := range argv {
		text, ok := item.(string)
		if !ok || text == "" {
			return nil
		}
		values = append(values, text)
	}
	_, err := fmt.Fprintf(writer, "  %s: %s\n", label, shellWords(values))
	return err
}

func boolField(value map[string]any, key string) bool {
	result, _ := value[key].(bool)
	return result
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
