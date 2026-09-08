package cli

import (
	"fmt"
	"io"
)

func renderRuntimeActivity(writer io.Writer, activity map[string]any) error {
	if !boolField(activity, "available") {
		return nil
	}
	items, _ := activity["observations"].([]any)
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, err := fmt.Fprintf(writer, "Last scheduler observation: %s at %s (ticket version %s; historical, not current state)\n", safeSelectionLabel(stringField(item, "outcome")), safeSelectionLabel(stringField(item, "observed_at")), displayField(item, "observed_ticket_version")); err != nil {
			return err
		}
		switch stringField(item, "outcome") {
		case "readiness_failed":
			if _, err := io.WriteString(writer, "Inspect doctor for the repository and ticket logs; this check did not establish runtime readiness.\n"); err != nil {
				return err
			}
		case "worker_failed":
			if _, err := io.WriteString(writer, "Inspect ticket logs and the current durable state before retrying; this observation does not authorize replay.\n"); err != nil {
				return err
			}
		}
	}
	return nil
}
