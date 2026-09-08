package cli

import (
	"fmt"
	"io"

	"github.com/nysa-company/sf/internal/redact"
)

func renderReviewDiagnostic(writer io.Writer, review map[string]any) error {
	if !boolField(review, "available") {
		_, err := io.WriteString(writer, "Review diagnostic unavailable; no verdict inferred.\n")
		return err
	}
	if _, err := fmt.Fprintf(writer, "Recorded review: %s (attempt %s, ticket version %s; historical, not approval)\nReviewed head: %s\n", safeSelectionLabel(stringField(review, "decision")), displayField(review, "attempt"), displayField(review, "ticket_version"), safeSelectionLabel(stringField(review, "reviewed_head"))); err != nil {
		return err
	}
	truncated := boolField(review, "truncated")
	if findings, ok := review["findings"].([]any); ok {
		truncated = truncated || len(findings) > 5
		for i, raw := range findings {
			if i == 5 {
				break
			}
			if finding, ok := raw.(string); ok {
				finding = redact.String(finding)
				truncated = truncated || len([]rune(finding)) > 160
				if _, err := fmt.Fprintf(writer, "  Reviewer finding: %s\n", safeSelectionLabel(finding)); err != nil {
					return err
				}
			}
		}
	}
	if truncated {
		_, err := io.WriteString(writer, "Review diagnostic truncated.\n")
		return err
	}
	return nil
}
