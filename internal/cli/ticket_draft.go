package cli

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"syscall"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/ticket"
	"github.com/spf13/cobra"
)

const ticketTemplate = `---
type: feature
merge: guarded
max_duration: 1h
max_cost_usd: 10
---
# Describe one small, concrete change

Describe the current behavior, desired behavior, and exact files or component
in scope. Replace this paragraph with your project's requirements. Do not
include credentials. Keep unrelated changes and new dependencies out of scope.

## Acceptance
- Replace with an observable success case and its expected result.
- Replace with an edge or failure case and its expected result.
- Existing tests continue to pass; unrelated files remain unchanged.
`

func (a *app) ticketDraftCommand() *cobra.Command {
	command := &cobra.Command{Use: "ticket", Short: "Prepare and validate Markdown tickets locally"}
	command.AddCommand(a.newTicketDraftCommand())
	command.AddCommand(a.importTicketCommand())
	command.AddCommand(&cobra.Command{
		Use: "template", Short: "Print an editable ticket template without submitting it", Args: cobra.NoArgs,
		Example: "  " + binaryForChannel(a.channel) + " ticket template > ticket.md",
		RunE: func(_ *cobra.Command, _ []string) error {
			data, _ := json.Marshal(map[string]any{"ticket_template": ticketTemplate})
			return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Data: data})
		},
	})
	command.AddCommand(&cobra.Command{
		Use: "validate <ticket.md>", Short: "Check ticket syntax locally before submission", Args: cobra.ExactArgs(1),
		Long: "Validate with the same Markdown parser used at submission. This does not check project policy, provider readiness, or ticket feasibility, and does not start the deadline.",
		RunE: func(_ *cobra.Command, args []string) error {
			parsed, err := readLocalTicket(args[0])
			if err != nil {
				return a.emit(failure("invalid_ticket", err.Error(), []string{binaryForChannel(a.channel), "ticket", "validate", "--help"}))
			}
			warnings := []string{}
			if len(parsed.Acceptance) == 0 {
				warnings = append(warnings, "Add observable acceptance criteria before submitting.")
			}
			if parsed.MaxDuration == 0 {
				warnings = append(warnings, "Duration is omitted; submission resolves the project deadline policy.")
			}
			if parsed.MaxCostMicroUSD == 0 {
				warnings = append(warnings, "Cost ceiling is omitted; submission resolves the project budget policy.")
			}
			data, _ := json.Marshal(map[string]any{"ticket_validation": map[string]any{
				"syntax": "valid", "title": parsed.Title, "acceptance_count": len(parsed.Acceptance), "source_digest": parsed.Digest,
				"warnings": warnings, "note": "Syntax only; project policy and runtime readiness are not checked. Nothing was submitted or started.",
			}})
			return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Data: data})
		},
	})
	return command
}

// Refuse special files before opening: validation must never block on a FIFO
// or device. The bounded shared parser remains the only Markdown grammar.
func readLocalTicket(path string) (ticket.Parsed, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() {
		return ticket.Parsed{}, errors.New("ticket must be a readable regular file, not a symlink or device")
	}
	if before.Size() > ticket.MaxSourceBytes {
		return ticket.Parsed{}, errors.New("ticket exceeds the 1 MiB limit")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return ticket.Parsed{}, errors.New("ticket file could not be opened")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return ticket.Parsed{}, errors.New("ticket file changed while opening")
	}
	parsed, err := ticket.Parse(file)
	if err != nil {
		// The parser may quote an invalid input value. Keep the diagnostic kind
		// but never echo that potentially secret-bearing value.
		reason, _, _ := strings.Cut(err.Error(), "\"")
		return ticket.Parsed{}, errors.New(strings.TrimSpace(safeSelectionLabel(reason)))
	}
	return parsed, nil
}
