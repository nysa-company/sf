package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/ticket"
	"github.com/spf13/cobra"
)

func (a *app) newTicketDraftCommand() *cobra.Command {
	return &cobra.Command{Use: "new <ticket.md>", Short: "Create a ticket interactively after reviewing its full contents", Args: cobra.ExactArgs(1),
		Long: "Collect a title, problem and acceptance criteria, then preview a guarded ticket with a 1h/$10 ceiling. Explicit confirmation saves a new private file; existing files are never overwritten. No submission occurs. Noninteractive callers can use ticket template and ticket validate.",
		RunE: func(cmd *cobra.Command, args []string) error {
			fail := func(message string) error { return a.emit(failure("invalid_ticket", message, commandHelpAction(cmd))) }
			if !a.canSelectInteractively() {
				return fail("ticket new requires an interactive terminal; use ticket template and ticket validate for scripts")
			}
			path, err := filepath.Abs(args[0])
			if err != nil || strings.ContainsAny(path, "\x00\r\n\t") {
				return fail("invalid output path")
			}
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return fail("output already exists or cannot be inspected; choose a new path")
			}
			input := a.input
			if input == nil {
				input = os.Stdin
			}
			source, err := collectTicketDraft(input, a.errOut)
			if err != nil {
				return fail(err.Error())
			}
			if err := cmd.Context().Err(); err != nil {
				return fail("creation cancelled; no file written")
			}
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
			if err != nil {
				return fail("output could not be created exclusively; no existing file was overwritten")
			}
			_, writeErr := io.WriteString(file, source)
			if writeErr == nil {
				writeErr = file.Sync()
			}
			closeErr := file.Close()
			if writeErr != nil || closeErr != nil {
				response := failure("invalid_ticket", "draft write failed; a partial new file may remain, inspect it before retrying", commandHelpAction(cmd))
				response.Mutation = api.Mutation{Attempted: true, Kind: "ticket.draft.write"}
				return a.emit(response)
			}
			data, _ := json.Marshal(map[string]any{"ticket_draft": map[string]any{"path": path, "note": "Saved locally. Nothing was submitted or started. Review the file, then validate and submit when ready."}})
			return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{Attempted: true, Kind: "ticket.draft.write"}, Data: data})
		},
	}
}

func collectTicketDraft(input io.Reader, output io.Writer) (string, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 1024), 16*1024)
	read := func(prompt string) (string, error) {
		if _, err := fmt.Fprint(output, prompt); err != nil {
			return "", errors.New("could not display prompt; no file written")
		}
		if !scanner.Scan() {
			return "", errors.New("input ended or exceeded the line limit; no file written")
		}
		value := strings.TrimSpace(scanner.Text())
		if !utf8.ValidString(value) {
			return "", errors.New("input must be valid UTF-8; no file written")
		}
		for _, r := range value {
			if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
				return "", errors.New("input contains control characters; no file written")
			}
		}
		return value, nil
	}
	title, err := read("Title: ")
	if err != nil {
		return "", err
	}
	if title == "" {
		return "", errors.New("title is required; no file written")
	}
	problem, err := read("Problem, desired behavior and scope (one line): ")
	if err != nil {
		return "", err
	}
	if problem == "" {
		return "", errors.New("problem is required; no file written")
	}
	criteria := []string{}
	for len(criteria) < 32 {
		criterion, err := read("Acceptance criterion (blank to finish): ")
		if err != nil {
			return "", err
		}
		if criterion == "" {
			break
		}
		criteria = append(criteria, criterion)
	}
	if len(criteria) == 0 {
		return "", errors.New("at least one acceptance criterion is required; no file written")
	}
	source := "---\ntype: feature\nmerge: guarded\nmax_duration: 1h\nmax_cost_usd: 10\n---\n# " + title + "\n\n" + problem + "\n\n## Acceptance\n- " + strings.Join(criteria, "\n- ") + "\n"
	if _, err := ticket.Parse(strings.NewReader(source)); err != nil {
		return "", errors.New("answers do not form a valid Markdown ticket; no file written")
	}
	if _, err := fmt.Fprintf(output, "\nReview the complete draft:\n\n%s\nThe deadline starts at submission. Edit the saved file to change limits.\n", source); err != nil {
		return "", errors.New("preview failed; no file written")
	}
	confirmation, err := read("Save this draft? Type yes to save; anything else cancels: ")
	if err != nil {
		return "", err
	}
	if confirmation != "yes" {
		return "", errors.New("creation cancelled; no file written")
	}
	return source, nil
}
