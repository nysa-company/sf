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
	var multiline bool
	var noAI bool
	var project, model string
	var contextFiles []string
	var statusSession, statusTurn string
	command := &cobra.Command{Use: "new [ticket.md]", Short: "Create a ticket interactively after reviewing its full contents", Args: cobra.MaximumNArgs(1),
		Long: "Draft interactively with explicitly consented Claude authoring turns, or use --no-ai for the offline manual form. Preview a guarded ticket with a 1h duration and $10 policy budget, not a hard billing cap. Save requires confirmation and never overwrites; Start is a separate explicit action. Scripts use ticket template and ticket validate without inference.",
		RunE: func(cmd *cobra.Command, args []string) error {
			fail := func(message string) error { return a.emit(failure("invalid_ticket", message, commandHelpAction(cmd))) }
			if cmd.Flags().Changed("status") || cmd.Flags().Changed("turn") {
				if len(args) > 0 || noAI || multiline || project != "" || model != "" || len(contextFiles) > 0 {
					return fail("authoring status cannot create, save, refine or start a draft")
				}
				return a.readAuthoringStatus(cmd, statusSession, statusTurn)
			}
			if !a.canSelectInteractively() {
				return fail("ticket new requires an interactive terminal; use ticket template and ticket validate for scripts")
			}
			if !noAI {
				return a.authorTicketDraft(cmd, args, project, model, contextFiles)
			}
			path := ""
			if len(args) != 0 {
				var err error
				path, err = draftOutputPath(args[0])
				if err != nil {
					return fail(err.Error())
				}
			}
			input := a.input
			if input == nil {
				input = os.Stdin
			}
			source, err := collectTicketDraftWithMode(input, a.errOut, multiline, path)
			if err != nil {
				return fail(err.Error())
			}
			if err := cmd.Context().Err(); err != nil {
				return fail("creation cancelled; no file written")
			}
			if path == "" {
				parsed, _ := ticket.Parse(strings.NewReader(source))
				path, err = draftOutputPath(draftFilename(parsed.Title))
				if err != nil {
					return fail(err.Error())
				}
			}
			return a.saveTicketDraft(cmd, path, source)
		},
	}
	command.Flags().BoolVar(&multiline, "multiline", false, "paste a multiline problem; finish with a line containing only a dot")
	command.Flags().BoolVar(&noAI, "no-ai", false, "use the offline manual form without provider calls")
	command.Flags().StringVar(&project, "project", "", "registered project for authoring")
	command.Flags().StringVar(&model, "model", "", "explicit Claude authoring model")
	command.Flags().StringSliceVar(&contextFiles, "context", nil, "opt-in project-relative reference files (no tools or repository access)")
	command.Flags().StringVar(&statusSession, "status", "", "read an existing authoring session turn without inference")
	command.Flags().StringVar(&statusTurn, "turn", "", "exact existing turn key to inspect with --status")
	return command
}

func draftOutputPath(value string) (string, error) {
	path, err := filepath.Abs(value)
	if err != nil || strings.ContainsAny(path, "\x00\r\n\t") {
		return "", errors.New("invalid output path")
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return "", errors.New("output already exists or cannot be inspected; choose a new path")
	}
	return path, nil
}

// A bounded ASCII basename cannot escape the selected working directory.
func draftFilename(title string) string {
	var name strings.Builder
	for _, r := range strings.ToLower(title) {
		if name.Len() >= 60 {
			break
		}
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			name.WriteRune(r)
		} else if name.Len() > 0 && !strings.HasSuffix(name.String(), "-") {
			name.WriteByte('-')
		}
	}
	base := strings.Trim(name.String(), "-")
	if base == "" {
		base = "ticket"
	}
	return base + ".md"
}

func (a *app) saveTicketDraft(cmd *cobra.Command, path, source string) error {
	fail := func(message string) error { return a.emit(failure("invalid_ticket", message, commandHelpAction(cmd))) }
	if cmd.Context().Err() != nil {
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
}

func collectTicketDraft(input io.Reader, output io.Writer) (string, error) {
	return collectTicketDraftWithMode(input, output, false, "")
}

func collectTicketDraftWithMode(input io.Reader, output io.Writer, multiline bool, path string) (string, error) {
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
	prompt := "Problem, desired behavior and scope (one line): "
	if multiline {
		prompt = "Problem, desired behavior and scope (paste lines; a single . finishes):\n"
	}
	problem, err := read(prompt)
	if err != nil {
		return "", err
	}
	if problem == "" {
		return "", errors.New("problem is required; no file written")
	}
	if multiline {
		var lines []string
		for problem != "." {
			lines = append(lines, problem)
			if len(lines) > 256 || len(strings.Join(lines, "\n")) > 64*1024 {
				return "", errors.New("problem exceeds 256 lines or 64 KiB; no file written")
			}
			problem, err = read("")
			if err != nil {
				return "", err
			}
		}
		problem = strings.TrimSpace(strings.Join(lines, "\n"))
		if problem == "" {
			return "", errors.New("problem is required; no file written")
		}
	}
	if _, err := fmt.Fprintln(output, "Keep this to one small change. Include an observable success case and an edge/failure case; avoid unrelated changes and credentials."); err != nil {
		return "", err
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
	if path == "" {
		path, err = draftOutputPath(draftFilename(title))
		if err != nil {
			return "", err
		}
	}
	if _, err := fmt.Fprintf(output, "\nOutput: %s\n", safeSelectionLabel(path)); err != nil {
		return "", err
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
