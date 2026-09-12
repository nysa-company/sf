package cli

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
)

var homeProjectPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)

// Home is navigation, not a new lifecycle controller. Every selected action
// passes through the same public command and daemon checks as direct usage.
func (a *app) homeCommand() *cobra.Command {
	var project string
	var model string
	var noAI bool
	command := &cobra.Command{Use: "home", Short: "Choose a project-scoped task in an interactive terminal", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			fail := func(message string) error {
				return a.emit(failure("invalid_argument", message, commandHelpAction(cmd)))
			}
			if !a.canSelectInteractively() {
				return fail("home requires a terminal; use ticket new, ticket start, ticket list or ticket approve explicitly")
			}
			if !noAI {
				return a.homeIntent(cmd, project, model)
			}
			reader := a.input
			if reader == nil {
				reader = os.Stdin
			}
			read := func(prompt string) (string, error) {
				if _, err := io.WriteString(a.errOut, prompt); err != nil {
					return "", err
				}
				if cmd.Context().Err() != nil {
					return "", cmd.Context().Err()
				}
				answer, err := readBoundedAnswer(reader, 4096)
				if cmd.Context().Err() != nil {
					return "", cmd.Context().Err()
				}
				return strings.TrimSpace(answer), err
			}
			answer, err := read("SF home\n1) Create a local ticket draft\n2) Start a saved draft\n3) View tickets\n4) Review a ticket awaiting approval\nq) Cancel\nChoose: ")
			if err != nil || answer == "" || answer == "q" {
				return fail("home cancelled; no action taken")
			}
			if answer != "1" && answer != "2" && answer != "3" && answer != "4" {
				return fail("choose one listed action; no action taken")
			}
			if answer == "1" {
				args := []string{"ticket", "new", "--multiline", "--no-ai"}
				if project != "" {
					args = append(args, "--project", project)
				}
				return a.dispatchHome(cmd, args)
			}
			if project == "" {
				project, err = read("Registered project name (q cancels): ")
				if err != nil || project == "q" {
					return fail("home cancelled; no action taken")
				}
			}
			if !homeProjectPattern.MatchString(project) {
				return fail("supply a valid registered project name; no action taken")
			}
			if _, err := fmt.Fprintf(a.errOut, "Project: %s  Channel: %s\n", safeSelectionLabel(project), a.channel); err != nil {
				return fail("could not display project; no action taken")
			}
			switch answer {
			case "2":
				path, err := read("Saved draft path (q cancels): ")
				if err != nil || path == "" || path == "q" {
					return fail("home cancelled; no action taken")
				}
				parsed, err := readLocalTicket(path)
				if err != nil {
					return fail(err.Error())
				}
				if _, err := fmt.Fprintf(a.errOut, "Review source before submission (deadline starts at submission):\n%s\nProvider pair/readiness are checked by the daemon. No estimated-cost consent is implied.\n", safeDraftPreview(string(parsed.Source))); err != nil {
					return fail("could not display draft; no action taken")
				}
				confirm, err := read("Submit and start this draft? Type run for verified-cost accounting, or run estimates to accept estimated costs (NOT a hard billing cap). Anything else cancels: ")
				if err != nil || confirm != "run" && confirm != "run estimates" {
					return fail("start cancelled; nothing submitted")
				}
				a.expectedDraftDigest = parsed.Digest
				defer func() { a.expectedDraftDigest = "" }()
				args := []string{"ticket", "start", "--file", path, "--project", project}
				if confirm == "run estimates" {
					args = append(args, "--accept-cost-estimates")
				}
				return a.dispatchHome(cmd, args)
			case "3":
				return a.dispatchHome(cmd, []string{"ticket", "list", "--project", project})
			default:
				return a.dispatchHome(cmd, []string{"ticket", "approve", "--project", project})
			}
		}}
	command.Flags().StringVar(&project, "project", "", "registered project to use; never guessed from directory name")
	command.Flags().StringVar(&model, "model", "", "explicit Claude model for intent interpretation")
	command.Flags().BoolVar(&noAI, "no-ai", false, "use deterministic numbered navigation without inference")
	return command
}

func (a *app) dispatchHome(parent *cobra.Command, args []string) error {
	if parent.Context().Err() != nil {
		return parent.Context().Err()
	}
	child := a.command()
	child.SetArgs(args)
	return child.ExecuteContext(parent.Context())
}

// Preserve line layout but do not allow terminal escape/control injection.
func safeDraftPreview(source string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' {
			return r
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, source)
}
