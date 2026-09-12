package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

func (a *app) ticketViewCommand() *cobra.Command {
	var section string
	command := &cobra.Command{Use: "view <ticket>", Short: "Inspect source, stored plan, proof results, and candidate evidence", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		switch section {
		case "all", "source", "plan", "acceptance", "proof", "changes", "pr":
		default:
			return a.emit(failure("invalid_argument", "section must be all, source, plan, acceptance, proof, changes, or pr", commandHelpAction(cmd)))
		}
		return a.emit(a.request("ticket.show", args[0], params(map[string]any{"section": section}, a.channel)))
	}}
	command.Flags().StringVar(&section, "section", "all", "artifact section: all, source, plan, acceptance, proof, changes, pr")
	_ = command.RegisterFlagCompletionFunc("section", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		return []string{"all", "source", "plan", "acceptance", "proof", "changes", "pr"}, cobra.ShellCompDirectiveNoFileComp
	})
	return command
}

func (a *app) ticketWatchCommand() *cobra.Command {
	return &cobra.Command{Use: "watch <ticket>", Short: "Follow durable state and bounded live provider activity; Ctrl-C detaches", Long: "Follow durable state, SF process observations, and supported provider-reported activity. Provider reports are not proof results. Quiet output does not establish a stalled process. Ctrl-C detaches without cancelling. JSON emits response snapshots as NDJSON.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !a.json {
			if _, err := io.WriteString(a.errOut, "Following state and observed activity. Ctrl-C detaches; the ticket continues. Provider reports are not verification results.\n"); err != nil {
				return err
			}
		}
		return a.watchTicketStatus(cmd.Context(), args[0])
	}}
}

func renderTicketArtifacts(writer io.Writer, object map[string]any) error {
	if err := renderTicket(writer, object, object["evidence"], object); err != nil {
		return err
	}
	artifacts, _ := object["artifacts"].(map[string]any)
	for _, name := range []string{"source", "plan", "acceptance", "proof", "changes", "pr"} {
		value, ok := artifacts[name].(map[string]any)
		if !ok {
			continue
		}
		if _, err := fmt.Fprintf(writer, "\n%s\n", name); err != nil {
			return err
		}
		if text := stringField(value, "text"); text != "" {
			if _, err := fmt.Fprintln(writer, safeDraftPreview(text)); err != nil {
				return err
			}
		} else if _, err := fmt.Fprintln(writer, safeDraftPreview(stableJSON(value))); err != nil {
			return err
		}
		if boolField(value, "truncated") {
			if _, err := io.WriteString(writer, "[Artifact display truncated]\n"); err != nil {
				return err
			}
		}
	}
	return nil
}
