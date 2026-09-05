package cli

import "github.com/spf13/cobra"

// Help describes the existing authority-bearing commands; it does not add a
// second workflow or imply that a reserved command is implemented.
func configureCommandHelp(root *cobra.Command) {
	descriptions := map[string]string{
		"submit":            "Register a Markdown ticket without starting work",
		"start":             "Start a submitted ticket using its project policy",
		"status":            "Show ticket progress, waits, and the next action",
		"show":              "Inspect a ticket and its recorded evidence",
		"logs":              "Read sanitized ticket events and phase diagnostics",
		"pause":             "Stop and drain a ticket before pausing",
		"resume":            "Resume an eligible paused ticket after safety checks",
		"recover":           "Recover an eligible blocked ticket with fresh authority",
		"cancel":            "Cancel a ticket after draining and checking external state",
		"retry":             "Retry an eligible failed phase after worktree checks",
		"take":              "Stop a ticket and retain its worktree for operator takeover",
		"approve":           "Approve the exact reviewed head for guarded merge",
		"reject":            "Reject the reviewed candidate with a reason",
		"doctor":            "Check local prerequisites, authentication, and provider readiness",
		"auth":              "Inspect or open official provider authentication flows",
		"auth status":       "Show authentication status without displaying credentials",
		"auth login":        "Open the selected provider's official login flow",
		"init":              "Register a trusted repository and freeze its configuration",
		"providers":         "Manage independently qualified provider roles",
		"providers qualify": "Qualify Builder and Reviewer through the running daemon",
		"daemon":            "Run or inspect this channel's local foreground daemon",
		"config":            "Manage immutable project configuration generations",
		"update":            "Reserved: automatic updates are not configured in this build",
		"rollback":          "Reserved: automatic rollback is not configured in this build",
		"version":           "Show the build version, commit, channel, and protocol",
	}
	var visit func(*cobra.Command, string)
	visit = func(parent *cobra.Command, prefix string) {
		for _, command := range parent.Commands() {
			path := prefix + command.Name()
			if description, ok := descriptions[path]; ok {
				command.Short = description
			}
			visit(command, path+" ")
		}
	}
	visit(root, "")
	root.Long = "Delegate a Markdown ticket to a local, operator-controlled software factory.\n\nStart with init, run the daemon, qualify providers, then submit and start a ticket.\nGuarded merge requires your approval of the exact reviewed head.\nCurrent runtime support is macOS-only and limited to supported trusted repositories."
	examples := map[string]string{
		"init":    "init --project my-app --repo /absolute/path/to/my-app",
		"submit":  "submit ticket.md --project my-app",
		"start":   "start SF-<ticket-id>",
		"status":  "status SF-<ticket-id> --watch",
		"logs":    "logs SF-<ticket-id> --follow",
		"doctor":  "doctor --repo /absolute/path/to/my-app",
		"approve": "approve SF-<ticket-id>",
	}
	for _, command := range root.Commands() {
		if example, ok := examples[command.Name()]; ok {
			command.Example = "  " + root.Name() + " " + example
		}
	}
}

func commandHelpAction(command *cobra.Command) []string {
	var path []string
	for command != nil && command.HasParent() {
		path = append([]string{command.Name()}, path...)
		command = command.Parent()
	}
	return append(append([]string{binaryName()}, path...), "--help")
}
