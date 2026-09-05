package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"runtime"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/bundle"
	"github.com/nysa-company/sf/internal/version"
	"github.com/spf13/cobra"
)

func (a *app) bundleCommand() *cobra.Command {
	command := &cobra.Command{Use: "bundle", Short: "Verify or install a complete local distribution bundle", Long: "Integrity checks do not authenticate a publisher. Use only bundles obtained from a trusted source. Installation never changes PATH, services, databases or running daemons."}
	for _, operation := range []string{"manifest", "verify", "install"} {
		operation := operation
		use, short := "verify <directory>", "Verify inventory, build identity, modes and hashes without executing payloads"
		if operation == "manifest" {
			use, short = "manifest <directory>", "Write a new manifest for a complete matching-version build directory"
		}
		if operation == "install" {
			use, short = "install <directory> --to <new-directory>", "Verify and copy this channel's bundle to a new private directory"
		}
		var destination string
		child := &cobra.Command{Use: use, Short: short, Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), 45*time.Second)
			defer cancel()
			source, err := filepath.Abs(args[0])
			if err != nil {
				return a.emit(failure("invalid_argument", "bundle path is unavailable", commandHelpAction(cmd)))
			}
			var manifest bundle.Manifest
			attempted := false
			switch operation {
			case "manifest":
				attempted = true
				manifest, err = bundle.CreateManifest(ctx, source, bundle.Identity{Version: version.Version, Commit: version.Commit, Channel: string(a.channel), OS: runtime.GOOS, Arch: runtime.GOARCH})
			case "verify":
				manifest, err = bundle.Verify(ctx, source)
			case "install":
				manifest, err = bundle.Verify(ctx, source)
				if err == nil && manifest.Identity.Channel != string(a.channel) {
					return a.emit(failure("wrong_channel", "bundle channel differs from this executable", commandHelpAction(cmd)))
				}
				if err == nil {
					destination, err = filepath.Abs(destination)
				}
				if err == nil {
					manifest, attempted, err = bundle.Install(ctx, source, destination)
				}
			}
			mutation := api.Mutation{Attempted: attempted}
			if attempted {
				mutation.Kind = "bundle." + operation
			}
			if err != nil {
				message := err.Error()
				if operation == "install" && attempted {
					message += "; partial destination retained; do not execute it, choose a new destination after inspection"
				}
				response := failure("invalid_configuration", message, commandHelpAction(cmd))
				response.Mutation = mutation
				return a.emit(response)
			}
			data, _ := json.Marshal(map[string]any{"bundle": manifest, "destination": destination, "note": "Integrity verified against the manifest; publisher authenticity is not established. PATH, services, databases and daemons were not changed."})
			return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: mutation, Data: data})
		}}
		if operation == "install" {
			child.Flags().StringVar(&destination, "to", "", "new destination directory (existing paths are refused)")
			_ = child.MarkFlagRequired("to")
		}
		command.AddCommand(child)
	}
	return command
}
