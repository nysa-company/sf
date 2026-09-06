package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/pythonprepare"
	"github.com/spf13/cobra"
)

func (a *app) runtimesCommand() *cobra.Command {
	root := &cobra.Command{Use: "runtimes", Short: "Prepare verified test runtimes without changing a project"}
	var download bool
	child := &cobra.Command{Use: "prepare python [--download]", Short: "Preview pinned Python preparation; --download explicitly prepares this channel's cache", Long: "Without --download, preview only: no network or filesystem changes. Preparation verifies pinned Python 3.13 and pytest artifacts in a private channel cache. It never changes PATH, starts a daemon, registers a project, or executes project code. Afterwards, select the explicit python-pytest-v1 project profile. Additional project dependencies are not installed.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "python" {
			return a.emit(failure("unsupported_runtime", "only pinned Python preparation is available", commandHelpAction(cmd)))
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return a.emit(failure("invalid_configuration", "current-user home is unavailable", commandHelpAction(cmd)))
		}
		return a.emit(runPythonPreparation(cmd.Context(), a.channel, home, runtime.GOOS, runtime.GOARCH, download, pythonprepare.Prepare))
	}}
	child.Flags().BoolVar(&download, "download", false, "allow pinned public artifact downloads and private cache creation (no project changes)")
	root.AddCommand(child)
	return root
}

func runPythonPreparation(ctx context.Context, channel domain.Channel, home, goos, arch string, download bool, prepare func(context.Context, string) (pythonprepare.Result, error)) api.Response {
	binary := binaryForChannel(channel)
	help := []string{binary, "runtimes", "prepare", "--help"}
	if ctx == nil || prepare == nil || !channel.Valid() {
		return failure("invalid_argument", "invalid channel", help)
	}
	catalog, err := pythonprepare.DefaultCatalog(goos, arch)
	if err != nil {
		return failure("unsupported_runtime", "pinned Python preparation currently requires macOS on Apple Silicon; no files were changed", help)
	}
	paths, err := config.PathsFor(home, channel)
	if err != nil {
		return failure("invalid_configuration", "channel paths are unavailable", help)
	}
	var bytes int64 = catalog.Runtime.Size
	for _, wheel := range catalog.Wheels {
		bytes += wheel.Size
	}
	data := map[string]any{"runtime": "Python 3.13.15 / pytest 8.4.2", "status": "preview", "destination": config.PythonSnapshotsPath(paths), "download_bytes": bytes, "environment_digest": catalog.EnvironmentDigest, "lock_digest": pythonprepare.LockDigest(), "note": "Pinned runtime only; additional project dependencies are not installed. Project setup, provider qualification and publication readiness remain separate. No PATH, database, project or daemon changes.", "next_command": binary + " runtimes prepare python --download"}
	mutation := api.Mutation{}
	if download {
		mutation = api.Mutation{Attempted: true, Kind: "runtime.prepare"}
		root, err := config.PreparePythonSnapshots(paths)
		if err == nil {
			var result pythonprepare.Result
			result, err = prepare(ctx, root)
			if err == nil && (result.EnvironmentDigest != catalog.EnvironmentDigest || result.LockDigest != pythonprepare.LockDigest()) {
				err = errors.New("prepared runtime identity mismatch")
			}
			if err == nil {
				data["status"] = "prepared"
				if result.AlreadyPrepared {
					data["status"] = "already prepared"
				}
				data["destination"] = root
				data["next_command"] = binary + " init --profile python-pytest-v1 --test tests"
			}
		}
		if err != nil {
			if errors.Is(err, pythonprepare.ErrCacheInvalid) {
				response := failure("runtime_cache_unverified", "existing Python cache failed verification and was retained; repeating a download will not repair it. Do not edit or delete a cache used by a running SF process. Operator inspection is required; no automatic repair is performed.", help)
				response.Mutation = mutation
				return response
			}
			response := failure("runtime_preparation_failed", "runtime could not be verified or prepared; existing snapshots were not replaced. Inspect available disk space and network access, then retry preparation.", []string{binary, "runtimes", "prepare", "python", "--download"})
			response.Mutation = mutation
			return response
		}
	}
	encoded, _ := json.Marshal(map[string]any{"runtime_preparation": data})
	return api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: mutation, Data: encoded}
}
