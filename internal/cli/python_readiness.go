package cli

import (
	"context"
	"path/filepath"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/pythonclosure"
	"github.com/nysa-company/sf/internal/pythonprepare"
)

func checkPythonRecipeReady(ctx context.Context, paths config.ChannelPaths, repository string, argv []string) error {
	recipe, err := pythonclosure.ParseRecipe(argv)
	if err != nil {
		return err
	}
	if err := pythonclosure.ValidateTestPath(repository, recipe.TestPath); err != nil {
		return err
	}
	root, err := filepath.EvalSymlinks(config.PythonSnapshotsPath(paths))
	if err != nil {
		return err
	}
	return pythonprepare.CheckRecipe(ctx, root, argv)
}

func checkConfiguredPython(ctx context.Context, paths config.ChannelPaths, repository string, effective config.Effective) error {
	for _, command := range []config.Command{effective.Commands.Verify, effective.Commands.Review} {
		if len(command.Argv) > 0 && filepath.Base(command.Argv[0]) == "python3" {
			if err := checkPythonRecipeReady(ctx, paths, repository, command.Argv); err != nil {
				return err
			}
		}
	}
	return nil
}
