package localruntime

import (
	"context"
	"errors"
	"runtime"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/store"
)

// CheckProjectStart rejects unsupported source recipes before spending a
// provider budget. It reads only the authenticated Store configuration, never
// mutable TOML or repository code. This is a necessary check, not a launch
// proof: executable, closure, provider and publication checks remain mandatory
// at their respective authority boundaries.
func CheckProjectStart(ctx context.Context, project store.Project) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if runtime.GOOS != "darwin" {
		return daemon.ErrStartRuntimeUnsupported
	}
	return checkProjectRecipes(project)
}

func checkProjectRecipes(project store.Project) error {
	frozen, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil || project.ConfigGeneration == 0 {
		return errors.New("start requires an authenticated configuration generation")
	}
	for _, command := range []config.Command{frozen.Commands.Verify, frozen.Commands.Review} {
		if !executionpolicy.EvaluateRepositoryCommand(command.Argv).Allowed {
			return daemon.ErrStartRecipeUnsupported
		}
	}
	return nil
}
