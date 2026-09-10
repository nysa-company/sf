package workflowruntime

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/goclosure"
)

var ErrExecutionBaseDependencies = errors.New("execution worktree Go dependency markers are unavailable")

// CheckExecutionBaseDependencies inspects the actual execution checkout, not
// the operator's primary working tree or a mutable .sf/config.toml. It only
// checks the existing Go marker contract; the compiler and supervisor retain
// full graph/executable admission. It installs/copies nothing and runs no tool.
func CheckExecutionBaseDependencies(ctx context.Context, effective config.Effective, worktree string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, command := range []config.Command{effective.Commands.Verify, effective.Commands.Review} {
		if len(command.Argv) != 0 && filepath.Base(command.Argv[0]) == "go" {
			if _, err := goclosure.Validate(worktree); err != nil {
				return diagnosticError{code: "execution_base_dependencies", err: ErrExecutionBaseDependencies}
			}
			break
		}
	}
	return ctx.Err()
}
