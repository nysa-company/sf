package workflowruntime

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

func TestPostbuildCommandErrorClassifiesOnlyObservedNonzeroResult(t *testing.T) {
	terminal := func(exit int) store.RepositoryCommandResult {
		return store.RepositoryCommandResult{
			Key:          contracts.RepositoryCommandResultKey{SemanticKey: "postbuild", ClaimEpoch: 1},
			Result:       contracts.CommandResult{ExitCode: exit, Observed: true},
			ResultDigest: "sha256:fixture",
		}
	}
	t.Run("success", func(t *testing.T) {
		if err := postbuildCommandError(terminal(0), nil); err != nil {
			t.Fatalf("successful post-build command: %v", err)
		}
	})

	t.Run("nonzero terminal result", func(t *testing.T) {
		err := postbuildCommandError(terminal(17), nil)
		if !errors.Is(err, workflowworker.ErrPostbuildCommandFailed) || !errors.Is(err, ErrRepositoryMaterialization) {
			t.Fatalf("nonzero post-build command err=%v", err)
		}
		var failure *workflowworker.PostbuildFailure
		if !errors.As(err, &failure) || failure.CommandResult != terminal(17).Key {
			t.Fatalf("immutable result key was lost: %v", err)
		}
	})

	t.Run("signal result is not diagnostic retry evidence", func(t *testing.T) {
		err := postbuildCommandError(terminal(-1), nil)
		var failure *workflowworker.PostbuildFailure
		if !errors.Is(err, workflowworker.ErrPostbuildCommandFailed) || errors.As(err, &failure) {
			t.Fatalf("signal exit exposed diagnostic retry evidence: %v", err)
		}
	})

	t.Run("unobserved result is not a terminal failure", func(t *testing.T) {
		result := terminal(17)
		result.Result.Observed = false
		err := postbuildCommandError(result, nil)
		if !errors.Is(err, ErrRepositoryMaterialization) || errors.Is(err, workflowworker.ErrPostbuildCommandFailed) {
			t.Fatalf("unobserved post-build command err=%v", err)
		}
		var failure *workflowworker.PostbuildFailure
		if errors.As(err, &failure) {
			t.Fatal("unobserved result exposed immutable failure key")
		}
	})

	t.Run("unavailable result remains fail closed", func(t *testing.T) {
		unavailable := errors.New("repository result unavailable")
		result := store.RepositoryCommandResult{Result: contracts.CommandResult{ExitCode: 17}}
		err := postbuildCommandError(result, unavailable)
		if !errors.Is(err, unavailable) || errors.Is(err, workflowworker.ErrPostbuildCommandFailed) {
			t.Fatalf("unavailable post-build command err=%v", err)
		}
	})
}
