// Package repositoryexec is the small composition boundary for durable,
// credential-free repository commands. It deliberately contains no planner
// or scheduler: a caller must first obtain a Store-issued claim. This is not
// hostile same-UID or network containment; autonomous execution remains
// ineligible under the execution policy.
package repositoryexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/processsupervisor"
)

var ErrInvalidBinding = errors.New("repository command binding is invalid")

type Request struct {
	Claim  contracts.RepositoryCommandClaim
	Spec   contracts.CommandSpec
	Policy executionpolicy.CommandSnapshot
}

type Executor struct {
	Authority  contracts.RepositoryCommandAuthority
	Supervisor processsupervisor.RepositoryCommandSupervisor
}

func CommandDigest(argv []string) (string, error) {
	if len(argv) == 0 {
		return "", ErrInvalidBinding
	}
	b, err := json.Marshal(argv)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:]), nil
}

func ExecutableDigest(path string) (string, error) {
	return processsupervisor.RepositoryExecutableDigest(path)
}

// SpecDigest binds every launch-affecting field. Stdin is hashed by the
// caller after it has been bounded and materialized.
func SpecDigest(spec contracts.CommandSpec, stdinDigest string) (string, error) {
	v := struct {
		Argv        []string
		Directory   string
		Timeout     int64
		Profile     contracts.ExecutionProfile
		StdinDigest string
	}{spec.Argv, spec.Directory, spec.Timeout.Nanoseconds(), spec.Profile, stdinDigest}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:]), nil
}

func (e Executor) Run(ctx context.Context, req Request) (contracts.CommandResult, error) {
	if e.Authority == nil || req.Spec.Profile != contracts.ProfileGuarded || len(req.Spec.Argv) == 0 || req.Spec.Directory != req.Claim.Worktree || req.Spec.Timeout <= 0 || req.Spec.Timeout > 45*time.Minute || req.Policy.Digest() != req.Claim.PolicyDigest {
		return contracts.CommandResult{}, ErrInvalidBinding
	}
	digest, err := CommandDigest(req.Spec.Argv)
	if err != nil || digest != req.Claim.CommandDigest {
		return contracts.CommandResult{}, ErrInvalidBinding
	}
	// Proof commands have no stdin contract. Rejecting it avoids an arbitrary
	// blocking reader before the durable launch gate.
	if req.Spec.Stdin != nil {
		return contracts.CommandResult{}, ErrInvalidBinding
	}
	var input []byte
	stdinSum := sha256.Sum256(input)
	specDigest, err := SpecDigest(req.Spec, "sha256:"+hex.EncodeToString(stdinSum[:]))
	if err != nil || specDigest != req.Claim.SpecDigest {
		return contracts.CommandResult{}, ErrInvalidBinding
	}
	if err := req.Policy.Authorize(req.Spec.Argv); err != nil {
		return contracts.CommandResult{}, err
	}
	if err := e.Supervisor.Preflight(req.Spec); err != nil {
		return contracts.CommandResult{}, err
	}
	if !filepath.IsAbs(req.Claim.Repository) || filepath.Clean(req.Claim.Repository) != req.Claim.Repository || !filepath.IsAbs(req.Claim.Worktree) || filepath.Clean(req.Claim.Worktree) != req.Claim.Worktree {
		return contracts.CommandResult{}, ErrInvalidBinding
	}
	if actual, err := filepath.EvalSymlinks(req.Claim.Worktree); err != nil || actual != req.Claim.Worktree {
		return contracts.CommandResult{}, fmt.Errorf("%w: worktree path changed", ErrInvalidBinding)
	}
	if _, err := os.Stat(req.Claim.Worktree); err != nil {
		return contracts.CommandResult{}, err
	}
	lease, err := e.Authority.AcquireRepositoryCommand(ctx, req.Claim)
	if err != nil {
		return contracts.CommandResult{}, err
	}
	result, runErr := e.Supervisor.Run(ctx, req.Claim, req.Spec, req.Policy, lease)
	// A cancellation/deadline is control-plane authority, not command evidence.
	// The supervisor may have reaped the child and therefore return an observed
	// non-zero result, but recording it would let cancellation masquerade as a
	// provider-declared red verification result. Retire the exact drained lease,
	// intent, and effect atomically without recording a result.
	if result.Observed && repositoryCommandCanceled(ctx, runErr) {
		if err := retireObservedCanceledRepositoryCommand(e.Authority, lease, req.Claim); err != nil {
			return result, err
		}
		if runErr != nil {
			return result, runErr
		}
		return result, ctx.Err()
	}
	// Once a lease is acquired, an unobserved result is always uncertain. Do
	// not infer success from CommandResult's zero-valued exit code.
	if !result.Observed {
		reason := "repository command result was not observed"
		if runErr != nil {
			reason = runErr.Error()
		}
		if recorder, ok := e.Authority.(contracts.RepositoryCommandResultRecorder); ok {
			persistCtx, cancel := repositoryPersistenceContext()
			err := recorder.MarkRepositoryCommandUncertain(persistCtx, req.Claim, reason)
			cancel()
			if err != nil {
				_ = lease.Quarantine()
				return result, fmt.Errorf("persist repository uncertainty: %w", err)
			}
		} else {
			_ = lease.Quarantine()
			return result, ErrInvalidBinding
		}
		_ = lease.Quarantine()
		if runErr == nil {
			runErr = ErrInvalidBinding
		}
		return result, runErr
	}
	if recorder, ok := e.Authority.(contracts.RepositoryCommandResultRecorder); ok {
		persistCtx, cancel := repositoryPersistenceContext()
		err := recorder.CompleteRepositoryCommand(persistCtx, req.Claim, result)
		cancel()
		if err != nil {
			staleCtx, staleCancel := repositoryPersistenceContext()
			staleErr := recorder.ReconcileStaleRepositoryCommandObservation(staleCtx, req.Claim, result)
			staleCancel()
			if staleErr == nil {
				if releaseErr := lease.Release(); releaseErr != nil {
					_ = lease.Quarantine()
					return result, releaseErr
				}
				return result, err
			}
			_ = lease.Quarantine()
			return result, err
		}
	} else if !ok {
		_ = lease.Quarantine()
		return result, ErrInvalidBinding
	}
	if releaseErr := lease.Release(); releaseErr != nil {
		_ = lease.Quarantine()
		if runErr != nil {
			return result, fmt.Errorf("%w: release: %v", runErr, releaseErr)
		}
		return result, releaseErr
	}
	return result, runErr
}

// RetireUnleased settles an exact issued claim when Run failed before the
// repository lease/child launch boundary. Persistence deliberately outlives
// the caller so cancellation cannot strand the executing effect.
func RetireUnleased(authority contracts.RepositoryCommandAuthority, claim contracts.RepositoryCommandClaim) error {
	retirer, ok := authority.(contracts.RepositoryCommandUnleasedRetirer)
	if !ok {
		return ErrInvalidBinding
	}
	persistCtx, cancel := repositoryPersistenceContext()
	err := retirer.RetireUnleasedRepositoryCommand(persistCtx, claim)
	cancel()
	if err != nil {
		return fmt.Errorf("retire unleased repository command: %w", err)
	}
	return nil
}

func repositoryCommandCanceled(ctx context.Context, runErr error) bool {
	return ctx.Err() != nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)
}

func retireObservedCanceledRepositoryCommand(authority contracts.RepositoryCommandAuthority, lease contracts.RepositoryCommandLease, claim contracts.RepositoryCommandClaim) error {
	recorder, ok := authority.(contracts.RepositoryCommandResultRecorder)
	if !ok {
		_ = lease.Quarantine()
		return ErrInvalidBinding
	}
	persistCtx, cancel := repositoryPersistenceContext()
	err := recorder.RetireObservedCanceledRepositoryCommand(persistCtx, claim)
	cancel()
	if err != nil {
		_ = lease.Quarantine()
		return fmt.Errorf("retire observed canceled repository command: %w", err)
	}
	return nil
}

// repositoryPersistenceContext intentionally does not inherit caller
// cancellation: once a child crossed the launch gate, the durable effect must
// become completed or uncertain, never silently remain executing.
func repositoryPersistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
