// Package repositoryexec is the small composition boundary for durable,
// credential-free repository commands. It deliberately contains no planner
// or scheduler: a caller must first obtain a Store-issued claim. This is not
// hostile same-UID or network containment; autonomous execution remains
// ineligible under the execution policy.
package repositoryexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:]), nil
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
	var input []byte
	if req.Spec.Stdin != nil {
		input, err = io.ReadAll(io.LimitReader(req.Spec.Stdin, 1<<20+1))
		if err != nil || len(input) > 1<<20 {
			return contracts.CommandResult{}, ErrInvalidBinding
		}
		req.Spec.Stdin = bytes.NewReader(input)
	}
	stdinSum := sha256.Sum256(input)
	specDigest, err := SpecDigest(req.Spec, "sha256:"+hex.EncodeToString(stdinSum[:]))
	if err != nil || specDigest != req.Claim.SpecDigest {
		return contracts.CommandResult{}, ErrInvalidBinding
	}
	if err := req.Policy.Authorize(req.Spec.Argv); err != nil {
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

// repositoryPersistenceContext intentionally does not inherit caller
// cancellation: once a child crossed the launch gate, the durable effect must
// become completed or uncertain, never silently remain executing.
func repositoryPersistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
