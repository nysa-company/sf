package contracts

import (
	"context"
	"io"
	"time"
)

type CommandSpec struct {
	Argv       []string
	Directory  string
	Timeout    time.Duration
	Profile    ExecutionProfile
	PolicyHash string
	Stdin      io.Reader
}

type CommandResult struct {
	ExitCode int
	Stdout   []byte
	Stderr   []byte
	Duration time.Duration
}

type CommandExecutor interface {
	Run(context.Context, CommandSpec) (CommandResult, error)
	Drain(context.Context, string) error
	NoLiveWriter(context.Context, string) (bool, error)
}

// GitMutationClaim names one narrowly-scoped Git effect.  The Git boundary
// deliberately cannot mint this claim: a supervisor/store must hold the
// repository's no-live-writer exclusion while the returned lease is live.
// This is what turns the trusted-repository assumption into an enforceable
// production integration point instead of a Runner-local boolean.
type GitMutationClaim struct {
	Repository string
	Worktree   string
	Branch     string
	Operation  string
}

// GitMutationLease remains valid only while the supervisor's exclusion is
// held. Implementations must make Check fail after revocation or expiry.
type GitMutationLease interface {
	Check(context.Context) error
	Release() error
}

// GitMutationAuthority is implemented by the daemon/store supervisor, never
// by git.Runner. Acquiring a lease must prove that no provider or repository
// command capable of writing the named repository is live, and prevent a new
// writer until Release.
type GitMutationAuthority interface {
	AcquireGitMutation(context.Context, GitMutationClaim) (GitMutationLease, error)
}

// ProtectedBranchWitness is the durable recovery witness for a completed
// merge. A verifier must freshly observe ProtectedRef at Origin and prove the
// reported MergeOID remains contained by it and descends from OriginalBaseOID.
type ProtectedBranchWitness struct {
	Repository      string
	Worktree        string
	Origin          string
	ProtectedRef    string
	OriginalBaseOID string
	MergeOID        string
}

type ProtectedBranchVerifier interface {
	VerifyProtectedBranch(context.Context, ProtectedBranchWitness) error
}
