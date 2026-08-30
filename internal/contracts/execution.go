package contracts

import (
	"context"
	"io"
	"time"

	"github.com/nysa-company/sf/internal/domain"
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
	// OutputLastMessage is the supervisor-owned, bounded final artifact file
	// produced by Codex --output-last-message. JSONL is telemetry only.
	OutputLastMessage          []byte
	StdoutTruncated            bool
	StderrTruncated            bool
	OutputLastMessageTruncated bool
	Duration                   time.Duration
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
	TicketRef       domain.TicketRef
	SemanticKey     string
	RequestDigest   string
	TicketVersion   uint64
	LeaderEpoch     uint64
	RunnerEpoch     uint64
	ClaimEpoch      uint64
	Repository      string
	Worktree        string
	Branch          string
	Operation       string
	BaseRef         string
	ExpectedBaseOID string
	ExpectedHeadOID string
}

// GitMutationLease remains valid only while the supervisor's exclusion is
// held. Implementations must make Check fail after revocation or expiry.
type GitMutationLease interface {
	Check(context.Context) error
	Release() error
}

// GitMutationRecoveryFactsLease records the one-way facts needed to recover
// an irreversible Git mutation after a lost response.  The production store
// implementation binds each write to the immutable intent and exact active
// lease nonce; runners must record the fact before performing the mutation.
// Test leases which drive mutation paths should implement this interface too.
type GitMutationRecoveryFactsLease interface {
	GitMutationLease
	RecordPreparedCommit(context.Context, string, string) error
	RecordPushPriorRemote(context.Context, string) error
}

// GitMutationLaunchLease is implemented by the production SQLite lease.  A
// Git child remains behind its supervisor gate until RecordGitMutationLaunch
// commits; FinishGitMutationLaunch is permitted only after the parent has
// observed that exact process group exit.  Test-only leases need not implement
// this extension.
type GitMutationLaunchLease interface {
	GitMutationLease
	RecordGitMutationLaunch(context.Context, GitMutationLaunch) error
	FinishGitMutationLaunch(context.Context, GitMutationLaunch) error
}

type GitMutationLaunch struct {
	PID, PGID            int
	BootIdentity         string
	ProcessStartIdentity string
}

// GitMutationDrainer is the startup-only OS identity verifier.  It must not
// signal a PID/group unless boot, start identity, and PGID match the durable
// launch; an unclear result leaves the repository quarantined.
type GitMutationDrainer interface {
	DrainGitMutation(context.Context, GitMutationLaunch) error
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
	MutationClaim   GitMutationClaim
}

type ProtectedBranchVerifier interface {
	VerifyProtectedBranch(context.Context, ProtectedBranchWitness) error
}
