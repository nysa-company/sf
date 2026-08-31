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
	// ObservedAt is the supervisor's UTC observation timestamp. It is recorded
	// with the bounded result so a later reader never has to infer completion
	// time from a lease or effect row.
	ObservedAt time.Time
	// Observed is true only after the repository-command supervisor has reaped
	// the launched process and proved its recorded containment state. The zero
	// value is deliberately not a successful exit.
	Observed bool
}

// RepositoryCommandResultKey identifies one immutable terminal observation.
// A semantic effect may be claimed more than once after a safe retry; the
// claim epoch is therefore part of the authority and results are never
// overwritten by a later retry.
type RepositoryCommandResultKey struct {
	SemanticKey string
	ClaimEpoch  uint64
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

// PreparedCommitObservation is the read-only identity of a commit recovered
// from a durable Git mutation intent. The parent is part of the observation so
// a commit created against a different branch head can never be accepted.
type PreparedCommitObservation struct {
	CommitOID string
	ParentOID string
	TreeOID   string
}

// PreparedCommitObserver is the narrow restart-reconciliation capability for
// a prepared commit. Implementations must authenticate the registered
// worktree and perform reads only; this contract has no stage, commit, or
// ref-update capability by design.
type PreparedCommitObserver interface {
	ObservePreparedCommit(context.Context, GitMutationClaim) (PreparedCommitObservation, error)
}

// RepositoryCommandClaim is a Store-issued, immutable binding for a
// credential-free command. The command boundary cannot mint or broaden it.
type RepositoryCommandClaim struct {
	TicketRef                                                        domain.TicketRef
	SemanticKey, RequestDigest                                       string
	TicketVersion, LeaderEpoch, RunnerEpoch, ClaimEpoch              uint64
	Repository, Worktree, WorktreeIdentity, Branch, BaseRef, BaseSHA string
	CommandDigest, SpecDigest, PolicyDigest                          string
	ExecutablePath, ExecutableDigest                                 string
}

type RepositoryCommandLaunch struct {
	PID, PGID                          int
	BootIdentity, ProcessStartIdentity string
}
type RepositoryCommandLease interface {
	Check(context.Context) error
	Release() error
	RecordRepositoryCommandLaunch(context.Context, RepositoryCommandLaunch) error
	FinishRepositoryCommandLaunch(context.Context, RepositoryCommandLaunch) error
	Quarantine() error
}

// RepositoryCommandGroupRecorder durably records an exact Go-test process
// group before its launch gate is opened. The separate dependency-free Node
// 22 recipe does not use a test-wrapper group: its Seatbelt forbids fork/exec
// after the single fd-gated staged Node launch.
type RepositoryCommandGroupRecorder interface {
	RecordRepositoryCommandProcessGroup(context.Context, RepositoryCommandLaunch) error
}
type RepositoryCommandAuthority interface {
	AcquireRepositoryCommand(context.Context, RepositoryCommandClaim) (RepositoryCommandLease, error)
}

type RepositoryCommandResultRecorder interface {
	CompleteRepositoryCommand(context.Context, RepositoryCommandClaim, CommandResult) error
	RetireObservedCanceledRepositoryCommand(context.Context, RepositoryCommandClaim) error
	MarkRepositoryCommandUncertain(context.Context, RepositoryCommandClaim, string) error
	ReconcileStaleRepositoryCommandObservation(context.Context, RepositoryCommandClaim, CommandResult) error
}

// RepositoryCommandUnleasedRetirer settles an issued claim that failed before
// AcquireRepositoryCommand. No child could cross the repository boundary in
// that state, so the exact intent may be retired and retried safely.
type RepositoryCommandUnleasedRetirer interface {
	RetireUnleasedRepositoryCommand(context.Context, RepositoryCommandClaim) error
}
type RepositoryCommandDrainer interface {
	DrainRepositoryCommand(context.Context, RepositoryCommandLaunch) error
}

// RepositoryCommandTreeDrainer is used when Go verification has spawned
// sandboxed, separately tracked test process groups. Recovery must drain the
// primary driver and every recorded group before releasing repository
// exclusion; treating the driver's old process group as a tree witness is
// unsafe on macOS.
type RepositoryCommandTreeDrainer interface {
	DrainRepositoryCommandTree(context.Context, RepositoryCommandLaunch, []RepositoryCommandLaunch) error
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
