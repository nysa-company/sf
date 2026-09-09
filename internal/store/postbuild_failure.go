package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// PostbuildFailureRequest names existing immutable evidence, not permission to
// retry, amend verification, or admit a dirty worktree.
type PostbuildFailureRequest struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	BuilderResult   ProviderAttemptResultKey
	CommandResult   contracts.RepositoryCommandResultKey
}

// PostbuildFailure is a snapshot proof/locator only. Callers must reauthenticate
// under their write transaction before deriving any new authority. Command
// output is bounded but untrusted; it is never an instruction or a verdict
// that the independent verification is wrong.
type PostbuildFailure struct {
	Ref                domain.TicketRef
	TicketVersion      uint64
	Fence              domain.Fence
	BuilderResult      ProviderAttemptResultKey
	BuilderTypedDigest string
	CommandResult      RepositoryCommandResult
	Verification       StoredVerification
}

// AuthenticatePostbuildFailure reads one consistent SQLite snapshot. It makes
// no state changes and does not consume a correction budget.
// The failed command must be at the exact current fence. Historical consumed
// endpoints need a separate nonrecursive proof before they can support recovery.
func (s *Store) AuthenticatePostbuildFailure(ctx context.Context, request PostbuildFailureRequest) (PostbuildFailure, error) {
	if s == nil || request.Fence.ClaimEpoch != 0 {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return PostbuildFailure{}, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return PostbuildFailure{}, err
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	return s.authenticatePostbuildFailureFrom(ctx, conn, request)
}

// authenticatePostbuildFailureFrom also serves callers already holding a
// Store write transaction. Never use public readers on a second connection.
func (s *Store) authenticatePostbuildFailureFrom(ctx context.Context, conn *sql.Conn, request PostbuildFailureRequest) (PostbuildFailure, error) {
	ref, key := request.Ref, request.BuilderResult
	if s == nil || conn == nil || ref.Validate() != nil || request.ExpectedVersion == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 || key.Ref != ref || key.Phase != domain.PhaseBuild || key.AttemptID <= 0 || key.Attempt <= 0 || request.CommandResult.SemanticKey == "" || request.CommandResult.ClaimEpoch == 0 {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	var state domain.State
	var version, runner uint64
	if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner); err != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	if version != request.ExpectedVersion || s.currentFence(ctx, conn, ref.Channel, version, runner, request.Fence) != nil {
		return PostbuildFailure{}, ErrStaleFence
	}
	if state != domain.StateBuilding || assertNoVerificationAmendmentDownstream(ctx, conn, ref) != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
	if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest != nil || builder.Claim.Role != "builder" || assertNewestBoundResult(ctx, conn, ref, domain.PhaseBuild, "builder", key) != nil || providerResultReachesFence(ctx, conn, key, builder, version, request.Fence) != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	verification, err := s.currentVerificationFrom(ctx, conn, ref)
	if err != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	command, found, err := loadRepositoryCommandResult(ctx, conn, request.CommandResult, true)
	if err != nil || !found || command.Result.ExitCode < 1 || command.Result.ExitCode > 255 || command.Claim.TicketRef != ref || command.Claim.TicketVersion != version || command.Claim.LeaderEpoch != request.Fence.LeaderEpoch || command.Claim.RunnerEpoch != runner {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	var snapshot []byte
	var configDigest, repository, baseRef, path, identity, base, branch, completed string
	if err := conn.QueryRowContext(ctx, `SELECT t.config_snapshot_bytes,t.config_digest,p.canonical_path,p.base_ref,w.path,w.identity_json,w.base_sha,w.branch_ref,r.created_at FROM tickets t JOIN projects p ON p.channel=t.channel AND p.id=t.project_id JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id AND w.state='registered' JOIN provider_attempt_results r ON r.provider_attempt_id=? WHERE t.channel=? AND t.project_id=? AND t.id=?`, key.AttemptID, ref.Channel, ref.Project, ref.Ticket).Scan(&snapshot, &configDigest, &repository, &baseRef, &path, &identity, &base, &branch, &completed); err != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	argv, err := frozenVerifyArgv(snapshot, configDigest)
	if err != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	digest, err := exactRepositoryCommandDigest(argv)
	if err != nil || command.Claim.CommandDigest != digest || command.Claim.Repository != repository || command.Claim.BaseRef != baseRef || command.Claim.Worktree != path || command.Claim.WorktreeIdentity != identity || command.Claim.BaseSHA != base || command.Claim.Branch != branch || builder.Claim.Worktree != path || builder.Claim.WorktreeIdentity != identity || builder.Claim.BaseSHA != base {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	finished, err := time.Parse(time.RFC3339Nano, completed)
	if err != nil || command.Result.ObservedAt.Before(finished) {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	evidence := commandEvidenceRequest(RepositoryCommandPurposePostbuildCandidate, ref, version, request.Fence, key, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Checkpoint.CommitOID, digest, command)
	if assertCommandEvidenceRequest(evidence, command) != nil {
		return PostbuildFailure{}, ErrEvidenceConflict
	}
	if repositoryHasProviderWriter(ctx, conn, repository) != nil || repositoryHasCommandWriter(ctx, conn, repository) != nil {
		return PostbuildFailure{}, ErrControlNotDrained
	}
	var count int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, repository).Scan(&count); err != nil || count != 0 {
		return PostbuildFailure{}, ErrControlNotDrained
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket).Scan(&count); err != nil || count != 0 {
		return PostbuildFailure{}, ErrControlNotDrained
	}
	return PostbuildFailure{Ref: ref, TicketVersion: version, Fence: request.Fence, BuilderResult: key, BuilderTypedDigest: builder.TypedSHA256, CommandResult: command, Verification: verification}, nil
}
