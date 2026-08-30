package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

const (
	repositoryCommandOutputLimit      = 64 << 10
	repositoryCommandLastMessageLimit = 1 << 20
	repositoryCommandResultMaxAge     = 48 * time.Hour
)

// RepositoryCommandResult is immutable, transcript-bounded historical
// evidence for exactly one Store-issued command claim. The individual output
// digests make accidental or one-column SQLite tampering observable; the full
// digest additionally covers every claim and result field.
type RepositoryCommandResult struct {
	Key                                                 contracts.RepositoryCommandResultKey
	Claim                                               contracts.RepositoryCommandClaim
	Result                                              contracts.CommandResult
	StdoutDigest, StderrDigest, OutputLastMessageDigest string
	ResultDigest                                        string
	CreatedAt                                           time.Time
}

func repositoryResultDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func repositoryCommandResultEnvelope(claim contracts.RepositoryCommandClaim, result contracts.CommandResult, createdAt time.Time) ([]byte, error) {
	// A struct (rather than a map) gives a stable field order. This payload is
	// never an API transcript; it is the compact authentication preimage for
	// the exact data already stored in separately bounded columns.
	return json.Marshal(struct {
		TicketRef                                                        domain.TicketRef
		SemanticKey, RequestDigest                                       string
		TicketVersion, LeaderEpoch, RunnerEpoch, ClaimEpoch              uint64
		Repository, Worktree, WorktreeIdentity, Branch, BaseRef, BaseSHA string
		CommandDigest, SpecDigest, PolicyDigest                          string
		ExecutablePath, ExecutableDigest                                 string
		ExitCode                                                         int
		Stdout, Stderr, OutputLastMessage                                []byte
		StdoutTruncated, StderrTruncated, OutputLastMessageTruncated     bool
		DurationNS                                                       int64
		Observed                                                         bool
		ObservedAt                                                       string
		CreatedAt                                                        string
	}{
		TicketRef: claim.TicketRef, SemanticKey: claim.SemanticKey, RequestDigest: claim.RequestDigest,
		TicketVersion: claim.TicketVersion, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch,
		Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, Branch: claim.Branch, BaseRef: claim.BaseRef, BaseSHA: claim.BaseSHA,
		CommandDigest: claim.CommandDigest, SpecDigest: claim.SpecDigest, PolicyDigest: claim.PolicyDigest, ExecutablePath: claim.ExecutablePath, ExecutableDigest: claim.ExecutableDigest,
		ExitCode: result.ExitCode, Stdout: result.Stdout, Stderr: result.Stderr, OutputLastMessage: result.OutputLastMessage,
		StdoutTruncated: result.StdoutTruncated, StderrTruncated: result.StderrTruncated, OutputLastMessageTruncated: result.OutputLastMessageTruncated,
		DurationNS: result.Duration.Nanoseconds(), Observed: result.Observed, ObservedAt: result.ObservedAt.Format(time.RFC3339Nano), CreatedAt: createdAt.Format(time.RFC3339Nano),
	})
}

func validRepositoryCommandResult(claim contracts.RepositoryCommandClaim, result contracts.CommandResult) error {
	if !validRepositoryCommandClaim(claim) || !result.Observed || result.ExitCode < -1 || result.ExitCode > 255 || result.Duration < 0 || result.Duration > repositoryCommandResultMaxAge || len(result.Stdout) > repositoryCommandOutputLimit || len(result.Stderr) > repositoryCommandOutputLimit || len(result.OutputLastMessage) > repositoryCommandLastMessageLimit {
		return ErrRepositoryCommandResult
	}
	// This is supervisor-owned wall-clock evidence. Historical rows must remain
	// readable indefinitely, so structural validation has no current-time age
	// test; admission adds the short current-observation window below.
	if result.ObservedAt.IsZero() || result.ObservedAt.Location() != time.UTC {
		return ErrRepositoryCommandResult
	}
	return nil
}

func validRepositoryCommandResultAdmission(claim contracts.RepositoryCommandClaim, result contracts.CommandResult, observed time.Time) error {
	if err := validRepositoryCommandResult(claim, result); err != nil {
		return err
	}
	if result.ObservedAt.After(observed.Add(5*time.Minute)) || result.ObservedAt.Before(observed.Add(-repositoryCommandResultMaxAge)) {
		return ErrRepositoryCommandResult
	}
	return nil
}

func resultDigests(claim contracts.RepositoryCommandClaim, result contracts.CommandResult, createdAt time.Time) (stdout, stderr, last, full string, err error) {
	if err = validRepositoryCommandResult(claim, result); err != nil {
		return "", "", "", "", err
	}
	if createdAt.IsZero() || createdAt.Location() != time.UTC {
		return "", "", "", "", ErrRepositoryCommandResult
	}
	payload, err := repositoryCommandResultEnvelope(claim, result, createdAt)
	if err != nil {
		return "", "", "", "", err
	}
	return repositoryResultDigest(result.Stdout), repositoryResultDigest(result.Stderr), repositoryResultDigest(result.OutputLastMessage), repositoryResultDigest(payload), nil
}

// CompleteRepositoryCommand inserts one immutable result and settles exactly
// the same active effect in one transaction. A failed command is still
// evidence (for example, a red pre-build regression); uncertain and stale
// observations deliberately use their narrower retirement paths and cannot
// create a result row.
func (s *Store) CompleteRepositoryCommand(ctx context.Context, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) error {
	// SQLite distinguishes NULL from an empty BLOB while a command with no
	// output has one canonical transcript: an empty byte sequence. Normalize at
	// the Store boundary before hashing and inserting so replay is exact.
	if result.Stdout == nil {
		result.Stdout = []byte{}
	}
	if result.Stderr == nil {
		result.Stderr = []byte{}
	}
	if result.OutputLastMessage == nil {
		result.OutputLastMessage = []byte{}
	}
	if err := validRepositoryCommandResultAdmission(claim, result, time.Now().UTC()); err != nil {
		return ErrRepositoryCommandResult
	}
	return s.write(ctx, func(c *sql.Conn) error {
		key := contracts.RepositoryCommandResultKey{SemanticKey: claim.SemanticKey, ClaimEpoch: claim.ClaimEpoch}
		if existing, found, loadErr := loadRepositoryCommandResult(ctx, c, key, false); loadErr != nil {
			return loadErr
		} else if found {
			stdoutDigest, stderrDigest, lastDigest, fullDigest, digestErr := resultDigests(claim, result, existing.CreatedAt)
			if digestErr == nil && existing.Claim == claim && existing.Result.ExitCode == result.ExitCode && bytes.Equal(existing.Result.Stdout, result.Stdout) && bytes.Equal(existing.Result.Stderr, result.Stderr) && bytes.Equal(existing.Result.OutputLastMessage, result.OutputLastMessage) && existing.Result.StdoutTruncated == result.StdoutTruncated && existing.Result.StderrTruncated == result.StderrTruncated && existing.Result.OutputLastMessageTruncated == result.OutputLastMessageTruncated && existing.Result.Duration == result.Duration && existing.Result.ObservedAt.Equal(result.ObservedAt) && existing.ResultDigest == fullDigest && existing.StdoutDigest == stdoutDigest && existing.StderrDigest == stderrDigest && existing.OutputLastMessageDigest == lastDigest {
				return nil
			}
			return ErrRepositoryCommandResult
		}
		createdAt := time.Now().UTC()
		stdoutDigest, stderrDigest, lastDigest, fullDigest, err := resultDigests(claim, result, createdAt)
		if err != nil {
			return ErrRepositoryCommandResult
		}
		if err := s.assertRepositoryCommandCurrent(ctx, c, claim); err != nil {
			return err
		}
		// A result proves completion only after the exact recorded command child
		// has drained. In particular, no active or quarantined child can outlive
		// a result record.
		var active int
		if err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND channel=? AND project_id=? AND ticket_id=? AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND worktree_path=? AND worktree_identity=? AND branch_ref=? AND base_ref=? AND base_sha=? AND command_digest=? AND spec_digest=? AND policy_digest=? AND executable_path=? AND executable_digest=? AND state='active' AND launch_state='drained'`, claim.Repository, claim.SemanticKey, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, claim.ExecutablePath, claim.ExecutableDigest).Scan(&active); err != nil || active != 1 {
			return ErrRepositoryCommandLease
		}
		_, err = c.ExecContext(ctx, `INSERT INTO repository_command_results(semantic_key,claim_epoch,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,repository_path,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,executable_path,executable_digest,exit_code,stdout,stderr,output_last_message,stdout_truncated,stderr_truncated,output_last_message_truncated,duration_ns,observed_at,stdout_digest,stderr_digest,output_last_message_digest,result_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, claim.SemanticKey, claim.ClaimEpoch, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.Repository, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, claim.ExecutablePath, claim.ExecutableDigest, result.ExitCode, result.Stdout, result.Stderr, result.OutputLastMessage, boolInt(result.StdoutTruncated), boolInt(result.StderrTruncated), boolInt(result.OutputLastMessageTruncated), result.Duration.Nanoseconds(), result.ObservedAt.Format(time.RFC3339Nano), stdoutDigest, stderrDigest, lastDigest, fullDigest, createdAt.Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		state := EffectFailed
		if result.ExitCode == 0 {
			state = EffectConfirmed
		}
		updated, err := c.ExecContext(ctx, `UPDATE effects SET state=?,observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=?`, state, "repository-command-result:"+fullDigest, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}

// LoadRepositoryCommandResult returns authenticated historical command
// evidence. It intentionally does not inspect effects.observed_identity: that
// legacy projection is never an input to result authority.
func (s *Store) LoadRepositoryCommandResult(ctx context.Context, key contracts.RepositoryCommandResultKey) (RepositoryCommandResult, error) {
	if key.SemanticKey == "" || key.ClaimEpoch == 0 {
		return RepositoryCommandResult{}, ErrRepositoryCommandResult
	}
	result, found, err := loadRepositoryCommandResult(ctx, s.db, key, true)
	if err != nil {
		return RepositoryCommandResult{}, err
	}
	if !found {
		return RepositoryCommandResult{}, ErrNotFound
	}
	return result, nil
}

type repositoryResultQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadRepositoryCommandResult(ctx context.Context, q repositoryResultQuerier, key contracts.RepositoryCommandResultKey, authenticateRelationship bool) (RepositoryCommandResult, bool, error) {
	var out RepositoryCommandResult
	var project, ticket, observed, created string
	var stdoutTruncated, stderrTruncated, lastTruncated int
	var duration int64
	err := q.QueryRowContext(ctx, `SELECT semantic_key,claim_epoch,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,repository_path,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,executable_path,executable_digest,exit_code,stdout,stderr,output_last_message,stdout_truncated,stderr_truncated,output_last_message_truncated,duration_ns,observed_at,stdout_digest,stderr_digest,output_last_message_digest,result_digest,created_at FROM repository_command_results WHERE semantic_key=? AND claim_epoch=?`, key.SemanticKey, key.ClaimEpoch).Scan(&out.Key.SemanticKey, &out.Key.ClaimEpoch, &out.Claim.TicketRef.Channel, &project, &ticket, &out.Claim.RequestDigest, &out.Claim.TicketVersion, &out.Claim.LeaderEpoch, &out.Claim.RunnerEpoch, &out.Claim.Repository, &out.Claim.Worktree, &out.Claim.WorktreeIdentity, &out.Claim.Branch, &out.Claim.BaseRef, &out.Claim.BaseSHA, &out.Claim.CommandDigest, &out.Claim.SpecDigest, &out.Claim.PolicyDigest, &out.Claim.ExecutablePath, &out.Claim.ExecutableDigest, &out.Result.ExitCode, &out.Result.Stdout, &out.Result.Stderr, &out.Result.OutputLastMessage, &stdoutTruncated, &stderrTruncated, &lastTruncated, &duration, &observed, &out.StdoutDigest, &out.StderrDigest, &out.OutputLastMessageDigest, &out.ResultDigest, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return RepositoryCommandResult{}, false, nil
	}
	if err != nil {
		return RepositoryCommandResult{}, false, err
	}
	out.Claim.TicketRef.Project, out.Claim.TicketRef.Ticket = domain.ProjectID(project), domain.TicketID(ticket)
	out.Claim.SemanticKey = out.Key.SemanticKey
	out.Claim.ClaimEpoch = out.Key.ClaimEpoch
	if out.Result.Stdout == nil {
		out.Result.Stdout = []byte{}
	}
	if out.Result.Stderr == nil {
		out.Result.Stderr = []byte{}
	}
	if out.Result.OutputLastMessage == nil {
		out.Result.OutputLastMessage = []byte{}
	}
	out.Result.Observed = true
	out.Result.StdoutTruncated, out.Result.StderrTruncated, out.Result.OutputLastMessageTruncated = stdoutTruncated == 1, stderrTruncated == 1, lastTruncated == 1
	out.Result.Duration = time.Duration(duration)
	var parseErr error
	out.Result.ObservedAt, parseErr = time.Parse(time.RFC3339Nano, observed)
	createdAt, createdErr := time.Parse(time.RFC3339Nano, created)
	out.CreatedAt = createdAt
	if parseErr != nil || createdErr != nil || out.Result.ObservedAt.Location() != time.UTC || out.CreatedAt.Location() != time.UTC || observed != out.Result.ObservedAt.Format(time.RFC3339Nano) || created != out.CreatedAt.Format(time.RFC3339Nano) || out.Result.ObservedAt.After(out.CreatedAt.Add(5*time.Minute)) || out.Key != key || out.Claim.ClaimEpoch != key.ClaimEpoch || stdoutTruncated < 0 || stdoutTruncated > 1 || stderrTruncated < 0 || stderrTruncated > 1 || lastTruncated < 0 || lastTruncated > 1 {
		return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
	}
	stdoutDigest, stderrDigest, lastDigest, fullDigest, validErr := resultDigests(out.Claim, out.Result, out.CreatedAt)
	if validErr != nil || stdoutDigest != out.StdoutDigest || stderrDigest != out.StderrDigest || lastDigest != out.OutputLastMessageDigest || fullDigest != out.ResultDigest {
		return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
	}
	if !authenticateRelationship {
		return out, true, nil
	}
	// Results remain valid after lease and intent cleanup. If an intent still
	// names this exact epoch it must agree byte-for-byte; if the effect is still
	// at this epoch it must be terminal with the corresponding exit semantics.
	// A later claim epoch is allowed as historical retry evidence.
	var effect Effect
	var effectProject, effectTicket string
	err = q.QueryRowContext(ctx, `SELECT semantic_key,channel,project_id,ticket_id,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest,observed_identity FROM effects WHERE semantic_key=?`, out.Claim.SemanticKey).Scan(&effect.SemanticKey, &effect.Ref.Channel, &effectProject, &effectTicket, &effect.Kind, &effect.State, &effect.TicketVersion, &effect.LeaderEpoch, &effect.RunnerEpoch, &effect.ClaimEpoch, &effect.RequestDigest, &effect.ObservedIdentity)
	if err != nil {
		return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
	}
	effect.Ref.Project, effect.Ref.Ticket = domain.ProjectID(effectProject), domain.TicketID(effectTicket)
	if effect.Ref != out.Claim.TicketRef || effect.Kind != "repository_command" || effect.ClaimEpoch != out.Claim.ClaimEpoch {
		return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
	}
	if effect.RequestDigest != out.Claim.RequestDigest || effect.TicketVersion != out.Claim.TicketVersion || effect.LeaderEpoch != out.Claim.LeaderEpoch || effect.RunnerEpoch != out.Claim.RunnerEpoch || effect.ObservedIdentity != "repository-command-result:"+out.ResultDigest || (out.Result.ExitCode == 0 && effect.State != EffectConfirmed) || (out.Result.ExitCode != 0 && effect.State != EffectFailed) {
		return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
	}
	var intentClaimEpoch uint64
	var intentCount int
	err = q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(claim_epoch),0) FROM repository_command_intents WHERE semantic_key=?`, out.Claim.SemanticKey).Scan(&intentCount, &intentClaimEpoch)
	if err != nil || intentCount > 1 {
		return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
	}
	if intentCount == 1 && intentClaimEpoch == out.Claim.ClaimEpoch {
		// The old intent is optional historical context, but a retained one is
		// another immutable witness and must never disagree with the result copy.
		var n int
		err = q.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_intents WHERE semantic_key=? AND channel=? AND project_id=? AND ticket_id=? AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND repository_path=? AND worktree_path=? AND worktree_identity=? AND branch_ref=? AND base_ref=? AND base_sha=? AND command_digest=? AND spec_digest=? AND policy_digest=? AND executable_path=? AND executable_digest=?`, out.Claim.SemanticKey, out.Claim.TicketRef.Channel, out.Claim.TicketRef.Project, out.Claim.TicketRef.Ticket, out.Claim.RequestDigest, out.Claim.TicketVersion, out.Claim.LeaderEpoch, out.Claim.RunnerEpoch, out.Claim.ClaimEpoch, out.Claim.Repository, out.Claim.Worktree, out.Claim.WorktreeIdentity, out.Claim.Branch, out.Claim.BaseRef, out.Claim.BaseSHA, out.Claim.CommandDigest, out.Claim.SpecDigest, out.Claim.PolicyDigest, out.Claim.ExecutablePath, out.Claim.ExecutableDigest).Scan(&n)
		if err != nil || n != 1 {
			return RepositoryCommandResult{}, false, ErrRepositoryCommandResult
		}
	}
	return out, true, nil
}

var _ contracts.RepositoryCommandResultRecorder = (*Store)(nil)

func (s *Store) MarkRepositoryCommandUncertain(ctx context.Context, claim contracts.RepositoryCommandClaim, reason string) error {
	if !validRepositoryCommandClaim(claim) || reason == "" {
		return ErrRepositoryCommandIntent
	}
	return s.write(ctx, func(c *sql.Conn) error {
		if err := s.assertRepositoryCommandCurrent(ctx, c, claim); err != nil {
			return err
		}
		result, err := c.ExecContext(ctx, `UPDATE effects SET state='uncertain',observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=?`, "uncertain:"+reason, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}

// ReconcileStaleRepositoryCommandObservation intentionally does not persist a
// result: an observation after fence invalidation may retire its exact effect,
// but can never mint reusable command authority.
func (s *Store) ReconcileStaleRepositoryCommandObservation(ctx context.Context, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) error {
	if !validRepositoryCommandClaim(claim) || !result.Observed {
		return ErrRepositoryCommandIntent
	}
	return s.write(ctx, func(c *sql.Conn) error {
		r, err := c.ExecContext(ctx, `UPDATE effects SET state='failed',observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=? AND EXISTS (SELECT 1 FROM repository_command_intents i WHERE i.semantic_key=effects.semantic_key AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=? AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.claim_epoch=? AND i.repository_path=? AND i.worktree_path=? AND i.worktree_identity=? AND i.branch_ref=? AND i.base_ref=? AND i.base_sha=? AND i.command_digest=? AND i.spec_digest=? AND i.policy_digest=? AND i.executable_path=? AND i.executable_digest=?)`, "stale:repository-command-observation", claim.SemanticKey, claim.ClaimEpoch, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Repository, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, claim.ExecutablePath, claim.ExecutableDigest)
		if err != nil {
			return err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}
