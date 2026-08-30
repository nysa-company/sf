package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// RepositoryCommandIntent is the durable, immutable binding for a read-only
// repository command. Commands are effects and must be issued from an
// executing effect fence before a lease can be acquired.
type RepositoryCommandIntent struct {
	EffectFence
	RequestDigest                                                    string
	Repository, Worktree, WorktreeIdentity, Branch, BaseRef, BaseSHA string
	CommandDigest, SpecDigest, PolicyDigest                          string
}

type repositoryCommandLease struct {
	store *Store
	claim contracts.RepositoryCommandClaim
	nonce []byte
}
type RepositoryCommandRecovery struct {
	Claim  contracts.RepositoryCommandClaim
	Nonce  []byte
	State  string
	Launch contracts.RepositoryCommandLaunch
}

func validRepositoryCommandIntent(i RepositoryCommandIntent) bool {
	return i.Ref.Validate() == nil && i.SemanticKey != "" && validClaimDigest(i.RequestDigest) && i.TicketVersion > 0 && i.Fence.LeaderEpoch > 0 && i.Fence.RunnerEpoch > 0 && validStorePath(i.Repository) && validStorePath(i.Worktree) && i.WorktreeIdentity != "" && i.Branch != "" && i.BaseRef != "" && validStoreOID(i.BaseSHA) && validClaimDigest(i.CommandDigest) && validClaimDigest(i.SpecDigest) && validClaimDigest(i.PolicyDigest)
}

func (s *Store) IssueRepositoryCommandClaim(ctx context.Context, intent RepositoryCommandIntent) (contracts.RepositoryCommandClaim, error) {
	if !validRepositoryCommandIntent(intent) {
		return contracts.RepositoryCommandClaim{}, ErrRepositoryCommandIntent
	}
	var claim contracts.RepositoryCommandClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, intent.Ref, intent.TicketVersion, intent.Fence); err != nil {
			return err
		}
		var repository, worktree, identity, branch, base string
		err := conn.QueryRowContext(ctx, `SELECT p.canonical_path,w.path,w.identity_json,w.branch_ref,p.base_ref FROM projects p JOIN worktrees w ON w.channel=p.channel AND w.project_id=p.id AND w.ticket_id=? WHERE p.channel=? AND p.id=?`, intent.Ref.Ticket, intent.Ref.Channel, intent.Ref.Project).Scan(&repository, &worktree, &identity, &branch, &base)
		if errors.Is(err, sql.ErrNoRows) || err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrRepositoryCommandIntent
			}
			return err
		}
		if repository != intent.Repository || worktree != intent.Worktree || identity != intent.WorktreeIdentity || branch != intent.Branch || base != intent.BaseRef {
			return ErrRepositoryCommandIntent
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return ErrRepositoryCommandIntent
		}
		if effect.Ref != intent.Ref || effect.Kind != "repository_command" || effect.RequestDigest != intent.RequestDigest || effect.TicketVersion != intent.TicketVersion || effect.LeaderEpoch != intent.Fence.LeaderEpoch || effect.RunnerEpoch != intent.Fence.RunnerEpoch || effect.State == EffectConfirmed || effect.State == EffectUncertain || effect.State == EffectExecuting {
			return ErrRepositoryCommandIntent
		}
		result, err := conn.ExecContext(ctx, `UPDATE effects SET state='executing',claim_epoch=claim_epoch+1 WHERE semantic_key=? AND state IN ('planned','failed') AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND request_digest=?`, intent.SemanticKey, intent.TicketVersion, intent.Fence.LeaderEpoch, intent.Fence.RunnerEpoch, intent.RequestDigest)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		effect, err = effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO repository_command_intents(semantic_key,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, effect.TicketVersion, effect.LeaderEpoch, effect.RunnerEpoch, effect.ClaimEpoch, intent.Repository, intent.Worktree, intent.WorktreeIdentity, intent.Branch, intent.BaseRef, intent.BaseSHA, intent.CommandDigest, intent.SpecDigest, intent.PolicyDigest, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		claim = contracts.RepositoryCommandClaim{TicketRef: intent.Ref, SemanticKey: intent.SemanticKey, RequestDigest: intent.RequestDigest, TicketVersion: effect.TicketVersion, LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch, Repository: intent.Repository, Worktree: intent.Worktree, WorktreeIdentity: intent.WorktreeIdentity, Branch: intent.Branch, BaseRef: intent.BaseRef, BaseSHA: intent.BaseSHA, CommandDigest: intent.CommandDigest, SpecDigest: intent.SpecDigest, PolicyDigest: intent.PolicyDigest}
		return nil
	})
	return claim, err
}

func (s *Store) AcquireRepositoryCommand(ctx context.Context, claim contracts.RepositoryCommandClaim) (contracts.RepositoryCommandLease, error) {
	if !validRepositoryCommandClaim(claim) {
		return nil, ErrRepositoryCommandIntent
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertRepositoryCommandCurrent(ctx, conn, claim); err != nil {
			return err
		}
		if err := repositoryHasProviderWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		if err := repositoryHasGitWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO repository_command_leases(repository_path,semantic_key,nonce,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,state,launch_state,acquired_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'active','unrecorded',?) ON CONFLICT(repository_path) DO NOTHING`, claim.Repository, claim.SemanticKey, nonce, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &repositoryCommandLease{store: s, claim: claim, nonce: nonce}, nil
}

func (l *repositoryCommandLease) Check(ctx context.Context) error {
	if l == nil || l.store == nil {
		return ErrRepositoryCommandLease
	}
	var n int
	if err := l.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&n); err != nil || n != 1 {
		return ErrRepositoryCommandLease
	}
	return l.store.write(ctx, func(c *sql.Conn) error { return l.store.assertRepositoryCommandCurrent(ctx, c, l.claim) })
}
func (l *repositoryCommandLease) Release() error {
	if l == nil || l.store == nil {
		return ErrRepositoryCommandLease
	}
	return l.store.write(context.Background(), func(c *sql.Conn) error {
		if err := l.store.assertRepositoryCommandCurrent(context.Background(), c, l.claim); err != nil {
			return ErrRepositoryCommandLease
		}
		r, e := c.ExecContext(context.Background(), `DELETE FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state IN ('unrecorded','drained')`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if e != nil {
			return e
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		return nil
	})
}

func (l *repositoryCommandLease) Quarantine() error {
	if l == nil || l.store == nil {
		return ErrRepositoryCommandLease
	}
	return l.store.write(context.Background(), func(c *sql.Conn) error {
		result, err := c.ExecContext(context.Background(), `UPDATE repository_command_leases SET state='quarantined',launch_state='quarantined' WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		return nil
	})
}
func validRepositoryCommandLaunch(v contracts.RepositoryCommandLaunch) bool {
	return v.PID > 0 && v.PGID > 0 && v.PID == v.PGID && v.BootIdentity != "" && v.ProcessStartIdentity != ""
}
func (l *repositoryCommandLease) RecordRepositoryCommandLaunch(ctx context.Context, v contracts.RepositoryCommandLaunch) error {
	if l == nil || l.store == nil || !validRepositoryCommandLaunch(v) {
		return ErrRepositoryCommandLease
	}
	return l.store.write(ctx, func(c *sql.Conn) error {
		if err := l.store.assertRepositoryCommandCurrent(ctx, c, l.claim); err != nil {
			return ErrRepositoryCommandLease
		}
		r, e := c.ExecContext(ctx, `UPDATE repository_command_leases SET launch_state='released',process_pid=?,process_pgid=?,process_boot_identity=?,process_start_identity=?,launched_at=? WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state='unrecorded'`, v.PID, v.PGID, v.BootIdentity, v.ProcessStartIdentity, time.Now().UTC().Format(time.RFC3339Nano), l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if e != nil {
			return e
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		return nil
	})
}
func (l *repositoryCommandLease) FinishRepositoryCommandLaunch(ctx context.Context, v contracts.RepositoryCommandLaunch) error {
	if l == nil || l.store == nil || !validRepositoryCommandLaunch(v) {
		return ErrRepositoryCommandLease
	}
	return l.store.write(ctx, func(c *sql.Conn) error {
		r, e := c.ExecContext(ctx, `UPDATE repository_command_leases SET launch_state='drained' WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state='released' AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?`, l.claim.Repository, l.claim.SemanticKey, l.nonce, v.PID, v.PGID, v.BootIdentity, v.ProcessStartIdentity)
		if e != nil {
			return e
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		return nil
	})
}

func (s *Store) ActiveRepositoryCommandLeases(ctx context.Context, channel domain.Channel) ([]RepositoryCommandRecovery, error) {
	if !channel.Valid() {
		return nil, ErrRepositoryCommandLease
	}
	rows, e := s.db.QueryContext(ctx, `SELECT semantic_key,nonce,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,state,process_pid,process_pgid,process_boot_identity,process_start_identity FROM repository_command_leases WHERE channel=? ORDER BY repository_path`, channel)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []RepositoryCommandRecovery
	for rows.Next() {
		var r RepositoryCommandRecovery
		var project, ticket string
		if e := rows.Scan(&r.Claim.SemanticKey, &r.Nonce, &project, &ticket, &r.Claim.RequestDigest, &r.Claim.TicketVersion, &r.Claim.LeaderEpoch, &r.Claim.RunnerEpoch, &r.Claim.ClaimEpoch, &r.Claim.Repository, &r.Claim.Worktree, &r.Claim.WorktreeIdentity, &r.Claim.Branch, &r.Claim.BaseRef, &r.Claim.BaseSHA, &r.Claim.CommandDigest, &r.Claim.SpecDigest, &r.Claim.PolicyDigest, &r.State, &r.Launch.PID, &r.Launch.PGID, &r.Launch.BootIdentity, &r.Launch.ProcessStartIdentity); e != nil {
			return nil, e
		}
		r.Claim.TicketRef = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Store) RecoverRepositoryCommandLeases(ctx context.Context, channel domain.Channel, leader uint64, d contracts.RepositoryCommandDrainer) error {
	leases, e := s.ActiveRepositoryCommandLeases(ctx, channel)
	if e != nil {
		return e
	}
	for _, l := range leases {
		if !validRepositoryCommandLaunch(l.Launch) || d == nil || d.DrainRepositoryCommand(ctx, l.Launch) != nil {
			_ = s.write(ctx, func(c *sql.Conn) error {
				_, x := c.ExecContext(ctx, `UPDATE repository_command_leases SET state='quarantined',launch_state='quarantined' WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, l.Claim.Repository, l.Claim.SemanticKey, l.Nonce)
				return x
			})
			return ErrRepositoryCommandLease
		}
		e = s.write(ctx, func(c *sql.Conn) error {
			var current uint64
			if x := c.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); x != nil {
				return x
			}
			if current != leader {
				return ErrStaleFence
			}
			r, x := c.ExecContext(ctx, `DELETE FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?`, l.Claim.Repository, l.Claim.SemanticKey, l.Nonce, l.Launch.PID, l.Launch.PGID, l.Launch.BootIdentity, l.Launch.ProcessStartIdentity)
			if x != nil {
				return x
			}
			if n, _ := r.RowsAffected(); n != 1 {
				return ErrRepositoryCommandLease
			}
			return nil
		})
		if e != nil {
			return e
		}
	}
	return nil
}

func validRepositoryCommandClaim(c contracts.RepositoryCommandClaim) bool {
	return c.TicketRef.Validate() == nil && c.SemanticKey != "" && validClaimDigest(c.RequestDigest) && c.TicketVersion > 0 && c.LeaderEpoch > 0 && c.RunnerEpoch > 0 && c.ClaimEpoch > 0 && validStorePath(c.Repository) && validStorePath(c.Worktree) && c.WorktreeIdentity != "" && c.Branch != "" && c.BaseRef != "" && validStoreOID(c.BaseSHA) && validClaimDigest(c.CommandDigest) && validClaimDigest(c.SpecDigest) && validClaimDigest(c.PolicyDigest)
}
func (s *Store) assertRepositoryCommandCurrent(ctx context.Context, c *sql.Conn, v contracts.RepositoryCommandClaim) error {
	if e := s.assertTicketFence(ctx, c, v.TicketRef, v.TicketVersion, domain.Fence{LeaderEpoch: v.LeaderEpoch, RunnerEpoch: v.RunnerEpoch}); e != nil {
		return e
	}
	var n int
	e := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_intents i JOIN effects e ON e.semantic_key=i.semantic_key WHERE i.semantic_key=? AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=? AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.claim_epoch=? AND i.repository_path=? AND i.worktree_path=? AND i.worktree_identity=? AND i.branch_ref=? AND i.base_ref=? AND i.base_sha=? AND i.command_digest=? AND i.spec_digest=? AND i.policy_digest=? AND e.state='executing' AND e.claim_epoch=i.claim_epoch`, v.SemanticKey, v.TicketRef.Channel, v.TicketRef.Project, v.TicketRef.Ticket, v.RequestDigest, v.TicketVersion, v.LeaderEpoch, v.RunnerEpoch, v.ClaimEpoch, v.Repository, v.Worktree, v.WorktreeIdentity, v.Branch, v.BaseRef, v.BaseSHA, v.CommandDigest, v.SpecDigest, v.PolicyDigest).Scan(&n)
	if e != nil {
		return e
	}
	if n != 1 {
		return ErrRepositoryCommandIntent
	}
	return nil
}
func repositoryHasGitWriter(ctx context.Context, c *sql.Conn, repository string) error {
	var n int
	if e := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=? AND state IN ('active','quarantined')`, repository).Scan(&n); e != nil {
		return e
	}
	if n != 0 {
		return ErrGitMutationLease
	}
	return nil
}

var _ contracts.RepositoryCommandAuthority = (*Store)(nil)
