package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

// RepositoryCommandIntent is the durable, immutable binding for a read-only
// repository command. Commands are effects and must be issued from an
// executing effect fence before a lease can be acquired.
type RepositoryCommandIntent struct {
	EffectFence
	RequestDigest                                                    string
	Repository, Worktree, WorktreeIdentity, Branch, BaseRef, BaseSHA string
	CommandDigest, SpecDigest, PolicyDigest                          string
	ExecutablePath, ExecutableDigest                                 string
}

type repositoryCommandLease struct {
	store *Store
	claim contracts.RepositoryCommandClaim
	nonce []byte
}
type RepositoryCommandRecovery struct {
	Claim       contracts.RepositoryCommandClaim
	Nonce       []byte
	State       string
	LaunchState string
	Launch      contracts.RepositoryCommandLaunch
	Groups      []contracts.RepositoryCommandLaunch
}

const repositoryCommandProcessGroupLimit = 64

func validRepositoryCommandIntent(i RepositoryCommandIntent) bool {
	return i.Ref.Validate() == nil && i.SemanticKey != "" && validClaimDigest(i.RequestDigest) && i.TicketVersion > 0 && i.Fence.LeaderEpoch > 0 && i.Fence.RunnerEpoch > 0 && validStorePath(i.Repository) && validStorePath(i.Worktree) && validRepositoryWorktreeIdentity(i.WorktreeIdentity, i.Repository, i.Worktree, i.Branch, i.BaseRef, i.BaseSHA) && validClaimDigest(i.CommandDigest) && validClaimDigest(i.SpecDigest) && validClaimDigest(i.PolicyDigest) && validStorePath(i.ExecutablePath) && validClaimDigest(i.ExecutableDigest)
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
		// The issue response is allowed to be lost. Before the gate has opened,
		// repeating the exact request must return the original durable claim,
		// rather than minting a second claim epoch or stranding the first one.
		if existing, found, err := repositoryCommandClaimForIntent(ctx, conn, intent); err != nil {
			return err
		} else if found {
			claim = existing
			return nil
		}
		var repository, worktree, identity, branch, base, durableSHA string
		err := conn.QueryRowContext(ctx, `SELECT p.canonical_path,w.path,w.identity_json,w.branch_ref,p.base_ref,w.base_sha FROM projects p JOIN worktrees w ON w.channel=p.channel AND w.project_id=p.id AND w.ticket_id=? WHERE p.channel=? AND p.id=?`, intent.Ref.Ticket, intent.Ref.Channel, intent.Ref.Project).Scan(&repository, &worktree, &identity, &branch, &base, &durableSHA)
		if errors.Is(err, sql.ErrNoRows) || err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrRepositoryCommandIntent
			}
			return err
		}
		if repository != intent.Repository || worktree != intent.Worktree || identity != intent.WorktreeIdentity || branch != intent.Branch || base != intent.BaseRef || durableSHA != intent.BaseSHA {
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
		_, err = conn.ExecContext(ctx, `INSERT INTO repository_command_intents(semantic_key,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,executable_path,executable_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, effect.TicketVersion, effect.LeaderEpoch, effect.RunnerEpoch, effect.ClaimEpoch, intent.Repository, intent.Worktree, intent.WorktreeIdentity, intent.Branch, intent.BaseRef, intent.BaseSHA, intent.CommandDigest, intent.SpecDigest, intent.PolicyDigest, intent.ExecutablePath, intent.ExecutableDigest, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		claim = contracts.RepositoryCommandClaim{TicketRef: intent.Ref, SemanticKey: intent.SemanticKey, RequestDigest: intent.RequestDigest, TicketVersion: effect.TicketVersion, LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch, Repository: intent.Repository, Worktree: intent.Worktree, WorktreeIdentity: intent.WorktreeIdentity, Branch: intent.Branch, BaseRef: intent.BaseRef, BaseSHA: intent.BaseSHA, CommandDigest: intent.CommandDigest, SpecDigest: intent.SpecDigest, PolicyDigest: intent.PolicyDigest, ExecutablePath: intent.ExecutablePath, ExecutableDigest: intent.ExecutableDigest}
		return nil
	})
	return claim, err
}

func repositoryCommandClaimForIntent(ctx context.Context, c *sql.Conn, intent RepositoryCommandIntent) (contracts.RepositoryCommandClaim, bool, error) {
	var claim contracts.RepositoryCommandClaim
	var project, ticket string
	err := c.QueryRowContext(ctx, `SELECT i.semantic_key,i.channel,i.project_id,i.ticket_id,i.request_digest,i.ticket_version,i.leader_epoch,i.runner_epoch,i.claim_epoch,i.repository_path,i.worktree_path,i.worktree_identity,i.branch_ref,i.base_ref,i.base_sha,i.command_digest,i.spec_digest,i.policy_digest,i.executable_path,i.executable_digest
		FROM repository_command_intents i JOIN effects e ON e.semantic_key=i.semantic_key
		WHERE i.semantic_key=? AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=? AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.repository_path=? AND i.worktree_path=? AND i.worktree_identity=? AND i.branch_ref=? AND i.base_ref=? AND i.base_sha=? AND i.command_digest=? AND i.spec_digest=? AND i.policy_digest=? AND i.executable_path=? AND i.executable_digest=? AND e.state='executing' AND e.claim_epoch=i.claim_epoch`, intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, intent.TicketVersion, intent.Fence.LeaderEpoch, intent.Fence.RunnerEpoch, intent.Repository, intent.Worktree, intent.WorktreeIdentity, intent.Branch, intent.BaseRef, intent.BaseSHA, intent.CommandDigest, intent.SpecDigest, intent.PolicyDigest, intent.ExecutablePath, intent.ExecutableDigest).Scan(&claim.SemanticKey, &claim.TicketRef.Channel, &project, &ticket, &claim.RequestDigest, &claim.TicketVersion, &claim.LeaderEpoch, &claim.RunnerEpoch, &claim.ClaimEpoch, &claim.Repository, &claim.Worktree, &claim.WorktreeIdentity, &claim.Branch, &claim.BaseRef, &claim.BaseSHA, &claim.CommandDigest, &claim.SpecDigest, &claim.PolicyDigest, &claim.ExecutablePath, &claim.ExecutableDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return contracts.RepositoryCommandClaim{}, false, nil
	}
	if err != nil {
		return contracts.RepositoryCommandClaim{}, false, err
	}
	claim.TicketRef.Project = domain.ProjectID(project)
	claim.TicketRef.Ticket = domain.TicketID(ticket)
	if !validRepositoryCommandClaim(claim) {
		return contracts.RepositoryCommandClaim{}, false, ErrRepositoryCommandIntent
	}
	return claim, true, nil
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
		result, err := conn.ExecContext(ctx, `INSERT INTO repository_command_leases(repository_path,semantic_key,nonce,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,executable_path,executable_digest,state,launch_state,acquired_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'active','unrecorded',?) ON CONFLICT(repository_path) DO NOTHING`, claim.Repository, claim.SemanticKey, nonce, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, claim.ExecutablePath, claim.ExecutableDigest, time.Now().UTC().Format(time.RFC3339Nano))
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
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	return l.store.write(ctx, func(c *sql.Conn) error {
		r, e := c.ExecContext(ctx, `DELETE FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state IN ('unrecorded','drained')`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
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
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	return l.store.write(ctx, func(c *sql.Conn) error {
		// The gate cannot open until the launch record commits, and drained is
		// already the supervisor's exact absence proof. Keep either witness when
		// reporting a later preparation/persistence failure; only a released
		// process has ambiguous lifecycle state.
		result, err := c.ExecContext(ctx, `UPDATE repository_command_leases SET state='quarantined',launch_state=CASE WHEN launch_state IN ('unrecorded','drained') THEN launch_state ELSE 'quarantined' END WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
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

// RecordRepositoryCommandProcessGroup is the Go-test child handshake. The
// wrapper has made itself a process-group leader but is still blocked before
// executing untrusted test code; this row must commit before the supervisor
// acknowledges the wrapper. Recovery consequently knows every group that can
// outlive the trusted Go driver.
func (l *repositoryCommandLease) RecordRepositoryCommandProcessGroup(ctx context.Context, v contracts.RepositoryCommandLaunch) error {
	if l == nil || l.store == nil || !validRepositoryCommandLaunch(v) {
		return ErrRepositoryCommandLease
	}
	return l.store.write(ctx, func(c *sql.Conn) error {
		if err := l.store.assertRepositoryCommandCurrent(ctx, c, l.claim); err != nil {
			return ErrRepositoryCommandLease
		}
		var n int
		if err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state='released'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&n); err != nil || n != 1 {
			return ErrRepositoryCommandLease
		}
		var exists int
		if err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_process_groups WHERE repository_path=? AND process_pid=? AND process_start_identity=?`, l.claim.Repository, v.PID, v.ProcessStartIdentity).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			return nil
		}
		if err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_process_groups WHERE repository_path=? AND semantic_key=? AND nonce=?`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&n); err != nil {
			return err
		}
		if n >= repositoryCommandProcessGroupLimit {
			return ErrRepositoryCommandLease
		}
		_, err := c.ExecContext(ctx, `INSERT INTO repository_command_process_groups(repository_path,semantic_key,nonce,process_pid,process_pgid,process_boot_identity,process_start_identity) VALUES(?,?,?,?,?,?,?) ON CONFLICT(repository_path,process_pid,process_start_identity) DO NOTHING`, l.claim.Repository, l.claim.SemanticKey, l.nonce, v.PID, v.PGID, v.BootIdentity, v.ProcessStartIdentity)
		return err
	})
}

func (s *Store) ActiveRepositoryCommandLeases(ctx context.Context, channel domain.Channel) ([]RepositoryCommandRecovery, error) {
	if !channel.Valid() {
		return nil, ErrRepositoryCommandLease
	}
	rows, e := s.db.QueryContext(ctx, `SELECT semantic_key,nonce,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,worktree_identity,branch_ref,base_ref,base_sha,command_digest,spec_digest,policy_digest,executable_path,executable_digest,state,launch_state,process_pid,process_pgid,process_boot_identity,process_start_identity FROM repository_command_leases WHERE channel=? ORDER BY repository_path`, channel)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []RepositoryCommandRecovery
	for rows.Next() {
		var r RepositoryCommandRecovery
		var project, ticket string
		if e := rows.Scan(&r.Claim.SemanticKey, &r.Nonce, &project, &ticket, &r.Claim.RequestDigest, &r.Claim.TicketVersion, &r.Claim.LeaderEpoch, &r.Claim.RunnerEpoch, &r.Claim.ClaimEpoch, &r.Claim.Repository, &r.Claim.Worktree, &r.Claim.WorktreeIdentity, &r.Claim.Branch, &r.Claim.BaseRef, &r.Claim.BaseSHA, &r.Claim.CommandDigest, &r.Claim.SpecDigest, &r.Claim.PolicyDigest, &r.Claim.ExecutablePath, &r.Claim.ExecutableDigest, &r.State, &r.LaunchState, &r.Launch.PID, &r.Launch.PGID, &r.Launch.BootIdentity, &r.Launch.ProcessStartIdentity); e != nil {
			return nil, e
		}
		r.Claim.TicketRef = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
		groups, x := s.repositoryCommandGroups(ctx, r.Claim.Repository, r.Claim.SemanticKey, r.Nonce)
		if x != nil {
			return nil, x
		}
		r.Groups = groups
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) repositoryCommandGroups(ctx context.Context, repository, semanticKey string, nonce []byte) ([]contracts.RepositoryCommandLaunch, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT process_pid,process_pgid,process_boot_identity,process_start_identity FROM repository_command_process_groups WHERE repository_path=? AND semantic_key=? AND nonce=? ORDER BY process_pid`, repository, semanticKey, nonce)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []contracts.RepositoryCommandLaunch
	for rows.Next() {
		if len(groups) >= repositoryCommandProcessGroupLimit {
			return nil, ErrRepositoryCommandLease
		}
		var group contracts.RepositoryCommandLaunch
		if err := rows.Scan(&group.PID, &group.PGID, &group.BootIdentity, &group.ProcessStartIdentity); err != nil {
			return nil, err
		}
		if !validRepositoryCommandLaunch(group) {
			return nil, ErrRepositoryCommandLease
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}
func (s *Store) RecoverRepositoryCommandLeases(ctx context.Context, channel domain.Channel, leader uint64, d contracts.RepositoryCommandDrainer) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	leases, e := s.ActiveRepositoryCommandLeases(ctx, channel)
	if e != nil {
		return e
	}
	for _, l := range leases {
		// The child gate opens only after RecordRepositoryCommandLaunch commits.
		// An unrecorded lease therefore proves that no repository code started;
		// retire it explicitly rather than trying to infer a nonexistent PID.
		if l.LaunchState == "unrecorded" {
			if err := s.retireRecoveredRepositoryCommand(ctx, channel, leader, l, "recovered:repository_command_unrecorded", true, "unrecorded"); err != nil {
				return err
			}
			continue
		}
		// The live supervisor writes drained only after its own exact-group
		// observation. A crash between that write and result persistence is not
		// a success, but it needs no second OS signal.
		if l.LaunchState == "drained" {
			if err := s.retireRecoveredRepositoryCommand(ctx, channel, leader, l, "recovered:repository_command_drained", true, "drained"); err != nil {
				return err
			}
			continue
		}
		treeDrainer, treeCapable := d.(contracts.RepositoryCommandTreeDrainer)
		drainErr := error(nil)
		if treeCapable {
			drainErr = treeDrainer.DrainRepositoryCommandTree(ctx, l.Launch, l.Groups)
		} else if len(l.Groups) != 0 || d == nil {
			drainErr = ErrRepositoryCommandLease
		} else if d != nil {
			drainErr = d.DrainRepositoryCommand(ctx, l.Launch)
		}
		if !validRepositoryCommandLaunch(l.Launch) || drainErr != nil {
			// Ambiguity is durable and blocks all repository writers, but daemon
			// availability must not depend on pretending an operator can resolve
			// it during socket startup. A later recovery may retry this exact row.
			if err := s.quarantineRecoveredRepositoryCommand(ctx, channel, leader, l); err != nil {
				return err
			}
			continue
		}
		if err := s.retireRecoveredRepositoryCommand(ctx, channel, leader, l, "recovered:repository_command_tree_gone", true, "launched"); err != nil {
			return err
		}
	}
	return nil
}

// RecoverUnleasedRepositoryCommands handles the narrow issue-before-acquire
// window before generic effect recovery. The supervisor gate has no launch
// identity until a lease exists, so an issued intent with no lease proves no
// child could have crossed the repository boundary. It is retired as a
// visible failure (never success), allowing a current worker to plan a fresh
// immutable claim without manual database surgery.
func (s *Store) RecoverUnleasedRepositoryCommands(ctx context.Context, channel domain.Channel, leader uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT i.semantic_key,i.channel,i.project_id,i.ticket_id,i.request_digest,i.ticket_version,i.leader_epoch,i.runner_epoch,i.claim_epoch,i.repository_path,i.worktree_path,i.worktree_identity,i.branch_ref,i.base_ref,i.base_sha,i.command_digest,i.spec_digest,i.policy_digest,i.executable_path,i.executable_digest
		FROM repository_command_intents i JOIN effects e ON e.semantic_key=i.semantic_key
		LEFT JOIN repository_command_leases l ON l.semantic_key=i.semantic_key
		WHERE i.channel=? AND l.semantic_key IS NULL AND e.state='executing' AND e.claim_epoch=i.claim_epoch ORDER BY i.semantic_key`, channel)
	if err != nil {
		return err
	}
	defer rows.Close()
	var stranded []RepositoryCommandRecovery
	for rows.Next() {
		var recovery RepositoryCommandRecovery
		var project, ticket string
		if err := rows.Scan(&recovery.Claim.SemanticKey, &recovery.Claim.TicketRef.Channel, &project, &ticket, &recovery.Claim.RequestDigest, &recovery.Claim.TicketVersion, &recovery.Claim.LeaderEpoch, &recovery.Claim.RunnerEpoch, &recovery.Claim.ClaimEpoch, &recovery.Claim.Repository, &recovery.Claim.Worktree, &recovery.Claim.WorktreeIdentity, &recovery.Claim.Branch, &recovery.Claim.BaseRef, &recovery.Claim.BaseSHA, &recovery.Claim.CommandDigest, &recovery.Claim.SpecDigest, &recovery.Claim.PolicyDigest, &recovery.Claim.ExecutablePath, &recovery.Claim.ExecutableDigest); err != nil {
			return err
		}
		recovery.Claim.TicketRef.Project = domain.ProjectID(project)
		recovery.Claim.TicketRef.Ticket = domain.TicketID(ticket)
		if !validRepositoryCommandClaim(recovery.Claim) {
			return ErrRepositoryCommandIntent
		}
		stranded = append(stranded, recovery)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, recovery := range stranded {
		if err := s.write(ctx, func(c *sql.Conn) error {
			if err := assertRepositoryRecoveryLeader(ctx, c, channel, leader); err != nil {
				return err
			}
			if err := s.setRecoveredRepositoryCommandEffect(ctx, c, recovery, EffectFailed, "recovered:repository_command_unleased"); err != nil {
				return err
			}
			r, err := c.ExecContext(ctx, `DELETE FROM repository_command_intents WHERE semantic_key=? AND NOT EXISTS(SELECT 1 FROM repository_command_leases WHERE semantic_key=repository_command_intents.semantic_key)`, recovery.Claim.SemanticKey)
			if err != nil {
				return err
			}
			if n, _ := r.RowsAffected(); n != 1 {
				return ErrRepositoryCommandIntent
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// retireRecoveredRepositoryCommand records a visible non-success outcome and
// deletes only the exact durable lease once either the unopened gate or the
// complete authenticated process tree has been proved gone. Quarantined rows
// remain eligible: a later successful proof is how an operator clears one.
func (s *Store) retireRecoveredRepositoryCommand(ctx context.Context, channel domain.Channel, leader uint64, lease RepositoryCommandRecovery, identity string, proven bool, launchState string) error {
	if !proven {
		return ErrRepositoryCommandLease
	}
	return s.write(ctx, func(c *sql.Conn) error {
		if err := assertRepositoryRecoveryLeader(ctx, c, channel, leader); err != nil {
			return err
		}
		if err := s.setRecoveredRepositoryCommandEffect(ctx, c, lease, EffectFailed, identity); err != nil {
			return err
		}
		query := `DELETE FROM repository_command_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state IN ('active','quarantined')`
		args := []any{lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce}
		if launchState == "unrecorded" {
			query += ` AND launch_state='unrecorded'`
		} else if launchState == "launched" {
			query += ` AND launch_state IN ('released','quarantined')`
			query += ` AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?`
			args = append(args, lease.Launch.PID, lease.Launch.PGID, lease.Launch.BootIdentity, lease.Launch.ProcessStartIdentity)
		} else if launchState == "drained" {
			query += ` AND launch_state='drained' AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?`
			args = append(args, lease.Launch.PID, lease.Launch.PGID, lease.Launch.BootIdentity, lease.Launch.ProcessStartIdentity)
		} else {
			return ErrRepositoryCommandLease
		}
		r, err := c.ExecContext(ctx, query, args...)
		if err != nil {
			return err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		var observed string
		if err := c.QueryRowContext(ctx, `SELECT observed_identity FROM effects WHERE semantic_key=?`, lease.Claim.SemanticKey).Scan(&observed); err != nil {
			return err
		}
		if observed == identity {
			deleted, err := c.ExecContext(ctx, `DELETE FROM repository_command_intents WHERE semantic_key=?`, lease.Claim.SemanticKey)
			if err != nil {
				return err
			}
			if n, _ := deleted.RowsAffected(); n != 1 {
				return ErrRepositoryCommandIntent
			}
		}
		return nil
	})
}

func (s *Store) quarantineRecoveredRepositoryCommand(ctx context.Context, channel domain.Channel, leader uint64, lease RepositoryCommandRecovery) error {
	return s.write(ctx, func(c *sql.Conn) error {
		if err := assertRepositoryRecoveryLeader(ctx, c, channel, leader); err != nil {
			return err
		}
		if err := s.setRecoveredRepositoryCommandEffect(ctx, c, lease, EffectUncertain, "uncertain:repository_command_recovery"); err != nil {
			return err
		}
		r, err := c.ExecContext(ctx, `UPDATE repository_command_leases SET state='quarantined',launch_state=CASE WHEN launch_state IN ('unrecorded','drained') THEN launch_state ELSE 'quarantined' END WHERE repository_path=? AND semantic_key=? AND nonce=? AND state IN ('active','quarantined')`, lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce)
		if err != nil {
			return err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandLease
		}
		return nil
	})
}

func assertRepositoryRecoveryLeader(ctx context.Context, c *sql.Conn, channel domain.Channel, leader uint64) error {
	var current uint64
	if err := c.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); err != nil {
		return err
	}
	if current != leader {
		return ErrStaleFence
	}
	return nil
}

// setRecoveredRepositoryCommandEffect intentionally authenticates the whole
// immutable intent but not the old ticket fence: recovery runs after the
// crashed runner may have been invalidated. A later recovery-linked uncertain
// effect may have a newer leader/claim epoch, but no new command can reuse its
// semantic key while that intent remains. Terminal observations are preserved.
func (s *Store) setRecoveredRepositoryCommandEffect(ctx context.Context, c *sql.Conn, lease RepositoryCommandRecovery, state EffectState, identity string) error {
	if state != EffectFailed && state != EffectUncertain {
		return ErrRepositoryCommandIntent
	}
	if err := repositoryCommandIntentMatches(ctx, c, lease.Claim); err != nil {
		return err
	}
	effect, err := effectFrom(ctx, c, lease.Claim.SemanticKey)
	if err != nil {
		return ErrRepositoryCommandIntent
	}
	if effect.Ref != lease.Claim.TicketRef || effect.Kind != "repository_command" || effect.RequestDigest != lease.Claim.RequestDigest || effect.TicketVersion != lease.Claim.TicketVersion || effect.RunnerEpoch != lease.Claim.RunnerEpoch {
		return ErrRepositoryCommandIntent
	}
	if effect.State == EffectConfirmed || effect.State == EffectFailed {
		// Completion was durable before the crash. Recovery may release a
		// proved-gone lease but must not overwrite its observed outcome.
		return nil
	}
	if effect.State == EffectExecuting {
		if effect.LeaderEpoch != lease.Claim.LeaderEpoch || effect.ClaimEpoch != lease.Claim.ClaimEpoch {
			return ErrRepositoryCommandIntent
		}
	} else if effect.State == EffectUncertain {
		if effect.LeaderEpoch < lease.Claim.LeaderEpoch || effect.ClaimEpoch < lease.Claim.ClaimEpoch {
			return ErrRepositoryCommandIntent
		}
	} else {
		return ErrRepositoryCommandIntent
	}
	if effect.State == state {
		return nil
	}
	r, err := c.ExecContext(ctx, `UPDATE effects SET state=?,observed_identity=? WHERE semantic_key=? AND state=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?`, state, identity, effect.SemanticKey, effect.State, effect.TicketVersion, effect.LeaderEpoch, effect.RunnerEpoch, effect.ClaimEpoch)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n != 1 {
		return ErrRepositoryCommandIntent
	}
	return nil
}

func repositoryCommandIntentMatches(ctx context.Context, c *sql.Conn, claim contracts.RepositoryCommandClaim) error {
	var n int
	err := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_intents WHERE semantic_key=? AND channel=? AND project_id=? AND ticket_id=? AND request_digest=? AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND repository_path=? AND worktree_path=? AND worktree_identity=? AND branch_ref=? AND base_ref=? AND base_sha=? AND command_digest=? AND spec_digest=? AND policy_digest=? AND executable_path=? AND executable_digest=?`, claim.SemanticKey, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Repository, claim.Worktree, claim.WorktreeIdentity, claim.Branch, claim.BaseRef, claim.BaseSHA, claim.CommandDigest, claim.SpecDigest, claim.PolicyDigest, claim.ExecutablePath, claim.ExecutableDigest).Scan(&n)
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrRepositoryCommandIntent
	}
	return nil
}

func validRepositoryCommandClaim(c contracts.RepositoryCommandClaim) bool {
	return c.TicketRef.Validate() == nil && c.SemanticKey != "" && validClaimDigest(c.RequestDigest) && c.TicketVersion > 0 && c.LeaderEpoch > 0 && c.RunnerEpoch > 0 && c.ClaimEpoch > 0 && validStorePath(c.Repository) && validStorePath(c.Worktree) && validRepositoryWorktreeIdentity(c.WorktreeIdentity, c.Repository, c.Worktree, c.Branch, c.BaseRef, c.BaseSHA) && validClaimDigest(c.CommandDigest) && validClaimDigest(c.SpecDigest) && validClaimDigest(c.PolicyDigest) && validStorePath(c.ExecutablePath) && validClaimDigest(c.ExecutableDigest)
}

// validRepositoryWorktreeIdentity rejects the historic opaque JSON blob.  The
// command authority needs the exact Git snapshot shape so the supervisor can
// compare an opened worktree FD to the durable device/inode witness before it
// opens the launch gate.
func validRepositoryWorktreeIdentity(raw, repository, worktree, branch, baseRef, baseSHA string) bool {
	var identity gitboundary.Identity
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&identity) != nil || identity.Repository != repository || identity.Worktree != worktree || identity.HeadRef != branch || identity.BaseRef != baseRef || identity.BaseHead != baseSHA {
		return false
	}
	for _, value := range []string{identity.Repository, identity.Worktree, identity.GitFile, identity.CommonDir, identity.Origin, identity.PushOrigin, identity.ConfigHash, identity.HooksHash} {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	gitFileTarget, ok := strings.CutPrefix(identity.GitFile, "gitdir: ")
	gitFileTarget = strings.TrimSpace(gitFileTarget)
	if !ok || !filepath.IsAbs(gitFileTarget) || filepath.Clean(gitFileTarget) != gitFileTarget {
		return false
	}
	for _, pair := range [][2]uint64{{identity.RepositoryDev, identity.RepositoryIno}, {identity.WorktreeDev, identity.WorktreeIno}, {identity.GitFileDev, identity.GitFileIno}, {identity.CommonDirDev, identity.CommonDirIno}} {
		if pair[0] == 0 || pair[1] == 0 {
			return false
		}
	}
	if filepath.IsAbs(identity.PushOrigin) && (identity.PushOriginDev == 0 || identity.PushOriginIno == 0) {
		return false
	}
	return true
}
func (s *Store) assertRepositoryCommandCurrent(ctx context.Context, c *sql.Conn, v contracts.RepositoryCommandClaim) error {
	if e := s.assertTicketFence(ctx, c, v.TicketRef, v.TicketVersion, domain.Fence{LeaderEpoch: v.LeaderEpoch, RunnerEpoch: v.RunnerEpoch}); e != nil {
		return e
	}
	var n int
	e := c.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_intents i JOIN effects e ON e.semantic_key=i.semantic_key WHERE i.semantic_key=? AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=? AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.claim_epoch=? AND i.repository_path=? AND i.worktree_path=? AND i.worktree_identity=? AND i.branch_ref=? AND i.base_ref=? AND i.base_sha=? AND i.command_digest=? AND i.spec_digest=? AND i.policy_digest=? AND i.executable_path=? AND i.executable_digest=? AND e.state='executing' AND e.claim_epoch=i.claim_epoch`, v.SemanticKey, v.TicketRef.Channel, v.TicketRef.Project, v.TicketRef.Ticket, v.RequestDigest, v.TicketVersion, v.LeaderEpoch, v.RunnerEpoch, v.ClaimEpoch, v.Repository, v.Worktree, v.WorktreeIdentity, v.Branch, v.BaseRef, v.BaseSHA, v.CommandDigest, v.SpecDigest, v.PolicyDigest, v.ExecutablePath, v.ExecutableDigest).Scan(&n)
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
