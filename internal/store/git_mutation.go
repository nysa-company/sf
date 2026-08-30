package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// GitMutationIntent is the store-owned description of one Git effect.  It is
// deliberately separate from contracts.GitMutationClaim: callers propose the
// effect binding here, but only SQLite can issue a claim after it has made the
// corresponding effect executing.
type GitMutationIntent struct {
	EffectFence
	RequestDigest   string
	Repository      string
	Worktree        string
	Branch          string
	Operation       string
	BaseRef         string
	ExpectedBaseOID string
	ExpectedHeadOID string
}

// RepositoryWriterIntent admits a credential-free repository command under
// the same exclusion as Git.  It shares the full effect fence, but does not
// expose a generic process/argv surface.
type RepositoryWriterIntent struct {
	EffectFence
	RequestDigest string
	Repository    string
	Worktree      string
	Operation     string
}

type gitMutationLease struct {
	store *Store
	claim contracts.GitMutationClaim
	nonce []byte
}

type repositoryWriterLease struct {
	store *Store
	claim contracts.GitMutationClaim
	nonce []byte
}

// GitMutationRecovery is the exact persisted repository child that must be
// drained before startup can admit any writer for its repository.
type GitMutationRecovery struct {
	Claim  contracts.GitMutationClaim
	Nonce  []byte
	State  string
	Launch contracts.GitMutationLaunch
}

func validGitIntent(i GitMutationIntent) bool {
	return i.Ref.Validate() == nil && i.SemanticKey != "" && validClaimDigest(i.RequestDigest) && i.TicketVersion != 0 &&
		i.Fence.LeaderEpoch != 0 && i.Fence.RunnerEpoch != 0 && validStorePath(i.Repository) && validStorePath(i.Worktree) &&
		i.Branch != "" && i.Operation != "" && i.BaseRef != "" && validStoreOID(i.ExpectedBaseOID) &&
		(i.ExpectedHeadOID == "" || validStoreOID(i.ExpectedHeadOID))
}

func validRepositoryWriterIntent(i RepositoryWriterIntent) bool {
	return i.Ref.Validate() == nil && i.SemanticKey != "" && validClaimDigest(i.RequestDigest) && i.TicketVersion != 0 &&
		i.Fence.LeaderEpoch != 0 && i.Fence.RunnerEpoch != 0 && validStorePath(i.Repository) && validStorePath(i.Worktree) && i.Operation != ""
}

func validStorePath(v string) bool {
	return filepath.IsAbs(v) && filepath.Clean(v) == v && v != string(filepath.Separator)
}
func validStoreOID(v string) bool {
	if len(v) != 40 && len(v) != 64 {
		return false
	}
	for _, c := range v {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
func validClaimDigest(v string) bool {
	if !strings.HasPrefix(v, "sha256:") || len(v) != len("sha256:")+64 {
		return false
	}
	for _, c := range strings.TrimPrefix(v, "sha256:") {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// IssueGitMutationClaim atomically establishes the external effect claim and
// persists its complete immutable Git binding.  It is the only minting path;
// AcquireGitMutation merely verifies this record and cannot elevate arbitrary
// caller input into authority.
func (s *Store) IssueGitMutationClaim(ctx context.Context, intent GitMutationIntent) (contracts.GitMutationClaim, error) {
	if !validGitIntent(intent) {
		return contracts.GitMutationClaim{}, ErrGitMutationIntent
	}
	var claim contracts.GitMutationClaim
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, intent.Ref, intent.TicketVersion, intent.Fence); err != nil {
			return err
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrNotFound) {
				if _, err := conn.ExecContext(ctx, `INSERT INTO effects(semantic_key,channel,project_id,ticket_id,effect_kind,state,ticket_version,leader_epoch,runner_epoch,claim_epoch,request_digest) VALUES(?,?,?,?,?,'planned',?,?,?,?,?)`, intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, "git/"+intent.Operation, intent.TicketVersion, intent.Fence.LeaderEpoch, intent.Fence.RunnerEpoch, 0, intent.RequestDigest); err != nil {
					return err
				}
				effect, err = effectFrom(ctx, conn, intent.SemanticKey)
			}
			if err != nil {
				return err
			}
		}
		if effect.Ref != intent.Ref || effect.Kind != "git/"+intent.Operation || effect.RequestDigest != intent.RequestDigest || effect.TicketVersion != intent.TicketVersion || effect.LeaderEpoch != intent.Fence.LeaderEpoch || effect.RunnerEpoch != intent.Fence.RunnerEpoch {
			return ErrGitMutationIntent
		}
		if effect.State == EffectConfirmed || effect.State == EffectUncertain || effect.State == EffectExecuting {
			return ErrGitMutationIntent
		}
		updated, err := conn.ExecContext(ctx, `UPDATE effects SET state='executing',claim_epoch=claim_epoch+1 WHERE semantic_key=? AND state IN ('planned','failed') AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND request_digest=?`, intent.SemanticKey, intent.TicketVersion, intent.Fence.LeaderEpoch, intent.Fence.RunnerEpoch, intent.RequestDigest)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrGitMutationIntent
		}
		effect, err = effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO git_mutation_intents(semantic_key,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,branch_ref,operation,base_ref,expected_base_oid,expected_head_oid,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, intent.SemanticKey, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, effect.TicketVersion, effect.LeaderEpoch, effect.RunnerEpoch, effect.ClaimEpoch, intent.Repository, intent.Worktree, intent.Branch, intent.Operation, intent.BaseRef, intent.ExpectedBaseOID, intent.ExpectedHeadOID, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		claim = contracts.GitMutationClaim{TicketRef: intent.Ref, SemanticKey: intent.SemanticKey, RequestDigest: intent.RequestDigest, TicketVersion: effect.TicketVersion, LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch, Repository: intent.Repository, Worktree: intent.Worktree, Branch: intent.Branch, Operation: intent.Operation, BaseRef: intent.BaseRef, ExpectedBaseOID: intent.ExpectedBaseOID, ExpectedHeadOID: intent.ExpectedHeadOID}
		return nil
	})
	return claim, err
}

// AcquireGitMutation implements contracts.GitMutationAuthority.  It admits a
// claim only when every caller field equals the immutable intent and its exact
// effect remains executing under the current ticket fence.
func (s *Store) AcquireGitMutation(ctx context.Context, claim contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	if !validContractClaim(claim) {
		return nil, ErrGitMutationIntent
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("git mutation nonce: %w", err)
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertGitIntentCurrent(ctx, conn, claim); err != nil {
			return err
		}
		if err := repositoryHasProviderWriter(ctx, conn, claim.Repository); err != nil {
			return err
		}
		var n int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_writer_leases WHERE repository_path=?`, claim.Repository).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return ErrRepositoryWriter
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO git_mutation_leases(repository_path,semantic_key,nonce,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,worktree_path,branch_ref,operation,base_ref,expected_base_oid,expected_head_oid,state,launch_state,acquired_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'active','unrecorded',?) ON CONFLICT(repository_path) DO NOTHING`, claim.Repository, claim.SemanticKey, nonce, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket, claim.RequestDigest, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch, claim.ClaimEpoch, claim.Worktree, claim.Branch, claim.Operation, claim.BaseRef, claim.ExpectedBaseOID, claim.ExpectedHeadOID, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gitMutationLease{store: s, claim: claim, nonce: nonce}, nil
}

func (l *gitMutationLease) Check(ctx context.Context) error {
	if l == nil || l.store == nil {
		return ErrGitMutationLease
	}
	var found int
	err := l.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&found)
	if err != nil || found != 1 {
		return ErrGitMutationLease
	}
	err = l.store.write(ctx, func(conn *sql.Conn) error { return l.store.assertGitIntentCurrent(ctx, conn, l.claim) })
	return err
}
func (l *gitMutationLease) Release() error {
	if l == nil || l.store == nil {
		return ErrGitMutationLease
	}
	return l.store.write(context.Background(), func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(context.Background(), conn, l.claim); err != nil {
			return ErrGitMutationLease
		}
		result, err := conn.ExecContext(context.Background(), `DELETE FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state IN ('unrecorded','drained')`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
}

func validGitLaunch(v contracts.GitMutationLaunch) bool {
	return v.PID > 0 && v.PGID > 0 && v.PID == v.PGID && v.BootIdentity != "" && v.ProcessStartIdentity != ""
}

// RecordGitMutationLaunch is called while the child is still behind the Git
// helper's inherited gate.  No launch record means recovery has no identity
// to drain and will quarantine rather than guess.
func (l *gitMutationLease) RecordGitMutationLaunch(ctx context.Context, launch contracts.GitMutationLaunch) error {
	if l == nil || l.store == nil || !validGitLaunch(launch) {
		return ErrGitMutationLease
	}
	return l.store.write(ctx, func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(ctx, conn, l.claim); err != nil {
			return ErrGitMutationLease
		}
		row, err := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET launch_state='released',process_pid=?,process_pgid=?,process_boot_identity=?,process_start_identity=?,launched_at=? WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state IN ('unrecorded','drained')`, launch.PID, launch.PGID, launch.BootIdentity, launch.ProcessStartIdentity, time.Now().UTC().Format(time.RFC3339Nano), l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if err != nil {
			return err
		}
		if n, _ := row.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
}
func (l *gitMutationLease) FinishGitMutationLaunch(ctx context.Context, launch contracts.GitMutationLaunch) error {
	if l == nil || l.store == nil || !validGitLaunch(launch) {
		return ErrGitMutationLease
	}
	return l.store.write(ctx, func(conn *sql.Conn) error {
		row, err := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET launch_state='drained' WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state='released' AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?`, l.claim.Repository, l.claim.SemanticKey, l.nonce, launch.PID, launch.PGID, launch.BootIdentity, launch.ProcessStartIdentity)
		if err != nil {
			return err
		}
		if n, _ := row.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
}

// ActiveGitMutationLeases exposes only exact, durable launch identities to
// daemon startup.  It deliberately includes unrecorded leases so that a
// process crash in the pre-exec window is quarantined, never timed out.
func (s *Store) ActiveGitMutationLeases(ctx context.Context, channel domain.Channel) ([]GitMutationRecovery, error) {
	if !channel.Valid() {
		return nil, ErrGitMutationLease
	}
	rows, err := s.db.QueryContext(ctx, `SELECT semantic_key,nonce,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,branch_ref,operation,base_ref,expected_base_oid,expected_head_oid,state,process_pid,process_pgid,process_boot_identity,process_start_identity FROM git_mutation_leases WHERE channel=? ORDER BY repository_path`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var out []GitMutationRecovery
	for rows.Next() {
		var r GitMutationRecovery
		var project, ticket string
		if err := rows.Scan(&r.Claim.SemanticKey, &r.Nonce, &project, &ticket, &r.Claim.RequestDigest, &r.Claim.TicketVersion, &r.Claim.LeaderEpoch, &r.Claim.RunnerEpoch, &r.Claim.ClaimEpoch, &r.Claim.Repository, &r.Claim.Worktree, &r.Claim.Branch, &r.Claim.Operation, &r.Claim.BaseRef, &r.Claim.ExpectedBaseOID, &r.Claim.ExpectedHeadOID, &r.State, &r.Launch.PID, &r.Launch.PGID, &r.Launch.BootIdentity, &r.Launch.ProcessStartIdentity); err != nil {
			return nil, err
		}
		r.Claim.TicketRef = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RecoverGitMutationLeases is deliberately proof-before-release.  Nothing in
// this method infers safety from elapsed time, a new leader, or a PID alone.
func (s *Store) RecoverGitMutationLeases(ctx context.Context, channel domain.Channel, leader uint64, drainer contracts.GitMutationDrainer) error {
	if !channel.Valid() || leader == 0 {
		return ErrGitMutationLease
	}
	leases, err := s.ActiveGitMutationLeases(ctx, channel)
	if err != nil {
		return err
	}
	for _, lease := range leases {
		if !validGitLaunch(lease.Launch) || drainer == nil || drainer.DrainGitMutation(ctx, lease.Launch) != nil {
			_ = s.write(ctx, func(conn *sql.Conn) error {
				_, e := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET state='quarantined',launch_state='quarantined' WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce)
				return e
			})
			return ErrGitMutationLease
		}
		if err := s.write(ctx, func(conn *sql.Conn) error {
			var current uint64
			if e := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); e != nil {
				return e
			}
			if current != leader {
				return ErrStaleFence
			}
			row, e := conn.ExecContext(ctx, `DELETE FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?`, lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce, lease.Launch.PID, lease.Launch.PGID, lease.Launch.BootIdentity, lease.Launch.ProcessStartIdentity)
			if e != nil {
				return e
			}
			if n, _ := row.RowsAffected(); n != 1 {
				return ErrGitMutationLease
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// AcquireRepositoryWriter reserves the same repository exclusion for a
// credential-free, non-Git command.  The intent must be backed by an exact
// executing effect; there is intentionally no argv or environment argument.
func (s *Store) AcquireRepositoryWriter(ctx context.Context, intent RepositoryWriterIntent) (contracts.GitMutationLease, error) {
	if !validRepositoryWriterIntent(intent) {
		return nil, ErrRepositoryWriter
	}
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	claim := contracts.GitMutationClaim{TicketRef: intent.Ref, SemanticKey: intent.SemanticKey, RequestDigest: intent.RequestDigest, TicketVersion: intent.TicketVersion, LeaderEpoch: intent.Fence.LeaderEpoch, RunnerEpoch: intent.Fence.RunnerEpoch, ClaimEpoch: 1, Repository: intent.Repository, Worktree: intent.Worktree, Operation: intent.Operation}
	err := s.write(ctx, func(conn *sql.Conn) error {
		if err := s.assertTicketFence(ctx, conn, intent.Ref, intent.TicketVersion, intent.Fence); err != nil {
			return err
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			return ErrRepositoryWriter
		}
		if effect.Ref != intent.Ref || effect.RequestDigest != intent.RequestDigest || effect.TicketVersion != intent.TicketVersion || effect.LeaderEpoch != intent.Fence.LeaderEpoch || effect.RunnerEpoch != intent.Fence.RunnerEpoch || effect.State != EffectExecuting {
			return ErrRepositoryWriter
		}
		claim.ClaimEpoch = effect.ClaimEpoch
		var n int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, intent.Repository).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return ErrGitMutationLease
		}
		if err := repositoryHasProviderWriter(ctx, conn, intent.Repository); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO repository_writer_leases(repository_path,semantic_key,nonce,channel,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,worktree_path,operation,acquired_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(repository_path) DO NOTHING`, intent.Repository, intent.SemanticKey, nonce, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket, intent.RequestDigest, intent.TicketVersion, intent.Fence.LeaderEpoch, intent.Fence.RunnerEpoch, effect.ClaimEpoch, intent.Worktree, intent.Operation, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryWriter
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &repositoryWriterLease{store: s, claim: claim, nonce: nonce}, nil
}
func (l *repositoryWriterLease) Check(ctx context.Context) error {
	var n int
	if l == nil || l.store == nil {
		return ErrRepositoryWriter
	}
	if err := l.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_writer_leases WHERE repository_path=? AND semantic_key=? AND nonce=?`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&n); err != nil || n != 1 {
		return ErrRepositoryWriter
	}
	return nil
}
func (l *repositoryWriterLease) Release() error {
	if l == nil || l.store == nil {
		return ErrRepositoryWriter
	}
	return l.store.write(context.Background(), func(conn *sql.Conn) error {
		r, e := conn.ExecContext(context.Background(), `DELETE FROM repository_writer_leases WHERE repository_path=? AND semantic_key=? AND nonce=?`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if e != nil {
			return e
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryWriter
		}
		return nil
	})
}

func validContractClaim(c contracts.GitMutationClaim) bool {
	return c.TicketRef.Validate() == nil && c.SemanticKey != "" && validClaimDigest(c.RequestDigest) && c.TicketVersion != 0 && c.LeaderEpoch != 0 && c.RunnerEpoch != 0 && c.ClaimEpoch != 0 && validStorePath(c.Repository) && validStorePath(c.Worktree) && c.Branch != "" && c.Operation != "" && c.BaseRef != "" && validStoreOID(c.ExpectedBaseOID) && (c.ExpectedHeadOID == "" || validStoreOID(c.ExpectedHeadOID))
}
func (s *Store) assertGitIntentCurrent(ctx context.Context, conn *sql.Conn, c contracts.GitMutationClaim) error {
	if err := s.assertTicketFence(ctx, conn, c.TicketRef, c.TicketVersion, domain.Fence{LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch}); err != nil {
		return err
	}
	var n int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_intents i JOIN effects e ON e.semantic_key=i.semantic_key WHERE i.semantic_key=? AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=? AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.claim_epoch=? AND i.repository_path=? AND i.worktree_path=? AND i.branch_ref=? AND i.operation=? AND i.base_ref=? AND i.expected_base_oid=? AND i.expected_head_oid=? AND e.state='executing' AND e.claim_epoch=i.claim_epoch`, c.SemanticKey, c.TicketRef.Channel, c.TicketRef.Project, c.TicketRef.Ticket, c.RequestDigest, c.TicketVersion, c.LeaderEpoch, c.RunnerEpoch, c.ClaimEpoch, c.Repository, c.Worktree, c.Branch, c.Operation, c.BaseRef, c.ExpectedBaseOID, c.ExpectedHeadOID).Scan(&n)
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrGitMutationIntent
	}
	return nil
}
func repositoryHasProviderWriter(ctx context.Context, conn *sql.Conn, repository string) error {
	var n int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE repository_path=? AND state IN ('active','quarantined')`, repository).Scan(&n)
	if err != nil {
		return err
	}
	if n != 0 {
		return ErrProviderAttempt
	}
	return nil
}

var _ contracts.GitMutationAuthority = (*Store)(nil)
