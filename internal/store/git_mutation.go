package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
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
	RequestDigest string
	Repository    string
	Worktree      string
	Branch        string
	Operation     string
	BaseRef       string
	// ExpectedBaseOID and ExpectedHeadOID are exact observations proposed by
	// the trusted Git coordinator. SQLite makes them immutable and fenced; the
	// Git runner independently re-reads and compares them immediately before it
	// acquires this claim. Store does not claim to observe repository objects.
	ExpectedBaseOID string
	ExpectedHeadOID string
}

type gitMutationLease struct {
	store *Store
	claim contracts.GitMutationClaim
	nonce []byte
}

// GitMutationRecovery is the exact persisted repository child that must be
// drained before startup can admit any writer for its repository.
type GitMutationRecovery struct {
	Claim               contracts.GitMutationClaim
	Nonce               []byte
	State               string
	LaunchState         string
	Launch              contracts.GitMutationLaunch
	PreparedCommitOID   string
	PreparedTreeOID     string
	PriorRemoteObserved int // 0 means unrecorded/legacy; 1 means recorded.
	PriorRemoteOID      string
}

// GitMutationIntentFacts is the immutable recovery authority that remains
// after a normally drained lease is deleted.  It deliberately contains no
// process identity: callers use it only to reconcile the already-fenced
// effect, never to start a new mutation.
type GitMutationIntentFacts struct {
	Claim               contracts.GitMutationClaim
	Effect              Effect
	ObservedIdentity    string
	PreparedCommitOID   string
	PreparedTreeOID     string
	PriorRemoteObserved bool
	PriorRemoteOID      string
}

// CanonicalGitMutationSemanticKey binds precisely the stable Git effect input.
// Epochs and claim counters are intentionally excluded: they are fences, not
// effect identity. Length-prefixing makes the hashed representation unambiguous
// even if a future validated field grammar is relaxed.
func CanonicalGitMutationSemanticKey(i GitMutationIntent) string {
	h := sha256.New()
	for _, value := range []string{
		"sf.git-mutation.semantic-key.v1", string(i.Ref.Channel), string(i.Ref.Project), string(i.Ref.Ticket),
		i.RequestDigest, i.Repository, i.Worktree, i.Branch, i.Operation, i.BaseRef, i.ExpectedBaseOID, i.ExpectedHeadOID,
	} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	return "git/v1/" + fmt.Sprintf("%x", h.Sum(nil))
}

func validGitIntent(i GitMutationIntent) bool {
	return i.Ref.Validate() == nil && i.SemanticKey == CanonicalGitMutationSemanticKey(i) && validClaimDigest(i.RequestDigest) && i.TicketVersion != 0 &&
		i.Fence.LeaderEpoch != 0 && i.Fence.RunnerEpoch != 0 && validStorePath(i.Repository) && validStorePath(i.Worktree) &&
		i.Branch != "" && validGitOperation(i.Operation) && i.BaseRef != "" && validStoreOID(i.ExpectedBaseOID) &&
		validGitOIDWidth(i.ExpectedBaseOID, i.ExpectedHeadOID)
}

func validGitOperation(operation string) bool {
	switch operation {
	case "create-worktree", "remove-worktree", "commit", "push", "protected-ref-fetch":
		return true
	default:
		return false
	}
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

// validGitOIDWidth binds every object in one Git mutation to the repository
// object format established by ExpectedBaseOID. A SHA-1/SHA-256 mixture is
// never a recoverable fact, even though each individual string is an OID.
func validGitOIDWidth(base string, oids ...string) bool {
	if !validStoreOID(base) {
		return false
	}
	for _, oid := range oids {
		if oid != "" && (!validStoreOID(oid) || len(oid) != len(base)) {
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

// TicketWorktreePath returns the one pre-registration path that this channel
// Store can authorize for a ticket. The database lives directly beneath the
// channel root, so deriving the path here keeps stable/dev isolation and path
// authority independent of a caller-supplied intent. Git still authenticates
// the directory and repository identity before and after creation.
func (s *Store) TicketWorktreePath(ref domain.TicketRef) (string, error) {
	if s == nil || ref.Validate() != nil || !validStorePath(s.worktreeRoot) || filepath.Dir(s.worktreeRoot) == string(filepath.Separator) {
		return "", ErrGitMutationIntent
	}
	path := filepath.Join(s.worktreeRoot, string(ref.Project), string(ref.Ticket))
	relative, err := filepath.Rel(s.worktreeRoot, path)
	if err != nil || relative == "." || !filepath.IsLocal(relative) || !validStorePath(path) {
		return "", ErrGitMutationIntent
	}
	return path, nil
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
		var repository, worktree, branch, baseRef string
		var err error
		if intent.Operation == "create-worktree" {
			// Worktree creation is the one Git mutation that necessarily happens
			// before a worktree identity can be registered. Bind it to the durable
			// project and ticket branch allocation instead of requiring the row
			// that this very operation is responsible for creating.
			worktree, err = s.TicketWorktreePath(intent.Ref)
			if err != nil {
				return ErrGitMutationIntent
			}
			err = conn.QueryRowContext(ctx, `SELECT p.canonical_path,b.branch_ref,p.base_ref
				FROM projects p JOIN branch_allocations b ON b.channel=p.channel AND b.project_id=p.id
				WHERE p.channel=? AND p.id=? AND b.ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).
				Scan(&repository, &branch, &baseRef)
		} else {
			err = conn.QueryRowContext(ctx, `SELECT p.canonical_path,w.path,w.branch_ref,p.base_ref
				FROM projects p JOIN worktrees w ON w.channel=p.channel AND w.project_id=p.id
				WHERE p.channel=? AND p.id=? AND w.ticket_id=?`, intent.Ref.Channel, intent.Ref.Project, intent.Ref.Ticket).
				Scan(&repository, &worktree, &branch, &baseRef)
		}
		if errors.Is(err, sql.ErrNoRows) {
			return ErrGitMutationIntent
		}
		if err != nil {
			return err
		}
		// Repository exclusion is keyed by the Store's canonical project path,
		// never by a caller-chosen alias. The exact durable worktree, branch and
		// protected base are likewise prerequisites for minting a Git claim.
		if repository != intent.Repository || worktree != intent.Worktree || branch != intent.Branch || baseRef != intent.BaseRef {
			return ErrGitMutationIntent
		}
		effect, err := effectFrom(ctx, conn, intent.SemanticKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrNotFound) {
				return ErrGitMutationIntent
			}
			return err
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
		if err := repositoryHasCommandWriter(ctx, conn, claim.Repository); err != nil {
			return err
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

// RecordPreparedCommit durably records the immutable commit-tree result before
// update-ref can make it reachable.  The same exact replay is harmless, while
// any changed or partially tampered value is refused.
func (l *gitMutationLease) RecordPreparedCommit(ctx context.Context, commit, tree string) error {
	if l == nil || l.store == nil || l.claim.Operation != "commit" || !validGitOIDWidth(l.claim.ExpectedBaseOID, l.claim.ExpectedHeadOID, commit, tree) {
		return ErrGitMutationLease
	}
	return l.store.write(ctx, func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(ctx, conn, l.claim); err != nil {
			return ErrGitMutationLease
		}
		var intentCommit, intentTree, leaseCommit, leaseTree string
		if err := conn.QueryRowContext(ctx, `SELECT prepared_commit_oid,prepared_tree_oid FROM git_mutation_intents WHERE semantic_key=? AND operation='commit'`, l.claim.SemanticKey).Scan(&intentCommit, &intentTree); err != nil {
			return ErrGitMutationLease
		}
		if err := conn.QueryRowContext(ctx, `SELECT prepared_commit_oid,prepared_tree_oid FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='commit'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&leaseCommit, &leaseTree); err != nil {
			return ErrGitMutationLease
		}
		if intentCommit == commit && intentTree == tree && leaseCommit == commit && leaseTree == tree {
			return nil
		}
		// A fresh lease may be acquired after a lost response. The immutable
		// intent remains the authority, so an exact replay copies that fact to
		// the new nonce lease; any other divergence is tamper/conflict.
		if intentCommit == commit && intentTree == tree && leaseCommit == "" && leaseTree == "" {
			result, err := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET prepared_commit_oid=?,prepared_tree_oid=? WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='commit' AND prepared_commit_oid='' AND prepared_tree_oid=''`, commit, tree, l.claim.Repository, l.claim.SemanticKey, l.nonce)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n != 1 {
				return ErrGitMutationLease
			}
			return nil
		}
		if intentCommit != "" || intentTree != "" || leaseCommit != "" || leaseTree != "" {
			return ErrGitMutationLease
		}
		result, err := conn.ExecContext(ctx, `UPDATE git_mutation_intents SET prepared_commit_oid=?,prepared_tree_oid=? WHERE semantic_key=? AND operation='commit' AND prepared_commit_oid='' AND prepared_tree_oid=''`, commit, tree, l.claim.SemanticKey)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		result, err = conn.ExecContext(ctx, `UPDATE git_mutation_leases SET prepared_commit_oid=?,prepared_tree_oid=? WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='commit' AND prepared_commit_oid='' AND prepared_tree_oid=''`, commit, tree, l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
}

// RecordPushPriorRemote records the candidate ref observation immediately
// before the ordinary push. An empty OID is a recorded observation that the
// branch was absent; a non-empty OID must be canonical. The separate flag
// keeps that fact distinct from an old, unrecorded default row.
func (l *gitMutationLease) RecordPushPriorRemote(ctx context.Context, oid string) error {
	if l == nil || l.store == nil || l.claim.Operation != "push" || !validGitOIDWidth(l.claim.ExpectedBaseOID, l.claim.ExpectedHeadOID, oid) {
		return ErrGitMutationLease
	}
	return l.store.write(ctx, func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(ctx, conn, l.claim); err != nil {
			return ErrGitMutationLease
		}
		var intentObserved, leaseObserved int
		var intentOID, leaseOID string
		if err := conn.QueryRowContext(ctx, `SELECT prior_remote_observed,prior_remote_oid FROM git_mutation_intents WHERE semantic_key=? AND operation='push'`, l.claim.SemanticKey).Scan(&intentObserved, &intentOID); err != nil {
			return ErrGitMutationLease
		}
		if err := conn.QueryRowContext(ctx, `SELECT prior_remote_observed,prior_remote_oid FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='push'`, l.claim.Repository, l.claim.SemanticKey, l.nonce).Scan(&leaseObserved, &leaseOID); err != nil {
			return ErrGitMutationLease
		}
		if intentObserved == 1 && intentOID == oid && leaseObserved == 1 && leaseOID == oid {
			return nil
		}
		if intentObserved == 1 && intentOID == oid && leaseObserved == 0 && leaseOID == "" {
			result, err := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET prior_remote_observed=1,prior_remote_oid=? WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='push' AND prior_remote_observed=0 AND prior_remote_oid=''`, oid, l.claim.Repository, l.claim.SemanticKey, l.nonce)
			if err != nil {
				return err
			}
			if n, _ := result.RowsAffected(); n != 1 {
				return ErrGitMutationLease
			}
			return nil
		}
		if intentObserved != 0 || intentOID != "" || leaseObserved != 0 || leaseOID != "" {
			return ErrGitMutationLease
		}
		result, err := conn.ExecContext(ctx, `UPDATE git_mutation_intents SET prior_remote_observed=1,prior_remote_oid=? WHERE semantic_key=? AND operation='push' AND prior_remote_observed=0 AND prior_remote_oid=''`, oid, l.claim.SemanticKey)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		result, err = conn.ExecContext(ctx, `UPDATE git_mutation_leases SET prior_remote_observed=1,prior_remote_oid=? WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND operation='push' AND prior_remote_observed=0 AND prior_remote_oid=''`, oid, l.claim.Repository, l.claim.SemanticKey, l.nonce)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrGitMutationLease
		}
		return nil
	})
}

func (l *gitMutationLease) Release() error {
	if l == nil || l.store == nil {
		return ErrGitMutationLease
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return l.store.write(ctx, func(conn *sql.Conn) error {
		if err := l.store.assertGitIntentCurrent(ctx, conn, l.claim); err != nil {
			return ErrGitMutationLease
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM git_mutation_leases WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active' AND launch_state IN ('unrecorded','drained')`, l.claim.Repository, l.claim.SemanticKey, l.nonce)
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
	rows, err := s.db.QueryContext(ctx, `SELECT semantic_key,nonce,project_id,ticket_id,request_digest,ticket_version,leader_epoch,runner_epoch,claim_epoch,repository_path,worktree_path,branch_ref,operation,base_ref,expected_base_oid,expected_head_oid,state,launch_state,process_pid,process_pgid,process_boot_identity,process_start_identity,prepared_commit_oid,prepared_tree_oid,prior_remote_observed,prior_remote_oid FROM git_mutation_leases WHERE channel=? ORDER BY repository_path`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	var candidates []GitMutationRecovery
	for rows.Next() {
		var r GitMutationRecovery
		var project, ticket string
		if err := rows.Scan(&r.Claim.SemanticKey, &r.Nonce, &project, &ticket, &r.Claim.RequestDigest, &r.Claim.TicketVersion, &r.Claim.LeaderEpoch, &r.Claim.RunnerEpoch, &r.Claim.ClaimEpoch, &r.Claim.Repository, &r.Claim.Worktree, &r.Claim.Branch, &r.Claim.Operation, &r.Claim.BaseRef, &r.Claim.ExpectedBaseOID, &r.Claim.ExpectedHeadOID, &r.State, &r.LaunchState, &r.Launch.PID, &r.Launch.PGID, &r.Launch.BootIdentity, &r.Launch.ProcessStartIdentity, &r.PreparedCommitOID, &r.PreparedTreeOID, &r.PriorRemoteObserved, &r.PriorRemoteOID); err != nil {
			return nil, err
		}
		r.Claim.TicketRef = domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(ticket)}
		candidates = append(candidates, r)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, r := range candidates {
		facts, err := s.GitMutationIntentFacts(ctx, r.Claim.SemanticKey)
		prior := 0
		if facts.PriorRemoteObserved {
			prior = 1
		}
		if err != nil || facts.Claim != r.Claim || !validContractClaim(r.Claim) || !validGitMutationFacts(r.Claim.Operation, r.Claim.ExpectedBaseOID, r.Claim.ExpectedHeadOID, r.PreparedCommitOID, r.PreparedTreeOID, r.PriorRemoteObserved, r.PriorRemoteOID) || facts.PreparedCommitOID != r.PreparedCommitOID || facts.PreparedTreeOID != r.PreparedTreeOID || prior != r.PriorRemoteObserved || facts.PriorRemoteOID != r.PriorRemoteOID {
			// A recovery reader must never silently skip an inconsistent lease:
			// preserve the concrete repository exclusion for operator recovery.
			_ = s.quarantineGitMutationLease(ctx, r)
			return nil, ErrGitMutationLease
		}
	}
	return candidates, nil
}

func (s *Store) quarantineGitMutationLease(ctx context.Context, lease GitMutationRecovery) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET state='quarantined',launch_state='quarantined' WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'`, lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce)
		return err
	})
}

// GitMutationIntentFacts loads the exact immutable recovery facts by semantic
// key. It re-validates both copies of the effect binding and rejects malformed
// or operation-inapplicable facts rather than treating a tampered row as
// authority after a lease has been released.
func (s *Store) GitMutationIntentFacts(ctx context.Context, semanticKey string) (GitMutationIntentFacts, error) {
	if s == nil || semanticKey == "" {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	return gitMutationIntentFactsFrom(ctx, s.db, semanticKey)
}

// gitMutationIntentFactsFrom is also used by the final reconciliation write
// transaction. A read performed through Store.db immediately before that
// transaction would leave a race in which the intent or effect changed before
// confirmation.
func gitMutationIntentFactsFrom(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, semanticKey string) (GitMutationIntentFacts, error) {
	if semanticKey == "" {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	var out GitMutationIntentFacts
	var project, ticket string
	var prior int
	err := query.QueryRowContext(ctx, `SELECT i.channel,i.project_id,i.ticket_id,i.request_digest,i.ticket_version,i.leader_epoch,i.runner_epoch,i.claim_epoch,i.repository_path,i.worktree_path,i.branch_ref,i.operation,i.base_ref,i.expected_base_oid,i.expected_head_oid,i.prepared_commit_oid,i.prepared_tree_oid,i.prior_remote_observed,i.prior_remote_oid,e.channel,e.project_id,e.ticket_id,e.effect_kind,e.state,e.request_digest,e.ticket_version,e.leader_epoch,e.runner_epoch,e.claim_epoch,e.observed_identity
		FROM git_mutation_intents i JOIN effects e ON e.semantic_key=i.semantic_key WHERE i.semantic_key=?`, semanticKey).
		Scan(&out.Claim.TicketRef.Channel, &project, &ticket, &out.Claim.RequestDigest, &out.Claim.TicketVersion, &out.Claim.LeaderEpoch, &out.Claim.RunnerEpoch, &out.Claim.ClaimEpoch, &out.Claim.Repository, &out.Claim.Worktree, &out.Claim.Branch, &out.Claim.Operation, &out.Claim.BaseRef, &out.Claim.ExpectedBaseOID, &out.Claim.ExpectedHeadOID, &out.PreparedCommitOID, &out.PreparedTreeOID, &prior, &out.PriorRemoteOID, &out.Effect.Ref.Channel, &out.Effect.Ref.Project, &out.Effect.Ref.Ticket, &out.Effect.Kind, &out.Effect.State, &out.Effect.RequestDigest, &out.Effect.TicketVersion, &out.Effect.LeaderEpoch, &out.Effect.RunnerEpoch, &out.Effect.ClaimEpoch, &out.ObservedIdentity)
	if errors.Is(err, sql.ErrNoRows) {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	if err != nil {
		return GitMutationIntentFacts{}, err
	}
	out.Claim.TicketRef.Project, out.Claim.TicketRef.Ticket, out.Claim.SemanticKey = domain.ProjectID(project), domain.TicketID(ticket), semanticKey
	out.Effect.SemanticKey, out.Effect.ObservedIdentity = semanticKey, out.ObservedIdentity
	intent := GitMutationIntent{EffectFence: EffectFence{SemanticKey: semanticKey, Ref: out.Claim.TicketRef, TicketVersion: out.Claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: out.Claim.LeaderEpoch, RunnerEpoch: out.Claim.RunnerEpoch}}, RequestDigest: out.Claim.RequestDigest, Repository: out.Claim.Repository, Worktree: out.Claim.Worktree, Branch: out.Claim.Branch, Operation: out.Claim.Operation, BaseRef: out.Claim.BaseRef, ExpectedBaseOID: out.Claim.ExpectedBaseOID, ExpectedHeadOID: out.Claim.ExpectedHeadOID}
	if !validGitIntent(intent) || !validContractClaim(out.Claim) || out.Effect.Ref != out.Claim.TicketRef || out.Effect.Kind != "git/"+out.Claim.Operation || out.Effect.RequestDigest != out.Claim.RequestDigest || (out.Effect.State != EffectExecuting && out.Effect.State != EffectUncertain && out.Effect.State != EffectConfirmed) || !linkedGitRecoveryEffect(out.Claim, out.Effect) {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	if !validGitMutationFacts(out.Claim.Operation, out.Claim.ExpectedBaseOID, out.Claim.ExpectedHeadOID, out.PreparedCommitOID, out.PreparedTreeOID, prior, out.PriorRemoteOID) {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	if out.Claim.Operation == "commit" && out.Effect.State == EffectConfirmed && out.Effect.ObservedIdentity != out.PreparedCommitOID {
		// A confirmed commit is only linked to this immutable intent when the
		// exact prepared object was recorded as its observed identity. This
		// keeps the widened post-fence linkage from accepting a tampered result.
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	out.PriorRemoteObserved = prior == 1
	return out, nil
}

func linkedGitRecoveryEffect(claim contracts.GitMutationClaim, effect Effect) bool {
	if effect.State == EffectExecuting {
		return effect.TicketVersion == claim.TicketVersion && effect.LeaderEpoch == claim.LeaderEpoch && effect.RunnerEpoch == claim.RunnerEpoch && effect.ClaimEpoch == claim.ClaimEpoch
	}
	if effect.LeaderEpoch < claim.LeaderEpoch || effect.ClaimEpoch < claim.ClaimEpoch {
		return false
	}
	if effect.TicketVersion == claim.TicketVersion && effect.RunnerEpoch == claim.RunnerEpoch {
		if effect.State == EffectUncertain {
			return effect.LeaderEpoch >= claim.LeaderEpoch && effect.ClaimEpoch >= claim.ClaimEpoch
		}
		// A recovered commit/worktree may be confirmed under the new leader
		// before FenceRecoveredRunners advances the ticket. A later startup
		// must still recognize that exact immutable claim as settled; requiring
		// the original leader/claim pair here would turn a legitimate confirmed
		// result into a false quarantine. The intent binding, request digest, and
		// confirmed identity are validated by the caller before this linkage is
		// used, so this is not an avenue for accepting a different effect.
		return effect.LeaderEpoch >= claim.LeaderEpoch && effect.ClaimEpoch >= claim.ClaimEpoch
	}
	return effect.LeaderEpoch > claim.LeaderEpoch && effect.ClaimEpoch > claim.ClaimEpoch && equalRecoveryAdvance(claim.TicketVersion, claim.RunnerEpoch, effect.TicketVersion, effect.RunnerEpoch)
}

// ConfirmRecoveredWorktreeCreation settles an old create claim only after the
// daemon has installed an equal version/runner recovery fence in planning.
// The immutable Git intent remains the original launch witness.
func (s *Store) ConfirmRecoveredWorktreeCreation(ctx context.Context, claim contracts.GitMutationClaim, identity string) (Effect, error) {
	if !validContractClaim(claim) || claim.Operation != "create-worktree" || identity == "" {
		return Effect{}, ErrGitMutationIntent
	}
	facts, err := s.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if err != nil || facts.Claim != claim {
		return Effect{}, ErrGitMutationIntent
	}
	var result Effect
	err = s.write(ctx, func(conn *sql.Conn) error {
		current, err := effectFrom(ctx, conn, claim.SemanticKey)
		if err != nil {
			return err
		}
		if current.Ref != claim.TicketRef || current.Kind != "git/create-worktree" || current.RequestDigest != claim.RequestDigest || !linkedGitRecoveryEffect(claim, current) {
			return ErrStaleFence
		}
		if current.State == EffectConfirmed && current.ObservedIdentity == identity {
			result = current
			return nil
		}
		if current.State != EffectUncertain {
			return ErrStaleFence
		}
		var state domain.State
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, claim.TicketRef.Channel, claim.TicketRef.Project, claim.TicketRef.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
			return err
		}
		if state != domain.StatePlanning || leader != current.LeaderEpoch || !equalRecoveryAdvance(current.TicketVersion, current.RunnerEpoch, version, runner) {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, claim.TicketRef.Channel, version, runner, domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner, ClaimEpoch: current.ClaimEpoch}); err != nil {
			return err
		}
		changed, err := conn.ExecContext(ctx, `UPDATE effects SET state='confirmed',observed_identity=?,ticket_version=?,runner_epoch=? WHERE semantic_key=? AND state='uncertain' AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=? AND request_digest=?`, identity, version, runner, current.SemanticKey, current.TicketVersion, current.LeaderEpoch, current.RunnerEpoch, current.ClaimEpoch, current.RequestDigest)
		if err != nil {
			return err
		}
		if n, _ := changed.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		result, err = effectFrom(ctx, conn, claim.SemanticKey)
		return err
	})
	return result, err
}

// WorktreeCreationIntent returns the sole unresolved or confirmed creation
// binding for a ticket. It is recovery-only authority: a caller can inspect
// the exact path it already owns, but it cannot mint a new Git mutation from
// these facts. More than one live fact is durable ambiguity and is refused.
func (s *Store) WorktreeCreationIntent(ctx context.Context, ref domain.TicketRef) (GitMutationIntentFacts, error) {
	if err := ref.Validate(); err != nil {
		return GitMutationIntentFacts{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT i.semantic_key
		FROM git_mutation_intents i JOIN effects e ON e.semantic_key=i.semantic_key
		WHERE i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.operation='create-worktree'
		AND e.state IN ('executing','uncertain','confirmed') ORDER BY i.semantic_key`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return GitMutationIntentFacts{}, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return GitMutationIntentFacts{}, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return GitMutationIntentFacts{}, err
	}
	if len(keys) == 0 {
		return GitMutationIntentFacts{}, ErrNotFound
	}
	if len(keys) != 1 {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	facts, err := s.GitMutationIntentFacts(ctx, keys[0])
	if err != nil || facts.Claim.TicketRef != ref || facts.Claim.Operation != "create-worktree" {
		if err != nil {
			return GitMutationIntentFacts{}, err
		}
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	return facts, nil
}

// PublicationPushIntent returns the sole durable candidate-push authority for
// a ticket. It is recovery-only: callers can reconcile the immutable remote
// target but cannot mint, rebind, or claim an effect from these facts.
func (s *Store) PublicationPushIntent(ctx context.Context, ref domain.TicketRef) (GitMutationIntentFacts, error) {
	if err := ref.Validate(); err != nil {
		return GitMutationIntentFacts{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT i.semantic_key FROM git_mutation_intents i
		JOIN effects e ON e.semantic_key=i.semantic_key
		WHERE i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.operation='push'
		AND e.state IN ('executing','uncertain','confirmed') ORDER BY i.semantic_key`, ref.Channel, ref.Project, ref.Ticket)
	if err != nil {
		return GitMutationIntentFacts{}, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return GitMutationIntentFacts{}, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return GitMutationIntentFacts{}, err
	}
	if len(keys) == 0 {
		return GitMutationIntentFacts{}, ErrNotFound
	}
	if len(keys) != 1 {
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	facts, err := s.GitMutationIntentFacts(ctx, keys[0])
	if err != nil || facts.Claim.TicketRef != ref || facts.Claim.Operation != "push" || facts.Effect.Kind != "git/push" || (facts.Effect.State != EffectExecuting && facts.Effect.State != EffectUncertain && facts.Effect.State != EffectConfirmed) {
		if err != nil {
			return GitMutationIntentFacts{}, err
		}
		return GitMutationIntentFacts{}, ErrGitMutationIntent
	}
	return facts, nil
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
		quarantined := false
		if err := s.write(ctx, func(conn *sql.Conn) error {
			var current uint64
			if e := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); e != nil {
				return e
			}
			if current != leader {
				return ErrStaleFence
			}
			// Draining necessarily happens outside SQLite. Re-check every durable
			// recovery input under this final writer transaction, so a fact or
			// intent change in that window cannot be silently deleted.
			row, e := conn.ExecContext(ctx, `DELETE FROM git_mutation_leases
				WHERE repository_path=? AND semantic_key=? AND nonce=? AND state='active'
				AND channel=? AND project_id=? AND ticket_id=? AND request_digest=?
				AND ticket_version=? AND leader_epoch=? AND runner_epoch=? AND claim_epoch=?
				AND worktree_path=? AND branch_ref=? AND operation=? AND base_ref=? AND expected_base_oid=? AND expected_head_oid=?
				AND launch_state=?
				AND process_pid=? AND process_pgid=? AND process_boot_identity=? AND process_start_identity=?
				AND prepared_commit_oid=? AND prepared_tree_oid=? AND prior_remote_observed=? AND prior_remote_oid=?
				AND EXISTS (SELECT 1 FROM git_mutation_intents i JOIN effects e ON e.semantic_key=i.semantic_key
					WHERE i.semantic_key=git_mutation_leases.semantic_key
					AND i.channel=? AND i.project_id=? AND i.ticket_id=? AND i.request_digest=?
					AND i.ticket_version=? AND i.leader_epoch=? AND i.runner_epoch=? AND i.claim_epoch=?
					AND i.repository_path=? AND i.worktree_path=? AND i.branch_ref=? AND i.operation=? AND i.base_ref=?
					AND i.expected_base_oid=? AND i.expected_head_oid=?
					AND i.prepared_commit_oid=? AND i.prepared_tree_oid=? AND i.prior_remote_observed=? AND i.prior_remote_oid=?
					AND e.channel=? AND e.project_id=? AND e.ticket_id=? AND e.effect_kind=?
					AND e.state IN ('executing','uncertain') AND e.request_digest=? AND e.ticket_version=?
					AND e.leader_epoch=? AND e.runner_epoch=? AND e.claim_epoch=?)`,
				lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce,
				lease.Claim.TicketRef.Channel, lease.Claim.TicketRef.Project, lease.Claim.TicketRef.Ticket, lease.Claim.RequestDigest,
				lease.Claim.TicketVersion, lease.Claim.LeaderEpoch, lease.Claim.RunnerEpoch, lease.Claim.ClaimEpoch,
				lease.Claim.Worktree, lease.Claim.Branch, lease.Claim.Operation, lease.Claim.BaseRef, lease.Claim.ExpectedBaseOID, lease.Claim.ExpectedHeadOID,
				lease.LaunchState, lease.Launch.PID, lease.Launch.PGID, lease.Launch.BootIdentity, lease.Launch.ProcessStartIdentity,
				lease.PreparedCommitOID, lease.PreparedTreeOID, lease.PriorRemoteObserved, lease.PriorRemoteOID,
				lease.Claim.TicketRef.Channel, lease.Claim.TicketRef.Project, lease.Claim.TicketRef.Ticket, lease.Claim.RequestDigest,
				lease.Claim.TicketVersion, lease.Claim.LeaderEpoch, lease.Claim.RunnerEpoch, lease.Claim.ClaimEpoch,
				lease.Claim.Repository, lease.Claim.Worktree, lease.Claim.Branch, lease.Claim.Operation, lease.Claim.BaseRef,
				lease.Claim.ExpectedBaseOID, lease.Claim.ExpectedHeadOID,
				lease.PreparedCommitOID, lease.PreparedTreeOID, lease.PriorRemoteObserved, lease.PriorRemoteOID,
				lease.Claim.TicketRef.Channel, lease.Claim.TicketRef.Project, lease.Claim.TicketRef.Ticket, "git/"+lease.Claim.Operation,
				lease.Claim.RequestDigest, lease.Claim.TicketVersion, lease.Claim.LeaderEpoch, lease.Claim.RunnerEpoch, lease.Claim.ClaimEpoch)
			if e != nil {
				return e
			}
			if n, _ := row.RowsAffected(); n != 1 {
				// Preserve the exact post-drain row for operator reconciliation;
				// never discard a lease whose validation snapshot no longer holds.
				if _, e := conn.ExecContext(ctx, `UPDATE git_mutation_leases SET state='quarantined',launch_state='quarantined' WHERE repository_path=? AND semantic_key=? AND nonce=?`, lease.Claim.Repository, lease.Claim.SemanticKey, lease.Nonce); e != nil {
					return e
				}
				// Commit quarantine before surfacing failure; returning an error from
				// this callback would roll its protective state back with the final
				// IMMEDIATE transaction.
				quarantined = true
				return nil
			}
			return nil
		}); err != nil {
			return err
		}
		if quarantined {
			return ErrGitMutationLease
		}
	}
	return nil
}

func validContractClaim(c contracts.GitMutationClaim) bool {
	if c.ClaimEpoch == 0 {
		return false
	}
	intent := GitMutationIntent{EffectFence: EffectFence{SemanticKey: c.SemanticKey, Ref: c.TicketRef, TicketVersion: c.TicketVersion, Fence: domain.Fence{LeaderEpoch: c.LeaderEpoch, RunnerEpoch: c.RunnerEpoch}}, RequestDigest: c.RequestDigest, Repository: c.Repository, Worktree: c.Worktree, Branch: c.Branch, Operation: c.Operation, BaseRef: c.BaseRef, ExpectedBaseOID: c.ExpectedBaseOID, ExpectedHeadOID: c.ExpectedHeadOID}
	return validGitIntent(intent)
}

func validGitMutationFacts(operation, base, expectedHead, preparedCommit, preparedTree string, priorObserved int, priorOID string) bool {
	if !validGitOIDWidth(base, expectedHead) {
		return false
	}
	switch operation {
	case "commit":
		// OIDs are individually optional to support other fact shapes, but a
		// prepared commit is an inseparable commit/tree tuple. Never let the
		// optional-width helper turn a partial tuple into a valid fact.
		return priorObserved == 0 && priorOID == "" && ((preparedCommit == "" && preparedTree == "") || (preparedCommit != "" && preparedTree != "" && validGitOIDWidth(base, preparedCommit, preparedTree)))
	case "push":
		return preparedCommit == "" && preparedTree == "" && (priorObserved == 0 && priorOID == "" || priorObserved == 1 && validGitOIDWidth(base, priorOID))
	default:
		return preparedCommit == "" && preparedTree == "" && priorObserved == 0 && priorOID == ""
	}
}
func (s *Store) assertGitIntentCurrent(ctx context.Context, conn *sql.Conn, c contracts.GitMutationClaim) error {
	if !validContractClaim(c) {
		return ErrGitMutationIntent
	}
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

func repositoryHasCommandWriter(ctx context.Context, conn *sql.Conn, repository string) error {
	var n int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM repository_command_leases WHERE repository_path=? AND state IN ('active','quarantined')`, repository).Scan(&n); err != nil {
		return err
	}
	if n != 0 {
		return ErrRepositoryCommandLease
	}
	return nil
}

var _ contracts.GitMutationAuthority = (*Store)(nil)
