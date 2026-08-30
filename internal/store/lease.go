package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

var ErrLeaseCapacity = errors.New("lease capacity is exhausted")

// ErrLeaseAdoption keeps capacity held when the ownership of an invalidated
// lease cannot be proven safe to transfer to a replacement runner.
var ErrLeaseAdoption = errors.New("invalidated leases cannot be adopted")

// ErrStartState identifies a queued ticket that cannot be admitted to a
// workflow without an operator-visible state decision.
var ErrStartState = errors.New("ticket cannot be started in its current state")

// FenceRecoveredRunners advances every actively owned ticket runner under the
// new durable leader. It never releases leases: only a later supervisor proof
// may free capacity that could still belong to a live old process.
func (s *Store) FenceRecoveredRunners(ctx context.Context, channel domain.Channel, leaderEpoch uint64) (int64, error) {
	if !channel.Valid() || leaderEpoch == 0 {
		return 0, errors.New("valid channel and leader epoch are required")
	}
	var changed int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var current uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); err != nil {
			return err
		}
		if current != leaderEpoch {
			return ErrStaleFence
		}
		result, err := conn.ExecContext(ctx, `UPDATE tickets SET runner_epoch=runner_epoch+1, version=version+1
			WHERE channel=? AND state IN ('planning','verifying','building','publishing','waiting_ci','reviewing','waiting_approval','waiting_manual_merge','merging','reconciling','stopping','cancelling')`, channel)
		if err != nil {
			return err
		}
		changed, _ = result.RowsAffected()
		return nil
	})
	return changed, err
}

// LeaseRequest describes one capacity dimension. Resource names are durable
// semantic identities such as "machine", a canonical project ID, or a
// qualified provider/version. Capacity is resolved from the frozen ticket
// configuration before this boundary is called.
type LeaseRequest struct {
	Scope    string
	Resource string
	Capacity int
}

type Lease struct {
	Ref         domain.TicketRef
	Scope       string
	ScopeKey    string
	RunnerEpoch uint64
	AcquiredAt  time.Time
}

// StartWithOwnership admits a queued ticket, reserves every capacity
// dimension, and establishes workflow ownership in one SQLite transaction.
// A replay of the same start observes the existing planning owner and leases
// without emitting another transition event.
func (s *Store) StartWithOwnership(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, workflowID string, requests []LeaseRequest, at time.Time) (Ticket, bool, error) {
	if err := ref.Validate(); err != nil {
		return Ticket{}, false, err
	}
	if workflowID == "" || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return Ticket{}, false, errors.New("workflow identity and fence are required")
	}
	requests, err := validateLeaseRequests(requests)
	if err != nil {
		return Ticket{}, false, err
	}
	if at.IsZero() {
		return Ticket{}, false, errors.New("start time is required")
	}
	observed := false
	err = s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var version, runner uint64
		var persistedWorkflow string
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch, workflow_id FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &persistedWorkflow); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		if state == domain.StatePlanning {
			if version != expectedVersion || persistedWorkflow != workflowID {
				return ErrStaleFence
			}
			observed = true
		} else {
			if state != domain.StateQueued || version != expectedVersion {
				return fmt.Errorf("%w: state=%s", ErrStartState, state)
			}
			for _, request := range requests {
				lease, ok, err := acquireLease(ctx, conn, ref, runner, request, at.UTC())
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("%w: scope=%s resource=%s capacity=%d", ErrLeaseCapacity, request.Scope, request.Resource, request.Capacity)
				}
				_ = lease
			}
			updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='planning', version=version+1, workflow_id=?,
				config_generation=(SELECT current_config_generation FROM projects WHERE channel=? AND id=?),
				config_digest=COALESCE((SELECT c.digest FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), ''),
				config_snapshot_bytes=COALESCE((SELECT c.snapshot_bytes FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), X'')
				WHERE channel=? AND project_id=? AND id=? AND state='queued' AND version=? AND runner_epoch=?`, workflowID,
				ref.Channel, ref.Project, ref.Channel, ref.Project, ref.Channel, ref.Project,
				ref.Channel, ref.Project, ref.Ticket, expectedVersion, runner)
			if err != nil {
				return err
			}
			if changed, _ := updated.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
			version++
			if _, err := conn.ExecContext(ctx, `INSERT INTO workflow_owners(channel, project_id, ticket_id, workflow_id, state, created_at) VALUES (?, ?, ?, ?, 'owned', ?)`, ref.Channel, ref.Project, ref.Ticket, workflowID, at.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, 'operator_start', 'queued', 'planning', '{}', ?)`, ref.Channel, ref.Project, ref.Ticket, version, at.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Ticket{}, false, err
	}
	result, err := s.Ticket(ctx, ref)
	return result, observed, err
}

// AcquireLeases admits a ticket to every requested capacity dimension in one
// transaction. If any dimension is full, none of the new leases are retained.
// Replaying the same fenced request returns the ticket's existing slots.
func (s *Store) AcquireLeases(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence, requests []LeaseRequest, at time.Time) ([]Lease, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	requests, err := validateLeaseRequests(requests)
	if err != nil {
		return nil, err
	}
	if at.IsZero() {
		return nil, errors.New("lease acquisition time is required")
	}
	var acquired []Lease
	err = s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if state.Terminal() || version != expectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		acquired = make([]Lease, 0, len(requests))
		for _, request := range requests {
			lease, ok, err := acquireLease(ctx, conn, ref, runner, request, at.UTC())
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("%w: scope=%s resource=%s capacity=%d", ErrLeaseCapacity, request.Scope, request.Resource, request.Capacity)
			}
			acquired = append(acquired, lease)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return acquired, nil
}

func acquireLease(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, runner uint64, request LeaseRequest, at time.Time) (Lease, bool, error) {
	for slot := 0; slot < request.Capacity; slot++ {
		key := leaseKey(request.Resource, slot)
		var project domain.ProjectID
		var ticket domain.TicketID
		var persistedRunner uint64
		var acquiredAt string
		err := conn.QueryRowContext(ctx, `SELECT project_id, ticket_id, runner_epoch, acquired_at FROM leases WHERE channel=? AND scope=? AND scope_key=?`, ref.Channel, request.Scope, key).Scan(&project, &ticket, &persistedRunner, &acquiredAt)
		if err == nil {
			if project == ref.Project && ticket == ref.Ticket && persistedRunner == runner {
				parsed, parseErr := time.Parse(time.RFC3339Nano, acquiredAt)
				if parseErr != nil {
					return Lease{}, false, fmt.Errorf("decode lease time: %w", parseErr)
				}
				return Lease{Ref: ref, Scope: request.Scope, ScopeKey: key, RunnerEpoch: runner, AcquiredAt: parsed}, true, nil
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Lease{}, false, err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO leases(channel, project_id, scope, scope_key, ticket_id, runner_epoch, acquired_at)
			VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(channel, scope, scope_key) DO NOTHING`, ref.Channel, ref.Project, request.Scope, key, ref.Ticket, runner, at.Format(time.RFC3339Nano))
		if err != nil {
			return Lease{}, false, err
		}
		if changed, _ := result.RowsAffected(); changed == 1 {
			return Lease{Ref: ref, Scope: request.Scope, ScopeKey: key, RunnerEpoch: runner, AcquiredAt: at}, true, nil
		}
	}
	return Lease{}, false, nil
}

// ReleaseLeases releases only the current runner's capacity. Callers must
// first prove that processes and uncertain effects are drained.
func (s *Store) ReleaseLeases(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (int64, error) {
	if err := ref.Validate(); err != nil {
		return 0, err
	}
	var released int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if version != expectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, runner)
		if err != nil {
			return err
		}
		released, _ = result.RowsAffected()
		return nil
	})
	return released, err
}

// StaleLeases is a leader-fenced startup observation. Runner invalidation alone
// never frees capacity: the supervisor must first prove the old runner has no
// live writer, then call ReleaseInvalidatedLeases for that exact epoch.
func (s *Store) StaleLeases(ctx context.Context, channel domain.Channel, leaderEpoch uint64) ([]Lease, error) {
	if !channel.Valid() || leaderEpoch == 0 {
		return nil, errors.New("valid channel and leader epoch are required")
	}
	var current uint64
	if err := s.db.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&current); err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	if current != leaderEpoch {
		return nil, ErrStaleFence
	}
	rows, err := s.db.QueryContext(ctx, `SELECT l.project_id, l.ticket_id, l.scope, l.scope_key, l.runner_epoch, l.acquired_at
		FROM leases AS l JOIN tickets AS t ON t.channel=l.channel AND t.project_id=l.project_id AND t.id=l.ticket_id
		WHERE l.channel=? AND l.runner_epoch<>t.runner_epoch ORDER BY l.project_id, l.ticket_id, l.runner_epoch, l.scope, l.scope_key`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	return scanLeases(rows, channel)
}

// ReleaseInvalidatedLeases is intentionally separate from observation. Its
// caller asserts that the exact old runner has passed process/effect drain.
func (s *Store) ReleaseInvalidatedLeases(ctx context.Context, ref domain.TicketRef, staleRunner, leaderEpoch uint64) (int64, error) {
	if err := ref.Validate(); err != nil {
		return 0, err
	}
	if staleRunner == 0 || leaderEpoch == 0 {
		return 0, errors.New("stale runner and leader epochs are required")
	}
	var released int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader, currentRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentRunner); err != nil {
			return err
		}
		if currentLeader != leaderEpoch || currentRunner <= staleRunner {
			return ErrStaleFence
		}
		result, err := conn.ExecContext(ctx, `DELETE FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, staleRunner)
		if err != nil {
			return err
		}
		released, _ = result.RowsAffected()
		return nil
	})
	return released, err
}

// AdoptInvalidatedLeases transfers a recovered ticket's durable global and
// project capacity from one invalidated runner to its current runner. It is a
// startup-only repair: it neither frees a slot nor changes its scope identity
// or acquisition time. Provider capacity is deliberately never transferable.
//
// The transfer is fail-closed. In particular, an active or quarantined
// provider attempt or Git mutation lease for the ticket means an old process
// could still be writing, so no capacity ownership is changed.
func (s *Store) AdoptInvalidatedLeases(ctx context.Context, ref domain.TicketRef, staleRunner, leaderEpoch uint64) (int64, error) {
	if err := ref.Validate(); err != nil {
		return 0, err
	}
	if staleRunner == 0 || leaderEpoch == 0 {
		return 0, errors.New("stale runner and leader epochs are required")
	}
	var adopted int64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var currentLeader, currentRunner uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&currentLeader); err != nil {
			return err
		}
		if err := conn.QueryRowContext(ctx, `SELECT runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentRunner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if currentLeader != leaderEpoch || currentRunner <= staleRunner {
			return ErrStaleFence
		}

		var found, unsupported int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN scope NOT IN ('global','project') THEN 1 ELSE 0 END), 0)
			FROM leases WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, staleRunner).Scan(&found, &unsupported); err != nil {
			return err
		}
		if unsupported != 0 {
			return ErrLeaseAdoption
		}
		// A prior successful transfer leaves no rows for this stale epoch. This
		// is the intentional replay result for a crash after commit.
		if found == 0 {
			return nil
		}

		var providerWriters, gitWriters int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts
			WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, ref.Channel, ref.Project, ref.Ticket).Scan(&providerWriters); err != nil {
			return err
		}
		if providerWriters != 0 {
			return ErrLeaseAdoption
		}
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases
			WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, ref.Channel, ref.Project, ref.Ticket).Scan(&gitWriters); err != nil {
			return err
		}
		if gitWriters != 0 {
			return ErrLeaseAdoption
		}

		result, err := conn.ExecContext(ctx, `UPDATE leases SET runner_epoch=?
			WHERE channel=? AND project_id=? AND ticket_id=? AND runner_epoch=? AND scope IN ('global','project')`,
			currentRunner, ref.Channel, ref.Project, ref.Ticket, staleRunner)
		if err != nil {
			return err
		}
		adopted, _ = result.RowsAffected()
		return nil
	})
	return adopted, err
}

func (s *Store) Leases(ctx context.Context, channel domain.Channel) ([]Lease, error) {
	if !channel.Valid() {
		return nil, fmt.Errorf("invalid channel %q", channel)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT project_id, ticket_id, scope, scope_key, runner_epoch, acquired_at FROM leases WHERE channel=? ORDER BY scope, scope_key`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	return scanLeases(rows, channel)
}

func scanLeases(rows *sql.Rows, channel domain.Channel) ([]Lease, error) {
	var leases []Lease
	for rows.Next() {
		var lease Lease
		lease.Ref.Channel = channel
		var acquiredAt string
		if err := rows.Scan(&lease.Ref.Project, &lease.Ref.Ticket, &lease.Scope, &lease.ScopeKey, &lease.RunnerEpoch, &acquiredAt); err != nil {
			return nil, err
		}
		parsedAt, err := time.Parse(time.RFC3339Nano, acquiredAt)
		if err != nil {
			return nil, fmt.Errorf("decode lease time: %w", err)
		}
		lease.AcquiredAt = parsedAt
		leases = append(leases, lease)
	}
	return leases, rows.Err()
}

func validateLeaseRequests(requests []LeaseRequest) ([]LeaseRequest, error) {
	if len(requests) == 0 || len(requests) > 16 {
		return nil, errors.New("between one and sixteen lease requests are required")
	}
	copy := append([]LeaseRequest(nil), requests...)
	sort.Slice(copy, func(i, j int) bool {
		if copy[i].Scope != copy[j].Scope {
			return copy[i].Scope < copy[j].Scope
		}
		return copy[i].Resource < copy[j].Resource
	})
	for index, request := range copy {
		if request.Scope != "global" && request.Scope != "project" && request.Scope != "provider" {
			return nil, fmt.Errorf("invalid lease scope %q", request.Scope)
		}
		if strings.TrimSpace(request.Resource) != request.Resource || request.Resource == "" || len(request.Resource) > 200 || strings.ContainsRune(request.Resource, '\x00') {
			return nil, errors.New("lease resource must be a bounded nonempty identity")
		}
		if request.Capacity < 1 || request.Capacity > 64 {
			return nil, errors.New("lease capacity must be between one and sixty-four")
		}
		if index > 0 && request.Scope == copy[index-1].Scope && request.Resource == copy[index-1].Resource {
			return nil, errors.New("duplicate lease request")
		}
	}
	return copy, nil
}

func leaseKey(resource string, slot int) string {
	return strconv.Itoa(len(resource)) + ":" + resource + ":" + strconv.Itoa(slot)
}
