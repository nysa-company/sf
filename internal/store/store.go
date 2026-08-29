// Package store owns the only mutable application authority: a channel's
// SQLite database. It deliberately contains no Git, GitHub, or provider code.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	_ "modernc.org/sqlite"
)

var (
	ErrBusy       = errors.New("sqlite write deadline exceeded")
	ErrStaleFence = errors.New("ticket fence is stale")
	ErrNotFound   = errors.New("store row not found")
)

const schemaVersion = 1

type Store struct{ db *sql.DB }

type Project struct {
	Channel domain.Channel
	ID      domain.ProjectID
	Path    string
	BaseRef string
}

type Ticket struct {
	Ref          domain.TicketRef
	State        domain.State
	ResumeState  domain.State
	Version      uint64
	RunnerEpoch  uint64
	WorkflowID   string
	SourceDigest string
	Type         domain.TicketType
	MergeMode    domain.MergeMode
	BlockedCode  string
}

type Transition struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	From            domain.State
	To              domain.State
	ResumeState     domain.State
	Trigger         string
	Fence           domain.Fence
	EventPayload    string
}

type TransitionResult struct {
	Version uint64
	EventID int64
}

// Open configures SQLite for application-owned bounded retry. busy_timeout is
// intentionally zero: writes use retryWrite and always honor the caller's
// context rather than letting a driver-owned timeout outlive it.
func Open(ctx context.Context, path string) (*Store, error) {
	// modernc applies _pragma values to every pooled connection. A standalone
	// PRAGMA foreign_keys call would configure only whichever connection happened
	// to execute it, silently weakening foreign-key enforcement under load.
	dsn := path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(0)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	s := &Store{db: db}
	if err := s.configure(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) configure(ctx context.Context) error {
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=0",
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure sqlite (%s): %w", statement, err)
		}
	}
	return nil
}

func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	var current int
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if current > schemaVersion {
		return fmt.Errorf("database schema %d is newer than supported %d", current, schemaVersion)
	}
	if current == schemaVersion {
		return nil
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		for _, statement := range migrationV1 {
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("run migration %d: %w", schemaVersion, err)
			}
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)`, schemaVersion, time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
}

// write begins an IMMEDIATE transaction and retries only locally, only while
// ctx remains live. No background retry or leaked worker survives a deadline.
func (s *Store) write(ctx context.Context, fn func(*sql.Conn) error) error {
	backoff := time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v", ErrBusy, err)
		}
		conn, err := s.db.Conn(ctx)
		if err == nil {
			_, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE")
		}
		if err == nil {
			err = fn(conn)
			if err == nil {
				_, err = conn.ExecContext(ctx, "COMMIT")
			} else {
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
			conn.Close()
			if err == nil {
				return nil
			}
		} else if conn != nil {
			conn.Close()
		}
		if !isBusy(err) {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w: %v", ErrBusy, ctx.Err())
		case <-timer.C:
		}
		if backoff < 16*time.Millisecond {
			backoff *= 2
		}
	}
}

func isBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database is busy") || strings.Contains(message, "sqlite_busy")
}

func (s *Store) CreateProject(ctx context.Context, project Project) error {
	if !project.Channel.Valid() || project.ID == "" || project.Path == "" || project.BaseRef == "" {
		return fmt.Errorf("project channel, id, path, and base ref are required")
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO projects(channel, id, canonical_path, base_ref) VALUES (?, ?, ?, ?)`, project.Channel, project.ID, project.Path, project.BaseRef)
		return err
	})
}

func (s *Store) CreateTicket(ctx context.Context, ticket Ticket) error {
	if err := ticket.Ref.Validate(); err != nil {
		return err
	}
	if ticket.SourceDigest == "" || !ticket.Type.Valid() || !ticket.MergeMode.Valid() {
		return fmt.Errorf("ticket source digest, type, and merge mode are required")
	}
	if ticket.State == "" {
		ticket.State = domain.StateQueued
	}
	if ticket.Version == 0 {
		ticket.Version = 1
	}
	if ticket.RunnerEpoch == 0 {
		ticket.RunnerEpoch = 1
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		_, err := conn.ExecContext(ctx, `INSERT INTO tickets(
			channel, project_id, id, source_digest, ticket_type, merge_mode, state,
			resume_state, version, runner_epoch, workflow_id, blocked_code
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.SourceDigest,
			ticket.Type, ticket.MergeMode, ticket.State, nullableState(ticket.ResumeState),
			ticket.Version, ticket.RunnerEpoch, ticket.WorkflowID, ticket.BlockedCode)
		return err
	})
}

func (s *Store) Ticket(ctx context.Context, ref domain.TicketRef) (Ticket, error) {
	if err := ref.Validate(); err != nil {
		return Ticket{}, err
	}
	var ticket Ticket
	ticket.Ref = ref
	var resume sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT state, resume_state, version, runner_epoch, workflow_id, source_digest, ticket_type, merge_mode, blocked_code
		FROM tickets WHERE channel = ? AND project_id = ? AND id = ?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&ticket.State, &resume, &ticket.Version, &ticket.RunnerEpoch, &ticket.WorkflowID,
		&ticket.SourceDigest, &ticket.Type, &ticket.MergeMode, &ticket.BlockedCode,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Ticket{}, ErrNotFound
	}
	if err != nil {
		return Ticket{}, err
	}
	if resume.Valid {
		ticket.ResumeState = domain.State(resume.String)
	}
	return ticket, nil
}

// AcquireLeader increments the channel epoch. A daemon lock is enforced by the
// daemon package later; this durable epoch is the database fence used here.
func (s *Store) AcquireLeader(ctx context.Context, channel domain.Channel, identity string) (uint64, error) {
	if !channel.Valid() || identity == "" {
		return 0, fmt.Errorf("valid channel and daemon identity are required")
	}
	var epoch uint64
	err := s.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `INSERT INTO daemon_instances(channel, leader_epoch, identity, updated_at)
			VALUES (?, 0, '', '') ON CONFLICT(channel) DO NOTHING`, channel); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE daemon_instances SET leader_epoch = leader_epoch + 1, identity = ?, updated_at = ? WHERE channel = ?`, identity, time.Now().UTC().Format(time.RFC3339Nano), channel); err != nil {
			return err
		}
		return conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel = ?`, channel).Scan(&epoch)
	})
	return epoch, err
}

// StartOrAdopt atomically moves queued work to planning and records durable
// ownership. If a previous process committed planning then died before
// ownership, recovery supplies the same stable ID and creates exactly one row.
func (s *Store) StartOrAdopt(ctx context.Context, ref domain.TicketRef, workflowID string, fence domain.Fence) (Ticket, error) {
	if workflowID == "" {
		return Ticket{}, fmt.Errorf("stable workflow id is required")
	}
	err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		var persistedWorkflowID string
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch, workflow_id FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &persistedWorkflowID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		stateChanged := false
		if state == domain.StateQueued {
			result, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?, version=version+1, workflow_id=? WHERE channel=? AND project_id=? AND id=? AND version=? AND runner_epoch=?`, domain.StatePlanning, workflowID, ref.Channel, ref.Project, ref.Ticket, version, runner)
			if err != nil {
				return err
			}
			if changed, _ := result.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
			version++
			stateChanged = true
		} else if state != domain.StatePlanning {
			return fmt.Errorf("cannot start or adopt ticket in state %q", state)
		} else if persistedWorkflowID == "" {
			return fmt.Errorf("cannot adopt planning ticket without persisted workflow id")
		} else if persistedWorkflowID != workflowID {
			return fmt.Errorf("%w: workflow id does not match durable ticket identity", ErrStaleFence)
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO workflow_owners(channel, project_id, ticket_id, workflow_id, state, created_at)
			VALUES (?, ?, ?, ?, 'owned', ?) ON CONFLICT(channel, project_id, ticket_id) DO NOTHING`, ref.Channel, ref.Project, ref.Ticket, workflowID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		var ownedWorkflowID string
		if err := conn.QueryRowContext(ctx, `SELECT workflow_id FROM workflow_owners WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ownedWorkflowID); err != nil {
			return err
		}
		if ownedWorkflowID != workflowID {
			return fmt.Errorf("%w: workflow owner does not match durable ticket identity", ErrStaleFence)
		}
		// A replay that finds a planning ticket with its owner already present is
		// a read/confirmation, not a second transition or event.
		if !stateChanged {
			return nil
		}
		_, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at)
			VALUES (?, ?, ?, ?, 'start_or_adopt', ?, ?, '{}', ?)`, ref.Channel, ref.Project, ref.Ticket, version, state, domain.StatePlanning, time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
	if err != nil {
		return Ticket{}, err
	}
	return s.Ticket(ctx, ref)
}

// ReconcileOrphans detects planning tickets that lack durable workflow
// ownership and either adopts them with their persisted ID or blocks them when
// the ID was never persisted. It does not guess a new identity.
func (s *Store) ReconcileOrphans(ctx context.Context, channel domain.Channel, leaderEpoch uint64) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		var dbEpoch uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&dbEpoch); err != nil {
			return err
		}
		if dbEpoch != leaderEpoch {
			return ErrStaleFence
		}
		rows, err := conn.QueryContext(ctx, `SELECT project_id, id, workflow_id, version FROM tickets t
			WHERE channel=? AND state='planning' AND NOT EXISTS (
				SELECT 1 FROM workflow_owners o WHERE o.channel=t.channel AND o.project_id=t.project_id AND o.ticket_id=t.id
			)`, channel)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var project domain.ProjectID
			var ticket domain.TicketID
			var workflow string
			var version uint64
			if err := rows.Scan(&project, &ticket, &workflow, &version); err != nil {
				return err
			}
			if workflow == "" {
				if _, err := conn.ExecContext(ctx, `UPDATE tickets SET state='blocked', blocked_code='workflow_ownership_unknown', version=version+1 WHERE channel=? AND project_id=? AND id=?`, channel, project, ticket); err != nil {
					return err
				}
				continue
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO workflow_owners(channel, project_id, ticket_id, workflow_id, state, created_at) VALUES (?, ?, ?, ?, 'owned', ?)`, channel, project, ticket, workflow, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}

func (s *Store) Transition(ctx context.Context, transition Transition) (TransitionResult, error) {
	if err := transition.Ref.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if !transition.To.Valid() || !transition.From.Valid() || transition.Trigger == "" {
		return TransitionResult{}, fmt.Errorf("valid from/to state and trigger are required")
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var actual domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state, version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &version, &runner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if actual != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?, resume_state=?, version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, transition.To, nullableState(transition.ResumeState), transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, transition.Trigger, transition.From, transition.To, transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

func (s *Store) InvalidateRunner(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, fence domain.Fence) (Ticket, error) {
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner); err != nil {
			return err
		}
		if version != expectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `UPDATE tickets SET runner_epoch=runner_epoch+1, version=version+1 WHERE channel=? AND project_id=? AND id=? AND version=? AND runner_epoch=?`, ref.Channel, ref.Project, ref.Ticket, version, runner)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		return nil
	})
	if err != nil {
		return Ticket{}, err
	}
	return s.Ticket(ctx, ref)
}

// RecordPhaseCompletion is idempotent. The unique phase attempt means a
// recovered process cannot create a second completed attempt for the same work.
func (s *Store) RecordPhaseCompletion(ctx context.Context, ref domain.TicketRef, phase domain.Phase, attempt int, fence domain.Fence) (bool, error) {
	inserted := false
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		if err := conn.QueryRowContext(ctx, `SELECT version, runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner); err != nil {
			return err
		}
		if err := s.currentFence(ctx, conn, ref.Channel, version, runner, fence); err != nil {
			return err
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO phase_runs(channel, project_id, ticket_id, phase, attempt, state, leader_epoch, runner_epoch, completed_at) VALUES (?, ?, ?, ?, ?, 'completed', ?, ?, ?) ON CONFLICT(channel, project_id, ticket_id, phase, attempt) DO NOTHING`, ref.Channel, ref.Project, ref.Ticket, phase, attempt, fence.LeaderEpoch, fence.RunnerEpoch, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		inserted = count == 1
		return nil
	})
	return inserted, err
}

func (s *Store) currentFence(ctx context.Context, conn *sql.Conn, channel domain.Channel, version, runner uint64, fence domain.Fence) error {
	var leader uint64
	if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&leader); err != nil {
		return err
	}
	if leader != fence.LeaderEpoch || runner != fence.RunnerEpoch {
		return ErrStaleFence
	}
	_ = version // version is checked by individual CAS updates.
	return nil
}

func nullableState(state domain.State) any {
	if state == "" {
		return nil
	}
	return string(state)
}
