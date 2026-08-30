// Package store owns the only mutable application authority: a channel's
// SQLite database. It deliberately contains no Git, GitHub, or provider code.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	_ "modernc.org/sqlite"
)

var (
	ErrBusy                  = errors.New("sqlite write deadline exceeded")
	ErrStaleFence            = errors.New("ticket fence is stale")
	ErrNotFound              = errors.New("store row not found")
	ErrBlocked               = errors.New("ticket is blocked")
	ErrTerminalReplay        = errors.New("terminal ticket replay requires an explicit new ticket")
	ErrStaleObservation      = errors.New("effect observation belongs to a stale ticket identity")
	ErrEffectBusy            = errors.New("effect already has a live claim")
	ErrEffectKey             = errors.New("effect semantic key conflicts with durable record")
	ErrEvidenceConflict      = errors.New("evidence conflicts with durable record")
	ErrBudgetExhausted       = errors.New("bounded ticket budget is exhausted")
	ErrProjectConflict       = errors.New("project registration conflicts with durable record")
	ErrBranchConflict        = errors.New("branch allocation conflicts with durable record")
	ErrQualificationConflict = errors.New("provider qualification conflicts with durable record")
	ErrProviderPairRefused   = errors.New("provider pair is not current, qualified, and independent")
	ErrReadOnly              = errors.New("store is read-only")
	ErrProviderCapacity      = errors.New("provider route capacity is exhausted")
	ErrProviderAttempt       = errors.New("provider attempt cannot be admitted")
	ErrProviderDrain         = errors.New("provider process has not drained")
)

const schemaVersion = 12

var migrationChecksums = map[int]string{
	1:  migrationChecksum(migrationV1),
	2:  migrationChecksum(migrationV2),
	3:  migrationChecksum(migrationV3),
	4:  migrationChecksum(migrationV4),
	5:  migrationChecksum(migrationV5),
	6:  migrationChecksum(migrationV6),
	7:  migrationChecksum(migrationV7),
	8:  migrationChecksum(migrationV8),
	9:  migrationChecksum(migrationV9),
	10: migrationChecksum(migrationV10),
	11: migrationChecksum(migrationV11),
	12: migrationChecksum(migrationV12),
}

func migrationChecksum(statements []string) string {
	hash := sha256.New()
	for _, statement := range statements {
		_, _ = hash.Write([]byte(statement))
		_, _ = hash.Write([]byte{0})
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

type Store struct {
	db       *sql.DB
	commit   func(context.Context, *sql.Conn) error
	readOnly bool
}

type Project struct {
	Channel          domain.Channel
	ID               domain.ProjectID
	Path             string
	BaseRef          string
	ConfigGeneration uint64
	ConfigDigest     string
	ConfigSnapshot   []byte
}

type Ticket struct {
	Ref              domain.TicketRef
	State            domain.State
	ResumeState      domain.State
	Version          uint64
	RunnerEpoch      uint64
	WorkflowID       string
	SourceDigest     string
	Type             domain.TicketType
	MergeMode        domain.MergeMode
	BlockedCode      string
	Title            string
	Problem          string
	Acceptance       []string
	Source           []byte
	Priority         string
	CreatedAt        time.Time
	MaxDuration      time.Duration
	MaxCostMicroUSD  int64
	ConfigGeneration uint64
	ConfigDigest     string
	ConfigSnapshot   []byte
}

type Event struct {
	ID            uint64
	Ref           domain.TicketRef
	TicketVersion uint64
	Trigger       string
	From          domain.State
	To            domain.State
	Payload       string
	CreatedAt     time.Time
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
	s := &Store{db: db, commit: commitTransaction}
	if err := s.configure(ctx); err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	if err := s.integrity(ctx); err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	if err := s.validateSchema(ctx); err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	return s, nil
}

// OpenReadOnly opens an existing, fully migrated authority for diagnostics.
// It never creates a database, changes write-affecting pragmas, or runs
// migrations, so doctor/status cannot become a second state writer.
func OpenReadOnly(ctx context.Context, path string) (*Store, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("read-only sqlite path must be absolute and clean")
	}
	uri := (&url.URL{Scheme: "file", Path: path}).String()
	dsn := uri + "?mode=ro&_pragma=foreign_keys(1)&_pragma=query_only(1)&_pragma=busy_timeout(0)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open read-only sqlite: %w", err)
	}
	// Schema validation deliberately nests index-detail reads while the index
	// listing cursor is open, so it needs the same small read pool as normal
	// startup. query_only and mode=ro still make every connection non-mutating.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	value := &Store{db: db, commit: commitTransaction, readOnly: true}
	if err := value.validateSchema(ctx); err != nil {
		_ = db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	return value, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) configure(ctx context.Context) error {
	for _, statement := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=0",
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure sqlite (%s): %w", statement, normalizeBusy(ctx, err))
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
	for version := current + 1; version <= schemaVersion; version++ {
		version := version
		if err := s.write(ctx, func(conn *sql.Conn) error {
			statements := migrationV1
			if version == 2 {
				statements = migrationV2
			} else if version == 3 {
				statements = migrationV3
			} else if version == 4 {
				statements = migrationV4
			} else if version == 5 {
				statements = migrationV5
			} else if version == 6 {
				statements = migrationV6
			} else if version == 7 {
				statements = migrationV7
			} else if version == 8 {
				statements = migrationV8
			} else if version == 9 {
				statements = migrationV9
			} else if version == 10 {
				statements = migrationV10
			} else if version == 11 {
				statements = migrationV11
			} else if version == 12 {
				statements = migrationV12
			}
			for _, statement := range statements {
				if _, err := conn.ExecContext(ctx, statement); err != nil {
					return fmt.Errorf("run migration %d: %w", version, err)
				}
			}
			if version == 1 {
				_, err := conn.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)`, version, time.Now().UTC().Format(time.RFC3339Nano))
				return err
			}
			if _, err := conn.ExecContext(ctx, `UPDATE schema_migrations SET checksum=? WHERE version=1`, migrationChecksums[1]); err != nil {
				return err
			}
			_, err := conn.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at, checksum) VALUES (?, ?, ?)`, version, time.Now().UTC().Format(time.RFC3339Nano), migrationChecksums[version])
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

// write begins an IMMEDIATE transaction and retries only locally, only while
// ctx remains live. No background retry or leaked worker survives a deadline.
func (s *Store) write(ctx context.Context, fn func(*sql.Conn) error) error {
	if s.readOnly {
		return ErrReadOnly
	}
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
				err = s.commit(ctx, conn)
			}
			if err != nil {
				// COMMIT can fail after BEGIN succeeded (including cancellation and
				// disk faults). Always clear the transaction before returning this
				// pooled connection to another caller.
				_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			}
			conn.Close()
			if err == nil {
				return nil
			}
			if ctx.Err() != nil {
				return fmt.Errorf("%w: %v", ErrBusy, ctx.Err())
			}
		} else if conn != nil {
			conn.Close()
		}
		if !isBusy(err) {
			return normalizeBusy(ctx, err)
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

func commitTransaction(ctx context.Context, conn *sql.Conn) error {
	_, err := conn.ExecContext(ctx, "COMMIT")
	return err
}

func normalizeBusy(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %v", ErrBusy, ctx.Err())
	}
	if isBusy(err) {
		return fmt.Errorf("%w: %v", ErrBusy, err)
	}
	return err
}

func isBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") || strings.Contains(message, "database is busy") || strings.Contains(message, "sqlite_busy")
}

func (s *Store) CreateProject(ctx context.Context, project Project) error {
	if err := validateProjectRegistration(project, false); err != nil {
		return err
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		return insertProject(ctx, conn, project)
	})
}

// RegisterProject creates one durable registration or confirms an exact
// replay. It never silently changes a path, base branch, or configuration.
func (s *Store) RegisterProject(ctx context.Context, project Project) (bool, error) {
	if err := validateProjectRegistration(project, true); err != nil {
		return false, err
	}
	created := false
	err := s.write(ctx, func(conn *sql.Conn) error {
		var existing Project
		existing.Channel, existing.ID = project.Channel, project.ID
		err := conn.QueryRowContext(ctx, `SELECT p.canonical_path, p.base_ref, p.current_config_generation,
			COALESCE(c.digest, ''), COALESCE(c.snapshot_bytes, X'')
			FROM projects p LEFT JOIN project_configurations c
			ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation
			WHERE p.channel=? AND p.id=?`, project.Channel, project.ID).Scan(
			&existing.Path, &existing.BaseRef, &existing.ConfigGeneration, &existing.ConfigDigest, &existing.ConfigSnapshot,
		)
		if errors.Is(err, sql.ErrNoRows) {
			if err := insertProject(ctx, conn, project); err != nil {
				return err
			}
			created = true
			return nil
		}
		if err != nil {
			return err
		}
		if existing.Path != project.Path || existing.BaseRef != project.BaseRef || existing.ConfigGeneration != project.ConfigGeneration || existing.ConfigDigest != project.ConfigDigest || !bytes.Equal(existing.ConfigSnapshot, project.ConfigSnapshot) {
			return ErrProjectConflict
		}
		return nil
	})
	return created, err
}

func validateProjectRegistration(project Project, requireSnapshot bool) error {
	if !project.Channel.Valid() || project.ID == "" || project.Path == "" || project.BaseRef == "" {
		return fmt.Errorf("project channel, id, path, and base ref are required")
	}
	if len(project.ConfigSnapshot) == 0 {
		if requireSnapshot || project.ConfigGeneration != 0 || project.ConfigDigest != "" {
			return fmt.Errorf("complete project configuration snapshot is required")
		}
		return nil
	}
	if len(project.ConfigSnapshot) > 64*1024 || project.ConfigGeneration != 1 {
		return fmt.Errorf("initial project configuration generation is invalid")
	}
	digest := sha256.Sum256(project.ConfigSnapshot)
	if project.ConfigDigest != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("project configuration digest does not match snapshot")
	}
	return nil
}

func insertProject(ctx context.Context, conn *sql.Conn, project Project) error {
	if _, err := conn.ExecContext(ctx, `INSERT INTO projects(channel, id, canonical_path, base_ref, current_config_generation) VALUES (?, ?, ?, ?, ?)`, project.Channel, project.ID, project.Path, project.BaseRef, project.ConfigGeneration); err != nil {
		return err
	}
	if project.ConfigGeneration == 0 {
		return nil
	}
	_, err := conn.ExecContext(ctx, `INSERT INTO project_configurations(channel, project_id, generation, digest, snapshot_bytes, created_at) VALUES (?, ?, ?, ?, ?, ?)`, project.Channel, project.ID, project.ConfigGeneration, project.ConfigDigest, project.ConfigSnapshot, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// Project returns the immutable repository registration used by durable ticket
// records. Callers must not treat a later configuration value as authority.
func (s *Store) Project(ctx context.Context, channel domain.Channel, id domain.ProjectID) (Project, error) {
	if !channel.Valid() || id == "" {
		return Project{}, errors.New("valid project channel and id are required")
	}
	project := Project{Channel: channel, ID: id}
	err := s.db.QueryRowContext(ctx, `SELECT p.canonical_path, p.base_ref, p.current_config_generation,
		COALESCE(c.digest, ''), COALESCE(c.snapshot_bytes, X'')
		FROM projects p LEFT JOIN project_configurations c
		ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation
		WHERE p.channel=? AND p.id=?`, channel, id).Scan(&project.Path, &project.BaseRef, &project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot)
	if errors.Is(err, sql.ErrNoRows) {
		return Project{}, ErrNotFound
	}
	if err != nil {
		return Project{}, normalizeBusy(ctx, err)
	}
	return project, nil
}

func (s *Store) Projects(ctx context.Context, channel domain.Channel) ([]Project, error) {
	if !channel.Valid() {
		return nil, fmt.Errorf("invalid channel %q", channel)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.id, p.canonical_path, p.base_ref, p.current_config_generation,
		COALESCE(c.digest, ''), COALESCE(c.snapshot_bytes, X'')
		FROM projects p LEFT JOIN project_configurations c
		ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation
		WHERE p.channel=? ORDER BY p.id`, channel)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var projects []Project
	for rows.Next() {
		project := Project{Channel: channel}
		if err := rows.Scan(&project.ID, &project.Path, &project.BaseRef, &project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot); err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}
	return projects, rows.Err()
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
		return insertTicket(ctx, conn, ticket)
	})
}

// SubmitTicket creates the immutable ticket or returns the active ticket with
// the same source digest. A terminal replay must opt into a newly generated
// identity; the source bytes are never treated as mutable workflow state.
func (s *Store) SubmitTicket(ctx context.Context, ticket Ticket, allowNew bool) (Ticket, bool, error) {
	return s.submitTicket(ctx, ticket, allowNew, 0)
}

// SubmitTicketFenced is the daemon's submission boundary.  Unlike the legacy
// helper above, it proves the durable leader epoch in the same write
// transaction and records the normative submit_valid transition.
func (s *Store) SubmitTicketFenced(ctx context.Context, ticket Ticket, allowNew bool, leaderEpoch uint64) (Ticket, bool, error) {
	if leaderEpoch == 0 {
		return Ticket{}, false, ErrStaleFence
	}
	return s.submitTicket(ctx, ticket, allowNew, leaderEpoch)
}

func (s *Store) submitTicket(ctx context.Context, ticket Ticket, allowNew bool, leaderEpoch uint64) (Ticket, bool, error) {
	if err := ticket.Ref.Validate(); err != nil {
		return Ticket{}, false, err
	}
	if len(ticket.Source) == 0 {
		return Ticket{}, false, errors.New("ticket source bytes are required")
	}
	decodedDigest, err := hex.DecodeString(ticket.SourceDigest)
	if err != nil || len(decodedDigest) != sha256.Size || hex.EncodeToString(decodedDigest) != ticket.SourceDigest {
		return Ticket{}, false, errors.New("ticket source digest must be a canonical SHA-256 digest")
	}
	if err := validateTicketInput(ticket); err != nil {
		return Ticket{}, false, err
	}
	if leaderEpoch != 0 && ticket.State != "" && ticket.State != domain.StateQueued {
		return Ticket{}, false, errors.New("normative submission must be none to queued")
	}
	var existingRef domain.TicketRef
	created := false
	err = s.write(ctx, func(conn *sql.Conn) error {
		if leaderEpoch != 0 {
			var current uint64
			if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ticket.Ref.Channel).Scan(&current); err != nil {
				return err
			}
			if current != leaderEpoch {
				return ErrStaleFence
			}
		}
		var existingID domain.TicketID
		var existingState domain.State
		err := conn.QueryRowContext(ctx, `SELECT id, state FROM tickets WHERE channel=? AND project_id=? AND source_digest=? ORDER BY rowid DESC LIMIT 1`, ticket.Ref.Channel, ticket.Ref.Project, ticket.SourceDigest).Scan(&existingID, &existingState)
		if err == nil {
			existingRef = domain.TicketRef{Channel: ticket.Ref.Channel, Project: ticket.Ref.Project, Ticket: existingID}
			if !existingState.Terminal() {
				return nil
			}
			if !allowNew {
				return ErrTerminalReplay
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := insertTicketEvent(ctx, conn, ticket, leaderEpoch != 0); err != nil {
			return err
		}
		created = true
		existingRef = ticket.Ref
		return nil
	})
	if err != nil {
		return Ticket{}, false, err
	}
	result, err := s.Ticket(ctx, existingRef)
	return result, created, err
}

func validateTicketInput(ticket Ticket) error {
	if ticket.SourceDigest == "" || !ticket.Type.Valid() || !ticket.MergeMode.Valid() {
		return fmt.Errorf("ticket source digest, type, and merge mode are required")
	}
	if len(ticket.Source) > 0 {
		sum := sha256.Sum256(ticket.Source)
		if fmt.Sprintf("%x", sum[:]) != ticket.SourceDigest {
			return errors.New("ticket source bytes do not match source digest")
		}
	}
	if ticket.Priority == "" {
		ticket.Priority = "normal"
	}
	if ticket.Priority != "low" && ticket.Priority != "normal" && ticket.Priority != "high" {
		return fmt.Errorf("invalid ticket priority %q", ticket.Priority)
	}
	if ticket.MaxDuration < 0 || ticket.MaxCostMicroUSD < 0 {
		return errors.New("ticket ceilings cannot be negative")
	}
	return nil
}

func insertTicket(ctx context.Context, conn *sql.Conn, ticket Ticket) error {
	return insertTicketEvent(ctx, conn, ticket, false)
}

func insertTicketEvent(ctx context.Context, conn *sql.Conn, ticket Ticket, normative bool) error {
	if err := validateTicketInput(ticket); err != nil {
		return err
	}
	if ticket.State == "" {
		ticket.State = domain.StateQueued
	}
	if normative && ticket.State != domain.StateQueued {
		return errors.New("normative submission must target queued")
	}
	if ticket.Version == 0 {
		ticket.Version = 1
	}
	if ticket.RunnerEpoch == 0 {
		ticket.RunnerEpoch = 1
	}
	if ticket.Priority == "" {
		ticket.Priority = "normal"
	}
	if ticket.CreatedAt.IsZero() {
		ticket.CreatedAt = time.Now().UTC()
	}
	if ticket.Acceptance == nil {
		ticket.Acceptance = []string{}
	}
	if ticket.Source == nil {
		ticket.Source = []byte{}
	}
	acceptance, err := json.Marshal(ticket.Acceptance)
	if err != nil {
		return fmt.Errorf("encode ticket acceptance: %w", err)
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO tickets(
			channel, project_id, id, source_digest, ticket_type, merge_mode, state,
			resume_state, version, runner_epoch, workflow_id, blocked_code,
			title, problem, acceptance_json, source_bytes, priority, created_at,
			max_duration_ns, max_cost_micro_usd
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.SourceDigest,
		ticket.Type, ticket.MergeMode, ticket.State, nullableState(ticket.ResumeState),
		ticket.Version, ticket.RunnerEpoch, ticket.WorkflowID, ticket.BlockedCode,
		ticket.Title, ticket.Problem, string(acceptance), ticket.Source, ticket.Priority,
		ticket.CreatedAt.Format(time.RFC3339Nano), int64(ticket.MaxDuration), ticket.MaxCostMicroUSD)
	if err != nil {
		return err
	}
	trigger, from := "ticket_submitted", "queued"
	if normative {
		trigger, from = "submit_valid", "none"
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, '{}', ?)`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, ticket.Version, trigger, from, ticket.State, ticket.CreatedAt.Format(time.RFC3339Nano))
	return err
}

func (s *Store) Ticket(ctx context.Context, ref domain.TicketRef) (Ticket, error) {
	if err := ref.Validate(); err != nil {
		return Ticket{}, err
	}
	var ticket Ticket
	ticket.Ref = ref
	var resume sql.NullString
	var acceptance string
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT state, resume_state, version, runner_epoch, workflow_id, source_digest, ticket_type, merge_mode, blocked_code,
		title, problem, acceptance_json, source_bytes, priority, created_at, max_duration_ns, max_cost_micro_usd,
		config_generation, config_digest, config_snapshot_bytes
		FROM tickets WHERE channel = ? AND project_id = ? AND id = ?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&ticket.State, &resume, &ticket.Version, &ticket.RunnerEpoch, &ticket.WorkflowID,
		&ticket.SourceDigest, &ticket.Type, &ticket.MergeMode, &ticket.BlockedCode,
		&ticket.Title, &ticket.Problem, &acceptance, &ticket.Source, &ticket.Priority, &createdAt,
		&ticket.MaxDuration, &ticket.MaxCostMicroUSD, &ticket.ConfigGeneration, &ticket.ConfigDigest, &ticket.ConfigSnapshot,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Ticket{}, ErrNotFound
	}
	if err != nil {
		return Ticket{}, normalizeBusy(ctx, err)
	}
	if resume.Valid {
		ticket.ResumeState = domain.State(resume.String)
	}
	if err := json.Unmarshal([]byte(acceptance), &ticket.Acceptance); err != nil {
		return Ticket{}, fmt.Errorf("decode ticket acceptance: %w", err)
	}
	if ticket.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
		return Ticket{}, fmt.Errorf("decode ticket creation time: %w", err)
	}
	return ticket, nil
}

// TicketByID resolves the channel-unique operator identity without guessing a
// project from configuration or branch names.
func (s *Store) TicketByID(ctx context.Context, channel domain.Channel, id domain.TicketID) (Ticket, error) {
	if !channel.Valid() || id == "" {
		return Ticket{}, errors.New("valid channel and ticket id are required")
	}
	var project domain.ProjectID
	err := s.db.QueryRowContext(ctx, `SELECT project_id FROM tickets WHERE channel=? AND id=?`, channel, id).Scan(&project)
	if errors.Is(err, sql.ErrNoRows) {
		return Ticket{}, ErrNotFound
	}
	if err != nil {
		return Ticket{}, normalizeBusy(ctx, err)
	}
	return s.Ticket(ctx, domain.TicketRef{Channel: channel, Project: project, Ticket: id})
}

func (s *Store) Tickets(ctx context.Context, channel domain.Channel, project domain.ProjectID, limit int) ([]Ticket, error) {
	if !channel.Valid() {
		return nil, fmt.Errorf("invalid channel %q", channel)
	}
	if limit <= 0 || limit > 10_000 {
		return nil, errors.New("ticket query limit must be between 1 and 10000")
	}
	query := `SELECT project_id, id FROM tickets WHERE channel=?`
	arguments := []any{channel}
	if project != "" {
		query += ` AND project_id=?`
		arguments = append(arguments, project)
	}
	query += ` ORDER BY rowid DESC LIMIT ?`
	arguments = append(arguments, limit)
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var refs []domain.TicketRef
	for rows.Next() {
		var ref domain.TicketRef
		ref.Channel = channel
		if err := rows.Scan(&ref.Project, &ref.Ticket); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]Ticket, 0, len(refs))
	for _, ref := range refs {
		ticket, err := s.Ticket(ctx, ref)
		if err != nil {
			return nil, err
		}
		result = append(result, ticket)
	}
	return result, nil
}

func (s *Store) Events(ctx context.Context, channel domain.Channel, afterID uint64, limit int) ([]Event, error) {
	if !channel.Valid() || limit <= 0 || limit > 100_000 {
		return nil, errors.New("valid channel and event limit between 1 and 100000 are required")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at
		FROM events WHERE channel=? AND id>? ORDER BY id LIMIT ?`, channel, afterID, limit)
	if err != nil {
		return nil, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var result []Event
	for rows.Next() {
		var event Event
		event.Ref.Channel = channel
		var createdAt string
		if err := rows.Scan(&event.ID, &event.Ref.Project, &event.Ref.Ticket, &event.TicketVersion, &event.Trigger, &event.From, &event.To, &event.Payload, &createdAt); err != nil {
			return nil, err
		}
		var err error
		if event.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt); err != nil {
			return nil, fmt.Errorf("decode event creation time: %w", err)
		}
		result = append(result, event)
	}
	return result, rows.Err()
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
func (s *Store) StartOrAdopt(ctx context.Context, ref domain.TicketRef, expectedVersion uint64, workflowID string, fence domain.Fence) (Ticket, error) {
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
			if version != expectedVersion {
				return ErrStaleFence
			}
			result, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?, version=version+1, workflow_id=?,
				config_generation=(SELECT current_config_generation FROM projects WHERE channel=? AND id=?),
				config_digest=COALESCE((SELECT c.digest FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), ''),
				config_snapshot_bytes=COALESCE((SELECT c.snapshot_bytes FROM projects p JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?), X'')
				WHERE channel=? AND project_id=? AND id=? AND version=? AND runner_epoch=?`, domain.StatePlanning, workflowID,
				ref.Channel, ref.Project, ref.Channel, ref.Project, ref.Channel, ref.Project,
				ref.Channel, ref.Project, ref.Ticket, expectedVersion, runner)
			if err != nil {
				return err
			}
			if changed, _ := result.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
			version++
			stateChanged = true
		} else if state == domain.StateBlocked {
			return fmt.Errorf("%w: ticket requires an explicit recovery transition", ErrBlocked)
		} else if state != domain.StatePlanning {
			return fmt.Errorf("cannot start or adopt ticket in state %q", state)
		} else if persistedWorkflowID == "" {
			return fmt.Errorf("cannot adopt planning ticket without persisted workflow id")
		} else if version != expectedVersion+1 {
			return ErrStaleFence
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
		rows, err := conn.QueryContext(ctx, `SELECT project_id, id, workflow_id, version, runner_epoch FROM tickets t
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
			var version, runner uint64
			if err := rows.Scan(&project, &ticket, &workflow, &version, &runner); err != nil {
				return err
			}
			if workflow == "" {
				updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='blocked', blocked_code='workflow_ownership_unknown', version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='planning' AND version=? AND runner_epoch=?`, channel, project, ticket, version, runner)
				if err != nil {
					return err
				}
				if changed, _ := updated.RowsAffected(); changed != 1 {
					return ErrStaleFence
				}
				if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, 'typed_blocker', 'planning', 'blocked', '{"code":"workflow_ownership_unknown"}', ?)`, channel, project, ticket, version+1, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
					return err
				}
				continue
			}
			inserted, err := conn.ExecContext(ctx, `INSERT INTO workflow_owners(channel, project_id, ticket_id, workflow_id, state, created_at) SELECT ?, ?, ?, ?, 'owned', ? WHERE EXISTS (SELECT 1 FROM tickets WHERE channel=? AND project_id=? AND id=? AND state='planning' AND version=? AND runner_epoch=?)`, channel, project, ticket, workflow, time.Now().UTC().Format(time.RFC3339Nano), channel, project, ticket, version, runner)
			if err != nil {
				return err
			}
			if changed, _ := inserted.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, 'workflow_owner_recovered', 'planning', 'planning', '{}', ?)`, channel, project, ticket, version, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}

// BlockOrphanedWorkflows is the daemon-recovery variant of orphan handling.
// A replacement daemon must not silently adopt a workflow whose previous
// runner may still exist; it records a typed blocked state for explicit repair.
func (s *Store) BlockOrphanedWorkflows(ctx context.Context, channel domain.Channel, leaderEpoch uint64) error {
	return s.write(ctx, func(conn *sql.Conn) error {
		var dbEpoch uint64
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, channel).Scan(&dbEpoch); err != nil {
			return err
		}
		if dbEpoch != leaderEpoch {
			return ErrStaleFence
		}
		rows, err := conn.QueryContext(ctx, `SELECT project_id, id, version, runner_epoch FROM tickets t
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
			var version, runner uint64
			if err := rows.Scan(&project, &ticket, &version, &runner); err != nil {
				return err
			}
			updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='blocked', blocked_code='workflow_ownership_unknown', version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='planning' AND version=? AND runner_epoch=?`, channel, project, ticket, version, runner)
			if err != nil {
				return err
			}
			if changed, _ := updated.RowsAffected(); changed != 1 {
				return ErrStaleFence
			}
			if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at) VALUES (?, ?, ?, ?, 'typed_blocker', 'planning', 'blocked', '{"code":"workflow_ownership_unknown"}', ?)`, channel, project, ticket, version+1, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
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
func (s *Store) RecordPhaseCompletion(ctx context.Context, ref domain.TicketRef, phase domain.Phase, attempt int, expectedVersion uint64, fence domain.Fence) (bool, error) {
	inserted := false
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
		result, err := conn.ExecContext(ctx, `INSERT INTO phase_runs(channel, project_id, ticket_id, phase, attempt, state, leader_epoch, runner_epoch, expected_ticket_version, completed_at) VALUES (?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?) ON CONFLICT(channel, project_id, ticket_id, phase, attempt) DO NOTHING`, ref.Channel, ref.Project, ref.Ticket, phase, attempt, fence.LeaderEpoch, fence.RunnerEpoch, expectedVersion, time.Now().UTC().Format(time.RFC3339Nano))
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
