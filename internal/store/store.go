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
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
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
	ErrControlNotDrained     = errors.New("ticket control cannot complete before effects are reconciled")
	ErrReadOnly              = errors.New("store is read-only")
	ErrProviderCapacity      = errors.New("provider route capacity is exhausted")
	ErrProviderAttempt       = errors.New("provider attempt cannot be admitted")
	// ErrProviderAttemptReusable means BeginProviderAttempt found one exact,
	// immutable completed result under the current admission fence. It carries
	// no launch authority; callers must reload the key through Store.
	ErrProviderAttemptReusable = errors.New("provider attempt has a reusable completed result")
	ErrProviderDrain           = errors.New("provider process has not drained")
	// ErrProviderRecoveryBlocked means a durable provider row is intentionally
	// quarantined because its immutable launch authority is invalid. It is
	// distinct from a normal drained recovery: an operator must resolve it.
	ErrProviderRecoveryBlocked = errors.New("provider recovery is blocked by an invalid immutable launch claim")
	ErrGitMutationIntent       = errors.New("git mutation intent is not a current executing effect")
	ErrGitMutationLease        = errors.New("git mutation lease is unavailable or stale")
	ErrRepositoryCommandIntent = errors.New("repository command intent is not a current executing effect")
	ErrRepositoryCommandLease  = errors.New("repository command lease is unavailable or stale")
	ErrRepositoryCommandResult = errors.New("repository command result is missing, malformed, or conflicts with durable evidence")
	ErrPublicationEvidence     = errors.New("publication evidence is missing, malformed, stale, or conflicts with durable evidence")
	ErrPublicationBlockUnsafe  = errors.New("publication blocker requires publication and merge effects to be reconciled")
	ErrCIObservation           = errors.New("CI observation is missing, malformed, stale, or conflicts with durable evidence")
)

const schemaVersion = 47

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
	13: migrationChecksum(migrationV13),
	14: migrationChecksum(migrationV14),
	15: migrationChecksum(migrationV15),
	16: migrationChecksum(migrationV16),
	17: migrationChecksum(migrationV17),
	18: migrationChecksum(migrationV18),
	19: migrationChecksum(migrationV19),
	20: migrationChecksum(migrationV20),
	21: migrationChecksum(migrationV21),
	22: migrationChecksum(migrationV22),
	23: migrationChecksum(migrationV23),
	24: migrationChecksum(migrationV24),
	25: migrationChecksum(migrationV25),
	26: migrationChecksum(migrationV26),
	27: migrationChecksum(migrationV27),
	28: migrationChecksum(migrationV28),
	29: migrationChecksum(migrationV29),
	30: migrationChecksum(migrationV30),
	31: migrationChecksum(migrationV31),
	32: migrationChecksum(migrationV32),
	33: migrationChecksum(migrationV33),
	34: migrationChecksum(migrationV34),
	35: migrationChecksum(migrationV35),
	36: migrationChecksum(migrationV36),
	37: migrationChecksum(migrationV37),
	38: migrationChecksum(migrationV38),
	39: migrationChecksum(migrationV39),
	40: migrationChecksum(migrationV40),
	41: migrationChecksum(migrationV41),
	42: migrationChecksum(migrationV42),
	43: migrationChecksum(migrationV43),
	44: migrationChecksum(migrationV44),
	45: migrationChecksum(migrationV45),
	46: migrationChecksum(migrationV46),
	47: migrationChecksum(migrationV47),
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
	db           *sql.DB
	commit       func(context.Context, *sql.Conn) error
	readOnly     bool
	worktreeRoot string
	faultMu      sync.RWMutex
	writeFault   func() error
	mutations    *ExternalMutationGate
	// controlProofHook is package-test-only synchronization for proving the
	// Store control/admission linearization.
	controlProofHook func()
}

// SetWriteFaultForTest injects a deterministic write failure. It is reserved
// for package/integration tests that verify fail-closed persistence handling.
func (s *Store) SetWriteFaultForTest(fault func() error) {
	s.faultMu.Lock()
	s.writeFault = fault
	s.faultMu.Unlock()
}

func (s *Store) injectedWriteFault() error {
	s.faultMu.RLock()
	fault := s.writeFault
	s.faultMu.RUnlock()
	if fault != nil {
		return fault()
	}
	return nil
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
	return open(ctx, path, openPolicy{})
}

func open(ctx context.Context, path string, policy openPolicy) (*Store, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("sqlite path must be absolute and clean")
	}
	existed, err := existingDatabase(path)
	if err != nil {
		return nil, err
	}
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
	worktreeRoot, err := canonicalWorktreeRoot(path)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db, commit: commitTransaction, worktreeRoot: worktreeRoot}
	s.mutations = &ExternalMutationGate{store: s, gate: make(chan struct{}, 1), revoked: make(map[domain.TicketRef]mutationRevocation), controls: make(map[domain.TicketRef]mutationRevocation)}
	s.mutations.gate <- struct{}{}
	storedVersion, recognized, err := inspectStoredSchema(ctx, db)
	if err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	if storedVersion > schemaVersion {
		db.Close()
		return nil, fmt.Errorf("database schema %d is newer than supported %d", storedVersion, schemaVersion)
	}
	if existed && recognized && storedVersion < schemaVersion && policy.backupBeforeMigration {
		if err := s.backupBeforeMigration(ctx, policy.backupDir, storedVersion, schemaVersion, policy.now); err != nil {
			db.Close()
			return nil, fmt.Errorf("backup schema %d before migration: %w", storedVersion, err)
		}
	}
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
	// Refuse malformed or incomplete schemas before any startup recovery writer
	// can mutate runtime controls or reconcile legacy provider claims.
	if err := s.validateSchema(ctx); err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	if err := s.restoreRuntimeControls(ctx); err != nil {
		db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	// SQL migrations can establish only shape-level invariants. Re-run this
	// Go-level canonical-input audit on every writable open before callers can
	// trust or recover any provider claim.
	if err := s.reconcileProviderAttemptInputs(ctx); err != nil {
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
	worktreeRoot, err := canonicalWorktreeRoot(path)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	value := &Store{db: db, commit: commitTransaction, readOnly: true, worktreeRoot: worktreeRoot}
	if err := value.validateSchema(ctx); err != nil {
		_ = db.Close()
		return nil, normalizeBusy(ctx, err)
	}
	return value, nil
}

func (s *Store) Close() error { return s.db.Close() }

// canonicalWorktreeRoot fixes the one filesystem namespace that Store derives
// from its own database location. A /var -> /private/var alias must not leak
// into a durable worktree path, because Runner pins canonical path identities.
func canonicalWorktreeRoot(databasePath string) (string, error) {
	parent, err := filepath.EvalSymlinks(filepath.Dir(databasePath))
	if err != nil || !filepath.IsAbs(parent) || filepath.Clean(parent) != parent {
		return "", fmt.Errorf("sqlite parent cannot anchor canonical worktrees")
	}
	return filepath.Join(parent, "worktrees"), nil
}

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
			} else if version == 13 {
				statements = migrationV13
			} else if version == 14 {
				statements = migrationV14
			} else if version == 15 {
				statements = migrationV15
			} else if version == 16 {
				statements = migrationV16
			} else if version == 17 {
				statements = migrationV17
			} else if version == 18 {
				statements = migrationV18
			} else if version == 19 {
				statements = migrationV19
			} else if version == 20 {
				statements = migrationV20
			} else if version == 21 {
				statements = migrationV21
			} else if version == 22 {
				statements = migrationV22
			} else if version == 23 {
				statements = migrationV23
			} else if version == 24 {
				statements = migrationV24
			} else if version == 25 {
				statements = migrationV25
			} else if version == 26 {
				statements = migrationV26
			} else if version == 27 {
				statements = migrationV27
			} else if version == 28 {
				statements = migrationV28
			} else if version == 29 {
				statements = migrationV29
			} else if version == 30 {
				statements = migrationV30
			} else if version == 31 {
				statements = migrationV31
			} else if version == 32 {
				statements = migrationV32
			} else if version == 33 {
				statements = migrationV33
			} else if version == 34 {
				statements = migrationV34
			} else if version == 35 {
				statements = migrationV35
			} else if version == 36 {
				statements = migrationV36
			} else if version == 37 {
				statements = migrationV37
			} else if version == 38 {
				statements = migrationV38
			} else if version == 39 {
				statements = migrationV39
			} else if version == 40 {
				statements = migrationV40
			} else if version == 41 {
				statements = migrationV41
			} else if version == 42 {
				statements = migrationV42
			} else if version == 43 {
				statements = migrationV43
			} else if version == 44 {
				statements = migrationV44
			} else if version == 45 {
				statements = migrationV45
			} else if version == 46 {
				statements = migrationV46
			} else if version == 47 {
				statements = migrationV47
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
		if err := s.injectedWriteFault(); err != nil {
			return err
		}
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
	if ticket.Type == domain.TicketSpike && ticket.MergeMode == domain.MergeAutonomous {
		return fmt.Errorf("spike tickets cannot request autonomous merge")
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
	if ticket.Type == domain.TicketSpike && ticket.MergeMode == domain.MergeAutonomous {
		return fmt.Errorf("spike tickets cannot request autonomous merge")
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
	if err := s.DrainChannelExternalMutations(ctx, channel); err != nil {
		return 0, err
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
	if s.mutations == nil {
		return Ticket{}, ErrStaleFence
	}
	if err := s.mutations.lock(ctx); err != nil {
		return Ticket{}, err
	}
	defer s.mutations.unlock()
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
		if err := s.assertTicketFence(ctx, conn, ref, version, fence); err != nil {
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
		createdAt := time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := conn.ExecContext(ctx, `INSERT INTO events(channel, project_id, ticket_id, ticket_version, trigger, from_state, to_state, payload, created_at)
			VALUES (?, ?, ?, ?, 'start_or_adopt', ?, ?, '{}', ?)`, ref.Channel, ref.Project, ref.Ticket, version, state, domain.StatePlanning, createdAt); err != nil {
			return err
		}
		return recordRunnerStartAuthority(ctx, conn, ref, version, fence, workflowID, createdAt)
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
	if transition.Trigger == "typed_blocker" && transition.To == domain.StateBlocked && (transition.From == domain.StatePublishing || transition.From == domain.StateWaitingCI) {
		return s.TransitionPublishedBlock(ctx, transition)
	}
	// Publication retries that exhaust their bounded budget are an authenticated
	// pause, not a generic publication exit.  Keep the witness as the source of
	// truth so the subsequent operator resume can prove this exact pause.
	if semanticPublicationPauseTransition(transition) {
		return s.TransitionPublishedPause(ctx, transition)
	}
	// Publication is a separate trust boundary. A caller must not be able to
	// advance publishing based only on a ticket counter and arbitrary payload.
	if publicationSensitiveTransition(transition.From, transition.To) {
		return TransitionResult{}, ErrPublicationEvidence
	}
	if transition.Trigger == "phase_pass" && (transition.From == domain.StatePlanning || transition.From == domain.StateVerifying || transition.From == domain.StateBuilding) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if err := transition.Ref.Validate(); err != nil {
		return TransitionResult{}, err
	}
	if !transition.To.Valid() || !transition.From.Valid() || transition.Trigger == "" {
		return TransitionResult{}, fmt.Errorf("valid from/to state and trigger are required")
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, errors.New("transition event payload must be bounded JSON")
	}
	if transition.Trigger == "typed_blocker" && transition.To != domain.StateBlocked {
		return TransitionResult{}, errors.New("typed blocker must target blocked state")
	}
	blockedCode := ""
	if transition.Trigger == "typed_blocker" {
		var blocker struct {
			Code string `json:"code"`
		}
		if json.Unmarshal([]byte(transition.EventPayload), &blocker) != nil || !validBlockedCode(blocker.Code) {
			return TransitionResult{}, ErrEvidenceConflict
		}
		blockedCode = blocker.Code
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
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
		query := `UPDATE tickets SET state=?, resume_state=?, version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`
		args := []any{transition.To, nullableState(transition.ResumeState), transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner}
		if transition.Trigger == "typed_blocker" {
			query = `UPDATE tickets SET state=?, resume_state=?, blocked_code=?, version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`
			args = []any{transition.To, nullableState(transition.ResumeState), blockedCode, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner}
		}
		updated, err := conn.ExecContext(ctx, query, args...)
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

func publicationSensitiveTransition(from, to domain.State) bool {
	if to == domain.StatePublishing || to == domain.StateWaitingCI {
		return true
	}
	if from != domain.StatePublishing && from != domain.StateWaitingCI {
		return false
	}
	// An operator's stop/cancel path is a safety action, not publication
	// continuation. All other publication exits must go through a dedicated
	// evidence-bearing boundary.
	return to != domain.StateStopping && to != domain.StateCancelling
}

func validBlockedCode(value string) bool {
	if value == "" || len(value) > 100 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character != '-' && character != '_' && !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') {
			return false
		}
	}
	return true
}

// validatePublicationBlock proves that the ticket has not crossed any
// external publication or merge boundary before a runtime-availability
// blocker exits publishing. It runs inside the same fenced write as the
// transition; the caller's claim alone is not authority.
func validatePublicationBlock(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) error {
	var effects, intents int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command')`, ref.Channel, ref.Project, ref.Ticket).Scan(&effects); err != nil {
		return err
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&intents); err != nil {
		return err
	}
	if effects != 0 || intents != 0 {
		return ErrPublicationBlockUnsafe
	}
	return nil
}

// TransitionPlan consumes the current planner binding in the same SQLite write
// as planning -> verifying. Caller attributes are never evidence authority.
func (s *Store) TransitionPlan(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.From != domain.StatePlanning || transition.To != domain.StateVerifying || transition.Trigger != "phase_pass" {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		var digest, body string
		var artifact []byte
		var id int64
		var attempt int
		var phase, role, state, outcome string
		err := conn.QueryRowContext(ctx, `SELECT p.digest,p.body,p.artifact_bytes,b.provider_attempt_id,b.provider_attempt,a.phase,a.role,a.state,a.outcome
			FROM plans p JOIN plan_result_bindings b ON b.channel=p.channel AND b.project_id=p.project_id AND b.ticket_id=p.ticket_id AND b.plan_digest=p.digest
			JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id
			JOIN provider_attempts a ON a.id=r.provider_attempt_id
			WHERE p.channel=? AND p.project_id=? AND p.ticket_id=? AND b.binding_ticket_version=? AND b.leader_epoch=? AND b.runner_epoch=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version, transition.Fence.LeaderEpoch, transition.Fence.RunnerEpoch).Scan(&digest, &body, &artifact, &id, &attempt, &phase, &role, &state, &outcome)
		if err != nil || digest == "" || id <= 0 || attempt <= 0 || phase != "planning" || role != "planner" || state != "completed" || outcome != "completed" {
			return ErrEvidenceConflict
		}
		var document PlanDocument
		if !bytes.Equal([]byte(body), artifact) || sha256Digest(artifact) != digest || decodeEvidenceJSON(artifact, &document) != nil || document.ProviderResult == nil || document.ProviderResult.AttemptID != id || document.ProviderResult.Attempt != attempt || document.ProviderResult.Ref != transition.Ref || document.ProviderResult.Phase != domain.PhasePlanning {
			return ErrEvidenceConflict
		}
		var actual int
		if err := conn.QueryRowContext(ctx, `SELECT attempt FROM provider_attempts WHERE id=?`, id).Scan(&actual); err != nil || actual != attempt {
			return ErrEvidenceConflict
		}
		return assertNewestBoundResult(ctx, conn, transition.Ref, domain.PhasePlanning, "planner", ProviderAttemptResultKey{AttemptID: id, Ref: transition.Ref, Phase: domain.PhasePlanning, Attempt: attempt})
	})
}

// TransitionVerification consumes the current reviewer/checkpoint binding in
// the same SQLite write as verifying -> building.
func (s *Store) TransitionVerification(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.From != domain.StateVerifying || transition.To != domain.StateBuilding || transition.Trigger != "phase_pass" {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		var id int64
		var attempt int
		var phase, role, state, outcome, checkpoint, parent, tree, revisionCheckpoint string
		err := conn.QueryRowContext(ctx, `SELECT b.provider_attempt_id,b.provider_attempt,a.phase,a.role,a.state,a.outcome,b.checkpoint_commit_oid,b.checkpoint_parent_oid,b.checkpoint_tree_oid,r.checkpoint_id
			FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision
			JOIN verification_result_bindings b ON b.channel=r.channel AND b.project_id=r.project_id AND b.ticket_id=r.ticket_id AND b.revision=r.revision
			JOIN provider_attempt_results pr ON pr.provider_attempt_id=b.provider_attempt_id AND pr.channel=b.channel AND pr.project_id=b.project_id AND pr.ticket_id=b.ticket_id AND pr.phase='verification'
			JOIN provider_attempts a ON a.id=pr.provider_attempt_id AND a.attempt=b.provider_attempt AND a.channel=b.channel AND a.project_id=b.project_id AND a.ticket_id=b.ticket_id
			WHERE v.channel=? AND v.project_id=? AND v.ticket_id=? AND b.binding_ticket_version=? AND b.leader_epoch=? AND b.runner_epoch=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version, transition.Fence.LeaderEpoch, transition.Fence.RunnerEpoch).Scan(&id, &attempt, &phase, &role, &state, &outcome, &checkpoint, &parent, &tree, &revisionCheckpoint)
		if err != nil || id <= 0 || attempt <= 0 || phase != "verification" || role != "reviewer" || state != "completed" || outcome != "completed" || checkpoint != revisionCheckpoint || !validOID(checkpoint) || !validOID(parent) || !validOID(tree) {
			return ErrEvidenceConflict
		}
		var actual int
		if err := conn.QueryRowContext(ctx, `SELECT attempt FROM provider_attempts WHERE id=?`, id).Scan(&actual); err != nil || actual != attempt {
			return ErrEvidenceConflict
		}
		if err := assertNewestBoundResult(ctx, conn, transition.Ref, domain.PhaseVerification, "reviewer", ProviderAttemptResultKey{AttemptID: id, Ref: transition.Ref, Phase: domain.PhaseVerification, Attempt: attempt}); err != nil {
			return err
		}
		stored, err := s.currentVerificationFrom(ctx, conn, transition.Ref)
		if err != nil || stored.Revision.Revision == 0 || stored.Revision.CheckpointID != revisionCheckpoint || stored.ProviderResult.AttemptID != id || stored.ProviderResult.Attempt != attempt || stored.TicketVersion != version || stored.Fence != transition.Fence {
			return ErrEvidenceConflict
		}
		return nil
	})
}

// TransitionFinalReview consumes the exact final Reviewer attempt in the same
// transaction as reviewing -> waiting_*.  Provider-result rows are immutable
// and already bind the parsed pass artifact to the current ticket fence; this
// method is the missing lifecycle consumer that closes the crash window after
// provider completion but before the transition response reaches the worker.
func (s *Store) TransitionFinalReview(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.From != domain.StateReviewing || transition.Trigger != "review_pass" || (transition.To != domain.StateWaitingApproval && transition.To != domain.StateWaitingManualMerge && transition.To != domain.StateDone && transition.To != domain.StateBlocked) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		var ticketType domain.TicketType
		var mergeMode domain.MergeMode
		if err := conn.QueryRowContext(ctx, `SELECT ticket_type,merge_mode FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&ticketType, &mergeMode); err != nil {
			return err
		}
		if (ticketType == domain.TicketSpike && transition.To != domain.StateDone) || (ticketType != domain.TicketSpike && ((mergeMode == domain.MergeGuarded && transition.To != domain.StateWaitingApproval) || (mergeMode == domain.MergeManual && transition.To != domain.StateWaitingManualMerge) || (mergeMode == domain.MergeAutonomous && transition.To != domain.StateBlocked) || (mergeMode != domain.MergeAutonomous && transition.To == domain.StateDone))) {
			return ErrEvidenceConflict
		}
		authority, _, reviewer, err := s.finalReviewerResult(ctx, conn, transition.Ref, version, transition.Fence)
		if err != nil || reviewer.Decision != phaseartifact.ReviewPass {
			return ErrEvidenceConflict
		}
		if ticketType == domain.TicketSpike {
			_, verification, err := s.loadHistoricalProviderAttemptResult(ctx, conn, authority.Verification.ProviderResult)
			if err != nil || verification.Verify == nil || verification.Verify.PrebuildOutcome != "report_ready" {
				return ErrEvidenceConflict
			}
			var mergeEffects, mergeIntents int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind='merge'`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&mergeEffects); err != nil || mergeEffects != 0 {
				return ErrEvidenceConflict
			}
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&mergeIntents); err != nil || mergeIntents != 0 {
				return ErrEvidenceConflict
			}
		}
		return nil
	})
}

// TransitionReviewRepair consumes an exact Reviewer repair result and its
// durable correction budget in the same transaction as the lifecycle move.
func (s *Store) TransitionReviewRepair(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.From != domain.StateReviewing || transition.Trigger != "review_repair" || (transition.To != domain.StateBuilding && transition.To != domain.StateVerifying) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		authority, result, reviewer, err := s.finalReviewerResult(ctx, conn, transition.Ref, version, transition.Fence)
		if err != nil || reviewer.Decision != phaseartifact.ReviewRepair || (reviewer.RepairOwner == "builder" && transition.To != domain.StateBuilding) || (reviewer.RepairOwner == "reviewer" && transition.To != domain.StateVerifying) || (reviewer.RepairOwner != "builder" && reviewer.RepairOwner != "reviewer") {
			return ErrEvidenceConflict
		}
		requestID := fmt.Sprintf("final-review/%d/%s", result.AttemptID, result.TypedSHA256)
		if _, err = s.consumeBudgetDuringTransition(ctx, conn, BudgetUse{Ref: transition.Ref, ExpectedVersion: version, Fence: transition.Fence, Kind: "correction", RequestID: requestID}); err != nil {
			return err
		}
		// This immutable row is the repair-cycle cutover.  It intentionally
		// names the exact final-review result and budget use that invalidated
		// the previous target-phase result, so a later worker/recovery cannot
		// mistake that old Builder or Verifier output for a replay candidate.
		reason := strings.Join(reviewer.Findings, "\n")
		if authority.Verification.Revision.Revision == 0 || !boundedText(reason, 2_000) {
			return ErrEvidenceConflict
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO final_review_repair_boundaries(channel,project_id,ticket_id,target_state,transition_ticket_version,reviewer_attempt_id,reviewer_attempt,reviewer_typed_sha256,prior_verification_revision,amendment_reason,requester,correction_budget_kind,correction_budget_request_id,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.To, version+1, result.AttemptID, result.Claim.Attempt, result.TypedSHA256, authority.Verification.Revision.Revision, reason, "final-reviewer", "correction", requestID, version, transition.Fence.LeaderEpoch, runner, time.Now().UTC().Format(time.RFC3339Nano))
		return err
	})
}

// TransitionReviewNeedsOperator records an exact reviewer escalation as a
// typed blocked state. It never treats provider prose as a resume authority.
func (s *Store) TransitionReviewNeedsOperator(ctx context.Context, transition Transition) (TransitionResult, error) {
	if transition.From != domain.StateReviewing || transition.Trigger != "typed_blocker" || transition.To != domain.StateBlocked || transition.ResumeState != domain.StateReviewing || transition.EventPayload != `{"code":"review_needs_operator"}` {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionWithEvidence(ctx, transition, func(ctx context.Context, conn *sql.Conn, version, runner uint64) error {
		_, _, reviewer, err := s.finalReviewerResult(ctx, conn, transition.Ref, version, transition.Fence)
		if err != nil || reviewer.Decision != phaseartifact.ReviewNeedsOperator || reviewer.RepairOwner != "operator" {
			return ErrEvidenceConflict
		}
		return nil
	})
}

func (s *Store) finalReviewerResult(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence) (FinalReviewAuthority, ProviderAttemptResult, phaseartifact.Reviewer, error) {
	authority, err := s.finalReviewAuthorityFrom(ctx, conn, ref, version, fence)
	if err != nil {
		return FinalReviewAuthority{}, ProviderAttemptResult{}, phaseartifact.Reviewer{}, ErrEvidenceConflict
	}
	var attemptID int64
	var attempt int
	if err := conn.QueryRowContext(ctx, `SELECT r.provider_attempt_id,r.attempt FROM provider_attempt_results r JOIN provider_attempts a ON a.id=r.provider_attempt_id WHERE r.channel=? AND r.project_id=? AND r.ticket_id=? AND r.phase='review' AND r.role='reviewer' AND a.state='completed' AND a.outcome='completed' ORDER BY r.provider_attempt_id DESC LIMIT 1`, ref.Channel, ref.Project, ref.Ticket).Scan(&attemptID, &attempt); err != nil || attemptID <= 0 || attempt <= 0 {
		return FinalReviewAuthority{}, ProviderAttemptResult{}, phaseartifact.Reviewer{}, ErrEvidenceConflict
	}
	key := ProviderAttemptResultKey{AttemptID: attemptID, Ref: ref, Phase: domain.PhaseReview, Attempt: attempt}
	result, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
	if err != nil || result.Claim.Role != "reviewer" || parsed.Reviewer == nil || parsed.Reviewer.ReviewedHead != authority.Candidate.Snapshot.HeadSHA || parsed.Reviewer.ProofDigest != authority.Candidate.Snapshot.ProofDigest || providerResultReachesFence(ctx, conn, key, result, version, fence) != nil {
		return FinalReviewAuthority{}, ProviderAttemptResult{}, phaseartifact.Reviewer{}, ErrEvidenceConflict
	}
	return authority, result, *parsed.Reviewer, nil
}

func (s *Store) transitionWithEvidence(ctx context.Context, transition Transition, check func(context.Context, *sql.Conn, uint64, uint64) error) (TransitionResult, error) {
	if transition.Ref.Validate() != nil || !transition.To.Valid() || !transition.From.Valid() || transition.Trigger == "" {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	blockedCode := ""
	if transition.To == domain.StateBlocked && transition.Trigger == "typed_blocker" {
		var blocker struct {
			Code string `json:"code"`
		}
		if json.Unmarshal([]byte(transition.EventPayload), &blocker) != nil || !boundedText(blocker.Code, 128) {
			return TransitionResult{}, ErrEvidenceConflict
		}
		blockedCode = blocker.Code
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var actual domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &version, &runner); err != nil {
			return err
		}
		if actual != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		if err := check(ctx, conn, version, runner); err != nil {
			return err
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=?,blocked_code=CASE WHEN ?<>'' THEN ? WHEN state='blocked' THEN '' ELSE blocked_code END,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state=? AND version=? AND runner_epoch=?`, transition.To, nullableState(transition.ResumeState), blockedCode, blockedCode, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, transition.From, version, runner)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, transition.Trigger, transition.From, transition.To, transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = version + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

// TransitionCandidate atomically consumes one exact candidate generation with
// the build transition.  A worker must not validate "latest" in one read and
// signal in another: a newer same-version candidate could otherwise replace
// the proof between those operations.
func (s *Store) TransitionCandidate(ctx context.Context, transition Transition, candidate domain.CandidateSnapshot) (TransitionResult, error) {
	if err := transition.Ref.Validate(); err != nil || transition.From != domain.StateBuilding || (transition.To != domain.StatePublishing && transition.To != domain.StateReviewing) || transition.Trigger != "phase_pass" || validateCandidate(candidate) != nil {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if transition.EventPayload == "" {
		transition.EventPayload = "{}"
	}
	if len(transition.EventPayload) > maxEvidenceJSON || !json.Valid([]byte(transition.EventPayload)) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	// A repair-gated transition must not revoke the builder fence before its
	// missing successor completion can be recorded. The same check is repeated
	// inside the write transaction below for race-safe authority.
	var repairPending int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.Generation).Scan(&repairPending); err != nil {
		return TransitionResult{}, err
	}
	if repairPending > 0 {
		var completed int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=? AND final_candidate_head_sha=? AND final_candidate_tree_sha=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.Generation, candidate.HeadSHA, candidate.TreeSHA).Scan(&completed); err != nil {
			return TransitionResult{}, err
		}
		if completed != 1 {
			return TransitionResult{}, ErrEvidenceConflict
		}
	}
	if err := s.DrainExternalMutations(ctx, transition.Ref); err != nil {
		return TransitionResult{}, err
	}
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner uint64
		var actual domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&actual, &version, &runner); err != nil {
			return err
		}
		if actual != transition.From || version != transition.ExpectedVersion {
			return ErrStaleFence
		}
		var ticketType domain.TicketType
		if err := conn.QueryRowContext(ctx, `SELECT ticket_type FROM tickets WHERE channel=? AND project_id=? AND id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&ticketType); err != nil || (ticketType == domain.TicketSpike) != (transition.To == domain.StateReviewing) {
			return ErrEvidenceConflict
		}
		if err := s.currentFence(ctx, conn, transition.Ref.Channel, version, runner, transition.Fence); err != nil {
			return err
		}
		var stored domain.CandidateSnapshot
		err := conn.QueryRowContext(ctx, `SELECT generation,base_sha,head_sha,tree_sha,source_digest,verification_intent_digest,proof_digest,command_policy_digest,builder_evidence_digest FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? ORDER BY generation DESC LIMIT 1`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&stored.Generation, &stored.BaseSHA, &stored.HeadSHA, &stored.TreeSHA, &stored.SourceDigest, &stored.VerificationIntentDigest, &stored.ProofDigest, &stored.CommandPolicyDigest, &stored.BuilderEvidenceDigest)
		if err != nil {
			return ErrEvidenceConflict
		}
		if stored != candidate {
			return ErrEvidenceConflict
		}
		var repairPending int
		if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.Generation).Scan(&repairPending); err != nil {
			return err
		}
		if repairPending > 0 {
			var completed int
			if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=? AND final_candidate_head_sha=? AND final_candidate_tree_sha=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.Generation, candidate.HeadSHA, candidate.TreeSHA).Scan(&completed); err != nil || completed != 1 {
				return ErrEvidenceConflict
			}
		}
		// Re-authenticate on the transaction connection.  A separate pooled
		// connection can observe a different snapshot (or block behind this
		// write), turning an otherwise valid same-fence candidate into a false
		// evidence conflict.  The transaction-scoped reader also keeps the
		// candidate proof and ticket fence in one authenticated snapshot.
		authenticated, err := s.latestCandidateFrom(ctx, conn, transition.Ref, true)
		if err != nil || authenticated.Snapshot != candidate || authenticated.TicketVersion != version || authenticated.Fence != transition.Fence || !candidatePolicyMatches(candidate.CommandPolicyDigest, authenticated.CommandBinding.PolicyDigest) {
			return ErrEvidenceConflict
		}
		var attemptID int64
		var attempt, actualAttempt int
		var parent string
		var phase, role, state, outcome string
		if err := conn.QueryRowContext(ctx, `SELECT b.provider_attempt_id,b.provider_attempt,b.commit_parent_oid,r.phase,a.role,a.state,a.outcome
			FROM candidate_result_bindings b JOIN provider_attempt_results r ON r.provider_attempt_id=b.provider_attempt_id JOIN provider_attempts a ON a.id=r.provider_attempt_id
			WHERE b.channel=? AND b.project_id=? AND b.ticket_id=? AND b.generation=? AND b.binding_ticket_version=? AND b.leader_epoch=? AND b.runner_epoch=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, candidate.Generation, version, transition.Fence.LeaderEpoch, transition.Fence.RunnerEpoch).Scan(&attemptID, &attempt, &parent, &phase, &role, &state, &outcome); err != nil || attemptID <= 0 || attempt <= 0 || phase != "build" || role != "builder" || state != "completed" || outcome != "completed" || !validOID(parent) {
			return ErrEvidenceConflict
		}
		if err := conn.QueryRowContext(ctx, `SELECT attempt FROM provider_attempts WHERE id=?`, attemptID).Scan(&actualAttempt); err != nil || actualAttempt != attempt {
			return ErrEvidenceConflict
		}
		if err := assertNewestBoundResult(ctx, conn, transition.Ref, domain.PhaseBuild, "builder", ProviderAttemptResultKey{AttemptID: attemptID, Ref: transition.Ref, Phase: domain.PhaseBuild, Attempt: attempt}); err != nil {
			return err
		}
		var source, base, intent, proof string
		if err := conn.QueryRowContext(ctx, `SELECT t.source_digest,w.base_sha,v.intent_digest,v.proof_digest FROM tickets t JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id JOIN verifications v ON v.channel=t.channel AND v.project_id=t.project_id AND v.ticket_id=t.id WHERE t.channel=? AND t.project_id=? AND t.id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&source, &base, &intent, &proof); err != nil {
			return ErrEvidenceConflict
		}
		var checkpoint string
		if err := conn.QueryRowContext(ctx, `SELECT r.checkpoint_id FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision WHERE v.channel=? AND v.project_id=? AND v.ticket_id=?`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&checkpoint); err != nil || checkpoint != parent {
			return ErrEvidenceConflict
		}
		if source != candidate.SourceDigest || base != candidate.BaseSHA || intent != candidate.VerificationIntentDigest || proof != candidate.ProofDigest {
			return ErrEvidenceConflict
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state=?,resume_state=?,version=version+1 WHERE channel=? AND project_id=? AND id=? AND state='building' AND version=? AND runner_epoch=?`, transition.To, nullableState(transition.ResumeState), transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version, runner)
		if err != nil {
			return err
		}
		if n, _ := updated.RowsAffected(); n != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, version+1, transition.Trigger, transition.From, transition.To, transition.EventPayload, time.Now().UTC().Format(time.RFC3339Nano))
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
	if err := s.DrainExternalMutations(ctx, ref); err != nil {
		return Ticket{}, err
	}
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
		result, err := conn.ExecContext(ctx, `INSERT INTO phase_runs(channel, project_id, ticket_id, phase, attempt, state, leader_epoch, runner_epoch, expected_ticket_version, completed_at, outcome) VALUES (?, ?, ?, ?, ?, 'completed', ?, ?, ?, ?, 'completed') ON CONFLICT(channel, project_id, ticket_id, phase, attempt) DO NOTHING`, ref.Channel, ref.Project, ref.Ticket, phase, attempt, fence.LeaderEpoch, fence.RunnerEpoch, expectedVersion, time.Now().UTC().Format(time.RFC3339Nano))
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
