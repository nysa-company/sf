package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// TicketControlProof is one linearized control observation. It is produced
// while the external-mutation gate is held and a BEGIN IMMEDIATE transaction
// has fenced the current durable identity and inspected every writer/effect.
type TicketControlProof struct {
	Ticket                               Ticket
	Fence                                domain.Fence
	ProviderWriters                      int
	RepositoryCommandWriters             int
	GitMutationWriters                   int
	UnreconciledEffects                  int
	PublicationOrMergeEffects            int
	MergeIntents                         int
	OutstandingPublicationOrMergeEffects int
	OutstandingMergeIntents              int
}

// RuntimeRearmCapability is an opaque, one-use authorization produced only by
// RearmProof. Its fields are deliberately private and it can be redeemed only
// through Store.ActivateRearm, never handed directly to a runtime bundle.
type RuntimeRearmCapability struct {
	mu         sync.Mutex
	ref        domain.TicketRef
	version    uint64
	fence      domain.Fence
	issued     bool
	activating bool
}

type durableRuntimeControl struct {
	state      string
	generation uint64
	stop       mutationRevocation
	authority  mutationRevocation
}

func runtimeControlFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (durableRuntimeControl, error) {
	var value durableRuntimeControl
	err := conn.QueryRowContext(ctx, `SELECT state,generation,stop_version,stop_leader_epoch,stop_runner_epoch,authority_version,authority_leader_epoch,authority_runner_epoch
		FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(
		&value.state, &value.generation, &value.stop.version, &value.stop.leader, &value.stop.runner,
		&value.authority.version, &value.authority.leader, &value.authority.runner)
	if errors.Is(err, sql.ErrNoRows) {
		return durableRuntimeControl{}, ErrStaleFence
	}
	return value, err
}

// restoreRuntimeControls converts every interrupted rearm or active admission
// into a sealed generation and rebuilds the in-process process-start gate. A
// persisted open state has lost its owning scheduler on process crash and
// must never authorize a fresh Store writer after reopen.
func (s *Store) restoreRuntimeControls(ctx context.Context) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	return s.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET state='sealed',updated_at=? WHERE state IN ('armed','open')`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		rows, err := conn.QueryContext(ctx, `SELECT channel,project_id,ticket_id,authority_version,authority_leader_epoch,authority_runner_epoch FROM runtime_ticket_controls WHERE state='sealed'`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var ref domain.TicketRef
			var value mutationRevocation
			if err := rows.Scan(&ref.Channel, &ref.Project, &ref.Ticket, &value.version, &value.leader, &value.runner); err != nil {
				return err
			}
			s.mutations.latch(ref, value)
		}
		return rows.Err()
	})
}

func sealRuntimeControl(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, value mutationRevocation) error {
	_, err := conn.ExecContext(ctx, `INSERT INTO runtime_ticket_controls(
		channel,project_id,ticket_id,state,generation,stop_version,stop_leader_epoch,stop_runner_epoch,authority_version,authority_leader_epoch,authority_runner_epoch,updated_at)
		VALUES(?,?,?,'sealed',1,?,?,?,?,?,?,?)
		ON CONFLICT(channel,project_id,ticket_id) DO UPDATE SET state='sealed',generation=runtime_ticket_controls.generation+1,
		stop_version=excluded.stop_version,stop_leader_epoch=excluded.stop_leader_epoch,stop_runner_epoch=excluded.stop_runner_epoch,
		authority_version=excluded.authority_version,authority_leader_epoch=excluded.authority_leader_epoch,authority_runner_epoch=excluded.authority_runner_epoch,updated_at=excluded.updated_at`,
		ref.Channel, ref.Project, ref.Ticket, value.version, value.leader, value.runner, value.version, value.leader, value.runner, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// RuntimeAdmissionCapability is minted only while ActivateRearm holds Store's
// mutation gate. It is the narrow object a runtime control bundle may accept
// to install the exact local admission token.
type RuntimeAdmissionCapability struct {
	mu       sync.Mutex
	ref      domain.TicketRef
	version  uint64
	fence    domain.Fence
	issued   bool
	consumed bool
	opening  bool
	opened   bool
	open     func(context.Context) error
	suspend  func(context.Context) (bool, error)
	seal     func(context.Context) error
}

func (c *RuntimeAdmissionCapability) ConsumeRuntimeAdmission() (domain.TicketRef, uint64, domain.Fence, bool) {
	if c == nil {
		return domain.TicketRef{}, 0, domain.Fence{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.issued {
		return domain.TicketRef{}, 0, domain.Fence{}, false
	}
	c.issued = false
	c.consumed = true
	return c.ref, c.version, c.fence, true
}

func (c *RuntimeAdmissionCapability) wasConsumed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumed
}

func (c *RuntimeAdmissionCapability) discard() {
	c.mu.Lock()
	c.issued = false
	c.open = nil
	c.suspend = nil
	c.seal = nil
	c.mu.Unlock()
}

// OpenStoreAdmission is intentionally useful only after ConsumeRuntimeAdmission
// has transferred the exact identity into the scheduler. The scheduler calls
// it from its first matching Begin while its per-ticket admission lock is
// held. Until then the Store hard latch rejects every authority tuple.
func (c *RuntimeAdmissionCapability) OpenStoreAdmission(ctx context.Context) error {
	if c == nil {
		return ErrStaleFence
	}
	c.mu.Lock()
	if !c.consumed || c.opened || c.opening || c.open == nil {
		c.mu.Unlock()
		return ErrStaleFence
	}
	c.opening = true
	open := c.open
	c.mu.Unlock()
	err := open(ctx)
	c.mu.Lock()
	c.opening = false
	if err == nil {
		c.opened = true
	}
	c.mu.Unlock()
	return err
}

// SuspendStoreAdmission compensates a Begin cancelled before it committed to
// runtime activity. It restores armed state only for the same exact authority.
// A concurrent operator seal stays sealed and reports non-retryable.
func (c *RuntimeAdmissionCapability) SuspendStoreAdmission(ctx context.Context) (bool, error) {
	if c == nil {
		return false, ErrStaleFence
	}
	c.mu.Lock()
	if !c.consumed || !c.opened || c.opening || c.suspend == nil {
		c.mu.Unlock()
		return false, ErrStaleFence
	}
	c.opening = true
	suspend := c.suspend
	c.mu.Unlock()
	retryable, err := suspend(ctx)
	c.mu.Lock()
	c.opening = false
	if err == nil {
		c.opened = false
	}
	c.mu.Unlock()
	return retryable, err
}

// SealStoreAdmission permanently seals an already-admitted activity. An
// earlier Controller seal is idempotent only for the identical authority.
func (c *RuntimeAdmissionCapability) SealStoreAdmission(ctx context.Context) error {
	if c == nil {
		return ErrStaleFence
	}
	c.mu.Lock()
	if !c.consumed || !c.opened || c.opening || c.seal == nil {
		c.mu.Unlock()
		return ErrStaleFence
	}
	c.opening = true
	seal := c.seal
	c.mu.Unlock()
	err := seal(ctx)
	c.mu.Lock()
	c.opening = false
	if err == nil {
		c.opened = false
	}
	c.mu.Unlock()
	return err
}

// RuntimeRetirementCapability is the terminal-only counterpart. It is issued
// after a linearized terminal proof and cannot authorize any work.
type RuntimeRetirementCapability struct {
	mu          sync.Mutex
	ref         domain.TicketRef
	issued      bool
	retiring    bool
	retireStore func(context.Context) error
}

// ConsumeRuntimeRetirement returns the terminal authorization exactly once.
// It is intentionally opaque: only Store can create a valid capability.
func (c *RuntimeRetirementCapability) ConsumeRuntimeRetirement() (domain.TicketRef, bool) {
	if c == nil {
		return domain.TicketRef{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.issued {
		return domain.TicketRef{}, false
	}
	c.issued = false
	return c.ref, true
}

// RetireRuntime applies the runtime side first, then removes Store's terminal
// latch and watermark. Both operations are retry-safe: a failed Store cleanup
// keeps the capability live and cannot create an admission route.
func (c *RuntimeRetirementCapability) RetireRuntime(ctx context.Context, retire func(domain.TicketRef) error) error {
	if c == nil || retire == nil {
		return ErrStaleFence
	}
	c.mu.Lock()
	if !c.issued || c.retiring || c.retireStore == nil {
		c.mu.Unlock()
		return ErrStaleFence
	}
	c.retiring = true
	ref, retireStore := c.ref, c.retireStore
	c.mu.Unlock()
	fail := func(err error) error {
		c.mu.Lock()
		c.retiring = false
		c.mu.Unlock()
		return err
	}
	if err := retire(ref); err != nil {
		return fail(err)
	}
	if err := retireStore(ctx); err != nil {
		return fail(err)
	}
	c.mu.Lock()
	c.issued = false
	c.retiring = false
	c.mu.Unlock()
	return nil
}

func (p TicketControlProof) Drained() bool {
	// Historical pre-publication effects may be excluded only when they are
	// settled (confirmed/failed) or merely planned. The allowlist is limited to
	// local worktree/commit and repository-command records; executing or
	// uncertain rows are always counted above. Publication/merge rows and merge
	// intents are counted whenever outstanding, while their historical presence
	// still makes StrictlyPrePublication fail closed below.
	return p.ProviderWriters == 0 && p.RepositoryCommandWriters == 0 && p.GitMutationWriters == 0 && p.UnreconciledEffects == 0 && p.OutstandingPublicationOrMergeEffects == 0 && p.OutstandingMergeIntents == 0
}

// StrictlyPrePublication is intentionally an allowlist. Any unknown effect
// may be a future publication path, so it fails closed.
func (p TicketControlProof) StrictlyPrePublication() bool {
	if p.PublicationOrMergeEffects != 0 || p.MergeIntents != 0 {
		return false
	}
	switch p.Ticket.State {
	case domain.StateQueued:
		return true
	case domain.StateStopping, domain.StateCancelling, domain.StatePaused, domain.StateBlocked:
		return prePublicationState(p.Ticket.ResumeState)
	default:
		return prePublicationState(p.Ticket.State)
	}
}

func prePublicationState(state domain.State) bool {
	return state == domain.StatePlanning || state == domain.StateVerifying || state == domain.StateBuilding
}

// SealRuntimeControl durably closes a ticket before any runtime cancellation
// is requested.  It intentionally performs no drain count: callers must
// release Store's gate, join the runtime, then obtain ControlProof.
func (s *Store) SealRuntimeControl(ctx context.Context, ref domain.TicketRef) error {
	if s == nil || s.mutations == nil || ref.Validate() != nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	var sealed mutationRevocation
	err := s.write(ctx, func(conn *sql.Conn) error {
		var version, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner, &leader); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		value := mutationRevocation{version: version, leader: leader, runner: runner}
		if err := sealRuntimeControl(ctx, conn, ref, value); err != nil {
			return err
		}
		sealed = value
		return nil
	})
	if err == nil {
		g.latch(ref, sealed)
	}
	return err
}

// StoppedRuntimeTicket reconstructs the stopped tuple from durable control
// authority. It is used after a Controller restart; Controller.tickets is an
// optimization, never the source of rearm authority.
func (s *Store) StoppedRuntimeTicket(ctx context.Context, ref domain.TicketRef) (Ticket, error) {
	if s == nil || ref.Validate() != nil {
		return Ticket{}, ErrStaleFence
	}
	var result Ticket
	err := s.db.QueryRowContext(ctx, `SELECT stop_version,stop_runner_epoch FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=? AND state='sealed'`, ref.Channel, ref.Project, ref.Ticket).Scan(&result.Version, &result.RunnerEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return Ticket{}, ErrStaleFence
	}
	if err != nil {
		return Ticket{}, normalizeBusy(ctx, err)
	}
	result.Ref = ref
	return result, nil
}

// MergeObservationPrePublication is a read-only classification used before a
// merge observer runs. It deliberately creates no control row or latch.
func (s *Store) MergeObservationPrePublication(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if s == nil || ref.Validate() != nil {
		return false, ErrStaleFence
	}
	var ticket Ticket
	var resume sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT state,resume_state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ticket.State, &resume, &ticket.Version, &ticket.RunnerEpoch)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	ticket.Ref = ref
	if resume.Valid {
		ticket.ResumeState = domain.State(resume.String)
	}
	var publication, intents int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command')`, ref.Channel, ref.Project, ref.Ticket).Scan(&publication); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&intents); err != nil {
		return false, normalizeBusy(ctx, err)
	}
	return (TicketControlProof{Ticket: ticket, PublicationOrMergeEffects: publication, MergeIntents: intents}).StrictlyPrePublication(), nil
}

// ControlProof atomically revokes this ticket's current identity and proves
// whether writers/effects are drained. Callers must not compose a read
// snapshot with a later drain. Its memory revocation is installed inside the
// IMMEDIATE transaction before counts are read, which closes the post-proof
// start gap while the mutation gate remains held.
func (s *Store) ControlProof(ctx context.Context, ref domain.TicketRef) (TicketControlProof, error) {
	if err := ref.Validate(); err != nil {
		return TicketControlProof{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if g == nil {
		return TicketControlProof{}, ErrStaleFence
	}
	if err := g.lock(ctx); err != nil {
		return TicketControlProof{}, err
	}
	defer g.unlock()
	proof, leader, err := s.controlProof(ctx, ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		value := mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch}
		return sealRuntimeControl(txCtx, conn, ref, value)
	}, nil)
	if err != nil {
		return TicketControlProof{}, err
	}
	// The gate stayed held through COMMIT, so mirroring after commit cannot
	// leave a volatile latch behind when a durable seal rolls back.
	g.latch(ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	return proof, nil
}

// RearmProof checks a newer active pre-publication identity while holding the
// mutation gate and a BEGIN IMMEDIATE transaction. It deliberately retains
// the stopped revocation; the controller turns this exact proof into one
// runtime admission token while holding its control mutex.
func (s *Store) RearmProof(ctx context.Context, ref domain.TicketRef, stopped Ticket) (*RuntimeRearmCapability, error) {
	if err := ref.Validate(); err != nil || stopped.Ref != ref || stopped.Version == 0 || stopped.RunnerEpoch == 0 {
		return nil, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if g == nil {
		return nil, ErrStaleFence
	}
	if err := g.lock(ctx); err != nil {
		return nil, err
	}
	defer g.unlock()
	proof, leader, err := s.controlProof(ctx, ref, g, func(txCtx context.Context, conn *sql.Conn, proof TicketControlProof, leader uint64) error {
		_, latched := g.control(ref)
		// Memory is only the process-start serialization gate. The durable row
		// is the authority that survives a Store reopen and authenticates the
		// exact stopped generation used by this rearm.
		control, err := runtimeControlFrom(txCtx, conn, ref)
		if err != nil || control.state != "sealed" || control.stop.version != stopped.Version || control.stop.runner != stopped.RunnerEpoch {
			return ErrStaleFence
		}
		// Authenticate the stop record against the exact current hard latch.
		// A leader-only successor is not a rearm: it has no newer ticket
		// identity and remains stopped until a real lifecycle transition.
		if !latched || (proof.Ticket.Version <= stopped.Version && proof.Ticket.RunnerEpoch == stopped.RunnerEpoch) {
			return ErrStaleFence
		}
		return nil
	}, func(_ context.Context, _ *sql.Conn, proof TicketControlProof, leader uint64) error {
		if !proof.Drained() || !proof.StrictlyPrePublication() || !prePublicationState(proof.Ticket.State) {
			return ErrControlNotDrained
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	g.latch(ref, mutationRevocation{version: proof.Ticket.Version, leader: leader, runner: proof.Ticket.RunnerEpoch})
	return &RuntimeRearmCapability{ref: ref, version: proof.Ticket.Version, fence: proof.Fence, issued: true}, nil
}

// ActivateRearm is the only proof-to-runtime handoff. It holds the mutation
// gate, consumes the opaque proof, and checks the newer durable identity.
// It then releases the gate while the hard latch remains closed to install
// runtime state. Only the scheduler's matching Begin can open that latch.
// A transition or direct Store writer in either ordering fails closed instead
// of inheriting the rearm authorization.
func (s *Store) ActivateRearm(ctx context.Context, capability *RuntimeRearmCapability, install func(*RuntimeAdmissionCapability) error) error {
	if capability == nil || install == nil || s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	capability.mu.Lock()
	if !capability.issued || capability.activating {
		capability.mu.Unlock()
		g.unlock()
		return ErrStaleFence
	}
	capability.activating = true
	ref, version, fence := capability.ref, capability.version, capability.fence
	capability.mu.Unlock()
	fail := func(err error) error {
		capability.mu.Lock()
		capability.activating = false
		capability.mu.Unlock()
		return err
	}

	current, fenced := g.control(ref)
	if !fenced || current != (mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}) {
		g.unlock()
		return fail(ErrStaleFence)
	}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		var currentVersion, runner, leader uint64
		if err := conn.QueryRowContext(ctx, `SELECT t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&currentVersion, &runner, &leader); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if currentVersion != version || runner != fence.RunnerEpoch || leader != fence.LeaderEpoch {
			return ErrStaleFence
		}
		control, err := runtimeControlFrom(ctx, conn, ref)
		if err != nil || control.state != "sealed" {
			return ErrStaleFence
		}
		updated, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET state='armed',authority_version=?,authority_leader_epoch=?,authority_runner_epoch=?,updated_at=?
			WHERE channel=? AND project_id=? AND ticket_id=? AND state='sealed' AND generation=?`, version, fence.LeaderEpoch, fence.RunnerEpoch, time.Now().UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket, control.generation)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		return nil
	}); err != nil {
		g.unlock()
		return fail(err)
	}
	// Do not hold Store's gate while installing scheduler state. The hard latch
	// remains closed, and the scheduler's first exact Begin re-validates this
	// tuple before it can release the latch. This gives every path one lock
	// order and avoids Store-gate -> admission versus admission -> Store-gate
	// inversion across tickets.
	g.unlock()
	admission := &RuntimeAdmissionCapability{ref: ref, version: version, fence: fence, issued: true}
	admission.open = func(openCtx context.Context) error {
		return s.openRuntimeAdmission(openCtx, ref, version, fence)
	}
	admission.suspend = func(suspendCtx context.Context) (bool, error) {
		return s.suspendRuntimeAdmission(suspendCtx, ref, version, fence)
	}
	admission.seal = func(sealCtx context.Context) error {
		return s.sealRuntimeAdmission(sealCtx, ref, version, fence)
	}
	if err := install(admission); err != nil {
		sealCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		sealErr := s.sealRuntimeAdmission(sealCtx, ref, version, fence)
		cancel()
		if sealErr != nil {
			return fail(errors.Join(err, sealErr))
		}
		return fail(err)
	}
	if !admission.wasConsumed() {
		admission.discard()
		sealCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		sealErr := s.sealRuntimeAdmission(sealCtx, ref, version, fence)
		cancel()
		if sealErr != nil {
			return fail(sealErr)
		}
		return fail(ErrStaleFence)
	}
	capability.mu.Lock()
	capability.issued = false
	capability.activating = false
	capability.mu.Unlock()
	return nil
}

// openRuntimeAdmission is reachable only through the consumed opaque
// capability held by the scheduler. It validates the current durable tuple
// under the external gate, then opens only that exact hard latch.
func (s *Store) openRuntimeAdmission(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	current, latched := g.control(ref)
	expected := mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}
	if !latched || current != expected {
		return ErrStaleFence
	}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		var gotVersion, gotRunner, gotLeader uint64
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &gotVersion, &gotRunner, &gotLeader); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if !prePublicationState(state) || gotVersion != version || gotRunner != fence.RunnerEpoch || gotLeader != fence.LeaderEpoch {
			return ErrStaleFence
		}
		updated, err := conn.ExecContext(ctx, `UPDATE runtime_ticket_controls SET state='open',updated_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='armed' AND authority_version=? AND authority_leader_epoch=? AND authority_runner_epoch=?`, time.Now().UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		return nil
	}); err != nil {
		return err
	}
	if !g.openControl(ref, expected) {
		return ErrStaleFence
	}
	return nil
}

// suspendRuntimeAdmission returns a pre-commit cancellation to armed state
// only while its exact authority remains current. A matching permanent seal
// wins the race and is reported as non-retryable; mismatched seals fail.
func (s *Store) suspendRuntimeAdmission(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (bool, error) {
	if s == nil || s.mutations == nil {
		return false, ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	// Durable seal comes first. The memory latch only mirrors that committed
	// authority for an already-running process; it is never the proof itself.
	expected := mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := g.lock(closeCtx); err != nil {
		return false, err
	}
	defer g.unlock()
	retryable := false
	err := s.write(closeCtx, func(conn *sql.Conn) error {
		control, err := runtimeControlFrom(closeCtx, conn, ref)
		if err != nil {
			return err
		}
		if control.authority != expected {
			return ErrStaleFence
		}
		if control.state == "sealed" {
			return nil
		}
		if control.state != "open" {
			return ErrStaleFence
		}
		updated, err := conn.ExecContext(closeCtx, `UPDATE runtime_ticket_controls SET state='armed',updated_at=? WHERE channel=? AND project_id=? AND ticket_id=? AND state='open' AND authority_version=? AND authority_leader_epoch=? AND authority_runner_epoch=?`, time.Now().UTC().Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket, version, fence.LeaderEpoch, fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if changed, _ := updated.RowsAffected(); changed != 1 {
			return ErrStaleFence
		}
		retryable = true
		return nil
	})
	if err != nil {
		return false, normalizeBusy(closeCtx, err)
	}
	g.latch(ref, expected)
	return retryable, nil
}

// sealRuntimeAdmission permanently seals an exact active or pending
// authority. Controller's Store-first seal is idempotent for that exact
// authority. It also recognizes the one immediate successor created by the
// normative stopping/cancelling transition: that successor was already sealed
// atomically with runner invalidation, so the old activity may be cancelled
// and joined without ever modifying the newer authority.
func (s *Store) sealRuntimeAdmission(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	expected := mutationRevocation{version: version, leader: fence.LeaderEpoch, runner: fence.RunnerEpoch}
	latched := expected
	sealCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := g.lock(sealCtx); err != nil {
		return err
	}
	defer g.unlock()
	err := s.write(sealCtx, func(conn *sql.Conn) error {
		control, err := runtimeControlFrom(sealCtx, conn, ref)
		if err != nil {
			return err
		}
		if control.authority != expected {
			var state domain.State
			var version, runner, leader uint64
			if err := conn.QueryRowContext(sealCtx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &version, &runner, &leader); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return ErrNotFound
				}
				return err
			}
			// This is not a second seal. The Store's atomic control transition
			// already sealed Y before Controller asked the runtime to stop X.
			// Accept no other mismatch: preserving the exact +1 tuple and the
			// stopping/cancelling state prevents an old activity from closing a
			// different or later owner.
			if control.state == "sealed" && (state == domain.StateStopping || state == domain.StateCancelling) {
				if control.authority.version == expected.version+1 && control.authority.runner == expected.runner+1 && control.authority.leader == expected.leader && version == control.authority.version && runner == control.authority.runner && leader == control.authority.leader {
					latched = control.authority
					return nil
				}
			}
			return ErrStaleFence
		}
		if control.state == "sealed" {
			return nil
		}
		if control.state != "open" && control.state != "armed" {
			return ErrStaleFence
		}
		return sealRuntimeControl(sealCtx, conn, ref, expected)
	})
	if err != nil {
		return normalizeBusy(sealCtx, err)
	}
	g.latch(ref, latched)
	return nil
}

// TerminalControlProof safely clears a retained in-memory stop record only
// after Store proves terminal state and no writer/publication ambiguity.
func (s *Store) TerminalControlProof(ctx context.Context, ref domain.TicketRef) (*RuntimeRetirementCapability, error) {
	proof, err := s.ControlProof(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !proof.Ticket.State.Terminal() || !proof.Drained() {
		return nil, ErrControlNotDrained
	}
	value := mutationRevocation{version: proof.Ticket.Version, leader: proof.Fence.LeaderEpoch, runner: proof.Ticket.RunnerEpoch}
	return &RuntimeRetirementCapability{ref: ref, issued: true, retireStore: func(retireCtx context.Context) error {
		return s.retireRuntimeControl(retireCtx, ref, value)
	}}, nil
}

func (s *Store) retireRuntimeControl(ctx context.Context, ref domain.TicketRef, value mutationRevocation) error {
	if s == nil || s.mutations == nil {
		return ErrStaleFence
	}
	if ctx == nil {
		ctx = context.Background()
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	if current, ok := g.control(ref); !ok || current != value {
		return ErrStaleFence
	}
	if err := s.write(ctx, func(conn *sql.Conn) error {
		var state domain.State
		if err := conn.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state); err != nil {
			return err
		}
		if !state.Terminal() {
			return ErrStaleFence
		}
		control, err := runtimeControlFrom(ctx, conn, ref)
		if err != nil || control.authority != value {
			return ErrStaleFence
		}
		if _, err := conn.ExecContext(ctx, `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=? AND state='sealed'`, ref.Channel, ref.Project, ref.Ticket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	if !g.retireControl(ref, value) {
		return ErrStaleFence
	}
	return nil
}

func (s *Store) controlProof(ctx context.Context, ref domain.TicketRef, gate *ExternalMutationGate, beforeCounts, afterCounts func(context.Context, *sql.Conn, TicketControlProof, uint64) error) (TicketControlProof, uint64, error) {
	var proof TicketControlProof
	var leader uint64
	err := s.write(ctx, func(conn *sql.Conn) error {
		var resume sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT state,resume_state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&proof.Ticket.State, &resume, &proof.Ticket.Version, &proof.Ticket.RunnerEpoch); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		proof.Ticket.Ref = ref
		if resume.Valid {
			proof.Ticket.ResumeState = domain.State(resume.String)
		}
		if err := conn.QueryRowContext(ctx, `SELECT leader_epoch FROM daemon_instances WHERE channel=?`, ref.Channel).Scan(&leader); err != nil {
			return err
		}
		proof.Fence = domain.Fence{LeaderEpoch: leader, RunnerEpoch: proof.Ticket.RunnerEpoch}
		if beforeCounts != nil {
			// This store-side fence is deliberately installed while this BEGIN
			// IMMEDIATE transaction owns SQLite's writer slot. An admission that
			// committed before us is visible to the following counts; one that
			// starts after our commit observes this fence and is rejected.
			if err := beforeCounts(ctx, conn, proof, leader); err != nil {
				return err
			}
		}
		// Only this three-kind allowlist can be historical yet non-outstanding:
		// planned means no process began, confirmed/failed are terminal durable
		// observations, and executing/uncertain are included separately. Every
		// other kind is future-publication-sensitive and therefore remains in
		// both the historical and outstanding publication/merge checks.
		counts := []struct {
			query string
			out   *int
		}{
			{`SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, &proof.ProviderWriters},
			{`SELECT COUNT(*) FROM repository_command_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, &proof.RepositoryCommandWriters},
			{`SELECT COUNT(*) FROM git_mutation_leases WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('active','quarantined')`, &proof.GitMutationWriters},
			{`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('executing','uncertain')`, &proof.UnreconciledEffects},
			{`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command')`, &proof.PublicationOrMergeEffects},
			{`SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?`, &proof.MergeIntents},
			{`SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command') AND state IN ('planned','executing','uncertain')`, &proof.OutstandingPublicationOrMergeEffects},
			{`SELECT COUNT(*) FROM merge_intents m JOIN effects e ON e.semantic_key=m.semantic_key WHERE m.channel=? AND m.project_id=? AND m.ticket_id=? AND e.state IN ('planned','executing','uncertain')`, &proof.OutstandingMergeIntents},
		}
		for _, count := range counts {
			if err := conn.QueryRowContext(ctx, count.query, ref.Channel, ref.Project, ref.Ticket).Scan(count.out); err != nil {
				return err
			}
		}
		if afterCounts != nil {
			if err := afterCounts(ctx, conn, proof, leader); err != nil {
				return err
			}
		}
		if s.controlProofHook != nil {
			s.controlProofHook()
		}
		return nil
	})
	return proof, leader, err
}
