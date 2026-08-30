package store

import (
	"context"
	"database/sql"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// ExternalMutationGate closes the gap between checking a durable effect and
// starting an external process. It deliberately holds no SQLite transaction
// while the process runs. Lifecycle callers drain it before changing a ticket
// or replacing a leader; a draining mark rejects old claims that arrive after
// the drain and allows only a newer ticket/leader/runner identity through.
type ExternalMutationGate struct {
	store   *Store
	gate    chan struct{}
	revoked map[domain.TicketRef]mutationRevocation
}

type mutationRevocation struct {
	version uint64
	leader  uint64
	runner  uint64
}

var _ contracts.ExternalMutationGuard = (*ExternalMutationGate)(nil)

func (s *Store) ExternalMutationGuard() contracts.ExternalMutationGuard { return s.mutations }

func (g *ExternalMutationGate) lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.gate:
		return nil
	}
}
func (g *ExternalMutationGate) unlock() { g.gate <- struct{}{} }

func (g *ExternalMutationGate) RunExternalMutation(ctx context.Context, claim domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
	if start == nil {
		return nil, ErrStaleFence
	}
	if err := g.lock(ctx); err != nil {
		return nil, err
	}
	defer g.unlock()
	if revoked, ok := g.revoked[claim.Ref]; ok && claim.TicketVersion <= revoked.version && claim.LeaderEpoch <= revoked.leader && claim.RunnerEpoch <= revoked.runner {
		return nil, ErrStaleFence
	}
	// This validation and the process start are protected by the same gate.
	// Invalidation waits for start (and for this synchronous external command)
	// rather than allowing a stale claim to launch in the forced gap.
	if err := g.store.ValidateExternalEffectClaim(ctx, claim); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return start(ctx)
}

// DrainExternalMutations must be called before any pause/take/cancel/leader
// replacement transition. It waits for an already-started command to drain,
// then fences every older identity for this ticket without holding SQLite open.
func (s *Store) DrainExternalMutations(ctx context.Context, ref domain.TicketRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	var version, runner, leader uint64
	err := s.db.QueryRowContext(ctx, `SELECT t.version, t.runner_epoch, d.leader_epoch
		FROM tickets t JOIN daemon_instances d ON d.channel=t.channel
		WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version, &runner, &leader)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	g.revoked[ref] = mutationRevocation{version: version, leader: leader, runner: runner}
	return nil
}

// DrainChannelExternalMutations is the leader-replacement counterpart of the
// ticket drain. New leader claims carry a higher leader epoch and may proceed;
// every old claimant remains fenced after the handoff completes.
func (s *Store) DrainChannelExternalMutations(ctx context.Context, channel domain.Channel) error {
	if !channel.Valid() {
		return ErrStaleFence
	}
	g := s.mutations
	if err := g.lock(ctx); err != nil {
		return err
	}
	defer g.unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT t.project_id, t.id, t.version, t.runner_epoch, d.leader_epoch
		FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=?`, channel)
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	defer rows.Close()
	for rows.Next() {
		var project domain.ProjectID
		var ticket domain.TicketID
		var version, runner, leader uint64
		if err := rows.Scan(&project, &ticket, &version, &runner, &leader); err != nil {
			return err
		}
		g.revoked[domain.TicketRef{Channel: channel, Project: project, Ticket: ticket}] = mutationRevocation{version: version, leader: leader, runner: runner}
	}
	return rows.Err()
}
