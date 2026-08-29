// Package testkit contains deterministic boundaries used by unit and
// integration tests. None of the fakes contact the network or read a user's
// credential stores.
package testkit

import (
	"context"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// FakeClock is a manually advanced clock. Sleep waits for Advance or for the
// caller's context to be cancelled; it never sleeps the test process.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters map[uint64]clockWaiter
	next    uint64
}

type clockWaiter struct {
	target time.Time
	ready  chan struct{}
}

// NewFakeClock returns a clock starting at start. The value is normalized to
// UTC so serialized observations are stable across machines.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start.UTC(), waiters: make(map[uint64]clockWaiter)}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set moves the clock to t. Moving backwards is allowed for tests that need
// to model a bad wall clock, but monotonic time is deliberately stripped.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t.UTC()
	c.releaseReadyLocked()
	c.mu.Unlock()
}

// Advance moves the clock forward by d. Negative durations are ignored to
// keep accidental test setup from waking a waiter unexpectedly.
func (c *FakeClock) Advance(d time.Duration) {
	if d < 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.releaseReadyLocked()
	c.mu.Unlock()
}

func (c *FakeClock) releaseReadyLocked() {
	for id, waiter := range c.waiters {
		if !c.now.Before(waiter.target) {
			close(waiter.ready)
			delete(c.waiters, id)
		}
	}
}

// Sleep implements the clock boundary expected by workflow code.
func (c *FakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	c.mu.Lock()
	if !c.now.Before(c.now.Add(d)) {
		c.mu.Unlock()
		return nil
	}
	c.next++
	id := c.next
	ready := make(chan struct{})
	c.waiters[id] = clockWaiter{target: c.now.Add(d), ready: ready}
	c.mu.Unlock()

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.waiters, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

// IDs generates stable ticket identifiers and branch suffixes. A new source
// intentionally starts at one for each channel; callers should include the
// channel in the TicketRef when asserting cross-channel isolation.
type IDs struct {
	mu   sync.Mutex
	next uint64
	seed uint64
}

func NewIDs(seed uint64) *IDs { return &IDs{next: 1, seed: seed} }

// Ticket returns a deterministic ticket reference such as SF-0001.
func (i *IDs) Ticket(channel domain.Channel, project string) domain.TicketRef {
	i.mu.Lock()
	n := i.next
	i.next++
	i.mu.Unlock()
	return domain.TicketRef{Channel: channel, Project: domain.ProjectID(project), Ticket: domain.TicketID(formatTicket(n))}
}

// NextTicketID returns the same identifier sequence without requiring a
// domain package in tests that only exercise serialization.
func (i *IDs) NextTicketID() string {
	i.mu.Lock()
	n := i.next
	i.next++
	i.mu.Unlock()
	return formatTicket(n)
}

// BranchSuffix returns a deterministic, non-empty suffix suitable for a
// channel/project/ticket branch namespace.
func (i *IDs) BranchSuffix() string {
	i.mu.Lock()
	n := i.next + i.seed
	i.next++
	i.mu.Unlock()
	return formatSuffix(n)
}

func formatTicket(n uint64) string { return "SF-" + formatSuffix(n) }
func formatSuffix(n uint64) string {
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return "00000000"
	}
	var buf [8]byte
	for pos := len(buf) - 1; pos >= 0; pos-- {
		buf[pos] = alphabet[n%uint64(len(alphabet))]
		n /= uint64(len(alphabet))
	}
	return string(buf[:])
}
