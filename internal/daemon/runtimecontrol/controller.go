// Package runtimecontrol adapts the ticket-scoped workflow runtime to the
// daemon's narrow operator-control interface.  It intentionally performs no
// daemon composition and never calls GitHub.
package runtimecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowruntime"
)

var ErrMergeProofRequired = errors.New("merge observation requires an authenticated observer after publication begins")

// MergeObserver must be an authenticated, read-only observation boundary.
// It is deliberately injected: this package does not invent GitHub access.
type MergeObserver interface {
	MergeObserved(context.Context, domain.TicketRef) (bool, error)
}

type MergeObserverFunc func(context.Context, domain.TicketRef) (bool, error)

func (f MergeObserverFunc) MergeObserved(ctx context.Context, ref domain.TicketRef) (bool, error) {
	return f(ctx, ref)
}

// Controller satisfies daemon.RuntimeController structurally.  It first
// stops exactly the active target tick, then fences external mutation starts,
// and only reports drained after Store proves the ticket has neither durable
// writers nor executing/uncertain effects.
type Controller struct {
	store    *store.Store
	runtime  *workflowruntime.ControlBundle
	observer MergeObserver
	git      *git.Runner

	mu      sync.Mutex // protects tickets only; never held across Store/runtime/observer I/O.
	tickets map[domain.TicketRef]*ticketControl
}

type ticketControl struct {
	mu      sync.Mutex
	stopped store.Ticket
	hasStop bool
	refs    int // protected by Controller.mu
}

func New(database *store.Store, runtime *workflowruntime.ControlBundle, observer MergeObserver, runners ...git.Runner) (*Controller, error) {
	if database == nil || runtime == nil || !runtime.Valid() {
		return nil, errors.New("store and ticket runtime are required")
	}
	if len(runners) > 1 {
		return nil, errors.New("at most one takeover Git inspector is allowed")
	}
	controller := &Controller{store: database, runtime: runtime, observer: observer, tickets: make(map[domain.TicketRef]*ticketControl)}
	if len(runners) == 1 {
		controller.git = &runners[0]
	}
	return controller, nil
}

// InspectTakeover authenticates the retained worktree before it is shown to
// an operator or used to select the unchanged-resume path. A dirty checkout
// is classified, not rejected: the daemon can explain that its source is not
// yet durably adopted without ever treating it as a valid candidate.
func (c *Controller) InspectTakeover(ctx context.Context, ref domain.TicketRef) (contracts.TakeoverInspection, error) {
	if c == nil || c.store == nil {
		return contracts.TakeoverInspection{}, errors.New("takeover inspection is unavailable")
	}
	registered, err := c.store.Worktree(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return contracts.TakeoverInspection{Clean: true, ChangeKind: "no_worktree"}, nil
	}
	if err != nil || c.git == nil || registered.State != "registered" {
		if err != nil {
			return contracts.TakeoverInspection{}, err
		}
		return contracts.TakeoverInspection{}, errors.New("authenticated takeover Git inspection is unavailable")
	}
	var identity git.Identity
	if err := json.Unmarshal(registered.IdentityJSON, &identity); err != nil {
		return contracts.TakeoverInspection{}, fmt.Errorf("registered worktree identity is malformed: %w", err)
	}
	canonical, err := json.Marshal(identity)
	if err != nil || !bytes.Equal(canonical, registered.IdentityJSON) || identity.Worktree != registered.Path || identity.HeadRef != registered.Branch || identity.BaseHead != registered.BaseSHA {
		return contracts.TakeoverInspection{}, errors.New("registered worktree identity is not canonical")
	}
	worktree := git.Worktree{Path: registered.Path, Branch: registered.Branch, Identity: identity}
	inspection := contracts.TakeoverInspection{Registered: true, Path: registered.Path, Branch: registered.Branch, Repository: identity.Repository, BaseSHA: identity.BaseHead}
	changes, err := c.git.InspectWorktreeChanges(ctx, worktree)
	if err != nil {
		return contracts.TakeoverInspection{}, err
	}
	inspection.HeadSHA = changes.Head
	// The registration head is valid before a checkpoint exists. A candidate
	// head is valid after it has been recorded. During building, the current
	// verification checkpoint is the only head from which a dirty source diff
	// can safely be handed back to the Builder.
	allowedHeads := map[string]struct{}{registered.HeadSHA: {}}
	if candidate, candidateErr := c.store.RecoverableCandidate(ctx, ref); candidateErr == nil {
		allowedHeads[candidate.Snapshot.HeadSHA] = struct{}{}
	} else if !errors.Is(candidateErr, store.ErrNotFound) {
		return contracts.TakeoverInspection{}, candidateErr
	}
	verification, verificationErr := c.store.RecoverableVerification(ctx, ref)
	if verificationErr == nil {
		allowedHeads[verification.Checkpoint.CommitOID] = struct{}{}
	} else if !errors.Is(verificationErr, store.ErrNotFound) {
		return contracts.TakeoverInspection{}, verificationErr
	}
	if _, allowed := allowedHeads[changes.Head]; !allowed {
		inspection.ChangeKind = "unadopted_commit"
		return inspection, nil
	}
	if len(changes.Paths) == 0 {
		inspection.Clean = true
		inspection.ChangeKind = "none"
		return inspection, nil
	}
	inspection.ChangedFiles = changes.Paths
	if verificationErr != nil || changes.Head != verification.Checkpoint.CommitOID {
		inspection.ChangeKind = "unadopted_changes"
		return inspection, nil
	}
	if touchesOwnedFiles(changes.Paths, verification.Revision.OwnedFiles) {
		inspection.ChangeKind = "verification_changes"
		return inspection, nil
	}
	plan, planErr := c.store.Plan(ctx, ref)
	if planErr != nil || !allWithinPlan(changes.Paths, plan.Document.Planner.Paths) {
		inspection.ChangeKind = "source_out_of_scope"
		return inspection, nil
	}
	inspection.ChangeKind = "source_changes"
	inspection.SourceResumable = true
	return inspection, nil
}

func takeoverPathMatches(path, prefix string) bool {
	prefix = strings.Trim(prefix, "/")
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func touchesOwnedFiles(paths, owned []string) bool {
	for _, path := range paths {
		for _, item := range owned {
			if takeoverPathMatches(path, item) || takeoverPathMatches(item, path) {
				return true
			}
		}
	}
	return false
}

func allWithinPlan(paths, allowed []string) bool {
	if len(allowed) == 0 {
		return false
	}
	for _, path := range paths {
		matched := false
		for _, prefix := range allowed {
			if takeoverPathMatches(path, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func (c *Controller) acquireTicket(ref domain.TicketRef) *ticketControl {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.tickets[ref]
	if entry == nil {
		entry = &ticketControl{}
		c.tickets[ref] = entry
	}
	entry.refs++
	return entry
}

func (c *Controller) releaseTicket(ref domain.TicketRef, entry *ticketControl) {
	entry.mu.Lock()
	retained := entry.hasStop
	entry.mu.Unlock()
	c.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	if c.tickets[ref] == entry && entry.refs == 0 && !retained {
		delete(c.tickets, ref)
	}
	c.mu.Unlock()
}

func (c *Controller) Drain(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if c == nil || c.store == nil || c.runtime == nil {
		return false, errors.New("runtime controller is not configured")
	}
	entry := c.acquireTicket(ref)
	defer c.releaseTicket(ref, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	// The Store seal is the cancellation authority. It commits before runtime
	// cancellation starts and releases the Store gate before Drain joins, so a
	// Store/admission/runtime lock cycle cannot form.
	if err := c.store.SealRuntimeControl(ctx, ref); err != nil {
		return false, err
	}
	if err := c.runtime.Drain(ctx, ref); err != nil {
		return false, err
	}
	proof, err := c.store.ControlProof(ctx, ref)
	if err != nil {
		return false, err
	}
	drained := proof.Drained()
	if drained {
		entry.stopped, entry.hasStop = proof.Ticket, true
	}
	return drained, nil
}

// Rearm is the only supported way to clear the runtime's ticket stop latch.
// It re-reads Store after a completed control/resume transition and accepts
// only a newer durable active pre-publication identity with no writer or
// uncertain effect.  A caller cannot rearm a ticket merely by retaining an
// old Runtime pointer.
func (c *Controller) Rearm(ctx context.Context, ref domain.TicketRef) error {
	if c == nil || c.store == nil || c.runtime == nil {
		return errors.New("runtime controller is not configured")
	}
	entry := c.acquireTicket(ref)
	defer c.releaseTicket(ref, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	stopped, found := entry.stopped, entry.hasStop
	if !found {
		var err error
		stopped, err = c.store.StoppedRuntimeTicket(ctx, ref)
		if err != nil {
			return errors.New("ticket has not completed a controller drain")
		}
		entry.stopped, entry.hasStop = stopped, true
	}
	current, err := c.store.Ticket(ctx, ref)
	if err != nil {
		return err
	}
	var capability *store.RuntimeRearmCapability
	if current.State == domain.StatePublishing || current.State == domain.StateWaitingCI || current.State == domain.StateReviewing || current.State == domain.StateWaitingApproval || current.State == domain.StateWaitingManualMerge || current.State == domain.StateMerging || current.State == domain.StateReconciling {
		capability, err = c.store.PostPublicationRearmProof(ctx, ref, stopped)
	} else {
		capability, err = c.store.RearmProof(ctx, ref, stopped)
	}
	if err != nil {
		return err
	}
	if err := c.store.ActivateRearm(ctx, capability, c.runtime.ApplyRearm); err != nil {
		return err
	}
	// Keep the stop proof until terminal retirement. A scheduler Begin may be
	// cancelled after Store opens and compensates by re-latching; retaining this
	// durable predecessor lets a later lifecycle successor obtain a fresh exact
	// proof instead of stranding the ticket on a stale pending tuple.
	return nil
}

// RuntimeRearmNeeded exposes Store's sealed-only crash-retry discriminator to
// the daemon without exposing a mutable admission capability.
func (c *Controller) RuntimeRearmNeeded(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if c == nil || c.store == nil {
		return false, errors.New("runtime controller is not configured")
	}
	return c.store.RuntimeRearmNeeded(ctx, ref)
}

// Retire clears a terminal ticket's in-memory stop record only after Store's
// linearized terminal proof.  This bounds long-lived controller/runtime maps
// without creating any route back to admission.
func (c *Controller) Retire(ctx context.Context, ref domain.TicketRef) error {
	if c == nil || c.store == nil || c.runtime == nil {
		return errors.New("runtime controller is not configured")
	}
	entry := c.acquireTicket(ref)
	entry.mu.Lock()
	capability, err := c.store.TerminalControlProof(ctx, ref)
	if err != nil {
		entry.mu.Unlock()
		c.releaseTicket(ref, entry)
		return err
	}
	if err := c.runtime.ApplyRetirement(ctx, capability); err != nil {
		entry.mu.Unlock()
		c.releaseTicket(ref, entry)
		return err
	}
	entry.stopped, entry.hasStop = store.Ticket{}, false
	entry.mu.Unlock()
	c.releaseTicket(ref, entry)
	return nil
}

func (c *Controller) MergeObserved(ctx context.Context, ref domain.TicketRef) (bool, error) {
	if c == nil || c.store == nil {
		return false, errors.New("runtime controller store is not configured")
	}
	entry := c.acquireTicket(ref)
	defer c.releaseTicket(ref, entry)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if c.observer != nil {
		return c.observer.MergeObserved(ctx, ref)
	}
	// Observing merge state must never itself cancel or latch runtime work.
	// Read-only local state only decides whether an injected observer is
	// required for publication-sensitive tickets.
	prePublication, err := c.store.MergeObservationPrePublication(ctx, ref)
	if err != nil {
		return false, err
	}
	if !prePublication {
		return false, ErrMergeProofRequired
	}
	return false, nil
}
