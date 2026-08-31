package localruntime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

var ErrCIUnavailable = errors.New("CI observation capability is unavailable")

// CIObserver is deliberately read-only.  Store owns the authoritative
// candidate binding, policy witness, observation, budget use, and transition;
// this boundary merely reads GitHub's current required-check facts.
type CIObserver interface {
	contracts.CIRequiredCheckPolicyObserver
	RequiredChecks(context.Context, contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error)
}

// CIWorker performs one bounded, durable waiting_ci observation.  It never
// waits for GitHub, creates a worktree, starts a provider, or retries an
// external action.  A later scheduler tick is the only poll retry.
type CIWorker struct {
	Store    *store.Store
	Observer CIObserver
	Now      func() time.Time
}

func (w CIWorker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Store == nil || w.Observer == nil {
		return workflowworker.RunResult{Ref: ref}, ErrCIUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticket, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	result := workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}
	if ticket.State != domain.StateWaitingCI {
		return result, nil
	}
	if fence.LeaderEpoch == 0 || fence.RunnerEpoch != ticket.RunnerEpoch {
		return result, store.ErrStaleFence
	}
	now := time.Now
	if w.Now != nil {
		now = w.Now
	}
	admission, err := w.Store.AdmitCIPoll(ctx, ref, fence, now().UTC())
	if err != nil {
		return result, err
	}
	if !admission.Due {
		current, ticketErr := w.Store.Ticket(ctx, ref)
		if ticketErr != nil {
			return result, ticketErr
		}
		return workflowworker.RunResult{Ref: ref, State: current.State, Version: current.Version}, nil
	}

	// The required set is server-defined. Persist it before accepting current
	// states so the Store can reject a changed, omitted, or injected set.
	if err := w.Store.RecordCIRequiredCheckPolicyFromObserver(ctx, ref, w.Observer); err != nil {
		return result, err
	}
	publication, err := w.Store.LoadPublishedCandidate(ctx, ref)
	if err != nil {
		return result, err
	}
	checks, err := w.Observer.RequiredChecks(ctx, publication.PullRequest)
	if err != nil {
		return result, err
	}
	observation := store.CIObservation{
		Ref:                      ref,
		CandidateGeneration:      publication.Candidate.Snapshot.Generation,
		CandidateHeadSHA:         publication.Candidate.Snapshot.HeadSHA,
		CandidateTreeSHA:         publication.Candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest,
		PullRequest:              publication.PullRequest,
		ObservedTicketVersion:    ticket.Version,
		ObservedFence:            fence,
		ObservedAt:               now().UTC(),
		RequiredChecks:           ciChecks(checks),
		Classification:           ciClassification(checks),
	}
	if err := w.Store.RecordAuthenticatedCIObservation(ctx, observation); err != nil {
		return result, err
	}
	stored, err := w.Store.LoadCIObservation(ctx, ref)
	if err != nil {
		return result, err
	}
	transition := store.CIObservationTransition{Ref: ref, ObservationDigest: stored.ObservationDigest, ExpectedVersion: ticket.Version, Fence: fence}
	if stored.Classification == "red" {
		requestID := "ci-red/" + strings.TrimPrefix(stored.ObservationDigest, "sha256:")
		// The Store allocates this request atomically with checks_red, repair
		// binding, and the ticket advance. No budget mutation occurs here.
		transition.CorrectionBudget = &store.CorrectionBudgetAuthority{Ref: ref, RequestID: requestID, TicketVersion: ticket.Version, Fence: fence}
	}
	transitioned, err := w.Store.ConsumeAuthenticatedCIObservation(ctx, transition)
	if err != nil {
		return result, err
	}
	current, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return result, err
	}
	return workflowworker.RunResult{Ref: ref, State: current.State, Version: transitioned.Version, Transitioned: transitioned.Version == ticket.Version+1}, nil
}

func ciChecks(checks []contracts.RequiredCheck) []store.CIObservationCheck {
	result := make([]store.CIObservationCheck, len(checks))
	for i, check := range checks {
		result[i] = store.CIObservationCheck{CanonicalName: check.Name, ExternalID: check.ExternalID, NormalizedState: check.State}
	}
	return result
}

func ciClassification(checks []contracts.RequiredCheck) string {
	// Store independently canonicalizes and verifies this reduction. Keeping
	// the same small reduction here makes malformed adapter data fail closed at
	// RecordAuthenticatedCIObservation instead of acquiring a fallback meaning.
	pending := false
	for _, check := range checks {
		switch strings.ToLower(strings.TrimSpace(check.State)) {
		case "failure", "failed", "error", "timed_out", "timed-out", "action_required", "cancelled", "canceled", "cancelled_failure":
			return "red"
		case "pending", "queued", "in_progress", "in-progress", "requested", "waiting":
			pending = true
		}
	}
	if pending {
		return "pending"
	}
	return "green"
}
