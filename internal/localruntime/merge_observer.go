package localruntime

import (
	"context"
	"errors"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// publishedMergeObserver is the authenticated read-only bridge used by
// cancellation after publication. It starts from Store-authenticated PR
// evidence and accepts only the exact factory-owned PR identity returning a
// merged state from GitHub; branch/name guesses are never adopted.
type publishedMergeObserver struct {
	Store  historicalPublicationStore
	GitHub contracts.GitHub
}

type historicalPublicationStore interface {
	LoadHistoricalPublishedCandidate(context.Context, domain.TicketRef) (store.PublishedCandidateEvidence, error)
	MergeObservationPrePublication(context.Context, domain.TicketRef) (bool, error)
}

func (o publishedMergeObserver) MergeObserved(ctx context.Context, ref domain.TicketRef) (bool, error) {
	observation, err := o.Observe(ctx, ref)
	if err != nil {
		return false, err
	}
	return observation.Observed.Merged, nil
}

// Observe returns the complete read-only witness used by the manual merge
// Store boundary. MergeObserved remains the compatibility wrapper used by
// cancellation and older callers.
func (o publishedMergeObserver) Observe(ctx context.Context, ref domain.TicketRef) (store.ManualMergeObservation, error) {
	if o.Store == nil || o.GitHub == nil {
		return store.ManualMergeObservation{}, errors.New("published merge observer is not configured")
	}
	prePublication, err := o.Store.MergeObservationPrePublication(ctx, ref)
	if err != nil {
		return store.ManualMergeObservation{}, err
	}
	evidence, err := o.Store.LoadHistoricalPublishedCandidate(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		// Absence is benign only when the Store also proves that no durable
		// publication effect or merge intent exists. An effect-before-witness
		// crash must remain a hard cancellation ambiguity.
		if prePublication {
			return store.ManualMergeObservation{}, nil
		}
		return store.ManualMergeObservation{}, store.ErrPublicationEvidence
	}
	if err != nil {
		return store.ManualMergeObservation{}, err
	}
	observer, ok := o.GitHub.(interface {
		ObservePublishedPullRequest(context.Context, contracts.PullRequestIdentity) (contracts.PublishedPullRequestObservation, error)
	})
	if !ok {
		return store.ManualMergeObservation{}, errors.New("GitHub client does not provide authenticated published-PR observation")
	}
	observed, err := observer.ObservePublishedPullRequest(ctx, evidence.PullRequest)
	if err != nil {
		return store.ManualMergeObservation{}, err
	}
	if !samePublishedIdentity(observed.Identity, evidence.PullRequest) {
		return store.ManualMergeObservation{}, errors.New("published pull request identity changed during merge observation")
	}
	if !validMergeObservation(observed) {
		return store.ManualMergeObservation{}, errors.New("published pull request merge observation is malformed")
	}
	if !observed.Merged {
		return store.ManualMergeObservation{}, nil
	}
	return store.NewManualMergeObservation(evidence, observed), nil
}

// ObserveExternal is the typed sibling used by active waiting states. It
// retains publication evidence and all merged/base facts, but permits the
// source head in GitHub's current observation to differ from the published
// head. The Store decides whether that difference is an unverified terminal
// outcome or the existing exact manual-reconciliation path.
func (o publishedMergeObserver) ObserveExternal(ctx context.Context, ref domain.TicketRef) (store.ExternalMergeObservation, error) {
	if o.Store == nil || o.GitHub == nil {
		return store.ExternalMergeObservation{}, errors.New("published merge observer is not configured")
	}
	prePublication, err := o.Store.MergeObservationPrePublication(ctx, ref)
	if err != nil {
		return store.ExternalMergeObservation{}, err
	}
	evidence, err := o.Store.LoadHistoricalPublishedCandidate(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		if prePublication {
			return store.ExternalMergeObservation{}, nil
		}
		return store.ExternalMergeObservation{}, store.ErrPublicationEvidence
	}
	if err != nil {
		return store.ExternalMergeObservation{}, err
	}
	observer, ok := o.GitHub.(contracts.ExternalMergeObserver)
	if !ok {
		return store.ExternalMergeObservation{}, errors.New("GitHub client does not provide authenticated external-merge observation")
	}
	observed, err := observer.ObserveExternalMerge(ctx, evidence.PullRequest)
	if err != nil {
		return store.ExternalMergeObservation{}, err
	}
	if !validMergeObservation(observed) {
		return store.ExternalMergeObservation{}, errors.New("external pull request merge observation is malformed")
	}
	if !observed.Merged {
		return store.ExternalMergeObservation{}, nil
	}
	return store.NewManualMergeObservation(evidence, observed), nil
}

func validMergeObservation(observed contracts.PublishedPullRequestObservation) bool {
	if observed.State != "OPEN" && observed.State != "CLOSED" && observed.State != "MERGED" {
		return false
	}
	if (observed.State == "MERGED") != observed.Merged || !validMergeOID(observed.Identity.BaseOID) || !validMergeOID(observed.BaseHeadOID) || observed.Identity.BaseOID != observed.BaseHeadOID {
		return false
	}
	if observed.Merged {
		return !observed.Draft && validMergeOID(observed.MergeCommit)
	}
	return true
}

func validMergeOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	return strings.Trim(value, "0123456789abcdef") == ""
}

func samePublishedIdentity(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.Number == right.Number && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef && left.FactoryOwned && right.FactoryOwned
}
