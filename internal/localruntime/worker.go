package localruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	githubboundary "github.com/nysa-company/sf/internal/github"
	pub "github.com/nysa-company/sf/internal/publication"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/workflowworker"
)

var ErrPublishingUnavailable = errors.New("publishing capability is unavailable")

const publishingUnavailableCode = "publication_runtime_unavailable"
const ciUnavailableCode = "ci_runtime_unavailable"

// Worker is the state dispatch boundary for the local runtime. It performs a
// fresh Store read for every scheduler admission, then delegates the three
// provider phases to workflowworker and the publication phase to publication.
// Keeping this adapter at the runtime edge avoids widening either worker's
// result interface merely to compose the two lifecycles.
type Worker struct {
	Store       *store.Store
	Engine      workflowworker.StateMachine
	Workflow    workflowworker.Worker
	Publication pub.Worker
	CI          CIWorker
	// PublicationEnabled is explicit so a pre-publishing composition cannot
	// accidentally treat a zero publication worker as a successful phase.
	PublicationEnabled bool
}

// NewWorker constructs the runtime dispatcher while retaining concrete phase
// workers. The value form makes the adapter easy to use in hermetic tests and
// does not add another authority or lifecycle.
func NewWorker(database *store.Store, workflow workflowworker.Worker, publication pub.Worker) Worker {
	return Worker{Store: database, Workflow: workflow, Publication: publication, PublicationEnabled: true}
}

func (w Worker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Store == nil {
		return workflowworker.RunResult{Ref: ref}, workflowworker.ErrUnsupportedState
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticket, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return workflowworker.RunResult{Ref: ref}, err
	}
	ready, err := w.Store.RuntimeAdmissionReady(ctx, ref, ticket.Version, fence)
	if err != nil {
		return workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}, err
	}
	if !ready {
		return workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}, store.ErrStaleFence
	}
	switch ticket.State {
	case domain.StatePlanning, domain.StateVerifying, domain.StateBuilding, domain.StateReviewing:
		return w.Workflow.Run(ctx, ref, fence)
	case domain.StatePublishing:
		if !w.PublicationEnabled {
			return w.blockPublishing(ctx, ticket, fence)
		}
		result, err := w.Publication.Run(ctx, ref, fence)
		return workflowworker.RunResult{
			Ref:          result.Ref,
			State:        result.State,
			Version:      result.Version,
			Phase:        domain.PhasePublish,
			Transitioned: result.Transitioned,
			Replayed:     result.Replayed,
		}, err
	case domain.StateMerging:
		return w.merge(ctx, ticket, fence)
	case domain.StateReconciling:
		return w.reconcile(ctx, ticket, fence)
	case domain.StateWaitingManualMerge:
		return w.observeManualMerge(ctx, ticket, fence)
	case domain.StateWaitingApproval:
		return w.observeExternalMerge(ctx, ticket, fence)
	case domain.StateWaitingCI:
		if w.CI.Observer == nil {
			return w.blockCI(ctx, ticket, fence)
		}
		return w.CI.Run(ctx, ref, fence)
	default:
		// Control and terminal states are inert. The scheduler normally filters
		// them, but keeping this dispatch boundary inert makes direct calls safe.
		return workflowworker.RunResult{Ref: ref, State: ticket.State, Version: ticket.Version}, nil
	}
}

// observeManualMerge is intentionally read-only. Manual mode never marks a
// PR ready or asks GitHub to merge; it only records an authenticated external
// merge. Exact source-head observations retain the verified reconciliation
// path, while a changed source head becomes external_merged.
func (w Worker) observeManualMerge(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	result := workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhaseReconcile}
	if !w.PublicationEnabled || w.Engine == nil || w.Publication.GitHub == nil {
		return result, ErrPublishingUnavailable
	}
	observer := publishedMergeObserver{Store: w.Store, GitHub: w.Publication.GitHub}
	observation, err := observer.ObserveExternal(ctx, ticket.Ref)
	if err != nil {
		return result, fmt.Errorf("observe manual merge: %w", err)
	}
	if !observation.Observed.Merged {
		return result, nil
	}
	observation, err = w.Store.BindExternalMergeObservation(ctx, ticket.Ref, observation, fence)
	if err != nil {
		return result, fmt.Errorf("bind manual merge observation: %w", err)
	}
	to := domain.StateExternalMerged
	if observation.Observed.Identity.HeadOID == observation.Publication.HeadOID {
		to = domain.StateReconciling
	}
	transition, err := w.Store.RecordExternalMergeObservation(ctx, store.Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateWaitingManualMerge, To: to, Trigger: "external_merge_observed", Fence: fence}, observation)
	if err != nil {
		return result, fmt.Errorf("record manual merge observation: %w", err)
	}
	if to == domain.StateExternalMerged {
		current, err := w.Store.Ticket(ctx, ticket.Ref)
		if err != nil {
			return result, err
		}
		result.State, result.Version, result.Transitioned = current.State, current.Version, true
		return result, nil
	}
	_, err = w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: transition.Version, From: domain.StateReconciling, Trigger: "reconcile_pass", Fence: fence, Attributes: map[string]string{"terminal_remote_truth_exact": "true"}})
	if err != nil {
		return result, err
	}
	current, err := w.Store.Ticket(ctx, ticket.Ref)
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = current.State, current.Version, true
	return result, nil
}

// observeExternalMerge handles guarded approval without a checkout or remote
// mutation. A merged PR is accepted only through the typed observer and the
// evidence-bearing Store boundary, which classifies it as external_merged.
func (w Worker) observeExternalMerge(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	result := workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhaseReconcile}
	if !w.PublicationEnabled || w.Engine == nil || w.Publication.GitHub == nil {
		return result, ErrPublishingUnavailable
	}
	observer := publishedMergeObserver{Store: w.Store, GitHub: w.Publication.GitHub}
	observation, err := observer.ObserveExternal(ctx, ticket.Ref)
	if err != nil {
		return result, fmt.Errorf("observe external merge: %w", err)
	}
	if !observation.Observed.Merged {
		return result, nil
	}
	observation, err = w.Store.BindExternalMergeObservation(ctx, ticket.Ref, observation, fence)
	if err != nil {
		return result, fmt.Errorf("bind external merge observation: %w", err)
	}
	_, err = w.Store.RecordExternalMergeObservation(ctx, store.Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateWaitingApproval, To: domain.StateExternalMerged, Trigger: "external_merge_observed", Fence: fence}, observation)
	if err != nil {
		return result, fmt.Errorf("record external merge observation: %w", err)
	}
	current, err := w.Store.Ticket(ctx, ticket.Ref)
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = current.State, current.Version, true
	return result, nil
}

func (w Worker) merge(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	result := workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhaseMerge}
	if !w.PublicationEnabled || w.Publication.Store == nil || w.Publication.GitHub == nil || w.Engine == nil || ticket.MergeMode != domain.MergeGuarded {
		return result, ErrPublishingUnavailable
	}
	method, err := frozenMergeMethod(ticket)
	if err != nil {
		return result, err
	}
	candidate, err := w.Store.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		return result, fmt.Errorf("load recoverable merge candidate: %w", err)
	}
	published, err := w.Store.LoadHistoricalPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		return result, fmt.Errorf("load historical published candidate: %w", err)
	}
	if published.PullRequest.HeadOID != candidate.Snapshot.HeadSHA {
		return result, store.ErrEvidenceConflict
	}
	approved := false
	decisions, err := w.Store.OperatorDecisions(ctx, ticket.Ref)
	if err != nil {
		return result, fmt.Errorf("load guarded merge decisions: %w", err)
	}
	for _, decision := range decisions {
		if decision.Decision == "approved" && !decision.Invalidated && decision.ReviewedHead == candidate.Snapshot.HeadSHA {
			approved = true
		}
	}
	if !approved {
		return result, store.ErrEvidenceConflict
	}
	identity := published.PullRequest
	authorization := domain.MergeAuthorization{ReviewedHead: candidate.Snapshot.HeadSHA, CurrentHead: candidate.Snapshot.HeadSHA, ReviewedBaseSHA: candidate.Snapshot.BaseSHA, CurrentBaseSHA: candidate.Snapshot.BaseSHA, ReviewedBaseHeadOID: identity.BaseOID, CurrentBaseHeadOID: identity.BaseOID, Approved: true, GatesGreen: true}
	readyDigest := githubboundary.CanonicalReadyRequestDigest(identity)
	readyKey := "merge-ready/" + string(ticket.Ref.Channel) + "/" + string(ticket.Ref.Project) + "/" + string(ticket.Ref.Ticket) + "/" + candidate.Snapshot.HeadSHA
	readyConfirmed, err := w.reconcileReady(ctx, ticket, fence, readyKey, identity)
	if err != nil {
		return result, fmt.Errorf("reconcile ready effect: %w", err)
	}
	if !readyConfirmed {
		if _, err := w.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: readyKey, Ref: ticket.Ref, Kind: "pr_ready", TicketVersion: ticket.Version, Fence: fence, RequestDigest: readyDigest}); err != nil {
			return result, err
		}
		ready, err := w.Store.ClaimEffect(ctx, store.EffectFence{SemanticKey: readyKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
		if err != nil {
			return result, err
		}
		if ready.Claimed {
			if err := w.Publication.GitHub.MarkReady(ctx, ready.ExternalClaim(), identity); err != nil {
				_, _ = w.Store.MarkEffectUncertain(ctx, store.EffectFence{SemanticKey: readyKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: ready.Effect.LeaderEpoch, RunnerEpoch: ready.Effect.RunnerEpoch, ClaimEpoch: ready.Effect.ClaimEpoch}})
				return result, err
			}
			if _, err := w.Store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: readyKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: ready.Effect.LeaderEpoch, RunnerEpoch: ready.Effect.RunnerEpoch, ClaimEpoch: ready.Effect.ClaimEpoch}}, "ready/"+candidate.Snapshot.HeadSHA); err != nil {
				return result, err
			}
		}
	}
	mergeDigest := githubboundary.CanonicalMergeRequestDigest(identity, candidate.Snapshot.HeadSHA, method, authorization)
	mergeKey := "merge/" + string(ticket.Ref.Channel) + "/" + string(ticket.Ref.Project) + "/" + string(ticket.Ref.Ticket) + "/" + candidate.Snapshot.HeadSHA
	mergeConfirmed, err := w.reconcileMerge(ctx, ticket, fence, mergeKey)
	if err != nil {
		return result, fmt.Errorf("reconcile merge effect: %w", err)
	}
	if !mergeConfirmed {
		if _, err := w.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: mergeKey, Ref: ticket.Ref, Kind: "merge", TicketVersion: ticket.Version, Fence: fence, RequestDigest: mergeDigest}); err != nil {
			return result, err
		}
		claim, err := w.Store.ClaimEffect(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
		if err != nil {
			return result, err
		}
		if claim.Claimed {
			if err := w.Publication.GitHub.MergeExactHead(ctx, claim.ExternalClaim(), identity, candidate.Snapshot.HeadSHA, method, authorization); err != nil {
				_, _ = w.Store.MarkEffectUncertain(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}})
				return result, err
			}
			if _, err := w.Store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: mergeKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, "merged/"+candidate.Snapshot.HeadSHA); err != nil {
				return result, err
			}
		}
	}
	transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: domain.StateMerging, Trigger: "merge_observed", Fence: fence, Attributes: map[string]string{"source_head_equals_reviewed_head": "true", "protected_branch_contains_merge": "true"}})
	if err != nil {
		return result, err
	}
	_, err = w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: transition.TicketVersion, From: domain.StateReconciling, Trigger: "reconcile_pass", Fence: fence, Attributes: map[string]string{"terminal_remote_truth_exact": "true"}})
	if err != nil {
		return result, err
	}
	done, err := w.Store.Ticket(ctx, ticket.Ref)
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = done.State, done.Version, true
	return result, nil
}

// frozenMergeMethod reads the project authority retained on the ticket rather
// than consulting a mutable repository configuration or choosing a runtime
// default. The exact value is included in the durable merge-effect digest and
// subsequently rechecked by the GitHub boundary.
func frozenMergeMethod(ticket store.Ticket) (string, error) {
	effective, err := config.DecodeSnapshot(ticket.ConfigSnapshot, ticket.ConfigDigest)
	if err != nil {
		return "", fmt.Errorf("frozen merge configuration: %w", err)
	}
	return effective.MergeMethod, nil
}

// reconcileReady settles only a claim that was invalidated by a recovery
// fence. A same-fence unknown response remains uncertain and never becomes a
// retry authorization. The all-state PR observation supplies the exact remote
// fact needed to either confirm the old ready effect or prove its absence.
func (w Worker) reconcileReady(ctx context.Context, ticket store.Ticket, fence domain.Fence, key string, identity contracts.PullRequestIdentity) (bool, error) {
	effect, err := w.Store.Effect(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	expectedIdentity := "ready/" + identity.HeadOID
	expectedDigest := githubboundary.CanonicalReadyRequestDigest(identity)
	if effect.Ref != ticket.Ref || effect.SemanticKey != key || effect.Kind != "pr_ready" || effect.RequestDigest != expectedDigest {
		return false, store.ErrEvidenceConflict
	}
	if effect.State == store.EffectConfirmed {
		if effect.ClaimEpoch == 0 || effect.ObservedIdentity != expectedIdentity {
			return false, store.ErrEvidenceConflict
		}
		if effect.TicketVersion == ticket.Version && effect.LeaderEpoch == fence.LeaderEpoch && effect.RunnerEpoch == fence.RunnerEpoch {
			return true, nil
		}
		// A confirmed ready mutation is immutable evidence, but its original
		// runner may have been revoked by a restart or operator handoff. Promote
		// that exact evidence under the current fenced endpoint; never treat a
		// merely confirmed row with a different binding as current authority.
		promoted, promoteErr := w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{
			Prior: store.EffectObservation{
				EffectFence: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: effect.TicketVersion, Fence: domain.Fence{LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch}},
				Present:     true, Identity: expectedIdentity,
			},
			Current: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence},
		})
		if promoteErr != nil {
			return false, promoteErr
		}
		return promoted.State == store.EffectConfirmed && promoted.TicketVersion == ticket.Version && promoted.LeaderEpoch == fence.LeaderEpoch && promoted.RunnerEpoch == fence.RunnerEpoch, nil
	}
	if (effect.State == store.EffectExecuting || effect.State == store.EffectUncertain) && effect.ClaimEpoch == 0 {
		return false, store.ErrEvidenceConflict
	}
	if effect.ObservedIdentity != "" {
		return false, store.ErrEvidenceConflict
	}
	if effect.State != store.EffectExecuting && effect.State != store.EffectUncertain {
		return false, nil
	}
	observer, ok := w.Publication.GitHub.(interface {
		ObservePublishedPullRequest(context.Context, contracts.PullRequestIdentity) (contracts.PublishedPullRequestObservation, error)
	})
	if !ok {
		return false, store.ErrEffectBusy
	}
	observed, err := observer.ObservePublishedPullRequest(ctx, identity)
	if err != nil || !samePublishedIdentity(observed.Identity, identity) || !validMergeObservation(observed) {
		return false, store.ErrEvidenceConflict
	}
	observedIdentity := ""
	if observed.Ready {
		observedIdentity = expectedIdentity
	}
	_, err = w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{Prior: store.EffectObservation{EffectFence: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: effect.TicketVersion, Fence: domain.Fence{LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch}}, Present: observed.Ready, Identity: observedIdentity}, Current: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence}})
	if err != nil {
		return false, err
	}
	return observed.Ready, nil
}

// reconcileMerge is the restart-only completion path for a lost exact-head
// merge response. Store verifies the immutable merge intent and promotes the
// uncertain old claim only after GitHub re-observes the exact protected-branch
// merge proof; it never issues another merge call.
func (w Worker) reconcileMerge(ctx context.Context, ticket store.Ticket, fence domain.Fence, key string) (bool, error) {
	effect, err := w.Store.Effect(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if effect.State == store.EffectConfirmed {
		if effect.TicketVersion == ticket.Version && effect.LeaderEpoch == fence.LeaderEpoch && effect.RunnerEpoch == fence.RunnerEpoch {
			return true, nil
		}
		// Reconciling performs its own exact MergeIntent observation below. The
		// Store proof reader authenticates the original merge_observed handoff and
		// any later pause/resume chain, so this path never promotes the parent
		// effect or launches a second merge.
		if ticket.State == domain.StateReconciling {
			return true, nil
		}
		observer, ok := w.Publication.GitHub.(contracts.MergeIntentObserver)
		if !ok {
			return false, store.ErrEffectBusy
		}
		intent, found, err := w.Store.MergeIntent(ctx, key)
		if err != nil {
			return false, fmt.Errorf("load confirmed merge intent: %w", err)
		}
		if !found || intent.SemanticKey != effect.SemanticKey || intent.Ref != effect.Ref || effect.Ref != ticket.Ref || effect.Kind != "merge" || intent.RequestDigest != effect.RequestDigest || effect.TicketVersion < intent.TicketVersion || effect.LeaderEpoch < intent.LeaderEpoch || effect.RunnerEpoch < intent.RunnerEpoch || effect.ClaimEpoch < intent.ClaimEpoch {
			return false, store.ErrEvidenceConflict
		}
		identity, err := observer.ObserveMergeIntent(ctx, intent)
		if err != nil {
			return false, fmt.Errorf("observe confirmed merge intent: %w", err)
		}
		if identity == "" {
			return false, store.ErrEvidenceConflict
		}
		promoted, err := w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{
			Prior: store.EffectObservation{
				EffectFence: store.EffectFence{
					SemanticKey:   key,
					Ref:           ticket.Ref,
					TicketVersion: effect.TicketVersion,
					Fence: domain.Fence{
						LeaderEpoch: effect.LeaderEpoch,
						RunnerEpoch: effect.RunnerEpoch,
						ClaimEpoch:  effect.ClaimEpoch,
					},
				},
				Present:  true,
				Identity: identity,
			},
			Current: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence},
		})
		if err != nil {
			return false, fmt.Errorf("promote confirmed merge effect: %w", err)
		}
		return promoted.State == store.EffectConfirmed, nil
	}
	if effect.State != store.EffectUncertain {
		return false, nil
	}
	observer, ok := w.Publication.GitHub.(contracts.MergeIntentObserver)
	if !ok {
		return false, store.ErrEffectBusy
	}
	recovered, err := w.Store.RecoverMergeIntent(ctx, key, observer)
	if errors.Is(err, store.ErrNotFound) && ticket.State == domain.StateMerging {
		if _, found, intentErr := w.Store.MergeIntent(ctx, key); intentErr != nil || found {
			if intentErr != nil {
				return false, intentErr
			}
			return false, store.ErrEvidenceConflict
		}
		// MergeExactHead persists its immutable intent before entering the
		// mutation guard. Its durable absence therefore proves this revoked claim
		// never crossed the external handoff and may be retired, not replayed.
		_, settleErr := w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{
			Prior:   store.EffectObservation{EffectFence: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: effect.TicketVersion, Fence: domain.Fence{LeaderEpoch: effect.LeaderEpoch, RunnerEpoch: effect.RunnerEpoch, ClaimEpoch: effect.ClaimEpoch}}, Present: false},
			Current: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence},
		})
		return false, settleErr
	}
	if err != nil {
		return false, err
	}
	return recovered.State == store.EffectConfirmed, nil
}

// reconcile completes a guarded merge after a crash between merge_observed
// and reconcile_pass. It re-observes the immutable merge intent through the
// same production GitHub/protected-ref boundary; a confirmed proof is reused
// by mergeproof.Coordinator and no new merge mutation is authorized.
func (w Worker) reconcile(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	result := workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhaseReconcile}
	if !w.PublicationEnabled || w.Publication.Store == nil || w.Publication.GitHub == nil || w.Engine == nil {
		return result, ErrPublishingUnavailable
	}
	if ticket.MergeMode == domain.MergeManual {
		observer := publishedMergeObserver{Store: w.Store, GitHub: w.Publication.GitHub}
		observation, err := observer.Observe(ctx, ticket.Ref)
		if err != nil {
			return result, err
		}
		if !observation.Observed.Merged {
			return result, store.ErrPublicationEvidence
		}
		if err := w.Store.ReconcileManualMergeObservation(ctx, ticket.Ref, observation); err != nil {
			return result, err
		}
		transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: domain.StateReconciling, Trigger: "reconcile_pass", Fence: fence, Attributes: map[string]string{"terminal_remote_truth_exact": "true"}})
		if err != nil {
			return result, err
		}
		result.State, result.Version, result.Transitioned = transition.To, transition.TicketVersion, true
		return result, nil
	}
	if ticket.MergeMode != domain.MergeGuarded {
		return result, ErrPublishingUnavailable
	}
	candidate, err := w.Store.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		return result, err
	}
	key := "merge/" + string(ticket.Ref.Channel) + "/" + string(ticket.Ref.Project) + "/" + string(ticket.Ref.Ticket) + "/" + candidate.Snapshot.HeadSHA
	if confirmed, err := w.reconcileMerge(ctx, ticket, fence, key); err != nil || !confirmed {
		if err != nil {
			return result, err
		}
		return result, store.ErrEvidenceConflict
	}
	intent, found, err := w.Store.MergeIntent(ctx, key)
	if err != nil || !found {
		return result, store.ErrEvidenceConflict
	}
	observer, ok := w.Publication.GitHub.(contracts.MergeIntentObserver)
	if !ok {
		return result, store.ErrEffectBusy
	}
	if _, err := observer.ObserveMergeIntent(ctx, intent); err != nil {
		return result, err
	}
	transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{Ticket: ticket.Ref, TicketVersion: ticket.Version, From: domain.StateReconciling, Trigger: "reconcile_pass", Fence: fence, Attributes: map[string]string{"terminal_remote_truth_exact": "true"}})
	if err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = transition.To, transition.TicketVersion, true
	return result, nil
}

func (w Worker) blockCI(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Engine == nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version}, ErrCIUnavailable
	}
	binary := "sf"
	if ticket.Ref.Channel == domain.ChannelDev {
		binary = "sf-dev"
	}
	payload, err := json.Marshal(struct {
		Code       string `json:"code"`
		Reason     string `json:"reason"`
		NextAction string `json:"next_action"`
		Guidance   string `json:"guidance"`
	}{
		Code:       ciUnavailableCode,
		Reason:     "CI observation is unavailable because the GitHub CLI or its authentication is not configured",
		NextAction: binary + " doctor",
		Guidance:   "install gh and authenticate GitHub; sf never creates or copies credentials",
	})
	if err != nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version}, err
	}
	transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{
		Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State,
		Trigger: "typed_blocker", Fence: fence,
		Attributes:   map[string]string{"no_unreconciled_external_mutation": "true"},
		EventPayload: string(payload),
	})
	if err != nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version}, err
	}
	return workflowworker.RunResult{Ref: ticket.Ref, State: domain.StateBlocked, Version: transition.TicketVersion, Transitioned: true}, nil
}
func (w Worker) blockPublishing(ctx context.Context, ticket store.Ticket, fence domain.Fence) (workflowworker.RunResult, error) {
	if w.Engine == nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhasePublish}, ErrPublishingUnavailable
	}
	binary := "sf"
	if ticket.Ref.Channel == domain.ChannelDev {
		binary = "sf-dev"
	}
	payload, err := json.Marshal(struct {
		Code       string `json:"code"`
		Reason     string `json:"reason"`
		NextAction string `json:"next_action"`
		Guidance   string `json:"guidance"`
	}{
		Code:       publishingUnavailableCode,
		Reason:     "publishing is unavailable because the GitHub CLI or its authentication is not configured",
		NextAction: binary + " doctor",
		Guidance:   "install gh and authenticate GitHub; sf never creates or copies credentials",
	})
	if err != nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhasePublish}, err
	}
	transition, err := w.Engine.Signal(ctx, contracts.SignalRequest{
		Ticket: ticket.Ref, TicketVersion: ticket.Version, From: ticket.State,
		Trigger: "typed_blocker", Fence: fence,
		Attributes:   map[string]string{"no_unreconciled_external_mutation": "true"},
		EventPayload: string(payload),
	})
	if err != nil {
		return workflowworker.RunResult{Ref: ticket.Ref, State: ticket.State, Version: ticket.Version, Phase: domain.PhasePublish}, err
	}
	return workflowworker.RunResult{Ref: ticket.Ref, State: domain.StateBlocked, Version: transition.TicketVersion, Phase: domain.PhasePublish, Transitioned: true}, nil
}

var _ interface {
	Run(context.Context, domain.TicketRef, domain.Fence) (workflowworker.RunResult, error)
} = Worker{}
