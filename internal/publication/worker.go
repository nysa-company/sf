// Package publication owns the small, fenced publishing phase.  It is the
// only workflow code that combines the authenticated candidate with Git and
// GitHub public effects; SQLite remains the authority for every effect and
// for the publishing -> waiting_ci transition.
package publication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	githubboundary "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/store"
)

var (
	ErrNotPublishing       = errors.New("publication worker requires a publishing ticket")
	ErrBoundaryUnavailable = errors.New("publication worker requires Git, GitHub, and draft-PR recovery boundaries")
	ErrPublicationDrift    = errors.New("publication identity drifted; a fresh candidate is required")
	ErrRemoteCandidate     = errors.New("remote candidate branch is not the durable publication target")
	ErrPullRequest         = errors.New("draft pull request is not the exact factory-owned publication target")
)

// Worker is deliberately concrete over Store.  Publication evidence combines
// several immutable Store records and must not be reconstructed by a mockable
// partial interface that could accidentally become another authority.
type Worker struct {
	Store  *store.Store
	Git    gitboundary.Runner
	GitHub contracts.GitHub
}

type Result struct {
	Ref          domain.TicketRef
	State        domain.State
	Version      uint64
	Transitioned bool
	Replayed     bool
}

// Run reconciles an existing publication first, then performs at most the two
// public effects and commits their immutable witness.  An unknown push/PR
// response never authorizes a blind retry: the exact remote fact is observed
// through the corresponding recovery boundary before any later launch.
func (w Worker) Run(ctx context.Context, ref domain.TicketRef, fence domain.Fence) (Result, error) {
	if w.Store == nil || w.GitHub == nil {
		return Result{}, ErrBoundaryUnavailable
	}
	observer, ok := w.GitHub.(contracts.DraftPullRequestObserver)
	if !ok {
		return Result{}, ErrBoundaryUnavailable
	}
	if ref.Validate() != nil || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return Result{}, store.ErrStaleFence
	}
	ticket, err := w.Store.Ticket(ctx, ref)
	if err != nil {
		return Result{}, err
	}
	result := Result{Ref: ref, State: ticket.State, Version: ticket.Version}
	if ticket.State == domain.StateWaitingCI {
		return result, nil
	}
	if ticket.State != domain.StatePublishing || ticket.RunnerEpoch != fence.RunnerEpoch {
		return result, ErrNotPublishing
	}
	if _, err := config.DecodeSnapshot(ticket.ConfigSnapshot, ticket.ConfigDigest); err != nil {
		return result, fmt.Errorf("frozen publication configuration: %w", err)
	}
	if existing, err := w.Store.LoadPublishedCandidate(ctx, ref); err == nil {
		if existing.CurrentTicketVersion != ticket.Version || existing.CurrentFence != fence {
			return result, store.ErrStaleFence
		}
		project, projectErr := w.Store.Project(ctx, ref.Channel, ref.Project)
		if projectErr != nil {
			return result, projectErr
		}
		liveWorktree, _, identityErr := publicationWorktree(project, existing.Worktree, existing.Candidate)
		if identityErr != nil || w.validatePublished(ctx, observer, liveWorktree, existing.Candidate, existing.PullRequest) != nil {
			if identityErr != nil {
				return result, identityErr
			}
			return result, ErrPublicationDrift
		}
		if _, err := w.Store.TransitionPublishedCandidate(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence}); err != nil {
			return result, err
		}
		result.State, result.Version, result.Transitioned, result.Replayed = domain.StateWaitingCI, ticket.Version+1, true, true
		return result, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return result, err
	}

	project, err := w.Store.Project(ctx, ref.Channel, ref.Project)
	if err != nil {
		return result, err
	}
	candidate, err := w.Store.LatestCandidate(ctx, ref)
	if err != nil {
		return result, err
	}
	worktree, err := w.Store.Worktree(ctx, ref)
	if err != nil {
		return result, err
	}
	gitWorktree, repository, err := publicationWorktree(project, worktree, candidate)
	if err != nil {
		return result, err
	}
	if candidate.TicketVersion+1 != ticket.Version || candidate.Fence.LeaderEpoch != fence.LeaderEpoch || candidate.Fence.RunnerEpoch != fence.RunnerEpoch || candidate.Snapshot.SourceDigest != ticket.SourceDigest || worktree.TicketVersion != ticket.Version || worktree.Fence != fence {
		return result, ErrPublicationDrift
	}
	if err := w.GitHub.AuthStatus(ctx); err != nil {
		return result, err
	}
	if got, err := w.GitHub.Repository(ctx, repository); err != nil || got != repository {
		if err != nil {
			return result, err
		}
		return result, ErrPublicationDrift
	}
	push, err := w.ensurePush(ctx, ticket, fence, project, candidate, worktree, gitWorktree)
	if err != nil {
		return result, err
	}
	identity := contracts.PullRequestIdentity{Repository: repository, HeadOwner: repository.Owner, HeadRepository: repository.Name, HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: project.BaseRef, BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	title, body := publicationText(ticket, candidate)
	pr, observedAt, err := w.ensureDraft(ctx, ticket, fence, observer, identity, title, body)
	if err != nil {
		return result, err
	}
	if err := w.validatePublished(ctx, observer, gitWorktree, candidate, pr); err != nil {
		return result, err
	}
	prEffect, err := w.Store.Effect(ctx, draftKey(identity, title, body))
	if err != nil {
		return result, err
	}
	value := store.PublishedCandidateEvidence{Ref: ref, TicketVersion: ticket.Version, Fence: fence, Candidate: candidate, ConfigGeneration: ticket.ConfigGeneration, ConfigDigest: ticket.ConfigDigest, ConfigSnapshotDigest: digest(ticket.ConfigSnapshot), Worktree: worktree, RemoteBranchRef: worktree.Branch, RemoteBranchOID: candidate.Snapshot.HeadSHA, RemoteBaseOID: candidate.Snapshot.BaseSHA, PushEffect: effectEvidence(push, store.PublicationPushEffectKind), PullRequest: pr, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: observedAt, PRCreateOrUpdateEffect: effectEvidence(prEffect, store.PublicationPRCreateEffectKind), CreatedAt: time.Now().UTC()}
	if err := w.Store.RecordPublishedCandidate(ctx, value); err != nil {
		return result, err
	}
	if _, err := w.Store.TransitionPublishedCandidate(ctx, store.Transition{Ref: ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence}); err != nil {
		return result, err
	}
	result.State, result.Version, result.Transitioned = domain.StateWaitingCI, ticket.Version+1, true
	return result, nil
}

func (w Worker) ensurePush(ctx context.Context, ticket store.Ticket, fence domain.Fence, project store.Project, candidate store.StoredCandidate, worktree store.StoredWorktree, gitWorktree gitboundary.Worktree) (store.Effect, error) {
	if facts, err := w.Store.PublicationPushIntent(ctx, ticket.Ref); err == nil {
		if facts.Claim.Repository != project.Path || facts.Claim.Worktree != worktree.Path || facts.Claim.Branch != worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != candidate.Snapshot.BaseSHA || facts.Claim.ExpectedHeadOID != candidate.Snapshot.HeadSHA {
			return store.Effect{}, ErrPublicationDrift
		}
		remote, observeErr := w.Git.ObserveRemoteBranch(ctx, gitWorktree, gitWorktree.Identity.PushOrigin, worktree.Branch)
		if observeErr != nil {
			return store.Effect{}, observeErr
		}
		if remote.OID != "" && remote.OID != candidate.Snapshot.HeadSHA {
			return store.Effect{}, ErrRemoteCandidate
		}
		present := remote.OID == candidate.Snapshot.HeadSHA
		if facts.Effect.State == store.EffectConfirmed {
			if !present {
				return store.Effect{}, ErrRemoteCandidate
			}
			if facts.Effect.ObservedIdentity != store.CanonicalPublicationPushObservation(worktree.Branch, candidate.Snapshot.HeadSHA) {
				return store.Effect{}, ErrRemoteCandidate
			}
			return facts.Effect, nil
		}
		prior := store.EffectObservation{EffectFence: effectFence(facts.Effect), Present: present}
		if present {
			prior.Identity = store.CanonicalPublicationPushObservation(worktree.Branch, candidate.Snapshot.HeadSHA)
		}
		current := store.EffectFence{SemanticKey: facts.Effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence}
		if facts.Effect.TicketVersion == ticket.Version && facts.Effect.LeaderEpoch == fence.LeaderEpoch && facts.Effect.RunnerEpoch == fence.RunnerEpoch {
			effect, settleErr := w.Store.ObserveEffect(ctx, prior)
			if settleErr != nil {
				return store.Effect{}, settleErr
			}
			if !present {
				return w.ensurePush(ctx, ticket, fence, project, candidate, worktree, gitWorktree)
			}
			return effect, nil
		}
		effect, settleErr := w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{Prior: prior, Current: current})
		if settleErr != nil {
			return store.Effect{}, settleErr
		}
		if !present {
			return w.ensurePush(ctx, ticket, fence, project, candidate, worktree, gitWorktree)
		}
		return effect, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return store.Effect{}, err
	}
	request := pushRequestDigest(ticket.Ref, project.Path, worktree, candidate)
	intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence}, RequestDigest: request, Repository: project.Path, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "push", BaseRef: project.BaseRef, ExpectedBaseOID: candidate.Snapshot.BaseSHA, ExpectedHeadOID: candidate.Snapshot.HeadSHA}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if _, err := w.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: ticket.Ref, Kind: "git/push", TicketVersion: ticket.Version, Fence: fence, RequestDigest: request}); err != nil {
		return store.Effect{}, err
	}
	claim, err := w.Store.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		return store.Effect{}, err
	}
	head, err := w.Git.PushWithRequest(ctx, gitWorktree, gitboundary.PushRequest{ExpectedHead: candidate.Snapshot.HeadSHA, ExpectedPriorHead: "", MutationClaim: claim})
	if err != nil {
		return store.Effect{}, err
	}
	if head != candidate.Snapshot.HeadSHA {
		return store.Effect{}, ErrRemoteCandidate
	}
	return w.Store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: intent.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, store.CanonicalPublicationPushObservation(worktree.Branch, head))
}

func (w Worker) ensureDraft(ctx context.Context, ticket store.Ticket, fence domain.Fence, observer contracts.DraftPullRequestObserver, identity contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, time.Time, error) {
	key, request := draftKey(identity, title, body), githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body)
	effect, err := w.Store.Effect(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		if _, err = w.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: key, Ref: ticket.Ref, Kind: "draft_pr", TicketVersion: ticket.Version, Fence: fence, RequestDigest: request}); err != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, err
		}
		effect, err = w.Store.Effect(ctx, key)
	}
	if err != nil || effect.Ref != ticket.Ref || effect.Kind != "draft_pr" || effect.RequestDigest != request {
		if err != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, err
		}
		return contracts.PullRequestIdentity{}, time.Time{}, ErrPullRequest
	}
	observed, state, draft, found, observeErr := observer.ObserveDraftPullRequest(ctx, identity)
	if observeErr != nil {
		return contracts.PullRequestIdentity{}, time.Time{}, observeErr
	}
	if found && (!samePR(observed, identity) || state != "OPEN" || !draft) {
		return contracts.PullRequestIdentity{}, time.Time{}, ErrPullRequest
	}
	if effect.State == store.EffectConfirmed {
		if !found || effect.ObservedIdentity != store.CanonicalPublicationPRObservation(observed, state, draft) {
			return contracts.PullRequestIdentity{}, time.Time{}, ErrPullRequest
		}
		return observed, time.Now().UTC(), nil
	}
	if !found && (effect.State == store.EffectExecuting || effect.State == store.EffectUncertain) {
		prior := store.EffectObservation{EffectFence: effectFence(effect), Present: false}
		if effect.TicketVersion == ticket.Version && effect.LeaderEpoch == fence.LeaderEpoch && effect.RunnerEpoch == fence.RunnerEpoch {
			if _, err := w.Store.ObserveEffect(ctx, prior); err != nil {
				return contracts.PullRequestIdentity{}, time.Time{}, err
			}
		} else if _, err := w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{Prior: prior, Current: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence}}); err != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, err
		}
		return w.ensureDraft(ctx, ticket, fence, observer, identity, title, body)
	}
	if !found {
		claim, claimErr := w.Store.ClaimEffect(ctx, store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
		if claimErr != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, claimErr
		}
		if !claim.Claimed {
			return contracts.PullRequestIdentity{}, time.Time{}, ErrPullRequest
		}
		if _, err := w.GitHub.CreateDraftPullRequest(ctx, claim.ExternalClaim(), identity, title, body); err != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, err
		}
		observed, state, draft, found, observeErr = observer.ObserveDraftPullRequest(ctx, identity)
		if observeErr != nil || !found || !samePR(observed, identity) || state != "OPEN" || !draft {
			if observeErr != nil {
				return contracts.PullRequestIdentity{}, time.Time{}, observeErr
			}
			return contracts.PullRequestIdentity{}, time.Time{}, ErrPullRequest
		}
		effect, err = w.Store.ConfirmEffect(ctx, store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, store.CanonicalPublicationPRObservation(observed, state, draft))
		if err != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, err
		}
		_ = effect
		return observed, time.Now().UTC(), nil
	}
	// Presence is reconciliation, not a second creation attempt.  A recovery
	// fence uses the Store's invalidated-effect primitive so no old authority is
	// silently rebound to the current runner.
	prior := store.EffectObservation{EffectFence: effectFence(effect), Present: true, Identity: store.CanonicalPublicationPRObservation(observed, state, draft)}
	if effect.TicketVersion == ticket.Version && effect.LeaderEpoch == fence.LeaderEpoch && effect.RunnerEpoch == fence.RunnerEpoch {
		if _, err := w.Store.ObserveEffect(ctx, prior); err != nil {
			return contracts.PullRequestIdentity{}, time.Time{}, err
		}
	} else if _, err := w.Store.ReconcileInvalidatedEffect(ctx, store.InvalidatedEffectObservation{Prior: prior, Current: store.EffectFence{SemanticKey: key, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence}}); err != nil {
		return contracts.PullRequestIdentity{}, time.Time{}, err
	}
	return observed, time.Now().UTC(), nil
}

func effectFence(value store.Effect) store.EffectFence {
	return store.EffectFence{SemanticKey: value.SemanticKey, Ref: value.Ref, TicketVersion: value.TicketVersion, Fence: domain.Fence{LeaderEpoch: value.LeaderEpoch, RunnerEpoch: value.RunnerEpoch, ClaimEpoch: value.ClaimEpoch}}
}
func effectEvidence(value store.Effect, kind string) store.PublicationEffectEvidence {
	return store.PublicationEffectEvidence{SemanticKey: value.SemanticKey, Kind: kind, RequestDigest: value.RequestDigest, ClaimEpoch: value.ClaimEpoch, ObservedIdentity: value.ObservedIdentity}
}

func publicationWorktree(project store.Project, stored store.StoredWorktree, candidate store.StoredCandidate) (gitboundary.Worktree, contracts.RepositoryIdentity, error) {
	var identity gitboundary.Identity
	if json.Unmarshal(stored.IdentityJSON, &identity) != nil {
		return gitboundary.Worktree{}, contracts.RepositoryIdentity{}, ErrPublicationDrift
	}
	canonical, err := json.Marshal(identity)
	if err != nil || !bytes.Equal(canonical, stored.IdentityJSON) || identity.Repository != project.Path || identity.Worktree != stored.Path || identity.HeadRef != stored.Branch || identity.BaseRef != project.BaseRef || identity.BaseHead != candidate.Snapshot.BaseSHA || stored.BaseSHA != candidate.Snapshot.BaseSHA {
		return gitboundary.Worktree{}, contracts.RepositoryIdentity{}, ErrPublicationDrift
	}
	repository, ok := githubRepository(identity.Origin)
	if !ok {
		return gitboundary.Worktree{}, contracts.RepositoryIdentity{}, ErrPublicationDrift
	}
	if push, ok := githubRepository(identity.PushOrigin); !ok || push != repository {
		return gitboundary.Worktree{}, contracts.RepositoryIdentity{}, ErrPublicationDrift
	}
	return gitboundary.Worktree{Path: stored.Path, Branch: stored.Branch, Identity: identity}, repository, nil
}

// validatePublished is deliberately called immediately before every state
// transition. Publication evidence authenticates the original effects, but a
// later close, force-push, or foreign replacement is live remote truth and
// must never be carried into waiting_ci by a replay alone.
func (w Worker) validatePublished(ctx context.Context, observer contracts.DraftPullRequestObserver, worktree gitboundary.Worktree, candidate store.StoredCandidate, want contracts.PullRequestIdentity) error {
	remote, err := w.Git.ObserveRemoteBranch(ctx, worktree, worktree.Identity.PushOrigin, worktree.Branch)
	if err != nil || remote.OID != candidate.Snapshot.HeadSHA {
		if err != nil {
			return err
		}
		return ErrRemoteCandidate
	}
	pr, state, draft, found, err := observer.ObserveDraftPullRequest(ctx, want)
	if err != nil || !found || state != "OPEN" || !draft || !samePR(pr, want) {
		if err != nil {
			return err
		}
		return ErrPullRequest
	}
	return nil
}

func githubRepository(raw string) (contracts.RepositoryIdentity, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil {
		return contracts.RepositoryIdentity{}, false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(path.Clean(u.Path), "/"), ".git")
	parts := strings.Split(name, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return contracts.RepositoryIdentity{}, false
	}
	return contracts.RepositoryIdentity{Host: "github.com", Owner: parts[0], Name: parts[1]}, true
}
func pushRequestDigest(ref domain.TicketRef, repository string, worktree store.StoredWorktree, candidate store.StoredCandidate) string {
	return "sha256:" + digest([]byte("sf.publication.push.v1\x00"+string(ref.Channel)+"\x00"+string(ref.Project)+"\x00"+string(ref.Ticket)+"\x00"+repository+"\x00"+worktree.Path+"\x00"+worktree.Branch+"\x00"+candidate.Snapshot.BaseSHA+"\x00"+candidate.Snapshot.HeadSHA))
}
func draftKey(identity contracts.PullRequestIdentity, title, body string) string {
	return "github/draft-pr/v1/" + githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body)
}
func digest(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func samePR(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.Number > 0 && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef && left.BaseOID == right.BaseOID && left.FactoryOwned && right.FactoryOwned
}
func publicationText(ticket store.Ticket, candidate store.StoredCandidate) (string, string) {
	return "sf: " + string(ticket.Ref.Ticket), "<!-- sf:publication/v1 -->\n\nticket: " + string(ticket.Ref.Ticket) + "\ncandidate: " + candidate.Snapshot.HeadSHA + "\nsource: " + ticket.SourceDigest
}
