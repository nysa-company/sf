// Package worktreecoord owns the one bounded composition from a ticket fence
// to a registered, pristine linked worktree. It deliberately does not start a
// daemon, provider, repository command, GitHub operation, or cleanup path.
package worktreecoord

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/store"
)

var (
	// ErrAuthentication means the path did not re-prove the exact registered
	// repository/worktree/branch/base identity. It is safe only for an operator
	// to inspect; this coordinator never deletes it.
	ErrAuthentication = errors.New("worktree identity authentication failed")
	// ErrQuarantined means a path or durable effect is ambiguous, foreign, or
	// dirty. The worktree is intentionally retained for operator recovery.
	ErrQuarantined = errors.New("worktree is quarantined for operator recovery")
	// ErrInProgress means another claimant owns an unresolved creation effect.
	// Callers may retry Ensure; they must not create a second worktree.
	ErrInProgress = errors.New("worktree creation is already in progress")
)

// EnsureRequest is a daemon-acquired ticket identity. The coordinator never
// invents a version or fence, which keeps a stale leader/runner from reaching
// either filesystem or Git mutation work.
type EnsureRequest struct {
	Ref     domain.TicketRef
	Version uint64
	Fence   domain.Fence
}

// Coordinator composes only Store and the narrow Git Runner boundary.
type Coordinator struct {
	Store *store.Store
	Git   git.Runner
}

func (c Coordinator) Ensure(ctx context.Context, request EnsureRequest) (store.StoredWorktree, error) {
	if c.Store == nil || request.Ref.Validate() != nil || request.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 {
		return store.StoredWorktree{}, fmt.Errorf("%w: complete ticket fence and store are required", ErrAuthentication)
	}
	// This is a Store writer transaction solely to prove the current authority.
	// It happens before even making the derived worktree parent directory.
	if err := c.Store.ValidateTicketFence(ctx, request.Ref, request.Version, request.Fence); err != nil {
		return store.StoredWorktree{}, err
	}
	project, err := c.Store.Project(ctx, request.Ref.Channel, request.Ref.Project)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	path, err := c.Store.TicketWorktreePath(request.Ref)
	if err != nil {
		return store.StoredWorktree{}, err
	}

	registered, err := c.Store.Worktree(ctx, request.Ref)
	if err == nil {
		return c.authenticateRegistered(ctx, request, project, path, registered)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return store.StoredWorktree{}, err
	}

	// A recovered caller must prefer a durable create claim over new allocation.
	// This is what turns response loss between Git creation and registration
	// into exact adoption instead of a second linked worktree.
	if facts, factsErr := c.Store.WorktreeCreationIntent(ctx, request.Ref); factsErr == nil {
		return c.waitForCreation(ctx, request, project, path, facts)
	} else if !errors.Is(factsErr, store.ErrNotFound) {
		return store.StoredWorktree{}, fmt.Errorf("%w: creation intent is ambiguous: %v", ErrQuarantined, factsErr)
	}

	if err := ensureExactParent(path); err != nil {
		return store.StoredWorktree{}, fmt.Errorf("%w: unable to establish worktree parent: %v", ErrQuarantined, err)
	}
	branch, err := (git.Allocator{Authority: c.Store}).Allocate(ctx, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	repository, base, err := c.Git.ObserveRepositoryBase(ctx, project.Path, project.BaseRef)
	if err != nil {
		return store.StoredWorktree{}, fmt.Errorf("%w: registered repository preflight failed: %v", ErrAuthentication, err)
	}
	intent := store.GitMutationIntent{
		EffectFence:     store.EffectFence{Ref: request.Ref, TicketVersion: request.Version, Fence: request.Fence},
		RequestDigest:   ensureDigest(request.Ref, repository, path, branch, project.BaseRef, base),
		Repository:      repository,
		Worktree:        path,
		Branch:          branch,
		Operation:       "create-worktree",
		BaseRef:         project.BaseRef,
		ExpectedBaseOID: base,
		ExpectedHeadOID: base,
	}
	intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
	if _, err := c.Store.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: request.Ref, Kind: "git/create-worktree", TicketVersion: request.Version, Fence: request.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		return store.StoredWorktree{}, err
	}
	claim, err := c.Store.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		// A concurrent claimant may have crossed the durable boundary after the
		// initial recovery lookup. Let it finish/reconcile; never fabricate an
		// alternative branch/path claim.
		if facts, factsErr := c.Store.WorktreeCreationIntent(ctx, request.Ref); factsErr == nil {
			return c.waitForCreation(ctx, request, project, path, facts)
		}
		return store.StoredWorktree{}, fmt.Errorf("%w: durable Git claim was not available: %v", ErrInProgress, err)
	}
	created, createErr := c.Git.CreateWorktree(ctx, repository, path, branch, project.BaseRef, claim)
	if createErr != nil {
		if errors.Is(createErr, git.ErrMutationLeaseRelease) {
			return store.StoredWorktree{}, fmt.Errorf("%w: Git writer lease release was not durable: %v", ErrQuarantined, createErr)
		}
		facts, factsErr := c.Store.WorktreeCreationIntent(ctx, request.Ref)
		if factsErr == nil {
			if _, statErr := os.Lstat(path); statErr == nil {
				return c.reconcileCreation(ctx, request, project, path, facts)
			}
		}
		// A failing Git invocation is not proof that it never crossed the launch
		// gate: branch/control-plane state can exist even when no worktree leaf is
		// visible. Preserve an uncertain effect for recovery rather than allowing
		// a retry to run a second `worktree add -b` against ambiguous state.
		_, _ = c.Store.MarkEffectUncertain(ctx, effectFence(claim))
		return store.StoredWorktree{}, fmt.Errorf("%w: Git worktree creation did not produce an authenticated path: %v", ErrQuarantined, createErr)
	}
	return c.confirmAndRegister(ctx, request, project, created, claim, "", nil)
}

func (c Coordinator) authenticateRegistered(ctx context.Context, request EnsureRequest, project store.Project, expectedPath string, stored store.StoredWorktree) (store.StoredWorktree, error) {
	if stored.State != "registered" || stored.Path != expectedPath || stored.Branch == "" {
		return store.StoredWorktree{}, fmt.Errorf("%w: registered row has an unexpected path, state, or creation witness", ErrQuarantined)
	}
	worktree, identity, err := decodeWorktree(stored.Path, stored.Branch, stored.IdentityJSON)
	if err != nil || !sameIdentityJSON(stored.IdentityJSON, identity) || identity.Repository != project.Path || identity.BaseRef != project.BaseRef || identity.BaseHead != stored.BaseSHA {
		return store.StoredWorktree{}, fmt.Errorf("%w: registered identity does not bind its stored repository/base: %v", ErrAuthentication, err)
	}
	if _, err := c.Git.CleanWorktreeHead(ctx, worktree); err != nil {
		return store.StoredWorktree{}, fmt.Errorf("%w: registered worktree changed, is dirty, or no longer authenticates: %v", ErrQuarantined, err)
	}
	return stored, nil
}

func (c Coordinator) reconcileCreation(ctx context.Context, request EnsureRequest, project store.Project, expectedPath string, facts store.GitMutationIntentFacts) (store.StoredWorktree, error) {
	claim := facts.Claim
	if claim.TicketRef != request.Ref || claim.Repository != project.Path || claim.Worktree != expectedPath || claim.BaseRef != project.BaseRef || claim.ExpectedBaseOID != claim.ExpectedHeadOID {
		return store.StoredWorktree{}, fmt.Errorf("%w: creation claim is not the current exact ticket identity", ErrQuarantined)
	}
	identity, err := c.Git.Snapshot(ctx, claim.Worktree, claim.BaseRef)
	if err != nil || identity.Repository != claim.Repository || identity.Worktree != claim.Worktree || identity.HeadRef != claim.Branch || identity.BaseHead != claim.ExpectedBaseOID {
		return store.StoredWorktree{}, fmt.Errorf("%w: claimed creation path does not prove its exact repository, branch, and base: %v", ErrQuarantined, err)
	}
	worktree := git.Worktree{Path: claim.Worktree, Branch: claim.Branch, Identity: identity}
	return c.confirmAndRegister(ctx, request, project, worktree, claim, facts.ObservedIdentity, &facts)
}

func (c Coordinator) confirmAndRegister(ctx context.Context, request EnsureRequest, project store.Project, worktree git.Worktree, claim contracts.GitMutationClaim, observed string, facts *store.GitMutationIntentFacts) (store.StoredWorktree, error) {
	if claim.Repository != project.Path || claim.Worktree != worktree.Path || claim.Branch != worktree.Branch || claim.BaseRef != project.BaseRef || worktree.Identity.Repository != project.Path || worktree.Identity.BaseRef != project.BaseRef || worktree.Identity.BaseHead != claim.ExpectedBaseOID {
		return store.StoredWorktree{}, fmt.Errorf("%w: creation result does not bind the registered project and claim", ErrQuarantined)
	}
	head, err := c.Git.CleanWorktreeHead(ctx, worktree)
	if err != nil || head != claim.ExpectedHeadOID {
		return store.StoredWorktree{}, fmt.Errorf("%w: creation result is dirty, changed, or no longer authenticates: %v", ErrQuarantined, err)
	}
	identityJSON, err := json.Marshal(worktree.Identity)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	if observed != "" && observed != string(identityJSON) {
		return store.StoredWorktree{}, fmt.Errorf("%w: recovered worktree identity differs from the confirmed effect", ErrQuarantined)
	}
	if claim.TicketVersion == request.Version && claim.LeaderEpoch == request.Fence.LeaderEpoch && claim.RunnerEpoch == request.Fence.RunnerEpoch {
		if _, err := c.Store.ConfirmEffect(ctx, effectFence(claim), string(identityJSON)); err != nil {
			return store.StoredWorktree{}, err
		}
	} else if facts != nil && facts.Effect.State == store.EffectUncertain {
		if _, err := c.Store.ConfirmRecoveredWorktreeCreation(ctx, claim, string(identityJSON)); err != nil {
			return store.StoredWorktree{}, err
		}
	} else if facts == nil || facts.Effect.State != store.EffectConfirmed {
		return store.StoredWorktree{}, fmt.Errorf("%w: old create claim cannot be promoted under this ticket fence", ErrQuarantined)
	}
	if err := c.Store.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: request.Ref, ExpectedVersion: request.Version, Fence: request.Fence, Path: worktree.Path, Branch: worktree.Branch, IdentityJSON: identityJSON, BaseSHA: worktree.Identity.BaseHead, HeadSHA: head}); err != nil {
		return store.StoredWorktree{}, err
	}
	registered, err := c.Store.Worktree(ctx, request.Ref)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	return c.authenticateRegistered(ctx, request, project, worktree.Path, registered)
}

func (c Coordinator) waitForCreation(ctx context.Context, request EnsureRequest, project store.Project, path string, initial store.GitMutationIntentFacts) (store.StoredWorktree, error) {
	deadline := time.Now().Add(5 * time.Second)
	facts := initial
	for {
		// A path appears before the creator has completed Runner's post-create
		// snapshot and durable effect confirmation. Do not race that window: an
		// executing effect remains owned by its claimant even if Git has already
		// made the directory visible.
		if facts.Effect.State != store.EffectExecuting {
			if _, err := os.Lstat(path); err == nil {
				return c.reconcileCreation(ctx, request, project, path, facts)
			}
		}
		if time.Now().After(deadline) {
			return store.StoredWorktree{}, ErrInProgress
		}
		select {
		case <-ctx.Done():
			return store.StoredWorktree{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		var err error
		facts, err = c.Store.WorktreeCreationIntent(ctx, request.Ref)
		if err != nil {
			return store.StoredWorktree{}, fmt.Errorf("%w: creation effect changed while waiting: %v", ErrQuarantined, err)
		}
	}
}

func effectFence(claim contracts.GitMutationClaim) store.EffectFence {
	return store.EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}
}

func ensureExactParent(path string) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil || canonical != parent {
		return errors.New("worktree parent is not a canonical directory")
	}
	return nil
}

func ensureDigest(ref domain.TicketRef, repository, path, branch, baseRef, base string) string {
	hash := sha256.New()
	for _, value := range []string{"sf.worktree-create.request.v1", string(ref.Channel), string(ref.Project), string(ref.Ticket), repository, path, branch, baseRef, base} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func decodeWorktree(path, branch string, raw []byte) (git.Worktree, git.Identity, error) {
	var identity git.Identity
	if err := json.Unmarshal(raw, &identity); err != nil {
		return git.Worktree{}, git.Identity{}, err
	}
	if path == "" || branch == "" || identity.Worktree != path || identity.HeadRef != branch {
		return git.Worktree{}, git.Identity{}, errors.New("stored worktree identity is incomplete")
	}
	return git.Worktree{Path: path, Branch: branch, Identity: identity}, identity, nil
}

func sameIdentityJSON(raw []byte, identity git.Identity) bool {
	canonical, err := json.Marshal(identity)
	return err == nil && bytes.Equal(raw, canonical)
}
