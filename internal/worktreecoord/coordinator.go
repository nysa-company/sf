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
	// ErrQuarantinePersistence means the coordinator could not prove that an
	// effect which may have crossed Git's boundary is durably quarantined.  It
	// is deliberately distinct from ErrQuarantined: callers must not treat it
	// as a normal retryable recovery state.
	ErrQuarantinePersistence = errors.New("worktree effect quarantine persistence failed")
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

	// beforeCreationClaim is test-only synchronization for the stale-fence
	// boundary.  It is intentionally unexported so production composition
	// cannot install mutable behavior between validation and the durable claim.
	beforeCreationClaim func()
	afterCreationClaim  func(contracts.GitMutationClaim)
	afterCreate         func()
	afterConfirm        func()
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

	if c.beforeCreationClaim != nil {
		c.beforeCreationClaim()
	}
	// Allocation is a fence-checked Store transaction.  Do not create the
	// parent directory or allocate a branch on the strength of the earlier
	// read-side validation: a replacement leader may have arrived since then.
	branch, err := (git.Allocator{Authority: c.Store}).AllocateUnderFence(ctx, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, request.Version, request.Fence)
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
	// The durable executing claim is the final authority before any filesystem
	// write.  A stale caller therefore cannot create a parent or leaf after the
	// ticket fence has changed.
	if c.afterCreationClaim != nil {
		c.afterCreationClaim(claim)
	}
	if err := ensureExactParent(path); err != nil {
		return c.preRunnerClaimFailure(request, claim, fmt.Errorf("unable to establish worktree parent: %w", err))
	}
	created, createErr := c.Git.CreateWorktree(ctx, repository, path, branch, project.BaseRef, claim)
	if createErr != nil {
		if errors.Is(createErr, git.ErrMutationLeaseRelease) {
			// A failed lease release is itself ambiguous.  It can leave a live
			// writer, so neither confirmation nor an assumed release is safe.
			return store.StoredWorktree{}, fmt.Errorf("%w: Git writer lease release was not durable: %w", ErrQuarantined, createErr)
		}
		return c.postClaimFailure(request, project, path, claim, createErr)
	}
	if c.afterCreate != nil {
		c.afterCreate()
	}
	registered, err = c.confirmAndRegister(ctx, request, project, created, claim, "", nil)
	if err != nil {
		return c.postClaimFailure(request, project, path, claim, err)
	}
	return registered, nil
}

// preRunnerClaimFailure handles a failure after the durable claim but before
// Runner starts.  No Git effect was launched, but the executing claim still
// must become durably uncertain so a later Ensure cannot mint another claim.
func (c Coordinator) preRunnerClaimFailure(request EnsureRequest, claim contracts.GitMutationClaim, cause error) (store.StoredWorktree, error) {
	recoveryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.persistUncertain(recoveryCtx, request.Ref, claim, cause)
}

// postClaimFailure runs only after Runner has returned without
// ErrMutationLeaseRelease, which proves its mutation lease release completed.
// The caller context may be cancelled at exactly this point, so all
// observation and quarantine writes use a bounded independent context.  A
// clean visible result is confirmed/registered; otherwise the effect must be
// durably uncertain before returning a recoverable quarantine.
func (c Coordinator) postClaimFailure(request EnsureRequest, project store.Project, path string, claim contracts.GitMutationClaim, cause error) (store.StoredWorktree, error) {
	// Git authentication can invoke several bounded helper commands on a cold
	// filesystem; keep recovery finite but give it enough room to finish rather
	// than converting cancellation into an artificial persistence failure.
	recoveryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	facts, inspectErr := c.Store.WorktreeCreationIntent(recoveryCtx, request.Ref)
	if inspectErr == nil && facts.Claim == claim {
		if info, statErr := os.Lstat(path); statErr == nil && info.IsDir() {
			if recovered, reconcileErr := c.reconcileCreation(recoveryCtx, request, project, path, facts); reconcileErr == nil {
				return recovered, nil
			}
		}
	}
	if inspectErr != nil {
		cause = errors.Join(cause, fmt.Errorf("inspect creation effect: %w", inspectErr))
	}
	return c.persistUncertain(recoveryCtx, request.Ref, claim, cause)
}

func (c Coordinator) persistUncertain(ctx context.Context, ref domain.TicketRef, claim contracts.GitMutationClaim, cause error) (store.StoredWorktree, error) {
	if _, persistErr := c.Store.MarkEffectUncertain(ctx, effectFence(claim)); persistErr == nil {
		return store.StoredWorktree{}, fmt.Errorf("%w: creation result requires recovery: %w", ErrQuarantined, cause)
	} else {
		// A failed write is not proof that it failed before commit.  Re-read on
		// the independent context and accept only a durable terminal quarantine
		// (or a confirmed result) as proof that a duplicate cannot be launched.
		facts, verifyErr := c.Store.WorktreeCreationIntent(ctx, ref)
		if verifyErr == nil && facts.Claim == claim && (facts.Effect.State == store.EffectUncertain || facts.Effect.State == store.EffectConfirmed) {
			return store.StoredWorktree{}, fmt.Errorf("%w: creation result requires recovery: %w", ErrQuarantined, cause)
		}
		return store.StoredWorktree{}, errors.Join(ErrQuarantinePersistence, cause, fmt.Errorf("persist uncertainty: %w", persistErr), fmt.Errorf("verify creation effect: %v", verifyErr))
	}
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
		if c.afterConfirm != nil {
			c.afterConfirm()
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
	// Creation includes several authenticated Git observations.  Under SQLite
	// and repository-writer contention it can legitimately outlast five
	// seconds; callers still retain deterministic control through ctx.
	deadline := time.Now().Add(30 * time.Second)
	facts := initial
	for {
		// Caller cancellation always wins, even when the creation wait deadline
		// or timer is also ready.  Never turn a cancelled caller into a durable
		// ambiguity merely because a query raced its cancellation.
		if err := ctx.Err(); err != nil {
			return store.StoredWorktree{}, err
		}
		// A path appears before the creator has completed Runner's post-create
		// snapshot and durable effect confirmation. Do not race that window: an
		// executing effect remains owned by its claimant even if Git has already
		// made the directory visible.
		if facts.Effect.State != store.EffectExecuting {
			if info, err := os.Lstat(path); err == nil && info.IsDir() {
				return c.reconcileCreation(ctx, request, project, path, facts)
			} else {
				// An old result that is no longer executing must either prove the
				// exact visible directory or remain quarantined.  Treating a
				// missing/non-directory leaf as ordinary in-progress would hide a
				// durable ambiguity and make every retry wait pointlessly.
				return store.StoredWorktree{}, fmt.Errorf("%w: non-executing creation has no exact worktree directory", ErrQuarantined)
			}
		}
		if time.Now().After(deadline) {
			return store.StoredWorktree{}, ErrInProgress
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return store.StoredWorktree{}, ctx.Err()
		case <-timer.C:
		}
		if err := ctx.Err(); err != nil {
			return store.StoredWorktree{}, err
		}
		if time.Now().After(deadline) {
			return store.StoredWorktree{}, ErrInProgress
		}
		var err error
		facts, err = c.Store.WorktreeCreationIntent(ctx, request.Ref)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return store.StoredWorktree{}, ctxErr
		}
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
