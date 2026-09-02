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
	"strings"
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

// AuthenticateOperatorSourceResume is the deliberately narrow counterpart to
// Ensure for one clean operator source commit. It re-proves the registered
// filesystem identity, exact one-parent commit, immutable path/status set,
// frozen remote refs, and current Store proof before Scheduler may launch a
// fresh Reviewer or admits Builder only after that fresh checkpoint has been
// independently reauthenticated.
func (c Coordinator) AuthenticateOperatorSourceResume(ctx context.Context, request EnsureRequest, proof store.OperatorSourceResumeProof) (store.StoredWorktree, error) {
	if c.Store == nil || request.Ref.Validate() != nil || request.Version == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 || proof.Ref != request.Ref || proof.Version != request.Version || proof.Fence != request.Fence {
		return store.StoredWorktree{}, fmt.Errorf("%w: complete source-resume proof is required", ErrAuthentication)
	}
	// Reload the Store proof before touching the filesystem. It both prevents a
	// caller from inventing a struct and establishes the exact durable inputs
	// which the physical observation must match.
	current, found, err := c.Store.OperatorSourceResumeProof(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !found || !sameOperatorSourceResumeProof(current, proof) {
		if err != nil {
			return store.StoredWorktree{}, err
		}
		return store.StoredWorktree{}, fmt.Errorf("%w: source-resume proof is stale or absent", ErrAuthentication)
	}
	project, err := c.Store.Project(ctx, request.Ref.Channel, request.Ref.Project)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	expectedPath, err := c.Store.TicketWorktreePath(request.Ref)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	registered, err := c.Store.Worktree(ctx, request.Ref)
	if err != nil {
		return store.StoredWorktree{}, err
	}
	if !sameStoredWorktree(registered, proof.Worktree) || registered.Path != expectedPath || registered.State != "registered" {
		return store.StoredWorktree{}, fmt.Errorf("%w: registered worktree drifted from source-resume proof", ErrAuthentication)
	}
	worktree, identity, err := decodeWorktree(registered.Path, registered.Branch, registered.IdentityJSON)
	if err != nil || !sameIdentityJSON(registered.IdentityJSON, identity) || identity.Repository != project.Path || identity.BaseRef != project.BaseRef || identity.BaseHead != registered.BaseSHA {
		return store.StoredWorktree{}, fmt.Errorf("%w: registered identity does not bind source-resume proof: %v", ErrAuthentication, err)
	}
	ticket, err := c.Store.Ticket(ctx, request.Ref)
	if err != nil || (ticket.State != domain.StateVerifying && ticket.State != domain.StateBuilding) || ticket.Version != request.Version || ticket.RunnerEpoch != request.Fence.RunnerEpoch {
		if err != nil {
			return store.StoredWorktree{}, err
		}
		return store.StoredWorktree{}, fmt.Errorf("%w: source-resume ticket is no longer at the requested admission fence", ErrAuthentication)
	}
	switch ticket.State {
	case domain.StateVerifying:
		observed, observeErr := c.Git.ObserveOperatorSourceCommit(ctx, worktree, proof.Verification.Checkpoint.CommitOID)
		if observeErr == nil {
			paths := sourceCommitPaths(observed)
			if !sameOperatorSourceCommit(observed, proof.SourceCommit) || !sourceResumePathsAllowed(paths, proof.Plan.Document.Paths, proof.Verification.Revision.OwnedFiles) {
				return store.StoredWorktree{}, fmt.Errorf("%w: source-resume checkout no longer matches its authenticated commit and paths", ErrAuthentication)
			}
		} else {
			// A fresh Reviewer checkpoint may have crossed Git's update-ref boundary
			// immediately before the process died.  Admit only Store's one exact
			// prepared child F of source S; any ordinary dirty or foreign HEAD still
			// fails the normal source-commit contract above.
			prepared, preparedFound, preparedErr := c.Store.OperatorSourceResumePreparedCheckpoint(ctx, request.Ref, request.Version, request.Fence)
			if preparedErr != nil || !preparedFound {
				if preparedErr != nil {
					return store.StoredWorktree{}, preparedErr
				}
				return store.StoredWorktree{}, fmt.Errorf("%w: source-resume worktree no longer authenticates: %v", ErrAuthentication, observeErr)
			}
			head, headErr := c.Git.CleanWorktreeHead(ctx, worktree)
			commit, commitErr := c.Git.ObserveCommit(ctx, worktree)
			if headErr != nil || commitErr != nil || head != prepared.CommitOID || commit.CommitOID != prepared.CommitOID || commit.ParentOID != prepared.ParentOID || commit.TreeOID != prepared.TreeOID {
				return store.StoredWorktree{}, fmt.Errorf("%w: prepared source-resume checkpoint drifted: head=%v commit=%v", ErrAuthentication, headErr, commitErr)
			}
		}
	case domain.StateBuilding:
		// The fresh verifier checkpoints a clean child F of the retained source
		// commit S.  Re-running the source-commit observer here would require
		// HEAD=S and strand Builder.  Instead bind HEAD=F, F.parent=S, and the
		// exact fresh verification record written at the immediately preceding
		// verifying endpoint.
		fresh, freshErr := c.Store.CurrentVerification(ctx, request.Ref)
		if freshErr != nil {
			// A runner recovery deliberately makes the immutable fresh Reviewer
			// row historical. RecoverableVerification is admissible here only
			// because Store's source-resume proof re-authenticates its path to the
			// requested live fence below.
			fresh, freshErr = c.Store.RecoverableVerification(ctx, request.Ref)
		}
		if freshErr != nil || fresh.TicketVersion >= request.Version || fresh.Revision.Revision == proof.Verification.Revision.Revision || fresh.Checkpoint.CommitOID == "" || fresh.Checkpoint.CommitOID != fresh.Revision.CheckpointID || fresh.Checkpoint.ParentOID != proof.SourceCommit.CommitOID {
			if freshErr != nil {
				return store.StoredWorktree{}, freshErr
			}
			return store.StoredWorktree{}, fmt.Errorf("%w: fresh verification is not bound to this Builder admission", ErrAuthentication)
		}
		head, headErr := c.Git.CleanWorktreeHead(ctx, worktree)
		if headErr != nil {
			return store.StoredWorktree{}, fmt.Errorf("%w: Builder worktree head is not the fresh verification checkpoint: %v", ErrAuthentication, headErr)
		}
		commit, commitErr := c.Git.ObserveCommit(ctx, worktree)
		if head == fresh.Checkpoint.CommitOID {
			if commitErr != nil || commit.CommitOID != fresh.Checkpoint.CommitOID || commit.ParentOID != proof.SourceCommit.CommitOID || commit.TreeOID != fresh.Checkpoint.TreeOID {
				return store.StoredWorktree{}, fmt.Errorf("%w: fresh verification checkpoint changed before Builder admission: %v", ErrAuthentication, commitErr)
			}
		} else {
			// Builder may have prepared candidate G and crashed before its
			// RecordCandidate append. Only Store's exact Builder/command-bound
			// child of fresh F can cross this admission boundary.
			prepared, preparedFound, preparedErr := c.Store.OperatorSourceResumePreparedCandidate(ctx, request.Ref, request.Version, request.Fence)
			if preparedErr != nil || !preparedFound || head != prepared.CommitOID || commitErr != nil || commit.CommitOID != prepared.CommitOID || commit.ParentOID != prepared.ParentOID || commit.TreeOID != prepared.TreeOID || commit.ParentOID != fresh.Checkpoint.CommitOID {
				if preparedErr != nil {
					return store.StoredWorktree{}, preparedErr
				}
				return store.StoredWorktree{}, fmt.Errorf("%w: Builder worktree head is neither fresh checkpoint nor authenticated prepared candidate", ErrAuthentication)
			}
		}
		finalHead, finalErr := c.Git.CleanWorktreeHead(ctx, worktree)
		if finalErr != nil || finalHead != commit.CommitOID {
			return store.StoredWorktree{}, fmt.Errorf("%w: Builder worktree changed during fresh checkpoint observation: %v", ErrAuthentication, finalErr)
		}
	default:
		return store.StoredWorktree{}, fmt.Errorf("%w: source-resume only admits verifying or building", ErrAuthentication)
	}
	remote, err := c.Git.ObservePublicationRemote(ctx, worktree)
	if err != nil {
		return store.StoredWorktree{}, fmt.Errorf("%w: source-resume remote state is unavailable: %v", ErrAuthentication, err)
	}
	currentRemote := store.TakeoverRemoteBaseline{Registered: true, WorktreePath: registered.Path, WorktreeBranch: registered.Branch, WorktreeIdentity: takeoverWorktreeIdentityDigest(registered.IdentityJSON), CandidatePresent: remote.Candidate.OID != "", CandidateOID: remote.Candidate.OID, BaseOID: remote.BaseOID}
	if currentRemote != proof.Remote {
		return store.StoredWorktree{}, fmt.Errorf("%w: source-resume candidate branch or protected base changed after take", ErrAuthentication)
	}
	// Git observers check identity before and after each observation;
	// now repeat Store proof/fence after that external boundary. A control or
	// leader change cannot race this exception into a Builder launch.
	current, found, err = c.Store.OperatorSourceResumeProof(ctx, request.Ref, request.Version, request.Fence)
	if err != nil || !found || !sameOperatorSourceResumeProof(current, proof) {
		if err != nil {
			return store.StoredWorktree{}, err
		}
		return store.StoredWorktree{}, fmt.Errorf("%w: source-resume proof changed during inspection", ErrAuthentication)
	}
	if err := c.Store.ValidateTicketFence(ctx, request.Ref, request.Version, request.Fence); err != nil {
		return store.StoredWorktree{}, err
	}
	return registered, nil
}

func sameOperatorSourceResumeProof(left, right store.OperatorSourceResumeProof) bool {
	return left.Ref == right.Ref && left.Version == right.Version && left.Fence == right.Fence && sameStoredWorktree(left.Worktree, right.Worktree) && left.Operator == right.Operator && sameOperatorSourceCommit(left.SourceCommit, right.SourceCommit) && left.Remote == right.Remote && left.Verification.Revision.Revision == right.Verification.Revision.Revision && left.Verification.Revision.IntentDigest == right.Verification.Revision.IntentDigest && left.Verification.Revision.ProofDigest == right.Verification.Revision.ProofDigest && equalPaths(left.Verification.Revision.OwnedFiles, right.Verification.Revision.OwnedFiles) && left.Verification.Checkpoint == right.Verification.Checkpoint && left.Verification.TicketVersion == right.Verification.TicketVersion && left.Verification.Fence == right.Verification.Fence && left.Plan.Digest == right.Plan.Digest && equalPaths(left.Plan.Document.Paths, right.Plan.Document.Paths)
}

func sameOperatorSourceCommit(left, right contracts.OperatorSourceCommit) bool {
	if left.CommitOID != right.CommitOID || left.ParentOID != right.ParentOID || left.TreeOID != right.TreeOID || len(left.Changes) != len(right.Changes) {
		return false
	}
	for index := range left.Changes {
		if left.Changes[index] != right.Changes[index] {
			return false
		}
	}
	return true
}

func sourceCommitPaths(value contracts.OperatorSourceCommit) []string {
	paths := make([]string, 0, len(value.Changes))
	for _, change := range value.Changes {
		paths = append(paths, change.Path)
	}
	return paths
}

func sameStoredWorktree(left, right store.StoredWorktree) bool {
	return left.Path == right.Path && left.Branch == right.Branch && left.State == right.State && bytes.Equal(left.IdentityJSON, right.IdentityJSON) && left.BaseSHA == right.BaseSHA && left.HeadSHA == right.HeadSHA && left.TicketVersion == right.TicketVersion && left.Fence == right.Fence
}

func equalPaths(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sourceResumePathsAllowed(changed, allowed, owned []string) bool {
	if len(changed) == 0 || len(allowed) == 0 {
		return false
	}
	for _, path := range changed {
		within := false
		for _, prefix := range allowed {
			if sourceResumePathMatches(path, prefix) {
				within = true
				break
			}
		}
		if !within {
			return false
		}
		for _, protected := range owned {
			if sourceResumePathMatches(path, protected) || sourceResumePathMatches(protected, path) {
				return false
			}
		}
	}
	return true
}

func sourceResumePathMatches(path, prefix string) bool {
	prefix = strings.Trim(prefix, "/")
	return path == prefix || strings.HasPrefix(path, prefix+"/")
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

// takeoverWorktreeIdentityDigest shares Store's canonical evidence format:
// lowercase SHA-256 hex without an algorithm prefix. The source-resume proof
// compares this value byte-for-byte with the take-time durable baseline.
func takeoverWorktreeIdentityDigest(identity []byte) string {
	sum := sha256.Sum256(identity)
	return hex.EncodeToString(sum[:])
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
