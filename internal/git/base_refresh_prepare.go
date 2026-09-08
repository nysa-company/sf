package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// BaseRefreshPreparation is an observed immutable two-parent object. The
// branch, pinned base ref, index and working files have NOT changed. Applying
// the object requires a separate authenticated step under the same reservation.
type BaseRefreshPreparation = contracts.GitBaseRefreshPreparation

// baseRefreshRunnerInput is a projection of Store's canonical reservation.
// Unknown fields are retained in the hashed payload, not reserialized away:
// the exact bytes must equal the Store-issued claim's request digest. Only
// Store can mint that claim, after authenticating the complete payload.
type baseRefreshRunnerInput struct {
	Format         string
	Ref            domain.TicketRef
	TicketVersion  uint64
	Fence          domain.Fence
	Repository     string
	BaseRef        string
	NewBaseSHA     string
	ProtectedPaths []string
	Candidate      struct{ Snapshot domain.CandidateSnapshot }
	Worktree       struct {
		Path, Branch, State, BaseSHA string
		IdentityJSON                 []byte
	}
}

func decodeBaseRefreshRunnerInput(payload []byte, claim contracts.GitMutationClaim) (baseRefreshRunnerInput, Worktree, error) {
	var input baseRefreshRunnerInput
	var worktree Worktree
	sum := sha256.Sum256(payload)
	if len(payload) == 0 || len(payload) > 128<<10 || claim.Operation != "refresh-base" || claim.RequestDigest != "sha256:"+hex.EncodeToString(sum[:]) || !validMutationClaim(claim) || json.Unmarshal(payload, &input) != nil {
		return input, worktree, ErrIdentityMismatch
	}
	// The payload remains immutable across daemon recovery. Store authenticates
	// the exact signed fence bridge before a newer claim can acquire its nonce;
	// monotonic shape here is not itself authority (Acquire still checks it).
	if input.Format != "sf.protected-base-refresh.v1" || input.Ref != claim.TicketRef || input.TicketVersion == 0 || input.TicketVersion > claim.TicketVersion || input.Fence.LeaderEpoch == 0 || input.Fence.LeaderEpoch > claim.LeaderEpoch || input.Fence.RunnerEpoch == 0 || input.Fence.RunnerEpoch > claim.RunnerEpoch || input.Fence.ClaimEpoch != 0 || input.Repository != claim.Repository || input.BaseRef != claim.BaseRef || input.NewBaseSHA != claim.ExpectedBaseOID || input.Candidate.Snapshot.HeadSHA != claim.ExpectedHeadOID || input.Worktree.Path != claim.Worktree || input.Worktree.Branch != claim.Branch || input.Worktree.State != "registered" || input.Worktree.BaseSHA != input.Candidate.Snapshot.BaseSHA || len(input.ProtectedPaths) == 0 || len(input.ProtectedPaths) > 256 || json.Unmarshal(input.Worktree.IdentityJSON, &worktree.Identity) != nil {
		return input, Worktree{}, ErrIdentityMismatch
	}
	worktree.Path, worktree.Branch = input.Worktree.Path, input.Worktree.Branch
	if worktree.Identity.Repository != input.Repository || worktree.Identity.Worktree != worktree.Path || worktree.Identity.HeadRef != worktree.Branch || worktree.Identity.BaseRef != input.BaseRef || worktree.Identity.BaseHead != input.Worktree.BaseSHA || !validOID(input.Worktree.BaseSHA) || !validOID(input.NewBaseSHA) || !validOID(claim.ExpectedHeadOID) || !validOID(input.Candidate.Snapshot.TreeSHA) || len(input.Candidate.Snapshot.TreeSHA) != len(input.NewBaseSHA) || len(input.Worktree.BaseSHA) != len(input.NewBaseSHA) || len(claim.ExpectedHeadOID) != len(input.NewBaseSHA) || input.NewBaseSHA == input.Worktree.BaseSHA || input.NewBaseSHA == claim.ExpectedHeadOID || input.Worktree.BaseSHA == claim.ExpectedHeadOID {
		return input, Worktree{}, ErrIdentityMismatch
	}
	seen := map[string]bool{}
	for _, path := range input.ProtectedPaths {
		if !validRepoPath(path) || len(path) > 1000 || seen[path] {
			return input, Worktree{}, ErrIdentityMismatch
		}
		seen[path] = true
	}
	return input, worktree, nil
}

// PrepareProtectedBaseRefresh creates only unreachable immutable Git objects.
// It verifies exact Store-bound inputs, holds the repository writer lease for
// merge-tree/commit-tree, and records the object and BOTH ordered parents before
// returning. Conflicts, protected-test changes and further base movement refuse
// without changing the branch or checkout. No shell or arbitrary Git argv.
func (r Runner) PrepareProtectedBaseRefresh(ctx context.Context, payload []byte, claim contracts.GitMutationClaim) (prepared BaseRefreshPreparation, returnedErr error) {
	ctx, cancel := boundedGitContext(ctx)
	defer cancel()
	input, worktree, err := decodeBaseRefreshRunnerInput(payload, claim)
	if err != nil {
		return prepared, err
	}
	lease, err := r.acquireSuppliedMutation(ctx, claim, contracts.GitMutationClaim{Repository: input.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "refresh-base", BaseRef: input.BaseRef, ExpectedBaseOID: input.NewBaseSHA, ExpectedHeadOID: input.Candidate.Snapshot.HeadSHA})
	if err != nil {
		return prepared, err
	}
	defer func() {
		returnedErr = mergeMutationLeaseRelease(returnedErr, lease)
		if returnedErr != nil {
			prepared = BaseRefreshPreparation{}
		}
	}()
	ctx = withMutationLease(ctx, lease)
	recorder, ok := lease.(contracts.GitBaseRefreshPreparationLease)
	if !ok {
		return prepared, ErrIdentityMismatch
	}
	if head, err := r.StrictCleanWorktreeHead(ctx, worktree); err != nil || head != claim.ExpectedHeadOID {
		return prepared, fmt.Errorf("%w: refresh requires exact clean candidate", ErrUnsafeWorktree)
	}
	if tree, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "--verify", claim.ExpectedHeadOID+"^{tree}"); err != nil || tree != input.Candidate.Snapshot.TreeSHA {
		return prepared, ErrIdentityMismatch
	}
	extra, _, err := r.githubTransportEnvironment(worktree.Identity.Origin)
	if err != nil {
		return prepared, err
	}
	checkRemote := func() error {
		if err := requireMutationLease(ctx, lease); err != nil {
			return err
		}
		remote, err := r.remoteHeadEnv(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, worktree.Identity.Origin, input.BaseRef, extra)
		if err != nil {
			return err
		}
		if remote != input.NewBaseSHA {
			return fmt.Errorf("%w: protected base changed during refresh preparation", ErrUnexpectedRemote)
		}
		return nil
	}
	if err := checkRemote(); err != nil {
		return prepared, err
	}
	ancestry := baseRefreshTreeAncestry{CandidatePair: [2]string{input.Worktree.BaseSHA, claim.ExpectedHeadOID}, RefreshedBasePair: [2]string{input.Worktree.BaseSHA, input.NewBaseSHA}}
	for _, pair := range [][2]string{ancestry.CandidatePair, ancestry.RefreshedBasePair} {
		if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "merge-base", "--is-ancestor", pair[0], pair[1]); err != nil {
			return prepared, ErrUnexpectedRemote
		}
	}
	if err := requireMutationLease(ctx, lease); err != nil {
		return prepared, err
	}
	output, commandErr := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "merge-tree", "--write-tree", claim.ExpectedHeadOID, input.NewBaseSHA)
	code := 0
	if commandErr != nil {
		var exit *exec.ExitError
		if errors.As(commandErr, &exit) {
			code = exit.ExitCode()
		} else {
			return prepared, commandErr
		}
	}
	merged, err := validateBaseRefreshTreePreparation(baseRefreshTreeRequest{OriginalBaseOID: input.Worktree.BaseSHA, CandidateOID: claim.ExpectedHeadOID, RefreshedBaseOID: input.NewBaseSHA}, ancestry, baseRefreshMergeTreeResponse{Operands: [2]string{claim.ExpectedHeadOID, input.NewBaseSHA}, Output: output, ExitCode: code})
	if err != nil {
		return prepared, err
	}
	args := []string{"diff", "--no-renames", "--name-only", "-z", claim.ExpectedHeadOID, merged.TreeOID, "--"}
	for _, path := range input.ProtectedPaths {
		args = append(args, ":(literal)"+path)
	}
	changed, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, args...)
	if err != nil {
		return prepared, err
	}
	if len(changed) != 0 {
		return prepared, fmt.Errorf("%w: refresh changed verification-owned files", ErrUnsafeWorktree)
	}
	// Derive the deterministic timestamp from the immutable candidate commit,
	// not the wall clock, a provider string, or a mutable working-file timestamp.
	stamp, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "show", "-s", "--format=%ct", claim.ExpectedHeadOID)
	seconds, parseErr := strconv.ParseInt(stamp, 10, 64)
	if err != nil || parseErr != nil || seconds < 0 || seconds > 253402300799 || strconv.FormatInt(seconds, 10) != stamp {
		return prepared, ErrIdentityMismatch
	}
	if head, err := r.StrictCleanWorktreeHead(ctx, worktree); err != nil || head != claim.ExpectedHeadOID {
		return prepared, ErrUnsafeWorktree
	}
	if err := checkRemote(); err != nil {
		return prepared, err
	}
	commit, err := r.oneEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, deterministicCommitEnv(time.Unix(seconds, 0).UTC().Format(time.RFC3339)), "commit-tree", merged.TreeOID, "-p", merged.Parents[0], "-p", merged.Parents[1], "-m", "sf protected-base refresh\n\nSF-Refresh-Intent: "+claim.RequestDigest)
	if err != nil || !validOID(commit) || len(commit) != len(claim.ExpectedBaseOID) {
		return prepared, ErrUnsafeWorktree
	}
	observed, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "show", "-s", "--format=%T %P", commit)
	if err != nil || observed != strings.Join([]string{merged.TreeOID, merged.Parents[0], merged.Parents[1]}, " ") {
		return prepared, ErrIdentityMismatch
	}
	if err := requireMutationLease(ctx, lease); err != nil {
		return prepared, err
	}
	if err := recorder.RecordBaseRefreshPreparation(ctx, commit, merged.TreeOID, merged.Parents); err != nil {
		return prepared, err
	}
	if err := checkRemote(); err != nil {
		return prepared, err
	}
	if head, err := r.StrictCleanWorktreeHead(ctx, worktree); err != nil || head != claim.ExpectedHeadOID {
		return prepared, ErrUnsafeWorktree
	}
	return BaseRefreshPreparation{CommitOID: commit, TreeOID: merged.TreeOID, Parents: merged.Parents}, nil
}
