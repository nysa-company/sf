package git

import (
	"context"
	"fmt"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
)

type BaseRefreshApplied struct {
	Preparation BaseRefreshPreparation
	Identity    Identity
}

// baseRefreshRefTransaction performs both old-value comparisons in one Git
// ref transaction. No operation accepts a force-update or caller-chosen ref.
func baseRefreshRefTransaction(branch, oldBase, oldHead, newBase, newHead string) ([]byte, error) {
	if !validRef(branch) || strings.HasPrefix(branch, "refs/") || !validOID(oldBase) || !validOID(oldHead) || !validOID(newBase) || !validOID(newHead) || len(oldBase) != len(oldHead) || len(oldBase) != len(newBase) || len(oldBase) != len(newHead) || oldBase == newBase || oldHead == newHead {
		return nil, ErrIdentityMismatch
	}
	input := []byte("start\nupdate refs/heads/" + branch + " " + newHead + " " + oldHead + "\nupdate " + worktreeBaseRef(branch) + " " + newBase + " " + oldBase + "\nprepare\ncommit\n")
	if len(input) > 4096 {
		return nil, ErrIdentityMismatch
	}
	return input, nil
}

// ApplyProtectedBaseRefresh accepts only the prepared object read back from
// the live Store nonce. Recovery recognizes three exact states: unchanged old
// checkout; prepared index/files with old refs; or prepared index/files with
// both new refs. Mixed refs, foreign files or partial checkout changes refuse.
// No reset/clean or force push is used. SQLite completion remains a separate
// authority step; returning this observation does not advance the workflow.
func (r Runner) ApplyProtectedBaseRefresh(ctx context.Context, payload []byte, claim contracts.GitMutationClaim) (applied BaseRefreshApplied, returnedErr error) {
	ctx, cancel := boundedGitContext(ctx)
	defer cancel()
	input, worktree, err := decodeBaseRefreshRunnerInput(payload, claim)
	if err != nil {
		return applied, err
	}
	lease, err := r.acquireSuppliedMutation(ctx, claim, contracts.GitMutationClaim{Repository: input.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "refresh-base", BaseRef: input.BaseRef, ExpectedBaseOID: input.NewBaseSHA, ExpectedHeadOID: input.Candidate.Snapshot.HeadSHA})
	if err != nil {
		return applied, err
	}
	defer func() {
		returnedErr = mergeMutationLeaseRelease(returnedErr, lease)
		if returnedErr != nil {
			applied = BaseRefreshApplied{}
		}
	}()
	ctx = withMutationLease(ctx, lease)
	reader, ok := lease.(contracts.GitBaseRefreshPreparedReader)
	if !ok {
		return applied, ErrIdentityMismatch
	}
	prepared, found, err := reader.PreparedBaseRefresh(ctx)
	if err != nil || !found || !validOID(prepared.CommitOID) || !validOID(prepared.TreeOID) || len(prepared.CommitOID) != len(claim.ExpectedBaseOID) || len(prepared.TreeOID) != len(claim.ExpectedBaseOID) || prepared.Parents != [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID} {
		return applied, ErrIdentityMismatch
	}
	observed, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "show", "-s", "--format=%T %P", prepared.CommitOID)
	if err != nil || observed != strings.Join([]string{prepared.TreeOID, prepared.Parents[0], prepared.Parents[1]}, " ") {
		return applied, ErrIdentityMismatch
	}
	newIdentity := worktree.Identity
	newIdentity.BaseHead = input.NewBaseSHA
	branchRef := "refs/heads/" + worktree.Branch
	baseRef := worktreeBaseRef(worktree.Branch)
	readRef := func(ref string) (string, error) {
		value, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "for-each-ref", "--format=%(refname) %(objectname) %(symref)", ref)
		fields := strings.Fields(value)
		if err != nil || len(fields) != 2 || fields[0] != ref || !validOID(fields[1]) {
			return "", ErrIdentityMismatch
		}
		return fields[1], nil
	}
	readState := func() (bool, string, error) {
		head, err := readRef(branchRef)
		if err != nil {
			return false, "", err
		}
		base, err := readRef(baseRef)
		if err != nil {
			return false, "", err
		}
		isNew := head == prepared.CommitOID && base == input.NewBaseSHA
		isOld := head == claim.ExpectedHeadOID && base == input.Worktree.BaseSHA
		if !isNew && !isOld {
			return false, "", ErrUnexpectedRemote
		}
		expected := worktree.Identity
		if isNew {
			expected = newIdentity
		}
		if err := r.Reauthenticate(ctx, expected); err != nil {
			return false, "", err
		}
		flags, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "ls-files", "-v", "-z")
		if err != nil {
			return false, "", err
		}
		for _, entry := range splitNUL(flags) {
			if !strings.HasPrefix(entry, "H ") || !validRepoPath(strings.TrimPrefix(entry, "H ")) {
				return false, "", ErrUnsafeWorktree
			}
		}
		if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "diff", "--quiet", "--no-ext-diff", "--no-textconv"); err != nil {
			return false, "", ErrUnsafeWorktree
		}
		for _, ignored := range []bool{false, true} {
			args := []string{"ls-files", "--others", "--exclude-standard", "-z"}
			if ignored {
				args = append(args, "--ignored")
			}
			extra, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, args...)
			if err != nil || len(extra) != 0 {
				return false, "", ErrUnsafeWorktree
			}
		}
		if err := requireMutationLease(ctx, lease); err != nil {
			return false, "", err
		}
		tree, err := r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "write-tree")
		if err != nil || (isNew && tree != prepared.TreeOID) || (isOld && tree != prepared.TreeOID && tree != input.Candidate.Snapshot.TreeSHA) {
			return false, "", ErrUnsafeWorktree
		}
		return isNew, tree, nil
	}
	extra, _, err := r.githubTransportEnvironment(worktree.Identity.Origin)
	if err != nil {
		return applied, err
	}
	pushExtra, _, err := r.githubTransportEnvironment(worktree.Identity.PushOrigin)
	if err != nil {
		return applied, err
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
			return ErrUnexpectedRemote
		}
		// Pending recovery cannot use the ordinary old-identity remote
		// observer after the local paired CAS. Read the same exact source ref
		// under this nonce and the authenticated old/new directory descriptor.
		// A foreign hosted head never authorizes another local transformation.
		output, err := r.commandEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, pushExtra, "ls-remote", "--heads", worktree.Identity.PushOrigin, "refs/heads/"+worktree.Branch)
		if err != nil {
			return err
		}
		candidate, err := parseRemoteBranchOutput(output, worktree.Branch)
		if err != nil || (candidate != "" && candidate != claim.ExpectedHeadOID) {
			return ErrUnexpectedRemote
		}
		return nil
	}
	isNew, indexTree, err := readState()
	if err != nil {
		return applied, err
	}
	if err := checkRemote(); err != nil {
		return applied, err
	}
	if !isNew && indexTree != prepared.TreeOID {
		// Two-tree read-tree refuses local/index changes. It updates the exact
		// clean checkout before ref CAS; a crash after it is the staged state
		// above, never permission to overwrite an arbitrary dirty checkout.
		if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "read-tree", "-u", "-m", claim.ExpectedHeadOID, prepared.CommitOID); err != nil {
			return applied, ErrUnsafeWorktree
		}
	}
	isNew, indexTree, err = readState()
	if err != nil || indexTree != prepared.TreeOID {
		return applied, ErrUnsafeWorktree
	}
	if !isNew {
		if err := checkRemote(); err != nil {
			return applied, err
		}
		transaction, err := baseRefreshRefTransaction(worktree.Branch, input.Worktree.BaseSHA, claim.ExpectedHeadOID, input.NewBaseSHA, prepared.CommitOID)
		if err != nil {
			return applied, err
		}
		output, err := r.commandEnvInputExpectedWithHandoff(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, nil, transaction, nil, "update-ref", "--no-deref", "--stdin")
		if err != nil {
			return applied, err
		}
		if string(output) != "start: ok\nprepare: ok\ncommit: ok\n" {
			return applied, fmt.Errorf("%w: unexpected ref transaction result", ErrIdentityMismatch)
		}
	}
	isNew, indexTree, err = readState()
	if err != nil || !isNew || indexTree != prepared.TreeOID {
		return applied, ErrUnsafeWorktree
	}
	if err := checkRemote(); err != nil {
		return applied, err
	}
	return BaseRefreshApplied{Preparation: prepared, Identity: newIdentity}, nil
}
