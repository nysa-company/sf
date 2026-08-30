package git

import (
	"context"
	"fmt"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
)

// CommitObservation is the immutable identity of the currently checked-out
// commit and its single parent and tree. It is an observation only; it does
// not authorize any Git mutation.
type CommitObservation struct {
	CommitOID string
	ParentOID string
	TreeOID   string
}

// RegisteredWorktreeResolver supplies an already registered, authenticated
// worktree identity to the read-only prepared-commit adapter. The resolver is
// intentionally injected so git remains independent of SQLite; production
// composition resolves the row from Store and validates its canonical JSON.
type RegisteredWorktreeResolver func(context.Context, contracts.GitMutationClaim) (Worktree, error)

// PreparedCommitObserver adapts Runner.ObserveCommit to the daemon's narrow
// restart observer contract. It has no mutation method and never invokes the
// Runner's stage/commit/update-ref paths.
type PreparedCommitObserver struct {
	Runner  Runner
	Resolve RegisteredWorktreeResolver
}

func (o PreparedCommitObserver) ObservePreparedCommit(ctx context.Context, claim contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error) {
	if claim.Operation != "commit" || o.Resolve == nil {
		return contracts.PreparedCommitObservation{}, fmt.Errorf("%w: prepared commit observer requires a commit claim and registered worktree resolver", ErrIdentityMismatch)
	}
	worktree, err := o.Resolve(ctx, claim)
	if err != nil {
		return contracts.PreparedCommitObservation{}, err
	}
	if worktree.Path != claim.Worktree || worktree.Branch != claim.Branch || worktree.Identity.Repository != claim.Repository || worktree.Identity.BaseRef != claim.BaseRef || worktree.Identity.BaseHead != claim.ExpectedBaseOID {
		return contracts.PreparedCommitObservation{}, fmt.Errorf("%w: registered worktree does not bind prepared commit claim", ErrIdentityMismatch)
	}
	observed, err := o.Runner.ObserveCommit(ctx, worktree)
	if err != nil {
		return contracts.PreparedCommitObservation{}, err
	}
	return contracts.PreparedCommitObservation{CommitOID: observed.CommitOID, ParentOID: observed.ParentOID, TreeOID: observed.TreeOID}, nil
}

// RemoteBranchObservation records the exact head of one authenticated remote
// branch. OID is empty only when ls-remote returned no rows for that ref.
type RemoteBranchObservation struct {
	Branch string
	OID    string
}

// ObserveCommit authenticates the supplied worktree and then reads the
// checked-out commit, its one parent, and its tree. In particular, roots and
// merge commits are rejected because this observation is used where a single
// parent is part of the identity being proved.
func (r Runner) ObserveCommit(ctx context.Context, worktree Worktree) (CommitObservation, error) {
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}

	commitOutput, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno,
		"rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}
	commitOID, err := parseSingleOID(commitOutput, "commit")
	if err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}

	parentsOutput, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno,
		"rev-list", "--parents", "-n", "1", "HEAD")
	if err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}
	parentCommitOID, parentOID, err := parseCommitParents(parentsOutput)
	if err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}
	if parentCommitOID != commitOID || len(parentCommitOID) != len(parentOID) {
		return CommitObservation{}, fmt.Errorf("%w: commit parent output is inconsistent", ErrUnexpectedRemote)
	}

	treeOutput, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno,
		"rev-parse", "--verify", commitOID+"^{tree}")
	if err != nil {
		return CommitObservation{}, err
	}
	treeOID, err := parseSingleOID(treeOutput, "tree")
	if err != nil {
		return CommitObservation{}, err
	}
	if len(treeOID) != len(commitOID) {
		return CommitObservation{}, fmt.Errorf("%w: tree object format differs from commit", ErrUnexpectedRemote)
	}
	// Deriving the tree from the captured commit makes the tuple intrinsically
	// consistent even if HEAD is moved and moved back while this read runs.
	// The final identity check below closes the authenticated path and
	// repository/config boundary after the complete read set. Keep the final
	// HEAD read after this potentially lengthy reauthentication: otherwise HEAD
	// could move while the identity is being checked and remain undetected.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}
	finalCommitOutput, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno,
		"rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return CommitObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return CommitObservation{}, err
	}
	finalCommitOID, err := parseSingleOID(finalCommitOutput, "commit")
	if err != nil {
		return CommitObservation{}, err
	}
	if finalCommitOID != commitOID {
		return CommitObservation{}, fmt.Errorf("%w: HEAD changed during commit observation", ErrUnexpectedRemote)
	}

	return CommitObservation{CommitOID: commitOID, ParentOID: parentOID, TreeOID: treeOID}, nil
}

// ObserveRemoteBranch authenticates the supplied worktree, verifies that
// origin is one of its already-authenticated configured origins, and performs
// one read-only ls-remote query for the allocated branch.
func (r Runner) ObserveRemoteBranch(ctx context.Context, worktree Worktree, origin, branch string) (RemoteBranchObservation, error) {
	if err := ctx.Err(); err != nil {
		return RemoteBranchObservation{}, err
	}
	if _, err := validateBranch(branch); err != nil {
		return RemoteBranchObservation{}, err
	}
	if worktree.Branch != branch {
		return RemoteBranchObservation{}, fmt.Errorf("%w: remote branch is not bound to authenticated worktree", ErrIdentityMismatch)
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return RemoteBranchObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return RemoteBranchObservation{}, err
	}

	canonicalOrigin, err := safeOrigin(origin)
	if err != nil {
		return RemoteBranchObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return RemoteBranchObservation{}, err
	}
	if canonicalOrigin == "" || (canonicalOrigin != worktree.Identity.Origin && canonicalOrigin != worktree.Identity.PushOrigin) {
		return RemoteBranchObservation{}, fmt.Errorf("%w: remote origin is not an authenticated configured origin", ErrIdentityMismatch)
	}
	transportEnv, _, err := r.githubTransportEnvironment(canonicalOrigin)
	if err != nil {
		return RemoteBranchObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return RemoteBranchObservation{}, err
	}

	output, err := r.commandEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, transportEnv,
		"ls-remote", "--heads", canonicalOrigin, "refs/heads/"+branch)
	if err != nil {
		return RemoteBranchObservation{}, err
	}
	// Reauthenticate after the remote read as well. This ensures a repository,
	// worktree, pointer, hook, config, or configured origin replacement during
	// the command cannot be accepted as an observation of the original identity.
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return RemoteBranchObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return RemoteBranchObservation{}, err
	}
	oid, err := parseRemoteBranchOutput(output, branch)
	if err != nil {
		return RemoteBranchObservation{}, err
	}
	return RemoteBranchObservation{Branch: branch, OID: oid}, nil
}

func parseSingleOID(output []byte, kind string) (string, error) {
	text := strings.TrimSuffix(string(output), "\n")
	if text == "" || strings.ContainsAny(text, "\r\n") || strings.TrimSpace(text) != text || !validOID(text) {
		return "", fmt.Errorf("%w: malformed %s object id", ErrUnexpectedRemote, kind)
	}
	return text, nil
}

func parseCommitParents(output []byte) (string, string, error) {
	text := strings.TrimSuffix(string(output), "\n")
	fields := strings.Split(text, " ")
	// rev-list --parents for a non-root, non-merge commit is exactly HEAD and
	// one parent. This also rejects extra lines/tokens and malformed output.
	if len(fields) != 2 || strings.ContainsAny(text, "\r\n\t") || strings.TrimSpace(text) != text || !validOID(fields[0]) || !validOID(fields[1]) || len(fields[0]) != len(fields[1]) {
		return "", "", fmt.Errorf("%w: commit must have exactly one canonical parent", ErrUnexpectedRemote)
	}
	return fields[0], fields[1], nil
}

func parseRemoteBranchOutput(output []byte, branch string) (string, error) {
	target := "refs/heads/" + branch
	text := string(output)
	// Git's exact absence response is empty output. Whitespace is not a
	// remote row and must not be silently converted into absence.
	if text == "" {
		return "", nil
	}

	text = strings.TrimSuffix(text, "\n")
	if strings.ContainsAny(text, "\r\n") {
		return "", fmt.Errorf("%w: duplicate or extra remote branch rows", ErrUnexpectedRemote)
	}
	fields := strings.Fields(text)
	if len(fields) != 2 || !validOID(fields[0]) || fields[1] != target || text != fields[0]+"\t"+fields[1] {
		return "", fmt.Errorf("%w: malformed remote branch observation", ErrUnexpectedRemote)
	}
	return fields[0], nil
}
