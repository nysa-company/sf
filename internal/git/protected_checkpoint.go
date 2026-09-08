package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

// ProtectedCheckpointRequest must come from an exact Store-owned amendment
// authority. Digests are observations, not independently sufficient permission.
type ProtectedCheckpointRequest struct {
	EvidenceDigest       string
	Timestamp            time.Time
	OriginalCheckpoint   string
	ProtectedPaths       []string
	FullSnapshotDigest   string
	ImplementationDigest string
	MutationClaim        contracts.GitMutationClaim
}

type protectedCheckpointIndex struct {
	root *pinnedDirectory
	path string
}

func (p *protectedCheckpointIndex) valid(path string) bool {
	if p == nil || p.root == nil || path != p.path || p.root.verify() != nil {
		return false
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	return err == nil && info.Mode().IsRegular() && !linkCountIsNotOne(info) && ownedByCurrentOrRoot(info)
}

// CommitProtectedCheckpoint changes only the frozen protected portion of the
// tree/index. The worktree's implementation bytes and staging are retained.
// It uses the ordinary commit lease/prepared-object protocol, but never weakens
// Commit's clean-worktree contract. Rejection/restoration is not supported here.
func (r Runner) CommitProtectedCheckpoint(ctx context.Context, worktree Worktree, request ProtectedCheckpointRequest) (head string, returnedErr error) {
	if !validEvidenceDigest(request.EvidenceDigest) || !validEvidenceDigest(request.FullSnapshotDigest) || !validEvidenceDigest(request.ImplementationDigest) || !validOID(request.OriginalCheckpoint) || request.Timestamp.IsZero() || len(request.ProtectedPaths) == 0 || len(request.ProtectedPaths) > 256 {
		return "", ErrUnsafeWorktree
	}
	request.ProtectedPaths = append([]string(nil), request.ProtectedPaths...)
	sort.Strings(request.ProtectedPaths)
	for i, path := range request.ProtectedPaths {
		if !validRepoPath(path) || path == "." || len(path) > 4096 || i > 0 && path == request.ProtectedPaths[i-1] {
			return "", ErrUnsafeWorktree
		}
	}
	metadata := struct {
		Schema, Evidence, Parent, Full, Implementation string
		Protected                                      []string
	}{"sf.protected-checkpoint/v1", request.EvidenceDigest, request.OriginalCheckpoint, request.FullSnapshotDigest, request.ImplementationDigest, request.ProtectedPaths}
	raw, err := json.Marshal(metadata)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	commit := CommitRequest{EvidenceDigest: "sha256:" + hex.EncodeToString(sum[:]), Timestamp: request.Timestamp, BaseRef: worktree.Identity.BaseRef, ExpectedParent: request.OriginalCheckpoint}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	lease, err := r.acquireSuppliedMutation(ctx, request.MutationClaim, contracts.GitMutationClaim{Repository: worktree.Identity.Repository, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: worktree.Identity.BaseRef, ExpectedBaseOID: worktree.Identity.BaseHead, ExpectedHeadOID: request.OriginalCheckpoint})
	if err != nil {
		return "", err
	}
	defer func() {
		returnedErr = mergeMutationLeaseRelease(returnedErr, lease)
		if returnedErr != nil {
			head = ""
		}
	}()
	ctx = withMutationLease(ctx, lease)
	if err := requireMutationLease(ctx, lease); err != nil {
		return "", err
	}
	observed, replay, err := r.reconcileCommit(ctx, worktree, commit)
	if err != nil || !replay && observed != request.OriginalCheckpoint {
		return "", ErrUnsafeWorktree
	}
	checkImplementation := func() error {
		got, err := r.InspectRetainedImplementation(ctx, worktree, request.OriginalCheckpoint, request.ProtectedPaths)
		if err != nil || got != request.ImplementationDigest {
			return ErrUnsafeWorktree
		}
		return nil
	}
	checkFull := func() error {
		got, err := r.InspectRetainedWorktree(ctx, worktree)
		if err != nil || got.Changes.Head != request.OriginalCheckpoint || got.Digest != request.FullSnapshotDigest {
			return ErrUnsafeWorktree
		}
		return nil
	}
	if err := checkImplementation(); err != nil {
		return "", err
	}
	if !replay {
		if err := checkFull(); err != nil {
			return "", err
		}
	}
	// The environment helper creates/validates private HOME before allocation.
	if _, err := r.environment(nil); err != nil {
		return "", err
	}
	directory, err := os.MkdirTemp(r.Home, "protected-index-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(directory)
	root, err := openPinnedDirectory(directory)
	if err != nil {
		return "", err
	}
	defer root.Close()
	private := r
	private.privateCheckpointIndex = &protectedCheckpointIndex{root: root, path: filepath.Join(directory, "index")}
	env := []string{"GIT_INDEX_FILE=" + private.privateCheckpointIndex.path}
	command := func(args ...string) ([]byte, error) {
		if err := requireMutationLease(ctx, lease); err != nil {
			return nil, err
		}
		result, err := private.commandEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, env, args...)
		if err == nil && !private.privateCheckpointIndex.valid(private.privateCheckpointIndex.path) {
			return nil, ErrIdentityMismatch
		}
		return result, err
	}
	if _, err := command("read-tree", request.OriginalCheckpoint); err != nil {
		return "", err
	}
	paths := make([]string, len(request.ProtectedPaths))
	for i, path := range request.ProtectedPaths {
		paths[i] = ":(literal)" + path
	}
	if _, err := command(append([]string{"add", "-A", "--"}, paths...)...); err != nil {
		return "", err
	}
	tree, err := private.oneEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, env, "write-tree")
	if err != nil || !validOID(tree) {
		return "", ErrUnsafeWorktree
	}
	if err := r.validateImmutableTree(ctx, worktree.Path, request.OriginalCheckpoint, tree, DiffPolicy{AllowedPaths: request.ProtectedPaths, ExpectedHead: request.OriginalCheckpoint}); err != nil {
		return "", err
	}
	if err := checkImplementation(); err != nil {
		return "", err
	}
	if !replay {
		if err := checkFull(); err != nil {
			return "", err
		}
	}
	newHead, err := r.oneEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, deterministicCommitEnv(request.Timestamp.UTC().Format(time.RFC3339)), "commit-tree", tree, "-p", request.OriginalCheckpoint, "-m", candidateMessage(commit))
	if err != nil || !validOID(newHead) || replay && newHead != observed {
		return "", ErrUnsafeWorktree
	}
	if err := recordPreparedCommit(ctx, lease, newHead, tree); err != nil {
		return "", err
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if err := checkImplementation(); err != nil {
		return "", err
	}
	if !replay {
		if err := checkFull(); err != nil {
			return "", err
		}
		if err := requireMutationLease(ctx, lease); err != nil {
			return "", err
		}
		if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "update-ref", "--no-deref", "refs/heads/"+worktree.Branch, newHead, request.OriginalCheckpoint); err != nil {
			return "", err
		}
	}
	// Working protected bytes must still form the prepared tree. Rebuilding
	// the private index also detects edits after commit-tree before index sync.
	if _, err := command(append([]string{"add", "-A", "--"}, paths...)...); err != nil {
		return "", err
	}
	checkedTree, err := private.oneEnvExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, env, "write-tree")
	if err != nil || checkedTree != tree {
		return "", ErrUnsafeWorktree
	}
	if err := checkImplementation(); err != nil {
		return "", err
	}
	if err := requireMutationLease(ctx, lease); err != nil {
		return "", err
	}
	if _, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, append([]string{"reset", "--quiet", newHead, "--"}, paths...)...); err != nil {
		return "", err
	}
	if err := checkImplementation(); err != nil {
		return "", err
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return "", err
	}
	if head, err = r.oneExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, "rev-parse", "HEAD"); err != nil || head != newHead {
		return "", ErrUnsafeWorktree
	}
	for _, prefix := range [][]string{{"diff", "--name-only", "-z", newHead, "--"}, {"diff", "--cached", "--name-only", "-z", newHead, "--"}} {
		changed, err := r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, append(prefix, paths...)...)
		if err != nil || len(changed) != 0 {
			return "", ErrUnsafeWorktree
		}
	}
	return head, nil
}
