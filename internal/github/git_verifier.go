package github

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
)

// ProtectedBranchGitVerifier is the production Git-side proof.  It fetches
// the protected ref immediately before checking it, then proves both that the
// original reviewed base is an ancestor of the merge and that the merge is on
// the freshly fetched protected branch.  It intentionally never expects the
// live branch to remain at OriginalBaseOID.
type ProtectedBranchGitVerifier struct{ Worktree, Remote, Binary string }

func NewProtectedBranchGitVerifier(worktree, remote, binary string) (*ProtectedBranchGitVerifier, error) {
	if !filepath.IsAbs(worktree) || remote == "" || (binary != "" && !filepath.IsAbs(binary)) {
		return nil, ErrPolicyRefusal
	}
	if binary == "" {
		binary = "/usr/bin/git"
	}
	return &ProtectedBranchGitVerifier{Worktree: worktree, Remote: remote, Binary: binary}, nil
}

func (v *ProtectedBranchGitVerifier) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
	if v == nil || repository.Host != "github.com" || !validRef(baseRef) || !validOID(mergeCommit) || !validOID(originalBaseOID) {
		return contracts.ProtectedBranchObservation{}, ErrPolicyRefusal
	}
	url, err := v.git(ctx, "remote", "get-url", v.Remote)
	if err != nil || !matchesGitHubRepository(strings.TrimSpace(url), repository) {
		return contracts.ProtectedBranchObservation{}, ErrPolicyRefusal
	}
	localRef := "refs/sf/protected-proof/" + baseRef
	if _, err := v.git(ctx, "fetch", "--no-tags", "--force", v.Remote, "+refs/heads/"+baseRef+":"+localRef); err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	tip, err := v.git(ctx, "rev-parse", localRef)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	tip = strings.TrimSpace(tip)
	if !validOID(tip) || v.ancestor(ctx, originalBaseOID, mergeCommit) != nil || v.ancestor(ctx, mergeCommit, tip) != nil {
		return contracts.ProtectedBranchObservation{}, ErrExternalMerged
	}
	// Parent semantics prevent treating an arbitrary reachable commit as a
	// protected merge result: at least one direct parent must descend from the
	// sealed original base.
	parents, err := v.git(ctx, "show", "-s", "--format=%P", mergeCommit)
	if err != nil {
		return contracts.ProtectedBranchObservation{}, err
	}
	ok := false
	for _, parent := range strings.Fields(parents) {
		if validOID(parent) && v.ancestor(ctx, originalBaseOID, parent) == nil {
			ok = true
			break
		}
	}
	if !ok {
		return contracts.ProtectedBranchObservation{}, ErrExternalMerged
	}
	return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: tip, Contains: true}, nil
}

func matchesGitHubRepository(url string, repository contracts.RepositoryIdentity) bool {
	want := repository.Owner + "/" + repository.Name
	url = strings.TrimSuffix(strings.TrimSuffix(url, "/"), ".git")
	return strings.HasSuffix(url, "/"+want) && (strings.Contains(url, "github.com") || strings.Contains(url, "github.com:"))
}

func (v *ProtectedBranchGitVerifier) ancestor(ctx context.Context, older, newer string) error {
	_, err := v.git(ctx, "merge-base", "--is-ancestor", older, newer)
	return err
}
func (v *ProtectedBranchGitVerifier) git(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, v.Binary, append([]string{"-C", v.Worktree}, args...)...).Output()
	if err != nil {
		return "", errors.New("protected git proof failed")
	}
	return string(out), nil
}
