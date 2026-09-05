package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
)

// ObserveWorktreeCreationAbsent is a read-only certificate for a coordinator
// holding the Store's exclusive creation-observation lease. A missing directory
// alone is insufficient: partial Git creation may have left a branch, private
// base ref, or administrative registration. Every such artifact refuses retry.
// The caller must authenticate its lease before and after this bounded check.
func (r Runner) ObserveWorktreeCreationAbsent(ctx context.Context, claim contracts.GitMutationClaim) error {
	if !validMutationClaim(claim) || claim.Operation != "create-worktree" ||
		!validAbsolutePath(claim.Worktree) || !strings.HasPrefix(claim.Branch, "sf/") ||
		!validRef(claim.Branch) || claim.ExpectedBaseOID != claim.ExpectedHeadOID {
		return ErrIdentityMismatch
	}
	repository, err := canonicalExistingRepository(claim.Repository)
	if err != nil || repository != claim.Repository {
		return ErrIdentityMismatch
	}
	if err := r.PreflightRepository(ctx, repository, claim.BaseRef); err != nil {
		return err
	}
	dev, ino, err := directoryIdentity(repository)
	if err != nil {
		return err
	}
	parent := filepath.Dir(claim.Worktree)
	canonicalParent, err := filepath.EvalSymlinks(parent)
	if err != nil || canonicalParent != parent {
		return ErrIdentityMismatch
	}
	parentDev, parentIno, err := directoryIdentity(parent)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(claim.Worktree); !errors.Is(err, os.ErrNotExist) {
		return ErrWorktreeQuarantined
	}
	refs, err := r.commandExpected(ctx, repository, dev, ino, "for-each-ref", "--format=%(refname)", "refs/heads/"+claim.Branch, worktreeBaseRef(claim.Branch))
	if err != nil {
		return err
	}
	if len(refs) != 0 {
		return ErrWorktreeQuarantined
	}
	listing, err := r.commandExpected(ctx, repository, dev, ino, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return err
	}
	if len(listing) == 0 || listing[len(listing)-1] != 0 {
		return ErrIdentityMismatch
	}
	count := 0
	for _, field := range strings.Split(string(listing), "\x00") {
		switch {
		case strings.HasPrefix(field, "worktree "):
			count++
			path := strings.TrimPrefix(field, "worktree ")
			if count > 1024 || !validAbsolutePath(path) || filepath.Clean(path) == claim.Worktree {
				return ErrWorktreeQuarantined
			}
		case strings.HasPrefix(field, "branch "):
			if strings.TrimPrefix(field, "branch ") == "refs/heads/"+claim.Branch {
				return ErrWorktreeQuarantined
			}
		case field == "", field == "bare", field == "detached", strings.HasPrefix(field, "HEAD "),
			field == "locked", strings.HasPrefix(field, "locked "), field == "prunable", strings.HasPrefix(field, "prunable "):
		default:
			return ErrIdentityMismatch
		}
	}
	if count == 0 {
		return ErrIdentityMismatch
	}
	if d, i, err := directoryIdentity(repository); err != nil || d != dev || i != ino {
		return ErrIdentityMismatch
	}
	if d, i, err := directoryIdentity(parent); err != nil || d != parentDev || i != parentIno {
		return ErrIdentityMismatch
	}
	if _, err := os.Lstat(claim.Worktree); !errors.Is(err, os.ErrNotExist) {
		return ErrWorktreeQuarantined
	}
	return nil
}
