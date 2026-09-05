package git

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestVerifyExactProtectedBaseRequiresFreshExactRemoteTip(t *testing.T) {
	ctx, runner, repository, remote := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-exact-base-proof")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	originalBase := worktree.Identity.BaseHead

	refreshedBase := exactBaseProofCommit(t, repository, "base-one.txt", "base one\n", "base one")
	rawGit(t, repository, "push", "origin", "main")
	witness := exactBaseProofWitness(worktree, remote, originalBase, refreshedBase)
	if err := runner.VerifyExactProtectedBase(ctx, witness); err != nil {
		t.Fatalf("fresh exact protected base: %v", err)
	}

	unchanged := exactBaseProofWitness(worktree, remote, originalBase, originalBase)
	if err := runner.VerifyExactProtectedBase(ctx, unchanged); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("unchanged protected base err=%v", err)
	}

	nextBase := exactBaseProofCommit(t, repository, "base-two.txt", "base two\n", "base two")
	rawGit(t, repository, "push", "origin", "main")
	witness.MutationClaim = protectedFetchClaim(worktree, witness.OriginalBaseOID, witness.MergeOID)
	if err := runner.VerifyExactProtectedBase(ctx, witness); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("stale exact base accepted after remote advanced to %s: %v", nextBase, err)
	}
	witness.MutationClaim = protectedFetchClaim(worktree, witness.OriginalBaseOID, witness.MergeOID)
	if err := runner.VerifyProtectedBranch(ctx, witness); err != nil {
		t.Fatalf("containment proof rejected %s within %s: %v", refreshedBase, nextBase, err)
	}

	remoteTree := rawGit(t, remote, "rev-parse", "refs/heads/main^{tree}")
	unrelated := rawGit(t, remote,
		"-c", "user.name=exact-base-proof",
		"-c", "user.email=exact-base-proof@example.test",
		"commit-tree", remoteTree, "-m", "unrelated root")
	rawGit(t, remote, "update-ref", "refs/heads/main", unrelated)
	unrelatedWitness := exactBaseProofWitness(worktree, remote, originalBase, unrelated)
	if err := runner.VerifyExactProtectedBase(ctx, unrelatedWitness); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("unrelated protected base accepted: %v", err)
	}

	rawGit(t, remote, "update-ref", "-d", "refs/heads/main")
	missingWitness := exactBaseProofWitness(worktree, remote, originalBase, nextBase)
	if err := runner.VerifyExactProtectedBase(ctx, missingWitness); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("missing protected base accepted: %v", err)
	}
}

func exactBaseProofCommit(t *testing.T, repository, name, contents, message string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repository, "src", name), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	rawGit(t, repository, "add", "--", filepath.ToSlash(filepath.Join("src", name)))
	rawGit(t, repository, "commit", "-m", message)
	return rawGit(t, repository, "rev-parse", "HEAD^{commit}")
}

func exactBaseProofWitness(worktree Worktree, remote, originalBase, refreshedBase string) contracts.ProtectedBranchWitness {
	return contracts.ProtectedBranchWitness{
		Repository:      worktree.Identity.Repository,
		Worktree:        worktree.Path,
		Origin:          remote,
		ProtectedRef:    "main",
		OriginalBaseOID: originalBase,
		MergeOID:        refreshedBase,
		MutationClaim:   protectedFetchClaim(worktree, originalBase, refreshedBase),
	}
}
