package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func protectedCheckpointFixture(t *testing.T) (context.Context, Runner, Worktree, ProtectedCheckpointRequest) {
	t.Helper()
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-protected")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	write := func(path, value string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(worktree.Path, path), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("src/main.txt", "staged implementation\n")
	rawGit(t, worktree.Path, "add", "--", "src/main.txt")
	write("src/main.txt", "unstaged implementation\n")
	write("src/new.txt", "untracked implementation\n")
	write("proof.txt", "accepted independent proof\n")
	full, err := runner.InspectRetainedWorktree(ctx, worktree)
	if err != nil {
		t.Fatal(err)
	}
	implementation, err := runner.InspectRetainedImplementation(ctx, worktree, worktree.Identity.BaseHead, []string{"proof.txt"})
	if err != nil {
		t.Fatal(err)
	}
	request := ProtectedCheckpointRequest{EvidenceDigest: digest([]byte("accepted amendment")), Timestamp: time.Unix(1, 0).UTC(), OriginalCheckpoint: worktree.Identity.BaseHead, ProtectedPaths: []string{"proof.txt"}, FullSnapshotDigest: full.Digest, ImplementationDigest: implementation, MutationClaim: commitClaim(worktree, worktree.Identity.BaseHead)}
	return ctx, runner, worktree, request
}

func TestProtectedCheckpointRetainsImplementationAndReplaysIndexSync(t *testing.T) {
	ctx, runner, worktree, request := protectedCheckpointFixture(t)
	lease := &factMutationLease{}
	runner.MutationAuthority = factMutationAuthority{lease: lease}
	head, err := runner.CommitProtectedCheckpoint(ctx, worktree, request)
	if err != nil {
		t.Fatal(err)
	}
	if head == request.OriginalCheckpoint || lease.preparedCommit != head || lease.tree == "" {
		t.Fatal("checkpoint lacks exact prepared tuple")
	}
	if got := rawGit(t, worktree.Path, "show", head+":src/main.txt"); got != "base" {
		t.Fatalf("implementation entered protected commit: %q", got)
	}
	if got := rawGit(t, worktree.Path, "show", head+":proof.txt"); got != "accepted independent proof" {
		t.Fatalf("protected proof missing: %q", got)
	}
	if got, err := runner.InspectRetainedImplementation(ctx, worktree, request.OriginalCheckpoint, request.ProtectedPaths); err != nil || got != request.ImplementationDigest {
		t.Fatalf("implementation changed: %v", err)
	}
	// Model a crash after ref CAS but before protected index synchronization.
	// No implementation bytes or index entries are changed by this setup.
	rawGit(t, worktree.Path, "reset", "--quiet", request.OriginalCheckpoint, "--", "proof.txt")
	replayed, err := runner.CommitProtectedCheckpoint(ctx, worktree, request)
	if err != nil || replayed != head {
		t.Fatalf("replay=%s err=%v", replayed, err)
	}
	if got, err := runner.InspectRetainedImplementation(ctx, worktree, request.OriginalCheckpoint, request.ProtectedPaths); err != nil || got != request.ImplementationDigest {
		t.Fatalf("replay changed implementation: %v", err)
	}
	wrong := request
	wrong.FullSnapshotDigest = digest([]byte("different accepted snapshot"))
	if _, err := runner.CommitProtectedCheckpoint(ctx, worktree, wrong); err == nil {
		t.Fatal("different snapshot adopted an existing checkpoint")
	}
}

func TestProtectedCheckpointRefusesChangedProofOrImplementation(t *testing.T) {
	for _, name := range []string{"full_digest", "implementation_digest", "implementation_bytes", "protected_bytes", "scope", "prepared_failure"} {
		t.Run(name, func(t *testing.T) {
			ctx, runner, worktree, request := protectedCheckpointFixture(t)
			switch name {
			case "full_digest":
				request.FullSnapshotDigest = digest([]byte("wrong full"))
			case "implementation_digest":
				request.ImplementationDigest = digest([]byte("wrong implementation"))
			case "implementation_bytes":
				if err := os.WriteFile(filepath.Join(worktree.Path, "src/main.txt"), []byte("foreign edit\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "protected_bytes":
				if err := os.WriteFile(filepath.Join(worktree.Path, "proof.txt"), []byte("unreviewed proof\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "scope":
				request.ProtectedPaths = []string{"."}
			case "prepared_failure":
				runner.MutationAuthority = factMutationAuthority{lease: &factMutationLease{recordErr: errors.New("prepared persistence failed")}}
			}
			if head, err := runner.CommitProtectedCheckpoint(ctx, worktree, request); err == nil || head != "" {
				t.Fatalf("unsafe checkpoint=%s err=%v", head, err)
			}
			if head := rawGit(t, worktree.Path, "rev-parse", "HEAD"); head != request.OriginalCheckpoint {
				t.Fatal("ref advanced on refusal")
			}
			if name == "prepared_failure" {
				if full, err := runner.InspectRetainedWorktree(ctx, worktree); err != nil || full.Digest != request.FullSnapshotDigest {
					t.Fatalf("private staging changed original worktree/index on failure: %v", err)
				}
			}
		})
	}
}

func TestPrivateCheckpointIndexEnvironmentRequiresCapability(t *testing.T) {
	_, runner, _, _ := fixture(t)
	if _, err := runner.environment([]string{"GIT_INDEX_FILE=" + filepath.Join(t.TempDir(), "index")}); err == nil {
		t.Fatal("arbitrary private index environment admitted")
	}
}
